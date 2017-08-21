package com.sibat.utils

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD

object ODFilter {

  private def ssplit(x:String) = {
    val L = x.split(",")
    (L(0),List(L(1),L(0),L(3),L(2)).mkString(","))
  }

  private def delTime(t1:String,t2:String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
  }

  private def ODRuler(x:String,y:String) = {
    val L1 = x.split(",")
    val L2 = y.split(",")
    val difftime = delTime(L1(0),L2(0))
    if(
      ((L1(2) == "21") && (L2(2) == "22")) &&
      ( difftime < 10800) && (L1(3) != L2(3))
    ) List(L1(1),L1(0),L1(3),L2(0),L2(3),(difftime.toString)).mkString(",")
    else None
  }

  private def MakeOD(x:(String,Iterable[String])) = {
    val arr = x._2.toArray.sortWith((x,y) => x < y)
    for{
       i <- 0 until arr.size - 1;
       od = ODRuler(arr(i),arr(i+1))
     } yield od
  }

  def getOD(data:RDD[String]) = {
    data.map(ssplit)
        .groupByKey()
        .flatMap(MakeOD)
        .filter(x => x != None)
  }

}
