package com.sibat.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/******************
  join od and allpath
*****************/
object ALLPathChooser {

  case class OD(cardid:String,
                odstarttime:String,
                odstart:String,
                odendtime:String,
                odend:String,
                duringtime:Int)

  case class ALLPath(apstart:String,
                      apend:String,
                      aptime:Int,
                      appath:Array[String])


  private def ODSql(sqlContext:SQLContext,data:RDD[String]) = {
    import sqlContext.implicits._
    data.map(_.split(",")).
            map(arr => OD(arr(0),
                        arr(1).substring(arr(1).length-8,arr(1).length),
                        arr(2),
                        arr(3).substring(arr(1).length-8,arr(1).length),
                        arr(4),
                        arr(5).toInt)).toDF
  }

  private def ALLPathSql(sqlContext:SQLContext,data:RDD[String]) = {
    import sqlContext.implicits._
    data.map(_.split(",")).
          map(arr => ALLPath(arr(0),
                      arr(arr.length - 2),
                      arr(arr.length - 1).toInt,
                      arr.init)).toDF
  }


  private def ODAllPathJoin(sqlContext:SQLContext,od:RDD[String],path:RDD[String]) = {
    val ODDf = ODSql(sqlContext,od)
    val pathDf = ALLPathSql(sqlContext,path)
    ODDf.join( pathDf,(ODDf("odstart") === pathDf("apstart"))
                  &&  (ODDf("odend") === pathDf("apend"))).
                  select("cardid",
                        "odstart",
                        "odstarttime",
                        "odend",
                        "odendtime",
                        "duringtime",
                        "aptime",
                        "appath")
  }

  def getODAllpath(sqlContext:SQLContext,od:RDD[String],path:RDD[String]):RDD[Any] = {
    import org.apache.spark.sql.functions.concat_ws
    import org.apache.spark.sql.functions.col
    ODAllPathJoin(sqlContext,od,path).select(concat_ws(",",
                                          col("cardid"),
                                          col("odstart"),
                                          col("odstarttime"),
                                          col("odend"),
                                          col("odendtime"),
                                          col("duringtime"),
                                          col("aptime"),
                                          col("appath"))).rdd
                                          .map(x => x(0))
  }

}
