package com.sibat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import com.sibat.utils.ODFilter
import com.sibat.utils.StationCounter
import com.sibat.utils.ALLPathChooser
import com.sibat.utils.PathTime
import com.sibat.utils.SectionFlowCounter

object Main  {

	def sparkContextInit(url:String)(appName:String):SparkContext = new SparkContext(new SparkConf().setAppName(appName).setMaster(url))

	def main(args: Array[String]): Unit = {

		val sc = sparkContextInit("spark://node1:7077")("SubwayFlow")

		// ods
		val ods = ODFilter.getOD(sc.textFile("subway/20170301"))
		// ods.saveAsTextFile("ods/new20170301")

		//stationCounter
    // val indataRDD = sc.parallelize(
    //                   StationCounter.getStation(sc.textFile("subway/20170301"),30)
    //                 )
    // indataRDD.saveAsTextFile("stationCounter/20170301")

		// allPath
		val sqlContext = new SQLContext(sc)
		// val ods = sc.textFile("ods/new20170301")
		val path = sc.textFile("SubwayFlowConf/AllPath")
		val allpath = ALLPathChooser.getODAllpath(sqlContext,ods.asInstanceOf[RDD[String]],path)
		// allpath.saveAsTextFile("AllPathTest")

		//pathtime
		// val allpath = sc.textFile("AllPathTest")
		val timeTable = sc.textFile("SubwayFlowConf/timeTable/work")
		// val sqlContext = new SQLContext(sc)
		val walkIn = sc.textFile("SubwayFlowConf/walkIn")
		val trans = sc.textFile("SubwayFlowConf/minTrans")
		val pathtime = PathTime.getPathTime(sqlContext,allpath.asInstanceOf[RDD[String]],walkIn,trans,timeTable)
		// pathtime.saveAsTextFile("PathTime/new20170301")

		//section flow
		// val pathtime = sc.textFile("PathTime/new20170301")
		SectionFlowCounter.getSectionFlow(pathtime)
										.saveAsTextFile("SectionFlow/new20170301")


    sc.stop()
	}


}
