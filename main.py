## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys,datetime,pandas
import odFilter,nameNo
import ODDataFrame
import allPath,timeTable,tryPath 

## Module Constants
APP_NAME = "Subway_Flow"

master = "spark://192.168.40.97:7077"

## Closure Functions

## Main functionality

def main(sc,spark):
	data = sc.textFile("./input/"+sys.argv[1])
	NameNo = nameNo.NameNo(sc)
	odfilter = odFilter.ODFilter(NameNo)
	data = odfilter.getOD(data)
	data.saveAsTextFile('res')
        odd = ODDataFrame.ODData(sc,spark)
        od = odd.getSql()
        app = allPath.AllPath(sc,spark)
        path = app.getSql()
        res = od.join(path,[od.odstart == path.apstart,od.odend == path.apend])
        ress = res.select(od.cardid,od.odstart,od.odstarttime,od.odend,od.odendtime,od.odtime,path.aptime,path.appath)
        odpath = ress.rdd
        odpath = odpath.map(cleanSql)
        odpath.saveAsTextFile('odpath')
        tt = timeTable.TimeTable(sc,spark)
        work,week = tt.getSql()
        if(isWeekend(sys.argv[1])):
            wp = week.toPandas()
        else :
            wp = work.toPandas()
        tts = timeTable.TimeTableDict(wp)
        tp = tryPath.TryPath(tts)
        pathtime = tp.getAllPath(odpath)
        pathtime.saveAsTextFile('pathtime')



def isWeekend(s):
    tt = datetime.datetime.strptime(s,'%Y%m%d')
    return (tt.isoweekday() == 6)or(tt.isoweekday() == 7)


def cleanSql(rs):
    dd = rs.asDict()
    res = dd['cardid']+','+dd['odstart']+','+dd['odstarttime']+','+dd['odend']+','+dd['odendtime']+','+dd['odtime']+','+dd['aptime']
    for i in eval(dd['appath']):
        res = res+','+str(i)
    return res


if __name__ == '__main__':
# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster(master)
	sc   = SparkContext(conf=conf)
        spark = SparkSession.builder.master(master).appName(APP_NAME).getOrCreate()
	# Execute Main functionality
	main(sc,spark)
