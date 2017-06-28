## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys,datetime,pandas
import odFilter
import ODDataFrame,stationInOut,lineFlow
import allPath,timeTable,tryPath,flatPathTime,reduceFlow

'''

    output:starttime,sectionstatr,sectionend,laststation,flow,transflow

'''
## Module Constants
APP_NAME = "Subway_Flow"

master = "spark://192.168.40.97:7077"

## Closure Functions

## Main functionality

def main(sc,spark):
	data = sc.textFile("./subway/"+sys.argv[1])
        try:
            span = int(sys.argv[2])
        except:
            span = 15
##### get stationin stationout #########
        stationinout = stationInOut.StationInOut(span)
        stationdf = pandas.DataFrame(stationinout.runner(data))
        saveStation(stationdf,sys.argv[1],span)



###### get flow and trans ############
	odfilter = odFilter.ODFilter()
	data = odfilter.getOD(data)
	#data.saveAsTextFile('ods/'+sys.argv[1])
        odd = ODDataFrame.ODData(spark,data)
        od = odd.getSql()
        app = allPath.AllPath(sc,spark)
        path = app.getSql()
        res = od.join(path,[od.odstart == path.apstart,od.odend == path.apend])
        ress = res.select(od.cardid,od.odstart,od.odstarttime,od.odend,od.odendtime,od.odtime,path.aptime,path.appath)
        odpath = ress.rdd
        odpath = odpath.map(cleanSql)
        #odpath.saveAsTextFile('odpath')
        tt = timeTable.TimeTable(sc,spark)
        work,week = tt.getSql()
        if(isWeekend(sys.argv[1])):
            wp = week.toPandas()
        else :
            wp = work.toPandas()
        walk = sc.textFile('SubwayFlowConf/walkIn')
        walkIn = getWalkIn(walk)
	trans = sc.textFile('SubwayFlowConf/minTrans')
	transIn = getTranIn(trans)
        tts = timeTable.TimeTableDict(wp,transIn)
        tp = tryPath.TryPath(tts,walkIn)
        pathtime = tp.getAllPath(odpath)
        fpt = flatPathTime.FlatPathTime(walkIn)
        choosepath,flatpathtime = fpt.getAllSection(pathtime)
        #choosepath = sc.textFile('choosePath/'+sys.argv[1])
#### line flow ##############
        sec2line = pandas.read_csv('SubwayFlowConf/sec2line.csv',header=None)
        lf = lineFlow.LineFlow(sec2line,span)
        saveLineFlow(lf.runner(choosepath),span)
        #choosepath.saveAsTextFile('choosePath/'+sys.argv[1])
        res = reduceFlow.ReduceFlow().getReduceFlow(flatpathtime)
        #pathtime.saveAsTextFile('pathtime')
        #res.saveAsTextFile(sys.argv[1]+'_sectionFlow')
        sectionflow = saveAsCsv(res.collect(),span)

def saveStation(df,sdate,span):
    df[df[2] == '21'].to_csv('result/stationin/'+sdate+'_'+str(span),index = None,header = None)
    df[df[2] == '22'].to_csv('result/stationout/'+sdate+'_'+str(span),index = None,header = None)
    df[df[2] == '01'].to_csv('result/stationsale/'+sdate+'_'+str(span),index = None,header = None)


     
def saveLineFlow(data,span):
    with open('result/lineflow/'+sys.argv[1]+'_'+str(span),'w')as f:
        for i in data:
            f.write(i+'\n')

def saveAsCsv(data,span):
    L = map(lambda x:x.split(','),data)
    df = pandas.DataFrame(L)
    df.to_csv('result/sectionFlow/'+sys.argv[1]+'_s',header = None,index = None)
    saveSectionTimeSpan(sys.argv[1],span)
    return df

def getTranIn(data):
    transIn = {}
    for i in data.collect():
        L = i.split(',')
        transIn[L[0]] = int(float(L[1]))
    return transIn

def getWalkIn(data):
    walkIn = {}
    for i in data.collect():
        L = i.split(',')
        walkIn[L[0]] = int(float(L[1]))
    return walkIn

def isWeekend(s):
    tt = datetime.datetime.strptime(s,'%Y%m%d')
    return (tt.isoweekday() == 6)or(tt.isoweekday() == 7)


def cleanSql(rs):
    dd = rs.asDict()
    res = dd['cardid']+','+dd['odstart']+','+dd['odstarttime']+','+dd['odend']+','+dd['odendtime']+','+dd['odtime']+','+dd['aptime']
    for i in eval(dd['appath']):
        res = res+','+str(i)
    return res
    
def saveSectionTimeSpan(sdate,timeSpan):
    data = pandas.read_csv('result/sectionFlow/'+sdate+'_s',header = None)
    data = data.sort_values([0])
    data.index = data[0]
    timeformat = '%H:%M:%S'
    start = datetime.datetime.strptime('00:00:00',timeformat)
    end = datetime.datetime.strptime('23:59:59',timeformat)
    span = datetime.timedelta(minutes = timeSpan)
    L = []
    while start < end:
        str1 = datetime.datetime.strftime(start,timeformat)
        start = start + span
        str2 = datetime.datetime.strftime(start,timeformat)
        gpc = data[str1:str2].groupby(by=[1,2,3]).sum()
        gpc[6] = str2
        L.append(gpc)

    res = pandas.concat(L).reset_index()
    res.to_csv('result/sectionFlowSpan/'+sdate+'_'+str(timeSpan),header = None,index = None)
    saveTrans(res,sdate,timeSpan)

def saveTrans(data,sdate,timeSpan):
    res = data[(data[1] == data[3])&(data[1] != data[2])]
    trans = pandas.read_csv('SubwayFlowConf/trans',header=None)
    #trans = trans[0].tolist()
    #res = res[res[1] in trans]
    res = pandas.merge(res,trans,left_on=1,right_on=0)
    res.to_csv('result/trans/'+sdate+'_'+str(timeSpan),header = None,index = None)

def timeIndex(self,x):
    L = x.split(':')
    return str(((int(L[0])*60) + int(L[1]))/self.span)

def resetIndex(self,x):
    return '%02d:%02d'%((x*self.span)/60,(x*self.span)%60)




if __name__ == '__main__':
# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster(master)
	sc   = SparkContext(conf=conf)
        spark = SparkSession.builder.master(master).appName(APP_NAME).getOrCreate()
	# Execute Main functionality
	main(sc,spark)
