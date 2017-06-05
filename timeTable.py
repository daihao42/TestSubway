# sc is an existing SparkContext.
#from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

'''
    translate AllPath file to spark-sql dataframe
    to lookup path for OD2Path 
'''
__author__ = 'dai'

class TimeTable():
    def __init__(self,sc,spark):
        schemaString = 'start starttime end endtime time'
        fileds = [StructField(field_name,StringType(),True) for field_name in schemaString.split()]
        schema = StructType(fileds)

        # Load a text file and convert each line to a Row.
        lines = sc.textFile("SubwayFlowConf/timeTable/work")
        parts = lines.map(lambda l: l.split(","))
        paths = parts.map(lambda p: (p[0],p[1],p[2],p[3],int(p[4])))
        # Infer the schema, and register the DataFrame as a table.
        self.schemaWork = spark.createDataFrame(paths, schema)
        self.schemaWork.createOrReplaceTempView("worktimetable")
        
        lines = sc.textFile("SubwayFlowConf/timeTable/work")
        parts = lines.map(lambda l: l.split(","))
        paths = parts.map(lambda p: (p[0],p[1],p[2],p[3],int(p[4])))
        # Infer the schema, and register the DataFrame as a table.
        self.schemaWeek = spark.createDataFrame(paths, schema)
        self.schemaWeek.createOrReplaceTempView("weektimetable")


    def getSql(self):
        return self.schemaWork,self.schemaWeek

    # SQL can be run over DataFrames that have been registered as a table.
    #teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    #teenNames = teenagers.map(lambda p: "Name: " + p.name)
    #for teenName in teenNames.collect():
	  #print teenName

class TimeTableDict():
    def __init__(self,wp):
        self.stations = {}
        for i in range(len(wp)):
            L = wp.iloc[i]
            try:
                dic = self.stations[L['start']+L['end']]
                dic['index'].append(L['starttime'])
                sorted(dic['index'])
                dic['time'][L['starttime']] = L['endtime']
            except:
                self.stations[L['start']+L['end']] = {}
                dic = self.stations[L['start']+L['end']]
                dic['index'] = []
                dic['index'].append(L['starttime'])
                dic['time'] = {L['starttime']:L['endtime']}


    def getEndTime(self,start,end,starttime):
        dic = self.stations[start+end]
        try:
            endtime = dic['time'][starttime]
        except:
            starttime = self.dividSearch(starttime,dic['index'])
            endtime = dic['time'][starttime]
        return starttime,endtime

    def dividSearch(self,time,index):
        L = index
        length = len(index)
        i = length / 2
        while i>0 and i<len(L)-1:
            l1 = L[i + 1]
            if time > L[i]:
                L = L[i+1:]
            else:
                L = L[:i+1]
            i = len(L)/2
        return L[i]

