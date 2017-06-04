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
