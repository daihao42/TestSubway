# sc is an existing SparkContext.
#from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

'''
    save SubwayFlow as cardID,start,starttime,end,endtime,od
    use right join instead insert
'''
__author__ = 'dai'

class SubwayFlow():
    def __init__(self,sc,spark):
        schemaString = 'cardid time od'
        fileds = [StructField(field_name,StringType(),True) for field_name in schemaString.split()]
        schema = StructType(fileds)

        # Load a text file and convert each line to a Row.
        # Infer the schema, and register the DataFrame as a table.
        self.schemaPaths = spark.createDataFrame([], schema)
        self.schemaPaths.createOrReplaceTempView("subwayflow")

    def getSql(self):
        return self.schemaPaths

    # SQL can be run over DataFrames that have been registered as a table.
    #teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    #teenNames = teenagers.map(lambda p: "Name: " + p.name)
    #for teenName in teenNames.collect():
	  #print teenName
