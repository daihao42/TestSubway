## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys,datetime

'''
    input: sc.textFile('')
    output:"cardId,time,stationid,tradetype"
'''
__author__ = 'dai'


## Module Constants
APP_NAME = "SZTConvert"

master = "spark://192.168.40.97:7077"

## Closure Functions

## Main functionality

def main(sc,spark):
	data = sc.textFile("./szt/"+sys.argv[1])
        nameNo = NameNo(sc)
	sztfilter = SZTFilter(nameNo)
	data = sztfilter.Convert(data)
        data.saveAsTextFile('./subway/'+sys.argv[1])


class SZTFilter():
    def __init__(self,nn):
        self.NameNo = nn

    def formatTime(self,t):
        t = t.replace('T',' ')
	return t[:-5]

    def ssplit(self,x):
	L = x.split(',')
	return L[1]+','+self.formatTime(L[4])+','+self.NameNo.Name2No(L[6])+','+L[3]

    def Convert(self,data):
	data = data.map(self.ssplit)
	return data

'''
    change station's name to number
    by nameno.csv
'''

class NameNo():
    def __init__(self,sc):
        self.nameno = {}
	data = sc.textFile('SubwayFlowConf/nameno.csv')
	data = data.collect()
	for i in data:
	    L = i.split(',')
	    self.nameno[L[0]] = L[1]
    
    def Name2No(self,name):
        return self.nameno[name]



if __name__ == '__main__':
# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster(master)
	sc   = SparkContext(conf=conf)
        spark = SparkSession.builder.master(master).appName(APP_NAME).getOrCreate()
	# Execute Main functionality
	main(sc,spark)
