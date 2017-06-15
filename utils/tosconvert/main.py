## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys,datetime

'''
    input: sc.textFile('')
    output:"cardId,time,stationid,tradetype"

'''
## Module Constants
APP_NAME = "TOSConvert"

master = "spark://192.168.40.97:7077"

## Closure Functions

## Main functionality

def main(sc,spark):
	data = sc.textFile("./TOS/"+sys.argv[1])
	data = data.map(ssplit)
        data.saveAsTextFile('subway/'+(sys.argv[1].replace('-','')))


def formatTime(t):
        t = t.replace('T',' ')
	return t[:-5]

def ssplit(x):
    L = x.split(',')
    L = L[:-1]
    L[1] = formatTime(L[1])
    return ','.join(L)

if __name__ == '__main__':
# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster(master)
	sc   = SparkContext(conf=conf)
        spark = SparkSession.builder.master(master).appName(APP_NAME).getOrCreate()
	# Execute Main functionality
	main(sc,spark)
