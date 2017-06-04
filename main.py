## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
import sys
import odFilter,nameNo

## Module Constants
APP_NAME = "Subway_Flow"

master = "yarn-client"

## Closure Functions

## Main functionality

def main(sc):
	data = sc.textFile("./input/"+sys.argv[1])
	NameNo = nameNo.NameNo(sc)
	odfilter = odFilter.ODFilter(NameNo)
	data = odfilter.getOD(data)
	data.saveAsTextFile('res')

if __name__ == '__main__':
# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster(master)
	sc   = SparkContext(conf=conf)
	# Execute Main functionality
	main(sc)
