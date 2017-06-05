rm pac.zip
zip -0 pac.zip odFilter.py nameNo.py ODDataFrame.py allPath.py timeTable.py tryPath.py 
hadoop fs -rm -r res
hadoop fs -rm -r odpath
hadoop fs -rm -r pathtime
spark-submit --py-files pac.zip main.py 20160130
