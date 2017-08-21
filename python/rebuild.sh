rm pac.zip
zip -0 pac.zip odFilter.py ODDataFrame.py allPath.py timeTable.py tryPath.py flatPathTime.py reduceFlow.py stationInOut.py lineFlow.py
#hadoop fs -rm -r res
#hadoop fs -rm -r odpath
#hadoop fs -rm -r pathtime
spark-submit --py-files pac.zip main.py 20170301
#for i in {01..30}
#do
#spark-submit --py-files pac.zip main.py 201703$i
#done
#for i in {01..28}
#do
#spark-submit --py-files pac.zip main.py 201702$i
#done
