rm pac.zip
zip -0 pac.zip odFilter.py nameNo.py
fs -rm -r res
spark-submit --py-files pac.zip main.py 20160130
