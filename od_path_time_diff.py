## Imports
from allPath import AllPath

class ODDiff():
    def __init__(self,sc,spark):
        ap = AllPath(sc,spark)
	self.sp = ap.getSql().toPandas()

    def getTimes(self,ori):
        L = ori.split(',')
	#sp = self.sl.registerDataFrameAsTable(sp, "paths")
	res = self.sp[](self.sp.start == L[2]) \
	    .where(self.sp.end == L[4]) \
	    .orderBy(self.sp.time) \
	    .select(self.sp.time).collect()
	ress = [x[0] for x in res]
	return ori+','+str(int(L[-1]) - int(ress[0]))+','+str(int(L[-1]) - int(ress[-1]))

    def getDiff(self,sc):
        od = sc.textFile("res").collect()
        return od.map(self.getTimes)
