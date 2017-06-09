class ReduceFlow():
    def ssplit(self,x):
        L = x.split(',')
        return (','.join(L[:3]),','.join(L[3:5]))

    def ssreduce(self,x,y):
        L1 = x.split(',')
        L2 = y.split(',')
        return str(int(L1[0])+int(L2[0]))+','+str(int(L1[1])+int(L2[1]))

    def getReduceFlow(self,data):
        data = data.map(self.ssplit)
        data = data.reduceByKey(self.ssreduce)
        return data.map(lambda x:x[0]+','+x[1])

