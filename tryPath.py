class TryPath():
    def __init__(self,tts):
        self.tts = tts

    def dpkPath(self,x):
        L = x.split(',')
        starttime = L[2]
        res = L[:7]
        for i in range(7,len(L) - 1):
            starttime,endtime = self.tts.getEndTime(L[i],L[i+1],starttime)
            res.append(L[i])
            res.append(L[i+1])
            res.append(starttime)
            res.append(endtime)
            starttime = endtime
        ress = res[0]
        for i in res[1:]:
            ress = ress+','+i
        return ress

    def getAllPath(self,data):
        return data.map(self.dpkPath)


