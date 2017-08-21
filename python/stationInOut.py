'''
    use / to getIndex

'''

class StationInOut():
    def __init__(self,s):
        self.span = s

    def timeIndex(self,x):
        L = x.split(':')
        return str(((int(L[0])*60) + int(L[1]))/self.span)

    def resetIndex(self,x):
        return '%02d:%02d'%((x*self.span)/60,(x*self.span)%60)

    def ssplit(self,x):
        L = x.split(',')
        return (self.timeIndex(L[1][11:-3])+','+L[2]+','+L[3],x)

    def runner(self,data):
        data1 = data.map(self.ssplit)
        data1 = data1.countByKey()
        res = []
        for k,v in data1.items():
            L = k.split(',')
            L[0] = self.resetIndex(int(L[0]))
            L.append(v)
            res.append(L)
        return res


