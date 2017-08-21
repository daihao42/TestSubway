class LineFlow():
    def __init__(self,se,span):
        dic = {}
        for i in range(len(se)):
            tmp = str(se.iloc[i,1])+str(se.iloc[i,2])
            dic[tmp] = str(se.iloc[i,0])
        self.sec2line = dic
        self.span = span

    def flatsplit(self,path):
        path = path.split(',')
        path = path[7:]
        L = []
        lastLine = ''
        for i in range(0,len(path),4):
##### in and out in the same station
            if path[i+0] == path[i+1]:
                continue
            line = self.sec2line[path[i+0]+path[i+1]]
            res = ''
            if not(lastLine == line):
                res = (self.timeIndex(path[i+2][:-3])+','+line,'1')
                L.append(res)
                lastLine = line
        return L

    def runner(self,data):
        data = data.flatMap(self.flatsplit)
        data = data.countByKey()
        res = []
        for k,v in data.items():
            L = k.split(',')
            L[0] = self.resetIndex(L[0])
            L.append(str(v))
            res.append(','.join(L))
        return res

    def timeIndex(self,x):
        L = x.split(':')
        return str(((int(L[0])*60) + int(L[1]))/self.span)

    def resetIndex(self,x):
        x = int(x)
        return '%02d:%02d'%((x*self.span)/60,(x*self.span)%60)


