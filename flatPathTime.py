import datetime

'''
return :
    starttime,start,end,flowcounter(1),tarnscounter(1 or 0)

'''

class FlatPathTime():
    def __init__(self,walkIn):
        self.walkIn = walkIn

    def addWalkOutTime(self,station,s):
        fmt = "%H:%M:%S"
        dd = datetime.datetime.strptime(s,fmt)
        wl = datetime.timedelta(seconds = self.walkIn[station])
        return datetime.datetime.strftime(dd+wl,fmt)

    def flatsplit(self,x):
        if len(x[1].data) >1:
            path = self.filterPath(x[1].data)
        else :
            path = x[1].data[0]
        path = path.split(',')
        path = path[7:]
        L = []
        # mark endtime and if endtime != starttime means a trans happened
        lnext = path[3]
        for i in range(0,len(path),4):
            res = ''
            trans = 0
            # trans passenger
            # starttime != pre endtime
            if not(lnext == path[i+2]):
                trans = 1
            res = path[i+2]+','+path[i]+','+path[i+1]+',1'+','+str(trans)
            L.append(res)
            lnext = path[i+3]
        return L

    def filterPath(self,data):
        aviliable = []
        for i in data:
            L = i.split(',')
            if self.addWalkOutTime(L[-3],L[-1]) < L[4]:
                aviliable.append(i)
        if len(aviliable) == 0:
            return self.chooseCloser(data)
        else:
            return self.chooseCloser(aviliable)

    def minusTime(self,s1,s2):
        fmt = "%H:%M:%S"
        t1 = datetime.datetime.strptime(s1,fmt)
        t2 = datetime.datetime.strptime(s2,fmt)
        return abs((t2-t1).seconds)


    def chooseCloser(self,data):
        if len(data) == 1:
            return data[0]
        minstr = data[0]
        mins = 99999
        for i in data:
            L = i.split(',')
            tmp = self.minusTime(L[4],L[-1])
            if mins > tmp:
                minstr = i
                mins = tmp
        return minstr

    def groupSplit(self,x):
        L = x.split(',')
        return (L[0]+L[1]+L[2],x)
    
    def getAllSection(self,data):
        data = data.map(self.groupSplit)
        data = data.groupByKey()
        data = data.flatMap(self.flatsplit)
        return data
        
