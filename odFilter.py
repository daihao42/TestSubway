'''
    input: sc.textFile('')
    output:"cardId,starttime,startStation,endtime,endStation,timediff"
'''
__author__ = 'dai'

import datetime


class ODFilter():
    def __init__(self,nn):
        self.NameNo = nn

    def formatTime(self,t):
        t = t.replace('T',' ')
	return t[:-5]

    def delTime(self,t1,t2):
        t1 = self.formatTime(t1)
	t2 = self.formatTime(t2)
	t1 = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S')
        t2 = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S')
	return (t2-t1).seconds

    def ssplit(self,x):
	L = x.split(',')
	return (L[1],L[4]+','+L[1]+','+L[3]+','+L[6])

    def ODRuler(self,x,y):
	L1 = x.split(',')
	L2 = y.split(',')
	if not(L1[2] == '21' and L2[2] == '22'):
	    return None
	if int(L1[0][11:13]) - int(L2[0][11:13])> 3:
	    return None
	else :
	    return L1[1]+','+self.formatTime(L1[0])+','+self.NameNo.Name2No(L1[3])+','+self.formatTime(L2[0])+','+self.NameNo.Name2No(L2[3])+','+str(self.delTime(L1[0],L2[0]))

    def MakeOD(self,para):
	i = 0
	res = []
	dd = para[1].data
	dd.sort()
	while(i < len(dd)-1):
	    od = self.ODRuler(dd[i],dd[i+1])
	    if od is not None:
		res.append(od)
		i = i+2
	    else :
		i = i+1
	return res

    def getOD(self,data):
	data = data.map(self.ssplit)
	data = data.groupByKey()
	data = data.flatMap(self.MakeOD)
	return data


