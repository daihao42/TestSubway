'''
    input: sc.textFile('')
    output:"OD,starttime,endtime"
'''
def ssplit(x):
    L = x.split(',')
    return (L[1],L[4]+','+L[1]+','+L[3]+','+L[6])

def ODRuler(x,y):
    L1 = x.split(',')
    L2 = y.split(',')
    if not(L1[2] == '21' and L2[2] == '22'):
        return None
    if int(L1[0][11:13]) - int(L2[0][11:13])> 3:
        return None
    else :
        return L1[1]+','+L1[0]+','+L1[3]+','+L2[0]+','+L2[3]

def MakeOD(para):
    i = 0
    res = []
    dd = para[1].data
    dd.sort()
    while(i < len(dd)-1):
        od = ODRuler(dd[i],dd[i+1])
        if od is not None:
            res.append(od)
            i = i+2
        else :
            i = i+1
    return res

def ODFilter(data):
    data = data.map(ssplit)
    data = data.groupByKey()
    data = data.flatMap(MakeOD)
    return data


