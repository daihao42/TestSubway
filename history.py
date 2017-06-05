import TimeTable as tt
import TimeTable as tt
import timeTable as tt
timetable = tt.TimeTable(sc,spark)
work,week = timetable.getSql()
work.show()
work.show()
timetable = tt.TimeTable(sc,spark)
work,week = timetable.getSql()
work.show()
work.show()
work.where(work.starttime == '06:30:00')
work.where(work.starttime == '06:30:00').show()
ll
import ODDataFrame as of
od
odd = of.ODData(sc,spark)
od = odd.getSql()
od.show()
od.count()
import allPath as ap
al = ap.AllPath(sc,spark)
path = al.getSql()
res = od.join(path,[od.start == path.start,od.end == path.end])
res.show()
res[res.path]
res[res.paths]
res[path.path]
res.select(path.path)
res.select(path.path).show()
res.select(path.path).collect()
res.select(path.path).collect()[0]
res.select(path.path).collect()[0][path]
res.select(path.path).collect()[0]['path']
for i in res.select(path.path).where(od.start == '1268012000').where(od.end == '1268013000'):
    print i
for i in res.select(path.path).where(od.start == '1268012000').where(od.end == '1268013000').collect():
    print i
for i in res.select(path.path).where(od.start == '1268012000').where(od.end == '1268013000').collect():
    for j in i['path']:
        print j
for i in res.select(path.path).where(od.start == '1268012000').where(od.end == '1268013000').collect():
    for j in eval(i['path']):
        print j
res
for i in res.select(od.cardid,path.path).where(od.start == '1268012000').where(od.end == '1268013000').collect():
    for j in eval(i['path']):
        print j
for i in res.select(od.cardid,path.path).where(od.start == '1268012000').where(od.end == '1268013000').collect():
    print i['cardid']
    for j in eval(i['path']):
        print j
odf = od.toDF()
odf
od
od
od
res
res.rdd
ress = res.rdd
ress.saveAsTextFile('ress')
for i in res.select(od.cardid,path.path).collect():
    print i['cardid']
    for j in eval(i['path']):
        print j
res.select(od.cardid,path.path).collect()
res.select(od.cardid,path.path).rdd
ress
ress.map(lambda x:print x)
ress.map(lambda x:x)
ress = ress.map(lambda x:x)
ress.collect()
ress.map(lambda x:print x['path'])
ress.map(lambda x:x['path'][1])
res1 = ress.map(lambda x:x['path'][1])
res1.collect()
res1 = ress.map(lambda x:eval(x['path']))
res1.collect()
work
import subwayFlowDataFrame as sff
reload(sff)
sf =sff.SubwayFlow(sc,spark)
reload(sff)
sf =sff.SubwayFlow(sc,spark)
sf = sf.getSql()
sf.show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',sf.cardid == '123'],'right').show()
reload(sff)
sf =sff.SubwayFlow(sc,spark)
sf = sf.getSql()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',sf.cardid == '123'],'right').show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',sf.cardid = '123'],'right').show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',cardid == '123'],'right').show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000','cardid' == '123'],'right').show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',sf.cardid = '123'],'right').show()
sf.join(work,[work.start == '1268012000',work.end == '1268013000',sf.cardid == '123'],'right').show()
od.join(work,[work.start == '1268012000',work.end == '1268013000',work.starttime == od.starttime],'right').show()
od.show()
od.join(work,[work.start == '1268012000',work.end == '1268013000',work.starttime == od.starttime[-9:]]).show()
od.join(work,[work.start == '1268012000',work.end == '1268013000',work.starttime == od.starttime]).show()
reload(od)
odd
import ODDataFrame as of
reload(of)
odd = of.ODData(sc,spark)
od = odd.getSql()
od.show()
od.join(work,[work.start == '1268012000',work.end == '1268013000',work.starttime == od.starttime]).show()
reload(of)
odd = of.ODData(sc,spark)
od = odd.getSql()
od.show()
od.join(work,[work.start == '1268012000',work.end == '1268013000',work.starttime == od.starttime]).show()
od.join(work,[work.start == od.start,work.end == od.end,work.starttime == od.starttime]).show()
%history
%history -g
%history -f history.py
