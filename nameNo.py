'''
    change station's name to number
    by nameno.csv
'''
__author__ = 'dai'

class NameNo():
    def __init__(self,sc):
        self.nameno = {}
	data = sc.textFile('SubwayFlowConf/nameno.csv')
	data = data.collect()
	for i in data:
	    L = i.split(',')
	    self.nameno[L[0]] = L[1]
    
    def Name2No(self,name):
        return self.nameno[name]

