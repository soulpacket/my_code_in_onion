from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
db = client.run
coll = db.blog
#a = {'user':1,'location':1,'date':'112010'}
# coll.insert(a)
#a = datetime.strptime('112010','%m%d%H')
file1 = open('train.csv')
line = file1.readline()
line_1=line.rstrip()
s = line_1.split(',')
coll.insert({'user':s[0], 'date':datetime.strptime(s[1],'%m%d%H'), 'location':s[2]})
#print(s)
num = 0
while line:
    line = file1.readline()
    line_1 = line.rstrip()
    s = line_1.split(',')
    a = {'user':s[0], 'date':datetime.strptime(s[1],'%m%d%H'), 'location':s[2]}
    coll.insert(a)
    num = num+1
    print(num)
#line = file1.readline()
#print(a)
