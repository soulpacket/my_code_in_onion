import math
from pymongo import MongoClient
client=MongoClient('10.8.8.111',27017)
db=client.userValue
#collection=db.cache
collection = db['cache']
time='201610'
a=collection.aggregate([{'$match':{time:{'$exists':True}}}])
num=0
for i in a:
    if(i['201610']['vip']==True):
        num=num+1
print(num)