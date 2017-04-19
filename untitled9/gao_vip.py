from pymongo import MongoClient
from datetime import datetime
START = datetime(2016,10,1)
END = datetime(2016,11,1)
client=MongoClient('10.8.8.111',27017)
db = client.userValue
collection=db.cache
a = collection.aggregate([{'$match':{'201610':{'$exists':True}}}])
num_gao=0
num_vip=0
for i in a:
    num_gao=num_gao+1
    if(i['201610']['vip']==True):
        num_vip=num_vip+1
print(num_gao)
print(num_vip)