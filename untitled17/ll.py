from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111',27017)
db_1 = client.onions
coll_1 = db_1.dailysignins
START = datetime(2016,9,12)
a = coll_1.aggregate([{'$match':{'createTime':{'$gte':START}}}])
num = 0
user = set()
for i in a:
    #user.add(str(i['userId']))
    print(i)
print(len(user))