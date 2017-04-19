from pymongo import MongoClient
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111', 27017)
data = client.cache
db = client.eventsV4
coll = data.userAttr
coll_2 = db.orderEvents
r1 = ['hunan', 'zhejiang', 'shandong', 'shanghai', '湖南', '浙江', '山东', '上海']
agg_1 = coll.find({'$or': [{'location': {'$regex': 'hunan', '$options': '$i'}},
                           {'location': {'$regex': 'zhejing', '$options': '$i'}},
                           {'location': {'$regex': 'shanghai', '$options': '$i'}},
                           {'location': {'$regex': 'shandong', '$options': '$i'}},
                           {'location': {'$regex': '湖南', '$options': '$i'}},
                           {'location': {'$regex': '浙江', '$options': '$i'}},
                           {'location': {'$regex': '山东', '$options': '$i'}},
                           {'location': {'$regex': '上海', '$options': '$i'}}]
                   })
hn = list()
zj = list()
sd = list()
sh = list()
for i in agg_1:
    if len(i['location']) > 4:
        if i['location'][4] == 'n':
            hn.append(ObjectId(i['user']))
        if i['location'][4] == 'i':
            zj.append(ObjectId(i['user']))
        if i['location'][4] == 'd':
            sd.append(ObjectId(i['user']))
        if i['location'][4] == 'g':
            sh.append(ObjectId(i['user']))
    if i['location'][0] == '湖':
        hn.append(ObjectId(i['user']))
    if i['location'][0] == '浙':
        zj.append(ObjectId(i['user']))
    if i['location'][0] == '山':
        sd.append(ObjectId(i['user']))
    if i['location'][0] == '上':
        sh.append(ObjectId(i['user']))
print(len(hn))
print(len(zj))
print(len(sd))
print(len(sh))
agg_2 = coll_2.aggregate([
    {'$match': {'eventKey': 'paymentSuccess'}}
    # {'$project': {'$user': 1}}
                          ])
a = list()
for i in agg_2:
    a.append(i['user'])
print(len(set(a)))
print(len(set(a) & set(hn)))
print(len(set(a) & set(zj)))
print(len(set(a) & set(sd)))
print(len(set(a) & set(sh)))
