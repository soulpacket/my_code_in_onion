from pymongo import MongoClient
from datetime import datetime
client = MongoClient('10.8.8.111',27017)
db_1 = client.eventsV4
coll_1 = db_1.orders
db_2 = client.onions
coll_2 = db_2.dailysignins
coll_3 = db_2.rooms
START = datetime(2016,9,12)
user_1 = set()
user_2 = set()
user_3 = set()
a = coll_1.aggregate([{'$match':{'createdAt':{'$gte':START},'paid':True}}])
for i in a:
    user_1.add(i['userId'])
print(len(user_1))#全量付费用户
# b = coll_1.aggregate([{'$match':{'createdAt':{'$gte':START},'paid':True}},
#                       {'$group':{'_id':'$good','count':{'$sum':1}}}
#                       ])
# for i in b:
#     print(i)
userList=list(user_1)
rooms = []
c = coll_3.aggregate([])
for i in c:
    rooms.append(i['_id'])
b = coll_2.aggregate([{'$match':{'userId':{'$in':userList},'createTime':{'$gte':START},'rooms.0':{'$exists':True}}}])
for i in b:
    user_2.add(i['userId'])
    for i_1 in i['rooms']:
        if i_1 in rooms:
            user_3.add(i['userId'])
            break
print(len(user_2))
#user_2list=list(user_2)
set_4 = user_2-user_3
list_5 = list(set_4)
d = coll_1.aggregate([{'$match':{'createdAt':{'$gte':START},'paid':True,'userId':{'$in':list_5}}},
                      {'$group':{'_id':'$good','count':{'$sum':1}}}
                      ])
for i in d:
    print(i)

