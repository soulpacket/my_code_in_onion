from pymongo import MongoClient
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111', 27017)
data = client.onions
db = client.eventsV4
coll = data.users

coll_2 = db.orderEvents

qi = list()
ba = list()
jiu = list()
num = 0
agg_2 = coll_2.aggregate([{'$match': {'eventKey': 'paymentSuccess'}},
                          {'$group': {'_id': '$user', 'money': {'$sum': '$actualOrderAmount'}}}])
a = dict()
b = list()
for i in agg_2:

    a[i['_id']] = i['money']
    b.append(i['_id'])

agg_1 = coll.find({'_id': {'$in': b}})
for i in agg_1:
    num += 1
    print(num)
    # if i['semester'][0] == '七':
    #     qi.append(i['_id'])
    # elif i['semester'][0] == '八':
    #     ba.append(i['_id'])
    if i['semester'][0] == '九':
        jiu.append(i['_id'])
# c = set(qi) & set(b)
# d = set(ba) & set(b)
e = set(jiu) & set(b)
# print(len(c))
# print(len(d))
print(len(e))
count_1 = 0
# for i in c:
#     count_1 = a[i] + count_1
# count_2 = 0
# for i in d:
#     count_2 = a[i] + count_2
count_3 = 0
for i in e:
    count_3 = a[i] + count_3
# print(count_1)
# print(count_2)
print(count_3)


