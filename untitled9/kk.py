from pymongo import MongoClient
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
START = datetime(2016,10,1)
END = datetime(2016,11,1)
client=MongoClient('10.8.8.111',27017)
db = client.cache
db_2 = client.userValue
db_3=client.eventsV4
collection = db.userAttr
collection_2=db_2.cache
collection_3=db_3.orderEvents
a = collection.aggregate([{'$project':{'user':1,'daily':1}}])
user=[]
count=0
for i in a:
    for i_1 in i['daily']:
        if(datetime.strptime(i_1,'%Y%m%d')>=datetime(2016,10,1) and datetime.strptime(i_1,'%Y%m%d')<datetime(2016,11,1)):
            #print(i)
            user.append(i['user'])
            # count = count + 1
            # print(count)
            break

print('活跃用户一共有多少',end='')
print(len(user))
user1=user[0:200000]
user2=user[200000:400000]
user3=user[400000:600000]
user4=user[600000:800000]
user5=user[800000:]
#db_2 = client.userValue
b1 = collection_2.aggregate([{'$match':{'user':{'$in':user1},'201610.value':{'$gte':0.5}}}])
b2 = collection_2.aggregate([{'$match':{'user':{'$in':user2},'201610.value':{'$gte':0.5}}}])
b3 = collection_2.aggregate([{'$match':{'user':{'$in':user3},'201610.value':{'$gte':0.5}}}])
b4 = collection_2.aggregate([{'$match':{'user':{'$in':user4},'201610.value':{'$gte':0.5}}}])
b5 = collection_2.aggregate([{'$match':{'user':{'$in':user5},'201610.value':{'$gte':0.5}}}])
num1=0
num_v1=0
num2=0
num_v2=0
num3=0
num_v3=0
num4=0
num_v4=0
num5=0
num_v5=0
list_1=[]
for i in b1:
    num1=num1+1
    if(i['201610']['vip']==True):
        num_v1=num_v1+1
        try:
            list_1.append(ObjectId(i['user']))
        except:
            pass
print(num1,num_v1)
for i in b2:
    num2=num2+1
    if(i['201610']['vip']==True):
        num_v2=num_v2+1
        try:
            list_1.append(ObjectId(i['user']))
        except:
            pass
print(num2,num_v2)
for i in b3:
    num3=num3+1
    if(i['201610']['vip']==True):
        num_v3=num_v3+1
        try:
            list_1.append(ObjectId(i['user']))
        except:
            pass
print(num3,num_v3)
for i in b4:
    num4=num4+1
    if(i['201610']['vip']==True):
        num_v4=num_v4+1
        try:
            list_1.append(ObjectId(i['user']))
        except:
            pass
print(num4,num_v4)
for i in b5:
    num5=num5+1
    if(i['201610']['vip']==True):
        num_v5=num_v5+1
        try:
            list_1.append(ObjectId(i['user']))
        except:
            pass
print(num5,num_v5)
print('总的高倩和VIP数是:')
print(num1+num2+num3+num4+num5)
print(num_v1+num_v2+num_v3+num_v4+num_v5)
#print(len(set(list_1)))
#print(len(list_1))
list_2=[]
b = collection_3.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'paymentSuccess',
                                       'user': {'$in': list_1}}}])
for i in b:
    if ('orderInfo' in i):
        if (i['orderInfo']['good']['amount'] > 8):
            list_2.append(i['user'])
print(len(set(list_2)))






