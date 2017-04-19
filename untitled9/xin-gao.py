from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
START = datetime(2016,10,1)
END = datetime(2016,11,1)
client=MongoClient('10.8.8.111',27017)
db = client.userValue
collection=db.cache
db2=client.eventsV4
collection2=db2.orderEvents
a = collection.aggregate([{'$match':{'activateDate':{'$gte':START,'$lt':END}}}]).count()
#a= collection.find().count()
#print(a)
num_xin=0
num_gao=0
num_vip=0
list_1=[]
list_2=[]
for i in a:
    #print(i)
    num_xin=num_xin+1
    if(i['201610']['value']>=0.5):
        num_gao=num_gao+1
        if(i['201610']['vip']==True):
            num_vip=num_vip+1
            try:
                list_1.append(ObjectId(i['user']))
            except:
                pass
print(num_xin)
print(num_gao)
print(num_vip)
b=collection2.aggregate([{'$match':{'serverTime':{'$gte':START,'$lt':END},'eventKey':'paymentSuccess',
                                    'user':{'$in':list_1}}}])
for i in b:
    if('orderInfo' in i):
        if(i['orderInfo']['good']['amount']>8):
            list_2.append(i['user'])
print(len(set(list_2)))


