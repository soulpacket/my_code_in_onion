from pymongo import MongoClient
from datetime import datetime
START = datetime(2016,10,1)
END = datetime(2016,11,1)
client=MongoClient('10.8.8.111',27017)
db = client.userValue
collection=db.cache
db_2 = client.cache
collection_2 = db_2.userAttr
a = collection_2.aggregate([{'$match':{'activateDate':{'$gte':START,'$lt':END}}}])
user=[]
for i in a:
    #print(i)
    user.append(i['user'])
print(len(user))
user_1=user[0:200000]
user_2=user[200000:]
num1=0
num2=0
b1 = collection.aggregate([{'$match':{'user':{'$in':user_1}}}])
b2 = collection.aggregate([{'$match':{'user':{'$in':user_2}}}])
for i in b1:
    num1=num1+1
for i in b2:
    num2=num2+1
print(num1,num2)
print(num1+num2)