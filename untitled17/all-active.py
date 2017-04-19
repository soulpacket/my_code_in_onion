from pymongo import MongoClient
from datetime import datetime
client = MongoClient('10.8.8.111',27017)
db_1 = client.onions
coll_1 = db_1.dailysignins
coll_2 = db_1.rooms
START = datetime(2016,9,12)
rooms = []
b = coll_2.aggregate([])
for i in b:
    rooms.append(i['_id'])
a = coll_1.aggregate([{'$match':{'createTime':{'$gte':START},'rooms.0':{'$exists':True}}}])
#for i in a:

num = 0
user = set()
for i in a:
    user.add(i['userId'])
#     for i_1 in i['rooms']:
#         if i_1 in rooms:
#             num+=1
#             break
print(len(user))#有班级的活跃用户
#print(num)#有老师的有班级的用户
# userList=[]
# set_2=set()
# c = coll_1.aggregate([{'$match':{'userId':{'$in':userList},'createTime':{'$gte':START},'rooms.0':{'$exists':True}}}])
# for i in c:



