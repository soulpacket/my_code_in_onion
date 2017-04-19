#normScore
from pymongo import MongoClient
import math
client=MongoClient('10.8.8.111',27017)
db=client.userValue
coll=db.cache
time = '201609'
r=coll.aggregate([{'$match':{time:{'$exists':True}}}])
count=0
for i in r:
    a=math.atan(i[time]['score'])*3.2/math.pi
    if a<=1:
        coll.update_one({'user':i['user']},{"$set": {time+'.normScore': a}})
    elif a>1:
        coll.update_one({'user': i['user']}, {"$set": {time + '.normScore': 1}})
    count=count+1
    print(count)
