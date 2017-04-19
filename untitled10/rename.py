from pymongo import MongoClient
client=MongoClient('10.8.8.111',27017)
db=client.userValue
coll=db.cache
r=coll.aggregate([{'$match':{'201609':{'$exists':True}}}])
for i in r :
    coll.update_one({'user':i['user']},{'$rename':{'201609.value':'201609.pvip'}})
#collection.update_one({'user': i['user']}, {"$set": {test_time+'.value': d}})