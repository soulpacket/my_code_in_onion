from pymongo import MongoClient
client=MongoClient('10.8.8.111',27017)
db=client.userValue
collection=db.cache
training_time='201609'
a=collection.aggregate([{'$match':{training_time:{'$exists':True}}}])
for i in a:
    print(i)
