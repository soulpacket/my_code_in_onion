from pymongo import MongoClient
# from bson.objectid import ObjectId
import datetime
client = MongoClient('10.8.8.22', 27017)
db = client.eventsV4
coll = db.eventV4
START = datetime.datetime(2017, 1, 9)
END = datetime.datetime(2017, 1, 10)
a = coll.find({'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterList', 'd_appVersion': '3.2.0',
               'isTask': 'true'})
num = 0
for i in a:
    num += 1
    print(num)
# print(i)
