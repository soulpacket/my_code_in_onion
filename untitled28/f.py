from pymongo import MongoClient
import pandas as pd
# from bson.objectid import ObjectId
import datetime
client = MongoClient('10.8.8.22', 27017)
db = client.eventsV4
events = db.xinyuEvents
START = datetime.datetime(2017, 1, 10)
END = datetime.datetime(2017, 1, 11)
appVersion = ['3.2.0', '3.2.1', '3.2.5']
q5 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': {'$in': ['clickChapterTask', 'clickChapterTheme']},
      'd_appVersion': {'$in': appVersion}}
num = 0
for i in events.find(q5):
    num += 1
    print(num)
