from pymongo import MongoClient
import pandas as pd
# from bson.objectid import ObjectId
import datetime
client = MongoClient('10.8.8.22', 27017)
db = client.eventsV4
events = db.eventV4
START = datetime.datetime(2017, 1, 10)
END = datetime.datetime(2017, 1, 11)
appVersion = ['3.2.0', '3.2.1', '3.2.5']
# a = events.find({'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterList',
#                'd_appVersion': {'$in': appVersion}, 'isTask': 'true'})
q1 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterList',
      'd_appVersion': {'$in': appVersion}, 'isTask': 'true'}
q2 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterTaskNotice',
      'platform': 'app'}
num = 0
set_1 = set()
set_2 = set()
set_3 = set()
for i in events.find(q1):
    set_1.add(i['user'])
print(len(set_1))
for i in events.find(q2):
    num += 1
    print(num)
    set_1.add(i['user'])
q5 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickChapterTask',
      'd_appVersion': {'$in': appVersion}}
q6 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickChapterTheme',
      'd_appVersion': {'$in': appVersion}}
# df = pd.DataFrame(list(events.find(q5)))
# df_1 = df.groupby('eventKey')
# print(len(df_1['clickChapterTask']))
for i in events.find(q5):
    set_2.add(i['user'])
print(len(set_2))
for i in events.find(q6):
    set_3.add(i['user'])
print(len(set_3))
