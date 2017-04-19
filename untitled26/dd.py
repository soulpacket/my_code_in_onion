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
for i in events.find(q1):
    set_1.add(i['user'])
for i in events.find(q2):
    num += 1
    print(num)
    set_1.add(i['user'])
# df = pd.DataFrame(list(events.find(q1)))
# df_5 = pd.DataFrame(list(events.find(q2)))
# set_7 = set(list(df['user'].unique()))
# set_2 = set(list(df_5['user'].unique()))
# set_1 = set_7 | set_2

