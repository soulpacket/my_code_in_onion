from pymongo import MongoClient
import pandas as pd
# from bson.objectid import ObjectId
import datetime
client = MongoClient('10.8.8.22', 27017)
db = client.eventsV4
events = db.eventv4
START = datetime.datetime(2017, 1, 10)
END = datetime.datetime(2017, 1, 11)
appVersion = ['3.2.0', '3.2.1', '3.2.5']
q5 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': {'$in': ['clickChapterTask', 'clickChapterTheme']},
      'd_appVersion': {'$in': appVersion}, 'platform': 'app'}
agg = list(events.find(q5))
df = pd.DataFrame(agg)
df_1 = df.groupby('eventKey')

d1 = dict(list(df_1))['clickChapterTask']
c1 = list(d1['user'].unique())

d2 = dict(list(df_1))['clickChapterTheme']
c2 = list(d2['user'].unique())

print('clickChapterTask:', len(c1))
print('clickChapterTheme:', len(c2))
