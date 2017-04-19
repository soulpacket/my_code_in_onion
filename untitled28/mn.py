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
      'd_appVersion': {'$in': appVersion}, 'platform': 'app'}
a = events.aggregate([{'$match': q5},
                      {'$project': {'eventKey': 1, 'user': 1}},
                      {'$group': {'_id': '$eventKey', 'uv': {'$addToSet': '$user'}}}])
for i in a:
    if i['_id'] == 'clickChapterTask':
        print('clickChapterTask :', len(i['uv']))
    elif i['_id'] == 'clickChapterTheme':
        print('clickChapterTheme :', len(i['uv']))
