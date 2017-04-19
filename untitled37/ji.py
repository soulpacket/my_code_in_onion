from pymongo import MongoClient
from datetime import datetime
client_1 = MongoClient('10.8.8.111', 27017)
db_1 = client_1.xinyu
col_distinct = db_1.user_distinct
client = MongoClient('10.8.8.111', 27017)
db = client.eventsV4
col_bak = db.eventV4_bak
col_event = db.eventV4
app_43 = ['3.3.'+str(i) for i in range(10)]
f1 = col_bak.aggregate([{'$match': {'serverTime': {'$gte': datetime(2017, 3, 12, 16)},
                                    'eventKey': 'enterVideo',
                                    'd_appVersion': {'$in': app_43}}},
                        {'$project': {'user': 1, 'videoId': 1}},
                        {'$group': {'_id': '$user', 'video': {'$addToSet': '$videoId'}}}])
for i in f1:

