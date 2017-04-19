from pymongo import MongoClient
import pandas as pd
import time
from datetime import datetime, timedelta
from multiprocessing import Pool
from bson.objectid import ObjectId
START = datetime(2017, 2, 12, 16)
END = datetime(2017, 3, 12, 16)
client = MongoClient('10.8.8.111', 27017)
db_1 = client.eventsV4
col_event = db_1.eventV4
col_order = db_1.orderEvents
set_a = set()
set_b = set()

a = col_event.aggregate([{'$match': {'serverTime':{'$gte': START, '$lt': END},
                                     '$or': [{'eventKey': 'finishVideo', 'videoType': 'payment'},
                                             {'eventKey': 'clickPVFButton', 'button': 'toBuy'}]}},
                         {'$project': {'eventKey': 1, 'user': 1}}])
for i in a:
    if 'eventKey' in i:
        if i['eventKey'] == 'finishVideo':
            set_a.add(i['user'])
        elif i['eventKey'] == 'clickPVFButton':
            set_b.add(i['user'])
b = col_order.find({'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'paymentSuccess'})
df = pd.DataFrame(list(b))
list_vip = list(df['user'].unique())
set_vip = set()
for i in list_vip:
    if ObjectId.is_valid(i):
        set_vip.add(str(i))
print('a组用户数是:', len(set_a))
print('b组用户数是:', len(set_b))
print('a组付费用户数是:', len(set_a & set_vip))
print('b组付费用户数是:', len(set_b & set_vip))
