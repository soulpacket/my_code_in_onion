from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import time
from multiprocessing import Pool
import numpy as np
START = datetime(2016, 11, 20, 16)
MID = datetime(2017, 1, 8, 16)
END = datetime(2017, 2, 5, 16)
t1 = time.time()
appVersion_41 = ['3.1.0', '3.1.1']
appVersion_42 = ['3.2.0', '3.2.1', '3.2.5']
publisher = ['人教版', '北师大版', '华师大版', '苏科版']
client_1 = MongoClient('10.8.8.111', 27017)
db_4 = client_1.eventsV4
db_2 = client_1.cache
col_order = db_4.orderEvents
col_event = db_4.eventV4
col_userAttr = db_2.userAttr
num = 0
user = set()
w1 = open('w_video.txt', 'w')
f1 = col_event.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': MID}, 'eventKey': 'finishVideo'}}])
for i in f1:
    num += 1
    print(num)
    if 'user' in i:
        user.add(i['user'])
for i in user:
    w1.write(str(i))
    w1.write(' ')
w1.close()
