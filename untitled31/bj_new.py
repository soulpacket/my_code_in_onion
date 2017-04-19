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

appVersion_41 = ['3.1.0', '3.1.1']
appVersion_42 = ['3.2.0', '3.2.1', '3.2.5']
publisher = ['人教版', '北师大版', '华师大版', '苏科版']
client_1 = MongoClient('10.8.8.111', 27017)
db_4 = client_1.eventsV4
db_2 = client_1.cache
col_order = db_4.orderEvents
col_userAttr = db_2.userAttr
w1 = open('w_1.txt', 'w')
w2 = open('w_2.txt', 'w')
w3 = open('w_3.txt', 'w')
w4 = open('w_4.txt', 'w')
w5 = open('w_5.txt', 'w')
w6 = open('w_6.txt', 'w')
w7 = open('w_7.txt', 'w')
w8 = open('w_8.txt', 'w')
w_all = [w1, w2, w3, w4, w5, w6, w7, w8]
# w1 = open('w_1.txt', 'w')
# w1 = open('w_1.txt', 'w')
user_all = list()
f1 = col_userAttr.find({'appVersion': {'$in': appVersion_41}})
for o_1 in f1:
    for i_1 in o_1['daily']:
        if len(i_1) == 8:
            if START <= (datetime.strptime(i_1, '%Y%m%d') - timedelta(hours=8)) < MID:  # 记得改时间
                user_all.append(o_1['user'])
                break
print(len(user_all))
number = len(user_all)//8
print(number)
for i in range(8):
    if i != 7:
        for i_1 in user_all[i*number:(i+1)*number]:
            w_all[i].write(str(i_1))
            w_all[i].write(' ')
    else:
        for i_1 in user_all[i * number:]:
            w_all[i].write(str(i_1))
            w_all[i].write(' ')
w1.close()
w2.close()
w3.close()
w4.close()
w5.close()
w6.close()
w7.close()
w8.close()
# w9.close()
