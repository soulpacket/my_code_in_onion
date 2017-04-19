from pymongo import MongoClient
import pandas as pd
import time
from datetime import datetime, timedelta
from multiprocessing import Pool
from bson.objectid import ObjectId
LIMIT = datetime(2017, 1, 8, 16)


def process(t1):
    client = MongoClient('10.8.8.111', 27017)
    db_1 = client.eventsV4
    db_2 = client.cache
    col_event = db_1.eventV4
    col_order = db_1.orderEvents
    col_user = db_2.userAttr
    user_register = set()
    user_video = set()
    user_vip = set()
    a = col_user.find({'activateDate': {'$gte': t1, '$lt': t1+timedelta(days=1)}})
    for i in a:
        if 'user' in i:
            user_register.add(i['user'])
        # time_register.append(i['activateDate'])
    b = col_event.aggregate([{'$match': {'serverTime': {'$gte': t1, '$lt': (t1+timedelta(days=7))},
                                         'eventKey': 'clickShowVideo'}}])
    for i in b:
        if 'user' in i:
            user_video.add(i['user'])
    # print(len(user_video))
    user_a = user_register & user_video
    user_b = user_register - user_video
    c = col_order.aggregate([{'$match': {'serverTime': {'$gte': t1, '$lt': (t1+timedelta(days=7))},
                                         'eventKey': 'paymentSuccess'}}])
    for i in c:
        if 'user' in i:
            if ObjectId.is_valid(i['user']):
                user_vip.add(str(i['user']))
    a_vip = user_a & user_vip
    b_vip = user_b & user_vip
    # print(len(user_a))
    # print(len(user_b))
    # print(len(a_vip))
    # print(len(b_vip))
    return [len(user_a), len(user_b), len(a_vip), len(b_vip)]

# process(LIMIT)


def main():
    print('start')
    time_1 = time.time()
    uv_a = 0
    uv_b = 0
    uv_c = 0
    uv_d = 0
    # func_list = [process(LIMIT+timedelta(days=i)) for i in range(22)]
    pool = Pool(6)
    result = list()
    result_1 = list()
    for func in range(22):
        time_arg = LIMIT + timedelta(days=func)
        result.append(pool.apply_async(process, (time_arg, )))
    print(len(result))
    for i in result:
        result_1.append(i.get())
        print(i.get())
    for i in range(len(result_1)):
        uv_a += result_1[i][0]
    for i in range(len(result_1)):
        uv_b += result_1[i][1]
    for i in range(len(result_1)):
        uv_c += result_1[i][2]
    for i in range(len(result_1)):
        uv_d += result_1[i][3]
    print('a组用户数是:', uv_a)
    print('b组用户是:', uv_b)
    print('a组付费用户是:', uv_c)
    print('b组付费用户数是:', uv_d)
    print('a组付费率是:', uv_c/uv_a)
    print('b组付费率是:', uv_d/uv_b)
    time_2 = time.time()
    print(time_1 - time_2)
if __name__ == '__main__':
    main()

