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
col_userAttr = db_2.userAttr
# user_all = list()
# f5求付费总人数


def pay(time_1, time_2):
    f5 = col_order.find({'serverTime': {'$gte': time_1, '$lt': time_2}, 'eventKey': 'paymentSuccess'})  # 记得改时间
    df_1 = pd.DataFrame(list(f5))
    user_order_object = list(df_1['user'].unique())
    user_order = set()
    for o in user_order_object:
        user_order.add(str(o))
    return user_order


def active(user_active, user_order, ti_1, ti_2):
    client = MongoClient('10.8.8.111', 27017)
    db_1 = client.eventsV4
    db_3 = client.onions
    col_event = db_1.eventV4
    # col_order = db_1.orderEvents
    col_user = db_3.users
    # user_active = list()
    user_active_object = list()  # ObjectId(活跃用户)
    user_video = set()
    user_topic = set()
    user_publisher = set()
    # f1时间段内活跃

    for i in user_active:
        if ObjectId.is_valid(i):
            user_active_object.append(ObjectId(i))
    # f2所有时间段至少完成一个视频
    # f2 = col_event.aggregate([{'$match': {'serverTime': {'$gte': ti_1, '$lt': ti_2}, 'user': {'$in': user_active},
    #                           'eventKey': 'finishVideo'}}])
    # for i in f2:
    #     if 'user' in i:
    #         user_video.add(i['user'])
    f2 = col_event.find({'serverTime': {'$gte': ti_1, '$lt': ti_2}, 'eventKey': 'finishVideo',
                        'user': {'$in': user_active}
                         })
    a = pd.DataFrame(list(f2))
    user_video = set(list(a['user'].unique()))
    print('完成视频')
    # f3所有时间段完成3个知识点
    f3 = col_event.aggregate([{'$match': {'serverTime': {'$gte': ti_1, '$lt': ti_2}, 'eventKey': 'enterTopicFinish',
                                          'user': {'$in': user_active}}},
                             {'$project': {'user': 1, 'topicId': 1}},
                             {'$group': {'_id': '$user', 'topic': {'$addToSet': '$topicId'}}}], allowDiskUse=True)
    for i in f3:
        if len(set(i['topic'])) >= 3:
            user_topic.add(i['_id'])
    print('完成知识点')
    # f4课本版本
    f4 = col_user.find({'_id': {'$in': user_active_object}, 'publisher': {'$in': publisher}})
    df = pd.DataFrame(list(f4))
    user_publisher_list = list(df['_id'].unique())
    for i in user_publisher_list:
        user_publisher.add(str(i))
    print('完成user表')
    # f5付费用户 user_order
    user_a = len(set(user_active) & user_publisher)
    user_b = len(set(user_active) & user_video & user_publisher)
    user_c = len(set(user_active) & user_topic & user_publisher)
    user_d = len(set(user_active) - user_publisher)
    user_e = len((set(user_active) & user_video) - user_publisher)
    user_f = len((set(user_active) & user_topic) - user_publisher)
    user_a_vip = len(set(user_active) & user_publisher & user_order)
    user_b_vip = len(set(user_active) & user_video & user_publisher & user_order)
    user_c_vip = len(set(user_active) & user_topic & user_publisher & user_order)
    user_d_vip = len((set(user_active) - user_publisher) & user_order)
    user_e_vip = len(((set(user_active) & user_video) - user_publisher) & user_order)
    user_f_vip = len(((set(user_active) & user_topic) - user_publisher) & user_order)
    return [user_a, user_b, user_c, user_d, user_e, user_f, user_a_vip, user_b_vip, user_c_vip, user_d_vip, user_e_vip,
            user_f_vip]


def main():
    user_pay = pay(START, MID)
    print(len(user_pay))
    w1 = open('w_1.txt', 'r')
    w1_read = w1.read()
    user_seg = w1_read.split(' ')
    print(len(user_seg))
    print(active(user_seg, user_pay, START, MID))
    t2 = time.time()
    print(t2-t1)

if __name__ == '__main__':
    main()
