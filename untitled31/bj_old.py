from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import time
from multiprocessing.dummy import Pool
import numpy as np
START = datetime(2016, 11, 20, 16)
MID = datetime(2017, 1, 8, 16)
END = datetime(2017, 3, 7, 16)

appVersion_41 = ['3.1.0', '3.1.1']
appVersion_42 = ['3.2.0', '3.2.1', '3.2.5']
publisher = ['人教版', '北师大版', '华师大版', '苏科版']
client_1 = MongoClient('10.8.8.111', 27017)
db_4 = client_1.eventsV4
db_2 = client_1.cache
col_order = db_4.orderEvents
col_userAttr = db_2.userAttr
user_all = list()
# f5求付费总人数
f5 = col_order.find({'serverTime': {'$gte': START, '$lt': MID}, 'eventKey': 'paymentSuccess'})  # 记得改时间
df_1 = pd.DataFrame(list(f5))
user_order_object = list(df_1['user'].unique())
user_order = set()
for o in user_order_object:
    user_order.add(str(o))
# f1求活跃总人数
f1 = col_userAttr.find({'appVersion': {'$in': appVersion_41}})
for o_1 in f1:
    for i_1 in o_1['daily']:
        if len(i_1) == 8:
            if START <= (datetime.strptime(i_1, '%Y%m%d') - timedelta(hours=8)) < MID:  # 记得改时间
                user_all.append(o_1['user'])
                break
number = len(user_all)//8
print(number)
user_all_segmentation = [
                         user_all[0:number],
                         user_all[number:(2*number)],
                         user_all[(2*number):(3*number)],
                         user_all[(3 * number):(4 * number)],
                         user_all[(4 * number):(5 * number)],
                         user_all[(5 * number):(6 * number)],
                         user_all[(6 * number):(7 * number)],
                         user_all[(7 * number):]
                         ]
# user_all_segmentation = [user_all[0:number]]
del user_all


def active(user_active):
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
    f2 = col_event.find({'serverTime': {'$gte': START, '$lt': MID}, 'user': {'$in': user_active},
                         'eventKey': 'finishVideo'})
    for i in f2:
        if 'user' in i:
            user_video.add(i['user'])
    print('视频完成')
    # f3所有时间段完成3个知识点
    f3 = col_event.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': MID}, 'eventKey': 'enterTopicFinish',
                                          'user': {'$in': user_active}}},
                             {'$project': {'user': 1, 'topicId': 1}},
                             {'$group': {'_id': '$user', 'topic': {'$addToSet': '$topicId'}}}], allowDiskUse=True)
    for i in f3:
        if len(set(i['topic'])) >= 3:
            user_topic.add(i['_id'])
    print("知识点完成")
    # f4课本版本
    f4 = col_user.find({'_id': {'$in': user_active_object}, 'publisher': {'$in': publisher}})
    df = pd.DataFrame(list(f4))
    user_publisher_list = list(df['_id'].unique())
    for i in user_publisher_list:
        user_publisher.add(str(i))
    print("users表完成")
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
    print('start')
    time_1 = time.time()
    pool = Pool(20)
    result = list()
    count = np.array([0 * 12])
    for i in user_all_segmentation:
        # time_arg = MID + timedelta(days=(func * 7))
        result.append(pool.apply_async(active, (i, )))
    print(len(result))
    for i in result:
        print(i.get())
        count = count + np.array(i.get())
    print('总的:', list(count))
    time_2 = time.time()
    print(time_2 - time_1)


if __name__ == '__main__':
    main()
