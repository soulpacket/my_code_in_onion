from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import time
from multiprocessing import Pool
import numpy as np
START = datetime(2016, 11, 20, 16)
MID = datetime(2017, 1, 8, 16)
END = datetime(2017, 3, 7, 16)

appVersion_41 = ['3.1.0', '3.1.1', '3.1.5']
appVersion_42 = ['3.2.0', '3.2.1', '3.2.5']
publisher = ['人教版', '北师大版', '华师大版', '苏科版']
client_1 = MongoClient('10.8.8.111', 27017)
db_4 = client_1.eventsV4
db_2 = client_1.cache
db_1 = client_1.onions
col_daily = db_1.dailysignins
col_order = db_4.orderEvents
col_userAttr = db_2.userAttr
col_user = db_1.users
# user_all = list()


# f5求付费总人数
def pay(t1, t2):
    f5 = col_order.find({'serverTime': {'$gte': t1, '$lt': t2}, 'eventKey': 'paymentSuccess'})  # 记得改时间
    # df_1 = pd.DataFrame(list(f5))
    # user_order_object = list(df_1['user'].unique())
    user_order = set()
    for i in f5:
        user_order.add(str(i['user']))
    return user_order


# f1求活跃总人数
def active(t1, t2, app):
    user_all = list()
    f1 = col_userAttr.find({'appVersion': {'$in': app}})
    for o_1 in f1:
        for i_1 in o_1['daily']:
            if len(i_1) == 8:
                if t1 <= (datetime.strptime(i_1, '%Y%m%d') - timedelta(hours=8)) < t2:  # 记得改时间
                    user_all.append(o_1['user'])
                    break
    return set(user_all)


def dailysignin(t1, t2, app):
    user_daily_set = set()
    f7 = col_daily.find({'createTime': {'$gte': t1, '$lt': t2}, 'clientVersion': {'$in': app}})
    df_1 = pd.DataFrame(list(f7))
    user_daily_list = list(df_1['userId'].unique())
    for i in user_daily_list:
        user_daily_set.add(str(i))
    return user_daily_set


def publi(pub):
    count = 0
    user_pub = list()
    f2 = col_user.find({'publisher': {'$in': pub}})
    for i in f2:
        count += 1
        print(count)
        user_pub.append(str(i['_id']))
    return set(user_pub)


def main():
    vip = pay(START, MID)

    print("1")
    # act = active(START, MID, appVersion_41)
    act = dailysignin(START, END, appVersion_41)
    print("2")
    pub = publi(publisher)
    a = act & pub
    vip_a = a & vip
    b = act - pub
    vip_b = b & vip
    print("a组用户数:", len(a))
    print("b组用户数:", len(b))
    print("a组付费用户是:", len(vip_a))
    print("b组付费用户数是:", len(vip_b))
if __name__ == '__main__':
    main()
