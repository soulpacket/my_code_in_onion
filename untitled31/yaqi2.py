from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111', 27017)
db_1 = client.eventsV4
db_2 = client.cache
col_event = db_1.eventV4
col_order = db_1.orderEvents
col_user = db_2.userAttr
LIMIT = datetime(2017, 1, 8, 16)
START = datetime(2017, 1, 15, 16)
END = datetime(2017, 1, 22, 16)


def vip(string, t1, t2=0):
    user = set()
    if string == 'before':
        a = col_order.aggregate([{'$match': {'serverTime': {'$lt': t1}, 'eventKey': 'paymentSuccess'}}])
        for i in a:
            if ObjectId.is_valid(i['user']):
                user.add(str(i['user']))
    elif string == 'after':
        a = col_order.aggregate([{'$match': {'serverTime': {'$gte': t1}, 'eventKey': 'paymentSuccess'}}])
        for i in a:
            if ObjectId.is_valid(i['user']):
                user.add(str(i['user']))
    elif string == 'between':
        a = col_order.aggregate([{'$match': {'serverTime': {'$gte': t1, '$lt': t2}, 'eventKey': 'paymentSuccess'}}])
        for i in a:
            if ObjectId.is_valid(i['user']):
                user.add(str(i['user']))
    return user
    # elif string == 'between':
    #     a = col_order.aggregate([{'$match': {'serverTime': {'$gte': t1, ''}, 'eventKey': 'paymentSuccess'}}])
    #     for i in a:
    #         set(user).add(i['user'])


def work_1(user_work1):
    user_topic = set()
    a = col_event.aggregate(
        [{'$match': {'serverTime': {'$lt': START}, 'eventKey': 'enterTopicFinish', 'user': {'$in': user_work1}}},
         {'$project': {'user': 1, 'topicId': 1}},
         {'$group': {'_id': '$user', 'topic': {'$addToSet': '$topicId'}}}], allowDiskUse=True)
    for i in a:
        if len(set(i['topic'])) >= 3:
            user_topic.add(i['_id'])
    return user_topic


def filter_1(t1, t2, t3):
    """register before t1 and active between t2 and t3"""
    user = list()
    f1 = col_user.find({'activateDate': {'$lt': t1}})
    for i in f1:
        for i_1 in i['daily']:
            if len(i_1) == 8:
                if t2 <= (datetime.strptime(i_1, '%Y%m%d') - timedelta(hours=8)) < t3:
                    user.append(i['user'])
                    break
    user_show = f_event(START, END)
    # set_1 = set(user) & set(user_show)  # 看过视频和活跃的交集
    # set_2 = set(user) - set_1  # 活跃但没看过视频
    return user, user_show  # user是活跃过且注册 usershow是看过真人秀的


def f_event(t1, t2):
    """filter with filter_1"""
    f2 = col_event.find({'serverTime': {'$gte': t1, '$lt': t2}, 'eventKey': 'clickShowVideo'})
    df_7 = pd.DataFrame(list(f2))
    return list(df_7['user'].unique())


def main():
    print('1')
    user_all, user_sh = filter_1(LIMIT, START, END)
    user_3 = work_1(user_all)  # user_3是活跃注册大于3个知识点
    user_4 = vip('before', START)
    user_1 = (user_3 - user_4) & set(user_sh)  # 16号之前没付费且看过真人秀
    user_2 = (user_3 - user_4) - set(user_sh)
    print('a组用户数:', len(user_1))
    print('b组用户数:', len(user_2))
    # user_1 = user_1 & user_3 - user_4  # a组
    # user_2 = user_2 & user_3 - user_4  # b组
    user_5 = vip('after', START)
    print(len(user_4))
    print('a组付费用户数:', len(user_1 & user_5))
    print('b组付费用户数:', len(user_2 & user_5))
    print('a组付费率:', len(user_1 & user_5)/len(user_1))
    print('b组付费率:', len(user_2 & user_5) / len(user_2))


if __name__ == '__main__':
    main()
