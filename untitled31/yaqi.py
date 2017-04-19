from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
client = MongoClient('10.8.8.111', 27017)
db_1 = client.eventsV4
db_2 = client.cache
col_1 = db_1.eventV4
col_2 = db_2.userAttr
LIMIT = datetime(2017, 1, 15, 16)
START = datetime(2017, 1, 15, 16)
END = datetime(2017, 1, 22, 16)
# f_one = open('info_a.csv', 'w')


def filter_1(t1, t2, t3):
    """register before t1 and active between t2 and t3"""
    user = list()
    f1 = col_2.find({'activateDate': {'$lt': t1}})
    num = 0
    for i in f1:
        for i_1 in i['daily']:
            if len(i_1) == 8:
                if t2 <= (datetime.strptime(i_1, '%Y%m%d') - timedelta(hours=8)) < t3:
                    user.append(i['user'])
                    break
    set_1 = set(user) & set(f_event(START, END))  # 看过视频和活跃的交集
    set_2 = set(user) - set_1  # 活跃但没看过视频
    return set_1, set_2


def f_event(t1, t2):
    """filter with filter_1"""
    f2 = col_1.find({'serverTime': {'$gte': t1, '$lt': t2}, 'eventKey': 'clickShowVideo'})
    df_7 = pd.DataFrame(list(f2))
    return list(df_7['user'].unique())


def work(t4):
    """total register time"""
    total_time = 0
    # for index, row in t4.iterrows():
    #     total_time = total_time + (LIMIT - row['activateDate'][index]).days + 1
    # return total_time
    f2 = col_2.aggregate([{'$match': {'user': {'$in': t4}}}])
    for i in f2:
        a = LIMIT - i['activateDate']
        if a.seconds > 0:
            b = a.days+1
        else:
            b = a.days
        total_time += b
        # f_one.write(str(i['user']))
        # f_one.write(',')
        # f_one.write(str(i['activateDate']))
        # f_one.write('\n')
    return total_time


def work_1(user):
    total_topic = 0
    total_user = 0
    a = col_1.aggregate([{'$match': {'serverTime': {'$lt': LIMIT}, 'eventKey': 'enterTopicFinish', 'user': {'$in': user}}},
                         {'$project': {'user': 1, 'topicId': 1}},
                         {'$group': {'_id': '$user', 'topic': {'$addToSet': '$topicId'}}}], allowDiskUse=True)
    for i in a:
        total_user += 1
        total_topic += len(set(i['topic']))
    return total_topic/total_user


# def main():
df_1, df_2 = filter_1(LIMIT, START, END)
user_1 = len(df_1)
user_2 = len(df_2)
print('a组人数:', user_1)
print('b组人数:', user_2)
# work(list(df_1))

print('a组平均注册时长是:', work(list(df_1))/user_1)  # average_time
print('b组平均注册市场是:', work(list(df_2))/user_2)  # average_time
# print('a组平均知识点是:', work_1(list(df_1)))
# print('b组平均知识点是:', work_1(list(df_2)))
# f_one.close()
