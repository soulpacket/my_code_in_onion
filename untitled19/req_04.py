"""
Created on 04/01/2017
@author: xinyu
需求ID: 4
需求验证: 是否任务越多，越难，付费率越高
需求级别: P0
数据时间: 每周一输出一次累计数据
"""
from config import *

START = ONLINE_DATE
# END = datetime.datetime(2017, 1, 10, 0)
END = get_week_day(1)  # 本周一凌晨0点-8小时
workbook = xlsxwriter.Workbook('v42/data/req-04:' + start_2_end(START, END) + '.xlsx')
worksheet = workbook.add_worksheet()
worksheet.merge_range(0, 0, 0, 3, '完成任务与付费转化关系')
aaa = ["完成详情", "总用户", "付费用户", "付费率"]
for i in range(4):
    worksheet.write(1, i, aaa[i])

dd = [
    '完成1个任务',
    '完成2个任务',
    '完成3个任务',
    '完成4个任务',
    '未完成任务五',
    '完成了任务五',
    '完成过任务一',
    '完成过任务二',
    '完成过任务三',
    '完成过任务四',
    '完成任务三和四',
    '未完成任务三和四',
    '完成过任务'
]

for i in range(13):
    worksheet.write(2+i, 0, dd[i])
worksheet.merge_range(15, 0, 15, 1, '完成关键任务占比')
worksheet.write(16, 0, '指标')
worksheet.write(16, 1, '数据')
list_1 = ['完成任务三和四', '完成过任务', '完成关键任务占比']
for i in range(3):
    worksheet.write(17+i, 0, list_1[i])

coll_1 = onions.taskrewards
orders = db.orderEvents
all_user = []
vip_user = []
vip_rate = []
task_1 = []
task_2 = []
task_3 = []
task_4 = []
task_5 = []
time1 = []
time2 = []
time3 = []
time4 = []
time5 = []
vip_1 = 0
vip_2 = 0
vip_3 = 0
vip_4 = 0
vip_5 = 0
all_user_1 = 0  # 完成1个任务的总数
all_user_2 = 0  # 2个
all_user_3 = 0
all_user_4 = 0
all_user_5 = 0
pipline1 = [{'$match': {'createTime': {'$gte': START, '$lt': END}}},
            {'$project': {'userId': 1, 'createTime': 1}},
            {'$group': {'_id': '$userId', 'createTime': {'$push': '$createTime'}}}]
a1 = coll_1.aggregate(pipline1)

for i_1 in a1:

    if len(i_1['createTime']) == 1:
        task_1.append(i_1['_id'])
        time1.append(max(i_1['createTime']))
        all_user_1 += 1
    if len(i_1['createTime']) == 2:
        task_2.append(i_1['_id'])
        time2.append(max(i_1['createTime']))
        all_user_2 += 1
    if len(i_1['createTime']) == 3:
        task_3.append(i_1['_id'])
        time3.append(max(i_1['createTime']))
        all_user_3 += 1
    if len(i_1['createTime']) == 4:
        task_4.append(i_1['_id'])
        time4.append(max(i_1['createTime']))
        all_user_4 += 1
    if len(i_1['createTime']) >= 5:
        task_5.append(i_1['_id'])
        time5.append(max(i_1['createTime']))
        all_user_5 += 1
all_user.append(all_user_1)
all_user.append(all_user_2)
all_user.append(all_user_3)
all_user.append(all_user_4)
all_user.append(all_user_1+all_user_2+all_user_3+all_user_4)
all_user.append(all_user_5)
pipline2 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': task_1}, 'eventKey': 'paymentSuccess'}},
            {'$project': {'user': 1, 'serverTime': 1}},
            {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline3 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': task_2}, 'eventKey': 'paymentSuccess'}},
            {'$project': {'user': 1, 'serverTime': 1}},
            {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline4 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': task_3}, 'eventKey': 'paymentSuccess'}},
            {'$project': {'user': 1, 'serverTime': 1}},
            {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline5 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': task_4}, 'eventKey': 'paymentSuccess'}},
            {'$project': {'user': 1, 'serverTime': 1}},
            {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline6 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': task_5}, 'eventKey': 'paymentSuccess'}},
            {'$project': {'user': 1, 'serverTime': 1}},
            {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
a2 = orders.aggregate(pipline2)
for i in a2:
    if i['_id'] in task_1:
        if i['createTime'] >= time1[task_1.index(i['_id'])]:
            vip_1 += 1
vip_user.append(vip_1)
a3 = orders.aggregate(pipline3)
for i in a3:
    if i['_id'] in task_2:
        if i['createTime'] >= time2[task_2.index(i['_id'])]:
            vip_2 += 1
vip_user.append(vip_2)
a4 = orders.aggregate(pipline4)
for i in a4:
    if i['_id'] in task_3:
        if i['createTime'] >= time3[task_3.index(i['_id'])]:
            vip_3 += 1
vip_user.append(vip_3)
a5 = orders.aggregate(pipline5)
for i in a5:
    if i['_id'] in task_4:
        if i['createTime'] >= time4[task_4.index(i['_id'])]:
            vip_4 += 1
vip_user.append(vip_4)
a6 = orders.aggregate(pipline6)
for i in a6:
    if i['_id'] in task_5:
        if i['createTime'] >= time5[task_5.index(i['_id'])]:
            vip_5 += 1
vip_user.append(vip_1+vip_2+vip_3+vip_4)
vip_user.append(vip_5)
# -------------------------------------------------------------
pipline7 = [{'$match': {'createTime': {'$gte': START, '$lt': END}, 'taskNo': 1}}]
pipline8 = [{'$match': {'createTime': {'$gte': START, '$lt': END}, 'taskNo': 2}}]
pipline9 = [{'$match': {'createTime': {'$gte': START, '$lt': END}, 'taskNo': 3}}]
pipline10 = [{'$match': {'createTime': {'$gte': START, '$lt': END}, 'taskNo': 4}}]
a7 = coll_1.aggregate(pipline7)
a8 = coll_1.aggregate(pipline8)
a9 = coll_1.aggregate(pipline9)
a10 = coll_1.aggregate(pipline10)
user_7 = []
user_8 = []
user_9 = []
user_10 = []
time7 = []
time8 = []
time9 = []
time10 = []
all_user_7 = 0
all_user_8 = 0
all_user_9 = 0
all_user_10 = 0
for i in a7:
    user_7.append(i['userId'])
    time7.append(i['createTime'])
    all_user_7 += 1
all_user.append(all_user_7)
for i in a8:
    user_8.append(i['userId'])
    time8.append(i['createTime'])
    all_user_8 += 1
all_user.append(all_user_8)
for i in a9:
    user_9.append(i['userId'])
    time9.append(i['createTime'])
    all_user_9 += 1
all_user.append(all_user_9)
for i in a10:
    user_10.append(i['userId'])
    time10.append(i['createTime'])
    all_user_10 += 1
all_user.append(all_user_10)
# all_user.append(all_user_9+all_user_10)

pipline11 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': user_7}, 'eventKey': 'paymentSuccess'}},
             {'$project': {'user': 1, 'serverTime': 1}},
             {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline12 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': user_8}, 'eventKey': 'paymentSuccess'}},
             {'$project': {'user': 1, 'serverTime': 1}},
             {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline13 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': user_9}, 'eventKey': 'paymentSuccess'}},
             {'$project': {'user': 1, 'serverTime': 1}},
             {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
pipline14 = [{'$match': {'serverTime': {'$gte': START}, 'user': {'$in': user_10}, 'eventKey': 'paymentSuccess'}},
             {'$project': {'user': 1, 'serverTime': 1}},
             {'$group': {'_id': '$user', 'createTime': {'$max': '$serverTime'}}}]
a11 = orders.aggregate(pipline11)
a12 = orders.aggregate(pipline12)
a13 = orders.aggregate(pipline13)
a14 = orders.aggregate(pipline14)
vip_7 = 0
vip_8 = 0
vip_9 = 0
vip_10 = 0
user_11 = []
user_12 = []
user_13 = []
user_14 = []
for i in a11:
    if i['_id'] in user_7:
        if i['createTime'] >= time7[user_7.index(i['_id'])]:
            user_11.append(i['_id'])
            vip_7 += 1
vip_user.append(vip_7)

for i in a12:
    if i['_id'] in user_8:
        if i['createTime'] >= time8[user_8.index(i['_id'])]:
            user_12.append(i['_id'])
            vip_8 += 1
vip_user.append(vip_8)

for i in a13:
    if i['_id'] in user_9:
        if i['createTime'] >= time9[user_9.index(i['_id'])]:
            user_13.append(i['_id'])
            vip_9 += 1
vip_user.append(vip_9)

for i in a14:
    if i['_id'] in user_10:
        if i['createTime'] >= time10[user_10.index(i['_id'])]:
            user_14.append(i['_id'])
            vip_10 += 1
vip_user.append(vip_10)

set_1 = set(user_9) | set(user_10)
all_user.append(len(set_1))
set_2 = set(user_13) | set(user_14)
vip_user.append(len(set_2))

set_3 = set(user_7) | set(user_8)
all_user.append(len(set_3))
set_4 = set(user_11) | set(user_12)
vip_user.append(len(set_4))

all_user.append(all_user_1+all_user_2+all_user_3+all_user_4+all_user_5)
vip_user.append(vip_1+vip_2+vip_3+vip_4+vip_5)

for i in range(13):
    vip_rate.append(percent(vip_user[i], all_user[i]))
key = [all_user[10], all_user[12], percent(all_user[10], all_user[12])]
for i in range(13):
    worksheet.write(i+2, 1, all_user[i])
for i in range(13):
    worksheet.write(i+2, 2, vip_user[i])
for i in range(13):
    worksheet.write(i+2, 3, vip_rate[i])
for i in range(3):
    worksheet.write(17+i, 1, key[i])
workbook.close()
