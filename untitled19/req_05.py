"""
@author: xinyu
需求ID: 5
需求验证: 用户是否能完整走完体验流程
需求级别: P0
数据时间: 数据从20170116开始输出，每周一输出一次累计数据，连续输出一个月
"""
from config import *
START = ONLINE_DATE
# END = datetime.datetime(2017,1,10,0)
END = get_week_day(1)  # 本周一凌晨0点-8小时
workbook = xlsxwriter.Workbook('data/req05-'+start_2_end(START, END)+'.xlsx')
worksheet = workbook.add_worksheet()
worksheet.merge_range(0, 0, 0, 3, '任务体系漏斗')
aaa = ['步骤', 'UV', '整体转化率', '对上一步转化率']
for i in range(4):
    worksheet.write(1, i, aaa[i])
dd = ['进入体验任务页', '完成1个任务', '完成2个任务', '完成3个任务', '完成4个任务', '选择任务五章节', '完成任务五']
for i in range(7):
    worksheet.write(2+i, 0, dd[i])
# ————————————————————————————————————-——————————————————————————————————————————————————————————————————
UV = [0] * 7
coll_1 = db.eventV4
coll_2 = onions.taskrewards
agg_1 = coll_1.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTask'}},
                          {'$project': {'user': 1, 'serverTime': 1}},
                          {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}
                          ])
user = []
user_on = []
enterTime = []
createTime = []
all_user = 0
all_user_1 = 0
for i in agg_1:
    # print(i)
    if ObjectId.is_valid(i['_id']):
        user.append(ObjectId(i['_id']))
        enterTime.append(i['enterTime'])
        all_user += 1
UV[0] = all_user
index_1 = []
agg_2 = coll_2.aggregate([{'$match': {'createTime': {'$gte': START, '$lt': END}}},
                          {'$project': {'userId': 1, 'createTime': 1}},
                          {'$group': {'_id': '$userId', 'createTime': {'$push': '$createTime'}}}])
for i in agg_2:
    # print(i)
    user_on.append(i['_id'])
    createTime.append(i['createTime'])
for i in range(len(user_on)):
    if user_on[i] in user:
        for i_1 in enterTime[user.index(user_on[i])]:
            if (min(createTime[i]) - datetime.timedelta(days=2)) <= i_1:
                all_user_1 += 1
                index_1.append(i)
                break
UV[1] = all_user_1
index_2 = []
all_user_2 = 0
all_user_3 = 0
all_user_4 = 0
all_user_click = 0
all_user_5 = 0
for i in index_1:
    if len(createTime[i]) >= 2:
        createTime[i].sort()
        if (createTime[i][1] - datetime.timedelta(days=2)) <= createTime[i][0]:
            all_user_2 += 1
            if len(createTime[i]) >= 3:
                if (createTime[i][2] - datetime.timedelta(days=2)) <= createTime[i][1]:
                    all_user_3 += 1
                    if len(createTime[i]) >= 4:
                        if (createTime[i][3] - datetime.timedelta(days=2)) <= createTime[i][2]:
                            all_user_4 += 1
                            index_2.append(i)
UV[2] = all_user_2
UV[3] = all_user_3
UV[4] = all_user_4
user_1 = []
time_1 = []
for i in index_2:
    user_1.append(user_on[i])
    time_1.append(createTime[i])
agg_3 = coll_1.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTSCYes'}}])
for i in agg_3:
    if ObjectId.is_valid(i['user']):
        if ObjectId(i['user']) in user_1:
            if (i['serverTime'] - datetime.timedelta(days=2)) <= time_1[user_1.index(ObjectId(i['user']))][3]:
                all_user_click += 1
                if len(time_1[user_1.index(ObjectId(i['user']))]) == 5:
                    if (time_1[user_1.index(ObjectId(i['user']))][4] - datetime.timedelta(days=2)) <= i['serverTime']:
                        all_user_5 += 1
                if len(time_1[user_1.index(ObjectId(i['user']))]) == 6:
                    if (time_1[user_1.index(ObjectId(i['user']))][5] - datetime.timedelta(days=2)) <= i['serverTime']:
                        all_user_5 += 1

UV[5] = all_user_click
UV[6] = all_user_5

all_rate = []
for i in range(7):
    all_rate.append(percent(UV[i], UV[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(6):
    last_rate.append(percent(UV[i+1], UV[i]))
for i in range(7):
    worksheet.write(2+i, 1, UV[i])
for i in range(7):
    worksheet.write(2+i, 2, all_rate[i])
for i in range(7):
    worksheet.write(2+i, 3, last_rate[i])
workbook.close()
