"""
@author: xinyu
需求ID: 26
需求验证: 解锁了任务五的用户，进入章节检测的概率是否足够高，做完检测的概率是否足够高；
需求级别: P0
数据时间: 数据从20170116开始输出，每周一输出一次累计数据，连续输出一个月
"""
from config import *
START = datetime.datetime(2017,1,8,16)
# END = datetime.datetime(2017, 1, 10)
END = datetime.datetime(2017,2,5,16)  # 本周一凌晨0点-8小时
workbook = xlsxwriter.Workbook('v42/data/req26-'+start_2_end(START, END)+'.xlsx')
worksheet = workbook.add_worksheet()
worksheet.merge_range(0, 0, 0, 3, '章节检测漏斗')
a_1 = ['步骤', 'UV', '整体转化率', '对上一步转化率']
for i in range(4):
    worksheet.write(1, i, a_1[i])
a_2 = ['解锁任务五', '开始任务五(完成章节检测)', '进入章节检测封面', '进入章节检测题目页', '完成章节检测', '通过章节检测']
for i in range(6):
    worksheet.write(i+2, 0, a_2[i])

coll_1 = onions.taskstatuses
coll_2 = db.eventV4
agg_1 = coll_1.aggregate([{'$match': {'unlockTime':{'$gte':START,'$lt':END},
                                      'unlock': True, 'no': 5}},
                          {'$project': {'userId': 1, 'unlockTime': 1}}])
uv = [0, 0, 0, 0, 0, 0]
user_1 = []
time_1 = []
for i in agg_1:
    uv[0] += 1
    user_1.append(i['userId'])
    time_1.append(i['unlockTime'])
print(uv[0])
pip2 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTSCYes'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_2 = coll_2.aggregate(pip2)
user_2 = []
for i in agg_2:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            for i_1 in i['enterTime']:
                if (i_1 - datetime.timedelta(days=7)) <= time_1[c]:
                    user_2.append(i['_id'])
                    uv[1] += 1
                    break
pip3 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'user': {'$in': user_2},
                 'eventKey': 'enterExamCover', 'examType': 'chapter_exam'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_3 = coll_2.aggregate(pip3)
user_3 = []
for i in agg_3:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            for i_1 in i['enterTime']:
                if (i_1 - datetime.timedelta(days=7)) <= time_1[c]:
                    user_3.append(i['_id'])
                    uv[2] += 1
                    break
pip4 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'user': {'$in': user_3}, 'eventKey': 'enterHolyProblem'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_4 = coll_2.aggregate(pip4)
user_4 = []
for i in agg_4:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            for i_1 in i['enterTime']:
                if (i_1 - datetime.timedelta(days=7)) <= time_1[c]:
                    user_4.append(i['_id'])
                    uv[3] += 1
                    break
pip5 =\
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'user': {'$in': user_4}, 'eventKey': 'enterHolyScore'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_5 = coll_2.aggregate(pip5)
user_5 = []
for i in agg_5:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            for i_1 in i['enterTime']:
                if (i_1 - datetime.timedelta(days=7)) <= time_1[c]:
                    user_5.append(i['_id'])
                    uv[4] += 1
                    break
pip6 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterHolyScore', 'user': {'$in': user_5},
                 'type': {'$in': ['pass', 'perfect']}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_6 = coll_2.aggregate(pip6)
for i in agg_6:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            for i_1 in i['enterTime']:
                if (i_1 - datetime.timedelta(days=7)) <= time_1[c]:
                    uv[5] += 1
                    break
all_rate = []
for i in range(6):
    all_rate.append(percent(uv[i], uv[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(5):
    last_rate.append(percent(uv[i+1], uv[i]))
for i in range(6):
    worksheet.write(2+i, 1, uv[i])
for i in range(6):
    worksheet.write(2+i, 2, all_rate[i])
for i in range(6):
    worksheet.write(2+i, 3, last_rate[i])
workbook.close()
