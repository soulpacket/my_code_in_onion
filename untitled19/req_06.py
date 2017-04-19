"""
@author: xinyu
需求ID: 6
需求验证: 每个任务的体验流程是否足够流畅
需求级别: P0
数据时间: 数据从20170116开始输出，每周一输出一次累计数据，连续输出一个月
"""
from config import *
START = ONLINE_DATE
# START = datetime.datetime(2017, 1, 9, 0)
# END = datetime.datetime(2017, 1, 9, 6)
END = get_week_day(1)  # 本周一凌晨0点-8小时
workbook = xlsxwriter.Workbook('data/req06-'+start_2_end(START, END)+'.xlsx')
worksheet = workbook.add_worksheet()
worksheet.merge_range(0, 0, 0, 3, '任务一漏斗')
a_1 = ['步骤', 'UV', '整体转化率', '对上一步转化率']
for i in range(4):
    worksheet.write(1, i, a_1[i])
a_2 = ['进入体验任务页', '开始任务一', '完成任务一知识点', '进入任务一引导页', '领取任务一奖励']
for i in range(5):
    worksheet.write(i+2, 0, a_2[i])
worksheet.merge_range(7, 0, 7, 3, '任务二漏斗')
for i in range(4):
    worksheet.write(8, i, a_1[i])
a_3 = ['进入体验任务页', '开始任务二', '完成任务二知识点', '进入任务二引导页', '领取任务二奖励']
for i in range(5):
    worksheet.write(i+9, 0, a_3[i])
worksheet.merge_range(14, 0, 14, 3, '任务三漏斗')
for i in range(4):
    worksheet.write(15, i, a_1[i])
worksheet.merge_range(23, 0, 23, 3, '任务四漏斗')
for i in range(4):
    worksheet.write(24, i, a_1[i])
worksheet.merge_range(32, 0, 32, 3, '任务五漏斗')
for i in range(4):
    worksheet.write(33, i, a_1[i])

a_4 = ['进入体验任务页', '开始任务三', '完成任务三知识点', '开始任务三举一反三测试', '完成任务三举一反三测试', '进入任务三引导页', '领取任务三奖励']
for i in range(7):
    worksheet.write(i+16, 0, a_4[i])
a_5 = ['进入体验任务页', '开始任务四', '完成任务四知识点', '开始任务四大题验证测试', '完成任务四大题验证测试', '进入任务四引导页', '领取任务四奖励']
for i in range(7):
    worksheet.write(i+25, 0, a_5[i])
a_6 = ['进入体验任务页', '选择任务五章节', '开始任务五', '学习任务五知识点', '开始真题检测', '完成真题检测', '通过真题检测', '进入任务五引导页', '领取任务五奖励']
for i in range(9):
    worksheet.write(i+34, 0, a_6[i])


# 寻找entertask中离d2时间最近的小的
# d1是时间列表,d2是时间值
# 返回d1的一个时间值
def look_for(d1, d2):
    a = sorted(d1)
    for i_2 in range(len(a)):
        if a[i_2] == d2:
            return a[i_2]
        elif a[i_2] > d2:
            return a[i_2-1]
        elif i_2 == (len(a)-1):
            return a[i_2]


def copy(d3, d4):
    for i_4 in d3:
        d4.append(i_4)
    return d4

coll_1 = db.eventV4
uv_1 = [0] * 5
uv_2 = [0] * 5
uv_3 = [0] * 7
uv_4 = [0] * 7
uv_5 = [0] * 9
pip1 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTask'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_1 = []
user_2 = []
user_3 = []
user_4 = []
user_5 = []
enterTime_1 = []
enterTime_2 = []
enterTime_3 = []
enterTime_4 = []
enterTime_5 = []
index_1 = []
all_user = 0
num = 0
agg_1 = coll_1.aggregate(pip1)
for i in agg_1:
    # print(i)
    if ObjectId.is_valid(i['_id']):
        user_1.append(ObjectId(i['_id']))
        enterTime_1.append(i['enterTime'])
        all_user += 1
agg_1.close()
gong_user = user_1
Time = []
enterTime = copy(enterTime_1, Time)
pip2 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 1,
                    'taskState': 'unbegin'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_2 = coll_1.aggregate(pip2)
user_6 = []
for i in agg_2:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        user_6.append(i['_id'])
        enterTime_2.append(sorted(i['enterTime'])[0])
        uv_1[1] += 1
uv_1[0] = all_user
for i in range(len(user_2)):
    if user_2[i] in user_1:
        c = user_1.index(user_2[i])
        enterTime_1[c] = look_for(enterTime_1[c], enterTime_2[i])
        index_1.append(user_1.index(user_2[i]))
count = 0
pip3 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'd_appVersion': '3.2.0', 'eventKey': 'enterTopicFinish',
                    'isTaskSuccess': 'true', 'Verify': 'none'}}]
user_7 = []
agg_3 = coll_1.aggregate(pip3)
for i in agg_3:
    if ObjectId.is_valid(i['user']):
        if i['user'] in user_6:
            if ObjectId(i['user']) in user_1:
                c = user_1.index(ObjectId(i['user']))
                if type(enterTime_1[c]) != list:
                    if (i['serverTime'] - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_1[2] += 1
                        user_7.append(i['user'])
                        del user_6[user_6.index(i['user'])]
pip4 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTaskGuide', 'taskIndex': 1,
                 'user': {'$in': user_7}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_4 = coll_1.aggregate(pip4)
user_8 = []
for i in agg_4:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            if type(enterTime_1[c]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_1[3] += 1
                        user_8.append(i['_id'])
                        break
pip5 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 1,
                 'taskState': 'undraw', 'user': {'$in': user_8}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_5 = coll_1.aggregate(pip5)
for i in agg_5:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            if type(enterTime_1[c]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_1[4] += 1
                        break
del user_1
del user_2
del enterTime_1
del enterTime_2
all_rate = []
for i in range(5):
    all_rate.append(percent(uv_1[i], uv_1[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(4):
    last_rate.append(percent(uv_1[i+1], uv_1[i]))
for i in range(5):
    worksheet.write(2+i, 1, uv_1[i])
for i in range(5):
    worksheet.write(2+i, 2, all_rate[i])
for i in range(5):
    worksheet.write(2+i, 3, last_rate[i])
"""————————————----------————————————————————————————————————————————————————---——---漏斗2"""
pip6 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 2,
                    'taskState': 'unbegin'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_1 = gong_user
Time_1 = []
enterTime_1 = copy(enterTime, Time_1)
user_2 = []
enterTime_2 = []
agg_6 = coll_1.aggregate(pip6)
user_9 = []
for i in agg_6:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        enterTime_2.append(sorted(i['enterTime'])[0])
        user_9.append(i['_id'])
        uv_2[1] += 1
uv_2[0] = all_user
pip7 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'd_appVersion': '3.2.0', 'eventKey': 'enterTopicFinish',
                 'isTaskSuccess': 'true', 'Verify': 'none'}}]
user_10 = []
for i in range(len(user_2)):
    if user_2[i] in user_1:
        # print(enterTime_1[user_1.index(user_2[i])])
        enterTime_1[user_1.index(user_2[i])] = look_for(enterTime_1[user_1.index(user_2[i])], enterTime_2[i])
        # index_1.append(user_1.index(user_2[i]))
agg_7 = coll_1.aggregate(pip7)
for i in agg_7:
    if ObjectId.is_valid(i['user']):
        if i['user'] in user_9:
            if ObjectId(i['user']) in user_1:
                c = user_1.index(ObjectId(i['user']))
                if type(enterTime_1[c]) != list:
                    if (i['serverTime'] - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_2[2] += 1
                        user_10.append(i['user'])
                        del user_9[user_9.index(i['user'])]
pip8 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTaskGuide', 'taskIndex': 2,
                 'user': {'$in': user_10}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_11 = []
agg_8 = coll_1.aggregate(pip8)
for i in agg_8:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_2[3] += 1
                        user_11.append(i['_id'])
                        break
pip9 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 2,
                 'taskState': 'undraw', 'user': {'$in': user_11}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_9 = coll_1.aggregate(pip9)
for i in agg_9:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_2[4] += 1
                        break
del user_1
del user_2
del enterTime_1
del enterTime_2
all_rate = []
for i in range(5):
    all_rate.append(percent(uv_2[i], uv_2[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(4):
    last_rate.append(percent(uv_2[i+1], uv_2[i]))
for i in range(5):
    worksheet.write(9+i, 1, uv_2[i])
for i in range(5):
    worksheet.write(9+i, 2, all_rate[i])
for i in range(5):
    worksheet.write(9+i, 3, last_rate[i])
"""————————————————------——————————————————————————————-————------——————————————漏斗三"""
pip10 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 3,
                 'taskState': 'unbegin'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_13 = []
user_14 = []
user_15 = []
user_16 = []
user_1 = gong_user
Time_1 = []
enterTime_1 = copy(enterTime, Time_1)
user_2 = []
enterTime_2 = []
agg_10 = coll_1.aggregate(pip10)
user_12 = []
for i in agg_10:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        enterTime_2.append(sorted(i['enterTime'])[0])
        user_12.append(i['_id'])
        uv_3[1] += 1
uv_3[0] = all_user
for i in range(len(user_2)):
    if user_2[i] in user_1:
        enterTime_1[user_1.index(user_2[i])] = look_for(enterTime_1[user_1.index(user_2[i])], enterTime_2[i])
        # index_1.append(user_1.index(user_2[i]))
pip11 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'd_appVersion': '3.2.0', 'eventKey': 'enterTopicFinish',
                 'isTaskSuccess': 'false', 'Verify': 'jyfs'}}]
agg_11 = coll_1.aggregate(pip11)
for i in agg_11:
    if ObjectId.is_valid(i['user']):
        if i['user'] in user_12:
            if ObjectId(i['user']) in user_1:
                c = user_1.index(ObjectId(i['user']))
                if type(enterTime_1[c]) != list:
                    if (i['serverTime'] - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_3[2] += 1
                        user_13.append(i['user'])
                        del user_12[user_12.index(i['user'])]
pip12 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterExamCover', 'examType': 'jyfs',
                 'user': {'$in': user_13}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_12 = coll_1.aggregate(pip12)
for i in agg_12:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_3[3] += 1
                        user_14.append(i['_id'])
                        break
pip13 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterExamFinish', 'examType': 'jyfs',
                 'user': {'$in': user_14}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_13 = coll_1.aggregate(pip13)
for i in agg_13:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_3[4] += 1
                        user_15.append(i['_id'])
                        break
pip14 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTaskGuide', 'taskIndex': 3,
                 'user': {'$in': user_15}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_14 = coll_1.aggregate(pip14)
for i in agg_14:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_3[5] += 1
                        user_16.append(i['_id'])
                        break
pip15 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 3,
                 'taskState': 'undraw', 'user': {'$in': user_16}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_15 = coll_1.aggregate(pip15)
for i in agg_15:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_3[6] += 1
                        break
del user_1
del user_2
del enterTime_1
del enterTime_2
all_rate = []
for i in range(7):
    all_rate.append(percent(uv_3[i], uv_3[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(6):
    last_rate.append(percent(uv_3[i+1], uv_3[i]))
for i in range(7):
    worksheet.write(16+i, 1, uv_3[i])
for i in range(7):
    worksheet.write(16+i, 2, all_rate[i])
for i in range(7):
    worksheet.write(16+i, 3, last_rate[i])
"""————————————————------——————————————————————————————-————------——————————————漏斗四"""
pip16 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 4,
                 'taskState': 'unbegin'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_17 = []
user_18 = []
user_19 = []
user_20 = []
user_21 = []
user_1 = gong_user
Time_1 = []
enterTime_1 = copy(enterTime, Time_1)
user_2 = []
enterTime_2 = []
agg_16 = coll_1.aggregate(pip16)
for i in agg_16:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        enterTime_2.append(sorted(i['enterTime'])[0])
        user_17.append(i['_id'])
        uv_4[1] += 1
uv_4[0] = all_user
for i in range(len(user_2)):
    if user_2[i] in user_1:
        enterTime_1[user_1.index(user_2[i])] = look_for(enterTime_1[user_1.index(user_2[i])], enterTime_2[i])
        # index_1.append(user_1.index(user_2[i]))
pip17 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'd_appVersion': '3.2.0', 'eventKey': 'enterTopicFinish',
                 'isTaskSuccess': 'false', 'Verify': 'dtsz'}}]
agg_17 = coll_1.aggregate(pip17)
for i in agg_17:
    if ObjectId.is_valid(i['user']):
        if i['user'] in user_17:
            if ObjectId(i['user']) in user_1:
                c = user_1.index(ObjectId(i['user']))
                if type(enterTime_1[c]) != list:
                    if (i['serverTime'] - datetime.timedelta(days=7)) <= enterTime_1[c]:
                        uv_4[2] += 1
                        user_18.append(i['user'])
                        del user_17[user_17.index(i['user'])]
pip18 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterExamCover',
                 'user': {'$in': user_18}, 'examType': 'dtsz'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_18 = coll_1.aggregate(pip18)
for i in agg_18:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_4[3] += 1
                        user_19.append(i['_id'])
                        break
pip19 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterExamFinish',
                 'user': {'$in': user_19}, 'examType': 'dtsz'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_19 = coll_1.aggregate(pip19)
for i in agg_19:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_4[4] += 1
                        user_20.append(i['_id'])
                        break
pip20 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTaskGuide',
                 'user': {'$in': user_20}, 'taskIndex': 4}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_20 = coll_1.aggregate(pip20)
for i in agg_20:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            if type(enterTime_1[c]) != list:
                if (i['enterTime'][0] - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                    uv_4[5] += 1
                    user_21.append(i['_id'])
pip21 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 4,
                 'taskState': 'undraw',  'user': {'$in': user_21}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_21 = coll_1.aggregate(pip21)
for i in agg_21:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            c = user_1.index(ObjectId(i['_id']))
            if type(enterTime_1[c]) != list:
                if (i['enterTime'][0] - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                    uv_4[6] += 1
del user_1
del user_2
del enterTime_1
del enterTime_2
all_rate = []
for i in range(7):
    all_rate.append(percent(uv_4[i], uv_4[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(6):
    last_rate.append(percent(uv_4[i+1], uv_4[i]))
for i in range(7):
    worksheet.write(25+i, 1, uv_4[i])
for i in range(7):
    worksheet.write(25+i, 2, all_rate[i])
for i in range(7):
    worksheet.write(25+i, 3, last_rate[i])
"""————————————————------——————————————————————————————-————------——————————————漏斗五"""
pip22 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTaskSelectChapter'}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
user_22 = []
user_23 = []
user_24 = []
user_25 = []
user_26 = []
user_27 = []
user_28 = []
user_1 = gong_user
Time_1 = []
enterTime_1 = copy(enterTime, Time_1)
user_2 = []
enterTime_2 = []
agg_22 = coll_1.aggregate(pip22)
for i in agg_22:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        enterTime_2.append(sorted(i['enterTime'])[0])
        user_22.append(i['_id'])
        uv_5[1] += 1
uv_5[0] = all_user
for i in range(len(user_2)):
    if user_2[i] in user_1:
        enterTime_1[user_1.index(user_2[i])] = look_for(enterTime_1[user_1.index(user_2[i])], enterTime_2[i])
        # index_1.append(user_1.index(user_2[i]))
pip23 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTSCYes', 'user': {'$in': user_22}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_23 = coll_1.aggregate(pip23)
for i in agg_23:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[2] += 1
                        user_23.append(i['_id'])
                        break
pip24 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTaskTopic', 'user': {'$in': user_23}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_24 = coll_1.aggregate(pip24)
for i in agg_24:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[3] += 1
                        user_24.append(i['_id'])
                        break
pip25 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterExamCover', 'examType': 'chapter_exam',
                 'user': {'$in': user_24}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_25 = coll_1.aggregate(pip25)
for i in agg_25:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[4] += 1
                        user_25.append(i['_id'])
                        break
pip26 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterHolyScore', 'user': {'$in': user_25}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_26 = coll_1.aggregate(pip26)
for i in agg_26:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[5] += 1
                        user_26.append(i['_id'])
                        break
pip27 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterHolyScore', 'user': {'$in': user_26},
                 'type': {'$in': ['pass', 'perfect']}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_27 = coll_1.aggregate(pip27)
for i in agg_27:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[6] += 1
                        user_27.append(i['_id'])
                        break
pip28 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTaskGuide', 'taskIndex': 5,
                 'user': {'$in': user_27}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_28 = coll_1.aggregate(pip28)
for i in agg_28:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[7] += 1
                        user_28.append(i['_id'])
                        break
pip29 = \
    [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTaskGetGift', 'user': {'$in': user_28}}},
     {'$project': {'user': 1, 'serverTime': 1}},
     {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_29 = coll_1.aggregate(pip29)
for i in agg_29:
    if ObjectId.is_valid(i['_id']):
        if ObjectId(i['_id']) in user_1:
            if type(enterTime_1[user_1.index(ObjectId(i['_id']))]) != list:
                for i_1 in i['enterTime']:
                    if (i_1 - datetime.timedelta(days=7)) <= enterTime_1[user_1.index(ObjectId(i['_id']))]:
                        uv_5[8] += 1
                        break
all_rate = []
for i in range(9):
    all_rate.append(percent(uv_5[i], uv_5[0]))
last_rate = list()
last_rate.append(percent(1, 1))
for i in range(8):
    last_rate.append(percent(uv_5[i+1], uv_5[i]))
for i in range(9):
    worksheet.write(34+i, 1, uv_5[i])
for i in range(9):
    worksheet.write(34+i, 2, all_rate[i])
for i in range(9):
    worksheet.write(34+i, 3, last_rate[i])
workbook.close()
