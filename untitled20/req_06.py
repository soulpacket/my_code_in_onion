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
# END = datetime.datetime(2017, 1, 10, 0)
END = get_week_day(1)  # 本周一凌晨0点-8小时
workbook = xlsxwriter.Workbook('v42/data/req06-'+start_2_end(START, END)+'.xlsx')
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
print("hh")
agg_1 = coll_1.aggregate(pip1)
for i in agg_1:
    # print(i)
    if ObjectId.is_valid(i['_id']):
        user_1.append(ObjectId(i['_id']))
        enterTime_1.append(i['enterTime'])
        all_user += 1
agg_1.close()
print("kk")
gong_user = user_1
Time = []
enterTime = copy(enterTime_1, Time)
pip2 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 1,
                    'taskState': 'unbegin'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_2 = coll_1.aggregate(pip2)
user_6 = []
print("2")
for i in agg_2:
    if ObjectId.is_valid(i['_id']):
        user_2.append(ObjectId(i['_id']))
        user_6.append(i['_id'])
        enterTime_2.append(sorted(i['enterTime'])[0])
        uv_1[1] += 1
print("2fin")
uv_1[0] = all_user
# for i in range(len(user_2)):
#     if user_2[i] in user_1:
#         c = user_1.index(user_2[i])
#         enterTime_1[c] = look_for(enterTime_1[c], enterTime_2[i])
#         # index_1.append(user_1.index(user_2[i]))
# count = 0
print("3")
pip3 = [{'$match': {'d_appVersion': '3.2.0', 'eventKey': 'enterTopicFinish',
                    'isTaskSuccess': 'true', 'Verify': 'none'}}]
user_7 = []
agg_3 = coll_1.aggregate(pip3)
for i in agg_3:
    print(i)
    if i['serverTime'] >= START and i['serverTime'] <= END:
        if i['user'] in user_6:
            if ObjectId.is_valid(i['user']):
                if ObjectId(i['user']) in user_1:
                    c = user_1.index(ObjectId(i['user']))
                    if type(enterTime_1[c]) != list:
                        if (i['serverTime'] - datetime.timedelta(days=7)) <= enterTime_1[c]:
                            uv_1[2] += 1
                            user_7.append(i['user'])
                            del user_6[user_6.index(i['user'])]
print("3fin")
workbook.close()
