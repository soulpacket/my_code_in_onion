from config import *
START = ONLINE_DATE
# START = datetime.datetime(2017, 1, 9, 0)
# END = datetime.datetime(2017, 1, 10, 0)
END = get_week_day(1)  # 本周一凌晨0点-8小时
coll_1 = db.eventV4
pip3 = [{'$match': {'serverTime':{'$gte': START, '$lt': END}, 'd_appVersion': '3.2.0', 'isTaskSuccess': 'true', 'Verify': 'none', 'eventKey': 'enterTopicFinish'}}]
        #{'$match': {'serverTime': {'$gte': START, '$lt': END}}}

        #{'$match': {'isTaskSuccess': 'true', 'Verify': 'none'}}]
         #           # 'isTaskSuccess': 'true', 'Verify': 'none'}}]
         # {'$project': {'user': 1, 'serverTime': 1}},
         # {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
a = coll_1.aggregate(pip3)
for i in a:
    print(i)
# b = coll_1.find({'d_appVersion': '3.2.0', 'isTaskSuccess': 'true', 'Verify': 'none', 'eventKey': 'enterTopicFinish',
#                  'serverTime': {'$gte': START, '$lt': END}})
# for i in b:
#     print(i)
