from config import *
START = datetime.datetime(2017, 1, 9, 0)
END = datetime.datetime(2017, 1, 10, 16)
coll_1 = db.eventV4
pip1 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterTask'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
pip2 = [{'$match': {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickTOPButton', 'taskIndex': 1,
                    'taskState': 'unbegin'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
pip3 = [{'$match': {'eventKey': 'enterTopicFinish', 'd_appVersion': '3.2.0', 'serverTime': {'$gte': START, '$lt': END},
                    'isTaskSuccess': 'true','Verify':'none'}},
        {'$project': {'user': 1, 'serverTime': 1}},
        {'$group': {'_id': '$user', 'enterTime': {'$push': '$serverTime'}}}]
agg_3 = coll_1.aggregate(pip3)
for i in agg_3:
    print(i)
    break
# agg_1 = coll_1.aggregate(pip1)
# for i in agg_1:
#     print(i)