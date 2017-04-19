from pymongo import MongoClient
import datetime
client = MongoClient('10.8.8.111', 27017)
db = client.eventsV4
col_event = db.eventV4
appVersion = ['3.3.'+str(i) for i in range(10)]
print(appVersion)
START = datetime.datetime(2017, 3, 12, 16)
END = datetime.datetime(2017, 3, 14, 16)
# event_key = ['clickCCSOpenCoach', 'enterVideo', 'clickKernelTopic',
# 'enterTopicFinish', 'clickCoachFinishCoach', 'enterPayment']
# f3 = col_event.find({'serverTime': {'$gte': START, '$lt': END},
#                      'eventKey': 'enterPayment',
#                      'd_appVersion': {'$in': appVersion}
#                      })
f3 = col_event.aggregate([{'$match': {'serverTime': {'$gte': START, '$lt': END},
                                      'eventKey': 'enterTopicFinish'
                                      }},
                          {'$match': {'d_appVersion': '3.3.0'}}])
count = 0
for i in f3:
    count += 1
    print(count)
