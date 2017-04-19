from pymongo import MongoClient
from multiprocessing import Pool
import datetime
client = MongoClient('10.8.8.22', 27017)
db = client.eventsV4
events = db.xinyuEvents
START = datetime.datetime(2017, 1, 10)
END = datetime.datetime(2017, 1, 11)
appVersion = ['3.2.0', '3.2.1', '3.2.5']


def t1():
    set_1 = set()
    q1 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterList',
          'd_appVersion': {'$in': appVersion}, 'isTask': 'true'}
    for i in events.find(q1):
        set_1.add(i['user'])
    return set_1


def t2():
    set_2 = set()
    q2 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'enterChapterTaskNotice',
          'platform': 'app'}
    for i in events.find(q2):
        set_2.add(i['user'])
    return set_2


def t3():
    set_3 = set()
    q3 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickChapterTask',
          'd_appVersion': {'$in': appVersion}}
    for i in events.find(q3):
        set_3.add(i['user'])
    return set_3


def t4():
    set_4 = set()
    q4 = {'serverTime': {'$gte': START, '$lt': END}, 'eventKey': 'clickChapterTheme',
          'd_appVersion': {'$in': appVersion}}
    for i in events.find(q4):
        set_4.add(i['user'])
    return set_4
func_list = [t1(), t2(), t3(), t4()]
result = list()
pool = Pool(4)
for func in func_list:
    print('hello')
    result.append(pool.apply_async(func))




