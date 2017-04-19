from pymongo import MongoClient
from datetime import datetime
client=MongoClient('10.8.8.111',27017)
db_1=client.cache
collection_1=db_1.deviceAttr
db_2=client.eventsV4
collection_2=db_2.eventV4
# a=collection.find({"serverTime":{'$lt':datetime.fromtimestamp(1474300800.0),
#                                  '$gt':datetime.fromtimestamp(1473696000.0),
#                                  },'$or':[{'eventKey':'startVideo'},{'eventKey':'startPractice'}],
#                    })
a=collection_1.aggregate([{"$match":{'activateDate':{'$gte':datetime.strptime('20161015','%Y%m%d'),#改这个时间就行了
                                                     '$lt':datetime.strptime('20161016','%Y%m%d')},
                                        'os': 'ios'}
                           },
                          {"$project":{'device':1}}
                          ])
e=[]
number2=0
for i in a:
    e.append(i['device'])
    number2=number2+1
    print(number2)
b= collection_1.aggregate([{"$match": {'activateDate': {'$gte': datetime.strptime('20161015', '%Y%m%d'),#改这个时间
                                                         '$lt': datetime.strptime('20161016', '%Y%m%d')},
                                        'os': 'android'}
                             },
                            {
                                "$project":{'device':1}
                            }])
f=[]
number3=0
for i in b:
    f.append(i['device'])
    number3=number3+1
    print(number3)
c=collection_2.aggregate([{"$match":{'serverTime':{'$gte':datetime.strptime('20161015','%Y%m%d'),
                                                   '$lt':datetime.strptime('20161022','%Y%m%d')
                                                  },
                                    'os':'ios','device':{'$in': e},
                                    'eventKey':{'$in':['enterHome','enterChapterList','enterVideo','finishVideo','startPractice',
                                                   'enterTopicFinish']}
                                    }
                           },
                          {'$group':{'_id':{'device':'$device','eventKey':'$eventKey'}}}
                       #   {"$project":{'eventKey':1}}
                          ])
num_enterhome=0
num_chapter=0
num_entervideo=0
num_finishvideo=0
num_startpractice=0
num_entertopicfinish=0
for i_1 in c:
    if(i_1['_id']['eventKey']=='enterHome'):
        num_enterhome=num_enterhome+1
    elif(i_1['_id']['eventKey']=='enterChapterList'):
        num_chapter=num_chapter+1
    elif(i_1['_id']['eventKey']=='enterVideo'):
        num_entervideo=num_entervideo+1
    elif(i_1['_id']['eventKey']=='finishVideo'):
        num_finishvideo=num_finishvideo+1
    elif(i_1['_id']['eventKey']=='startPractice'):
        num_startpractice=num_startpractice+1
    elif(i_1['_id']['eventKey']=='enterTopicFinish'):
        num_entertopicfinish=num_entertopicfinish+1
print("ios的用户总数以及各项如下:")
print(number2)
print(num_enterhome)
print(num_chapter)
print(num_entervideo)
print(num_finishvideo)
print(num_startpractice)
print(num_entertopicfinish)
d = collection_2.aggregate([{"$match": {'serverTime': {'$gte': datetime.strptime('20161015', '%Y%m%d'),
                                                       '$lt': datetime.strptime('20161022', '%Y%m%d')
                                                       },
                                        'os': 'android','device':{'$in': f},
                                        'eventKey':{'$in':['enterHome','enterChapterList','enterVideo','finishVideo','startPractice',
                                                   'enterTopicFinish']}
                                        }
                             },
                            {'$group': {'_id': {'device': '$device', 'eventKey': '$eventKey'}}}
                          #  {"$project": {'eventKey': 1, 'device': 1}}
                            ])
number=0
number1=0
num_enterhome_and=0
num_chapter_and=0
num_entervideo_and=0
num_finishvideo_and=0
num_startpractice_and=0
num_entertopicfinish_and=0
for i_1 in d:
    if (i_1['_id']['eventKey'] == 'enterHome'):
        num_enterhome_and = num_enterhome_and + 1
    elif (i_1['_id']['eventKey'] == 'enterChapterList'):
        num_chapter_and = num_chapter_and + 1
    elif (i_1['_id']['eventKey'] == 'enterVideo'):
        num_entervideo_and = num_entervideo_and + 1
    elif (i_1['_id']['eventKey'] == 'finishVideo'):
        num_finishvideo_and = num_finishvideo_and + 1
    elif(i_1['_id']['eventKey']=='startPractice'):
        num_startpractice_and=num_startpractice_and+1
    elif (i_1['_id']['eventKey'] == 'enterTopicFinish'):
        num_entertopicfinish_and = num_entertopicfinish_and + 1
print("android如下:")
print(number3)
print(num_enterhome_and)
print(num_chapter_and)
print(num_entervideo_and)
print(num_finishvideo_and)
print(num_startpractice_and)
print(num_entertopicfinish_and)

