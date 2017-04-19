from bson.objectid import ObjectId
from datetime import datetime,timedelta
from pymongo import MongoClient
from collections import OrderedDict
client=MongoClient('10.8.8.111',27017)
db_1=client.eventsV4
collection_1=db_1.eventV4
db_2=client.onions
collection_2=db_2.topics
# file1=open('topic.txt','wb')
# file2=open('cishu.txt','wb')
START = datetime(2016,10,27)
END = datetime(2016,11,3)
a=collection_2.find()
topic_name=OrderedDict()
for i in a:
    c=str(i['_id'])
    topic_name[c]=0
b=collection_1.aggregate([{"$match":{'serverTime':{'$gte': START,'$lt': END},
                                     'eventKey':'startVideo',
                                     'topicId':{'$exists':True}
                                     }},
                          {"$project":{'topicId':1,'user':1}},
                          {"$group":{'_id':'$topicId','user':{'$addToSet':'$user'}}}

                          ],allowDiskUse=True)
#number=0
for i in b:
    if(i['_id'] in topic_name):
        topic_name[i['_id']]=len(i['user'])
for i in topic_name:
    print(i)
for i in topic_name:
    print(topic_name[i])

