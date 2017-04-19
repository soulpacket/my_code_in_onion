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
END = datetime(2016,10,28)
a=collection_2.find()
topic_name=OrderedDict()
# for i in a:
#     c=str(i['_id'])
#     topic_name[c]=0
   # print(str(i['_id']))
# a=str(ObjectId("54cc731fabc5bbb971f99bb5"))
# print(len(str(ObjectId("54cc731fabc5bbb971f99bb5"))))
# print(ObjectId(a))
# if(a=="54cc731fabc5bbb971f99bb5"):
#     print("hh")
b=collection_1.aggregate([{"$match":{'serverTime':{'$gte': START,'$lt': END},
                                     'eventKey':'startVideo',
                                     'topicId':{'$exists':True}
                                     }},
                          {"$project":{'topicId':1,'user':1}},
                          {"$group":{'_id':'$topicId','pv':{'$sum':1}}}
                          ])
for i in b:
    print(i)
