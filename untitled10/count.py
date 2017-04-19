from pymongo import MongoClient
import math
client=MongoClient('10.8.8.111',27017)
db=client.userValue
coll=db.cache
r=coll.aggregate([{'$match':{'201610':{'$exists':True},
                             '201610.normScore':{'$gte':0.75,'$lt':2},
                             '$or':[{'201610.startPractice':{'$ne':0}},{'201610.startVideo':{'$ne':0}},{'201610.finishVideo':{'$ne':0}},
                                    {'201610.enterTopicFinish':{'$ne':0}}]
                             }
                   }])
g=coll.aggregate([{'$match':{'201610':{'$exists':True},
                             '201610.startPractice':0,'201610.startVideo':0,'201610.finishVideo':0,'201610.enterTopicFinish':0
                             }
                   }])
count_ti=0
count_biao=0
count_vip=0
for i in g:
    if i['201610']['vip']==True:
        count_vip +=1
        if i['201610']['vip_gte_128']==True:
            count_biao +=1
        else:
            count_ti +=1
print('vip数是%s' %count_vip)
print('体验数是%s' %count_ti)
print('标准数是%s' %count_biao)