from pymongo import MongoClient
from datetime import datetime
client=MongoClient('10.8.8.111',27017)
db_1=client.cache
collection_1=db_1.deviceAttr
# a=collection_1.aggregate(["$match": {'activateDate':{'$gte':datetime.strptime('20160905','%Y%m%d'),
#                                                     '$lt':datetime.strptime('20160906','%Y%m%d')}
#                                     }
#                         ])
a = collection_1.aggregate([{"$match": {'activateDate': {'$gte': datetime.strptime('20161016', '%Y%m%d'),  # 改这个时间就行了
                                                         '$lt': datetime.strptime('20161017', '%Y%m%d')}}
                             },
                            {"$project": {'daily': 1}}
                            ])#daily是list
num=0
for i in a:
    for i_1 in i['daily']:
        a=datetime.strptime(i_1,'%Y%m%d')
        if(a>=datetime.strptime('20161023','%Y%m%d')):
            if(a<datetime.strptime('20161030','%Y%m%d')):
                num=num+1
                break
print(num)

