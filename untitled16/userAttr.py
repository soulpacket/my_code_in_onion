from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111',27017)
db = client.cache
coll = db.userAttr

db_1 = client.run
coll_1 = db_1.dump

START = datetime(2016,11,21)
END = datetime(2016,11,28)
a = coll.aggregate([{'$match':{'$or':[{'recentPCSession':{'$gte':START}},{'recentMobileSession':{'$gte':START}}]}
                    }])
#num = 0
#count = 0
user=[]
for i in a:
    #count+=1
    #print(count)
    for i_1 in i['daily']:
        if datetime.strptime(i_1,'%Y%m%d')>=START and datetime.strptime(i_1,'%Y%m%d')<END:
            #num = num+1
            user.append(i['user'])
            break
print(len(user))
user_1=user[0:250000]
user_2=user[250000:]
b = coll_1.aggregate([{'$match': {'user': {'$in': user_1},
                                  # '$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
                                  'serverTime': {'$gte': START, '$lt': END}
                                  }
                       },
                      {'$group': {'_id': '$user', 'count': {'$sum': '$request.duration'}}},
                      {'$match': {'count': {'$gte': 3600}}}
                      # {'$group':{'_id':'$new','count_1':{'$sum':1}}}
                      ])
count = 0
#user1_3600 = []
for i in b:
    count += 1
print(count)

b = coll_1.aggregate([{'$match': {'user': {'$in': user_2},
                                  # '$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
                                  'serverTime': {'$gte': START, '$lt': END}
                                  }
                       },
                      {'$group': {'_id': '$user', 'count': {'$sum': '$request.duration'}}},
                      {'$match': {'count': {'$gte': 3600}}}
                      # {'$group':{'_id':'$new','count_1':{'$sum':1}}}
                      ])
#count = 0
# user1_3600 = []
for i in b:
    count += 1
print(count)