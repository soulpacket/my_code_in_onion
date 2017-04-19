from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
client = MongoClient('10.8.8.111',27017)

db = client.onions
coll = db.dailysignins

db_1 = client.run
coll_1 = db_1.remove

# START = datetime(2016,10,24)
# END = datetime(2016,10,31)

START_1 = datetime(2016,10,31)
END_1 = datetime(2016,11,7)

START_2 = datetime(2016,11,7)
END_2 = datetime(2016,11,14)

START_3 = datetime(2016,11,14)#???//??
END_3 = datetime(2016,11,21)
wasteTime = 5400
a = coll.aggregate([{'$match':{'createTime':{'$gte':START_1,'$lt':END_1}}},
                    #{'$group':{'_id':'$_id','user':{'$addToSet':'$userId'}}}
                    {'$project':{'userId':1}}
                    ])
user = set()
for i in a:
    if 'userId' in i:
        user.add(str(i['userId']))
user_1=list(user)
print(len(user_1))#活跃总人数
b = coll_1.aggregate([{'$match':{'user':{'$in':user_1},
                                 #'$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
                                 'serverTime':{'$gte':START_1,'$lt':END_1}
                                 }
                    },
                      {'$group':{'_id':'$user','count':{'$sum':'$request.duration'}}},
                      {'$match':{'count':{'$gte':wasteTime}}}
                      #{'$group':{'_id':'$new','count_1':{'$sum':1}}}
                      ])

user1_3600=[]
for i in b:
    user1_3600.append(i['_id'])
print(len(user1_3600))#>=60的总人数

d = coll_1.aggregate([{'$match': {'user': {'$in': user1_3600},
                                  #{'user':{'$in':user1_3600}},
                                  # '$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
                                  'serverTime': {'$gte': START_2, '$lt': END_2}
                                  }
                       },
                      #{'$match':{'user':{'$in':user1_3600}}},
                      {'$group': {'_id': '$user', 'count': {'$sum': '$request.duration'}}},
                      {'$match': {'count': {'$gte': wasteTime}}}
                      # {'$group':{'_id':'$new','count_1':{'$sum':1}}}
                      ])
count = 0
#user1_3600 = []
for i in d:
    count += 1
print(count)

e = coll_1.aggregate([{'$match': {'user': {'$in': user1_3600},
                                  # {'user':{'$in':user1_3600}},
                                  # '$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
                                  'serverTime': {'$gte': START_3, '$lt': END_3}
                                  }
                       },
                      # {'$match':{'user':{'$in':user1_3600}}},
                      {'$group': {'_id': '$user', 'count': {'$sum': '$request.duration'}}},
                      {'$match': {'count': {'$gte': wasteTime}}}
                      # {'$group':{'_id':'$new','count_1':{'$sum':1}}}
                      ])
count = 0
#user1_3600 = []
for i in e:
    count += 1
print(count)

# f = coll_1.aggregate([{'$match': {'user': {'$in': user1_3600},
#                                   # {'user':{'$in':user1_3600}},
#                                   # '$or':[{'eventKey':'videoFinished'},{'eventKey':'videoQuitted'},{'eventKey':'problemAnswered'}],
#                                   'serverTime': {'$gte': START_3, '$lt': END_3}
#                                   }
#                        },
#                       # {'$match':{'user':{'$in':user1_3600}}},
#                       {'$group': {'_id': '$user', 'count': {'$sum': '$request.duration'}}},
#                       {'$match': {'count': {'$gte': wasteTime}}}
#                       # {'$group':{'_id':'$new','count_1':{'$sum':1}}}
#                       ])
# count = 0
# for i in f:
#     count += 1
# print(count)

#
#
