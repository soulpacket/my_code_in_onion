import numpy as np
#大概分个20组吧,每组区间内的人数写成一个列表.横坐标就是1/20=0.05
#(260967,125648,77433,51463,35827,26584,20184,15725,12619,10070,8154,7030,6027,5013,4370,3775,3099,2654,2134,8184)
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient('10.8.8.111',27017)
db = client.userValue
coll = db.cache
x = list(np.linspace(0,1,51))
z = list(np.linspace(0,1,11))
index = []
wid = []
for i in range(50):
    index.append(x[i])
    wid.append(0.01+x[i])
#y = (260967,125648,77433,51463,35827,26584,20184,15725,12619,10070,8154,7030,6027,5013,4370,3775,3099,2654,2134,8184)
#y=[127789, 96959, 68210, 50709, 42948, 34959, 29429, 25033, 21139, 18336, 15723, 13728, 12434, 10843, 9683, 8761, 7797, 7064, 6497, 5790, 5345, 5000, 4435, 4103, 3806, 3504, 3071, 3084, 2839, 2686, 2573, 2291, 2258, 1951, 1967, 1837, 1727, 1619, 1523, 1439, 1307, 1187, 1143, 1116, 1000, 880, 841, 814, 726, 7057]
y=[]
for i in range(50):
    num = 0
    if x[i+1]==1:
        r = coll.aggregate([{'$match': {'201610': {'$exists': True},
                                        '201610.normScore': {'$gte': 0.98, '$lte': 1},
                                        '$or': [{'201610.startPractice': {'$ne': 0}}, {'201610.startVideo': {'$ne': 0}},
                                                {'201610.finishVideo': {'$ne': 0}},
                                                {'201610.enterTopicFinish': {'$ne': 0}}],
                                        '201610.vip':True,
                                        '201610.vip_gte_128':True
                                        }
                             }])
    else:
        r = coll.aggregate([{'$match': {'201610': {'$exists': True},
                                        '201610.normScore': {'$gte': x[i], '$lt': x[i+1]},
                                        '$or': [{'201610.startPractice': {'$ne': 0}}, {'201610.startVideo': {'$ne': 0}},
                                                {'201610.finishVideo': {'$ne': 0}},
                                                {'201610.enterTopicFinish': {'$ne': 0}}],
                                        '201610.vip':True,
                                        '201610.vip_gte_128':True
                                        }
                             }])
    for i in r:
        num = num+1
    print(num)
    y.append(num)
print(y)
# y2=[]
# for i in range(50):
#     num1=0
#     if x[i+1]!=1:
#         g = coll.aggregate([{'$match': {'201610': {'$exists': True},
#                                         '201610.normScore':{'$gte':x[i],'$lt':x[i+1]},
#                                         '201610.startPractice': 0, '201610.startVideo': 0, '201610.finishVideo': 0,
#                                         '201610.enterTopicFinish': 0
#                                         }
#                              }])
#     else:
#         g = coll.aggregate([{'$match': {'201610': {'$exists': True},
#                                         '201610.normScore': {'$gte': 0.98, '$lte': 1},
#                                         '201610.startPractice': 0, '201610.startVideo': 0, '201610.finishVideo': 0,
#                                         '201610.enterTopicFinish': 0
#                                         }
#                              }])
#     for i_1 in g:
#         num1=num1+1
#     print(num1)
#     y2.append(num1)
# print(y2)
#y2=[200823, 9156, 1298, 122, 38, 14, 3, 3, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
#print(len(y2))
print(len(y))
fig, ax = plt.subplots()
opacity = 0.4
#wideth = 0.01
rects1 = plt.bar(index, y, 0.02, alpha=opacity, color='b')
#rects2 = plt.bar(wid,y2,0.01,alpha=opacity, color='r')
plt.xlabel('normScore')
plt.ylabel('人数')
plt.title('10月份标准vip人数分布情况')
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()

    #x = [0.01, 0.02, 0.03, 0.04, 0.05]# Make an array of x values
# y = [1, 4, 9, 16, 25]# Make an array of y values for each x value
# plt.plot(x, y)# use pylab to plot x and y
#
# plt.title('Plot of y vs.x')# give plot a title
# plt.xlabel('x')# make axis labels
# plt.ylabel('y')
#
# plt.xlim(0.0, 0.05)# set axis limits
# plt.ylim(0.0, 30.0)
#
# plt.show()# show the plot on the screen