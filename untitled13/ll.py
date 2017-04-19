# from datetime import datetime
# a = '110810'
# b = datetime.strptime(a,'%m%d%H')
# if datetime(1900,11,8,0)>=datetime(1900,11,8):
#     print(b)
# c = coll.aggregate([{'$match':{}}])
#/Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/ll.py
#327
#[9, 6, 0, 2, 0, 3, 16, 49, 122, 143, 171, 197, 201, 104, 114, 104, 118, 212, 185, 182, 174, 160, 109, 38, 23, 9, 0, 1, 0, 2, 10, 139, 228, 214, 586, 342, 245, 235, 188, 117, 181, 278, 306, 242, 210, 194, 148, 31, 6, 3, 3, 1, 0, 7, 11, 26, 120, 164, 181, 247, 227, 155, 119, 139, 137, 242, 225, 209, 156, 202, 75, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 1, 1, 4, 15, 43, 90, 208, 238, 164, 149, 94, 67, 70, 86, 177, 139, 148, 141, 159, 64, 12, 2, 0, 0, 1, 2, 3, 16, 42, 118, 141, 125, 170, 148, 112, 91, 74, 107, 201, 188, 187, 174, 147, 69, 0, 2, 2, 3, 1, 1, 4, 7, 25, 43, 109, 107, 181, 144, 125, 88, 112, 114, 205, 178, 149, 107, 146, 61, 18, 36, 3, 1, 3, 2, 4, 24, 103, 227, 212, 304, 332, 320, 223, 243, 180, 217, 338, 280, 220, 184, 183, 119, 24, 5, 4, 1, 1, 0, 4, 16, 74, 186, 205, 190, 285, 304, 227, 216, 207, 287, 323, 285, 230, 199, 233, 139, 24, 6, 3, 1, 2, 1, 2, 14, 46, 123, 166, 181, 229, 218, 159, 145, 144, 157, 248, 241, 150, 173, 169, 127, 23, 4, 3, 2, 2, 3, 4, 7, 36, 80, 92, 96, 165, 152, 117, 98, 109, 125, 180, 148, 210, 90, 126, 71, 21, 7, 4, 3, 5, 4, 4, 12, 186, 249, 398, 319, 462, 477, 443, 247, 403, 259, 565, 347, 195, 136, 107, 133, 21, 33, 17, 17, 10, 10, 10, 43, 203, 333, 459, 743, 934, 764, 504, 389, 493, 578, 655, 635, 716, 375, 200, 182, 106, 22, 16, 18, 14, 9, 19, 20, 215, 321, 353, 350, 491, 391, 419, 255, 220, 308, 498, 301, 330, 177, 220, 161, 49, 8, 4, 3, 5, 3, 5, 20, 105, 246, 222, 313, 495, 469, 507, 237, 268, 418, 482, 451, 290, 193, 192, 239, 63]
#432
from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
db = client.run
coll = db.blog
#星期一
date_set = [{'date':{'$gte':datetime(1900,7,4),'$lt':datetime(1900,7,5)}},
            {'date':{'$gte':datetime(1900,7,11),'$lt':datetime(1900,7,12)}},
            {'date':{'$gte':datetime(1900,7,18),'$lt':datetime(1900,7,19)}},
            {'date':{'$gte':datetime(1900,7,25),'$lt':datetime(1900,7,26)}},
            {'date':{'$gte':datetime(1900,8,1),'$lt':datetime(1900,8,2)}},
            {'date':{'$gte':datetime(1900,8,8),'$lt':datetime(1900,8,9)}},
            {'date':{'$gte':datetime(1900,8,15),'$lt':datetime(1900,8,16)}},
            {'date':{'$gte':datetime(1900,8,22),'$lt':datetime(1900,8,23)}},
            {'date':{'$gte':datetime(1900,8,29),'$lt':datetime(1900,8,30)}},
            {'date':{'$gte':datetime(1900,9,5),'$lt':datetime(1900,9,6)}},
            {'date':{'$gte':datetime(1900,9,12),'$lt':datetime(1900,9,13)}},
            {'date':{'$gte':datetime(1900,9,19),'$lt':datetime(1900,9,20)}},
            {'date':{'$gte':datetime(1900,9,26),'$lt':datetime(1900,9,27)}},
            {'date':{'$gte':datetime(1900,10,3),'$lt':datetime(1900,10,4)}},
            {'date':{'$gte':datetime(1900,10,10),'$lt':datetime(1900,10,11)}},
            {'date':{'$gte':datetime(1900,10,17),'$lt':datetime(1900,10,18)}},
            {'date':{'$gte':datetime(1900,10,24),'$lt':datetime(1900,10,25)}},
            {'date':{'$gte':datetime(1900,10,31),'$lt':datetime(1900,11,1)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,4,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,11,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,18,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,25,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,1,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,8,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,15,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,22,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,29,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,5,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,12,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,19,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,26,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,3,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,10,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,17,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,24,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,31,i_1)]=0
a = coll.aggregate([{'$match':{'location':'02','$or':date_set}},
                    {'$group':{'_id':'$date','count':{'$sum':1}}}
                    ])
# c = '1900-08-21 22:00:00.000Z'
# a = '1900-08-21 '+'22'+':00:00.000Z'
# if a==c:
#     print(a)
num = 0
for i in a:
    num = num+1
    # if i['_id']==datetime(1900,9,26,19):
    #     print('2')
    if i['_id'] in dict_1:
        dict_1[i['_id']]=i['count']
print(num)
list_1=[]
for i in dict_1:
    list_1.append(dict_1[i])
print(list_1)
print(len(list_1))#24*18=432
#画图
x = list(np.linspace(0,18,433))
z = list(np.linspace(0,18,19))
index = []
#wid = []
for i in range(432):
    index.append(x[i])
    #wid.append(0.01+x[i])
opacity = 0.4
#wideth = 0.01
rects1 = plt.bar(index, list_1,1/24 , alpha=opacity, color='b')
#rects2 = plt.bar(wid,y2,0.01,alpha=opacity, color='r')
plt.xlabel('日期')
plt.ylabel('人数')
plt.title('位置2的人数分布情况')
#name=(0704,0711,)
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()