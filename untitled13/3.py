# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/3.py
# 258
# [7, 2, 3, 2, 3, 7, 26, 327, 430, 374, 404, 529, 252, 430, 316, 257, 298, 462, 336, 261, 243, 206, 180, 29, 4, 1, 1, 1, 3, 7, 30, 159, 506, 329, 240, 503, 333, 351, 336, 219, 254, 437, 386, 284, 241, 274, 180, 39, 5, 3, 1, 1, 1, 2, 8, 55, 168, 180, 131, 259, 225, 144, 136, 99, 122, 249, 247, 219, 174, 192, 72, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 2, 2, 2, 10, 95, 177, 148, 176, 213, 240, 180, 99, 120, 129, 198, 179, 156, 138, 125, 74, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 1, 1, 1, 2, 6, 46, 126, 170, 189, 220, 171, 115, 124, 104, 102, 181, 184, 165, 162, 137, 39, 8, 3, 3, 4, 3, 2, 3, 20, 377, 281, 328, 557, 580, 408, 505, 396, 341, 410, 533, 366, 273, 223, 196, 126, 21, 13, 4, 3, 2, 1, 3, 22, 268, 493, 289, 299, 583, 386, 628, 357, 312, 407, 643, 533, 283, 360, 265, 131, 31, 4, 0, 3, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 271, 216, 391, 420, 433, 238, 487, 292, 564, 453, 295, 265, 279, 217, 32, 5, 1, 1, 0, 0, 1, 7, 36, 74, 122, 144, 171, 181, 110, 142, 105, 128, 248, 363, 136, 110, 145, 59, 19, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 3, 3, 2, 1, 2, 21, 229, 339, 362, 321, 499, 635, 589, 335, 443, 364, 830, 555, 270, 403, 412, 249, 50, 9, 8, 5, 8, 5, 16, 20, 223, 267, 397, 264, 439, 511, 536, 286, 474, 384, 673, 512, 272, 280, 373, 199, 31]
# 408
# /Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/matplotlib/axes/_axes.py:531: UserWarning: No labelled objects found. Use label='...' kwarg on individual plots.
#   warnings.warn("No labelled objects found. "
# /Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/matplotlib/tight_layout.py:222: UserWarning: tight_layout : falling back to Agg renderer
#   warnings.warn("tight_layout : falling back to Agg renderer")
#
# Process finished with exit code 0

from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
db = client.run
coll = db.blog
#星期三
date_set = [{'date':{'$gte':datetime(1900,7,6),'$lt':datetime(1900,7,7)}},
            {'date':{'$gte':datetime(1900,7,13),'$lt':datetime(1900,7,14)}},
            {'date':{'$gte':datetime(1900,7,20),'$lt':datetime(1900,7,21)}},
            {'date':{'$gte':datetime(1900,7,27),'$lt':datetime(1900,7,28)}},
            {'date':{'$gte':datetime(1900,8,3),'$lt':datetime(1900,8,4)}},
            {'date':{'$gte':datetime(1900,8,10),'$lt':datetime(1900,8,11)}},
            {'date':{'$gte':datetime(1900,8,17),'$lt':datetime(1900,8,18)}},
            {'date':{'$gte':datetime(1900,8,24),'$lt':datetime(1900,8,25)}},
            {'date':{'$gte':datetime(1900,8,31),'$lt':datetime(1900,9,1)}},
            {'date':{'$gte':datetime(1900,9,7),'$lt':datetime(1900,9,8)}},
            {'date':{'$gte':datetime(1900,9,14),'$lt':datetime(1900,9,15)}},
            {'date':{'$gte':datetime(1900,9,21),'$lt':datetime(1900,9,22)}},
            {'date':{'$gte':datetime(1900,9,28),'$lt':datetime(1900,9,29)}},
            {'date':{'$gte':datetime(1900,10,5),'$lt':datetime(1900,10,6)}},
            {'date':{'$gte':datetime(1900,10,12),'$lt':datetime(1900,10,13)}},
            {'date':{'$gte':datetime(1900,10,19),'$lt':datetime(1900,10,20)}},
            {'date':{'$gte':datetime(1900,10,26),'$lt':datetime(1900,10,27)}}
            #{'date':{'$gte':datetime(1900,10,31),'$lt':datetime(1900,11,1)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,6,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,13,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,20,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,27,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,3,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,10,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,17,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,24,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,31,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,7,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,14,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,21,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,28,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,5,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,12,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,19,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,26,i_1)]=0
# for i_1 in range(24):
#     dict_1[datetime(1900,10,31,i_1)]=0
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
x = list(np.linspace(0,17,409))
z = list(np.linspace(0,17,18))
index = []
#wid = []
for i in range(408):
    index.append(x[i])
    #wid.append(0.01+x[i])
opacity = 0.4
#wideth = 0.01
rects1 = plt.bar(index, list_1,1/24 , alpha=opacity, color='b')
#rects2 = plt.bar(wid,y2,0.01,alpha=opacity, color='r')
plt.xlabel('星期3')
plt.ylabel('人数')
plt.title('位置1的人数分布情况')
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()