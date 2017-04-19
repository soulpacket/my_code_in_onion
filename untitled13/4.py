# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/4.py
# 279
# [5, 2, 1, 2, 3, 7, 17, 143, 580, 318, 307, 617, 268, 479, 343, 225, 373, 493, 396, 303, 271, 267, 195, 45, 6, 1, 1, 1, 2, 3, 25, 116, 418, 277, 231, 485, 323, 308, 285, 204, 292, 442, 386, 276, 223, 277, 186, 57, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 4, 23, 121, 138, 154, 129, 244, 229, 166, 149, 143, 165, 230, 185, 182, 149, 155, 67, 11, 0, 0, 0, 2, 2, 6, 10, 56, 112, 106, 167, 205, 157, 112, 91, 94, 115, 171, 165, 171, 136, 136, 49, 15, 5, 2, 2, 1, 1, 1, 14, 35, 113, 123, 104, 209, 212, 116, 97, 103, 139, 264, 216, 244, 160, 170, 79, 15, 2, 2, 2, 0, 3, 19, 116, 459, 856, 755, 698, 1012, 625, 866, 660, 511, 578, 604, 501, 299, 290, 278, 176, 29, 3, 3, 9, 3, 1, 4, 18, 248, 402, 366, 255, 563, 324, 554, 352, 337, 379, 546, 515, 292, 260, 294, 176, 42, 0, 0, 0, 2, 1, 4, 7, 276, 244, 317, 296, 700, 438, 483, 326, 349, 380, 422, 377, 252, 265, 300, 185, 23, 5, 1, 1, 2, 1, 3, 18, 332, 308, 414, 295, 505, 426, 449, 235, 371, 312, 485, 459, 226, 247, 192, 133, 12, 7, 2, 2, 4, 2, 5, 9, 31, 69, 95, 156, 225, 180, 138, 131, 112, 184, 206, 205, 181, 137, 141, 58, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 7, 7, 8, 5, 3, 20, 311, 282, 398, 266, 590, 570, 561, 332, 457, 381, 699, 569, 300, 148, 268, 211, 37, 3, 3, 3, 2, 2, 7, 20, 291, 329, 326, 298, 576, 616, 616, 280, 461, 394, 839, 611, 315, 294, 424, 275, 84]
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
#星期四
date_set = [{'date':{'$gte':datetime(1900,7,7),'$lt':datetime(1900,7,8)}},
            {'date':{'$gte':datetime(1900,7,14),'$lt':datetime(1900,7,15)}},
            {'date':{'$gte':datetime(1900,7,21),'$lt':datetime(1900,7,22)}},
            {'date':{'$gte':datetime(1900,7,28),'$lt':datetime(1900,7,29)}},
            {'date':{'$gte':datetime(1900,8,4),'$lt':datetime(1900,8,5)}},
            {'date':{'$gte':datetime(1900,8,11),'$lt':datetime(1900,8,12)}},
            {'date':{'$gte':datetime(1900,8,18),'$lt':datetime(1900,8,19)}},
            {'date':{'$gte':datetime(1900,8,25),'$lt':datetime(1900,8,26)}},
            {'date':{'$gte':datetime(1900,9,1),'$lt':datetime(1900,9,2)}},
            {'date':{'$gte':datetime(1900,9,8),'$lt':datetime(1900,9,9)}},
            {'date':{'$gte':datetime(1900,9,15),'$lt':datetime(1900,9,16)}},
            {'date':{'$gte':datetime(1900,9,22),'$lt':datetime(1900,9,23)}},
            {'date':{'$gte':datetime(1900,9,29),'$lt':datetime(1900,9,30)}},
            {'date':{'$gte':datetime(1900,10,6),'$lt':datetime(1900,10,7)}},
            {'date':{'$gte':datetime(1900,10,13),'$lt':datetime(1900,10,14)}},
            {'date':{'$gte':datetime(1900,10,20),'$lt':datetime(1900,10,21)}},
            {'date':{'$gte':datetime(1900,10,27),'$lt':datetime(1900,10,28)}}
            #{'date':{'$gte':datetime(1900,10,31),'$lt':datetime(1900,11,1)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,7,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,14,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,21,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,28,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,4,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,11,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,18,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,25,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,1,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,8,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,15,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,22,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,29,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,6,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,13,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,20,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,27,i_1)]=0
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
plt.xlabel('日期')
plt.ylabel('人数')
plt.title('位置1的人数分布情况')
#name=(0704,0711,)
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()