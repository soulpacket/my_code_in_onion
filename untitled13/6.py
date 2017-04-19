# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/6.py
# 325
# [9, 8, 0, 2, 1, 6, 31, 744, 473, 991, 547, 535, 375, 281, 305, 315, 282, 486, 347, 270, 266, 258, 192, 68, 8, 0, 1, 2, 2, 6, 16, 118, 484, 384, 274, 574, 340, 405, 291, 228, 273, 402, 305, 253, 298, 218, 156, 45, 2, 0, 0, 0, 1, 2, 9, 81, 360, 321, 208, 411, 345, 276, 349, 244, 226, 423, 348, 266, 243, 210, 172, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 1, 1, 3, 8, 60, 137, 124, 105, 203, 135, 100, 103, 79, 105, 163, 171, 181, 162, 162, 78, 13, 6, 2, 1, 1, 1, 4, 22, 65, 120, 155, 174, 223, 175, 143, 147, 114, 156, 215, 139, 200, 161, 152, 66, 15, 6, 4, 3, 4, 4, 6, 14, 42, 76, 104, 63, 142, 134, 110, 97, 83, 108, 164, 226, 171, 174, 173, 53, 24, 3, 3, 0, 0, 0, 2, 22, 276, 520, 491, 442, 801, 469, 564, 380, 267, 408, 480, 321, 188, 168, 182, 96, 12, 8, 4, 2, 1, 1, 2, 10, 251, 456, 380, 372, 666, 452, 504, 289, 405, 324, 536, 439, 259, 203, 158, 90, 14, 12, 3, 3, 3, 3, 5, 20, 287, 282, 342, 330, 600, 375, 472, 261, 372, 325, 588, 364, 190, 227, 224, 117, 20, 7, 3, 1, 2, 3, 2, 10, 42, 73, 107, 111, 161, 173, 82, 133, 109, 107, 173, 194, 150, 117, 125, 59, 14, 4, 2, 4, 2, 3, 2, 13, 230, 202, 295, 256, 459, 429, 488, 267, 490, 234, 544, 428, 218, 219, 204, 161, 24, 0, 0, 0, 3, 3, 4, 18, 265, 312, 483, 436, 632, 386, 473, 251, 445, 325, 788, 517, 358, 351, 269, 193, 52, 9, 4, 2, 3, 4, 6, 16, 250, 281, 315, 255, 476, 381, 457, 275, 480, 302, 755, 564, 284, 269, 305, 240, 69, 8, 4, 4, 4, 4, 7, 21, 312, 357, 393, 368, 589, 564, 548, 315, 431, 277, 738, 636, 378, 408, 356, 257, 72]
# 432
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
#星期六
date_set = [{'date':{'$gte':datetime(1900,7,2),'$lt':datetime(1900,7,3)}},
            {'date':{'$gte':datetime(1900,7,9),'$lt':datetime(1900,7,10)}},
            {'date':{'$gte':datetime(1900,7,16),'$lt':datetime(1900,7,17)}},
            {'date':{'$gte':datetime(1900,7,23),'$lt':datetime(1900,7,24)}},
            {'date':{'$gte':datetime(1900,7,30),'$lt':datetime(1900,7,31)}},
            {'date':{'$gte':datetime(1900,8,6),'$lt':datetime(1900,8,7)}},
            {'date':{'$gte':datetime(1900,8,13),'$lt':datetime(1900,8,14)}},
            {'date':{'$gte':datetime(1900,8,20),'$lt':datetime(1900,8,21)}},
            {'date':{'$gte':datetime(1900,8,27),'$lt':datetime(1900,8,28)}},
            {'date':{'$gte':datetime(1900,9,3),'$lt':datetime(1900,9,4)}},
            {'date':{'$gte':datetime(1900,9,10),'$lt':datetime(1900,9,11)}},
            {'date':{'$gte':datetime(1900,9,17),'$lt':datetime(1900,9,18)}},
            {'date':{'$gte':datetime(1900,9,24),'$lt':datetime(1900,9,25)}},
            {'date':{'$gte':datetime(1900,10,1),'$lt':datetime(1900,10,2)}},
            {'date':{'$gte':datetime(1900,10,8),'$lt':datetime(1900,10,9)}},
            {'date':{'$gte':datetime(1900,10,15),'$lt':datetime(1900,10,16)}},
            {'date':{'$gte':datetime(1900,10,22),'$lt':datetime(1900,10,23)}},
            {'date':{'$gte':datetime(1900,10,29),'$lt':datetime(1900,10,30)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,2,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,9,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,16,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,23,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,30,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,6,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,13,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,20,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,27,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,3,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,10,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,17,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,24,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,1,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,8,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,15,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,22,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,29,i_1)]=0
a = coll.aggregate([{'$match':{'location':'01','$or':date_set}},
                    {'$group':{'_id':'$date','count':{'$sum':1}}}
                    ])

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
plt.title('位置1的人数分布情况')
#name=(0704,0711,)
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()