# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/5.py
# 306
# [14, 12, 2, 2, 2, 2, 19, 94, 243, 350, 278, 400, 292, 262, 267, 319, 222, 317, 293, 285, 305, 253, 165, 54, 11, 17, 7, 3, 2, 8, 12, 149, 529, 313, 299, 660, 295, 467, 318, 267, 274, 464, 346, 318, 253, 253, 148, 32, 6, 2, 0, 1, 1, 8, 20, 120, 455, 375, 268, 456, 354, 356, 256, 203, 234, 446, 392, 284, 268, 251, 191, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 1, 1, 2, 5, 14, 115, 166, 170, 147, 227, 194, 160, 110, 104, 137, 179, 183, 163, 171, 129, 65, 14, 5, 1, 1, 1, 1, 2, 8, 67, 126, 172, 176, 225, 184, 166, 126, 110, 149, 220, 199, 166, 144, 146, 57, 14, 5, 3, 2, 3, 4, 3, 15, 44, 134, 180, 161, 224, 213, 149, 123, 99, 132, 187, 211, 212, 139, 152, 63, 15, 5, 1, 3, 3, 2, 10, 82, 420, 608, 541, 540, 822, 433, 592, 407, 414, 461, 533, 378, 239, 195, 203, 152, 37, 5, 1, 1, 2, 1, 2, 54, 274, 421, 415, 291, 602, 318, 558, 312, 317, 374, 555, 453, 319, 297, 206, 151, 26, 5, 4, 2, 1, 2, 3, 20, 270, 282, 371, 440, 566, 341, 513, 291, 354, 391, 458, 435, 237, 273, 231, 167, 29, 4, 1, 0, 0, 0, 1, 20, 275, 247, 449, 311, 378, 562, 415, 259, 412, 193, 448, 312, 113, 138, 102, 77, 15, 4, 2, 0, 1, 0, 2, 12, 20, 72, 112, 122, 168, 200, 144, 152, 140, 160, 220, 228, 197, 140, 165, 71, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 5, 5, 2, 2, 1, 16, 283, 307, 372, 316, 528, 601, 622, 151, 396, 392, 687, 481, 226, 267, 292, 202, 24, 7, 2, 3, 2, 3, 4, 17, 290, 362, 398, 401, 595, 605, 731, 338, 459, 465, 686, 654, 291, 363, 399, 249, 62]
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
#星期五
date_set = [{'date':{'$gte':datetime(1900,7,1),'$lt':datetime(1900,7,2)}},
            {'date':{'$gte':datetime(1900,7,8),'$lt':datetime(1900,7,9)}},
            {'date':{'$gte':datetime(1900,7,15),'$lt':datetime(1900,7,16)}},
            {'date':{'$gte':datetime(1900,7,22),'$lt':datetime(1900,7,23)}},
            {'date':{'$gte':datetime(1900,7,29),'$lt':datetime(1900,7,30)}},
            {'date':{'$gte':datetime(1900,8,5),'$lt':datetime(1900,8,6)}},
            {'date':{'$gte':datetime(1900,8,12),'$lt':datetime(1900,8,13)}},
            {'date':{'$gte':datetime(1900,8,19),'$lt':datetime(1900,8,20)}},
            {'date':{'$gte':datetime(1900,8,26),'$lt':datetime(1900,8,27)}},
            {'date':{'$gte':datetime(1900,9,2),'$lt':datetime(1900,9,3)}},
            {'date':{'$gte':datetime(1900,9,9),'$lt':datetime(1900,9,10)}},
            {'date':{'$gte':datetime(1900,9,16),'$lt':datetime(1900,9,17)}},
            {'date':{'$gte':datetime(1900,9,23),'$lt':datetime(1900,9,24)}},
            {'date':{'$gte':datetime(1900,9,30),'$lt':datetime(1900,10,1)}},
            {'date':{'$gte':datetime(1900,10,7),'$lt':datetime(1900,10,8)}},
            {'date':{'$gte':datetime(1900,10,14),'$lt':datetime(1900,10,15)}},
            {'date':{'$gte':datetime(1900,10,21),'$lt':datetime(1900,10,22)}},
            {'date':{'$gte':datetime(1900,10,28),'$lt':datetime(1900,10,29)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,1,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,8,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,15,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,22,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,29,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,5,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,12,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,19,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,26,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,2,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,9,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,16,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,23,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,30,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,7,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,14,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,21,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,28,i_1)]=0
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