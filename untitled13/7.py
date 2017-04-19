# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/7.py
# 334
# [23, 12, 10, 3, 3, 2, 21, 96, 249, 281, 251, 400, 272, 276, 301, 276, 199, 471, 376, 294, 243, 227, 154, 40, 5, 1, 1, 1, 1, 4, 12, 70, 290, 232, 196, 373, 241, 326, 317, 280, 266, 398, 282, 636, 918, 738, 427, 55, 8, 0, 1, 1, 0, 6, 16, 64, 226, 181, 243, 372, 261, 210, 207, 181, 207, 324, 295, 249, 225, 158, 139, 31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 2, 3, 2, 2, 16, 37, 114, 109, 96, 152, 79, 60, 56, 61, 66, 123, 158, 110, 146, 119, 66, 20, 6, 1, 2, 3, 1, 6, 18, 47, 125, 143, 147, 192, 205, 132, 137, 123, 114, 159, 208, 176, 161, 141, 72, 15, 8, 3, 4, 4, 3, 4, 10, 31, 96, 139, 131, 150, 161, 87, 87, 77, 51, 120, 132, 103, 63, 97, 33, 8, 1, 3, 4, 2, 3, 6, 21, 495, 469, 333, 309, 512, 419, 486, 459, 391, 283, 244, 168, 76, 83, 76, 94, 22, 4, 2, 3, 3, 2, 3, 52, 279, 401, 389, 320, 513, 451, 522, 324, 350, 305, 530, 398, 259, 308, 207, 155, 30, 5, 3, 2, 2, 3, 3, 12, 211, 274, 336, 316, 513, 387, 406, 318, 341, 344, 440, 401, 219, 215, 228, 128, 36, 3, 4, 1, 1, 4, 2, 7, 33, 97, 117, 109, 164, 188, 122, 87, 105, 97, 153, 135, 126, 126, 152, 75, 19, 7, 2, 2, 1, 1, 5, 15, 220, 296, 444, 195, 445, 324, 373, 233, 376, 198, 445, 441, 234, 236, 251, 149, 31, 7, 4, 4, 4, 4, 6, 15, 257, 286, 409, 569, 578, 704, 611, 351, 551, 445, 842, 785, 666, 774, 356, 201, 82, 12, 7, 7, 3, 6, 2, 20, 230, 295, 355, 283, 501, 434, 459, 262, 393, 333, 713, 498, 227, 268, 239, 192, 59, 8, 6, 4, 3, 3, 5, 16, 263, 329, 454, 400, 682, 539, 552, 279, 530, 438, 716, 579, 289, 308, 357, 220, 52]
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
#星期一
date_set = [{'date':{'$gte':datetime(1900,7,3),'$lt':datetime(1900,7,4)}},
            {'date':{'$gte':datetime(1900,7,10),'$lt':datetime(1900,7,11)}},
            {'date':{'$gte':datetime(1900,7,17),'$lt':datetime(1900,7,18)}},
            {'date':{'$gte':datetime(1900,7,24),'$lt':datetime(1900,7,25)}},
            {'date':{'$gte':datetime(1900,7,31),'$lt':datetime(1900,8,1)}},
            {'date':{'$gte':datetime(1900,8,7),'$lt':datetime(1900,8,8)}},
            {'date':{'$gte':datetime(1900,8,14),'$lt':datetime(1900,8,15)}},
            {'date':{'$gte':datetime(1900,8,21),'$lt':datetime(1900,8,22)}},
            {'date':{'$gte':datetime(1900,8,28),'$lt':datetime(1900,8,29)}},
            {'date':{'$gte':datetime(1900,9,4),'$lt':datetime(1900,9,5)}},
            {'date':{'$gte':datetime(1900,9,11),'$lt':datetime(1900,9,12)}},
            {'date':{'$gte':datetime(1900,9,18),'$lt':datetime(1900,9,19)}},
            {'date':{'$gte':datetime(1900,9,25),'$lt':datetime(1900,9,26)}},
            {'date':{'$gte':datetime(1900,10,2),'$lt':datetime(1900,10,3)}},
            {'date':{'$gte':datetime(1900,10,9),'$lt':datetime(1900,10,10)}},
            {'date':{'$gte':datetime(1900,10,16),'$lt':datetime(1900,10,17)}},
            {'date':{'$gte':datetime(1900,10,23),'$lt':datetime(1900,10,24)}},
            {'date':{'$gte':datetime(1900,10,30),'$lt':datetime(1900,10,31)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,3,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,10,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,17,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,24,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,31,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,7,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,14,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,21,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,28,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,4,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,11,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,18,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,25,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,2,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,9,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,16,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,23,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,30,i_1)]=0
a = coll.aggregate([{'$match':{'location':'01','$or':date_set}},
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
plt.xlabel('7')
plt.ylabel('人数')
plt.title('位置1的人数分布情况')
#name=(0704,0711,)
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()