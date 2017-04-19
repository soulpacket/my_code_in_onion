# /Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5 /Users/root_1/PycharmProjects/untitled13/2.py
# 321
#a=[11, 2, 4, 5, 5, 6, 26, 47, 103, 133, 198, 225, 236, 150, 181, 157, 150, 248, 276, 273, 229, 223, 164, 52, 4, 3, 0, 1, 0, 4, 31, 109, 230, 216, 186, 298, 287, 250, 218, 179, 206, 321, 319, 283, 254, 207, 128, 37, 5, 1, 0, 0, 1, 0, 8, 29, 84, 121, 134, 183, 194, 159, 135, 163, 176, 246, 223, 221, 204, 180, 59, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 3, 13, 65, 153, 163, 182, 223, 173, 97, 76, 106, 93, 139, 200, 167, 119, 134, 62, 9, 2, 2, 3, 3, 1, 6, 12, 34, 94, 121, 116, 141, 171, 101, 110, 52, 91, 237, 206, 213, 160, 145, 56, 5, 0, 0, 5, 3, 0, 1, 11, 27, 81, 138, 142, 149, 134, 137, 139, 141, 152, 163, 165, 152, 124, 96, 52, 6, 13, 3, 3, 3, 3, 2, 19, 82, 174, 229, 193, 293, 265, 207, 197, 186, 215, 340, 332, 324, 229, 221, 107, 13, 76, 1, 2, 1, 3, 3, 11, 60, 170, 174, 178, 222, 233, 185, 186, 189, 196, 285, 256, 203, 215, 219, 166, 40, 5, 3, 2, 1, 11, 9, 18, 58, 164, 168, 180, 297, 250, 190, 166, 195, 233, 295, 279, 124, 94, 59, 96, 11, 6, 1, 0, 0, 0, 1, 12, 32, 87, 139, 147, 240, 193, 157, 133, 94, 57, 77, 75, 20, 72, 36, 86, 0, 6, 3, 1, 2, 2, 2, 13, 39, 69, 107, 121, 155, 151, 120, 95, 131, 101, 162, 192, 119, 111, 156, 53, 17, 2, 2, 1, 2, 2, 3, 9, 69, 160, 137, 258, 367, 241, 202, 233, 224, 210, 394, 436, 286, 273, 209, 157, 38, 27, 13, 8, 9, 8, 13, 83, 289, 847, 939, 1116, 1304, 1236, 839, 804, 920, 1041, 988, 519, 385, 268, 213, 463, 49, 5, 5, 8, 4, 2, 11, 19, 167, 174, 266, 256, 374, 388, 364, 181, 220, 344, 276, 331, 310, 199, 229, 150, 32]
# 408
# /Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/matplotlib/axes/_axes.py:531: UserWarning: No labelled objects found. Use label='...' kwarg on individual plots.
#   warnings.warn("No labelled objects found. "
# /Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/matplotlib/tight_layout.py:222: UserWarning: tight_layout : falling back to Agg renderer
#   warnings.warn("tight_layout : falling back to Agg renderer")

# Process finished with exit code 0

from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
db = client.run
coll = db.blog
#星期二
date_set = [{'date':{'$gte':datetime(1900,7,5),'$lt':datetime(1900,7,6)}},
            {'date':{'$gte':datetime(1900,7,12),'$lt':datetime(1900,7,13)}},
            {'date':{'$gte':datetime(1900,7,19),'$lt':datetime(1900,7,20)}},
            {'date':{'$gte':datetime(1900,7,26),'$lt':datetime(1900,7,27)}},
            {'date':{'$gte':datetime(1900,8,2),'$lt':datetime(1900,8,3)}},
            {'date':{'$gte':datetime(1900,8,9),'$lt':datetime(1900,8,10)}},
            {'date':{'$gte':datetime(1900,8,16),'$lt':datetime(1900,8,17)}},
            {'date':{'$gte':datetime(1900,8,23),'$lt':datetime(1900,8,24)}},
            {'date':{'$gte':datetime(1900,8,30),'$lt':datetime(1900,8,31)}},
            {'date':{'$gte':datetime(1900,9,6),'$lt':datetime(1900,9,7)}},
            {'date':{'$gte':datetime(1900,9,13),'$lt':datetime(1900,9,14)}},
            {'date':{'$gte':datetime(1900,9,20),'$lt':datetime(1900,9,21)}},
            {'date':{'$gte':datetime(1900,9,27),'$lt':datetime(1900,9,28)}},
            {'date':{'$gte':datetime(1900,10,4),'$lt':datetime(1900,10,5)}},
            {'date':{'$gte':datetime(1900,10,11),'$lt':datetime(1900,10,12)}},
            {'date':{'$gte':datetime(1900,10,18),'$lt':datetime(1900,10,19)}},
            {'date':{'$gte':datetime(1900,10,25),'$lt':datetime(1900,10,26)}}
            #{'date':{'$gte':datetime(1900,10,31),'$lt':datetime(1900,11,1)}}
          ]
#dateMonday = [datetime(1900,7,4),datetime(1900,7,4)]
dict_1 = OrderedDict()
for i_1 in range(24):
    dict_1[datetime(1900,7,5,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,12,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,19,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,7,26,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,2,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,9,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,16,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,23,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,8,30,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,6,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,13,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,20,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,9,27,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,4,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,11,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,18,i_1)]=0
for i_1 in range(24):
    dict_1[datetime(1900,10,25,i_1)]=0
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
plt.xlabel('星期2')
plt.ylabel('人数')
plt.title('位置1的人数分布情况')
plt.xticks(z, tuple(z))#前面是标记的位置,后面是标记的名字
#plt.ylim(0,40)
plt.legend()
plt.tight_layout()
plt.show()
#print(list_1[])