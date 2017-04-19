from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
from datetime import timedelta
import numpy as np
#import matplotlib.pyplot as plt
from collections import OrderedDict
db = client.run
coll = db.blog
file1 = open('v2.3.csv','w')
list_2a=['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22',
        '23','24','25','26','27','28','29','30','31','32','33','34','35','36']
list_3a=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
# list_4=['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22',
#         '23','24','25','26','27','28','29','30']
list_two=['01','08','15','22','29']
list_three=['02','09','16','23','30']
list_four=['03','10','17','24']
list_five=['04','11','18','25']
list_six=['05','12','19','26']
list_seven=['06','13','20','27']
list_one=['07','14','21','28']
count_3=0
for ii in list_2a:
    count_3+=1
    print(count_3)
    #1.
    dict_1 = OrderedDict()
    a = datetime(1900,10,26,0)
    for i in range(24):
        dict_1[a]=0
        a += timedelta(hours=1)
        #print(a)
    b_1 = coll.aggregate([{'$match':{'location':ii,'date':{'$gte':datetime(1900,10,26),'$lt':datetime(1900,10,27)}}},
                        {'$group':{'_id':'$date','count':{'$sum':1}}}
                        ])
    for i_1 in b_1:
        if i_1['_id'] in dict_1:
            dict_1[i_1['_id']]=i_1['count']
    #输出列表
    list_1=[]
    for i in dict_1:
        list_1.append(dict_1[i])
    #1.
    #2.
    dict_2 = OrderedDict()
    a = datetime(1900, 10, 10, 0)
    for i in range(48):
        dict_2[a] = 0
        a += timedelta(hours=1)
        # print(a)
    b_2 = coll.aggregate(
        [{'$match': {'location': ii, 'date': {'$gte': datetime(1900, 10, 10), '$lt': datetime(1900, 10, 12)}}},
         {'$group': {'_id': '$date', 'count': {'$sum': 1}}}
         ])
    for i_1 in b_2:
        if i_1['_id'] in dict_2:
            dict_2[i_1['_id']] = i_1['count']
    # 输出列表
    list_2 = []
    for i in dict_2:
        list_2.append(dict_2[i])
    #2.

    # #3.
    # dict_3 = OrderedDict()
    # a = datetime(1900, 9, 19, 0)
    # for i in range(168):
    #     dict_3[a] = 0
    #     a += timedelta(hours=1)
    #     # print(a)
    # b_3 = coll.aggregate(
    #     [{'$match': {'location': ii, 'date': {'$gte': datetime(1900, , 19), '$lt': datetime(1900, 9, 26)}}},
    #      {'$group': {'_id': '$date', 'count': {'$sum': 1}}}
    #      ])
    # for i_1 in b_3:
    #     if i_1['_id'] in dict_3:
    #         dict_3[i_1['_id']] = i_1['count']
    # # 输出列表
    # list_3 = []
    # for i in dict_3:
    #     list_3.append(dict_3[i])
    #3.
    #4.
    dict_4 = OrderedDict()
    a = datetime(1900, 10, 20, 0)
    for i in range(96):
        dict_4[a] = 0
        a += timedelta(hours=1)
        # print(a)
    b_4 = coll.aggregate(
        [{'$match': {'location': ii, 'date': {'$gte': datetime(1900, 10, 20), '$lt': datetime(1900, 10, 24)}}},
         {'$group': {'_id': '$date', 'count': {'$sum': 1}}}
         ])
    for i_1 in b_4:
        if i_1['_id'] in dict_4:
            dict_4[i_1['_id']] = i_1['count']
    # 输出列表
    list_4 = []
    for i in dict_4:
        list_4.append(dict_4[i])
    #4.
    list_5=[]
    for  i in range(168):
        list_5.append(0)
    for i in range(48):
        list_5[i]=int(list_2[i])
    for i in range(24):
        list_5[i+48]=int(list_1[i])
    for i in range(96):
        list_5[i+72]=int(list_4[i])
    # print(list_1)
    for y1 in list_one:
        count_1=0
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11'+y1+y2)
            file1.write(',')
            #count_2=0
            file1.write(str(list_5[count_1]))
            file1.write('\n')

            count_1+=1
    for y1 in list_two:
        count_1 = 24
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            file1.write('\n')
            count_1 += 1
    for y1 in list_three:
        count_1 = 48
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_four:
        count_1 = 72
        count_2 = 0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            file1.write('\n')
            count_1 += 1
    for y1 in list_five:
        count_1 = 96
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            file1.write('\n')
            count_1 += 1
    for y1 in list_six:
        count_1 = 120
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            file1.write('\n')
            count_1 += 1
    for y1 in list_seven:
        count_1 = 144
        count_2=0
        for y2 in list_3a:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            file1.write(str(list_5[count_1]))
            file1.write('\n')
            count_1 += 1
file1.close()
