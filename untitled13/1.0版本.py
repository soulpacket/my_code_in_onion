#v1.0 102 v1,1 82 v1.2明天交
from pymongo import MongoClient
client = MongoClient('localhost',27017)
from datetime import datetime
from datetime import timedelta
import numpy as np
#import matplotlib.pyplot as plt
from collections import OrderedDict
db = client.run
coll = db.blog
file1 = open('v1.2(2.0suiyi).csv','w')
list_2=['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22',
        '23','24','25','26','27','28','29','30','31','32','33','34','35','36']
list_3=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
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
for ii in list_2:
    count_3+=1
    print(count_3)
    dict_1 = OrderedDict()
    a = datetime(1900,10,24,0)
    for i in range(168):
        dict_1[a]=0
        a += timedelta(hours=1)
        #print(a)
    b = coll.aggregate([{'$match':{'location':ii,'date':{'$gte':datetime(1900,10,24),'$lt':datetime(1900,10,31)}}},
                        {'$group':{'_id':'$date','count':{'$sum':1}}}
                        ])
    for i_1 in b:
        if i_1['_id'] in dict_1:
            dict_1[i_1['_id']]=i_1['count']
    #输出列表
    list_1=[]
    for i in dict_1:
        #print(dict_1[i])
        list_1.append(dict_1[i])
    # print(list_1)
    # print(len(list_1))#24*18=432
    #count_1=0
    for y1 in list_one:
        count_1=0
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11'+y1+y2)
            file1.write(',')
            #count_2=0
            if count_2<=17 or count_2>=22:
                file1.write(str(list_1[count_1]))
            else :
                file1.write(str(list_1[count_1]-10))
            # if count_2 <= 6 or count_2 >= 22:
            #     file1.write(str(list_1[count_1]))
            # elif count_2 > 6 and count_2 < 17:
            #     file1.write(str(list_1[count_1] + 10))
            # else:
            #     file1.write(str(list_1[count_1] - 15))
            count_2+=1
            file1.write('\n')

            count_1+=1
    for y1 in list_two:
        count_1 = 24
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_three:
        count_1 = 48
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_four:
        count_1 = 72
        count_2 = 0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_five:
        count_1 = 96
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_six:
        count_1 = 120
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
    for y1 in list_seven:
        count_1 = 144
        count_2=0
        for y2 in list_3:
            file1.write(ii)
            file1.write(',')
            file1.write('11' + y1 + y2)
            file1.write(',')
            #count_2 = 0
            if count_2 <= 17 or count_2 >= 22:
                file1.write(str(list_1[count_1]))
            else:
                file1.write(str(list_1[count_1] - 10))
            count_2 += 1
            file1.write('\n')
            count_1 += 1
file1.close()

