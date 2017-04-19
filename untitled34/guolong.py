from config import *
from datetime import datetime
f1 = col_loudou.find({'os': 'ios'})
f2 = open('guo_long.csv', 'w')
count = 0
liu = set()

for i in f1:
    count_video = 0
    count_topic = 0
    if 'valid_user' in i:
        if i['valid_user'] is True:
            if 'tmp' in i:
                if datetime.fromtimestamp(i['clickCCSOpenCoach']/1000) < datetime(2017, 4, 3):
                    if len(i['tmp']['enterVideo']) != 0 and len(i['tmp']['clickKernelTopic']) != 0:
                        for i_1 in i['tmp']['enterVideo']:
                            if datetime.fromtimestamp(i_1/1000) < datetime(2017, 4, 3):
                                count_video += 1
                        for i_1 in i['tmp']['clickKernelTopic']:
                            if datetime.fromtimestamp(i_1 / 1000) < datetime(2017, 4, 3):
                                count_topic += 1
                        if count_video != 0:
                            if count_topic/count_video >= 0.8055:
                                liu.add(i['user'])
                                count += 1
                            # f2.write(i['user'])
                            # f2.write('\n')
f3 = open('result(5).csv', 'r')
guo = set(f3.read().split('\n'))
print(len(liu - guo))
for i in liu-guo:
    print(i)
    f2.write(str(i))
    f2.write('\n')
f3.close()
print(count)
f2.close()
