import math
from pymongo import MongoClient
client=MongoClient('10.8.8.111',27017)
db=client.userValue
#collection=db.cache
collection = db['cache']
time='201610'
a=collection.aggregate([{'$match':{time:{'$exists':True}}}])
data=[]
label=[]
score=[]
yuce=[]
count=0
count_vip=0
count_no=0
count_1, count_2 =(0, 0)
for i in a:
#     if(i[time]['vip']==True):
#
#         count_vip=count_vip+1
#         data.append(
#             [float(i[time]['startVideo']), float(i[time]['finishVideo']), float(i[time]['startPractice']),
#              float(i[time]['enterTopicFinish']), float(i[time]['days']), i['user']])
#         if (i[time]['vip'] == True):
#             label.append(1)
#         else:
#             label.append(0)
#     if count_vip==500:
#         break
#
# for i in a:
#     if (i[time]['vip'] == False):
#         count_no = count_no + 1
#         data.append(
#             [float(i[time]['startVideo']), float(i[time]['finishVideo']), float(i[time]['startPractice']),
#              float(i[time]['enterTopicFinish']), float(i[time]['days']), i['user']])
#         if (i[time]['vip'] == True):
#             label.append(1)
#         else:
#             label.append(0)
#     if count_no == 500:
#         break
    data.append(
        [ float(i[time]['startVideo']), float(i[time]['finishVideo']), float(i[time]['startPractice']),
         float(i[time]['enterTopicFinish']), float(i[time]['days']), i['user']])
    if (i[time]['vip'] == True):
        label.append(1)
    else:
        label.append(0)
    count=count+1
    # if count==20000:
    #     break
print(len(label))

for i in data:
    #c=-32.19+i[0]*10.39+i[1]*2.94+i[2]*0.42-i[3]*14.28+i[4]*35.13
    c = -2.20 + i[0] * 2.43 + i[1] * 0.04 + i[2] * 0.32 - i[3] * 1.31 + i[4] * 3.24
    #c=-7.69+i[0]*6.67+i[1]*3.68+i[2]*6.52-i[3]*5.78+i[4]*11.73
    d=1/(1+math.exp(-c))
    # score.append(d)
    # if label[data.index(i)]==0 and d==1:
    #     #print('hh')
    #     count_1=count_1+1
    #     print(count_1)
    #     #print("count1: %i" %count_1)
    # elif label[data.index(i)]==1 and d<1:
    #     count_2 = count_2 + 1
    #     print(count_2)
        #print("count2: %i, d: %.2f" %(count_2, d))

  #  collection.update_one({"user": i[-1]}, {"$set": {"valueM": d}})

    if(d>=0.7):
        yuce.append(1)
    else:
        yuce.append(0)
#print(count_1)
num=0
for i in range(len(yuce)):
    if(yuce[i]==label[i]):
        num=num+1
#print(num/len(yuce))#总的命中率
num=0
num_zong=0
num_yucezong=0
for i in range(len(yuce)):
    if(yuce[i]==1):
        num_yucezong=num_yucezong+1
for i in range(len(label)):
    if(label[i]==1):
        num_zong=num_zong+1
        if(yuce[i]==1):
            num=num+1
print('被归为有价值用户的vip占总VIP数: ',end='')
print(num/num_zong)#被归位有价值用户的vip占总VIP数
#print(num/num_yucezong)#有价值用户里的VIP占比
print('有价值用户里的VIP数: ',end='')
print(num)#被预测成功的VIP数
print('总VIP数: ',end='')
print(num_zong)#总VIP数
print('有价值人数: ',end='')
print(num_yucezong)#有价值用户
print('测试总人数: ',end='')
print(len(label))
print("有价值用户占总人数的百分比: ",end='')
print(num_yucezong/len(label))



