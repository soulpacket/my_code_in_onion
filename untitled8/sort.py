from pymongo import MongoClient
import numpy as np
# a=[1,2,3]
# b=[2,3,4]
# c=(np.array(a)-np.array(b))**2
# #print(((np.array(a)-np.array(b))**2).sum(axis=1))
# d=c.sum(axis=0)
# print(d)
client=MongoClient('10.8.8.111',27017)
db=client.userValue
collection=db.cache
time_xun='201610'
time_test='201609'
data=[]
data_test=[]
label=[]
yuce=[]
num_count=0
a=collection.aggregate([{'$match':{time_xun:{'$exists':True}}}])
for i in a:
    if(i[time_xun]['vip']==True):
        data.append(
            [float(i[time_xun]['startVideo']), float(i[time_xun]['finishVideo']), float(i[time_xun]['startPractice']),
             float(i[time_xun]['enterTopicFinish']), float(i[time_xun]['days'])])
b=collection.aggregate([{'$match':{time_test:{'$exists':True}}}])
for i in b:
    num_count=num_count+1
    #print(num_count)
    data_test.append(
            [float(i[time_test]['startVideo']), float(i[time_test]['finishVideo']), float(i[time_test]['startPractice']),
             float(i[time_test]['enterTopicFinish']), float(i[time_test]['days'])])
    if (i[time_test]['vip'] == True):
        label.append(1)
    else:
        label.append(0)
for i in range(num_count):
    yuce.append(0)
count=0
for i in data:
    dataSet=np.array(data_test)
    inX=np.array(i)
    datasize=dataSet.shape[0]
    diffMat=np.tile(inX,(datasize,1))-dataSet
    sqMat=diffMat**2
    sqDis=sqMat.sum(axis=1)
    distance=sqDis**0.5
    sort=distance.argsort()
    for i_1 in range(200):
        yuce[sort[i_1]]=1
    count=count+1
    print(count)

num=0
for i in range(len(yuce)):
    if(yuce[i]==label[i]):
        num=num+1
print(num/len(yuce))#总的命中率
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
print(num/num_zong)#被归位有价值用户的vip占总VIP数
print(num/num_yucezong)#有价值用户里的VIP占比
print(num)#被预测成功的VIP数
print(num_zong)#总VIP数
print(num_yucezong)#有价值用户








