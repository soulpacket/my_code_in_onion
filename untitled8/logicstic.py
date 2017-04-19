import numpy as np
from pymongo import MongoClient
client=MongoClient('10.8.8.111',27017)
db=client.userValue
collection=db.cache
time='201610'
a=collection.aggregate([{'$match':{time:{'$exists':True}}}])
dataMat=[]
labelMat=[]
for i in a:
    dataMat.append([1.0,float(i[time]['startVideo']),float(i[time]['finishVideo']),float(i[time]['startPractice']),
                   float(i[time]['enterTopicFinish']),float(i[time]['days'])])
    if(i[time]['vip']==True):
        labelMat.append(float(1))
    else:
        labelMat.append(float(0))
def sigmoid(inX):
    return 1.0/(1.0+np.exp(-inX))
dataMatrix=np.mat(dataMat)
labelMat=np.mat(labelMat).transpose()
#labelMat=np.array(labelMat).transpose()
m,n=np.shape(dataMatrix)
alpha=0.001
maxCycles=1000
weights=np.ones((n,1))
print(n)
count=0
for k in range(maxCycles):
    count=count+1
    b=np.dot(dataMatrix , weights)
    h1 = sigmoid(b[0:50000,:])
    h2=sigmoid(b[50000:100000,:])
    h8=sigmoid(b[100000:150000,:])
    h3=sigmoid(b[150000:200000,:])
    h4=sigmoid(b[200000:250000,:])
    h5=sigmoid(b[250000:300000,:])
    h6=sigmoid(b[300000:350000,:])
    h7=sigmoid(b[350000:400000,:])
    h9=sigmoid(b[400000:450000,:])
    h10=sigmoid(b[450000:500000,:])
    h11=sigmoid(b[500000:550000,:])
    h12=sigmoid(b[550000:600000,:])
    h13=sigmoid(b[600000:650000,:])
    h14=sigmoid(b[650000:,:])
    h=np.vstack((h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11,h12,h13,h14))
    error = labelMat-h
    weights = weights+alpha * dataMatrix.transpose()*error
    #print(weights)
    if(count==1000):
        break
# for i in weights.transpose(item():
#     print(i)
print(weights)
# if weights[0][0]<0:
#     print('jj')
# rr=np.array([1,1,1,1,1,1])
# print(rr*weights)
# if(rr*weights>5):
#     print('j')
