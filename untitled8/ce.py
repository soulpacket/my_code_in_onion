from pymongo import MongoClient
import numpy as np
from datetime import datetime
client=MongoClient('10.8.8.111',27017)
db=client.userValue
collection=db.cache
a=collection.aggregate([{'$match':{'201609':{'$exists':True}}}])
dataMat=[]
labelMat=[]
for i in a:
    dataMat.append([1.0,i['201609']['startVideo'],i['201609']['finishVideo'],i['201609']['startPractice'],
                    i['201609']['enterTopicFinish'],i['201609']['days']])
    if(i['201609']['vip']==True):
        labelMat.append(int(1))
    else:
        labelMat.append(int(0))
def sigmoid(inX):
    #A = 1.0 / (1.0 + np.exp(-inX))
    # list.append(A)
    return 1.0 / (1.0 + np.exp(-inX))
dataMatrix=np.mat(dataMat)
labelMat=np.mat(labelMat).transpose()
m,n=np.shape(dataMatrix)
alpha=0.001
maxCycles=500
weights=np.ones((n,1))
print(n)
#for k in range(maxCycles):
    #h = sigmoid(dataMatrix * weights[0:200000,0])
b=dataMatrix * weights
c=b[0:320000,:]
sigmoid(c)
 #   h2 = sigmoid(dataMatrix * weights[200001:360000,])
 #   h = np.vstack((h1, h2))
   # error = (labelMat - h)
    #weights = weights + alpha * dataMatrix.transpose() * error
#print(weights)