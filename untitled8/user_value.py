def start(training_time,test_time):
    import math
    import numpy as np
    from pymongo import MongoClient
    client=MongoClient('10.8.8.111',27017)
    db=client.userValue
    collection=db.cache_pre
    #training_time='201610'
    a=collection.aggregate([{'$match':{training_time:{'$exists':True}}}])
    dataMat=[]
    labelMat=[]
    for i in a:
        dataMat.append([1.0,float(i[training_time]['startVideo']),float(i[training_time]['finishVideo']),float(i[training_time]['startPractice']),
                       float(i[training_time]['enterTopicFinish']),float(i[training_time]['days'])])
        if(i[training_time]['vip']==True):
            labelMat.append(1)
        else:
            labelMat.append(0)
    def sigmoid(inX):
        return 1.0/(1.0+np.exp(-inX))
    dataMatrix=np.mat(dataMat)
    labelMat=np.mat(labelMat).transpose()
    m,n=np.shape(dataMatrix)
    alpha=0.001
    maxCycles=280
    weights=np.ones((n,1))
    #print(n)
    r=20000
    block=math.ceil(len(labelMat)/r)
    print(block)
    for k in range(maxCycles):
        b=dataMatrix * weights
        j=r
        #h=np.mat([])
        if(block==1):
            h=sigmoid(b[0:,:])
            #print('hh')
        else:
            h=sigmoid(b[0:r,:])
            for i in range(block-1):
                if(i==block-2):
                    h1=sigmoid(b[j:,:])
                    h=np.vstack((h,h1))
                else:
                    h1=sigmoid(b[j:j+r,:])
                    j=j+r
                    h=np.vstack((h,h1))
        error = (labelMat-h)
        # print(error.mean(),end=',')
        # print(k)
        weights = weights+alpha * dataMatrix.transpose()*error
    print(weights)
    # b=collection.aggregate([{'$match':{test_time:{'$exists':True}}}])
    # for i in b:
    #     c=float(weights[0][0])\
    #     +float(weights[1][0])*float(i[test_time]['startVideo'])\
    #     +float(weights[2][0])*float(i[test_time]['finishVideo'])\
    #     +float(weights[3][0])*float(i[test_time]['startPractice'])\
    #     +float(weights[4][0])*float(i[test_time]['enterTopicFinish'])\
    #     +float(weights[5][0])*float(i[test_time]['days'])
    #     d = 1 / (1 + math.exp(-c))#价值
    #     #print(d)
    #     #collection.update_one({"user": i['user']}, {"$set": {test_time:{"value":d}}},True,False)
    #     collection.update_one({'user': i['user']}, {"$set": {test_time+'.value': d}})