from pymongo import MongoClient
from datetime import datetime
#from sklearn.metrics import roc_auc_score
client=MongoClient('10.8.8.111',27017)
db=client.userValue
collection=db.cache
time='201609'
label=[]
score=[]
a=collection.aggregate([{'$match':{time:{'$exists':True}}}])
for i in a:
    score.append(i[time]['score'])
    if(i[time]['vip']==True):
        label.append(1)
    else:
        label.append(0)
num_yuzhi=0
num_zong=0
num_vip=0
for i in range(len(label)):
    if(score[i]>=3):
        num_zong=num_zong+1
        if(label[i]==1):
            num_yuzhi=num_yuzhi+1
    if(label[i]==1):
        num_vip=num_vip+1
print(num_zong)
print(num_yuzhi)
print(num_vip)
print(num_yuzhi/num_vip)
print(num_yuzhi/num_zong)


