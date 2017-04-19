from pymongo import MongoClient
from datetime import datetime
client=MongoClient('10.8.8.111',27017)
db=client.eventsV4
collection=db.eventV4
#for i in collection.find({"type":"A","user":"543e9209c8cb2d6e1ea0ee01","eventKey":"startVideo"}):
 #   print(i)
#print (collection.find({"type":"A","user":"543e9209c8cb2d6e1ea0ee01","eventKey":"startVideo"}).count())
#print(collection.find({"eventTime":{'$lt':1474300800.0},"eventTime":{'$gt':1473696000.0}}).count())
#for i in range(5):
a=collection.find({"serverTime":{'$lt':datetime.fromtimestamp(1474300800.0),
                                 '$gt':datetime.fromtimestamp(1473696000.0),
                                 },'$or':[{'eventKey':'startVideo'},{'eventKey':'startPractice'}],
                   "eventTime":{'$gt':1}
                   }).sort('eventTime',1)#这个地方用eventtime比较准确
#1473647646316.99
#print(a.count())
print("kaishi")
num=0
dict_A={}
dict_B={}
dict_C={}
dict_D={}
dict_E={}
for i in a:
    num = num + 1
    print(num)
    if(num==1274174):
        break
    if ('topicId' in i):
        # print(i['serverTime'])
        if ('type' in i):
            if ('user' in i):
               # print(i)
                if (i['type'] == 'A'):
                    if (i['user']+','+i['topicId'] in dict_A):
                        if (i['eventKey'] == 'startVideo'):
                            dict_A[i['user']+','+i['topicId']] = dict_A[i['user']+','+i['topicId']] + 1
                    else:
                        dict_A[i['user']+','+ i['topicId']] = 0
                elif (i['type'] == 'B'):
                    if (i['user']+','+i['topicId'] in dict_B):
                        if (i['eventKey'] == 'startVideo'):
                            dict_B[i['user']+','+i['topicId']] = dict_B[i['user']+','+i['topicId']] + 1
                    else:
                        dict_B[i['user']+','+i['topicId']] = 0
                elif (i['type'] == 'C'):
                    if (i['user']+','+i['topicId'] in dict_C):
                        if (i['eventKey'] == 'startVideo'):
                            dict_C[i['user']+','+i['topicId']] = dict_C[i['user']+','+i['topicId']] + 1
                    else:
                        dict_C[i['user']+','+i['topicId']] = 0
                elif (i['type'] == 'D'):
                    if (i['user']+','+i['topicId'] in dict_D):
                        if (i['eventKey'] == 'startVideo'):
                            dict_D[i['user']+','+i['topicId']] = dict_D[i['user']+','+i['topicId']] + 1
                    else:
                        dict_D[i['user']+','+i['topicId']] = 0
                if (i['type'] == 'E'):
                    if (i['user']+','+i['topicId'] in dict_E):
                        if (i['eventKey'] == 'startVideo'):
                            dict_E[i['user']+','+i['topicId']] = dict_E[i['user']+','+i['topicId']] + 1
                    else:
                        dict_E[i['user']+','+i['topicId']] = 0


per_A=[]
per_B=[]
per_C=[]
per_D=[]

per_E=[]
g_A1=[]
g_A2=[]
g_A3=[]
g_A4=[]
g_A5=[]
g_B1=[]
g_B2=[]
g_B3=[]
g_B4=[]
g_B5=[]

g_C1=[]
g_C2=[]
g_C3=[]
g_C4=[]
g_C5=[]

g_D1=[]
g_D2=[]
g_D3=[]
g_D4=[]
g_D5=[]

g_E1=[]
g_E2=[]
g_E3=[]
g_E4=[]
g_E5=[]
print("A类型总人数:",end='')
#print(len(dict_A))
for i in dict_A:
    g_A=i.split(',')[0]
    if(dict_A[i]==1):
        g_A1.append(g_A)
    elif(dict_A[i]==2):
        g_A2.append(g_A)
    elif(dict_A[i]==3):
        g_A3.append(g_A)
    elif(dict_A[i]==4):
        g_A4.append(g_A)
    elif(dict_A[i]>=5):
        g_A5.append(g_A)
    if (dict_A[i] >= 1):
      #  g=i.split(',')[0]
        per_A.append(g_A)
print("本周重访人数:",end='')
print(len(set(per_A)))
print(len(set(g_A1)),end=',')
print(len(set(g_A2)),end=',')
print(len(set(g_A3)),end=',')
print(len(set(g_A4)),end=',')
print(len(set(g_A5)))

#print("B类型总人数:",end='')
#print(len(dict_B))
for i in dict_B:
    g_B=i.split(',')[0]
    if(dict_B[i]==1):
        g_B1.append(g_B)
    elif(dict_B[i]==2):
        g_B2.append(g_B)
    elif(dict_B[i]==3):
        g_B3.append(g_B)
    elif(dict_B[i]==4):
        g_B4.append(g_B)
    elif(dict_B[i]>=5):
        g_B5.append(g_B)
    if(dict_B[i]>=1):
        #g = i.split(',')[0]
        per_B.append(g_B)
print("本周重访人数:",end='')
print(len(set(per_B)))
print(len(set(g_B1)),end=',')
print(len(set(g_B2)),end=',')
print(len(set(g_B3)),end=',')
print(len(set(g_B4)),end=',')
print(len(set(g_B5)))


for i in dict_C:
    g_C=i.split(',')[0]
    if(dict_C[i]==1):
        g_C1.append(g_C)
    elif(dict_C[i]==2):
        g_C2.append(g_C)
    elif(dict_C[i]==3):
        g_C3.append(g_C)
    elif(dict_C[i]==4):
        g_C4.append(g_C)
    elif(dict_C[i]>=5):
        g_C5.append(g_C)
    if (dict_C[i] >= 1):
        #g = i.split(',')[0]
        per_C.append(g_C)
print("本周重访人数:",end='')
print(len(set(per_C)))
print(len(set(g_C1)),end=',')
print(len(set(g_C2)),end=',')
print(len(set(g_C3)),end=',')
print(len(set(g_C4)),end=',')
print(len(set(g_C5)))

# print("D类型总人数:",end='')
# print(len(dict_D))
for i in dict_D:
    g_D = i.split(',')[0]
    if(dict_D[i]==1):
        g_D1.append(g_D)
    elif(dict_D[i]==2):
        g_D2.append(g_D)
    elif(dict_D[i]==3):
        g_D3.append(g_D)
    elif(dict_D[i]==4):
        g_D4.append(g_D)
    elif(dict_D[i]>=5):
        g_D5.append(g_D)
    if (dict_D[i] >= 1):
      #  g = i.split(',')[0]
        per_D.append(g_D)
print("本周重访人数:",end='')
print(len(set(per_D)))
print(len(set(g_D1)),end=',')
print(len(set(g_D2)),end=',')
print(len(set(g_D3)),end=',')
print(len(set(g_D4)),end=',')
print(len(set(g_D5)))

# print("E类型总人数:",end='')
# print(len(dict_E))
for i in dict_E:
    g_E=i.split(',')[0]
    if(dict_E[i]==1):
        g_E1.append(g_E)
    elif(dict_E[i]==2):
        g_E2.append(g_E)
    elif(dict_E[i]==3):
        g_E3.append(g_E)
    elif(dict_E[i]==4):
        g_E4.append(g_E)
    elif(dict_E[i]>=5):
        g_E5.append(g_E)
    if (dict_E[i] >= 1):
     #   g = i.split(',')[0]
        per_E.append(g_E)
print("本周重访人数:",end='')
print(len(set(per_E)))
print(len(set(g_E1)),end=',')
print(len(set(g_E2)),end=',')
print(len(set(g_E3)),end=',')
print(len(set(g_E4)),end=',')
print(len(set(g_E5)))





#print (num)




