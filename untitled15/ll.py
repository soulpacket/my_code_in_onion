from pymongo import MongoClient
client = MongoClient('10.8.8.111',27017)
db = client.cache
coll = db.userAttr
a = coll.aggregate([{'$match':{'location':{'$ne':None}}},
                    {'$project':{'location':1}}])
b=[]
file1=open('location.csv','w',encoding='utf-8')
# file2=open('zhuce.csv','w')
# file3=open('vip.csv','w')
num = 0
for i in a:
    #print(i)
    num+=1
    print(num)
    if 'location' in i:
        if i['location'][0:2]=='广东':
            if i['location'] not in b:
                b.append(i['location'])
                file1.write(i['location'])
                file1.write('\n')

print(b)
file1.close()
# file2.close()
# file3.close()