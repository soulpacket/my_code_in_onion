from pymongo import MongoClient
from bson.objectid import ObjectId
import re
client = MongoClient('10.8.8.111',27017)
db = client.removeAfterUse
coll = db.users0914
file1=open('id','r')
file2=open('scoreeeell.txt','w')
#file3=open('levellll.txt','w')
line = file1.readline()
line_copy='a'
num=0
while line:
    num=num+1
    line = file1.readline()
    if line_copy!=line:
        line_copy = line
        line_1 = re.findall('"(.*?)"', line)[0]
        ID = ObjectId(line_1)
        #print(ID)
        c = coll.aggregate([{'$match':{'_id':ID}}])
        for i in c:
            for i_1 in range(0,6):
                file2.write(str(int(i['abilities'][i_1]['scores'])))
                file2.write('\n')
                # file3.write(str(int(i['abilities'][i_1]['level']['no'])))
                # file3.write('\n')
    print(num)
file2.close()
file1.close()


