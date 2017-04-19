from pymongo import MongoClient
from datetime import datetime, timedelta
client=MongoClient('10.8.8.111',27017)
db_1=client.cache
collection_1=db_1.userAttr

START = datetime(2016,8,3)
while(START<=datetime(2016,10,12)):
    END = START + timedelta(days=7)
    a=collection_1.aggregate([{"$match":{'activateDate':{'$lt':START}}},
                              {"$project":{'daily':1}}
                              ])
    num1=0
    num2=0
    num3=0
    for i in a:
        #num2=num2+1
        if 'daily' in i:
            for i_1 in i['daily']:
                if(datetime.strptime(i_1,'%Y%m%d')>=START):
                    if(datetime.strptime(i_1,'%Y%m%d')<END):
                        num1=num1+1
                        num2=-1
        if(num2==-1):
            num3=num3+1
            num2=0
    print(num1)
    print(num3)
    print(num1/num3)
    START=START+timedelta(days=7)


