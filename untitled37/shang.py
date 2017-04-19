from pymongo import MongoClient
from datetime import datetime
client = MongoClient('10.8.8.111', 27017)
db_1 = client.cache
db_2 = client.onions
col_userAttr = db_1.userAttr
col_school = db_2.schools
yun_school = set()
school = set()
f2 = col_school.find({'region0': '云南'})
for i in f2:
    yun_school.add(i['_id'])
f1 = col_userAttr.find({'location': {'$ne': None}})
student = 0
teacher = 0
active_user = 0
t2 = datetime(2017, 3, 9)
t3 = datetime(2017, 4, 9)
count = 0
for i in f1:
    count += 1
    print(count)
    if 'school' in i:
        school.add(i['school'])
    try:
        if len(i['location']) >= 2 or len(i['location']) >= 6:
            if i['location'][:2] == '云南' or i['location'][:6] == 'Yunnan':
                if i['role'] == 'student':
                    student += 1
                elif i['role'] == 'teacher':
                    teacher += 1
                for i_1 in i['daily']:
                    if len(i_1) == 8:
                        if t2 <= datetime.strptime(i_1, '%Y%m%d') < t3:
                            active_user += 1
                            break
    except KeyError:
        continue
print('学生总数是:', student)
print('教师总数是:', teacher)
print('有学生的学校总数是:', len(yun_school & school))
print('近一个月的活跃总数是:', active_user)





