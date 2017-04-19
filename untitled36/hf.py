from pymongo import MongoClient
from config import *
client = MongoClient('10.8.8.111', 27017)
db = client.onions
# f1 = open('need.csv', 'w')
# f1.write(u'年级'+','+u'章节'+','+u'主题'+','+u'知识点'+','+u'视频时长'+','+u'习题数量')
# f1.write('\n')
# f1.write(str('年级'.encode('utf-8')))
col_chapter = db.chapters
col_themes = db.themes
col_topics = db.topics
col_practices = db.practices
col_hypervideos = db.hypervideos
count = 0
data = list()
data.append(['年级', '章节', '主题', '知识点', '视频时长', '习题数量'])
for i in col_chapter.find({'publisher': '人教版'}):
    # count += 1
    # print(count)
    # f1.write(i['semester']+','+i['name'])
    for i_1 in i['themes']:
        f2 = col_themes.find_one({'_id': i_1})
        if f2:
            print('1')
            # f1.write(i_1['name'])
            for i_2 in f2['topics']:
                f3 = col_topics.find_one({'_id': i_2})
                if f3:
                    print('2')
                    time = 0
                    practice_num = 0
                    f4 = col_hypervideos.find_one({'_id': f3['hyperVideo']})
                    if f4:
                        print('3')
                        time = f4['finishTime']
                    try:
                        f5 = col_practices.find_one({'_id': f3['practice']})
                        if f5:
                            print('4')
                            for i_3 in f5['levels']:
                                practice_num += len(i_3['problems'])
                    except KeyError:
                        continue
                    # f1.write(str(i['semester'])+','+str(i['name'])+','+str(f2['name'])+','+str(f3['name'])+','+str(time)+','+str(practice_num))
                    # f1.write('\n')
                    data.append([i['semester'], i['name'], f2['name'], f3['name'], time, practice_num])
write_excel(data, '02', '22')



# f1.close()
