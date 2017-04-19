import pandas as pd
from config import *

# a = [{'eventKey': 'a', 'pattern': 'b', 'haha': int(1.0)},
#      {'eventKey': 'b', 'pattern': 'b', 'topic': True},
#      {'eventKey': 'c', 'pattern': 'b', 'topic': False},
#      {'eventKey': 'd', 'pattern': 'a', 'topic': True},
#      {'eventKey': 'a', 'pattern': 'a', 'topic': False}
#      ]
# df = pd.DataFrame(a)
# b = list(df['topic'])
# if b[4]:
#     print(1)
# col_distinct.update_one({'user': 'aaa'},
#                         {'$set': {'phone': ['bbb', 'ddd']}})
a = col_loudou.find({})
video_error = 0
topic_error = 0
count = 0
for i in a:
    count += 1
    print(count)
    if 'tmp' in i:
        if 'enterVideo' in i['tmp']:
            list_video = list(set(i['tmp']['enterVideo']))
            if len(list_video) != len(i['tmp']['enterVideo']):
                video_error += 1
                # print(i)
                col_loudou.update_one({'user': i['user'], 'phone': i['os']},
                                      {'$pullAll': {'tmp.enterVideo': i['tmp']['enterVideo']}})
                col_loudou.update_one({'user': i['user'], 'phone': i['os']},
                                      {'$pushAll': {'tmp.enterVideo': list_video}})
        if 'clickKernelTopic' in i['tmp']:
            list_topic = list(set(i['tmp']['clickKernelTopic']))
            if len(list_topic) != len(i['tmp']['clickKernelTopic']):
                topic_error += 1
                col_loudou.update_one({'user': i['user'], 'phone': i['os']},
                                      {'$pullAll': {'tmp.enterVideo': i['tmp']['clickKernelTopic']}})
                col_loudou.update_one({'user': i['user'], 'phone': i['os']},
                                      {'$pushAll': {'tmp.enterVideo': list_topic}})
print(video_error)
print(topic_error)


