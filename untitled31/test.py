# from datetime import datetime
import pandas as pd
# # # a = [{'a': [1, 2, 3], 'b': 'bb'}, {'a': [1, 2, 3], 'b': 'cc'},{'a': [1, 2, 4], 'b': 'cc'}]
# # # df = pd.DataFrame(a)
# # # # print(list(df['b'].unique()))
# # # # print(df)
# # # #df_1 = df[df['b'].isin([3, 2])]
# # # df_1 = df.groupby('b')
# # # print(len(df_1))
# # # print(dict(list((df_1)))['cc']['a'])
# # # a = datetime(2017, 1, 10, 16) - datetime(2017, 1, 10, 16)
# # # print(a.days)
# # a = '20161109'
# # b = datetime.strptime(a, '%Y%m%d')
# # print(type(b))
# # import pandas as pd
a = pd.DataFrame([{'a': [1, 2, 3], 'b': 'bb'}, {'a': [1, 2, 3], 'b': 'cc'},{'a': [1, 2, 4], 'b': 'cc'}])
print(a)
# # a.append(pd.DataFrame([{'a': [1, 2, 3], 'b': 'bb'}, {'a': [1, 2, 3], 'b': 'cc'},{'a': [1, 2, 4], 'b': 'dd'}]))
# #
# # print(a)
# a = datetime(2017, 1, 10, 16) - datetime(2017, 1, 10, 16)
# b = a.seconds
# print(b)
# def vip(string, t1, t2=0):
#     if string == 'before':
#         print(t1)
#     elif string == 'after':
#         print(t2)
#
#
# vip('after', 1, 2)
from multiprocessing import Pool


def a(t1, t2, t3):
    return [t1, t2, t3]


# def b():
#     return [t2,3,4]

pool = Pool(4)
# func_list = [a, b]
result = list()
for i in range(20):
    time = 2+i
    result.append(pool.apply_async(a, (time, time, time)))
# print(result)
pool.close()
pool.join()
for i in result:
    print(i.get())
# from pymongo import MongoClient
# import pandas as pd
# import time
# from datetime import datetime, timedelta
# from multiprocessing import Pool
# from bson.objectid import ObjectId
#
# client = MongoClient('10.8.8.111', 27017)
# db_1 = client.eventsV4
# db_2 = client.cache
# col_event = db_1.eventV4
# col_order = db_1.orderEvents
# col_user = db_2.userAttr
# LIMIT = datetime(2017, 1, 8, 16)
# a = col_event.find({})
# for i in a:
#     print(i)

