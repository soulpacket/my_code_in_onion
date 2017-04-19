from config import *
from multiprocessing import Pool
import numpy as np
# START = datetime.datetime(2017, 3, 25, 16)
START = ONLINE_DATE
END = get_week_day(1)
f2 = col_chapter.find({})
df_1 = pd.DataFrame(list(f2))
# f5 = open('recommend.csv', 'w')
f3 = col_tutorialTopics.find({})
df_2 = pd.DataFrame(list(f3))
df_3 = df_2.groupby(['topicId', 'target'])
# print(dict(list(df_3)))
# f5 = open('recommend.csv', 'w')
f4 = col_topic.find({})
df_4 = pd.DataFrame(list(f4))

topic_all = list(df_4['_id'])  # ObjectId 类型
key_all = list(df_4['keyPoint'])
# a = list()


def before(start):

    mongo = MongoClient(host='10.8.8.111', port=27017, connect=False)
    db = mongo['eventsV4']
    col_event = db.eventV4
    f1 = col_event.find({'serverTime': {'$gte': start, '$lt': (start+datetime.timedelta(hours=1))},
                         'eventKey': {'$in': ['enterTopicFinish', 'enterKernel']},
                         })
    # {'$project': {'topicId': 1, 'u_pattern': 1, 'u_publisher': 1, 'eventKey': 1, 'os': 1}})
    ios_topic_recommend = 0
    ios_topic_key = 0
    android_topic_recommend = 0
    android_topic_key = 0
    ios_user_kernel = set()
    android_user_kernel = set()
    # count = 0
    for i in f1:
        # count += 1
        # print(count)
        if 'd_appVersion' in i:
            if i['d_appVersion'] in appVersion:
                if 'os' in i:
                    # if i['os'] == 'ios':
                    #     if i['eventKey'] == 'enterKernel':
                    #         if 'user' in i:
                    #             ios_user_kernel.add(i['user'])
                    #     else:
                    #         # if 'd_appVersion' in i:
                    #         if 'topicId' in i:
                    #             # if i['d_appVersion'] in appVersion:
                    #                 # print(i)
                    #                 # 判断是不是核心考点
                    #             if ObjectId.is_valid(i['topicId']):
                    #                 if ObjectId(i['topicId']) in topic_all:
                    #                     if list(df_4[df_4['_id'] == ObjectId(i['topicId'])]['keyPoint'])[0] is True:
                    #                         # 为True的时候
                    #                         ios_topic_key += 1
                    #             # 判断是不是推荐的核心考点
                    #             if 'u_publisher' in i:
                    #                 if 'u_pattern' in i:
                    #                     try:
                    #                         # group_1 = dict(list(df_3))[('hah', 'u_pattern')]
                    #                         chapter_all = list(df_1[df_1['publisher'] == i['u_publisher']]['_id'])
                    #                         # print(len(chapter_all))
                    #                         if ObjectId.is_valid(i['topicId']):
                    #                             group_1 = dict(list(df_3))[(ObjectId(i['topicId']), i['u_pattern'])]
                    #                             num = 0
                    #                             for i_1 in list(group_1['chapterId']):
                    #                                 num += 1
                    #                                 if i_1 in chapter_all:
                    #                                     # print('hh')
                    #                                     # print(list(group_1['recommended'])[list(group_1['chapterId']).index(i_1)])
                    #                                     if list(group_1['recommended'])[list(group_1['chapterId']).index(i_1)]:
                    #                                         ios_topic_recommend += 1
                    #                                         break
                    #                     except KeyError:
                    #                         continue
                                        # print(ios_topic_recommend)
                    if i['os'] == 'android':
                        # if 'd_appVersion' in i:
                        if i['eventKey'] == 'enterKernel':
                            if 'user' in i:
                                android_user_kernel.add(i['user'])
                        else:
                            if 'topicId' in i:
                                # if i['d_appVersion'] in appVersion:
                                    # print(i)
                                    # 判断是不是核心考点
                                if ObjectId.is_valid(i['topicId']):
                                    if ObjectId(i['topicId']) in topic_all:
                                        if list(df_4[df_4['_id'] == ObjectId(i['topicId'])]['keyPoint'])[0] is True:  # 为True的时候
                                            android_topic_key += 1
                                # 判断是不是推荐的核心考点
                                if 'u_publisher' in i:
                                    if 'u_pattern' in i:
                                        try:
                                            # group_1 = dict(list(df_3))[('hah', 'u_pattern')]
                                            chapter_all = list(df_1[df_1['publisher'] == i['u_publisher']]['_id'])
                                            # print(len(chapter_all))
                                            if ObjectId.is_valid(i['topicId']):
                                                group_1 = dict(list(df_3))[(ObjectId(i['topicId']), i['u_pattern'])]
                                                # num = 0
                                                for i_1 in list(group_1['chapterId']):
                                                    # num += 1
                                                    if i_1 in chapter_all:
                                                        # print('hh')
                                                        # print(list(group_1['recommended'])[list(group_1['chapterId']).index(i_1)])
                                                        if list(group_1['recommended'])[list(group_1['chapterId']).index(i_1)]:
                                                            android_topic_recommend += 1
                                                            # f5.write(str(i['_id']))
                                                            # f5.write(',')
                                                            # print(num)
                                                            # num += 1
                                                            # a.append(1)
                                                            break
                                        except KeyError:
                                            continue
    return [ios_topic_key, ios_topic_recommend, android_topic_key, android_topic_recommend, ios_user_kernel,
            android_user_kernel]

# print(android_topic_recommend)
# print('ios推荐的核心考点PV是:', ios_topic_recommend)
# print('ios核心考点PV是:', ios_topic_key)
# print('ios推荐占比是:', percent(ios_topic_recommend, ios_topic_key))
# print('android推荐的核心考点PV是:', android_topic_recommend)
# print('android核心考点PV是:', android_topic_key)
# print('android推荐占比是:', percent(android_topic_recommend, android_topic_key))
# data = [
#     ['ios完成核心考点数据'],
#     ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
#     [ios_topic_key, ios_topic_recommend, percent(ios_topic_recommend, ios_topic_key)],
#     ['android完成核心考点数据'],
#     ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
#     [android_topic_key, android_topic_recommend, percent(android_topic_recommend, android_topic_key)]
# ]
# write_excel(data, '05', start_2_end(START, END))


def run(ios_topic_key, ios_topic_recommend, android_topic_key,
        android_topic_recommend):
    ios_true = 0
    ios_false = 0
    android_true = 0
    android_false = 0
    ios_user_kernel = set()
    android_user_kernel = set()
    f1 = col_event.find({'serverTime': {'$gte': START, '$lt': END},
                         'eventKey': 'enterKernel', 'd_appVersion': {'$in': appVersion}
                         })
    count = 0
    for i in f1:
        count += 1
        print(count)
        try:
            if i['os'] == 'ios':
                ios_user_kernel.add(i['user'])
            elif i['os'] == 'android':
                android_user_kernel.add(i['user'])
        except KeyError:
            continue
    for i in col_loudou.find({}):
        if 'recommend_true' in i:
            if i['os'] == 'ios':
                ios_true += 1
            elif i['os'] == 'android':
                android_true += 1
        if 'recommend_false' in i:
            if i['os'] == 'ios':
                ios_false += 1
            elif i['os'] == 'android':
                android_false += 1
    data_1 = [
        ['IOS完成核心考点数据'],
        ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
        [ios_topic_key, ios_topic_recommend, percent(ios_topic_recommend, ios_topic_key)],
        ['IOS核心考点页点击数据'],
        ['核心考点页UV',	'推荐知识点点击UV', '推荐知识点点击率',	'非推荐知识点点击UV',	'非推荐知识点点击率'],
        [len(ios_user_kernel), ios_true, percent(ios_true, len(ios_user_kernel)), ios_false,
         percent(ios_false, len(ios_user_kernel))],
        ['android完成核心考点数据'],
        ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
        [android_topic_key, android_topic_recommend, percent(android_topic_recommend, android_topic_key)],
        ['Android核心考点页点击数据'],
        ['核心考点页UV', '推荐知识点点击UV', '推荐知识点点击率', '非推荐知识点点击UV', '非推荐知识点点击率'],
        [len(android_user_kernel), android_true, percent(android_true, len(android_user_kernel)), android_false,
         percent(android_false, len(android_user_kernel))]
    ]
    write_excel(data_1, '05', start_2_end(START, END))

run(0, 0, 0, 0)


def main():
    ios_topic_recommend = 0
    ios_topic_key = 0
    android_topic_recommend = 0
    android_topic_key = 0
    ios_user_kernel = set()
    android_user_kernel = set()
    # np1 = np.array([0] * 6)
    pool = Pool(6)
    result = list()
    result_1 = list()
    for func in range(24):
        time_arg = START + datetime.timedelta(hours=func)
        result.append(pool.apply_async(before, (time_arg,)))
    print(len(result))
    num = 0
    for i in result:
        result_1.append(i.get())
        num += 1
        print(num)
        # print(i.get())
    for i in range(len(result_1)):
        ios_topic_key += result_1[i][0]
    for i in range(len(result_1)):
        ios_topic_recommend += result_1[i][1]
    for i in range(len(result_1)):
        android_topic_key += result_1[i][2]
    for i in range(len(result_1)):
        android_topic_recommend += result_1[i][3]
    for i in range(len(result_1)):
        ios_user_kernel = ios_user_kernel | result_1[i][4]
    for i in range(len(result_1)):
        android_user_kernel = android_user_kernel | result_1[i][5]

    print('ios推荐的核心考点PV是:', ios_topic_recommend)
    print('ios核心考点PV是:', ios_topic_key)
    print('ios推荐占比是:', percent(ios_topic_recommend, ios_topic_key))
    print('android推荐的核心考点PV是:', android_topic_recommend)
    print('android核心考点PV是:', android_topic_key)
    print('android推荐占比是:', percent(android_topic_recommend, android_topic_key))
    print('ios_enter_kernel_uv_is:', len(ios_user_kernel))
    print('android_enter_kernel_uv_is:', len(android_user_kernel))
    # f5.close()
    # data = [
    #     ['ios完成核心考点数据'],
    #     ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
    #     [ios_topic_key, ios_topic_recommend, percent(ios_topic_recommend, ios_topic_key)],
    #     ['android完成核心考点数据'],
    #     ['完成核心考点数', '完成的推荐核心考点数', '推荐占比'],
    #     [android_topic_key, android_topic_recommend, percent(android_topic_recommend, android_topic_key)]
    # ]
    # write_excel(data, '05', start_2_end(START, END))

# if __name__ == '__main__':
#     main()
    # f5.close()



