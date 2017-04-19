from config import *


def entervideo(loudou_2, index, actual_topic_all, target, name, phone, chapter_id):
    if 'enterVideo' in list(loudou_2['eventKey'])[index:]:
        # index_1 = list(loudou_2['eventKey']).index('enterVideo')
        for h in range(len(list(loudou_2['eventKey'])[index:])):
            if list(loudou_2['eventKey'])[h + index] == 'enterVideo':
                topic_1 = list(loudou_2['topicId'])[h + index]
                target_1 = list(loudou_2['u_pattern'])[h + index]
                if target_1 == target:  # 判断难度是否相同
                    if ObjectId.is_valid(topic_1):  # 判断topic是否课objectid
                        if ObjectId(topic_1) in actual_topic_all:
                            col_loudou.update_one({'user': name, 'os': phone},
                                                  {'$set': {'enterVideo': 1}})
                            if 'enterTopicFinish' in list(loudou_2['eventKey'])[(index + h):]:
                                topic_all = list()
                                for h_1 in range(len(list(loudou_2['eventKey'])[(index + h):])):

                                    if list(loudou_2['eventKey'])[h + index + h_1] == 'enterTopicFinish':
                                        topic_2 = list(loudou_2['topicId'])[h + index + h_1]
                                        target_2 = list(loudou_2['u_pattern'])[h + index + h_1]
                                        event_time_2 = list(loudou_2['eventTime'])[h + index + h_1]

                                        if topic_2 not in topic_all:

                                            if target_2 == target:
                                                if ObjectId.is_valid(topic_2):
                                                    if ObjectId(topic_2) in actual_topic_all:
                                                        topic_all.append(topic_2)
                                                        col_loudou.update_one({'user': name, 'os': phone},
                                                                              {'$set': {
                                                                               'enterTopicFinish.' +
                                                                               topic_2: event_time_2}})
                                    if len(topic_all) == len(actual_topic_all):
                                        if 'clickCoachFinishCoach' in list(loudou_2['eventKey'])[
                                                                      (h + index + h_1):]:
                                            index_1 = list(loudou_2['eventKey'])[(h + index + h_1):].index(
                                                'clickCoachFinishCoach') + h + index + h_1
                                            if list(loudou_2['chapterId'])[index_1] == chapter_id:
                                                col_loudou.update_one({'user': name, 'os': phone},
                                                                      {'$set': {'clickCoachFinishCoach': 1}})
                                                if 'enterPayment' in list(loudou_2['eventKey'])[index_1:]:
                                                    index_2 = list(loudou_2['eventKey'])[index_1:].index(
                                                        'enterPayment') + index_1
                                                    event_time_3 = list(loudou_2['eventTime'])[index_2]
                                                    col_loudou.update_one({'user': name, 'os': phone},
                                                                          {'$set': {'enterPayment': event_time_3}})
                                        break
                            break


def entertopicfinish(loudou_2, index, h, actual_topic_all, name, phone, chapter_id, target, topic_all):
    if 'enterTopicFinish' in list(loudou_2['eventKey'])[(index + h):]:
        # topic_all = list()
        for h_1 in range(len(list(loudou_2['eventKey'])[(index + h):])):

            if list(loudou_2['eventKey'])[h + index + h_1] == 'enterTopicFinish':
                topic_2 = list(loudou_2['topicId'])[h + index + h_1]
                target_2 = list(loudou_2['u_pattern'])[h + index + h_1]
                event_time_2 = list(loudou_2['eventTime'])[h + index + h_1]

                if topic_2 not in topic_all:

                    if target_2 == target:
                        if ObjectId.is_valid(topic_2):
                            if ObjectId(topic_2) in actual_topic_all:
                                topic_all.append(topic_2)
                                col_loudou.update_one({'user': name, 'os': phone},
                                                      {'$set': {'enterTopicFinish.' + topic_2: event_time_2}})
            if len(topic_all) == len(actual_topic_all):
                if 'clickCoachFinishCoach' in list(loudou_2['eventKey'])[(h + index + h_1):]:
                    index_1 = list(loudou_2['eventKey'])[(h + index + h_1):].index('clickCoachFinishCoach') + h + index + h_1
                    if list(loudou_2['chapterId'])[index_1] == chapter_id:
                        col_loudou.update_one({'user': name, 'os': phone},
                                              {'$set': {'clickCoachFinishCoach': 1}})
                        if 'enterPayment' in list(loudou_2['eventKey'])[index_1:]:
                            index_2 = list(loudou_2['eventKey'])[index_1:].index('enterPayment') + index_1
                            event_time_3 = list(loudou_2['eventTime'])[index_2]
                            col_loudou.update_one({'user': name, 'os': phone},
                                                  {'$set': {'enterPayment': event_time_3}})
                break


def clickcoachfinishcoach(loudou_2, index, h, h_1, chapter_id, name, phone):
    if 'clickCoachFinishCoach' in list(loudou_2['eventKey'])[(h + index + h_1):]:
        index_1 = list(loudou_2['eventKey'])[(h + index + h_1):].index('clickCoachFinishCoach') + h + index + h_1
        if list(loudou_2['chapterId'])[index_1] == chapter_id:
            col_loudou.update_one({'user': name, 'os': phone},
                                  {'$set': {'clickCoachFinishCoach': 1}})
            if 'enterPayment' in list(loudou_2['eventKey'])[index_1:]:
                index_2 = list(loudou_2['eventKey'])[index_1:].index('enterPayment') + index_1
                event_time_3 = list(loudou_2['eventTime'])[index_2]
                col_loudou.update_one({'user': name, 'os': phone},
                                      {'$set': {'enterPayment': event_time_3}})


def enterpayment(loudou_2, index_1, name, phone):
    if 'enterPayment' in list(loudou_2['eventKey'])[index_1:]:
        index_2 = list(loudou_2['eventKey'])[index_1:].index('enterPayment') + index_1
        event_time_3 = list(loudou_2['eventTime'])[index_2]
        col_loudou.update_one({'user': name, 'os': phone},
                              {'$set': {'enterPayment': event_time_3}})


def event_key(loudou_2, target, actual_topic_all, name, phone):
    event_time_video = list()
    event_time_kernel = list()
    for i_2 in range(len(list(loudou_2['eventKey']))):
        if list(loudou_2['eventKey'])[i_2] == 'enterVideo':
            topic_1 = list(loudou_2['topicId'])[i_2]
            target_1 = list(loudou_2['u_pattern'])[i_2]
            if target_1 == target:  # 判断难度是否相同
                if ObjectId.is_valid(topic_1):  # 判断topic是否课objectid
                    if ObjectId(topic_1) in actual_topic_all:
                        event_time_video.append(list(loudou_2['eventTime'])[i_2])
        elif list(loudou_2['eventKey'])[i_2] == 'clickKernelTopic':
            if list(loudou_2['recommend'])[i_2] == 'true':
                col_loudou.update_one({'user': name, 'os': phone},
                                      {'$set': {'recommend_true': 1}})
            elif list(loudou_2['recommend'])[i_2] == 'false':
                col_loudou.update_one({'user': name, 'os': phone},
                                      {'$set': {'recommend_false': 1}})
            topic_1 = list(loudou_2['topicId'])[i_2]
            target_1 = list(loudou_2['u_pattern'])[i_2]
            if target_1 == target:  # 判断难度是否相同
                if ObjectId.is_valid(topic_1):  # 判断topic是否可objectid
                    if ObjectId(topic_1) in actual_topic_all:
                        event_time_kernel.append(list(loudou_2['eventTime'])[i_2])
        # elif list(loudou_2['eventKey'])[i_2] == 'enterKernel':
        #     col_loudou.update_one({'user': name, 'os': phone},
        #                           {'$set': {'tmp.enterKernel': 1}})

    col_loudou.update_one({'user': name, 'os': phone},
                          {'$push': {'tmp.enterVideo': {'$each': event_time_video}}})
    col_loudou.update_one({'user': name, 'os': phone},
                          {'$push': {'tmp.clickKernelTopic': {'$each': event_time_kernel}}})


# def judge_active(loudou_2, index):
#     if 'enterVideo' in list(loudou_2['eventKey'])[:index]:
#         col_loudou.update_one

def update_db(app, t1, t2):
    # 每一个chapterId里面有哪些topic
    f1 = col_tutorialTopics.find({})
    df_1 = pd.DataFrame(list(f1))
    df_2 = df_1.groupby(["chapterId", "target"])
    dict_chapter = dict(list(df_2))
    print('1')
    # f3 = col_event.find({'serverTime': {'$gte': t1, '$lt': t2},
    #                      'eventKey': {'$in':
    #                                       ['clickCCSOpenCoach', 'enterVideo',
    #                                        'enterTopicFinish', 'clickCoachFinishCoach', 'enterPayment']},
    #                      'd_appVersion': {'$in': app}
    #                      })
    field = {'eventTime': 1, 'eventKey': 1, 'os': 1, 'user': 1, 'topicId': 1, 'chapterId': 1, 'u_pattern': 1,
             'recommend': 1, 'd_appVersion': 1}
    # f3 = col_event.find({'serverTime': {'$gte': t1, '$lt': t2},
    #                      'eventKey': {'$in': ['clickCCSOpenCoach', 'enterVideo', 'clickKernelTopic',
    #                                           'enterTopicFinish', 'clickCoachFinishCoach', 'enterPayment']}
    #                      # 'd_appVersion': {'$in': app}
    #                      }, field)
    f3 = col_event.find({'serverTime': {'$gte': t1, '$lt': t2},
                         'eventKey': {'$in': ['clickCCSOpenCoach', 'enterVideo', 'clickKernelTopic']}
                         # 'd_appVersion': {'$in': app}
                         }, field)
    # print(len(list(f3)))
    df_4 = pd.DataFrame(list(f3))
    print('2')
    df_6 = df_4[df_4['d_appVersion'].isin(app)].sort_values(by='eventTime').groupby(['os', 'user'])
    # print(len(df_6))
    # print('6')
    # df_5 = df_7.sort_values(by='eventTime')
    # print('3')
    # df_6 = df_5.groupby(['os', 'user'])
    # # print(list(df_6))
    # print(len(df_6))
    # print('4')
    count = 0
    for (phone, name), group in df_6:
        # count += 1
        # print(count)
        loudou_1 = col_loudou.find_one({'user': name, 'os': phone})
        loudou_2 = dict(list(df_6))[(phone, name)]
        if loudou_1:
            chapter_id = loudou_1['chapterId']
            target = loudou_1['target']
            actual_topic_all = list(dict_chapter[(ObjectId(chapter_id), target)]['topicId'])
            event_key(loudou_2, target, actual_topic_all, name, phone)
            # if 'enterVideo' not in loudou_1:
            #     entervideo(loudou_2, 0, actual_topic_all, target, name, phone, chapter_id)
            # elif 'enterVideo' in loudou_1 and 'enterTopicFinish' not in loudou_1:
            #     entertopicfinish(loudou_2, 0, 0, actual_topic_all, name, phone, chapter_id, target, [])
            # elif 'enterTopicFinish' in loudou_1 and len(loudou_1['enterTopicFinish']) < len(actual_topic_all):
            #     entertopicfinish(loudou_2, 0, 0, actual_topic_all, name, phone, chapter_id, target,
            #                      list(loudou_1['enterTopicFinish'].keys()))
            # elif len(loudou_1['enterTopicFinish']) == len(actual_topic_all) and 'clickCoachFinishCoach' not in loudou_1:
            #     clickcoachfinishcoach(loudou_2, 0, 0, 0, chapter_id, name, phone)
            # elif 'clickCoachFinishCoach' in loudou_1 and 'enterPayment' not in loudou_1:
            #     enterpayment(loudou_2, 0, name, phone)
        else:
            if 'clickCCSOpenCoach' in list(loudou_2['eventKey']):
                index = list(loudou_2['eventKey']).index('clickCCSOpenCoach')
                if ObjectId.is_valid(list(loudou_2['chapterId'])[index]):
                    if (ObjectId(list(loudou_2['chapterId'])[index]), list(loudou_2['u_pattern'])[index]) in dict_chapter:
                        col_loudou.insert({'user': name,
                                           'os': phone,
                                           'clickCCSOpenCoach': list(loudou_2['eventTime'])[index],
                                           'chapterId': list(loudou_2['chapterId'])[index],
                                           'target': list(loudou_2['u_pattern'])[index]
                                           })
                        # 对用户分层 活跃用户\有效用户\目标用户
                        # if col_event.find_one({'eventTime': {'$lt': list(loudou_2['eventTime'])[index]}, 'user': name,
                        #                        'eventKey': 'enterVideo', 'os': phone}):
                        #     col_loudou.update_one({'user': name, 'os': phone},
                        #                           {'$set': {'valid_user': True, 'active_user': False}})
                        # else:
                        #     col_loudou.update_one({'user': name, 'os': phone},
                        #                           {'$set': {'valid_user': False, 'active_user': True}})
                        # f4 = col_event.find({'eventTime': {'$lt': list(loudou_2['eventTime'])[index]}, 'user': name,
                        #                      'eventKey': 'enterTopicFinish', 'os': phone})
                        # topic_set = set()
                        # if f4:
                        #     for i_1 in f4:
                        #         if 'topicId' in i_1:
                        #             topic_set.add(i_1['topicId'])
                        # if len(topic_set) >= 3:
                        #     col_loudou.update_one({'user': name, 'os': phone},
                        #                           {'$set': {'goal_user': True}})
                        # else:
                        #     col_loudou.update_one({'user': name, 'os': phone},
                        #                           {'$set': {'goal_user': False}})
                        chapter_id = list((loudou_2['chapterId']))[index]
                        target = list(loudou_2['u_pattern'])[index]
                        actual_topic_all = list(dict_chapter[(ObjectId(chapter_id), target)]['topicId'])
                        event_key(loudou_2, target, actual_topic_all, name, phone)
                        # entervideo(loudou_2, index, actual_topic_all, target, name, phone, chapter_id)


def main():
    start = datetime.datetime(2017, 3, 26, 16)
    while start != datetime.datetime(2017, 3, 26, 16):
        time_1 = time.time()
        update_db(appVersion, start, start+datetime.timedelta(hours=1))
        time_2 = time.time()
        print(start, time_2 - time_1)
        start += datetime.timedelta(hours=1)
    # f1 = col_loudou.find({'os': 'ios'})
if __name__ == '__main__':
    main()
# workbook.close()
