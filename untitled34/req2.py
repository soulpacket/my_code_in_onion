from config import *
ios_open_coach = 0
ios_enter_video = 0
ios_enter_topic = 0
ios_finish_all_topics = 0
ios_finish_coach = 0
ios_enter_payment = 0
ios_vip = 0
START = ONLINE_DATE
END = get_week_day(1)
android_open_coach = 0
android_enter_video = 0
android_enter_topic = 0
android_finish_all_topics = 0
android_finish_coach = 0
android_enter_payment = 0
android_vip = 0

f1 = col_tutorialTopics.find({})
df_1 = pd.DataFrame(list(f1))
df_2 = df_1.groupby(["chapterId", "target"])
dict_chapter = dict(list(df_2))

a = col_loudou.find({})

for i in a:
    if i['os'] == 'ios':
        ios_open_coach += 1
        if 'enterVideo' in i:
            ios_enter_video += 1
        if 'enterTopicFinish' in i:
            ios_enter_topic += 1
            if len(dict_chapter[(ObjectId(i['chapterId']), i['target'])]) == len(i['enterTopicFinish']):
                ios_finish_all_topics += 1
        if 'clickCoachFinishCoach' in i:
            ios_finish_coach += 1
        if 'enterPayment' in i:
            ios_enter_payment += 1
            if col_pay.find_one({'eventKey': 'paymentSuccess',
                                 'serverTime': {'$gte': datetime.datetime.fromtimestamp(i['enterPayment']/1000) -
                                                datetime.timedelta(hours=8)},
                                 'user': ObjectId(i['user'])}):
                ios_vip += 1

    elif i['os'] == 'android':
        android_open_coach += 1
        if 'enterVideo' in i:
            android_enter_video += 1
        if 'enterTopicFinish' in i:
            android_enter_topic += 1
            if len(dict_chapter[(ObjectId(i['chapterId']), i['target'])]) == len(i['enterTopicFinish']):
                android_finish_all_topics += 1
        if 'clickCoachFinishCoach' in i:
            android_finish_coach += 1
        if 'enterPayment' in i:
            android_enter_payment += 1
            if col_pay.find_one({'eventKey': 'paymentSuccess',
                                 'serverTime': {'$gte': datetime.datetime.fromtimestamp(i['enterPayment'] / 1000) -
                                                datetime.timedelta(hours=8)},
                                 'user': ObjectId(i['user'])}):
                android_vip += 1
data = [
    ['IOS金牌提分辅导付费转化漏斗'],
    ['步骤', '用户数', '整体转化率', '对上一步转化率'],
    ['开启金牌提分辅导', ios_open_coach, '100%', '100%'],
    ['学习一个知识点', ios_enter_video, percent(ios_enter_video, ios_open_coach), percent(ios_enter_video, ios_open_coach)],
    ['完成一个知识点', ios_enter_topic, percent(ios_enter_topic, ios_open_coach), percent(ios_enter_topic, ios_enter_video)],
    ['完成所有知识点', ios_finish_all_topics, percent(ios_finish_all_topics, ios_open_coach), percent(ios_finish_all_topics, ios_enter_topic)],
    ['完成辅导', ios_finish_coach, percent(ios_finish_coach, ios_open_coach), percent(ios_finish_coach, ios_finish_all_topics)],
    ['付费页面', ios_enter_payment, percent(ios_enter_payment, ios_open_coach), percent(ios_enter_payment, ios_finish_coach)],
    ['付费', ios_vip, percent(ios_vip, ios_open_coach), percent(ios_vip, ios_enter_payment)],
    ['Android金牌提分辅导付费转化漏斗'],
    ['步骤', '用户数', '整体转化率', '对上一步转化率'],
    ['开启金牌提分辅导', android_open_coach, '100%', '100%'],
    ['学习一个知识点', android_enter_video, percent(android_enter_video, android_open_coach), percent(android_enter_video, android_open_coach)],
    ['完成一个知识点', android_enter_topic, percent(android_enter_topic, android_open_coach), percent(android_enter_topic, android_enter_video)],
    ['完成所有知识点', android_finish_all_topics, percent(android_finish_all_topics, android_open_coach), percent(android_finish_all_topics, android_enter_topic)],
    ['完成辅导', android_finish_coach, percent(android_finish_coach, android_open_coach), percent(android_finish_coach, android_finish_all_topics)],
    ['付费页面', android_enter_payment, percent(android_enter_payment, android_open_coach), percent(android_enter_payment, android_finish_coach)],
    ['付费', android_vip, percent(android_vip, android_open_coach), percent(android_vip, android_enter_payment)]
]
write_excel(data, '02', start_2_end(START, END))
