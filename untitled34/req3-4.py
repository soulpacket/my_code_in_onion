from config import *
START = ONLINE_DATE
END = get_week_day(1)
a = col_loudou.find({})
ios_video_uv = 0
ios_video_pv = 0
ios_topic_uv = 0
ios_topic_pv = 0
android_video_uv = 0
android_video_pv = 0
android_topic_uv = 0
android_topic_pv = 0
for i in a:
    if i['os'] == 'ios':
        if 'tmp' in i:
            if len(i['tmp']['enterVideo']) != 0:
                ios_video_uv += 1
                ios_video_pv += len(i['tmp']['enterVideo'])
            if len(i['tmp']['clickKernelTopic']) != 0:
                ios_topic_uv += 1
                ios_topic_pv += len(i['tmp']['clickKernelTopic'])
    elif i['os'] == 'android':
        if 'tmp' in i:
            if len(i['tmp']['enterVideo']) != 0:
                android_video_uv += 1
                android_video_pv += len(i['tmp']['enterVideo'])
            if len(i['tmp']['clickKernelTopic']) != 0:
                android_topic_uv += 1
                android_topic_pv += len(i['tmp']['clickKernelTopic'])
data = [
    ['IOS核心考点播放来源数据'],
    ['核心考点视频播放量', '核心考点页知识点点击量',	'核心考点页PV占比', '核心考点视频播放UV'	, '核心考点页知识点点击UV', '核心考点页UV占比'],
    [ios_video_pv, ios_topic_pv, percent(ios_topic_pv, ios_video_pv), ios_video_uv, ios_topic_uv,
     percent(ios_topic_uv, ios_video_uv)],
    ['Android核心考点播放来源数据'],
    ['核心考点视频播放量', '核心考点页知识点点击量',	'核心考点页PV占比', '核心考点视频播放UV'	, '核心考点页知识点点击UV', '核心考点页UV占比'],
    [android_video_pv, android_topic_pv, percent(android_topic_pv, android_video_pv), android_video_uv, android_topic_uv,
     percent(android_topic_uv, android_video_uv)],
]
write_excel(data, '03', start_2_end(START, END))

ios_mid = ios_topic_pv/ios_video_pv
android_mid = android_topic_pv/android_video_pv
print(ios_mid)
print(android_mid)
# ios_active_high = set()
# ios_active_low = set()
# ios_valid_high = set()
# ios_valid_low = set()
# ios_goal_high = set()
# ios_goal_low = set()
#
# android_active_high = set()
# android_active_low = set()
# android_valid_high = set()
# android_valid_low = set()
# android_goal_high = set()
# android_goal_low = set()

vip = set()
b = col_pay.find({'serverTime': {'$gte': datetime.datetime(2017, 3, 12, 16)}, 'eventKey': 'paymentSuccess'})
for i in b:
    vip.add(i['user'])


def judge(mid, user, oss):
    set_high = set()
    set_low = set()
    c = col_loudou.find({})
    for i in c:
        if i['os'] == oss:
            if user in i:
                if i[user]:
                    if len(i['tmp']['clickKernelTopic']) == 0:
                        set_low.add(ObjectId(i['user']))
                    elif len(i['tmp']['enterVideo']) == 0:
                        set_low.add(ObjectId(i['user']))
                    else:
                        if len(i['tmp']['clickKernelTopic']) / len(i['tmp']['enterVideo']) >= mid:
                            set_high.add(ObjectId(i['user']))
                        else:
                            set_low.add(ObjectId(i['user']))
    return set_high, set_low

ios_active_high, ios_active_low = judge(ios_mid, 'active_user', 'ios')
ios_valid_high, ios_valid_low = judge(ios_mid, 'valid_user', 'ios')
ios_goal_high, ios_goal_low = judge(ios_mid, 'goal_user', 'ios')

android_active_high, android_active_low = judge(android_mid, 'active_user', 'android')
android_valid_high, android_valid_low = judge(android_mid, 'valid_user', 'android')
android_goal_high, android_goal_low = judge(android_mid, 'goal_user', 'android')


ios_active_high_vip = len(ios_active_high & vip)
ios_active_low_vip = len(ios_active_low & vip)
ios_valid_high_vip = len(ios_valid_high & vip)
ios_valid_low_vip = len(ios_valid_low & vip)
ios_goal_high_vip = len(ios_goal_high & vip)
ios_goal_low_vip = len(ios_goal_low & vip)

android_active_high_vip = len(android_active_high & vip)
android_active_low_vip = len(android_active_low & vip)
android_valid_high_vip = len(android_valid_high & vip)
android_valid_low_vip = len(android_valid_low & vip)
android_goal_high_vip = len(android_goal_high & vip)
android_goal_low_vip = len(android_goal_low & vip)

data_1 = [
    ['IOS开启辅导用户付费率对比（活跃用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(ios_active_high), ios_active_high_vip, percent(ios_active_high_vip, len(ios_active_high))],
    ['核心页面播放视频PV占比低', len(ios_active_low), ios_active_low_vip, percent(ios_active_low_vip, len(ios_active_low))],
    ['IOS开启辅导用户付费率对比（有效用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(ios_valid_high), ios_valid_high_vip, percent(ios_valid_high_vip, len(ios_valid_high))],
    ['核心页面播放视频PV占比低', len(ios_valid_low), ios_valid_low_vip, percent(ios_valid_low_vip, len(ios_valid_low))],
    ['IOS开启辅导用户付费率对比（目标用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(ios_goal_high), ios_goal_high_vip, percent(ios_goal_high_vip, len(ios_goal_high))],
    ['核心页面播放视频PV占比低', len(ios_goal_low), ios_goal_low_vip, percent(ios_goal_low_vip, len(ios_goal_low))],
    ['android开启辅导用户付费率对比（活跃用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(android_active_high), android_active_high_vip,
     percent(android_active_high_vip, len(android_active_high))],
    ['核心页面播放视频PV占比低', len(android_active_low), android_active_low_vip, percent(android_active_low_vip, len(android_active_low))],
    ['android开启辅导用户付费率对比（有效用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(android_valid_high), android_valid_high_vip, percent(android_valid_high_vip, len(android_valid_high))],
    ['核心页面播放视频PV占比低', len(android_valid_low), android_valid_low_vip, percent(android_valid_low_vip, len(android_valid_low))],
    ['android开启辅导用户付费率对比（目标用户)'],
    ['用户群特征', '用户数', '付费用户数', '付费率'],
    ['核心页面播放视频PV占比高', len(android_goal_high), android_goal_high_vip, percent(android_goal_high_vip, len(android_goal_high))],
    ['核心页面播放视频PV占比低', len(android_goal_low), android_goal_low_vip, percent(android_goal_low_vip, len(android_goal_low))]
]
write_excel(data_1, '04', start_2_end(START, END))


