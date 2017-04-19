from config import *
click_pv_ios = 0
click_uv_ios = set()
finish_pv_ios = 0
finish_uv_ios = set()
start_pv_ios = 0
# start_uv_ios = set()
videoFinish_pv_ios = 0
# videoFinish_uv_ios = set()

click_pv_android = 0
click_uv_android = set()
finish_pv_android = 0
finish_uv_android = set()
start_pv_android = 0
# start_uv_android = set()
videoFinish_pv_android = 0
# videoFinish_uv_android = set()

start_uv_ios_1 = set()
videoFinish_uv_ios_1 = set()
start_uv_android_1 = set()
videoFinish_uv_android_1 = set()

# file_1 = open('start_uv_ios.csv', 'a+')
# file_2 = open('videoFinish_uv_ios.csv', 'a+')
# file_3 = open('start_uv_android.csv', 'a+')
# file_4 = open('videoFinish_uv_android.csv', 'a+')
vip = set()
START = datetime.datetime(2017, 3, 12, 16)
# END = datetime.datetime(2017, 3, 22, 16)
END = datetime.datetime(2017, 4, 9, 16)
# set_1 = set(file_1.read().split(','))
# set_2 = set(file_2.read().split(','))
# set_3 = set(file_3.read().split(','))
# set_4 = set(file_4.read().split(','))

app_v41 = ['3.1.0', '3.1.1', '3.1.5']
app_v43 = ['3.3.'+str(i) for i in range(10)]
# 4.1时期对比数据
# f1 = col_event.aggregate([{'$match': {
#     'serverTime': {'$gte': datetime.datetime(2016, 11, 20, 16), '$lt': datetime.datetime(2017, 1, 8, 16)},
#     'eventKey': {'$in': ['clickPaymentVideo', 'finishPaymentVideo']},
#     'd_appVersion': {'$in': app_v41}}}
#     ])
# count = 0
#
# for i in f1:
#     count += 1
#     print(count)
#     try:
#         if 'os' in i:
#             if i['os'] == 'ios':
#                 if i['eventKey'] == 'clickPaymentVideo':
#                     click_pv_ios += 1
#                     click_uv_ios.add(i['user'])
#
#                 elif i['eventKey'] == 'finishPaymentVideo':
#                     finish_pv_ios += 1
#                     finish_uv_ios.add(i['user'])
#             elif i['os'] == 'android':
#                 if i['eventKey'] == 'clickPaymentVideo':
#                     click_pv_android += 1
#                     click_uv_android.add(i['user'])
#                 elif i['eventKey'] == 'finishPaymentVideo':
#                     finish_pv_android += 1
#                     finish_uv_android.add(i['user'])
#     except KeyError:
#         continue
# print(click_pv_ios)
# print(len(click_uv_ios))
# print(finish_pv_ios)
# print(len(finish_uv_ios))
#
# print(click_pv_android)
# print(len(click_uv_android))
# print(finish_pv_android)
# print(len(finish_uv_android))
# 4.3时期封面视频
f2 = col_event_bak.find({'serverTime': {'$gte': START, '$lt': END}, 'eventKey': {'$in': ['clickCCVideo', 'finishCCVideo']},
                         'd_appVersion': {'$in': app_v43}})
count = 0
for i in f2:
    count += 1
    print(count)
    # if i['videoType'] == 'coach':
    try:
        if i['os'] == 'ios':
            if i['eventKey'] == 'clickCCVideo':
                start_pv_ios += 1
                if ObjectId.is_valid(i['user']):
                    start_uv_ios_1.add(i['user'])

            elif i['eventKey'] == 'finishCCVideo':
                videoFinish_pv_ios += 1
                if ObjectId.is_valid(i['user']):
                    videoFinish_uv_ios_1.add(i['user'])

        elif i['os'] == 'android':
            if i['eventKey'] == 'clickCCVideo':
                start_pv_android += 1
                if ObjectId.is_valid(i['user']):
                    start_uv_android_1.add(i['user'])

            elif i['eventKey'] == 'finishCCVideo':
                videoFinish_pv_android += 1
                if ObjectId.is_valid(i['user']):
                    videoFinish_uv_android_1.add(i['user'])
    except KeyError:
        continue
print(start_pv_ios)
print(videoFinish_pv_ios)
print(start_pv_android)
print(videoFinish_pv_android)
for i in videoFinish_uv_android_1 - start_uv_android_1:
    print(i)
# start_uv_ios = start_uv_ios_1 | set_1
# videoFinish_uv_ios = videoFinish_uv_ios_1 | set_2
# start_uv_android = start_uv_android_1 | set_3
# videoFinish_uv_android = videoFinish_uv_android_1 | set_4
# for i in start_uv_ios_1 - set_1:
#     file_1.write(str(i))
#     file_1.write(',')
# for i in videoFinish_uv_ios_1 - set_2:
#     file_2.write(str(i))
#     file_2.write(',')
# for i in start_uv_android_1 - set_3:
#     file_3.write(str(i))
#     file_3.write(',')
# for i in videoFinish_uv_android_1 - set_4:
#     file_4.write(str(i))
#     file_4.write(',')
#
# # vip所有的付费人数
# f3 = col_pay.find({'eventKey': 'paymentSuccess', 'serverTime': {'$gte': START}})
# for i in f3:
#     vip.add(i['user'])
# finish_vip_ios = videoFinish_uv_ios & vip
# noFinish_ios = start_uv_ios - videoFinish_uv_ios  # 未完成视频的uv
# noFinish_vip_ios = noFinish_ios & vip  # 未完成视频的付费人数
#
# finish_vip_android = videoFinish_uv_android & vip
# noFinish_android = start_uv_android - videoFinish_uv_android  # 未完成视频的uv
# noFinish_vip_android = noFinish_android & vip  # 未完成视频的付费人数
# data = [
#     ['IOS 4.1时代付费介绍视频数据'],
#     ['指标', '开始付费介绍视频', '完成付费介绍视频',	'付费介绍视频完成率'],
#     ['PV', 52102, 11673, percent(11673, 52102)],
#     ['UV', 15529, 8752, percent(8752, 15529)],
#     ['IOS 4.3时代核心考点封面页视频数据'],
#     ['指标', '开始考点封面视频', '完成考点封面视频', '考点封面视频完成率'],
#     ['PV', start_pv_ios, videoFinish_pv_ios, percent(videoFinish_pv_ios, start_pv_ios)],
#     ['UV', len(start_uv_ios), len(videoFinish_uv_ios), percent(len(videoFinish_uv_ios), len(start_uv_ios))],
#     ['Android 4.1时代付费介绍视频数据'],
#     ['指标', '开始付费介绍视频', '完成付费介绍视频', '付费介绍视频完成率'],
#     ['PV', 598220, 59218, percent(59218, 598220)],
#     ['UV', 131688, 42196, percent(42196, 131688)],
#     ['android 4.3时代核心考点封面页视频数据'],
#     ['指标', '开始考点封面视频', '完成考点封面视频', '考点封面视频完成率'],
#     ['PV', start_pv_android, videoFinish_pv_android, percent(videoFinish_pv_android, start_pv_android)],
#     ['UV', len(start_uv_android), len(videoFinish_uv_android),
#      percent(len(videoFinish_uv_android), len(start_uv_android))]
# ]
# write_excel(data, '06', start_2_end(START, END))
#
# data_1 = [
#     ['IOS 考点封面视频付费转化数据'],
#     ['用户群', '用户数', '付费人数', '付费率'],
#     ['完成封面页视频', len(videoFinish_uv_ios), len(finish_vip_ios), percent(len(finish_vip_ios), len(videoFinish_uv_ios))],
#     ['未完成封面页视频', len(noFinish_ios), len(noFinish_vip_ios), percent(len(noFinish_vip_ios), len(noFinish_ios))],
#     ['Android 考点封面视频付费转化数据'],
#     ['用户群', '用户数', '付费人数', '付费率'],
#     ['完成封面页视频', len(videoFinish_uv_android), len(finish_vip_android),
#      percent(len(finish_vip_android), len(videoFinish_uv_android))],
#     ['未完成封面页视频', len(noFinish_android), len(noFinish_vip_android),
#      percent(len(noFinish_vip_android), len(noFinish_android))]
# ]
# write_excel(data_1, '07', start_2_end(START, END))
# file_1.close()
# file_2.close()
# file_3.close()
# file_4.close()
