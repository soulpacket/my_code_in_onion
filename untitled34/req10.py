from config import *
START = datetime.datetime(2017, 3, 12, 16)
END = datetime.datetime(2017, 4, 2, 16)
col = onions.users
# f1 = col.find({'publisher': {'$nin': ['人教版', '北师大版', '华师大版', '苏科版']}})
user_all = set()
vip_42 = set()
vip_43 = set()
ios_enter_42 = set()
android_enter_42 = set()
ios_enter_43 = set()
android_enter_43 = set()
# f4 = open('req10_user_all.csv', 'w')
app_42 = ['3.2.0', '3.2.1', '3.2.5', '3.2.6', '3.2.8']
# app_43 = ['3.3.'+str(i) for i in range(10)]
# num = 0
# for i in f1:
#     num += 1
#     print(num)
#     user_all.add(i['_id'])
# 4.2版本的付费用户
f3 = col_pay.find({'eventKey': 'paymentSuccess', 'serverTime': {'$gte': datetime.datetime(2017, 1, 8, 16),
                                                                '$lt': datetime.datetime(2017, 3, 12, 16)}})
for i in f3:
    vip_42.add(i['user'])
# 4.3版本的付费用户
f5 = col_pay.find({'eventKey': 'paymentSuccess', 'serverTime': {'$gte': START,
                                                                '$lt': END}})
for i in f5:
    vip_43.add(i['user'])
# 4.2版本
# f2 = col_event.find({'serverTime': {'$gte': datetime.datetime(2017, 1, 8, 16),
#                                     '$lt': datetime.datetime(2017, 3, 12, 16)},
#                      'eventKey': 'enterPayment',
#                      'd_appVersion': {'$in': app_42}})
# count = 0
# for i in f2:
#     count += 1
#     print(count)
#     try:
#         if 'os' in i:
#             if ObjectId.is_valid(i['user']):
#                 if i['os'] == 'ios':
#                     ios_enter_42.add(ObjectId(i['user']))
#                 elif i['os'] == 'android':
#                     android_enter_42.add(ObjectId(i['user']))
#     except KeyError:
#         continue
# ios_enter_42 = ios_enter_42 & user_all
# android_enter_42 = android_enter_42 & user_all
#
# ios_enter_42_vip = ios_enter_42 & vip_42
# android_enter_42_vip = android_enter_42 & vip_42
# print(len(ios_enter_42))
# print(len(ios_enter_42_vip))
# print(len(android_enter_42))
# print(len(android_enter_42_vip))
# 4.3版本
f4 = col_event.find({'serverTime': {'$gte': START, '$lt': END},
                     'eventKey': 'enterPayment',
                     'd_appVersion': {'$in': appVersion},
                     'u_publisher': {'$nin': ['人教版', '北师大版', '华师大版', '苏科版']}})
count = 0
for i in f4:
    count += 1
    print(count)
    try:
        if 'os' in i:
            if ObjectId.is_valid(i['user']):
                if i['os'] == 'ios':
                    ios_enter_43.add(ObjectId(i['user']))
                elif i['os'] == 'android':
                    android_enter_43.add(ObjectId(i['user']))
    except KeyError:
        continue
# ios_enter_43_new = ios_enter_43
# android_enter_43_new = android_enter_43
print(len(ios_enter_43))
print(len(android_enter_43))
# print(len(ios_enter_43_new))
# print(len(android_enter_43_new))

# ios_enter_43_vip = ios_enter_43 & vip_43
# android_enter_43_vip = android_enter_43 & vip_43
# data = [
#     ['IOS 无任务教材版本付费转化率'],
#     ['时期',	'进入付费介绍页UV', '付费', '付费率'],
#     ['4.2', 14479, 385, percent(385, 14479)],
#     ['4.3', len(ios_enter_43), len(ios_enter_43_vip), percent(len(ios_enter_43_vip), len(ios_enter_43))],
#     ['Android 无任务教材版本付费转化率'],
#     ['时期', '进入付费介绍页UV', '付费', '付费率'],
#     ['4.2', 88402, 838, percent(838, 88402)],
#     ['4.3', len(android_enter_43), len(android_enter_43_vip), percent(len(android_enter_43_vip), len(android_enter_43))]
# ]
# write_excel(data, '10', start_2_end(START, END))


