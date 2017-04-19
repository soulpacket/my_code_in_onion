#!/usr/bin/python3
# encoding: utf-8
"""
Created on 04/01/2017
@author: Data Team
"""
# _*_ coding:utf-8 _*_
from pymongo import MongoClient
import datetime
import calendar
import sys
import os
import time
import math
from bson.objectid import ObjectId
from collections import OrderedDict, Counter, defaultdict
import pandas as pd
import numpy as np
import xlsxwriter
import multiprocessing
import threading
import pickle
import logging


sys.path.append(os.path.abspath(__file__))
mongo = MongoClient(host='10.8.8.111', port=27017, connect=False)
event_mongo = MongoClient('10.8.8.22:27017')

db = mongo['eventsV4']  # 111埋点相关数据库
onions = mongo['onions']  # 用户、课程相关数据库
cache = mongo['cache']  # cache数据库

events = event_mongo['eventsV4']['eventV4']  # 埋点
events_mock = event_mongo['eventsV4']['mockEvents']  # 埋点(模拟)
events_test = event_mongo['eventsV4']['testEvents']  # 测试用埋点
events_xinyu = event_mongo['eventsV4']['xinyuEvents']  # 新宇脚本用埋点

events_v4 = db['eventV4']  # 4.0之后版本全量埋点数据


# env == "dev" -> events mock数据
# env == "test" -> events test数据
# env == "product" -> 生产环境数据
# env == "xinyu" -> 新宇脚本用的埋点
def switch_events(env):
    events_map = {
        "dev": events_mock,
        "test": events_test,
        "product": events,
        "xinyu": events_xinyu
    }
    return events_map[env]

DEFAULT_ENV = "product"

orders = db['orders']  # 订单
orderEvents = db['orderEvents']  # 订单事件

users = onions['users']  # 用户
schools = onions['schools']  # 学校
rooms = onions['rooms']  # 班级

taskReward = onions['taskreward']  # 任务奖励
taskStatus = onions['taskstatuses']

coupons = onions['trialcoupons']  # 体验券
circles = onions['circles']  # 洋葱圈

chapters = onions['chapters']  # 章节
themes = onions['themes']  # 主题
topics = onions['topics']  # 知识点
videos = onions['hypervideos']  # 视频
problems = onions['problems']  # 题目

userAttr = cache['userAttr']  # cache userAttr
deviceAttr = cache['deviceAttr']  # cache deviceAttr

ONLINE_DATE = datetime.datetime(2017, 1, 8, 16)  # 4.2版本上线时间
FEB = datetime.datetime(2017, 2, 5, 16)

now = datetime.datetime.now()
TODAY = datetime.datetime(now.year, now.month, now.day) - datetime.timedelta(hours=8)  # 今天凌晨12点的日期

A_WEEK_AGO = TODAY - datetime.timedelta(weeks=1)  # 一周前

YESTERDAY = TODAY - datetime.timedelta(days=1)  # 昨天

# 4.2上线前一周数据对比 时间段: 20161224-20161230
START_BEFORE_ONLINE = datetime.datetime(2016, 12, 23, 16)
END_BEFORE_ONLINE = datetime.datetime(2016, 12, 30, 16)

NUM_OF_PROCESS = 7  # 多进程时的进程数


def get_week_day(weekday):
    """
    获得今天之前的本周或上周星期几的凌晨十二点时间, 比如想知道这个周六的日期(如果今天等于或大于周六, 否则得到的上周六的日期): get_week_day(6)
    :param weekday: 星期一:1, 星期二:2,... 星期天: 7.
    :return: 日期
    """
    idx = ((TODAY+datetime.timedelta(hours=8)).weekday() + 1) % 7
    return TODAY - datetime.timedelta(idx-weekday+(0 if idx >= weekday else 7))


# END = get_week_day(1)  # 本周一凌晨12点日期
# START = END - datetime.timedelta(weeks=1)  # 本周一的一周前日期

# Excel表单格式
HEADER_FORMAT = {
    'bold': 1,  # 字体粗细
    'border': 1,  # 框线粗细
    'align': 'center',  # 水平方向对齐
    'valign': 'vcenter',  # 垂直方向对齐
    'fg_color': 'green',  # 背景颜色
    'color': 'white',  # 字体颜色,
    'font_size': 14
    }


def write_excel(data, no, s2e):
    """
    将数据写入excel文件
    :param data: 二维数组, 数据格式如下:
    [
        ['表头1'],
        ['步骤', 'uv', '转化率'],
        ['步骤1', 10],
        ['步骤2', 5, '50%'],
        ['步骤3', 1, '10%'],
        ['表头2'],
        [['视频类型', 0, 0], ['普通教学视频', 1, 2], ['洋葱真人秀视频', 3, 6]]  # 如果为二维数组, 表示该行需要分块合并单元格,数据依次为[内容, 合并开始列,合并结束列]
        ['指标', '用户数', '付费用户', '付费率'],
        ['指标1', 100, 10, '10%'],
        ['指标2', 100, 10, '10%']
    ]
    :param no: 需求编号, 可以为数字或字符串, 2 或 '02'
    :param s2e: 起始时间, 如: '20170109-20170115'
    :return:
    """
    if type(no) is int:
        no = ('' if no//10 else '0') + str(no)
    workbook = xlsxwriter.Workbook('data/req' + no + '-' + s2e + '.xlsx')
    worksheet = workbook.add_worksheet('req'+no)

    width = max([len(d) for d in data]) - 1
    worksheet.set_column(0, 0, 25)
    worksheet.set_column(1, width, 15)
    header_format = workbook.add_format(HEADER_FORMAT)
    align_center = workbook.add_format({'align': 'center'})
    # align_right = workbook.add_format({'align': 'right'})

    r = 0
    for i, d in enumerate(data):
        if len(d) == 1:  # 如果该数据长度为1, 则认为是表头, 表头单元格宽度设为下一行数据的宽度
            r += 2 if i != 0 else 0
            w = 1 if i+1 >= len(data) else len(data[i+1])-1 if not all(isinstance(dd, list) for dd in data[i+1]) else width
            worksheet.merge_range(r, 0, r, w, d[0], header_format)
            r += 1
        elif all(isinstance(i, list) for i in d):  # 如果元素是二维数组, 分块合并单元格
            for dd in d:
                if dd[1] >= dd[2]:
                    worksheet.write(r, dd[1], dd[0], align_center)
                else:
                    worksheet.merge_range(r, dd[1], r, dd[2], dd[0], align_center)
            r += 1
        else:
            worksheet.write_row(r, 0, d)
            r += 1
    workbook.close()


def rotate_matrix(data):
    c = len(data)
    r = len(data[0])
    res = [[0 for x in range(c)] for y in range(r)]
    for i in range(c):
        for j in range(r):
            res[j][i] = data[i][j]
    return res


def percent(d1, d2, r=2):
    """
    百分比 (格式: '10.23%')
    :param d1: 分子
    :param d2: 分母
    :param r: 小数点后位数, 默认2
    :return: 百分比, 如果分母为0的话, 返回 NA
    """
    return str(round(d1/d2*100, r)) + '%' if d2 else 'NA'


def start_2_end(start, end, time_difference=True):
    """
    获得起始时间 "2016.05.01-2016.05.10" 格式的字符串
    :param start: 开始时间
    :param end: 结束时间
    :param time_difference 是否需要加八小时的时差,False直接打印传入的start和end
    :return: "2016.05.01-2016.05.10"格式的字符串
    """

    if time_difference:
        start = start + datetime.timedelta(hours=8)
        # end = end + datetime.timedelta(hours=8)

    return str(start.strftime("%Y%m%d")) + '-' + str(end.strftime("%Y%m%d"))


def get_show_videos():
    """
    获取所有洋葱真人秀视频, 按照数据库中的order顺序返回
    :return: [{id, name, order, duration}, ]
    """
    show_videos = list(videos.find({"tag": "show"}, {"name": 1, "order": 1, "duration": 1}))
    return sorted(show_videos, key=lambda k: k["order"])


def unpack(iterable):
    """
    unpack a nested array
    :param iterable: input array, e.g. [[1, 2], [3, 4], [4]]
    :return: unpacked array, e.g. [1, 2, 3, 4, 4]
    """
    result = []
    for x in iterable:
        if type(x) is list:
            result.extend(unpack(x))
        else:
            result.append(x)
    return result


def get_active_users(start, end, platform=None, cond=None, input_users=None, count=True):
    """
    从cache.userAttr中
    计算某段时间内的活跃注册用户(如果input_users为None)
    计算某些用户在某段时间的留存(如果input_users不为None)
    :param start: 开始时间
    :param end: 结束时间
    :param platform: android/ios/pc, 可以为字符串或数组; 如果为None, 则表示所有平台
    :param cond: 其他条件, e.g. {'payments.0': {$exists:True}}
    :param input_users: 初始输入用户
    :param count: 如果只需要知道活跃用户的个数则为True, 如果需要活跃用户的id则为False
    :return: 返回活跃用户的ids或个数
    """
    query = dict()

    days = (end - start).days
    dates = [s.strftime("%Y%m%d") for s in [start+datetime.timedelta(hours=8+24*i) for i in range(days)]]

    if platform is None:
        query.update({"daily": {"$elemMatch": {"$in": dates}}})
    else:
        platform = platform if type(platform) is list else [platform]
        q = [{p+'Daily': {"$elemMatch": {"$in": dates}}} for p in platform]
        query.update({"$or": q})

    if cond is not None:
        query.update(cond)

    if input_users is not None:
        query.update({'user': {"$in": list(map(str, input_users))}})

    if count:
        active = userAttr.count(query)
    else:
        active = list(userAttr.find(query, {'user': 1, '_id': 0}))
        active = [d['user'] for d in active if d['user']]
    return active


def eventtime_2_datetime(t):
    """
    将eventTime(前端时间)转化为datetime形式(UTC时间)
    :param t: 时间戳
    :return: datetime格式的时间(UTC时间)
    """
    return datetime.datetime.fromtimestamp(t/1000) - datetime.timedelta(hours=8)


def datetime_2_timestamp(d):
    """
    将datetime时间(UTC)转化成时间戳
    :param d: datetime格式时间
    :return: 时间戳
    """
    return calendar.timegm(d.timetuple()) * 1000


def generate_sheet(data, workbook, sheetname):
    sheet = workbook.add_worksheet(sheetname)
    for r, row in enumerate(data):
        for c, col in enumerate(row):
            sheet.write(r, c, col)
