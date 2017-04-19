#!/usr/bin/python3
# encoding: utf-8
"""
Created on 04/01/2017
@author: Tao
"""
# _*_ coding:utf-8 _*_
from pymongo import MongoClient
import datetime
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

mongo = MongoClient(host='10.8.8.111', port=27017, connect=False)

db = mongo['eventsV4']  # 埋点相关数据库
onions = mongo['onions']  # 用户、课程相关数据库
cache = mongo['cache']  # cache数据库

events = db['eventV4']  # 埋点
events_mock = db['mock']  # 埋点(模拟)

orders = db['orders']  # 订单
orderEvents = db['orderEvents']  # 订单事件

users = onions['users']  # 用户
schools = onions['schools']  # 学校
rooms = onions['rooms']  # 班级

coupons = onions['trialcoupons']  # 体验券
circles = onions['circles']  # 洋葱圈

chapters = onions['chapters']  # 章节
themes = onions['themes']  # 主题
topics = onions['topics']  # 知识点
videos = onions['hypervideos']  # 视频
problems = onions['problems']  # 题目

userAttr = cache['userAttr']  # cache userAttr
deviceAttr = cache['deviceAttr']  # cache deviceAttr

ONLINE_DATE = datetime.datetime(2017, 1, 4, 16)  # 4.2版本上线时间

now = datetime.datetime.now()
TODAY = datetime.datetime(now.year, now.month, now.day) - datetime.timedelta(hours=8)  # 今天凌晨12点的日期


def get_week_day(weekday):
    """
    获得今天之前的本周或上周星期几的日期, 比如想知道这个周六的日期(如果今天等于或大于周六, 否则得到的上周六的日期): get_week_day(6)
    :param weekday: 星期一:1, 星期二:2,... 星期天: 7.
    :return: 日期
    """
    idx = ((TODAY+datetime.timedelta(hours=8)).weekday() + 1) % 7
    if idx >= weekday:
        return TODAY - datetime.timedelta(idx-weekday)
    else:
        return TODAY - datetime.timedelta(7+idx-weekday)


END = get_week_day(1)  # 本周一凌晨12点日期
START = END - datetime.timedelta(weeks=1)  # 本周一的一周前日期

# Excel表单格式
HEADER_FORMAT = {
        'bold': 1,  # 字体粗细
        'border': 1,  # 框线粗细
        'align': 'center',  # 水平方向对齐
        'valign': 'vcenter',  # 垂直方向对齐
        'fg_color': 'green',  # 背景颜色
        'color': 'white'  # 字体颜色
    }


def percent(d1, d2):
    """
    百分比 (格式: '10.23%')
    :param d1: 分子
    :param d2: 分母
    :return: 百分比, 如果分母为0的话, 返回 NA
    """
    return str(round(d1/d2*100, 2)) + '%' if d2 else 'NA'


def start_2_end(start, end):
    """
    获得起始时间 "2016.05.01-2016.05.10" 格式的字符串
    :param start: 开始时间
    :param end: 结束时间
    :return: "2016.05.01-2016.05.10"格式的字符串
    """
    return str((start+datetime.timedelta(hours=8)).strftime("%Y.%m.%d")) + '-' + str(end.strftime("%Y.%m.%d"))


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
