import marketing
import multiprocessing as mp

func_map = {
    "地理位置/用户": marketing.get_location_users,
    "教师用户/学生用户": marketing.get_teacher_students,
    "渠道号/教师用户": marketing.channel_teachers,
    "渠道号/注册用户": marketing.channel_register,
    "用户/激活用户": marketing.get_activated_users
}


# 并集 TODO
def union(func_list, input_param):
    result = list()
    res = set()
    pool = mp.Pool(4)
    for func in func_list:
        result.append(pool.apply_async(func))
    for i in range(len(result)):
        res = set(result[i]) | res
    return list(res)


# 交集
def intersection(func_list, input_param):
    result = list()
    res = set()
    pool = mp.Pool(4)
    for func in func_list:
        result.append(pool.apply_async(func))
    for i in range(len(result)):
        res = set(result[i]) & res
    return list(res)

step_operations = {
    "并集": union,
    "交集": intersection
}


# 事件调用序列
def series(func_list, input_param, step_output=False):
    i = input_param
    v = list()
    for each in func_list:
        print(each)
        if isinstance(each, list):
            funcs = [func_map[t] for t in each[1]]
            x = step_operations[each[0]]
            x = x(funcs, i)
# ["并集",["事件...."]]
        else:
            x = func_map[each]
            x = x(i)
        i = x.copy()
        if step_output:
            v.append(i)
    return tuple([i, v])

# a = series(["渠道号/教师用户", "教师用户/学生用户", "用户/激活用户"], "bd", True)
