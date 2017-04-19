import config as cfg

onions = cfg.conn8["onion40-backup"]


# 计算渠道号注册用户
def channel_register(channel):
    users = onions["users"]
    pipeline = [
        {"$match": {"channel": channel}},
        {"$group": {"_id": None, "users": {"$addToSet": "$_id"}}}
    ]
    x = list(users.aggregate(pipeline))
    if len(x) == 0:
        return list()

    return [str(t) for t in x[0]["users"]]


# 计算渠道号注册教师用户
def channel_teachers(channel):
    users = onions["users"]
    pipeline = [
        {"$match": {"channel": channel, "role": "teacher"}},
        {"$group": {"_id": None, "users": {"$addToSet": "$_id"}}}
    ]
    x = list(users.aggregate(pipeline))
    if len(x) == 0:
        return list()

    return [str(t) for t in x[0]["users"]]


# 计算教师用户名下班级学生
def get_teacher_students(teacher_list):
    rooms = onions["rooms"]
    teacher_id = cfg.get_validate_id(teacher_list)
    pipeline = [
        {"$unwind": "$owners"},
        {"$match": {"owners": {"$in": teacher_id}}},
        {"$unwind": "$members"},
        {"$group": {"_id": None, "users": {"$addToSet": "$members"}}}
    ]
    x = list(rooms.aggregate(pipeline))

    if len(x) == 0:
        return list()

    return [str(t) for t in x[0]["users"]]


# 计算user_list中,激活用户
def get_activated_users(user_list):
    users = onions["users"]
    user_ids = cfg.get_validate_id(user_list)
    pipeline = [
        {"$match": {"_id": {"$in": user_ids}, "dailySignIn.times": {"$gt": 0}}},
        {"$group": {"_id": None, "users": {"$addToSet": "$_id"}}}
    ]
    x = list(users.aggregate(pipeline))

    if len(x) == 0:
        return list()

    return [str(t) for t in x[0]["users"]]


# 利用user_attr表,计算location的user集合
# location 是个re pattern
def get_location_users(location):
    user_attr = cfg.conn["cache"]["userAttr"]
    pipeline = [
        {"$match": {"location": {"$regex": location}}},
        {"$group": {"_id": None, "users": {"$addToSet": "$user"}}}
    ]
    x = list(user_attr.aggregate(pipeline))

    if len(x) == 0:
        return list()

    return x[0]["users"]
