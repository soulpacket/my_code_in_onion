from pymongo import MongoClient
from bson.objectid import ObjectId

conn = MongoClient("10.8.8.111:27017")
conn8 = MongoClient("10.8.8.8:27017")


# 返回id_list中可转化为objectid的列表,如不可转化则丢弃
def get_validate_id(id_list):
    result = list()
    for _id in id_list:
        if ObjectId.is_valid(_id):
            result.append(ObjectId(_id))

    return result
