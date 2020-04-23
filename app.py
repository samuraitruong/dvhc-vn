# -- coding:utf8 --

import pandas as pd
from pandas import DataFrame
import sys
from dotenv import load_dotenv
import os
import pymongo
import time
from bson.objectid import ObjectId

load_dotenv()

mongoClient = pymongo.MongoClient(os.getenv("DB_CONNECTION_STRING"))
db = mongoClient[os.getenv("DB_NAME")]


def extract_name(name):
    units = ['Tỉnh', 'Thành phố', 'Thị xã', 'Quận', 'Huyện', 'Phường', 'Xã']
    try:
        for u in units:
            if u in name:
                return {
                    'unit': u,
                    'value': name.replace(u, '').strip()
                }
    except:
        print(name)

    return {
        'unit': '',
        'value': name
    }


def write_data_to_db(data, collectionName, overwrite=True):
    col = db[collectionName]
    if overwrite:
        col.drop()
    col.insert_many(data)
    print(f"Write data to collection: %s, length: %d" %
          (collectionName, len(data)))
    batch_size = 1000
    # for index in range(0, len(data), batch_size):
    #     page = data[index:index+batch_size]
    #     db.data.insert_many(page)

    #     print(f'Write %d item from index: %d took : %d' %
    #         (len(page), index))


file = r'input.xls'
df = pd.read_excel(file, encoding=sys.getfilesystemencoding())
# for r in df:
#   print(r)
data = []
dict = {}
items = []
for index, row in df.iterrows():
    province = extract_name(row["Tỉnh Thành Phố"])
    district = extract_name(row["Quận Huyện"])
    area = extract_name(row["Phường Xã"])
    item = {
        '_id': ObjectId(),
        'province': province['value'],
        'provinceType': province['unit'],
        'code': row["Mã TP"],
        'district': district["value"],
        'districtType': district['unit'],
        'districtCode': row["Mã QH"],
        'area': area["value"],
        # "areaType": area["unit"]
        'areaCode': row['Mã PX'],
        'areaType': row['Cấp']
    }
    print(item)
    data.append(item)

    if dict.get(item['province']) == None:
        dict[item['province']] = ObjectId()
        items.append({
            'type': item["provinceType"],
            '_id':  dict[item['province']],
            'code': item['code'],
            'name': item['province']
        })
    if dict.get(item['province'] + item['district']) == None:
        dict[item['province']+item['district']] = ObjectId()
        items.append({
            'type': item['districtType'],
            '_id':   dict[item['province']+item['district']],
            'code': item['districtCode'],
            'name': item['district'],
            'parent_id': dict[item['province']]
        })

    items.append({
        '_id':   ObjectId(),
        'code': item['areaCode'],
        'name': item['area'],
        'type': item['areaType'],
        'parent_id': dict[item['province']+item['district']]
    })

write_data_to_db(items, "items")
write_data_to_db(data, "data")
