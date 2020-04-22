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


def extract_name(name):
    units = ['Tỉnh', 'Thành phố', 'Quận', 'Huyện', 'Phường', 'Xã']
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


mongoClient = pymongo.MongoClient(os.getenv("DB_CONNECTION_STRING"))
db = mongoClient[os.getenv("DB_NAME")]

print('Drop all data in collection')
db.data.delete_many({})
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

db.items.drop()
db.items.insert_many(items)
batch_size = 1000
for index in range(0, len(data), batch_size):
    page = data[index:index+batch_size]
    start = time.process_time()
    db.data.insert_many(page)
    elapsed_time = time.process_time() - start

    print(f'Write %d item from index: %d took : %d' %
          (len(page), index, elapsed_time))
