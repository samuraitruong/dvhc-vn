# -- coding:utf8 --

import pandas as pd
from pandas import DataFrame
import sys
from dotenv import load_dotenv
import os
import pymongo

load_dotenv()

mongoClient = pymongo.MongoClient(os.getenv("DB_CONNECTION_STRING"))
db = mongoClient[os.getenv("DB_NAME")]

print(' Drop data collection')
db.data.drop()
file = r'input.xls'
df = pd.read_excel(file, encoding=sys.getfilesystemencoding())
# for r in df:
#   print(r)
data = []
for index, row in df.iterrows():
    item = {
        'provine': row["Tỉnh Thành Phố"][5::],
        'code': row["Mã TP"],
        'district': row["Quận Huyện"],
        'districtCode': row["Mã QH"],
        'area': row['Phường Xã'],
        'areaCode': row['Mã PX'],
        'areaType': row['Cấp']
    }
    print(item)
    data.append(item)

for index in range(0, len(data), 100):
    page = data[index:index+100]
    db.data.insert_many(page)
