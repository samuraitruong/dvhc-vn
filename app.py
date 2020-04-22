# -- coding:utf8 --

import pandas as pd
from pandas import DataFrame
import sys
from dotenv import load_dotenv
import os
import pymongo
import time

load_dotenv()

mongoClient = pymongo.MongoClient(os.getenv("DB_CONNECTION_STRING"))
db = mongoClient[os.getenv("DB_NAME")]

print('Drop all data in collection')
db.data.delete_many({})
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

batch_size = 500
for index in range(0, len(data), batch_size):
    page = data[index:index+batch_size]
    start = time.process_time()
    db.data.insert_many(page)
    elapsed_time = time.process_time() - start

    print(f'Write %d item from index: %d took : %d' %
          (len(page), index, elapsed_time))
