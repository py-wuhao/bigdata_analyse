import os

import pandas as pd
from clickhouse_driver import Client

ck_host = os.environ.get('CK_HOST') or '127.0.0.1'
client = Client(host=ck_host, port=9000, user='wuhao', password='123456')

file1 = r"G:\迅雷下载\tap4fun竞赛数据\tap4fun竞赛数据\tap_fun_test.csv"
file2 = r"G:\迅雷下载\tap4fun竞赛数据\tap4fun竞赛数据\tap_fun_train.csv"
data_list = []
for path in [file1, file2]:
    data = pd.read_csv(path)
    data = data[
        ['user_id', 'register_time', 'pvp_battle_count', 'pvp_lanch_count', 'pvp_win_count', 'pve_battle_count',
         'pve_lanch_count', 'pve_win_count', 'avg_online_minutes', 'pay_price', 'pay_count']
    ]
    data_list.append(data)
df = pd.concat(data_list)

loc = 0
n = 30000
while True:
    tmp_df = df.iloc[loc:loc + n]
    loc += n
    if not len(tmp_df):
        break
    tmp_df['register_time'] = tmp_df['register_time'].apply(lambda x: pd.Timestamp(x, tz='Asia/Shanghai'))
    client.insert_dataframe('insert into test.age_of_barbarians values', tmp_df)
