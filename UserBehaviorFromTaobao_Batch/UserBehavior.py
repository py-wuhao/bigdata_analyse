import os

import pandas as pd
from clickhouse_driver import Client


ck_host = os.environ.get('CK_HOST') or '127.0.0.1'
client = Client(host=ck_host, port=9000, user='wuhao', password='123456')


def read_csv(path):
    chunks = pd.read_csv(
        path,
        iterator=True,
        header=None,
        names=['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
    )
    while True:
        try:
            df = chunks.get_chunk(10000)
        except StopIteration:
            return
        except Exception as e:
            raise e

        yield df


def main():
    path = r'G:\迅雷下载\UserBehavior.csv\UserBehavior.csv'
    # path = r'G:\迅雷下载\UserBehavior.csv\aaa.csv'
    st = pd.Timestamp('2017-11-25 00:00:00', tz='Asia/Shanghai')
    et = pd.Timestamp('2017-12-03 23:59:59', tz='Asia/Shanghai')
    for df in read_csv(path):
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).apply(
            lambda x: x.tz_convert('Asia/Shanghai'))
        df = df[(df['timestamp'] >= st) & (df['timestamp'] <= et)]
        df[['user_id', 'item_id', 'category_id', 'behavior_type']] = df[
            ['user_id', 'item_id', 'category_id', 'behavior_type']].astype(str)
        res = client.insert_dataframe('INSERT INTO test.user_behavior VALUES', df)
        print(res)


if __name__ == '__main__':
    main()
