import boto3
import datetime
import os
import pandas as pd
import json
import sys
import uuid
from urllib.parse import unquote_plus
from traceback import print_exc
import yaml

import pymysql



# s3クライアントの取得
s3_client = boto3.client('s3')


def set_config():
    """
        RDSの設定をymlファイルに書き込む処理
    """
    with open('config.yml', 'w') as yaml_file:
        yaml.dump({
            'db_server': {
                'endpoint': 'endpoint',
                'username': 'username',
                'password': 'password',
                'database_name': 'dbname',
                'port': 3306
            }
        }, yaml_file, default_flow_style=False)


def check_connection():
    """
        yamlファイルからRDSの設定情報を読み込み、SQL(SELECT文)を発行することでデータベースとのコネクションを確認
    """
    try:
        with open('config.yml', 'r') as yaml_file:
            data = yaml.safe_load(yaml_file)
            endpoint = data['db_server']['endpoint']
            username = data['db_server']['username']
            password = data['db_server']['password']
            database_name = data['db_server']['database_name']
            port = data['db_server']['port']
    except Exception as e:
        print('Exception occured while loading YAML...', file=sys.stderr)
        print(e)
        sys.exit(1)

    # コネクション
    try:
        connection = pymysql.connect(endpoint, user=username, passwd=password, db=database_name, connect_timeout=5)
        cursor = connection.cursor()
        cursor.execute('SELECT id, username from userlang LIMIT 5')
        rows = cursor.fetchall()
        for r in rows:
            print("row: {}".format(r))
    except Exception as e:
        print("Connection error:{}".format(e))
        sys.exit()


def lambda_handler(event, context):
    """
        Lambdaから最初に呼ばれるハンドラ関数
        接続維持ではなく都度接続するためグローバルではなく関数内でコネクションを確立する
    """
    try:
        with open('config.yml', 'r') as yaml_file:
            data = yaml.safe_load(yaml_file)
            endpoint = data['db_server']['endpoint']
            username = data['db_server']['username']
            password = data['db_server']['password']
            database_name = data['db_server']['database_name']
            port = data['db_server']['port']
    except Exception as e:
        print('Exception occured while loading YAML...', file=sys.stderr)
        print(e)
        sys.exit(1)

    # コネクション（都度接続を確立）
    try:
        connection = pymysql.connect(endpoint, user=username, passwd=password, db=database_name, connect_timeout=5)
    except Exception as e:
        print("Connection error:{}".format(e))
        sys.exit()

    """
        csvファイルの列要素
        language:
            'en': 英語データ
            'zh': 中国語データ
            'ko': 韓国語データ
        status:
            0: 更新なし
            1: 新規登録
            2: 更新
            3: 削除
    """
    for record in event['Records']:
        print("record: {}".format(record))
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        # S3からダウンロードしたファイルの保存先を設定
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        # S3からファイルをダウンロード
        s3_client.download_file(bucket, key, download_path)

        print("Batch operation start")
        # Excelから作成のcsvファイルはencodingが必要
        df = pd.read_csv(download_path, encoding='shift-jis')
        # Insert処理
        df_create = df[df.status == 1]
        cursor = connection.cursor()
        try:
            dt_now = datetime.datetime.now()
            print("datetime: {}".format(dt_now))
            for index, row in df_create.iterrows():
                sql = "INSERT INTO userlang" \
                    "(owner_id, language, username, self_intro, address_prefecture, address_city, address_steet, entrance_fee, business_hours, holiday, upload_date)" \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                print(sql)
                cursor.execute(sql, (int(row['owner_id']), row['username'], row['language'], row['self_intro'], row['address_prefecture'], row['address_city'], row['address_steet'], row['entrance_fee'], row['business_hours'], row['holiday'], dt_now))
            connection.commit()
            print("Insert query is safely commited!")
        except Exception as e:
            cursor.rollback()
            print("MySQL Database Insert Error: {}".format(e))
        finally:
            cursor.close()
            print("cursor is closed")

        # Update処理
        df_update = df[df.status == 2]
        cursor = connection.cursor()
        try:
            dt_now = datetime.datetime.now()
            print("update datetime: {}".format(dt_now))
            for index, row in df_update.iterrows():
                owner_id = row['owner_id']
                sql = "UPDATE userlang SET language = %s, username = %s, self_intro = %s, address_prefecture = %s, address_city = %s, address_steet = %s," \
                    "entrance_fee = %s, business_hours = %s, holiday = %s, upload_date = %s WHERE id = %s"
                print(sql)
                cursor.execute(sql, (row['language'], row['username'], row['self_intro'], row['address_prefecture'], row['address_city'], row['address_steet'], row['entrance_fee'], row['business_hours'], row['holiday'], dt_now, int(owner_id)))
            connection.commit()
            print("Update query is safely commited!")
        except Exception as e:
            cursor.rollback()
            print("MySQL Database Update Error: {}".format(e))
        finally:
            cursor.close()
            print("cursor is closed")

        # 削除処理
        df_delete = df[df.status == 3]
        cursor = connection.cursor()
        try:
            for index, row in df_delete.iterrows():
                owner_id = row['owner_id']
                sql = "DELETE FROM userlang WHERE id = %s"
                print(sql)
                cursor.execute(sql, (int(owner_id)))
            connection.commit()
            print("Delete query is safely commited!")
        except Exception as e:
            cursor.rollback()
            print("MySQL Database Delete Error: {}".format(e))
        finally:
            cursor.close()
            print("cursor is closed")

        print("Batch operation ended")


if __name__ == "__main__":
    # データベースの設定情報のymlファイル書き込み
    # set_config()
    # データベース接続テスト
    # check_connection()
