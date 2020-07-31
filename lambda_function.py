# import boto3
import os
import json
import sys
#import uuid
#from urllib.parse import unquote_plus
from traceback import print_exc
import yaml

import pymysql



# s3クライアントの取得
# s3_client = boto3.client('s3')


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
        yamlファイルからRDSの設定情報を読み込む
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
        cursor.execute('SELECT * from User')
        rows = cursor.fetchall()
        for row in rows:
            print("{0} {1} {2}".format(row[0], row[1], row[2]))
    except Exception as e:
        print("Connection error:{}".format(e))
        sys.exit()


def lambda_handler(event, context):
    """
        Lambdaから最初に呼ばれるハンドラ関数
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
    except Exception as e:
        print("Connection error:{}".format(e))
        sys.exit()

    cursor = connection.cursor()

    # for record in event['Records']:
    #     print("record: {}".format(record))
    #     bucket = record['s3']['bucket']['name']
    #     upload_bucket = "startlens-media-resized"
    #     # 引数からS3のKey(フォルダ名/ファイル名)を抽出
    #     key = unquote_plus(record['s3']['object']['key'])
    #     # thumbnails/ファイル名のパスを作成
    #     tmpkey = key.replace('/', '')
    #     # S3からダウンロードしたファイルの保存先を設定
    #     download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    #     # 加工したファイルの一時保存先を設定
    #     upload_path = '/tmp/resized-{}'.format(tmpkey)
    #     # S3からファイルをダウンロード
    #     s3_client.download_file(bucket, key, download_path)
    #     # Exif削除と回転処理
    #     remove_exif(download_path, upload_path)
    #     # 処理後のファイルをS3にアップロード（ダウンロード元とバケットを変更する）
    #     s3_client.upload_file(upload_path, upload_bucket, key)


if __name__ == "__main__":
    # データベースの設定情報のymlファイル書き込み
    # set_config()
    # データベース接続テスト
    check_connection()
