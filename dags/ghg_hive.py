from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyhive import hive
from datetime import datetime, timedelta


# Hiveへの接続とクエリ実行を行うPython関数
def query_hive():
    try:
        print("処理開始")

        # Hive接続情報
        conn = hive.Connection(host="10.191.30.19",
                                port=10000, # HiveServer2 のデフォルトポート
                                username="hd-dev-user02",
                                password="W6fdnp%f#y",
                                database='db_ghg',  # 接続するデータベース名
                                auth='CUSTOM'  # カスタム認証を使用
                                )
        
        print("debug:1")
        
        # PyHiveを使用してクエリ実行
        cursor = conn.cursor()
        print("debug:2")
        cursor.execute("SELECT * FROM ancate_info")
        print("debug:3")
        for result in cursor.fetchall():
            print(result)

        # 接続を閉じる
        cursor.close()
        conn.close()

        print("処理終了")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

# デフォルトパラメータの設定
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAGの定義
dag = DAG(
    'ghg_hive_query',   # DAGのID
    default_args=default_args,
    description='hive_queryを試す',
    schedule_interval='@once',
)

# PythonOperatorを使用してタスクを定義
query_hive_task = PythonOperator(
    task_id='query_hive',   # タスクID
    python_callable=query_hive, # 呼び出すPython関数
    dag=dag,
)

# タスクの実行順序の設定
query_hive_task