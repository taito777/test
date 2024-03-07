# ライブラリたちをインポート
from datetime import datetime, date
from pyspark.sql import Row, SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pds

# 実行するPython関数を定義
def test_pyspark():
    try:
        print("処理開始")

        # Spark処理のためにデータフレーム作る
        spark = SparkSession.builder.getOrCreate()
        data_frame = spark.createDataFrame([
            Row(DATE=datetime(2000, 7, 1, 7, 1), LEVEL="Warning", DESCRIPTION="The world was ended. All systems has been shutdown."),
            Row(DATE=datetime(2000, 7, 1, 7, 2), LEVEL="Error", DESCRIPTION="Who am I? Please tell me. Please."),
            Row(DATE=datetime(2000, 7, 1, 7, 3), LEVEL="Listen", DESCRIPTION="There has no one. Where has everyone gone?")
        ])
        # 作ったデータフレーム見てみる
        data_frame.show()
        # データフレームは何行あるでしょう？
        print("計", data_frame.count(), "行" , len(data_frame.columns), "列")
        # LEVEL列の値がErrorの行を抽出
        print("\n\n")
        data_frame.filter(data_frame["LEVEL"] == "Error").show()

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
    'ghg_test_pyspark',            # DAGのID
    default_args=default_args,
    description='pysparkを試す',
    schedule_interval='@once',
)

# PythonOperatorを使用してタスクを設定
test_pyspark_task = PythonOperator(
    task_id='test_pyspark',   # タスクID
    python_callable=test_pyspark,  # 呼び出すPython関数
    dag=dag,
)

# タスクの実行順序の設定
test_pyspark_task
