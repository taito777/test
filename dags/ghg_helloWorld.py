from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# 実行するPython関数を定義
def print_hello_world():
    try:
        print("TEST処理開始")

        print('Hello world!')

        print("TEST処理終了")
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
    'ghg_print_hello_world',            # DAGのID
    default_args=default_args,
    description='print_hello_worldを試す',
    schedule_interval='@once',
)

# PythonOperatorを使用してタスクを設定
print_hello_world_task = PythonOperator(
    task_id='print_hello_world',   # タスクID
    python_callable=print_hello_world,  # 呼び出すPython関数
    dag=dag,
)

# タスクの実行順序の設定
print_hello_world_task