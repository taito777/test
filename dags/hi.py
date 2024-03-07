from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils import dates
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sql_module 

def print_hello():
    sql_module.upload_data([1,2,3],"aa","bb")
    return 'Hello world!'

# 定义 DAG 的默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# 创建一个 DAG 实例
dag = DAG('hello_world', default_args=default_args, schedule_interval='@once')

# 定义一个 PythonOperator，用于执行打印 Hello world! 的任务
hello_task = PythonOperator(
    task_id='print_hive',
    python_callable=print_hello,
    dag=dag,
)

# 设置任务的依赖关系
hello_task