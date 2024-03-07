from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils import dates
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.hooks.hive_hooks import HiveCliHook
#from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.models import DAG

def print_hello():
    print('AAAAAAAAA123')
    #hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')
    # 执行查询
    #sql_query = "select count(*) from web_log"
    #result = hive_hook.get_records(sql_query)
    #print(result)
    #return result


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
dag = DAG('hello_hive2', default_args=default_args, schedule_interval='@once')

# 定义一个 PythonOperator,用于执行打印 Hello world! 的任务
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# 设置任务的依赖关系
hello_task 