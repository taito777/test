from airflow import DAG
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.hooks.hive_hooks import HiveCliHook
from datetime import datetime
from datetime import datetime, timedelta
from airflow.utils import dates
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils import dates


# 定义 DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
    'retries': 1
}

dag = DAG('hive_connection_example', default_args=default_args, schedule_interval='@once')
# Create a HiveHook using the connection ID
#hive_hook = HiveServer2Hook(hiveserver2_conn_id='hiveserver2_default')

# Python 函数：连接到 Hive 并执行查询
def execute_hive_query():
    print("AAAAAAAA")
    # 连接到 Hive
    hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')
    #hive_hook = HiveServer2Hook(hiveserver2_conn_id='hiveserver2_default')
    print("BBBBBBBB")

    # 执行查询
    query = "select count(*) from web_log"
    result = hive_hook.get_records(sql_query)
    #result = hive_hook.run_cli(hql=query)
    print("CCCCCCCC")

    # 打印查询结果
    print(result)

# 创建 PythonOperator 任务
execute_query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_hive_query,
    dag=dag
)

# 设置任务依赖关系
execute_query_task