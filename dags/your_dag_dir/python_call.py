from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils import dates
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.ssh.operators.ssh import SSHOperator

def python_call():
    return 'python_call start!'



# 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# 
#with DAG('python_call', default_args=default_args, schedule_interval='@once') as dag:
dag = DAG('python_call', default_args=default_args, schedule_interval='@once')
# 
pythoncall = SSHOperator(
    task_id="python_call",
    ssh_conn_id="ssh_18",  # AirflowでSSH,XX.XX.XX.18
    command=f'[ -f /home/hd-dev-user01@ts.tdh.tepcube.jp ] && echo "File exists" || echo "File does not exist"',
    dag=dag,
)


# 
pythoncall
