from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.ssh.operators.ssh import SSHOperator
 
args = {
    'owner': 'tak',
    'start_date': days_ago(2),
}
 
dag = DAG(
    dag_id='example_remote_shell',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example']
)
 
task01 = SSHOperator(
    task_id="ssh_task01",
    ssh_conn_id="tak-df-test",
    command="echo Hello",
    dag=dag)
 
task02 = SSHOperator(
    task_id="ssh_task02",
    ssh_conn_id="tak-df-test",
    command="echo World",
    dag=dag)
 
task03 = SSHOperator(
    task_id="ssh_task03",
    ssh_conn_id="tak-df-test",
    command='hive -e "show databases;"',
    dag=dag)
 
task01 >> task02 >> task03