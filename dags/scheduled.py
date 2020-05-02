from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2)
}

update_servers_dag = DAG(
    'update_servers',
    schedule_interval="0 4 * * *",
    catchup=False,
    default_args=default_args
)

cmd = 'sudo apt-get dist-upgrade -y && sudo apt-get update && sudo apt-get upgrade -y && sudo apt autoremove -y'

t1 = SSHOperator(
    ssh_conn_id='pi1_ssh',
    task_id='pi1_update',
    command=cmd,
    dag=update_servers_dag)


t2 = BashOperator(
    task_id='pi2_update',
    bash_command=cmd,
    dag=update_servers_dag)


t3 = SSHOperator(
    ssh_conn_id='pi3_ssh',
    task_id='pi3_update',
    command=cmd,
    dag=update_servers_dag)


t4 = SSHOperator(
    ssh_conn_id='pi4_ssh',
    task_id='pi4_update',
    command=cmd,
    dag=update_servers_dag)


t5 = SSHOperator(
    ssh_conn_id='pi5_ssh',
    task_id='pi5_update',
    command=cmd,
    dag=update_servers_dag)
