import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1)
}

db_connection = 'pi4_postgresdb'
main_dir = '/home/pi/data_pipelines'

# **********************
# git pull
# **********************

git_pull = DAG(
    'git_pull',
    schedule_interval=None,
    catchup=False,
    default_args=default_args
)

cmd = 'cd /home/pi/data_pipelines && git pull'

pull = BashOperator(
    task_id='pull_code_from_git_repo',
    bash_command=cmd,
    dag=git_pull)
