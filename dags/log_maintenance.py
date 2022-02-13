from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import os

main_dir = '/home/pi/data_pipelines'

default_args = {
'owner'                 : 'JDrago',
'depend_on_past'        : False,
'start_date'            : datetime(2022, 2, 10)
}

with DAG('log_maintenance', default_args=default_args, schedule_interval='0 0 * * SUN', catchup=False) as log_maintenance:

    clean_command = str('./' + os.path.join(main_dir, "maintenance.py", "cleanup_logs.sh "))

    check_feeds_task = BashOperator(
        task_id='log_maintenance',
        bash_command=clean_command,
        dag=log_maintenance)