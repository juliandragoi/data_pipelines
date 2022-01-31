from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
'owner'                 : 'airflow',
'depend_on_past'        : False,
'start_date'            : datetime(2018, 1, 3)
}

with DAG('docker_dag', default_args=default_args, schedule_interval="30 * * * *", catchup=False) as dag:

    task_1 = DockerOperator(
        dag=dag,
        task_id='news_docker',
        image='dummyorg/dummy_api_tools:v1',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        command='python extract_from_api_or_something.py'
    )
