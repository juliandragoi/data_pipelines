from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
'owner'                 : 'airflow',
'depend_on_past'        : False,
'start_date'            : datetime(2022, 1, 31)
}

with DAG('news_docker_dag', default_args=default_args, schedule_interval="30 * * * *", catchup=False) as dag:

    task_1 = DockerOperator(
        dag=dag,
        task_id='news_container',
        image='news_rss',
        auto_remove=True,
        command="python3 /home/news/get_news_rss.py"
    )
