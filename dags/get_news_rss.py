from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
import os

db_connection = 'pi4_postgresdb'
main_dir = '/home/pi/data_pipelines'

default_args = {
'owner'                 : 'JDrago',
'depend_on_past'        : False,
'start_date'            : datetime(2022, 1, 31)
}

with DAG('get_news_rss', default_args=default_args, schedule_interval="0 * * * *", catchup=False,
         template_searchpath=os.path.join(main_dir)) as news_grab_dag:

    rss_news_task = BashOperator(
        task_id='rrs_news',
        bash_command=str('python3 ' + os.path.join(main_dir, "news", "get_news_rss.py ")),
        dag=news_grab_dag)

    news_insert_task = PostgresOperator(
        task_id='news_insert',
        sql=os.path.join('transform', 'news_insert.sql'),
        postgres_conn_id=db_connection,
        autocommit=True,
        dag=news_grab_dag
    )

rss_news_task >> news_insert_task

