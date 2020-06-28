import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1)
}

db_connection = 'pi4_postgresdb'
main_dir = '/home/pi/data_pipelines'


# **********************
# daily server update
# **********************

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


# **********************
# news feeds
# **********************

news_grab_dag = DAG(
    'news_grab',
    schedule_interval='0 6,8,10,12,14,17,18,23 * * *',
    catchup=False,
    template_searchpath=os.path.join(main_dir),
    default_args=default_args
)

rss_news_task = BashOperator(
    task_id='rrs_news',
    bash_command=str('python3 ' + os.path.join(main_dir, "news","get_news_rss.py ")),
    dag=news_grab_dag)

api_news_task = BashOperator(
    task_id='api_news',
    bash_command=str('python3 ' + os.path.join(main_dir, "news","get_news.py ")),
    dag=news_grab_dag)

news_raw_task = PostgresOperator(
        task_id='news_raw_insert',
        sql=os.path.join('transform','news_transform.sql'),
        postgres_conn_id=db_connection,
        autocommit=True,
        dag=news_grab_dag
    )


# **********************
# fashion tweets
# **********************
fashion_tweets_dag = DAG(
    'fashion_tweets',
    schedule_interval='0 3 * * 0',
    catchup=False,
    template_searchpath=os.path.join(main_dir),
    default_args=default_args
)

fashion_tweets_task = BashOperator(
    task_id='fashion_tweets',
    bash_command=str('python3 ' + os.path.join(main_dir, "twee","fashion.py ")),
    dag=fashion_tweets_dag)

fashion_insert = PostgresOperator(
        task_id='fashion_insert',
        sql=os.path.join('transform','fashion_transform.sql'),
        postgres_conn_id=db_connection,
        autocommit=True,
        dag=fashion_tweets_dag
    )

# **********************
# deps
# **********************

rss_news_task.set_downstream(news_raw_task)
api_news_task.set_downstream(news_raw_task)

fashion_tweets_task.set_downstream(fashion_insert)