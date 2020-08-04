
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from airflow.models import Variable
import psycopg2
import json



def get_connection():

    conn_dict = BaseHook.get_connection('pi4_postgresdb')

    return psycopg2.connect(host=conn_dict.host,
                            port=conn_dict.port,
                            database=conn_dict.schema,
                            user=conn_dict.login,
                            password=conn_dict.get_password())


def get_engine():
    conn_dict = BaseHook.get_connection('pi4_postgresdb')

    con = 'postgresql://' + str(conn_dict.login) + ':' + str(conn_dict.get_password()) + '@' + str(
        conn_dict.host) + ':' + str(conn_dict.port) + '/' + str(conn_dict.schema)
    engine = create_engine(con)
    return engine



def get_news_api_key():

    return Variable.get('secret_news_api_key')


def twitter_consumer_key():
    return Variable.get('secret_twitter_consumer_key')


def twitter_consumer_secret():
    return Variable.get('secret_twitter_consumer_secret')


def twitter_access_token():
    return Variable.get('secret_twitter_access_token')


def twitter_access_secret():
    return Variable.get('secret_twitter_access_secret')


def reddit_creds():
    return Variable.get('secret_reddit_creds')
