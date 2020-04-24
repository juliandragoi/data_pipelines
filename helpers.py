
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

