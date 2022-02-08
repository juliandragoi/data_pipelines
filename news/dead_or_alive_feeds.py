import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import yaml
import os
import psycopg2
from pathlib import Path
import requests


def get_feed_list(con_user, con_pass, con_host, con_port, con_database):

    try:
        connection = psycopg2.connect(user=con_user,
                                      password=con_pass,
                                      host=con_host,
                                      port=con_port,
                                      database=con_database)
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from staging.feed_list where status = 'alive'"

        cursor.execute(postgreSQL_select_Query)
        feed_list = cursor.fetchall()
        to_list = []

        for row in feed_list:
            to_list.append(row[0])

        return to_list


    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()


def check_url_status(url):

    r = requests.head(url)
    status  = r.status_code
    captured_at = str(datetime.now().strftime("%Y-%m-%d_%H:%M"))

    return url, status, captured_at


if __name__ == '__main__':

    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'utils', 'config.yaml')

    with open(file_location, 'r') as stream:
        creds = yaml.safe_load(stream)
        news_db_creds = creds['pi4_db']
        news_creds = creds['rss_news_ingest']

    feeds = get_feed_list(news_db_creds['user'], news_db_creds['password'], news_db_creds['host'], news_db_creds['port']
                          , news_db_creds['database'])

    print(feeds)

    feed_status = []

    for i in feeds:
        check_url_status(i)
        feed_status.append(check_url_status(i))

    feed_status_df = pd.DataFrame(data=feed_status, columns=['url', 'status', 'captured_at'])

    print(feed_status_df)

    engine = create_engine(news_creds['engine'], convert_unicode=True)
    feed_status_df.to_sql(schema=news_creds['schema'], name='feed_status', con=engine, if_exists='replace',
                index=False)

