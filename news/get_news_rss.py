import feedparser
import pandas as pd
from datetime import datetime
import re
from sqlalchemy import create_engine
import yaml
from pathlib import Path
import os
import psycopg2


def get_feed_list(con_user, con_pass, con_host, con_port, con_database):

    try:
        connection = psycopg2.connect(user=con_user,
                                      password=con_pass,
                                      host=con_host,
                                      port=con_port,
                                      database=con_database)
        cursor = connection.cursor()
        postgreSQL_select_Query = "select distinct url from staging.feed_list where status = 'alive'"

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


def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext


def get_source(url):
    source = re.findall(r'https?://\w+.(\w+)',url)
    return source[0]


def get_posts(feeds):
    posts = []
    for url in feeds:
        # print(url)
        feed = feedparser.parse(url)
        source = get_source(url)
        try:
            for post in feed.entries:
                # print(post)
                row = post.title, post.summary, post.link, source, post.author, post.published
                posts.append(row)
        except AttributeError:
            for post in feed.entries:
                # print(post)
                row = post.title, ' ', post.link, source, ' ', ' '
                posts.append(row)
        else:
            pass

    posts_df = pd.DataFrame(posts, columns=['title', 'summary', 'link', 'source', 'author', 'published'])
    posts_df['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))

    return posts_df


if __name__ == '__main__':
    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'utils', 'config.yaml')

    with open(file_location, 'r') as stream:
        creds = yaml.safe_load(stream)
        news_db_creds = creds['pi4_db']
        news_creds = creds['rss_news_ingest']

    feeds_list = get_feed_list(news_db_creds['user'], news_db_creds['password'], news_db_creds['host']
                               , news_db_creds['port'], news_db_creds['database'])

    data = get_posts(feeds_list)

    # data.to_csv('test.csv', index=False)

    engine = create_engine(news_creds['engine'], convert_unicode=True)
    data.to_sql(schema=news_creds['schema'], name=news_creds['table_name'], con=engine, if_exists='replace', index=False)
