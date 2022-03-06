import scrapy
import re
import pandas as pd
import psycopg2
import yaml
from pathlib import Path
import os
import re
import json


def get_pstgrs_conn():

    connection = psycopg2.connect(user=get_creds()['pi4']['user'],
                                  password=get_creds()['pi4']['password'],
                                  host=get_creds()['pi4']['host'],
                                  port=get_creds()['pi4']['port'],
                                  database=get_creds()['pi4']['database'])
    connection.autocommit = True
    return connection


def get_creds():
    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'utils', 'config.yaml')

    with open(file_location, 'r') as stream:
        creds = yaml.safe_load(stream)
        db_creds = {'pi4': creds['pi4_db'], 'scraped': creds['scraped_news']}

    return db_creds


def get_news_links(auth):
    cursor = auth.cursor()

    data = []

    try:
        postgreSQL_select_Query = "select post_id, link from core.news;"

        cursor.execute(postgreSQL_select_Query)
        print("Selecting rows................")
        mobile_records = cursor.fetchall()

        print("Print each row and it's columns values")
        for row in mobile_records:
            print(row)
            data.append(row[0])

        return data

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        if auth:
            cursor.close()
            auth.close()
            print("Postgres connection is closed")

#
# def single_insert(conn, insert_req):
#     """ Execute a single INSERT request """
#     cursor = conn.cursor()
#     try:
#         cursor.execute(insert_req)
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     cursor.close()


link = get_news_links(get_pstgrs_conn())

article_text_list = []

class LinkCheckerSpider(scrapy.Spider):
    name = 'link_checker'
    start_urls = link

    def parse(self, response):
        """ Main function that parses downloaded pages """
        # Print what the spider is doing
        # print(response.url)
        # Get all the <a> tags
        a_selectors = response.xpath("//p").extract()
        # print(a_selectors)

        for i in a_selectors:
            cleanr = re.compile('<.*?>')
            cleantext = re.sub(cleanr, ' ', i)
            data = response.url, cleantext
            article_text_list.append(data)

        df = pd.DataFrame(article_text_list, columns=['link', 'text'])
        df = df[df['text'].notna()]

        # for index, row in df.iterrows():
        #     print(row)
        #     escaped_link = re.escape(row['link'])
        #     escaped_link = json.dumps(escaped_link)
        #     escaped_text = re.escape(row['text']).replace("\\", "")
        #     escaped_text = json.dumps(escaped_text)
        #
        #     query = str('SET SEARCH_PATH TO staging; INSERT into staging.news_scraped(link, text) values (' + "'" + escaped_link+ "'" + ',' + " ' " + escaped_text + "'" +');')
        #     print(query)
        #     single_insert(get_pstgrs_conn(), query)
        #     get_pstgrs_conn().close()

        df.to_csv('scraped_news.csv', index=False)
        #
        # db_in = pd.read_csv('scraped_news.csv')
        # db_in.to_sql(schema=get_creds()['scraped']['schema'], name=get_creds()['scraped']['table_name']
        #              ,con=get_creds()['scraped']['engine'], if_exists='replace', index=False)

        # df.to_sql(schema=get_creds()['scraped']['schema'], name=get_creds()['scraped']['table_name'],
        #           con=get_creds()['scraped']['engine'], if_exists='replace', index=False)
