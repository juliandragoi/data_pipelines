import scrapy
import re
import pandas as pd
import psycopg2
import yaml
from pathlib import Path
import os


def get_creds():
    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'utils', 'config.yaml')

    with open(file_location, 'r') as stream:
        creds = yaml.safe_load(stream)
        db_creds = creds['pi4_db']

    return db_creds


def get_hostorical_news_links():
    connection = psycopg2.connect(user=get_creds()['user'],
                                  password=get_creds()['password'],
                                  host=get_creds()['host'],
                                  port=get_creds()['port'],
                                  database=get_creds()['database'])
    cursor = connection.cursor()

    data = []


    try:
        postgreSQL_select_Query = "select link from staging.news_rss;"

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
        if connection:
            cursor.close()
            connection.close()
            print("Postgres connection is closed")


link = get_hostorical_news_links()

article_text_list = []

class LinkCheckerSpider(scrapy.Spider):
    name = 'link_checker'
    start_urls = link

    def parse(self, response):
        """ Main function that parses downloaded pages """
        # Print what the spider is doing
        print(response.url)
        # Get all the <a> tags
        a_selectors = response.xpath("//p").extract()
        print(a_selectors)

        for i in a_selectors:
            cleanr = re.compile('<.*?>')
            cleantext = re.sub(cleanr, ' ', i)
            data = response.url, cleantext
            article_text_list.append(data)

        df = pd.DataFrame(article_text_list, columns=['link','text'])
        df = df[df['text'].notna()]
        # df.to_csv('test.csv', index=False)
        # df.to_sql(schema=news_creds['schema'], name=news_creds['table_name'], con=engine, if_exists='replace', index=False)
        print('printing DF -------------------------')
        print(df)
