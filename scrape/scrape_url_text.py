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
        db_creds = {'pi4': creds['pi4_db'],'scraped': creds['scraped_news']}
        print(db_creds)

    return db_creds


def get_hostorical_news_links():
    connection = psycopg2.connect(user=get_creds()['pi4']['user'],
                                  password=get_creds()['pi4']['password'],
                                  host=get_creds()['pi4']['host'],
                                  port=get_creds()['pi4']['port'],
                                  database=get_creds()['pi4']['database'])
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
        df.to_sql(schema=get_creds()['scraped']['schema'], name=get_creds()['scraped']['table_name'], con=get_creds()['pi4']['engine'], if_exists='replace', index=False)
        print('printing DF -------------------------')
        print(df)
