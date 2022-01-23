import pandas as pd
from pathlib import Path
import os
import yaml
from datetime import datetime


def get_creds():
    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'utils', 'config.yaml')

    with open(file_location, 'r') as stream:
        creds = yaml.safe_load(stream)
        db_creds = {'pi4': creds['pi4_db'], 'scraped': creds['scraped_news']}

    return db_creds


if __name__ == '__main__':
    script_location = Path(__file__).absolute().parent
    head, tail = os.path.split(script_location)
    file_location = os.path.join(head, 'scrape', 'scraped_news.csv')

    db_in = pd.read_csv('scraped_news.csv')
    db_in['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))
    print('inserting scraped data -----------------------------------------------')
    db_in.to_sql(schema=get_creds()['scraped']['schema'], name=get_creds()['scraped']['table_name']
                 ,con=get_creds()['scraped']['engine'], if_exists='replace', index=False, method='multi')