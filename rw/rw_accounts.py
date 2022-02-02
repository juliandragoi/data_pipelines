import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from pathlib import Path
import yaml
import os

raw_accounts_file = '/Users/juliandragoi/Desktop/RW_list.csv'

rw_accounts = pd.read_csv(raw_accounts_file)
rw_accounts['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))


script_location = Path(__file__).absolute().parent
head, tail = os.path.split(script_location)
file_location = os.path.join(head, 'utils', 'config.yaml')

with open(file_location, 'r') as stream:
    creds = yaml.safe_load(stream)
    news_creds = creds['rss_news_ingest']


data = rw_accounts


engine = create_engine(news_creds['engine'], convert_unicode=True)
data.to_sql(schema=news_creds['schema'], name='rw_accounts', con=engine, if_exists='append', index=False)