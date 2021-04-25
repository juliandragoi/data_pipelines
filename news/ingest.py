from news.get_news import get_articles_content
from AuthFile import get_news_engine
import pandas as pd
import logging
import os
from datetime import datetime
from pandas.io.json import json_normalize


logging.basicConfig(filename=os.path.relpath('ingestion.log'), filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":

    content = get_articles_content()
    news_content_table = pd.DataFrame(json_normalize(content))
    news_content_table['inserted_dt'] = str(datetime.now())
    news_content_table.to_sql('new'+ str(datetime.now().strftime("%Y%b%d_%H")), get_news_engine(), schema='news'
                              , if_exists='append', index=False)



