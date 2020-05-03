
from newsapi import NewsApiClient
# from AuthFile import get_news_key, get_news_engine
from ..helpers import get_news_api_key, get_engine
import pandas as pd
from datetime import datetime

# Init
newsapi = NewsApiClient(api_key=get_news_api_key())


def get_sources():

    sources = newsapi.get_sources()
    source_dump = sources.get('sources')

    for item in source_dump:
        print(item['id'])


sources_list = ['bbc-news', 'bloomberg', 'cnbc', 'abc-news', 'bloomberg', 'cnbc', 'cnn', 'crypto-coins-news'
    , 'financial-post', 'fox-news', 'google-news', 'hacker-news', 'independent', 'rt' 'the-huffington-post'
    , 'the-new-york-times', 'usa-today']

sources_list_string = ', '.join(map(str, sources_list))


def get_articles_content():

    content = newsapi.get_everything(sources=sources_list_string,language='en', page_size=100)

    return content['articles']


posts = []
for i in get_articles_content():
    post = i['title'],i['source']['name'], i['description'], i['content']
    posts.append(post)


df = pd.DataFrame(posts, columns=['title', 'brand', 'description', 'content'])
df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

print(df)

df.to_sql(schema='staging', name='api_news', con=get_engine(), if_exists='replace', index=False)
