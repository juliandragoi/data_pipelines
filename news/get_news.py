from newsapi import NewsApiClient
# from AuthFile import get_news_key, get_news_engine

import pandas as pd
from datetime import datetime
from utils.helpers import get_news_api_key, get_engine

# Init
newsapi = NewsApiClient(api_key=get_news_api_key())


def get_sources():

    sources = newsapi.get_sources()
    source_dump = sources.get('sources')

    for item in source_dump:
        print(item['id'])


def get_articles_content(list_of_sources):

    all_content = []
    for source in list_of_sources:
        print('getting news for: ' + str(source))
        content = newsapi.get_everything(sources=str(source),language='en', page_size=100)
        all_content.append(content)

    return all_content


if __name__ == '__main__':

    sources_list = ['bbc-news', 'bloomberg', 'cnbc', 'abc-news', 'cnn', 'crypto-coins-news'
        , 'financial-post', 'fox-news', 'google-news', 'hacker-news', 'independent']

    sources_list_string = ', '.join(map(str, sources_list))

    cont = get_articles_content(sources_list)

    posts = []
    for i in cont:
        articles = i['articles']
        for a in articles:
            post = a['title'],a['source']['name'], a['description'], a['content']
            posts.append(post)

    print(posts)


    df = pd.DataFrame(posts, columns=['title', 'brand', 'description', 'content'])
    df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

    print(df)

    df.to_sql(schema='staging', name='api_news', con=get_engine(), if_exists='replace')
