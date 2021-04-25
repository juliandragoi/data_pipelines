from newsapi import NewsApiClient
# from AuthFile import get_news_key, get_news_engine

import pandas as pd
from datetime import datetime
from utils.helpers import get_news_api_key, get_engine

# Init
newsapi = NewsApiClient(api_key=get_news_api_key())

def get_sources(lang, cat):
    list_of_sources = []
    count = 0

    sources = newsapi.get_sources()
    source_dump = sources.get('sources')

    for item in source_dump:
        count = count + 1
        if (item['language'] == lang) & (item['category'] == cat):
            list_of_sources.append(item)

            print(count,':' ,item)

    return list_of_sources

def get_articles_content(list_of_sources):

    all_content = []
    for source in list_of_sources:
        print('getting news for: ' + str(source))
        content = newsapi.get_everything(sources=str(source),language='en', page_size=100)
        all_content.append(content)

    return all_content


if __name__ == '__main__':

    sources_list = ['abc-news', 'bbc-news', 'bloomberg','buzzfeed', 'cnn', 'cbs-news' ,'crypto-coins-news'
        , 'financial-post', 'fox-news', 'google-news', 'hacker-news', 'independent']

    # for i in sources_list:
    #     if any(i in s for s in get_sources()):
    #         print(i)

    # sources_list_string = ', '.join(map(str, sources_list))

    cont = get_articles_content(sources_list)

    posts = []
    for i in cont:
        articles = i['articles']
        for a in articles:
            post = a['title'],a['source']['name'], a['description'], a['content']
            posts.append(post)

    print(posts)

    df = pd.DataFrame(posts, columns=['title', 'brand', 'description', 'content'])
    df['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))

    print(df)

    # df.to_sql(schema='staging', name='api_news', con=get_engine(), if_exists='replace')
