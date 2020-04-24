
from newsapi import NewsApiClient
from AuthFile import get_news_key


# --------------------------------------------------------------------------------------------------
# This is the generic way of calling the api
# --------------------------------------------------------------------------------------------------
# url = ('https://newsapi.org/v2/everything?sources='+sources_list_string+get_news_key())
# response = requests.get(url)
# print(response.json())
# --------------------------------------------------------------------------------------------------

# Init
newsapi = NewsApiClient(api_key=get_news_key())


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

    return(content['articles'])


get_sources()