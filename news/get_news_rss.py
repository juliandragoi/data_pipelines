import feedparser
import pandas as pd
from datetime import datetime
import re
from sqlalchemy import create_engine
import yaml

feeds = ['http://feeds.bbci.co.uk/news/rss.xml'
            ,'http://feeds.bbci.co.uk/news/world/rss.xml'
            ,'http://feeds.bbci.co.uk/news/science_and_environment/rss.xml'
            ,'http://feeds.bbci.co.uk/news/technology/rss.xml'
            ,'https://news.google.com/rss?x=1571747254.2933&hl=en-US&gl=US&ceid=US:en'
            ,'https://www.reddit.com/r/worldnews/.rss'
            ,'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/world/rss.xml'
            ,'https://www.buzzfeed.com/world.xml'
            ,'https://www.aljazeera.com/xml/rs s/all.xml'
            ,'https://www.thecipherbrief.com/feed'
            ,'http://rss.cnn.com/rss/edition_world.rss'
            ,'https://www.theguardian.com/world/rss'
            ,'https://www.huffpost.com/section/front-page/feed?x=1'
            ,'http://rssfeeds.usatoday.com/UsatodaycomNation-TopStories'
            ,'https://www.politico.com/rss/politicopicks.xml'
            ,'https://www.lifehacker.com/rss'
            ,'https://www.yahoo.com/news/rss'
            ,'https://www.latimes.com/local/rss2.0.xml'
            ,'https://feeds.npr.org/1008/rss.xml'
            ,'https://www.medium.com/feed/topic/politics'
            ,'https://www.wired.com/feed/category/ideas/latest/rss'
            ,'https://www.medium.com/feed/topic/society'
            ,'https://www.medium.com/feed/topic/culture'
            ,'https://www.medium.com/feed/topic/equality'
            ,'https://www.medium.com/feed/topic/health']


def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext


def get_source(url):
    source = re.findall(r'https?://\w+.(\w+)',url)
    return source[0]


def get_posts():
    posts = []
    for url in feeds:
        print(url)
        try:
            feed = feedparser.parse(url)
            source = get_source(url)
            for post in feed.entries:
                print(post)
                row = post.title, post.summary, post.link, source, post.author, post.published
                posts.append(row)
        except:
            pass
    posts_df = pd.DataFrame(posts, columns=['title', 'summary', 'link', 'source', 'author', 'published'])
    posts_df['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))

    return posts_df


if __name__ == '__main__':

    with open("utils/config.yaml", 'r') as stream:
        creds = yaml.safe_load(stream)
        news_creds = creds['rss_news_ingest']

    for i in feeds:
        data = get_source(i)
        print(data)

    data = get_posts()

    engine = create_engine(news_creds['engine'], convert_unicode=True)
    data.to_sql(schema=news_creds['schema'], name=news_creds['table_name'], con=engine, if_exists='replace')

