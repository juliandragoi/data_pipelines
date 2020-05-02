import feedparser
import pandas as pd
from datetime import datetime
from AuthFile import get_news_engine
from bs4 import BeautifulSoup
import re
from io import StringIO

reuters_feeds = ['http://feeds.reuters.com/reuters/UKTopNews'
            , 'http://feeds.reuters.com/reuters/UKdomesticNews'
            , 'http://feeds.reuters.com/reuters/technologyNews'
            , 'http://feeds.reuters.com/reuters/UKScienceNews'
            , 'http://feeds.reuters.com/reuters/UKWorldNews'
            ]

bbc_feeds = ['http://feeds.bbci.co.uk/news/rss.xml'
            , 'http://feeds.bbci.co.uk/news/world/rss.xml'
            , 'http://feeds.bbci.co.uk/news/science_and_environment/rss.xml'
            , 'http://feeds.bbci.co.uk/news/technology/rss.xml'
            ]

google_feed = 'https://news.google.com/rss?x=1571747254.2933&hl=en-US&gl=US&ceid=US:en'
reddit_feed = 'https://www.reddit.com/r/worldnews/.rss'
nytimes_feed = 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/world/rss.xml'
buzzfeed_feed = 'https://www.buzzfeed.com/world.xml'
aljazeera_feed = 'https://www.aljazeera.com/xml/rss/all.xml'
cipher_feed = 'https://www.thecipherbrief.com/feed'
cnn_feed = 'http://rss.cnn.com/rss/edition_world.rss'
guardian_feed = 'https://www.theguardian.com/world/rss'
washingtonpost_feed = 'http://feeds.washingtonpost.com/rss/world'


all_dfs = []

# reuters
reuters_posts = []
for url in reuters_feeds:
    feed = feedparser.parse(url)
    for post in feed.entries:
        reuters_posts.append(post.title)


reauters_posts_df = pd.DataFrame(reuters_posts, columns=['title'])
reauters_posts_df['brand'] = 'reuters'
reauters_posts_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

# bbc
bbc_posts = []
for url in bbc_feeds:
    feed = feedparser.parse(url)
    for post in feed.entries:
        bbc_posts.append(post.title)


bbc_posts_df = pd.DataFrame(bbc_posts, columns=['title'])
bbc_posts_df['brand'] = 'bbc'
bbc_posts_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

# google
google_posts = []
get_google = feedparser.parse(google_feed)
for post in get_google.entries:
    google_posts.append(post.title)

google_df = pd.DataFrame(google_posts, columns=['title'])
google_df['brand'] = 'google'
google_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))


# reddit
reddit_posts = []
get_reddit = feedparser.parse(reddit_feed)
for post in get_reddit.entries:
    reddit_posts.append(post.title)

reddit_df = pd.DataFrame(reddit_posts, columns=['title'])
reddit_df['brand'] = 'reddit'
reddit_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

# nytimes
nytimes_posts = []
get_nytimes = feedparser.parse(nytimes_feed)
for post in get_nytimes.entries:
    nytimes_posts.append(post.title)

nytimes_df = pd.DataFrame(nytimes_posts, columns=['title'])
nytimes_df['brand'] = 'nytimes'
nytimes_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

# buzzfeed
buzzfeed_posts = []
get_buzzfeed = feedparser.parse(buzzfeed_feed)
for post in get_buzzfeed.entries:
    buzzfeed_posts.append(post.title)

buzzfeed_df = pd.DataFrame(buzzfeed_posts, columns=['title'])
buzzfeed_df['brand'] = 'buzzfeed'
buzzfeed_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))


# print(df.head(10))
#
# df.to_sql(schema='staging', name='news_now', con=get_news_engine(), if_exists='replace')
