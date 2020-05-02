import feedparser
import pandas as pd
from datetime import datetime
# from AuthFile import get_news_engine
from helpers import get_engine

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
        row = post.title, post.summary
        reuters_posts.append(row)


reauters_posts_df = pd.DataFrame(reuters_posts, columns=['title', 'summary'])
reauters_posts_df['brand'] = 'reuters'
reauters_posts_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

# bbc
bbc_posts = []
for url in bbc_feeds:
    feed = feedparser.parse(url)
    for post in feed.entries:
        row = post.title, post.summary
        bbc_posts.append(row)


bbc_posts_df = pd.DataFrame(bbc_posts, columns=['title', 'summary'])
bbc_posts_df['brand'] = 'bbc'
bbc_posts_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(bbc_posts_df)

# google
google_posts = []
get_google = feedparser.parse(google_feed)
for post in get_google.entries:
    row = post.title, post.summary
    google_posts.append(row)

google_df = pd.DataFrame(google_posts, columns=['title', 'summary'])
google_df['brand'] = 'google'
google_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(google_df)


# reddit
reddit_posts = []
get_reddit = feedparser.parse(reddit_feed)
for post in get_reddit.entries:
    row = post.title, post.summary
    reddit_posts.append(row)

reddit_df = pd.DataFrame(reddit_posts, columns=['title', 'summary'])
reddit_df['brand'] = 'reddit'
reddit_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(reddit_df)

# nytimes
nytimes_posts = []
get_nytimes = feedparser.parse(nytimes_feed)
for post in get_nytimes.entries:
    row = post.title, post.summary
    nytimes_posts.append(row)

nytimes_df = pd.DataFrame(nytimes_posts, columns=['title', 'summary'])
nytimes_df['brand'] = 'nytimes'
nytimes_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(nytimes_df)

# buzzfeed
buzzfeed_posts = []
get_buzzfeed = feedparser.parse(buzzfeed_feed)
for post in get_buzzfeed.entries:
    row = post.title, post.summary
    buzzfeed_posts.append(row)

buzzfeed_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
buzzfeed_df['brand'] = 'buzzfeed'
buzzfeed_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(buzzfeed_df)


# aljazeera
aljazeera_posts = []
get_aljazeera = feedparser.parse(aljazeera_feed)
for post in get_aljazeera.entries:
    row = post.title, post.summary
    aljazeera_posts.append(row)

aljazeera_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
aljazeera_df['brand'] = 'aljazeera'
aljazeera_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(aljazeera_df)


# cipher
cipher_posts = []
get_cipher = feedparser.parse(cipher_feed)
for post in get_cipher.entries:
    row = post.title, post.summary
    cipher_posts.append(row)

cipher_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
cipher_df['brand'] = 'cipher'
cipher_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(cipher_df)


# cnn
cnn_posts = []
get_cnn = feedparser.parse(cnn_feed)
for post in get_cnn.entries:
    row = post.title, post.summary
    cnn_posts.append(row)

cnn_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
cnn_df['brand'] = 'cnn'
cnn_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(cnn_df)


# guardian
guardian_posts = []
get_guardian = feedparser.parse(guardian_feed)
for post in get_guardian.entries:
    row = post.title, post.summary
    guardian_posts.append(row)

guardian_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
guardian_df['brand'] = 'guardian'
guardian_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(guardian_df)


# guardian
washingtonpost_posts = []
get_washingtonpost = feedparser.parse(washingtonpost_feed)
for post in get_washingtonpost.entries:
    row = post.title, post.summary
    washingtonpost_posts.append(row)

washingtonpost_df = pd.DataFrame(buzzfeed_posts, columns=['title', 'summary'])
washingtonpost_df['brand'] = 'washingtonpost'
washingtonpost_df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))
all_dfs.append(washingtonpost_df)


all_frames = pd.concat(all_dfs)

print(all_frames)


all_frames.to_sql(schema='staging', name='rrs_news', con=get_engine(), if_exists='replace', index=False)
