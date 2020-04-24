import feedparser
import pandas as pd
from datetime import datetime

raw_rss = ['https://news.google.com/rss?x=1571747254.2933&hl=en-US&gl=US&ceid=US:en'
            , 'https://www.reddit.com/r/worldnews/.rss'
            , 'http://feeds.bbci.co.uk/news/world/rss.xml'
            , 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/world/rss.xml'
            , 'https://www.buzzfeed.com/world.xml'
            , 'https://www.aljazeera.com/xml/rss/all.xml'
            , 'https://www.thecipherbrief.com/feed'
            , 'http://rss.cnn.com/rss/edition_world.rss'
            , 'https://www.theguardian.com/world/rss'
            , 'http://feeds.washingtonpost.com/rss/world'
            , 'http://feeds.bbci.co.uk/news/rss.xml'
            , 'http://feeds.bbci.co.uk/news/science_and_environment/rss.xml'
            , 'http://feeds.bbci.co.uk/news/technology/rss.xml'
            , 'http://feeds.reuters.com/reuters/UKTopNews'
            , 'http://feeds.reuters.com/reuters/UKdomesticNews'
            , 'http://feeds.reuters.com/reuters/technologyNews'
            , 'http://feeds.reuters.com/reuters/UKScienceNews'
            , 'http://feeds.reuters.com/reuters/UKWorldNews'
            ]


feeds = [] # list of feed objects
for url in raw_rss:
    feeds.append(feedparser.parse(url))

posts = [] # list of posts [(title1, link1, summary1), (title2, link2, summary2) ... ]
for feed in feeds:
    for post in feed.entries:
        posts.append((post.title, post.link, post.summary))

df = pd.DataFrame(posts, columns=['title', 'link', 'summary'])

df['captured_at'] = str(datetime.now().strftime("%Y%m%d_%H:00"))

print(df)

# df.to_csv('news' + str(datetime.now().strftime("_%Y%m%d_%H00")+ '.csv'), index=False, mode='w')

