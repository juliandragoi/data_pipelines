import tweepy
import pandas as pd
from datetime import datetime
from utils.helpers import get_engine, get_twitter_auth


# WOEID
# New York - 2459115
# Los Angeles - 2442047
# Toronto - 4118
# New Delhi - 2295019
# London - 44418
# Paris - 615702
# San Francisco - 2487956

def get_twitter_trends(auth):
    trends = auth.trends_place(44418)
    data = trends[0]
    trends = data['trends']
    keywords = [trend['name'] for trend in trends]

    df = pd.DataFrame(keywords, columns=['keywords'])

    df['captured_at'] = str(datetime.now().strftime("%Y-%m-%d_%H:00"))

    print(df)

    return df


if __name__ == '__main__':

    auth = get_twitter_auth()

    trends = get_twitter_trends(auth)

    trends.to_sql(schema='staging', name='twitter_trends_today_uk', con=get_engine(), if_exists='replace')
