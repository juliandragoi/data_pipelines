from AuthFile import twitter_consumer_key, twitter_consumer_secret, twitter_access_token, twitter_access_secret
from datetime import datetime
import pandas as pd
import argparse
import tweepy
import os
import glob
import csv


def get_auth():
    auth = tweepy.OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
    auth.set_access_token(twitter_access_token(), twitter_access_secret())
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def get_audience_ids(file):
    df = pd.read_csv(file)
    df = df[df['id'].notna()]
    print(df)
    res = df['id']
    out = res.tolist()
    print(out)
    return out


def get_tweets(auth, ids, num_tweets, screen_name=None):

    folder = 'account_tweets'
    if screen_name is not None:
        handle = screen_name
    else:
        handle = None

    dfs = []

    for i in ids:

        full = []

        try:
            for status in tweepy.Cursor(auth.user_timeline, id=int(i), screen_name=handle, tweet_mode="extended").items(num_tweets):
                data = status.created_at, status.user.id, status.user.screen_name, status.user.name, status.user.description, status.full_text, status.user.location
                print(data)
                full.append(data)

                df = pd.DataFrame(full)
                df.columns = ['created_at', 'id', 'screen_name', 'name', 'user_desc', 'tweet_text', 'location']
                dfs.append(df)

                acc_id = int(i)

                df.to_csv(os.path.join(folder, 'tweets_' + str(acc_id) + '_' + now.strftime("%Y-%m-%d") + '.csv'),
                          index=False, encoding='utf-8')

        except tweepy.error.TweepError:

            df = pd.DataFrame(full)
            if df.empty == True:
                print('DataFrame is empty, moving on')
                pass

    merged = pd.concat(dfs)

    print(merged)

    return merged


if __name__ == '__main__':

    now = datetime.now()

    authentication = get_auth()

    aud_file = os.path.join('audiences','fashion_brands.csv')

    aud_list = get_audience_ids(aud_file)

    tweets = get_tweets(authentication, aud_list, 100)

    # tweets.to_sql(schema='staging', name='fashion_brand_tweets', con=get_engine(), if_exists='replace')




