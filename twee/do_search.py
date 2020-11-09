from datetime import datetime
from AuthFile import *
import pandas as pd
import argparse
import tweepy
import os
import glob
import csv

import boto
import boto.s3
import sys
from boto.s3.key import Key
import time, threading



def get_auth():
    auth = tweepy.OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
    auth.set_access_token(twitter_access_token(), twitter_access_secret())
    api = tweepy.API(auth)
    # wait_on_rate_limit=True
    return api



def get_tweets(creds, query, d_since, num_tweets):

    searched_tweets = []

    for status in tweepy.Cursor(creds.search, q=query, since=d_since,lang="en").items(num_tweets):
        searched_tweets.append(status._json)
        print(status._json)


    df = pd.DataFrame(searched_tweets)

    print(df)

    # df.to_csv('singapore_elections_tweets' + str(datetime.now().strftime("%Y-%m-%d_%H:00")) + '.csv')

    max_created_at = max(df['created_at'])

    print(max_created_at)

    return df



if __name__ == '__main__':


    date_since = "2020-01-01"

    since = 1277834415947657216

    a = get_auth()

    keywords = ['singapore election', 'SingaporeElection', 'singaporevotes', 'SGE', 'sgelections', 'singaporevotes'
        , 'SDP', 'SingaporeDemocraticParty', 'singaporeGE2020']

    query = ' OR '.join(keywords)

    twe = get_tweets(a, query, since, 100)

    print(twe.columns)
