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




def get_auth():
    auth = tweepy.OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
    auth.set_access_token(twitter_access_token(), twitter_access_secret())
    api = tweepy.API(auth)
    # wait_on_rate_limit=True
    return api



def get_tweets(creds):
    # date_since = "2018-11-16"
    # since=date_since

    query = 'corona OR covid19 OR covid OR coronavirus' # this needs to be made dynamic


    max_tweets = 2000
    searched_tweets = []

    for status in tweepy.Cursor(creds.search, q=query).items(max_tweets):
        searched_tweets.append(status)
        print(status)


    df = pd.DataFrame(searched_tweets)

    df.to_csv('test_covid_tweets.csv')

    return df



if __name__ == '__main__':

    a = get_auth()

    twe = get_tweets(a)

    print(twe)






