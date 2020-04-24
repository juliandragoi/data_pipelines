from datetime import datetime
from AuthFile import *
import pandas as pd
import argparse
import tweepy
import os

#  for local use with an auth/helpers file

def get_auth():
    auth = tweepy.OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
    auth.set_access_token(twitter_access_token(), twitter_access_secret())
    api = tweepy.API(auth) #can add an argument called "wait_on_rate_limit" if we are to run a big crawl overnight
    return api


# def get_auth(consumer_key, consumer_secret, access_token, access_secret):
#     auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#     auth.set_access_token(access_token, access_secret)
#     api = tweepy.API(auth)
#     return api


# get_audience method expects a pandas dataframe with only one column of ids
# the below just renames the column so it can be used easier

def get_audience(file, audience_type, num_accounts):
    df = pd.read_csv(file)
    df = df.sort_values('importance', ascending=False)
    df = df[df['label'] == audience_type]
    res = df['twitter_id'].head(num_accounts)
    return res


def get_all_tweets(auth, id, num_tweets):
    full = []
    for status in tweepy.Cursor(auth.user_timeline, id=id, tweet_mode="extended").items(num_tweets):
        # print(status)
        data = status.user.id, status.full_text, status.entities['hashtags'], status.entities['user_mentions'], status.user.location
        print(data)
        full.append(data)
    df = pd.DataFrame(full)
    df.columns = ['id', 'tweet_text', 'hashtags', 'user_mentions','location']
    return df


def get_common_hashtags(df:pd.DataFrame):
    hashtags = []
    for ent in df["hashtags"]:
        if ent is not None:
            for i in ent:
                hashtag = i["text"]
                if hashtag is not None:
                    hashtags.append(hashtag)
    hashtag_df = pd.DataFrame(hashtags)
    hashtag_df.columns = ['hashtag']
    counts = hashtag_df["hashtag"].value_counts()
    out = pd.DataFrame(counts)

    folder = 'audience_hashtags'

    if not os.path.exists(folder):
        os.makedirs(folder)
    now = datetime.now()
    out.to_csv(os.path.join(folder, 'hashtags' + now.strftime("%Y-%m-%d") + '.csv'))

    return counts


def get_mentions(df:pd.DataFrame):
    mentions = []
    for ent in df["user_mentions"]:
        if ent is not None:
            for i in ent:
                mention = i["screen_name"]
                if mention is not None:
                    mentions.append(mention)
    mentions_df = pd.DataFrame(mentions)
    mentions_df.columns = ['mention']
    counts = mentions_df["mention"].value_counts()
    out = pd.DataFrame(counts)

    folder = 'audience_mentions'

    if not os.path.exists(folder):
        os.makedirs(folder)
    now = datetime.now()
    out.to_csv(os.path.join(folder, 'mentions' + now.strftime("%Y-%m-%d") + '.csv'))
    print(counts)
    return counts


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='audience hastags and mentions')
    parser.add_argument('--consumer_key', help='twitter consumer_key')
    parser.add_argument('--consumer_secret', help='twitter consumer_secret')
    parser.add_argument('--access_token', help='twitter access_token')
    parser.add_argument('--access_secret', help='twitter access_secret')
    parser.add_argument('--num_tweets', help='number of tweets you want to collect for analysis', default=200)
    parser.add_argument('--audience_type', help='audience type we want analysis on', default='parent')
    parser.add_argument('--num_accounts', help='number of accounts we want to crawl for analysis', default=5)

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    # auth = get_auth(args.consumer_key, args.consumer_secret, args.access_token, args.access_secret)
    auth = get_auth()

    audience_file = 'davide_savvy_fan_ids.csv'
    # audience = get_audience(audience_file, args.audience_type, args.num_accounts)
    audience = get_audience(audience_file, 'parent', 5)
    for i in audience:
        print(i)

    all_frames = []

    for i in audience:
        # tweets = get_all_tweets(auth, str(i), args.num_tweets)
        tweets = get_all_tweets(auth, str(i), 200)
        all_frames.append(tweets)

    all_frames = pd.concat(all_frames)

    # tweets = get_all_tweets(get_auth(), '18646108', 200)

    get_common_hashtags(all_frames)
    get_mentions(all_frames)



