from datetime import datetime
# from AuthFile import *
import pandas as pd
import argparse
import tweepy
import os
import glob
import csv

#  for local use with an auth/helpers file

# def get_auth():
#     auth = tweepy.OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
#     auth.set_access_token(twitter_access_token(), twitter_access_secret())
#     api = tweepy.API(auth, wait_on_rate_limit=True)
#     return api


def get_auth(twitter_consumer_key, twitter_consumer_secret, twitter_access_token, twitter_access_secret):
    auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
    auth.set_access_token(twitter_access_token, twitter_access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


# get_audience method expects a pandas dataframe with only one column of ids
# the below just renames the column so it can be used easier

def get_audience(file_name, num_accounts):
    file = os.path.join('audiences',file_name)
    output_list = []
    df = pd.read_csv(file)
    # df = df.sort_values('importance', ascending=False)
    df = df[['fan_id']].drop_duplicates()
    res = df['fan_id'].head(num_accounts)
    for i in res:
        print(i)
        output_list.append(i)
    return output_list


def get_audience_with_filter(file, audience_type, num_accounts):
    df = pd.read_csv(file)
    df = df.sort_values('importance', ascending=False)
    df = df[df['label'] == audience_type]
    df = df[['twitter_id']].drop_duplicates()
    res = df['twitter_id'].head(num_accounts)
    out = res.tolist()
    return out


def get_tweets(auth, id, num_tweets):
    full = []
    for status in tweepy.Cursor(auth.user_timeline, id=id, tweet_mode="extended").items(num_tweets):
        data = status.user.id, status.full_text, status.entities['hashtags'], status.entities['user_mentions'], status.user.location
        print(data)
        full.append(data)

    df = pd.DataFrame(full)
    df.columns = ['id', 'tweet_text', 'hashtags', 'user_mentions','location']

    return df


def get_common_hashtags(df):
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


def get_mentions(df):
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


def get_all_tweets(auth, ids, num_tweets):
    folder = 'account_tweets'

    if not os.path.exists(folder):
        os.makedirs(folder)
    now = datetime.now()

    if os.path.exists('done_accounts.csv'):
        os.remove('done_accounts.csv')
    if os.path.exists('bad_ids.csv'):
        os.remove('bad_ids.csv')

    for i in ids:
        with open('done_accounts.csv', 'a+') as done_file:
            file_writer = csv.writer(done_file)
            file_writer.writerow([str(i)])

        full = []

        try:
            for status in tweepy.Cursor(auth.user_timeline, id=i, tweet_mode="extended").items(num_tweets):
                data = status.created_at, status.user.id, status.user.description, status.full_text, status.entities['hashtags'], status.entities['user_mentions'], status.user.location
                print(data)
                full.append(data)

                df = pd.DataFrame(full)
                df.columns = ['created_at', 'id', 'user_desc', 'tweet_text', 'hashtags', 'user_mentions', 'location']
                df.to_csv(os.path.join(folder, 'tweets_' + str(i) + '_' + now.strftime("%Y-%m-%d") + '.csv'),
                          index=False, encoding='utf-8')

        except tweepy.error.TweepError:

            df = pd.DataFrame(full)
            if df.empty == True:
                print('DataFrame is empty, moving on')
                pass

            with open('bad_ids.csv', 'a+') as bad_file:
                file_writer = csv.writer(bad_file)
                file_writer.writerow([str(i)])

def delete_done_account(audience_file, done_file):

    lines = list()

    with open(audience_file, 'r') as readFile:
        reader = csv.reader(readFile)
        for row in reader:
            lines.append(row)
            for field in row:
                if field == id:
                    lines.remove(row)
                    with open('done_accounts/done_accounts.csv', 'w') as done_file:
                        done_file.write(id)

    with open(audience_file, 'w') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(lines)



def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='audience hastags and mentions')
    parser.add_argument('--consumer_key', help='twitter consumer_key', type=str, action='store')
    parser.add_argument('--consumer_secret', help='twitter consumer_secret', type=str, action='store')
    parser.add_argument('--access_token', help='twitter access_token', type=str , action='store')
    parser.add_argument('--access_secret', help='twitter access_secret', type=str , action='store')
    parser.add_argument('--audience_file', help='file with list of ids to crawl', action='store')
    parser.add_argument('--num_tweets', help='number of tweets you want to collect for analysis', default=200)
    parser.add_argument('--audience_type', help='audience type we want analysis on', default='parent', required=False)
    # parser.add_argument('--num_accounts', help='number of accounts we want to crawl for analysis', default=5, required=True)

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    auth = get_auth(args.consumer_key, args.consumer_secret, args.access_token, args.access_secret)
    # auth = get_auth()

    audience_file = args.audience_file
    # audience = get_audience(audience_file, args.num_accounts)
    # audience = get_audience_with_filter(audience_file, args.audience_type, args.num_accounts)
    audience = get_audience(audience_file, 50) #<<<<<<<<<<------------- CHANGE the number for the batch amount

    tweets = get_all_tweets(auth, audience, 200)


    # all_frames = []
    #
    # for i in audience:
    #     # tweets = get_all_tweets(auth, str(i), args.num_tweets)
    #     tweets = get_tweets(auth, str(i), 200)
    #     all_frames.append(tweets)
    #
    # all_frames = pd.concat(all_frames)

    # tweets = get_all_tweets(get_auth(), ['18646108','32691527'], 200)

    # get_common_hashtags(all_frames)
    # get_mentions(all_frames)


