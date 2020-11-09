from utils.helpers import get_twitter_creds as creds
import pandas as pd
import tweepy
import time


def create_auth(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def get_followers(api, handle):
    ids = []
    try:
        for page in tweepy.Cursor(api.followers_ids, screen_name=handle).pages():
            print(page)
            ids.extend(page)

    except:
        print('We got a timeout ... Sleeping for 15 minutes')
        time.sleep(15 * 60)
        for page in tweepy.Cursor(api.followers_ids, screen_name=handle).pages():
            print(page)
            ids.extend(page)

    df = pd.DataFrame(ids)
    df.to_csv(handle+'_followers.csv')

    return ids


if __name__ == '__main__':
    twitter_handle = 'human_digital'

    api = create_auth(creds()['consumer_key'], creds()['consumer_secret'], creds()['access_token'],
                      creds()['access_token_secret'])

    get_followers(api, twitter_handle)