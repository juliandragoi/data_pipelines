from tweepy import Stream
from twitter.TrackWords import *
from twitter.twitter_procs import *


def get_tweets_to_file():
        twitter_stream = Stream(auth, MyListener())
        twitter_stream.filter(track=f1_words())

def get_tweets_to_mongo():
        listener = StreamListenerMongo(api=tweepy.API(wait_on_rate_limit=True))
        streamer = tweepy.Stream(auth=auth, listener=listener)
        # print("Tracking: " + str(f1_words()))
        streamer.filter(locations=[-0.5861177308,51.2368503848,0.3100357535,51.7330313437])


if __name__ == '__main__':
    get_tweets_to_mongo()
