from tweepy import Stream
from twee.TrackWords import *
from twee.twitter_procs import *


def get_tweets_to_file():
        twitter_stream = Stream(auth, StreamListenerFile())
        print("Tracking: " + str(singapore_elections_words()))
        twitter_stream.filter(track=singapore_elections_words())

def get_tweets_to_mongo():
        listener = StreamListenerMongo(api=tweepy.API(wait_on_rate_limit=True))
        streamer = tweepy.Stream(auth=auth, listener=listener)
        print("Tracking: " + str(singapore_elections_words()))
        # streamer.filter(locations=[-0.5861177308,51.2368503848,0.3100357535,51.7330313437])


if __name__ == '__main__':
        get_tweets_to_file()
