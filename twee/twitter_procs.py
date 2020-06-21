from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
from AuthFile import *
import datetime
import pymongo
import tweepy
import json
import glob
import csv
import os

auth = OAuthHandler(twitter_consumer_key(), twitter_consumer_secret())
auth.set_access_token(twitter_access_token(), twitter_access_secret())


class MyListener(StreamListener):

    def on_data(self, data):
        try:
            with open('feed.json', 'a') as f:
                f.write(data)
                print('appending...' + str(datetime.datetime.now()))
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


class StreamListenerMongo(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = pymongo.MongoClient(get_mongo_coonnection())
            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.twitterdb

            # Decode the JSON from Twitter
            datajson = json.loads(data)

            # grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']

            # grab the name of the person tweeted
            name = datajson['user']['name']

            # grab location of user
            location = datajson['user']['location']

            # print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(created_at) + " from: " + str(name) + " in: " + str(location))

            db.twitter_collection.insert(datajson)
        except Exception as e:
            print(e)


class CSVExport():

    def top_accounts_to_csv(self,host, port, coll_name, db_name):
        client = MongoClient(host, port)
        db = client[coll_name]
        collection = db[db_name]
        data_python = collection.aggregate(
            [{"$group": {"_id": '$user.screen_name', "count": {"$sum": 1}}}, {"$sort": {"count": -1}}])

        field_names = ["Account", "Count"]

        with open('TopAccounts' + str(datetime.datetime.now()) + '.csv', 'w') as f_output:
            csv_output = csv.writer(f_output)
            csv_output.writerow(field_names)

            for data in data_python:
                csv_output.writerow([
                    data['_id'].encode('utf8', 'ignore'),
                    data['count']
                ])

    def top_hashtags_to_csv(self,host, port, coll_name, db_name):
        client = MongoClient(host, port)
        db = client[coll_name]
        collection = db[db_name]
        data_python = collection.aggregate(
            [{"$unwind": '$entities.hashtags'}, {"$group": {"_id": '$entities.hashtags.text', "tagCount": {"$sum": 1}}},
             {"$sort": {"tagCount": -1}}])

        field_names = ["Hashtag", "Count"]

        with open('TopHashtags' + str(datetime.datetime.now()) + '.csv', 'w') as f_output:
            csv_output = csv.writer(f_output)
            csv_output.writerow(field_names)

            for data in data_python:
                csv_output.writerow([
                    data['_id'].encode('utf8', 'ignore'),
                    data['tagCount']
                ])

    def top_languages_to_csv(self,host, port, coll_name, db_name):
        client = MongoClient(host, port)
        db = client[coll_name]
        collection = db[db_name]

        data_python = collection.aggregate([{"$group": {"_id": '$lang', "count": {"$sum": 1}}}, {"$sort": {"count": -1}}])

        field_names = ["Language", "Count"]

        with open('TopLangauges' + str(datetime.datetime.now()) + '.csv', 'w') as f_output:
            csv_output = csv.writer(f_output)
            csv_output.writerow(field_names)

            for data in data_python:
                csv_output.writerow([
                    data['_id'].encode('utf8', 'ignore'),
                    data['count']
                ])

    def get_all_users(self,host, port, coll_name, db_name):
        client = MongoClient(host, port)
        db = client[coll_name]
        collection = db[db_name]
        data_python = collection.find()

        field_names = ["created_at", "id", "text", "user.name", "user.location"]

        with open('AllUsers' + str(datetime.datetime.now()) + '.csv', 'w') as f_output:
            csv_output = csv.writer(f_output)
            csv_output.writerow(field_names)

            for data in data_python:
                csv_output.writerow([
                    data['created_at'],
                    data['user']['id'],
                    data['text'].encode('utf8', 'ignore'),
                    data['user']['screen_name'].encode('utf8'),
                    data['user']['location']
                ])


class Ingest():

    def change_path(self,files_path):
        os.chdir(files_path)


    def get_file_with_json_ext(self):
        files = []
        for file in glob.glob("*.json"):
            files.append(file)

        for file in files:
            print(file)

    def parse_ingest(self,files_list,host, port, coll_name, db_name):
        client = MongoClient(host, port)
        db = client[coll_name]
        collection = db[db_name]

        for file in files_list:
            f = open(file, "r")
            for tweet in f:
                list_of_tweets = json.loads(tweet)
                collection.insert_many(list_of_tweets)
                print('inserting...')

        client.close()
