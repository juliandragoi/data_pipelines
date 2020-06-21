
#Aggregating tweets to usernames

db.tweets.aggregate([{"$group": {"_id": '$user.screen_name',"count": {"$sum": 1}}},{"$sort": {"count": -1}}])

#Aggregating tweets to languages

db.tweets.aggregate([{"$group": {"_id": '$lang',"count": {"$sum": 1}}},{"$sort": {"count": -1}}])

#Aggregating tweets to most occuring hashtag

db.tweets.aggregate([{"$unwind": '$entities.hashtags'},{ "$group": {"_id": '$entities.hashtags.text', "tagCount": {"$sum": 1}}},{"$sort": {"tagCount": -1}}])
