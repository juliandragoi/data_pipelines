import praw
from praw.models import MoreComments
from AuthFile import get_reddit_auth
import pandas as pd
from datetime import datetime


creds = get_reddit_auth()


r = praw.Reddit(username = creds['username'],
password = creds['password'],
client_id = creds['client_id'],
client_secret = creds['client_secret'],
user_agent = creds['user_agent'])


chosen_sub = 'games'
subreddit = r.subreddit(chosen_sub)


def get_submissions():
    for submission in subreddit.search(chosen_sub, sort='new', time_filter='day', limit=None):
        post = r.submission(id=submission)

        title = submission.title
        author = submission.author
        created_at = submission.created_utc
        id = submission.id
        is_original_content = submission.is_original_content
        name = submission.name
        score = submission.score
        url = submission.url

        comments = []

        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, MoreComments):
                continue
            comments.append(top_level_comment.body)
    pass
