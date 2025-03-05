import os
import praw
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from celery import Celery

# Load environment variables
load_dotenv()

# Initialize Celery
celery = Celery("tasks")
celery.config_from_object("celery_config")

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=os.getenv("PRAW_CLIENT_ID"),
    client_secret=os.getenv("PRAW_CLIENT_SECRET"),
    user_agent=os.getenv("PRAW_USER_AGENT")
)

CSV_FILE = "askreddit.csv"
subreddit_name = "AskReddit"

@celery.task
def fetch_reddit_posts():
    subreddit = reddit.subreddit(subreddit_name)

    if not os.path.exists(CSV_FILE):
        df = pd.DataFrame(columns=["datetime_post", "title"])
        df.to_csv(CSV_FILE, index=False)

    print(f"Listening for new posts in r/{subreddit_name}...")

    for post in subreddit.stream.submissions(skip_existing=True):
        created_utc = post.created_utc
        datetime_post = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S')

        new_data = pd.DataFrame([{
            "datetime_post": datetime_post,
            "title": post.title,
        }])

        new_data.to_csv(CSV_FILE, mode='a', header=False, index=False)

        print(f"New post saved: {post.title}")
