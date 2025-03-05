import os
import praw
import pandas as pd
import json
import threading
from flask import Flask, jsonify
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=os.getenv("PRAW_CLIENT_ID"),
    client_secret=os.getenv("PRAW_CLIENT_SECRET"),
    user_agent=os.getenv("PRAW_USER_AGENT")
)

# Flask app
app = Flask(__name__)

# File paths
JSON_FILE = "askreddit.json"
SUBREDDIT_NAME = "AskReddit"

# Ensure JSON file exists
if not os.path.exists(JSON_FILE):
    with open(JSON_FILE, "w") as f:
        json.dump([], f)

def fetch_reddit_posts():
    """Continuously fetch new posts and save them to a JSON file."""
    print(f"Listening for new posts in r/{SUBREDDIT_NAME}...")

    while True:
        try:
            for post in reddit.subreddit(SUBREDDIT_NAME).stream.submissions(skip_existing=True):
                created_utc = post.created_utc
                datetime_post = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S')

                # Load existing data
                with open(JSON_FILE, "r") as f:
                    data = json.load(f)

                # Append new post
                data.append({"datetime_post": datetime_post, "title": post.title})

                # Save back to JSON
                with open(JSON_FILE, "w") as f:
                    json.dump(data, f, indent=4)

                print(f"New post saved: {post.title}")

        except Exception as e:
            print(f"Error: {e}")

# Start Reddit fetching in a separate thread
thread = threading.Thread(target=fetch_reddit_posts, daemon=True)
thread.start()

@app.route("/posts", methods=["GET"])
def get_posts():
    """Return the stored Reddit posts as JSON."""
    with open(JSON_FILE, "r") as f:
        data = json.load(f)
    return jsonify(data)

# Run Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
