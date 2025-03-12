import pandas as pd
import json
import os
import praw
import logging
from datetime import datetime
from pytz import timezone
import schedule
import time
import threading
from thefuzz import process
from openai import OpenAI
import prawcore
from flask import Flask, render_template
from dotenv import load_dotenv

load_dotenv()

# Logging setup
logging.basicConfig(filename="perfume_scraper.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Reddit API setup
reddit = praw.Reddit(
    client_id=os.getenv("PRAW_CLIENT_ID"),
    client_secret=os.getenv("PRAW_CLIENT_SECRET"),
    user_agent=os.getenv("PRAW_USER_AGENT")
)

# Flask app
app = Flask(__name__)

subreddit_name = "tester_jethya"
keyword = "wts"
subreddit = reddit.subreddit(subreddit_name)

CSV_FILE = "merged_perfume_data_copy.csv"
JSON_FILE = "montagne_official_data.json"
LOWEST_PRICES_FILE = "PerfumeLowestPrices.csv"
CLEANUP_TIME = "01:15"  # Time in 24-hour format (server time)

# Load JSON data
with open(JSON_FILE, "r", encoding="utf-8") as f:
    json_data = json.load(f)
json_df = pd.DataFrame(json_data)
json_df.rename(columns={"name": "Perfume_name", "url": "official_link", "is_in_stock": "official_availability", "price": "official_price"}, inplace=True)

# Load existing CSV
if os.path.exists(CSV_FILE):
    df = pd.read_csv(CSV_FILE)
else:
    df = pd.DataFrame(columns=["post_id", "link_flair_text", "Perfume_name", "permalink", "bottle_cost", "clone_of", "official_link", "official_availability", "official_price"])

# Function to remove sold/deleted posts
def remove_sold_or_deleted_posts():
    logging.info("Running daily cleanup for sold/deleted posts.")
    global df
    if not os.path.exists(CSV_FILE):
        logging.warning("CSV file not found. Skipping sold/deleted post removal.")
        return
    
    existing_posts = df["post_id"].tolist()
    for post_id in existing_posts:
        try:
            submission = reddit.submission(id=post_id)
            if submission.link_flair_text == "Sold":
                df = df[df["post_id"] != post_id]
                logging.info(f"Removed sold post: {submission.title}")
        except (praw.exceptions.PRAWException, prawcore.exceptions.NotFound):
            df = df[df["post_id"] != post_id]
            logging.info(f"Post {post_id} no longer exists. Removed from CSV.")
    
    df.to_csv(CSV_FILE, index=False, encoding="utf-8")
    logging.info("Cleanup completed: Removed sold and deleted posts.")

# Function to run scheduled cleanup in a separate thread
def schedule_cleanup():
    schedule.every().day.at(CLEANUP_TIME).do(remove_sold_or_deleted_posts)
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Function to listen for new Reddit posts
def listen_for_new_posts():
    global df
    logging.info(f"Listening for new posts in r/{subreddit_name} containing '{keyword}'...")
    
    while True:
        try:
            for post in subreddit.stream.submissions(skip_existing=True):
                if keyword.lower() in post.title.lower() or keyword.lower() in post.selftext.lower():
                    post_text = f"{post.title} {post.selftext}".strip()
                    
                    if post_text:
                        try:
                            perfumes = parse_perfume_post(post_text)
                            if not perfumes:
                                logging.info(f"Skipping post: {post.title} (No valid perfumes detected)")
                                continue
                            
                            for perfume in perfumes:
                                perfume_name = get_best_match(perfume.get("Perfume_name", ""), json_df["Perfume_name"].tolist())
                                matched_json = json_df[json_df["Perfume_name"] == perfume_name].iloc[0]
                                
                                new_data = {
                                    "post_id": post.id,
                                    "link_flair_text": post.link_flair_text,
                                    "Perfume_name": perfume_name,
                                    "permalink": post.url,
                                    "bottle_cost": perfume.get("bottle_cost", ""),
                                    "clone_of": matched_json.get("clone_of", ""),
                                    "official_link": matched_json["official_link"],
                                    "official_availability": matched_json["official_availability"],
                                    "official_price": matched_json["official_price"]
                                }
                                
                                df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
                                logging.info(f"New post added: {post.title}")
                        except Exception as e:
                            logging.error(f"Error processing post: {e}")
                    
                    df.to_csv(CSV_FILE, index=False, encoding="utf-8")
                    update_lowest_prices()

        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(10)  # Prevents crash loops
        pass

# Function to update lowest prices
def update_lowest_prices():
    if not os.path.exists(CSV_FILE):
        logging.warning("Main CSV file not found. Skipping lowest prices update.")
        return
    
    df = pd.read_csv(CSV_FILE)
    df_filtered = df[df["link_flair_text"] != "Sold"]
    lowest_prices = (
        df_filtered.sort_values(by=["Perfume_name", "bottle_cost"])
        .groupby("Perfume_name")
        .head(5)
    )
    lowest_prices.to_csv(LOWEST_PRICES_FILE, index=False, encoding="utf-8")
    logging.info("Updated PerfumeLowestPrices.csv with 5 lowest prices per perfume (excluding 'Sold').")

# Function to get the best match using fuzzy matching
def get_best_match(noisy_name, choices):
    best_match, _ = process.extractOne(noisy_name, choices)
    return best_match

# Function to parse perfume posts using DeepSeek API
def parse_perfume_post(post_text: str) -> list:
    client = OpenAI(api_key=os.getenv("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com/v1")
    system_prompt = """
    Extract perfume details from the following Reddit post and return ONLY a valid JSON list of dictionaries with the following structure:

    [
        {"Perfume_name": "First Perfume", "bottle_quantity": "100ml", "bottle_cost": 50, "currency": "USD"},
        {"Perfume_name": "Second Perfume", "bottle_quantity": "50ml", "bottle_cost": 35, "currency": "USD"}
    ]

    ### **Formatting Rules:**
    - Do **NOT** include any text, explanations, or markdown (`json`, `python`, etc.). Output **must be raw JSON** only.
    - Ensure `bottle_cost` is **always a number** (e.g., `50`). If missing, keep it blank `` instead of `null`.
    - Ensure `bottle_quantity` is **always a string** (e.g., `"100ml"`). If missing, default to `"50ml"`.
    - Ensure `currency` is **always a string** (e.g., `"USD"`). If missing, default to `"USD"`.
    - **Do not return `null` values**; instead, apply the default values above.

    Now, extract data from this Reddit post and return ONLY the valid JSON list:
    """

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": post_text}],
        temperature=0.1
    )

    result = response.choices[0].message.content.strip()
    
    try:
        perfumes = json.loads(result)
        return [p for p in perfumes if "bottle_cost" in p and p["bottle_cost"]]
    except json.JSONDecodeError:
        logging.error(f"Unable to parse API response: {result}")
        return []

def count_numbers():
    count = 1
    while True:
        print(count)
        count += 1
        time.sleep(1)  # Adjust the sleep time as needed to control the speed

# Create and start the counting thread
count_thread = threading.Thread(target=count_numbers)
count_thread.daemon = True  # This makes the thread exit when the main program exits
count_thread.start()

# Start the cleanup scheduler in a separate thread
cleanup_thread = threading.Thread(target=schedule_cleanup, daemon=True)
cleanup_thread.start()

# Start listening for new posts in a separate thread
reddit_thread = threading.Thread(target=listen_for_new_posts, daemon=True)
reddit_thread.start()

@app.route("/lowest-prices")
def show_lowest_prices():
    if os.path.exists(LOWEST_PRICES_FILE):
        df = pd.read_csv(LOWEST_PRICES_FILE)
        return render_template("lowest_prices.html", tables=[df.to_html(classes="table table-striped", index=False)], titles=df.columns.values)
    else:
        return "No data available", 404

# Run Flask app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render dynamically assigns a port
    app.run(host="0.0.0.0", port=port, debug=True)
