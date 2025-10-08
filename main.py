import os
import time
import pandas as pd
from datetime import datetime
from textblob import TextBlob
import praw
from pymongo import MongoClient
from dotenv import load_dotenv

# =========================
# LOAD ENVIRONMENT VARIABLES
# =========================
load_dotenv()

# Reddit API setup
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

# MongoDB setup
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
posts_collection = db[os.getenv("MONGO_COLLECTION_POSTS")]         # e.g. reddit_posts
snapshots_collection = db[os.getenv("MONGO_COLLECTION_SNAPSHOTS")] # e.g. sentiment_snapshots

# =========================
# CONFIG
# =========================
FETCH_INTERVAL = 600  # 10 minutes
SUBREDDITS = ["CryptoCurrency", "Bitcoin", "ethtrader", "CryptoMarkets"]

# =========================
# FUNCTIONS
# =========================
def fetch_posts(limit=20):
    posts = []
    for sub in SUBREDDITS:
        for post in reddit.subreddit(sub).new(limit=limit):
            posts.append({
                "id": post.id,
                "title": post.title,
                "selftext": post.selftext,
                "created_utc": post.created_utc,
                "subreddit": post.subreddit.display_name,
                "url": post.url
            })
    return posts

def add_sentiment(posts):
    for post in posts:
        text = (post.get("title") or "") + " " + (post.get("selftext") or "")
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0.1:
            sentiment = "positive"
        elif polarity < -0.1:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        post["sentiment"] = sentiment
        post["polarity"] = polarity
    return posts

def save_to_mongo(posts):
    if posts:
        posts_collection.insert_many(posts)
        print(f"💾 Inserted {len(posts)} new posts into MongoDB!")

def summarize_and_store(posts, label):
    if not posts:
        print(f"⚠️ No posts found for {label}")
        return
    
    df = pd.DataFrame(posts)
    summary = df["sentiment"].value_counts(normalize=True) * 100
    print(f"\n📊 Sentiment for {label}:")
    print(summary.round(2).to_string())

    # Save snapshot to MongoDB
    snapshot = {
        "time": datetime.utcnow(),
        "positive": summary.get("positive", 0),
        "neutral": summary.get("neutral", 0),
        "negative": summary.get("negative", 0),
        "total": len(posts)
    }
    snapshots_collection.insert_one(snapshot)
    print("📌 Snapshot saved to MongoDB!")

# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Starting automated crypto sentiment tracking...")
    while True:
        print(f"\n⏰ Fetching new data at {datetime.utcnow()} ...")
        new_posts = fetch_posts(limit=20)
        new_posts = add_sentiment(new_posts)

        save_to_mongo(new_posts)
        summarize_and_store(new_posts, "latest batch")

        print(f"✅ Saved {len(new_posts)} posts. Sleeping for {FETCH_INTERVAL/60} minutes...\n")
        time.sleep(FETCH_INTERVAL)
