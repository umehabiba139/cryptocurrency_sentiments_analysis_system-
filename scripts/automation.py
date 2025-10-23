# automation.py
import os
import json
import time
import pandas as pd
from datetime import datetime
from textblob import TextBlob
import praw
from dotenv import load_dotenv
from pymongo import MongoClient

# =========================
# LOAD ENVIRONMENT VARIABLES
# =========================
load_dotenv()

print("\n🔍 Checking environment variables...")
required_env = [
    "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT",
    "MONGO_URI", "MONGO_DB", "MONGO_COLLECTION_POSTS", "MONGO_COLLECTION_SNAPSHOTS"
]

missing = [var for var in required_env if not os.getenv(var)]
if missing:
    print(f"⚠️ Missing environment variables: {missing}")
    print("❌ Please fix your .env file before running again.\n")
    exit()

# =========================
# SETUP REDDIT API & MONGO (with SSL fix)
# =========================
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

# 👇 Ignore SSL handshake errors
client = MongoClient(
    os.getenv("MONGO_URI"),
    tls=True,
    tlsAllowInvalidCertificates=True
)

MONGO_URI = "mongodb+srv://aribafaryad:uGZKX4AZ5F7vEjkW@tweets.d0g9ckv.mongodb.net/?retryWrites=true&w=majority&appName=tweets"
DB_NAME = "crypto_reddit_db"
COLLECTION_NAME = "latest_reddit"
COLLECTION_NAME = "latest_snapshot"

db = client[os.getenv("MONGO_DB")]
posts_collection = db[os.getenv("MONGO_COLLECTION_POSTS")]
snapshots_collection = db[os.getenv("MONGO_COLLECTION_SNAPSHOTS")]

print("✅ Environment loaded and MongoDB connected successfully (SSL ignored)!\n")

# =========================
# CONFIG
# =========================
FETCH_INTERVAL = 3600  # 10 minutes
OUTPUT_POSTS = "reddit_crypto_posts.jsonl"
OUTPUT_SNAPSHOTS = "sentiment_snapshots.csv"
SUBREDDITS = ["CryptoCurrency", "Bitcoin", "ethtrader", "CryptoMarkets", 'Solana','sol']

# =========================
# FUNCTIONS
# =========================
def fetch_posts(limit=50):
    COIN_KEYWORDS = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "SOL": ["solana", "sol"],
    }

    posts = []
    for sub in SUBREDDITS:
        for post in reddit.subreddit(sub).new(limit=limit):
            text = (post.title or "") + " " + (post.selftext or "")
            text_lower = text.lower()

            detected_coin = None
            for coin, keywords in COIN_KEYWORDS.items():
                if any(keyword in text_lower for keyword in keywords):
                    detected_coin = coin
                    break

            posts.append({
                "id": post.id,
                "title": post.title,
                "selftext": post.selftext,
                "created_utc": datetime.utcfromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
                "subreddit": post.subreddit.display_name,
                "url": post.url,
                "coin": detected_coin or "UNKNOWN",  # <- add coin label here
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

def save_posts(posts):
    # Save locally
    with open(OUTPUT_POSTS, "a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post) + "\n")

    # Save to MongoDB
    if posts:
        try:
            posts_collection.insert_many(posts)
            print(f"💾 {len(posts)} posts saved to MongoDB collection.")
        except Exception as e:
            print(f"⚠️ Could not save posts to MongoDB: {e}")

def summarize(posts, label):
    if not posts:
        print(f"⚠️ No posts found for {label}")
        return

    df = pd.DataFrame(posts)
    summary = df["sentiment"].value_counts(normalize=True) * 100
    print(f"\n📊 Sentiment for {label}:")
    print(summary.round(2).to_string())

    snapshot = {
        "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "positive": summary.get("positive", 0),
        "neutral": summary.get("neutral", 0),
        "negative": summary.get("negative", 0),
        "total": len(posts)
    }

    # Save locally
    df_snap = pd.DataFrame([snapshot])
    file_exists = os.path.isfile(OUTPUT_SNAPSHOTS)
    df_snap.to_csv(OUTPUT_SNAPSHOTS, mode="a", header=not file_exists, index=False)

    # Save to MongoDB
    try:
        snapshots_collection.insert_one(snapshot)
        print("📸 Snapshot saved to MongoDB.\n")
    except Exception as e:
        print(f"⚠️ Could not save snapshot to MongoDB: {e}\n")

# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Starting automated crypto sentiment tracking with MongoDB...\n")

    while True:
        print(f"⏰ Fetching new data at {datetime.utcnow()} ...")
        new_posts = fetch_posts(limit=50)
        new_posts = add_sentiment(new_posts)

        save_posts(new_posts)
        summarize(new_posts, "latest batch")

        print(f"✅ Saved {len(new_posts)} posts. Sleeping for {FETCH_INTERVAL/60} minutes...\n")
        time.sleep(FETCH_INTERVAL)
