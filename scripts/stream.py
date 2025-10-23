# stream.py
import praw
import time
import json
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

# Subreddits & keywords for crypto tracking
SUBREDDITS = "CryptoCurrency+Bitcoin+ethereum+ethtrader"
KEYWORDS = ["bitcoin", "btc", "ethereum", "eth"]
OUTPUT_FILE = "reddit_stream.jsonl"

def stream_reddit():
    print("🔴 Streaming live from Reddit... (Ctrl+C to stop)")
    for submission in reddit.subreddit(SUBREDDITS).stream.submissions(skip_existing=True):
        text = (submission.title + " " + (submission.selftext or "")).lower()
        if any(kw in text for kw in KEYWORDS):
            post = {
                "id": submission.id,
                "title": submission.title,
                "selftext": submission.selftext,
                "created_utc": submission.created_utc,
                "subreddit": submission.subreddit.display_name,
                "score": submission.score,
                "url": submission.url,
            }
            # Print a short preview
            print(f"📥 New post: {submission.title[:60]}...")
            # Save to file
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(post) + "\n")

if __name__ == "__main__":
    stream_reddit()
