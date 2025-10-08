import os
import pandas as pd
from datetime import datetime
from textblob import TextBlob
from pymongo import MongoClient
from dotenv import load_dotenv

# ======================
# LOAD ENVIRONMENT
# ======================
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB", "crypto_sentiment")
collection_name = os.getenv("MONGO_COLLECTION", "latest_reddit")

client = MongoClient(mongo_uri)
db = client[db_name]
posts_collection = db[collection_name]

print("\n🔍 Connected to MongoDB:", db_name)
print(f"📂 Reading posts from collection: {collection_name}\n")

# ======================
# SENTIMENT ANALYSIS FUNCTION
# ======================
def get_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    else:
        return "neutral"

# ======================
# SENTIMENT TIME SERIES
# ======================
def sentiment_time_series(df, time_unit):
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = pd.to_datetime(df["created_utc"], unit="s")
    df["sentiment"] = df["text"].apply(get_sentiment)

    grouped = df.groupby([pd.Grouper(key="datetime", freq=time_unit), "sentiment"]).size().unstack(fill_value=0)
    grouped = grouped.reset_index().fillna(0)

    totals = grouped[["negative", "neutral", "positive"]].sum(axis=1)
    for col in ["negative", "neutral", "positive"]:
        grouped[col] = (grouped[col] / totals * 100).round(2)

    grouped.rename(columns={"datetime": "timestamp"}, inplace=True)
    return grouped

# ======================
# PROCESS COIN
# ======================
def process_coin(coin, df):
    if df.empty:
        print(f"⚠️ No posts found for {coin}.")
        return

    print(f"\n===============================")
    print(f"📊 Sentiment Analysis for {coin}")
    print(f"===============================")

    time_units = {
        "Hourly": "h",
        "Daily": "D",
        "Weekly": "W",
        "Monthly": "M"
    }

    for label, freq in time_units.items():
        summary = sentiment_time_series(df.copy(), freq)
        if summary.empty:
            continue

        print(f"\n📈 {coin} {label} Sentiment (%):")
        print(summary.tail())

        collection_name = f"sentiment_{label.lower()}"
        summary_records = summary.to_dict("records")

        # Add coin tag
        for record in summary_records:
            record["coin"] = coin

        db[collection_name].insert_many(summary_records)
        print(f"✅ Saved {len(summary_records)} {label} records for {coin} to MongoDB.")

# ======================
# MAIN EXECUTION
# ======================
if __name__ == "__main__":
    posts = list(posts_collection.find({}, {"_id": 0, "coin": 1, "title": 1, "selftext": 1, "created_utc": 1}))
    if not posts:
        print("⚠️ No posts found in MongoDB. Exiting.")
        exit()

    df = pd.DataFrame(posts)
    df["text"] = df["title"].fillna("") + " " + df["selftext"].fillna("")

    for coin in df["coin"].unique():
        coin_posts = df[df["coin"] == coin]
        process_coin(coin, coin_posts)

    print("\n🎯 All sentiment summaries saved to MongoDB successfully!")
