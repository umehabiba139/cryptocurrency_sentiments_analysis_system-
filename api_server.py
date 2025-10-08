# backend/api_server.py
import os
from datetime import datetime, timedelta
from typing import Optional
from dateutil import parser as dateparse

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # loads MONGO_URI, MONGO_DB, MONGO_COLLECTION

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "crypto_reddit_db")
COL_NAME = os.getenv("MONGO_COLLECTION", "latest_reddit")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COL_NAME]

app = FastAPI()

# allow requests from your frontend (dev). Change origin in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

def parse_created_utc(value):
    """
    Handles several formats:
      - UNIX timestamp (int/float)
      - ISO datetime string "2025-10-08 06:00:00"
      - MongoDB Date object (already datetime)
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(float(value))
    if isinstance(value, str):
        try:
            return dateparse.parse(value)
        except Exception:
            return None
    if hasattr(value, "tzinfo") or isinstance(value, datetime):
        return value
    return None

@app.get("/api/sentiment-timeseries")
def sentiment_timeseries(
    coin: Optional[str] = Query(None, description="coin name or coin_ml"),
    timeframe: str = Query("7d", description="24h,7d,30d"),
    interval: str = Query("H", description="H=hour, D=day, W=week, M=month"),
):
    # compute start_time based on timeframe
    now = datetime.utcnow()
    if timeframe == "24h":
        start_time = now - timedelta(hours=24)
    elif timeframe == "7d":
        start_time = now - timedelta(days=7)
    elif timeframe == "30d":
        start_time = now - timedelta(days=30)
    else:
        start_time = now - timedelta(days=7)

    # Query MongoDB (if created_utc stored as string, we'll filter client-side)
    query = {}
    if coin:
        # check both coin_ml and coin fields
        query["$or"] = [{"coin_ml": coin}, {"coin": coin}, {"coin": coin.lower()}, {"coin": coin.upper()}]

    docs = list(collection.find(query))
    # Extract rows
    rows = []
    for d in docs:
        created = parse_created_utc(d.get("created_utc") or d.get("timestamp") or d.get("created_at"))
        if not created or created < start_time:
            continue
        sentiment = None
        # try common fields
        if isinstance(d.get("sentiment"), dict):
            # earlier format: { "polarity": 0.5, "label": "positive" }
            sentiment = d["sentiment"].get("label") or d["sentiment"].get("sentiment")
        else:
            sentiment = d.get("sentiment") or d.get("sentiment_label") or d.get("label")

        rows.append({"datetime": created, "sentiment": sentiment or "neutral"})

    # If no data, return empty
    if not rows:
        return {"coin": coin, "frame": timeframe, "interval": interval, "series": []}

    # Build DataFrame and group
    df = pd.DataFrame(rows)
    df = df.set_index(pd.DatetimeIndex(df["datetime"]))
    grouped = df.groupby([pd.Grouper(freq=interval), "sentiment"]).size().unstack(fill_value=0)

    # convert to percentages
    total = grouped.sum(axis=1)
    percent = (grouped.T / total).T * 100
    percent = percent.reset_index().fillna(0)

    # Build response: list of { timestamp, positive, neutral, negative }
    series = []
    for _, row in percent.iterrows():
        series.append({
            "timestamp": row["datetime"].isoformat(),
            "positive": float(row.get("positive", 0)),
            "neutral": float(row.get("neutral", 0)),
            "negative": float(row.get("negative", 0)),
        })

    return {"coin": coin, "frame": timeframe, "interval": interval, "series": series}
