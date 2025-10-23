from fastapi import APIRouter, Query
from app.db.mongo import db
from datetime import datetime, timedelta

router = APIRouter(prefix="/api")

@router.get("/sentiment-timeseries")
def sentiment_timeseries(
    coin: str = Query(..., description="Coin symbol, e.g., BTC, ETH"),
    timeframe: str = Query("30d", description="24h, 7d, 30d, 60d")
):
    collection = db["coin_sentiment_summary"]

    # Normalize coin
    coin = coin.upper()

    # ---------------- TIME RANGE ----------------
    now = datetime.utcnow()
    if timeframe == "24h":
        start_time = now - timedelta(hours=24)
    elif timeframe == "7d":
        start_time = now - timedelta(days=7)
    elif timeframe == "30d":
        start_time = now - timedelta(days=30)
    elif timeframe == "60d":
        start_time = now - timedelta(days=60)
    else:
        start_time = now - timedelta(days=30)

    # ---------------- QUERY ----------------
    query = {
        "coin": coin,
        "timestamp": {"$gte": start_time}
    }

    print(f"[📦 Mongo Query] {query}")

    docs = list(collection.find(query).sort("timestamp", 1))
    print(f"[📊 Found {len(docs)} documents for {coin}]")

    # ---------------- FORMAT ----------------
    if not docs:
        return {
            "coin": coin,
            "frame": timeframe,
            "series": [],
            "message": f"No data available for {coin} in the last {timeframe}."
        }

    rows = [
        {
            "timestamp": d["timestamp"].isoformat() if "timestamp" in d else None,
            "positive": round(float(d.get("positive", 0)), 2),
            "neutral": round(float(d.get("neutral", 0)), 2),
            "negative": round(float(d.get("negative", 0)), 2),
        }
        for d in docs
    ]

    return {"coin": coin, "frame": timeframe, "series": rows}
