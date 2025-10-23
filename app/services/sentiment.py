from datetime import datetime, timedelta
from dateutil import parser as dateparse
from collections import defaultdict
def parse_timestamp(ts):
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            try:
                return dateparse.parse(ts)
            except Exception:
                return None
    return None



def get_sentiment_timeseries(collection, coin: str, timeframe: str):
    print("="*80)
    print(f"[🟢 START] get_sentiment_timeseries called at {datetime.utcnow().isoformat()}")
    print(f"[ℹ️ Params] coin={coin}, timeframe={timeframe}")

    # Parse timeframe into days
    days_map = {"24h": 1, "7d": 7, "30d": 30, "60d": 60}
    days = days_map.get(timeframe.lower(), 7)
    cutoff = datetime.utcnow() - timedelta(days=days)

    # Mongo query
    query = {
        "coin": {"$regex": f"^{coin}$", "$options": "i"},
    }

    print(f"[📦 Mongo Query] {query}")
    docs = list(collection.find(query))
    print(f"[📊 Mongo Results] Found {len(docs)} documents for query")

    if not docs:
        return {"coin": coin, "frame": timeframe, "series": []}

    # Filter by date
    filtered = []
    for d in docs:
        try:
            dt = datetime.strptime(d["created_utc"], "%Y-%m-%d %H:%M:%S")
            if dt >= cutoff:
                filtered.append({**d, "created_dt": dt})
        except Exception:
            continue

    print(f"[✅ After filtering by time] Remaining rows: {len(filtered)}")

    # Group by date (day) and sentiment
    daily_counts = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0})
    for d in filtered:
        day = d["created_dt"].strftime("%Y-%m-%d")
        sent = d.get("sentiment", "").lower()
        if sent in daily_counts[day]:
            daily_counts[day][sent] += 1

    # Convert to sorted list
    timeseries = [
        {
            "timestamp": day + "T00:00:00",
            "positive": daily_counts[day]["positive"],
            "neutral": daily_counts[day]["neutral"],
            "negative": daily_counts[day]["negative"],
        }
        for day in sorted(daily_counts.keys())
    ]

    print(f"[📈 Sample Output] {timeseries[:5]}")
    print("="*80)

    return {"coin": coin, "frame": timeframe, "series": timeseries}
