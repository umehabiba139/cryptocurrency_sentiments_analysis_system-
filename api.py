from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Allow React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
client = MongoClient(os.getenv("MONGO_URI"))
db = client["crypto_reddit_db"]
collection = db["latest_reddit"]

@app.get("/api/sentiment-timeseries")
def get_sentiment_timeseries(coin: str = Query(...)):
    """Fetch average daily sentiment for a specific coin"""
    pipeline = [
        {"$match": {"coin": coin}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_utc"}},
                "avg_sentiment": {"$avg": "$sentiment"},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    data = list(collection.aggregate(pipeline))
    return {"coin": coin, "data": data}
