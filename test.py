import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from transformers import pipeline
from tqdm import tqdm

# =========================
# LOAD ENVIRONMENT VARIABLES
# =========================
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB", "crypto_reddit_db")
collection_name = os.getenv("MONGO_COLLECTION", "latest_reddit")

client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

print(f"\n🔍 Connected to MongoDB: {db_name}")
print(f"📂 Reading posts from collection: {collection_name}\n")

# =========================
# SET UP ML MODEL
# =========================
print("🧠 Loading zero-shot classification model (BART)... this may take a few seconds.\n")
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

candidate_labels = ["Bitcoin", "Ethereum", "Solana", "Cardano", "Ripple", "Dogecoin", "Polygon", "Polkadot", "Other"]

# =========================
# BATCH SETTINGS
# =========================
BATCH_SIZE = 20  # number of posts per batch
SLEEP_BETWEEN_BATCHES = 5  # seconds delay between batches

# Filter: only process posts that don't have ML coin label yet
query = {"coin_ml": {"$exists": False}}

total_to_process = collection.count_documents(query)
print(f"🧾 Found {total_to_process} posts without coin_ml label.\n")

# =========================
# PROCESS IN BATCHES
# =========================
processed = 0

while True:
    posts = list(collection.find(query, {"_id": 1, "title": 1, "selftext": 1}).limit(BATCH_SIZE))
    if not posts:
        break

    for post in tqdm(posts, desc=f"Processing batch ({processed}/{total_to_process})"):
        text = f"{post.get('title', '')} {post.get('selftext', '')}".strip()
        if not text:
            continue

        try:
            result = classifier(text, candidate_labels=candidate_labels)
            top_label = result["labels"][0]
            confidence = float(result["scores"][0])

            # Update in MongoDB
            collection.update_one(
                {"_id": post["_id"]},
                {"$set": {"coin_ml": top_label, "ml_confidence": confidence}}
            )

            processed += 1
        except Exception as e:
            print(f"⚠️ Error analyzing post {post.get('_id')}: {e}")

    print(f"✅ Processed {processed} so far. Sleeping {SLEEP_BETWEEN_BATCHES}s...\n")
    time.sleep(SLEEP_BETWEEN_BATCHES)

print(f"\n🎉 Done! Labeled {processed} posts with ML-based coin names.\n")
