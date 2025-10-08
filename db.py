from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
collection = db[os.getenv("MONGO_COLLECTION")]

# Test insert
test_doc = {"message": "MongoDB connection successful!"}
collection.insert_one(test_doc)

print("✅ Connection successful and document inserted!")
