import os
from fastapi import FastAPI
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()
mongo_client = os.getenv("MONGO_CLIENT", "mongodb://mongo:27017/")
app = FastAPI()
client = MongoClient(mongo_client)
db = client.news


@app.get("/trending")
def trending_topics():
    topics = list(db.topics.find({}, {"_id": 0}).limit(20))
    return {"topics": topics}
