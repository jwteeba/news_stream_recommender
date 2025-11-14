import os
import logging
import tempfile
from fastapi import FastAPI, Depends, HTTPException
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

log_file = os.path.join(tempfile.gettempdir(), "news_stream.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)],
)


def get_mongo_client(uri: str) -> MongoClient:
    """Create Mongo Client

    Args:
        uri (str): Mongo connection uri

    Returns:
        MongoClient: MongoDB Client
    """
    client = MongoClient(uri, server_api=ServerApi("1"))
    try:
        client.admin.command("ping")
        logging.info("Successfully connected to MongoDB!")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        raise


load_dotenv()
mongo_client = os.getenv("MONGO_URI")


def get_db():
    """Return the MongoDB database via FastAPI DI."""
    client = get_mongo_client(mongo_client)
    return client.news


app = FastAPI()


@app.get("/trending")
def trending_topics(db=Depends(get_db)):
    try:
        pipeline = [
            {"$sort": {"publishedAt": -1}},  # newest first
            {
                "$group": {
                    "_id": "$topic",
                    "doc": {"$first": "$$ROOT"},  # newest doc for each topic
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {"_id": 0}},
            {"$sort": {"publishedAt": -1}},  # optional re-sort
        ]

        topics = list(db.topics.aggregate(pipeline))
        return {"topics": topics}

    except Exception as e:
        logging.error(f"Aggregation error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
