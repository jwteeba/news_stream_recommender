import os
import logging
import tempfile
from fastapi import FastAPI
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
app = FastAPI()
client = get_mongo_client(mongo_client)
db = client.news


@app.get("/trending")
def trending_topics():
    topics = list(db.topics.find({}, {"_id": 0}).limit(20))
    return {"topics": topics}
