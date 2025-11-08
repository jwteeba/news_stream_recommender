import os
import json
import time
import socket
import requests
import logging
import tempfile
from kafka import KafkaProducer
from dotenv import load_dotenv


class NewsProducer:
    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        load_dotenv()
        self.api_key = os.getenv("NEWSAPI_KEY")
        self.producer = None

    def setup_logging(self):
        log_file = os.path.join(tempfile.gettempdir(), "news_stream.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_file),
            ],
        )

    def wait_for_kafka(self, timeout=300):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(("kafka", 9092))
                sock.close()
                if result == 0:
                    self.logger.info("Kafka is ready!")
                    return True
            except Exception as e:
                self.logger.error(f"Connection error: {e}")
            self.logger.info("Waiting for Kafka...")
            time.sleep(10)
        raise Exception("Kafka connection timeout")

    def initialize_producer(self):
        self.wait_for_kafka()
        time.sleep(5)
        self.producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def fetch_news(self):
        url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={self.api_key}"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                articles = response.json().get("articles", [])
                for article in articles:
                    if article.get("source") and isinstance(article["source"], dict):
                        article["source"] = article["source"].get("name", "Unknown")
                return articles
        except Exception as e:
            self.logger.error(f"Error fetching news: {e}")
        return []

    def publish_articles(self, articles):
        for i, article in enumerate(articles):
            self.producer.send("raw_news", article)
            self.logger.debug(
                f"Sent article {i + 1}: {article.get('title', 'No title')[:50]}..."
            )
        self.producer.flush()

    def run(self):
        self.initialize_producer()

        self.logger.info("Testing initial fetch...")
        test_articles = self.fetch_news()
        self.logger.info(f"Fetched {len(test_articles)} articles")
        self.logger.info("Starting main loop...")

        while True:
            self.logger.info("Fetching and publishing news...")
            articles = self.fetch_news()
            self.logger.info(f"Fetched {len(articles)} articles")

            if articles:
                self.publish_articles(articles)
                self.logger.info("Batch sent successfully. Waiting 60 seconds...")
            else:
                self.logger.warning("No articles fetched. Waiting 60 seconds...")

            time.sleep(60)


def main():
    producer = NewsProducer()
    producer.run()


if __name__ == "__main__":
    main()
