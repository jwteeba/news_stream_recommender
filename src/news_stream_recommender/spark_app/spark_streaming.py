import os
import time
import socket
import logging
import tempfile
from openai import OpenAI
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    concat,
    lit,
    lower,
    regexp_replace,
    monotonically_increasing_id,
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from news_stream_recommender.frontend.streamlit_app import NewsRecommenderApp

load_dotenv()


class NewsStreamProcessor:
    """Real-time news stream processor that ingests Kafka messages, applies NLP preprocessing,
    generates AI-powered topic classifications, and stores results in MongoDB.

    This processor handles the complete pipeline from raw news data to categorized topics,
    integrating Spark Structured Streaming, OpenAI API, and MongoDB for scalable news analysis.
    """

    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = None
        self.mongo_collection = None
        self.schema = self._create_schema()

        self.openai_client = OpenAI(api_key=NewsRecommenderApp().openai_api_key)

    def setup_logging(self):
        """Configure logging with both console and file handlers for comprehensive monitoring.

        Sets up INFO level logging with timestamped format to track processing status,
        errors, and system events during streaming operations.
        """
        log_file = os.path.join(tempfile.gettempdir(), "news_stream.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(), logging.FileHandler(log_file)],
        )

    def _create_schema(self):
        """Define the expected schema for incoming Kafka news messages.

        Returns:
            StructType: Spark SQL schema matching NewsAPI JSON structure with fields
                       for source, author, title, description, url, image, timestamp, and content.
        """
        return StructType(
            [
                StructField("source", StringType(), True),
                StructField("author", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("url", StringType(), True),
                StructField("urlToImage", StringType(), True),
                StructField("publishedAt", StringType(), True),
                StructField("content", StringType(), True),
            ]
        )

    def wait_for_kafka(self, timeout=300):
        """Wait for Kafka broker to become available before starting stream processing.

        Args:
            timeout (int): Maximum seconds to wait for Kafka connection (default: 300)

        Returns:
            bool: True if Kafka is ready

        Raises:
            Exception: If Kafka connection timeout is exceeded
        """
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

    def initialize_connections(self):
        """Initialize Spark session and MongoDB connection after Kafka readiness check.

        Creates SparkSession for stream processing and establishes MongoDB client
        connection to the 'news.topics' collection for storing processed results.
        """
        self.wait_for_kafka()
        time.sleep(10)
        self.spark = SparkSession.builder.appName("NewsStreamProcessor").getOrCreate()
        mongo_client = self.get_mongo_client(os.getenv("MONGO_URI"))
        self.mongo_collection = mongo_client["news"]["topics"]
        self.mongo_collection.create_index("url", unique=True, sparse=True)

    def preprocess_text(self, df):
        """Apply NLP preprocessing pipeline to clean and tokenize news text data.

        Args:
            df: Spark DataFrame containing news records with title and description

        Returns:
            DataFrame: Processed DataFrame with additional columns for cleaned text,
                      tokenized words, and filtered tokens (stopwords removed)
        """
        text_df = df.withColumn(
            "text",
            lower(
                regexp_replace(
                    concat(col("title"), lit(" "), col("description")),
                    "[^a-zA-Z\\s]",
                    "",
                )
            ),
        )

        tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
        tokenized_df = tokenizer.transform(text_df)

        remover = StopWordsRemover(
            inputCol="words",
            outputCol="filtered",
            stopWords=StopWordsRemover.loadDefaultStopWords("english"),
        )
        return remover.transform(tokenized_df)

    def generate_topic(self, title, description):
        """Calls OpenAI for one record and returns a clean topic string."""
        try:

            TOPIC_CLASSIFIER_PROMPT = f"""
                You are a classifier that assigns a single meaningful topic to a news item.
                You must choose exactly one topic from the allowed list:
                ["Sport", "Entertainment", "Politics", "Weather", "Economy", "Legal/Justice", "Technology"]

                Follow the rules:
                - Always return one topic only.
                - Do NOT invent new categories.
                - Base your decision solely on the title and description.
                - If the content is unclear, choose the closest reasonable topic.
                - Return ONLY the topic, nothing else \n.

                Title: {title} \n
                Description: {description}
            """

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional news categorizer.",
                    },
                    {"role": "user", "content": TOPIC_CLASSIFIER_PROMPT},
                ],
                max_tokens=30,
                temperature=0.4,
            )

            topic = response.choices[0].message.content.strip()
            return topic.replace("\n", " ").replace('"', "").replace("'", "").strip()
        except Exception as e:
            self.logger.error(f"OpenAI API error: {e}")
            return "Unknown Topic"

    def save_to_mongo(self, df):
        """Process DataFrame records, generate AI topics, and persist to MongoDB.

        Only inserts a news document if it is not already in MongoDB.
        Uniqueness is determined by the article URL.
        """
        rows = (
            df.withColumn("row_id", monotonically_increasing_id())
            .orderBy("row_id")
            .select(
                "row_id",
                "source",
                "author",
                "title",
                "description",
                "url",
                "urlToImage",
                "publishedAt",
                "content",
            )
            .collect()
        )

        for row in rows:
            title = row.title or ""
            description = row.description or ""
            url = row.url or ""

            if url:
                existing = self.mongo_collection.find_one({"url": url})
                if existing:
                    self.logger.info(f"Skipping duplicate: {title[:70]}")
                    continue
            else:
                existing = self.mongo_collection.find_one(
                    {"title": title, "publishedAt": row.publishedAt}
                )
                if existing:
                    self.logger.info(f"Skipping duplicate (title/time): {title[:70]}")
                    continue

            topic = self.generate_topic(title, description)

            doc = {
                "row_id": int(row.row_id),
                "title": title,
                "description": description,
                "url": url,
                "urlToImage": row.urlToImage,
                "publishedAt": row.publishedAt,
                "content": row.content,
                "source": row.source,
                "author": row.author,
                "topic": topic,
            }

            self.mongo_collection.insert_one(doc)
            self.logger.info(f"[INSERTED] [Topic: {topic}] — {title[:70]}")

    def process_batch(self, df, epoch_id):
        """Process a single micro-batch from the Kafka stream.

        Applies text preprocessing and saves results to MongoDB if batch contains data.

        Args:
            df: Spark DataFrame containing the current batch of news records
            epoch_id: Unique identifier for this streaming batch
        """
        count = df.count()
        if count > 0:
            self.logger.info(f"Processing batch {epoch_id} with {count} records...")
            filtered_df = self.preprocess_text(df)
            self.save_to_mongo(filtered_df)
            self.logger.info(f"Batch {epoch_id} completed.")

    def start_streaming(self):
        """Start the main Spark Structured Streaming pipeline.

        Configures Kafka source, applies JSON parsing and filtering, then processes
        micro-batches every 15 seconds with checkpoint recovery for fault tolerance.
        """
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "raw_news")
            .option("startingOffsets", "latest")
            .load()
        )

        parsed = (
            df.select(from_json(col("value").cast("string"), self.schema).alias("data"))
            .select("data.*")
            .filter(col("description").isNotNull() & (col("description") != ""))
        )

        query = (
            parsed.writeStream.foreachBatch(self.process_batch)
            .option(
                "checkpointLocation", os.path.join(tempfile.gettempdir(), "checkpoint")
            )
            .trigger(processingTime="15 seconds")
            .start()
        )

        query.awaitTermination()

    def get_mongo_client(self, uri: str) -> MongoClient:
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


def main():
    """Entry point for the news stream processing application.

    Initializes the NewsStreamProcessor, establishes connections, and starts
    the continuous streaming pipeline for real-time news topic classification.
    """
    processor = NewsStreamProcessor()
    processor.initialize_connections()
    processor.start_streaming()


if __name__ == "__main__":
    main()
