import time
import socket
import logging
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StopWordsRemover, CountVectorizer, RegexTokenizer
from pyspark.ml.clustering import LDA
from pymongo import MongoClient


class NewsStreamProcessor:
    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = None
        self.mongo_collection = None
        self.schema = self._create_schema()

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

    def _create_schema(self):
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
        self.wait_for_kafka()
        time.sleep(10)

        self.spark = SparkSession.builder.appName("NewsStreamProcessor").getOrCreate()

        mongo_client = MongoClient("mongodb://mongo:27017/")
        self.mongo_collection = mongo_client["news"]["topics"]

    def preprocess_text(self, df):
        text_df = df.withColumn(
            "text",
            lower(
                regexp_replace(
                    concat(col("title"), lit(" "), col("content")),
                    "[^a-zA-Z\\s]",
                    "",
                )
            ),
        )

        tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
        words_df = tokenizer.transform(text_df)

        custom_stop_words = StopWordsRemover.loadDefaultStopWords("english") + [
            "said",
            "says",
            "according",
            "new",
            "news",
            "report",
            "reports",
            "reuters",
            "ap",
            "cnn",
            "bbc",
            "former",
            "current",
            "latest",
            "today",
            "yesterday",
            "week",
            "month",
            "year",
            "time",
            "people",
            "man",
            "woman",
            "person",
            "one",
            "two",
            "three",
            "first",
            "last",
            "also",
            "would",
            "could",
            "may",
        ]

        remover = StopWordsRemover(
            inputCol="words", outputCol="filtered", stopWords=custom_stop_words
        )
        return remover.transform(words_df)

    def apply_topic_modeling(self, filtered_df):
        cv = CountVectorizer(
            inputCol="filtered", outputCol="features", minDF=2, vocabSize=1000
        )
        cv_model = cv.fit(filtered_df)
        vectorized_df = cv_model.transform(filtered_df)

        lda = LDA(k=6, maxIter=20, seed=42)
        lda_model = lda.fit(vectorized_df)
        predictions_df = lda_model.transform(vectorized_df)

        return predictions_df, lda_model, cv_model.vocabulary

    def generate_topic_names(self, lda_model, vocab, epoch_id):
        topics = lda_model.describeTopics(15)
        self.logger.info(f"Processing topics for batch {epoch_id}")

        topic_names = {}
        for row in topics.collect():
            topic_id = row["topic"]
            word_indices = row["termIndices"]
            words = [vocab[i] for i in word_indices if len(vocab[i]) > 2]

            meaningful_words = [
                w for w in words[:5] if w not in ["news", "report", "story", "article"]
            ]
            topic_name = (
                " ".join(meaningful_words[:2]).title()
                if meaningful_words
                else f"Topic {topic_id}"
            )

            topic_names[topic_id] = topic_name
            self.logger.debug(
                f"Topic {topic_id} ({topic_name}): {', '.join(words[:10])}"
            )

        return topic_names

    def save_to_mongo(self, predictions_df, topic_names):
        for row in predictions_df.select(
            "source",
            "author",
            "title",
            "description",
            "url",
            "urlToImage",
            "publishedAt",
            "content",
            "filtered",
            "topicDistribution",
        ).collect():
            topic_probs = row.topicDistribution.toArray()
            dominant_topic = int(topic_probs.argmax())

            doc = {
                "title": row.title,
                "description": row.description,
                "url": row.url,
                "urlToImage": row.urlToImage,
                "publishedAt": row.publishedAt,
                "content": row.content,
                "source": row.source,
                "author": row.author,
                "filtered_words": row.filtered,
                "topic": topic_names.get(dominant_topic, "Unknown"),
            }
            self.mongo_collection.insert_one(doc)

    def process_batch(self, df, epoch_id):
        if df.count() > 0:
            filtered_df = self.preprocess_text(df)
            predictions_df, lda_model, vocab = self.apply_topic_modeling(filtered_df)
            topic_names = self.generate_topic_names(lda_model, vocab, epoch_id)
            self.save_to_mongo(predictions_df, topic_names)
            self.logger.info(f"Processed batch {epoch_id} with {df.count()} records")

    def start_streaming(self):
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
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination()


def main():
    processor = NewsStreamProcessor()
    processor.initialize_connections()
    processor.start_streaming()


if __name__ == "__main__":
    main()
