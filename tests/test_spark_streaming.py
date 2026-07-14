from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StructType

from news_stream_recommender.spark_app.spark_streaming import NewsStreamProcessor


class TestNewsStreamProcessor:
    """Test suite for Spark News Stream Processor"""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Automatically set environment variables for every single test."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-api-key")

    @pytest.fixture
    def processor(self):
        """Create NewsStreamProcessor instance with active mocks."""
        # Use 'yield' to keep the context managers alive for the duration of the test
        with (
            patch("news_stream_recommender.spark_app.spark_streaming.load_dotenv"),
            patch(
                "news_stream_recommender.spark_app.spark_streaming.OpenAI"
            ) as mock_openai_cls,
        ):

            proc = NewsStreamProcessor()

            # Extract and expose the mock client instance so tests can assign return values to it
            proc.openai_client = mock_openai_cls.return_value

            yield proc

    def test_init(self, processor):
        """Test NewsStreamProcessor initialization"""
        assert processor.spark is None
        assert processor.mongo_collection is None
        assert isinstance(processor.schema, StructType)
        assert len(processor.schema.fields) == 8

    def test_create_schema(self, processor):
        """Test schema creation"""
        schema = processor._create_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "source",
            "author",
            "title",
            "description",
            "url",
            "urlToImage",
            "publishedAt",
            "content",
        ]
        assert field_names == expected_fields

    @patch("socket.socket")
    def test_wait_for_kafka_success(self, mock_socket, processor):
        """Test successful Kafka connection wait"""
        mock_sock_instance = Mock()
        mock_socket.return_value = mock_sock_instance
        mock_sock_instance.connect_ex.return_value = 0

        result = processor.wait_for_kafka(timeout=10)

        assert result is True
        mock_sock_instance.connect_ex.assert_called_with(("kafka", 9092))

    @patch("socket.socket")
    @patch("time.sleep")
    def test_wait_for_kafka_timeout(self, mock_sleep, mock_socket, processor):
        """Test Kafka connection timeout"""
        mock_sock_instance = Mock()
        mock_socket.return_value = mock_sock_instance
        mock_sock_instance.connect_ex.return_value = 1

        with pytest.raises(Exception, match="Kafka connection timeout"):
            processor.wait_for_kafka(timeout=1)

    @patch("news_stream_recommender.spark_app.spark_streaming.SparkSession")
    @patch.object(NewsStreamProcessor, "get_mongo_client")
    @patch.object(NewsStreamProcessor, "wait_for_kafka")
    @patch("time.sleep")
    def test_initialize_connections(
        self, mock_sleep, mock_wait, mock_mongo, mock_spark, processor
    ):
        """Test initialization of Spark and MongoDB connections"""
        mock_spark_instance = Mock()
        mock_spark.builder.appName.return_value.getOrCreate.return_value = (
            mock_spark_instance
        )
        mock_mongo_client = Mock()
        mock_mongo_collection = Mock()
        mock_mongo_client.__getitem__ = Mock(return_value=Mock())
        mock_mongo_client.__getitem__.return_value.__getitem__ = Mock(
            return_value=mock_mongo_collection
        )
        mock_mongo.return_value = mock_mongo_client
        mock_wait.return_value = True

        processor.initialize_connections()

        assert processor.spark == mock_spark_instance
        mock_wait.assert_called_once()
        mock_mongo.assert_called_once()

    def test_generate_topic_success(self, processor, mock_openai_response):
        """Test successful topic generation with OpenAI"""
        processor.openai_client.chat.completions.create.return_value = (
            mock_openai_response
        )

        topic = processor.generate_topic("Test Title", "Test Description")

        assert topic == "Technology"
        processor.openai_client.chat.completions.create.assert_called_once()

    def test_generate_topic_api_error(self, processor):
        """Test topic generation with OpenAI API error"""
        processor.openai_client.chat.completions.create.side_effect = Exception(
            "API Error"
        )

        topic = processor.generate_topic("Test Title", "Test Description")

        assert topic == "Unknown Topic"

    @patch("news_stream_recommender.spark_app.spark_streaming.MongoClient")
    def test_get_mongo_client_success(self, mock_client, processor):
        """Test successful MongoDB client creation"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        mock_instance.admin.command.return_value = True

        client = processor.get_mongo_client("mongodb://test:test@localhost:27017/test")

        assert client == mock_instance
        mock_instance.admin.command.assert_called_once_with("ping")

    @patch("news_stream_recommender.spark_app.spark_streaming.MongoClient")
    def test_get_mongo_client_failure(self, mock_client, processor):
        """Test MongoDB client creation failure"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        mock_instance.admin.command.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):
            processor.get_mongo_client("mongodb://invalid:uri")

    def test_process_batch_with_data(self, processor):
        """Test processing a batch with data"""
        mock_df = Mock()
        mock_df.count.return_value = 5

        with patch.object(processor, "preprocess_text") as mock_preprocess:
            with patch.object(processor, "save_to_mongo") as mock_save:
                mock_filtered_df = Mock()
                mock_preprocess.return_value = mock_filtered_df

                processor.process_batch(mock_df, 1)

                mock_preprocess.assert_called_once_with(mock_df)
                mock_save.assert_called_once_with(mock_filtered_df)

    def test_process_batch_empty(self, processor):
        """Test processing an empty batch"""
        mock_df = Mock()
        mock_df.count.return_value = 0

        with patch.object(processor, "preprocess_text") as mock_preprocess:
            with patch.object(processor, "save_to_mongo") as mock_save:
                processor.process_batch(mock_df, 1)

                mock_preprocess.assert_not_called()
                mock_save.assert_not_called()
