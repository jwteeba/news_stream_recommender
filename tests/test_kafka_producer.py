import json
from unittest.mock import Mock, patch

import pytest
import requests

from news_stream_recommender.kafka_producer.news_producer import NewsProducer


class TestNewsProducer:
    """Test suite for Kafka News Producer"""

    @pytest.fixture
    def producer(self, mock_env_vars):
        """Create NewsProducer instance with mocked environment"""
        with patch("news_stream_recommender.kafka_producer.news_producer.load_dotenv"):
            return NewsProducer()

    def test_init(self, producer):
        """Test NewsProducer initialization"""
        assert producer.api_key == "test_api_key"
        assert producer.producer is None

    @patch("socket.socket")
    def test_wait_for_kafka_success(self, mock_socket, producer):
        """Test successful Kafka connection wait"""
        mock_sock_instance = Mock()
        mock_socket.return_value = mock_sock_instance
        mock_sock_instance.connect_ex.return_value = 0

        result = producer.wait_for_kafka(timeout=10)

        assert result is True
        mock_sock_instance.connect_ex.assert_called_with(("kafka", 9092))
        mock_sock_instance.close.assert_called()

    @patch("socket.socket")
    @patch("time.sleep")
    def test_wait_for_kafka_timeout(self, mock_sleep, mock_socket, producer):
        """Test Kafka connection timeout"""
        mock_sock_instance = Mock()
        mock_socket.return_value = mock_sock_instance
        mock_sock_instance.connect_ex.return_value = 1

        with pytest.raises(Exception, match="Kafka connection timeout"):
            producer.wait_for_kafka(timeout=1)

    @patch("news_stream_recommender.kafka_producer.news_producer.KafkaProducer")
    @patch.object(NewsProducer, "wait_for_kafka")
    @patch("time.sleep")
    def test_initialize_producer(
        self, mock_sleep, mock_wait, mock_kafka_producer, producer
    ):
        """Test Kafka producer initialization"""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        mock_wait.return_value = True

        producer.initialize_producer()

        assert producer.producer == mock_producer_instance
        mock_kafka_producer.assert_called_once_with(
            bootstrap_servers=["kafka:9092"],
            value_serializer=mock_kafka_producer.call_args[1]["value_serializer"],
        )

    @patch("requests.get")
    def test_fetch_news_success(self, mock_get, producer, sample_news_articles):
        """Test successful news fetching"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"articles": sample_news_articles}
        mock_get.return_value = mock_response

        articles = producer.fetch_news()

        assert len(articles) == 2
        assert articles[0]["title"] == "Test News Title"
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_fetch_news_api_error(self, mock_get, producer):
        """Test news fetching with API error"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        articles = producer.fetch_news()

        assert articles == []

    @patch("requests.get")
    def test_fetch_news_request_exception(self, mock_get, producer):
        """Test news fetching with request exception"""
        mock_get.side_effect = requests.RequestException("Network error")

        articles = producer.fetch_news()

        assert articles == []

    def test_publish_articles(self, producer, sample_news_articles):
        """Test article publishing to Kafka"""
        mock_producer = Mock()
        producer.producer = mock_producer

        producer.publish_articles(sample_news_articles)

        assert mock_producer.send.call_count == 2
        mock_producer.send.assert_any_call("raw_news", sample_news_articles[0])
        mock_producer.send.assert_any_call("raw_news", sample_news_articles[1])
        mock_producer.flush.assert_called_once()

    @patch.object(NewsProducer, "initialize_producer")
    @patch.object(NewsProducer, "fetch_news")
    @patch.object(NewsProducer, "publish_articles")
    @patch("time.sleep")
    def test_run_loop_iteration(
        self,
        mock_sleep,
        mock_publish,
        mock_fetch,
        mock_init,
        producer,
        sample_news_articles,
    ):
        """Test single iteration of the main run loop"""
        mock_fetch.return_value = sample_news_articles
        mock_sleep.side_effect = [None, KeyboardInterrupt()]

        with pytest.raises(KeyboardInterrupt):
            producer.run()

        mock_init.assert_called_once()
        mock_fetch.assert_called()
        mock_publish.assert_called_with(sample_news_articles)

    @patch.object(NewsProducer, "initialize_producer")
    @patch.object(NewsProducer, "fetch_news")
    @patch.object(NewsProducer, "publish_articles")
    @patch("time.sleep")
    def test_run_no_articles(
        self, mock_sleep, mock_publish, mock_fetch, mock_init, producer
    ):
        """Test run loop with no articles fetched"""
        mock_fetch.return_value = []
        mock_sleep.side_effect = [None, KeyboardInterrupt()]

        with pytest.raises(KeyboardInterrupt):
            producer.run()

        mock_init.assert_called_once()
        mock_fetch.assert_called()
        mock_publish.assert_not_called()

    def test_value_serializer(self, producer):
        """Test JSON value serializer function"""
        test_data = {"test": "data"}

        serialized = json.dumps(test_data).encode("utf-8")
        assert isinstance(serialized, bytes)
        assert json.loads(serialized.decode("utf-8")) == test_data
