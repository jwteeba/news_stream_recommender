from unittest.mock import Mock, patch

import mongomock
import pandas as pd
import pytest

from news_stream_recommender.backend.app.main import app as fastapi_app
from news_stream_recommender.frontend.streamlit_app import NewsRecommenderApp
from news_stream_recommender.kafka_producer.news_producer import NewsProducer


class TestIntegration:
    """Integration tests for the complete news stream pipeline"""

    @pytest.fixture
    def mock_mongo_db(self):
        """Mock MongoDB database with sample data"""
        client = mongomock.MongoClient()
        db = client.news

        sample_topics = [
            {
                "title": "AI Revolution in Healthcare",
                "description": "Artificial intelligence is transforming medical diagnosis",
                "topic": "Technology",
                "url": "https://example.com/ai-healthcare",
                "publishedAt": "2024-01-01T12:00:00Z",
                "author": "Dr. Smith",
                "source": "Tech News",
            },
            {
                "title": "Olympic Games Update",
                "description": "Latest updates from the Olympic games",
                "topic": "Sports",
                "url": "https://example.com/olympics",
                "publishedAt": "2024-01-01T13:00:00Z",
                "author": "Sports Reporter",
                "source": "Sports Daily",
            },
        ]

        db.topics.insert_many(sample_topics)
        return db

    @pytest.fixture
    def fastapi_client(self, mock_mongo_db):
        """FastAPI test client with mocked database"""
        from fastapi.testclient import TestClient

        with patch("news_stream_recommender.backend.app.main.db", mock_mongo_db):
            client = TestClient(fastapi_app)
            yield client

    def test_end_to_end_data_flow(self, fastapi_client, mock_env_vars):
        """Test complete data flow from API to frontend"""
        response = fastapi_client.get("/trending")
        assert response.status_code == 200

        data = response.json()
        assert "topics" in data
        assert len(data["topics"]) == 2

        topic = data["topics"][0]
        required_fields = ["title", "description", "topic", "url", "publishedAt"]
        for field in required_fields:
            assert field in topic

    def test_kafka_producer_integration(self, mock_env_vars, sample_news_articles):
        """Test Kafka producer with mocked external dependencies"""
        with patch("socket.socket") as mock_socket:
            with patch(
                "news_stream_recommender.kafka_producer.news_producer.KafkaProducer"
            ) as mock_kafka:
                with patch("requests.get") as mock_requests:
                    mock_socket_instance = Mock()
                    mock_socket.return_value = mock_socket_instance
                    mock_socket_instance.connect_ex.return_value = 0

                    mock_producer_instance = Mock()
                    mock_kafka.return_value = mock_producer_instance

                    mock_response = Mock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = {"articles": sample_news_articles}
                    mock_requests.return_value = mock_response

                    producer = NewsProducer()
                    producer.initialize_producer()
                    articles = producer.fetch_news()

                    assert len(articles) == 2
                    assert articles[0]["title"] == "Test News Title"

                    producer.publish_articles(articles)
                    assert mock_producer_instance.send.call_count == 2

    def test_frontend_backend_integration(self, fastapi_client, mock_env_vars):
        """Test frontend integration with backend API"""
        with patch("streamlit.set_page_config"):
            with patch("requests.get") as mock_requests:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "topics": [
                        {
                            "title": "Integration Test Article",
                            "description": "Testing integration between components",
                            "topic": "Technology",
                            "url": "https://test.com",
                            "publishedAt": "2024-01-01T12:00:00Z",
                        }
                    ]
                }
                mock_response.raise_for_status.return_value = None
                mock_requests.return_value = mock_response

                app = NewsRecommenderApp()
                app.fetch_news.clear()
                df = app.fetch_news()

                assert isinstance(df, pd.DataFrame)
                assert len(df) >= 1
