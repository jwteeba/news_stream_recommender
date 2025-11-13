from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from pymongo.errors import ConnectionFailure

from news_stream_recommender.backend.app.main import app, get_mongo_client


class TestBackendAPI:
    """Test suite for FastAPI backend"""

    @pytest.fixture
    def client(self):
        """FastAPI test client"""
        return TestClient(app)

    @pytest.fixture
    def mock_db_data(self):
        """Mock database data"""
        return {
            "topics": [
                {
                    "title": "Test Article 1",
                    "description": "Test description 1",
                    "topic": "Technology",
                    "url": "https://test1.com",
                    "publishedAt": "2024-01-01T12:00:00Z",
                },
                {
                    "title": "Test Article 2",
                    "description": "Test description 2",
                    "topic": "Sports",
                    "url": "https://test2.com",
                    "publishedAt": "2024-01-01T13:00:00Z",
                },
            ]
        }

    @patch("news_stream_recommender.backend.app.main.db")
    def test_trending_topics_success(self, mock_db, client, mock_db_data):
        """Test successful trending topics retrieval"""
        mock_db.topics.find.return_value.limit.return_value = mock_db_data["topics"]

        response = client.get("/trending")

        assert response.status_code == 200
        data = response.json()
        assert "topics" in data
        assert len(data["topics"]) == 2
        assert data["topics"][0]["title"] == "Test Article 1"
        assert data["topics"][1]["topic"] == "Sports"

    @patch("news_stream_recommender.backend.app.main.db")
    def test_trending_topics_empty(self, mock_db, client):
        """Test trending topics with empty database"""
        mock_db.topics.find.return_value.limit.return_value = []

        response = client.get("/trending")

        assert response.status_code == 200
        data = response.json()
        assert data["topics"] == []

    @patch("news_stream_recommender.backend.app.main.MongoClient")
    def test_get_mongo_client_success(self, mock_client):
        """Test successful MongoDB connection"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        mock_instance.admin.command.return_value = True

        client = get_mongo_client("mongodb://test:test@localhost:27017/test")

        assert client == mock_instance
        mock_instance.admin.command.assert_called_once_with("ping")

    @patch("news_stream_recommender.backend.app.main.MongoClient")
    def test_get_mongo_client_failure(self, mock_client):
        """Test MongoDB connection failure"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        mock_instance.admin.command.side_effect = ConnectionFailure("Connection failed")

        with pytest.raises(ConnectionFailure):
            get_mongo_client("mongodb://invalid:8000")

    @patch("news_stream_recommender.backend.app.main.db")
    def test_trending_topics_database_error(self, mock_db, client):
        """Test trending topics with database error"""
        mock_db.topics.find.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            client.get("/trending")
