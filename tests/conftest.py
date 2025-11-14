import os
from unittest.mock import Mock, patch

import mongomock
import pytest

from news_stream_recommender.backend.app.main import app, get_db


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing"""
    env_vars = {
        "MONGO_URI": "mongodb://test:test@localhost:27017/test",
        "NEWSAPI_KEY": "test_api_key",
        "OPENAI_API_KEY": "test_openai_key",
        "FASTAPI_URL": "http://localhost:8000",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_mongo_client():
    """Mock MongoDB client using mongomock"""
    return mongomock.MongoClient()


@pytest.fixture
def sample_news_article():
    """Sample news article for testing"""
    return {
        "source": "Test Source",
        "author": "Test Author",
        "title": "Test News Title",
        "description": "This is a test news description",
        "url": "https://test.com/article",
        "urlToImage": "https://test.com/image.jpg",
        "publishedAt": "2024-01-01T12:00:00Z",
        "content": "Test content",
    }


@pytest.fixture
def sample_news_articles(sample_news_article):
    """Multiple sample news articles"""
    return [
        sample_news_article,
        {
            **sample_news_article,
            "title": "Another Test Article",
            "description": "Another test description",
        },
    ]


@pytest.fixture
def mock_openai_response():
    """Mock OpenAI API response"""
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = "Technology"
    return mock_response


@pytest.fixture
def mock_db():
    mock = Mock()
    mock.topics.find.return_value.limit.return_value = [
        {"title": "Test1", "topic": "Tech"},
        {"title": "Test2", "topic": "Sports"},
    ]
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    yield
    app.dependency_overrides.clear()
