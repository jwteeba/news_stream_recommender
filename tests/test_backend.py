import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from pymongo.errors import ConnectionFailure

from news_stream_recommender.backend.app.main import app, get_db, get_mongo_client


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_db():
    mock = Mock()
    mock.topics.aggregate.return_value = [
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
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    yield
    app.dependency_overrides.clear()


def test_trending_topics_success(client):

    response = client.get("/trending")

    assert response.status_code == 200
    assert len(response.json()["topics"]) == 2


def test_trending_topics_empty(client, mock_db):
    """Should return an empty list when no topics exist."""
    mock_db.topics.aggregate.return_value = []

    response = client.get("/trending")

    assert response.status_code == 200
    assert response.json() == {"topics": []}


@patch("news_stream_recommender.backend.app.main.MongoClient")
def test_get_mongo_client_success(mock_client):
    """Mongo connection should succeed and return client instance."""
    mock_instance = Mock()
    mock_client.return_value = mock_instance
    mock_instance.admin.command.return_value = True  # simulate successful ping

    client = get_mongo_client("mongodb://test:27017")

    assert client == mock_instance
    mock_instance.admin.command.assert_called_once_with("ping")


@patch("news_stream_recommender.backend.app.main.MongoClient")
def test_get_mongo_client_failure(mock_client):
    """Should raise ConnectionFailure when Mongo ping fails."""
    mock_instance = Mock()
    mock_client.return_value = mock_instance
    mock_instance.admin.command.side_effect = ConnectionFailure("fail")

    with pytest.raises(ConnectionFailure):
        get_mongo_client("mongodb://invalid:27017")

    mock_instance.admin.command.assert_called_once_with("ping")


def test_trending_topics_database_error(client, mock_db):
    """Should raise exception if MongoDB query throws error."""
    mock_db.topics.aggregate.side_effect = Exception("Database error")

    response = client.get("/trending")

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal Server Error"
