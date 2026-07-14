from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from news_stream_recommender.frontend.streamlit_app import NewsRecommenderApp


class TestNewsRecommenderApp:
    """Test suite for Streamlit News Recommender App"""

    @pytest.fixture
    def app(self, mock_env_vars):
        """Create NewsRecommenderApp instance with mocked environment"""
        with patch("news_stream_recommender.frontend.streamlit_app.load_dotenv"):
            with patch("streamlit.set_page_config"):
                return NewsRecommenderApp()

    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame(
            [
                {
                    "title": "Tech News Article",
                    "description": "This is about technology and innovation",
                    "topic": "Technology",
                    "url": "https://test.com/tech",
                    "urlToImage": "https://test.com/tech.jpg",
                    "publishedAt": "2024-01-01T12:00:00Z",
                    "author": "Tech Author",
                    "source": "Tech Source",
                },
                {
                    "title": "Sports News Article",
                    "description": "This is about sports and games",
                    "topic": "Sports",
                    "url": "https://test.com/sports",
                    "urlToImage": "https://test.com/sports.jpg",
                    "publishedAt": "2024-01-01T13:00:00Z",
                    "author": "Sports Author",
                    "source": "Sports Source",
                },
            ]
        )

    def test_init(self, app):
        """Test NewsRecommenderApp initialization"""
        assert app.fastapi_url == "http://localhost:8000"
        assert app.logger is not None

    @patch("streamlit.set_page_config")
    def test_setup_page(self, mock_set_page_config, app):
        """Test page setup configuration"""
        app.setup_page()
        mock_set_page_config.assert_called_with(
            page_title="📰 News Recommender Dashboard", layout="wide"
        )

    def test_format_date_valid_iso(self, app):
        """Test date formatting with valid ISO date"""
        iso_date = "2024-01-01T12:00:00Z"
        formatted = app.format_date(iso_date)
        assert formatted == "January 01, 2024"

    def test_format_date_invalid(self, app):
        """Test date formatting with invalid date"""
        invalid_date = "invalid-date"
        formatted = app.format_date(invalid_date)
        assert formatted == "invalid-date"

    def test_format_date_none(self, app):
        """Test date formatting with None"""
        formatted = app.format_date(None)
        assert formatted is None

    @patch("requests.get")
    def test_fetch_news_success(self, mock_get, app, sample_news_articles):
        """Test successful news fetching"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"topics": sample_news_articles}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = app.fetch_news()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "title" in df.columns
        mock_get.assert_called_once_with(f"{app.fastapi_url}/trending", timeout=10)

    @patch("requests.get")
    @patch("streamlit.error")
    def test_fetch_news_request_error(self, mock_st_error, mock_get, app):
        """Test news fetching with request error"""
        mock_get.side_effect = requests.RequestException("Network error")

        app.fetch_news.clear()
        df = app.fetch_news()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch("requests.get")
    @patch("streamlit.error")
    def test_fetch_news_http_error(self, mock_st_error, mock_get, app):
        """Test news fetching with HTTP error"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        df = app.fetch_news()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_render_sidebar(self, app, mocker):
        """Test sidebar rendering using the native pytest mocker fixture"""

        # 1. Mock the base sidebar object
        mock_sidebar = mocker.patch("streamlit.sidebar")

        # 2. Setup the behavior of the elements on the sidebar
        mock_sidebar.button.return_value = True
        mock_sidebar.text_input.return_value = "fake-api-key"

        # 3. Execute the function under test
        refresh, api_key = app.render_sidebar()

        # 4. Assert values
        assert refresh is True
        assert api_key == "fake-api-key"

        # 5. Assert the sidebar calls directly on the mock object
        mock_sidebar.title.assert_called_once_with("🧭 Controls")
        mock_sidebar.text_input.assert_called_once_with(
            "🔑 Enter your OpenAI API key:",
            type="password",
        )
        mock_sidebar.button.assert_called_once_with("🔄 Refresh Data")

    @patch("streamlit.columns")
    @patch("streamlit.selectbox")
    @patch("streamlit.text_input")
    @patch("streamlit.info")
    def test_render_filters_with_topics(
        self, mock_info, mock_text_input, mock_selectbox, mock_columns, app, sample_df
    ):
        """Test filter rendering with topics available"""
        mock_col1 = Mock()
        mock_col2 = Mock()
        mock_col1.__enter__ = Mock(return_value=mock_col1)
        mock_col1.__exit__ = Mock(return_value=None)
        mock_col2.__enter__ = Mock(return_value=mock_col2)
        mock_col2.__exit__ = Mock(return_value=None)
        mock_columns.return_value = [mock_col1, mock_col2]
        mock_selectbox.return_value = "Technology"
        mock_text_input.return_value = "tech"

        selected_topic, keyword = app.render_filters(sample_df)

        assert selected_topic == "Technology"
        assert keyword == "tech"

    @patch("streamlit.columns")
    @patch("streamlit.selectbox")
    @patch("streamlit.text_input")
    @patch("streamlit.info")
    def test_render_filters_no_topics(
        self, mock_info, mock_text_input, mock_selectbox, mock_columns, app
    ):
        """Test filter rendering without topics"""
        empty_df = pd.DataFrame()
        mock_col1 = Mock()
        mock_col2 = Mock()
        mock_col1.__enter__ = Mock(return_value=mock_col1)
        mock_col1.__exit__ = Mock(return_value=None)
        mock_col2.__enter__ = Mock(return_value=mock_col2)
        mock_col2.__exit__ = Mock(return_value=None)
        mock_columns.return_value = [mock_col1, mock_col2]
        mock_text_input.return_value = ""

        selected_topic, keyword = app.render_filters(empty_df)

        assert selected_topic == "All"
        assert keyword == ""

    def test_filter_data_by_topic(self, app, sample_df):
        """Test data filtering by topic"""
        filtered_df = app.filter_data(sample_df, "Technology", "")

        assert len(filtered_df) == 1
        assert filtered_df.iloc[0]["topic"] == "Technology"

    def test_filter_data_by_keyword(self, app, sample_df):
        """Test data filtering by keyword"""
        filtered_df = app.filter_data(sample_df, "All", "sports")

        assert len(filtered_df) == 1
        assert "sports" in filtered_df.iloc[0]["description"].lower()

    def test_filter_data_combined(self, app, sample_df):
        """Test data filtering by both topic and keyword"""
        filtered_df = app.filter_data(sample_df, "Technology", "technology")

        assert len(filtered_df) == 1
        assert filtered_df.iloc[0]["topic"] == "Technology"

    def test_filter_data_no_matches(self, app, sample_df):
        """Test data filtering with no matches"""
        filtered_df = app.filter_data(sample_df, "Technology", "sports")

        assert len(filtered_df) == 0

    @patch("streamlit.subheader")
    @patch("streamlit.container")
    @patch("streamlit.columns")
    @patch("streamlit.image")
    @patch("streamlit.markdown")
    def test_render_articles(
        self,
        mock_markdown,
        mock_image,
        mock_columns,
        mock_container,
        mock_subheader,
        app,
        sample_df,
    ):
        """Test article rendering"""
        mock_container_instance = Mock()
        mock_container_instance.__enter__ = Mock(return_value=mock_container_instance)
        mock_container_instance.__exit__ = Mock(return_value=None)
        mock_container.return_value = mock_container_instance

        mock_col1 = Mock()
        mock_col2 = Mock()
        mock_col1.__enter__ = Mock(return_value=mock_col1)
        mock_col1.__exit__ = Mock(return_value=None)
        mock_col2.__enter__ = Mock(return_value=mock_col2)
        mock_col2.__exit__ = Mock(return_value=None)
        mock_columns.return_value = [mock_col1, mock_col2]

        app.render_articles(sample_df)

        mock_subheader.assert_called_once_with("🗞️ Showing 2 Articles")

    @patch("streamlit.subheader")
    @patch("streamlit.container")
    @patch("streamlit.columns")
    @patch("streamlit.markdown")
    def test_render_articles_no_image(
        self, mock_markdown, mock_columns, mock_container, mock_subheader, app
    ):
        """Test article rendering without images"""
        df_no_image = pd.DataFrame(
            [
                {
                    "title": "Test Article",
                    "description": "Test description",
                    "topic": "Test",
                    "url": "https://test.com",
                    "urlToImage": None,
                    "publishedAt": "2024-01-01T12:00:00Z",
                    "author": "Test Author",
                    "source": "Test Source",
                }
            ]
        )

        mock_container.return_value.__enter__ = Mock()
        mock_container.return_value.__exit__ = Mock()
        mock_columns.return_value = [Mock(), Mock()]

        app.render_articles(df_no_image)

        mock_subheader.assert_called_once_with("🗞️ Showing 1 Articles")

    @patch.object(NewsRecommenderApp, "render_sidebar")
    @patch.object(NewsRecommenderApp, "fetch_news")
    @patch.object(NewsRecommenderApp, "render_filters")
    @patch.object(NewsRecommenderApp, "filter_data")
    @patch.object(NewsRecommenderApp, "render_articles")
    @patch("streamlit.cache_data.clear")
    @patch("streamlit.title")
    @patch("streamlit.subheader")
    @patch("streamlit.markdown")
    def test_run_success(
        self,
        mock_markdown,
        mock_subheader,
        mock_title,
        mock_clear,
        mock_render_articles,
        mock_filter_data,
        mock_render_filters,
        mock_fetch_news,
        mock_render_sidebar,
        app,
        sample_df,
    ):
        """Test successful app run"""
        mock_render_sidebar.return_value = (False, "Test-API-Key")
        mock_fetch_news.return_value = sample_df
        mock_render_filters.return_value = ("All", "")
        mock_filter_data.return_value = sample_df

        app.run()

        mock_render_sidebar.assert_called_once()
        mock_fetch_news.assert_called_once()
        mock_render_filters.assert_called_once_with(sample_df)
        mock_filter_data.assert_called_once()
        mock_render_articles.assert_called_once()

    @patch.object(NewsRecommenderApp, "render_sidebar")
    @patch.object(NewsRecommenderApp, "fetch_news")
    @patch("streamlit.cache_data.clear")
    @patch("streamlit.warning")
    @patch("streamlit.stop")
    def test_run_no_data(
        self,
        mock_stop,
        mock_warning,
        mock_clear,
        mock_fetch_news,
        mock_render_sidebar,
        app,
    ):
        """Test app run with no data"""
        mock_render_sidebar.return_value = (False, "Test-API-Key")
        mock_fetch_news.return_value = pd.DataFrame()

        app.run()

        mock_warning.assert_called_once_with("No news articles available.")
        mock_stop.assert_called_once()

    @patch.object(NewsRecommenderApp, "render_sidebar")
    @patch.object(NewsRecommenderApp, "fetch_news")
    @patch("streamlit.cache_data.clear")
    def test_run_with_refresh(
        self, mock_clear, mock_fetch_news, mock_render_sidebar, app, sample_df
    ):
        """Test app run with refresh button clicked"""
        mock_render_sidebar.return_value = (True, "Test-API-Key")
        mock_fetch_news.return_value = sample_df

        with patch.object(app, "render_filters", return_value=("All", "")):
            with patch.object(app, "filter_data", return_value=sample_df):
                with patch.object(app, "render_articles"):
                    with patch("streamlit.title"):
                        with patch("streamlit.subheader"):
                            with patch("streamlit.markdown"):
                                app.run()

        mock_clear.assert_called_once()
