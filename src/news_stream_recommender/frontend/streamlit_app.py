import os
import requests
import streamlit as st
import pandas as pd
import logging
import tempfile
from datetime import datetime
from dotenv import load_dotenv


log_file = os.path.join(tempfile.gettempdir(), "news_stream.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)],
)


class NewsRecommenderApp:
    """Streamlit application for displaying news recommendations with topic filtering.

    This class provides a web interface for browsing news articles that have been
    processed and categorized by topic using machine learning.
    """

    def __init__(self):
        """Initialize the NewsRecommenderApp with configuration and setup."""
        self.fastapi_url = os.getenv("FASTAPI_URL")
        self.openai_api_key = ""
        print(self.fastapi_url)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_page()

    def setup_page(self):
        """Configure Streamlit page settings and layout."""
        st.set_page_config(page_title="📰 News Recommender Dashboard", layout="wide")

    @staticmethod
    def format_date(iso_date):
        """Convert ISO date string to human-readable format.

        Args:
            iso_date (str): ISO 8601 formatted date string

        Returns:
            str: Formatted date string (e.g., "November 07, 2025") or original string if parsing fails
        """
        if iso_date is None:
            return None
        try:
            return datetime.fromisoformat(iso_date.replace("Z", "+00:00")).strftime(
                "%B %d, %Y"
            )
        except (ValueError, TypeError, AttributeError):
            return iso_date

    @st.cache_data(ttl=300)
    def fetch_news(_self):
        """Fetch news data from FastAPI backend with caching.

        Returns:
            pd.DataFrame: DataFrame containing news articles with topics, or empty DataFrame on error
        """
        try:
            res = requests.get(f"{_self.fastapi_url}/trending", timeout=10)
            res.raise_for_status()
            return pd.DataFrame(res.json()["topics"])
        except Exception as e:
            _self.logger.error(f"Failed to fetch data: {e}")
            st.error(f"❌ Failed to fetch data: {e}")
            return pd.DataFrame()

    def render_sidebar(self):
        """Render sidebar controls for the application.

        Returns:
            bool: True if refresh button was clicked, False otherwise
        """
        st.sidebar.title("🧭 Controls")
        openai_api_key = st.sidebar.text_input("🔑 Enter your OpenAI API key:", type="password" )
        refresh = st.sidebar.button("🔄 Refresh Data")
        st.sidebar.markdown("---")
        st.sidebar.write("💡 Use filters or search to explore news topics")
        return refresh, openai_api_key

    def render_filters(self, df):
        """Render topic selection and keyword search filters.

        Args:
            df (pd.DataFrame): News articles DataFrame

        Returns:
            tuple: (selected_topic, keyword) - user's filter selections
        """
        col1, col2 = st.columns([2, 2])
        with col1:
            if "topic" in df.columns:
                topic_options = sorted(df["topic"].dropna().unique())
                selected_topic = st.selectbox(
                    "🧠 Select Topic", ["All"] + topic_options
                )
            else:
                st.info("Topics will appear once Spark processes the data")
                selected_topic = "All"
        with col2:
            keyword = st.text_input("🔍 Search by keyword")
        return selected_topic, keyword

    def filter_data(self, df, selected_topic, keyword):
        """Filter news data based on topic and keyword selections.

        Args:
            df (pd.DataFrame): Original news articles DataFrame
            selected_topic (str): Selected topic filter or "All"
            keyword (str): Keyword to search in title and description

        Returns:
            pd.DataFrame: Filtered DataFrame based on user selections
        """
        filtered_df = df.copy()
        if selected_topic != "All" and "topic" in df.columns:
            filtered_df = filtered_df[filtered_df["topic"] == selected_topic]
        if keyword:
            filtered_df = filtered_df[
                filtered_df["description"].str.contains(keyword, case=False, na=False)
                | filtered_df["title"].str.contains(keyword, case=False, na=False)
            ]
        return filtered_df

    def render_articles(self, filtered_df):
        """Render news articles in a card-based layout with images.

        Args:
            filtered_df (pd.DataFrame): Filtered news articles to display
        """
        st.subheader(f"🗞️ Showing {len(filtered_df)} Articles")
        for _, row in filtered_df.iterrows():
            with st.container():
                col1, col2 = st.columns([1, 3])

                with col1:
                    if row.get("urlToImage"):
                        try:
                            st.image(
                                row.get("urlToImage"),
                                width=200,
                                use_container_width=True,
                            )
                        except Exception:
                            st.markdown("📷 *Image unavailable*")
                    else:
                        st.markdown("📷 *No image*")

                with col2:
                    st.markdown(
                        f"#### 📰 [{row.get('title', 'Untitled')}]({row.get('url', '#')})"
                    )
                    st.markdown(
                        f"**Topic:** {row.get('topic', 'N/A')} | **Published:** {self.format_date(row.get('publishedAt', 'N/A'))} | **Author:** {row.get('author', 'N/A')} | **Source:** {row.get('source', 'N/A')}"
                    )
                    st.markdown(
                        f"{row.get('description', '')[:400]}{'...' if len(row.get('description', '')) > 400 else ''}"
                    )

                st.markdown("---")

    def run(self):
        """Main application execution flow.

        Orchestrates the entire application by rendering components,
        fetching data, applying filters, and displaying results.
        """
        refresh, openai_api_key = self.render_sidebar()

        if refresh:
            st.cache_data.clear()

        self.openai_api_key = openai_api_key

        df = self.fetch_news()

        if df.empty:
            st.warning("No news articles available.")
            st.stop()

        st.title("📰 News Stream Recommender")
        st.subheader("Live Trending Topics")

        selected_topic, keyword = self.render_filters(df)
        filtered_df = self.filter_data(df, selected_topic, keyword)
        self.render_articles(filtered_df)

        st.markdown(
            "<center>Built with ❤️ using Streamlit & FastAPI by Jah-Wilson Teeba</center>",
            unsafe_allow_html=True,
        )


def main():
    """Entry point for the Streamlit application.

    Creates and runs the NewsRecommenderApp instance.
    """
    load_dotenv()
    app = NewsRecommenderApp()
    app.run()


if __name__ == "__main__":
    main()
