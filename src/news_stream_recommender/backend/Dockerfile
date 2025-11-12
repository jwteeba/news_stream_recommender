FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install poetry && poetry install --no-root
COPY app ./app
CMD ["uvicorn", "news_stream_recommender.backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]