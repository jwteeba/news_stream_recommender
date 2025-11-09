FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi
COPY app ./app
EXPOSE 8000
CMD ["uvicorn", "news_stream_recommender.backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]