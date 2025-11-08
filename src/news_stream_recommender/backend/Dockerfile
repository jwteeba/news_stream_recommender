FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install poetry && poetry install --no-root
COPY app ./app
