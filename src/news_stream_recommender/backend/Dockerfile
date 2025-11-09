FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install poetry && poetry install --no-root
RUN pip install --upgrade pip
RUN pip install streamlit fastapi uvicorn pymongo python-dotenv
COPY app ./app
