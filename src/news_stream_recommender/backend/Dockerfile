FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install poetry fastapi uvicorn openai pymongo python-dotenv
COPY app/ /app/
ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["uvicorn", "news_stream_recommender.backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]


    
