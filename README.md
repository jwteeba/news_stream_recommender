# 📰 News Stream Recommender (Topic Clustering)

> Real-Time NLP Data Pipeline using **Docker**, **FastAPI**, **Kafka**, **Spark**, **MongoDB**, **Streamlit**, and **Poetry**

---

[![Docker](https://img.shields.io/badge/Docker-Ready-blue?logo=docker)]()
[![FastAPI](https://img.shields.io/badge/FastAPI-API-green?logo=fastapi)]()
[![Apache Spark](https://img.shields.io/badge/Spark-Streaming-orange?logo=apache-spark)]()
[![MongoDB](https://img.shields.io/badge/MongoDB-Database-brightgreen?logo=mongodb)]()
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)]()
[![Poetry](https://img.shields.io/badge/Poetry-Dependency%20Mgmt-blueviolet?logo=python)]()

---

## 🧠 Overview

The **News Stream Recommender** continuously ingests breaking news from live sources (NewsAPI), applies NLP topic modeling in real time using **Apache Spark**, stores results in **MongoDB**, and visualizes insights via a **FastAPI** + **Streamlit** frontend.

---

## 🚀 Features

- 🌍 **Real-Time Ingestion** – fetches live headlines via Kafka producers  
- 🧩 **Streaming NLP Pipeline** – tokenization, TF-IDF, LDA topic modeling  
- 💾 **Data Persistence** – stores clustered topics in MongoDB  
- ⚡ **RESTful API** – built with FastAPI for frontend data access  
- 📊 **Interactive Dashboard** – live topic view via Streamlit  
- 🐳 **Fully Containerized** – built and orchestrated using Docker Compose  
- 📦 **Poetry-Managed** – clean and reproducible Python environments  

---

## 🏗️ Architecture

![Design](images/news_feed_data_pipeline.png)

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|-------------|
| Ingestion | **Kafka**, **NewsAPI**, **RSS Feeds** |
| Processing | **Apache Spark** (Structured Streaming, TF-IDF, LDA) |
| Storage | **MongoDB** |
| Backend | **FastAPI** |
| Frontend | **Streamlit** |
| Management | **Docker Compose**, **Poetry** |
