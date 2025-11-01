<h1 align="center">💹 FinPulse-ETL</h1>

<p align="center">
  <em>End-to-End Automated Financial Data Engineering Pipeline using Apache Airflow, PySpark & MySQL</em><br><br>
  <img src="https://raw.githubusercontent.com/Elbehery/GIFs-For-README/main/airflow-spark.gif" width="720" alt="Airflow Spark ETL Animation">
</p>

<p align="center">
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white"></a>
  <a href="https://airflow.apache.org/"><img src="https://img.shields.io/badge/Apache%20Airflow-2.9.3-blue?logo=apacheairflow&logoColor=white"></a>
  <a href="https://spark.apache.org/"><img src="https://img.shields.io/badge/Apache%20Spark-3.5.2-orange?logo=apachespark&logoColor=white"></a>
  <a href="https://www.mysql.com/"><img src="https://img.shields.io/badge/MySQL-8.0-lightblue?logo=mysql&logoColor=white"></a>
</p>

---

## 🌍 Overview

**FinPulse-ETL** is a **production-grade financial data pipeline** that automates extraction, transformation, and loading of real-time market data from the **Yahoo Finance API**.

It is a complete data-engineering system built on:

- 🌀 **Apache Airflow** – task orchestration & scheduling  
- ⚡ **Apache Spark (PySpark)** – distributed transformation engine  
- 🐬 **MySQL** – persistent analytical warehouse  
- 🧪 **Pytest** – data-quality validation  
- 🐳 **Docker** – containerized, reproducible deployment  

---

## 🧭 Architecture Diagram

<p align="center">
  <img src="https://raw.githubusercontent.com/Elbehery/GIFs-For-README/main/data-pipeline.gif" width="780" alt="ETL Workflow Animation">
</p>

            ┌────────────────────────────┐
            │   Yahoo Finance API        │
            └────────────┬───────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │ Airflow DAG  (Extract)     │
            │  - Fetch & schedule data   │
            └────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │ PySpark Engine (Transform) │
            │  - Clean, compute returns  │
            │  - Moving averages, joins  │
            └────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │ MySQL Warehouse (Load)     │
            │  - Store curated dataset   │
            └────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │  Jupyter / BI Tools        │
            │  - Visualization & ML      │
            └────────────────────────────┘

---

## ⚙️ Technology Stack

| Layer | Technology | Purpose |
|-------|-------------|----------|
| 🧩 Orchestration | **Apache Airflow 2.9.3** | DAG scheduling, monitoring |
| ⚡ Processing | **Apache Spark 3.5.2 (PySpark)** | Distributed data transformation |
| 🐬 Storage | **MySQL 8.0** | Analytical warehouse |
| 💹 Data Source | **Yahoo Finance API (yfinance)** | Market data ingestion |
| 🧪 Testing | **Pytest 8.3** | Unit & integration tests |
| 📊 Validation | **Jupyter Notebook** | Exploratory data analysis |
| 🐳 Infrastructure | **Docker + Compose** | Reproducible environment |

---
