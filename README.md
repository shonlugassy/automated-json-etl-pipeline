# JSON to DWH ETL Pipeline

Automated ETL pipeline that extracts nested JSON data, transforms it into a structured tabular format, and loads it into Data Warehouse systems (MSSQL and PostgreSQL).

This project demonstrates a real-world data engineering workflow including data extraction from APIs or files, transformation of nested JSON structures, and loading the processed data into relational databases.

---

# Project Overview

The pipeline performs the following steps:

1. **Extract**
   - Fetches JSON data from:
     - REST API
     - Local JSON file

2. **Transform**
   - Normalizes nested JSON structures
   - Detects nested arrays and flattens them
   - Converts list fields into structured columns
   - Adds ETL metadata

3. **Load**
   - Loads processed data into:
     - Microsoft SQL Server
     - PostgreSQL

4. **Logging**
   - Tracks pipeline execution
   - Creates timestamped log files

---

# Architecture

Data Source (API / JSON file)
↓
Extract
↓
Transform (JSON normalization & flattening)
↓
Pandas DataFrame
↓
Load
↓
MSSQL Data Warehouse
PostgreSQL Data Warehouse


---

# Features

- Automated ETL pipeline
- Handles **nested JSON structures**
- Supports **multiple data sources**
- Loads data into **multiple databases**
- Logging and monitoring
- Production-ready structure

---

# Technologies Used

- Python
- Pandas
- SQLAlchemy
- Microsoft SQL Server
- PostgreSQL
- REST APIs
- JSON processing
- Logging

---

# Project Structure
json-to-dwh-etl
│
├── json_to_dwh_etl.py
├── README.md
├── requirements.txt
└── logs/


---

# Example Use Cases

- Loading JSON API data into a Data Warehouse
- Flattening nested JSON for analytics
- Building automated ETL pipelines
- Practicing data engineering workflows

---

# Possible Future Improvements

- Incremental loading
- Docker containerization
- Airflow orchestration
- Data validation
- Monitoring dashboards

---

# Author

Software Engineering graduate focused on **Data Analysis and Data Engineering**, with experience in building ETL pipelines, analyzing business data, and designing data workflows.
