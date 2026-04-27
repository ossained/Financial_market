# CapitalEdge Analytics – Automated Financial Market Data Pipeline
CapitalEdge Analytics is a global financial services company providing investment management, market research, and financial advisory services to institutional and retail clients.
To stay competitive in a fast‑moving financial landscape, the company requires accurate, real‑time, and scalable data infrastructure to support analytics and decision‑making.

This project delivers a fully automated, cloud‑based financial data pipeline that ingests, processes, and loads daily market data into an enterprise data warehouse on Azure.

# Project Overview
The goal of this project is to replace CapitalEdge’s manual, error‑prone data collection process with a modern, automated, scalable ETL pipeline.

The system:

* Extracts financial market data from external APIs

* Processes and transforms the data using Python

* Orchestrates workflows using Dockerized Apache Airflow

* Stores raw and curated data in Azure Data Lake

* Loads structured data into Azure SQL Database

* Uses Azure Data Factory (ADF) to move data into enterprise staging and EDW layers

* Supports daily incremental updates for analytics

# Problem Statement

## Operational Issues
* Manual API extraction leading to delays and human errors

* No automated staging or transformation

* Bulk loading causing redundant data and high storage costs

* On‑premise systems unable to scale with growing data volumes

* Inconsistent datasets across departments


## Business Impact
* Slower investment decision‑making

* Outdated or incomplete market insights

* Reduced operational efficiency

* Higher risk of analytical errors

# Project Objectives
## This project aims to:

* Automate data collection from financial APIs

* Enable incremental data loading to reduce redundancy

* Improve data accuracy and timeliness

* Ensure data consistency across the organization

* Scale effortlessly as data volume and client base grow

* Enhance operational efficiency by reducing manual work

# Data Sources
Financial Market Data (e.g., stock prices, company overview)

* APIs Used: Alpha Vantage

* Formats: JSON,  Parquet

* Frequency: Daily ingestion

# Architecture Overview

## End‑to‑End Pipeline Flow
## 🏗️ Architecture Overview

                +----------------------+
                |    Financial APIs    |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |  Python ETL (Docker) |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Airflow (Docker)     |
                |  Orchestration (DAG) |
                +----------+-----------+
                           |
                           v
        +-------------------------------------------+
        |              Azure Data Lake              |
        |   Raw Zone  --->  Curated Zone (Parquet) |
        +-------------------+-----------------------+
                           |
                           v
                +----------------------+
                |   ADF Pipelines     |
                |    (Daily Loads)    |
                +----------+-----------+
                           |
                           v
        +-------------------------------------------+
        |          Enterprise Data Warehouse        |
        |   dim_date | dim_exchange | fact_prices   |
        +-------------------------------------------+

# Tech Stack


| **Layer**          | **Technology**              |
|--------------------|-----------------------------|
| Orchestration      | Apache Airflow (Docker)     |
| Compute            | Python                      |
| Cloud Storage      | Azure Data Lake Gen2        |
| Datawarehouse      | Azure data studio          |
| Data Movement      | Azure Data Factory          |
| Containerization   | Docker                      |
| Version Control    | GitHub                      |

# Pipeline Workflow
## 1. API Data Extraction
Python scripts fetch daily stock price and company overview data from financial APIs.

## 2. Data Staging in Azure Data Lake
Raw JSON is stored in the Raw Zone.
Transformed Parquet files are stored in the Curated Zone.

## 3. Data Transformation
Python performs:

* Cleaning

* Feature engineering

* Price direction calculation

* Incremental filtering

## 4. Airflow Orchestration
Airflow (running in Docker) schedules and manages:

* Daily ETL runs

* Error handling

* Logging

* Retries

## 5. ADF Enterprise Loading
ADF pipelines move data from staging into:

* dim_date

* dim_exchange

* fact_prices

with incremental logic.

# Key Features
* Fully automated daily ingestion

* Incremental loading to reduce storage

* Cloud native, scalable architecture

* Modular, maintainable Python ETL

* Airflow DAGs for orchestration

* Azure SQL for structured analytics

* ADF for enterprise‑grade data movement

#  Business Impact
* Faster, more accurate investment insights

* Reduced manual workload

* Consistent datasets across teams

* Scalable infrastructure for future growth

* Improved decision‑making speed
