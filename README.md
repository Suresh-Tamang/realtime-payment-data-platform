# Realtime Payment Data Platform

A comprehensive data engineering platform for processing real-time payment transactions, featuring fraud detection, settlement reporting, and a medallion architecture (Bronze → Silver → Gold) using Apache Spark, Apache Airflow, DBT, and Docker.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Monitoring and Logging](#monitoring-and-logging)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project implements a real-time payment data processing platform designed to handle high-volume payment transactions. It includes:

- Real-time data ingestion from payment producers
- Fraud detection using streaming analytics
- Automated settlement processing and reporting
- Data warehouse integration with DBT transformations
- Containerized deployment using Docker

The platform follows modern data engineering best practices with a medallion architecture, ensuring data quality and reliability at each layer.

## Architecture

```
Payment Producer → Kafka/Streaming → Bronze Layer (Raw Data)
                      ↓
              Silver Layer (Cleaned & Enriched)
                      ↓
                 Gold Layer (Aggregated & Analytics)
                      ↓
            DBT Models → Reports & Dashboards
```

### Components

- **Apache Airflow**: Orchestrates ETL pipelines and scheduled jobs
- **Apache Spark**: Handles batch and streaming data processing
- **DBT**: Data transformation and modeling
- **Docker**: Containerization for easy deployment
- **PostgreSQL/MySQL**: Metadata and result storage as data warehouse

## Features

- **Real-time Processing**: Streaming analytics for fraud detection
- **Batch Processing**: Hourly and daily settlement reports
- **Fraud Detection**: Rule-based fraud alerts
- **Data Quality**: Validation and cleansing at each layer
- **Scalable Architecture**: Containerized deployment
- **Monitoring**: Comprehensive logging and alerting
- **CI/CD Ready**: Docker-based deployment pipeline

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Java 8+ (for Spark)
- Git

## Setup and Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Suresh-Tamang/realtime-payment-data-platform.git
   cd realtime-payment-data-platform
   ```

2. **Start the platform:**
   ```bash
   docker-compose up -d --build (one time)
   docker-compose up -d
   ```

   This will start all services including Airflow, Spark, DBT, and databases.

3. **Access the services:**
   - Airflow UI: http://localhost:8088
   - Spark Master: http://localhost:8080
   - Spark Worker: http://localhost:8081

4. **Initialize DBT:**
   ```bash
   docker exec -it dbt
   dbt deps
   dbt run
   ```

## Usage

### Running Pipelines

1. **Trigger Airflow DAGs:**
   - Access Airflow UI at http://localhost:8088
   - Enable and trigger desired DAGs (e.g., `warehouse_pipeline`, `settlement_dag`)

2. **Payment Producer:**
   ```bash
   python producer/payment_producer.py
   ```

3. **Manual Spark Jobs:**
   ```bash
   docker exec spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --deploy-mode client \
     /opt/spark-apps/streaming/process_streaming_data.py
   ```

### Data Flow

1. **Bronze Layer**: Raw payment data ingestion
2. **Silver Layer**: Data cleansing, deduplication, and enrichment
3. **Gold Layer**: Aggregated data for analytics and reporting
4. **Reports**: Automated generation of settlement and fraud reports

## Data Pipeline

### Streaming Pipeline
- Real-time payment processing
- Fraud detection rules application
- Alert generation

### Batch Pipeline
- Hourly data aggregation
- Daily settlement calculations
- Report generation

### DBT Models
- Dimensional modeling
- Business logic implementation
- Data quality tests

## Monitoring and Logging

- **Airflow Logs**: Available in `airflow/logs/`
- **Spark Logs**: Accessible via Spark UI
- **Application Logs**: Check Docker container logs
- **DBT Logs**: Located in `dbt/payments_dbt/logs/`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

Please refer to the [runbook](docs/runbook.md) for detailed operational procedures.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- Check the [runbook](docs/runbook.md)
- Review [fraud rules](docs/fraud_rules.md)
- Review [system flow diagram](docs/HighLevelArchitecture.png)
- Examine system flow diagram: `docs/HighLevelArchitecture.png`

---
