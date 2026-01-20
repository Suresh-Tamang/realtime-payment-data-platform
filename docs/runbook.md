# Realtime Payment Data Platform - Runbook

## Table of Contents

- [Introduction](#introduction)
- [System Overview](#system-overview)
- [Deployment and Startup](#deployment-and-startup)
- [Daily Operations](#daily-operations)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Troubleshooting](#troubleshooting)
- [Maintenance Procedures](#maintenance-procedures)
- [Emergency Response](#emergency-response)
- [Contacts](#contacts)

## Introduction

This runbook provides operational procedures for the Realtime Payment Data Platform. It covers deployment, monitoring, troubleshooting, and maintenance of the data processing pipeline that handles real-time payment transactions, fraud detection, and settlement reporting.

**Last Updated**: January 20, 2026  
**Version**: 1.0  
**Primary Contact**: Data Engineering Team

## System Overview

### Architecture Components

- **Apache Airflow**: Workflow orchestration and scheduling
- **Apache Spark**: Distributed data processing (batch and streaming)
- **DBT**: Data transformation and modeling
- **Docker Compose**: Container orchestration
- **PostgreSQL/MySQL**: Metadata and result storage
- **Kafka/Streaming**: Real-time data ingestion (assumed)
- **Payment Producer**: Synthetic data generation

### Data Flow

1. **Ingestion**: Payment producer generates transaction data
2. **Bronze Layer**: Raw data storage in Spark
3. **Silver Layer**: Data cleansing and enrichment
4. **Gold Layer**: Aggregated analytics and reporting
5. **DBT**: Business logic and dimensional modeling

### Key Services

- Airflow Web UI: http://localhost:80888
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081

## Deployment and Startup

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 8GB+ RAM available
- 10GB+ disk space
- Ports 8080, 4040,8081, 8088 available

### Initial Deployment

1. **Clone Repository**
   ```bash
   git clone https://github.com/Suresh-Tamang/realtime-payment-data-platform.git
   cd realtime-payment-data-platform
   ```

2. **Start Services**
   ```bash
   docker-compose up -d
   ```

3. **Verify Services**
   ```bash
   docker-compose ps
   # Check logs
   docker-compose logs airflow-webserver
   docker-compose logs spark-master
   ```

5. **Initialize DBT**
   ```bash
   docker exec -it dbt /bin/bash
   dbt deps
   dbt run
   ```

### Service Startup Order

1. Warehouse (PostgreSQL)
2. Kafka
3. Payment producer (as needed)
4. Spark cluster
5. Airflow
4. DBT container

## Daily Operations

### Morning Checklist

1. **Check Service Health**
   ```bash
   docker-compose ps
   # Ensure all containers are 'Up'
   ```

2. **Review Airflow DAGs**
   - Access Airflow UI
   - Check DAG states (should be 'success' or 'running')
   - Review recent task failures

3. **Monitor Data Ingestion**
   - Check payment producer logs
   - Verify data landing in bronze layer
   - Review Spark streaming job status

4. **Check DBT Models**
   ```bash
   docker exec dbt dbt test
   ```

### Scheduled Tasks

- **Hourly**: Medallion pipeline runs
- **Daily**: Settlement reports at 00:00 UTC
- **Weekly**: Full data quality audits

### Manual Operations

#### Starting Payment Producer
```bash
python ./producer/payment_producer.py
```

#### Running Spark Jobs Manually to process realtime data from payment_producer
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/streaming/process_streaming_data.py
```

#### Triggering Airflow DAGs
- Via Airflow UI: DAGs → Select DAG → Trigger DAG
- Via CLI: `docker exec airflow-webserver airflow dags trigger <dag_id>`

## Monitoring and Alerting

### Key Metrics to Monitor

- **Airflow**: DAG success rates, task duration, scheduler heartbeat
- **Spark**: Job completion status, executor memory usage, streaming lag
- **Data Quality**: Row counts, null percentages, duplicate detection
- **System**: CPU usage, memory usage, disk space

### Log Locations

- **Airflow**: `airflow/logs/`
- **Spark**: Container logs + Spark UI
- **DBT**: `dbt/payments_dbt/logs/`
- **Application**: `docker-compose logs <service>`

### Alert Conditions

- DAG failures > 3 consecutive runs
- Spark job failures
- Data pipeline lag > 1 hour
- Disk usage > 80%
- Memory usage > 90%

### Monitoring Commands

```bash
# Check container resource usage
docker stats

# View specific service logs
docker-compose logs --tail=100 airflow-scheduler

# Check Airflow health
curl http://localhost:8080/health


## Troubleshooting

### Common Issues

#### Airflow DAGs Not Running

**Symptoms**: DAGs stuck in 'queued' state  
**Cause**: Scheduler not running or resource constraints  
**Solution**:
```bash
docker-compose restart airflow-scheduler
# Check scheduler logs
docker-compose logs airflow-scheduler
```

#### Spark Job Failures

**Symptoms**: Streaming job exits with error  
**Cause**: Memory issues, network problems, or code errors  
**Solution**:
1. Check Spark UI for error details
2. Review application logs
3. Increase executor memory if needed
4. Restart Spark cluster

#### Data Pipeline Lag

**Symptoms**: Bronze layer not receiving data  
**Cause**: Payment producer down or network issues  
**Solution**:
1. Restart payment producer
2. Check network connectivity
3. Verify Kafka/Streaming service health

#### DBT Model Failures

**Symptoms**: dbt run fails with compilation errors  
**Cause**: Schema changes or syntax errors  
**Solution**:
```bash
docker exec dbt dbt compile
# Fix identified issues
docker exec dbt dbt run
```

#### Container Resource Issues

**Symptoms**: Containers restarting or OOM errors  
**Cause**: Insufficient host resources  
**Solution**:
1. Increase Docker memory limits
2. Scale down concurrent operations
3. Optimize Spark configurations

### Diagnostic Commands

```bash
# Check all service health
docker-compose ps

# View recent logs
docker-compose logs --since 1h

# Check disk usage
df -h

# Monitor network
docker network ls
docker network inspect realtime-payment-data-platform_default

# Test database connectivity
docker exec postgres psql -U airflow -d airflow -c "SELECT 1;"
```

## Maintenance Procedures

### Weekly Maintenance

1. **Log Rotation**
   ```bash
   # Rotate Airflow logs
   docker exec airflow-webserver airflow db clean --clean-before-timestamp 2026-01-13T00:00:00+00:00
   ```

2. **Data Cleanup**
   - Archive old bronze layer data (>30 days)
   - Clean temporary Spark files

3. **Update Dependencies**
   ```bash
   # Update DBT packages
   docker exec dbt dbt deps
   ```

### Monthly Maintenance

1. **Full System Backup**
   ```bash
   # Backup databases
   docker exec postgres-container pg_dump -U airflow airflow > backup.sql
   ```

2. **Performance Tuning**
   - Review and optimize Spark configurations
   - Analyze Airflow DAG performance

3. **Security Updates**
   - Update Docker images
   - Review and rotate credentials

### Quarterly Maintenance

1. **Major Version Updates**
   - Update Airflow, Spark, DBT versions
   - Test compatibility with new versions

2. **Architecture Review**
   - Assess scalability needs
   - Plan infrastructure upgrades

## Emergency Response

### Critical Incident Response

1. **Stop Data Ingestion**
   ```bash
   # Stop payment producer
   pkill -f payment_producer.py
   ```

2. **Pause Airflow DAGs**
   - Airflow UI: DAGs → Select DAG → Pause

3. **Isolate Affected Components**
   ```bash
   # Restart specific services
   docker-compose restart <affected-service>
   ```

4. **Data Recovery**
   - Restore from backups if needed
   - Reprocess failed batches

### Escalation Procedures

- **Level 1**: On-call engineer investigates
- **Level 2**: Data engineering team lead (if issue persists >1 hour)
- **Level 3**: Senior management (if business impact >4 hours)

### Communication

- Internal Slack channel: #data-platform-alerts
- Email distribution: data-eng-team@company.com
- Incident tracking: JIRA project DATAENG

## Contacts

### Primary Contacts

- **Data Engineering Lead**: [Name] - [Email] - [Phone]
- **DevOps Engineer**: [Name] - [Email] - [Phone]
- **On-call Rotation**: data-eng-oncall@company.com

### External Support

- **Docker Support**: https://docs.docker.com/
- **Apache Airflow**: https://airflow.apache.org/docs/
- **Apache Spark**: https://spark.apache.org/docs/
- **DBT**: https://docs.getdbt.com/

### Emergency Contacts

- **Infrastructure Team**: infra-support@company.com
- **Security Team**: security@company.com
- **Executive On-call**: ceo@company.com

---

**Note**: This runbook should be reviewed and updated quarterly or after significant system changes. All procedures should be tested in a staging environment before production deployment.