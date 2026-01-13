Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

## Payments dbt Models

### Layers
- **Staging**: Clean, typed views from raw warehouse tables
- **Marts**:
  - `fact_transactions`: all transactions
  - `fact_fraud_signals`: only flagged fraud
  - `fact_settlement_daily`: merchant settlements

### Fraud Handling
Fraud records are not removed.
They are flagged at ingestion and modeled separately for analytics and reporting.
