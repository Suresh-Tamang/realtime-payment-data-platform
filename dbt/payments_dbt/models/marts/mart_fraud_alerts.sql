-- models/marts/mart_fraud_alerts.sql
{{ config(materialized='table') }}

select
    f.date,
    f.transaction_id,
    f.merchant_id,
    f.amount,
    f.currency,
    f.is_fraud,
    f.fraud_reason,
    f.rule_high_amount,
    f.rule_blacklist
from {{ ref('fact_transactions') }} f
join {{ ref('dim_merchants') }} m
  on f.merchant_id = m.merchant_id
where f.is_fraud = true
