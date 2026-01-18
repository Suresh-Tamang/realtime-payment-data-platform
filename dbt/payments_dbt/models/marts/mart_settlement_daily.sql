-- models/marts/mart_settlement_report.sql
{{ config(materialized='table') }}

select
    f.date,
    f.merchant_id,
    f.currency,
    sum(f.amount) as total_amount,
    count(distinct f.transaction_id) as transaction_count,
    -- fee calculation: 2% of total amount
    round(sum(f.amount) * 0.02, 2) as fees
from {{ ref('fact_transactions') }} f
join {{ ref('dim_merchants') }} m
  on f.merchant_id = m.merchant_id
where f.is_fraud = false
group by f.date, f.merchant_id, f.currency
