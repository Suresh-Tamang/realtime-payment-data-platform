-- models/marts/mart_settlement_report.sql
{{ config(materialized='table') }}

with base as (
    select
        f.date,
        f.merchant_id,
        f.currency,
        sum(f.amount) as total_amount,
        count(distinct f.transaction_id) as transaction_count,
        round(sum(f.amount) * 0.02, 2) as fees
    from {{ ref('fact_transactions') }} f
    join {{ ref('dim_merchants') }} m
      on f.merchant_id = m.merchant_id
    where f.is_fraud = false and f.auth_result= 'APPROVED'
    group by f.date, f.merchant_id, f.currency
)

select
    date,
    merchant_id,
    currency,
    total_amount,
    transaction_count,
    fees,
    round(total_amount - fees, 2) as total_settlement
from base
