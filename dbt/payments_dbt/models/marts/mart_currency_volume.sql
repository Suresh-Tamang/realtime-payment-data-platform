select
    f.date,
    f.currency,
    c.currency_code,
    sum(f.amount) as total_amount,
    count(distinct f.transaction_id) as transaction_count
from {{ ref('fact_transactions') }} f
join {{ ref('dim_currency') }} c
  on f.currency = c.currency_code
group by f.date, f.currency, c.currency_code
