select distinct
    date(event_ts) as date_id,
    extract(year from event_ts) as year,
    extract(month from event_ts) as month,
    extract(day from event_ts) as day
from {{ ref('stg_fact_transactions') }}