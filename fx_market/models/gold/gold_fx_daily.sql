{{ config(materialized='table')}}

with rate_rank as(
select
    row_number() over(partition by currency order by rate_date desc, ingestion_ts desc) as row_number,
    rate_date,
    currency,
    rate,
    variation_pct
from 
    {{ref('fx_rates_silver')}}
)
select
    rate_date,
    currency,
    rate,
    variation_pct
from rate_rank
where row_number=1