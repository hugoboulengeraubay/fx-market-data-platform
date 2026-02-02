{{ config(materialized='table', schema='GOLD')}}

with rate_rank as(
select
    row_number() over(partition by currency order by rate_date desc, ingestion_ts desc) as row_number,
    rate_date,
    currency,
    rate
from 
    {{ref('fx_rates_silver')}}
)
select
    rate_date,
    currency,
    rate
from rate_rank
where row_number=1