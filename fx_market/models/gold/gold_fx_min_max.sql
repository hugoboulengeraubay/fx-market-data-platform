{{config(materialized='table')}}

with rate_last_4w as(
select
    rate,
    currency,
    row_number() over(partition by currency order by rate_date desc) as row_number
from
    {{ref('fx_rates_silver')}}
)
select
    currency,
    max(rate) as max_rate,
    min(rate) as min_rate
from rate_last_4w
where row_number <= 20
group by currency