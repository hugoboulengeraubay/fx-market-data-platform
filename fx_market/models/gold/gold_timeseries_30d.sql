{{config(materialized='table')}}

select
    rate_date,
    currency,
    rate
from 
    {{ref('fx_rates_silver')}}
where rate_date > CURRENT_DATE - 30