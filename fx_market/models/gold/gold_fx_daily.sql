{{ config(materialized='table') }}

with base as (
    select
        rate_date,
        currency,
        rate,
        lag(rate) over (
            partition by currency
            order by rate_date
        ) as rate_prev
    from {{ ref('fx_rates_silver') }}
),

with_variation as (
    select
        rate_date,
        currency,
        rate,
        (rate - rate_prev) * 100 / rate_prev as variation_pct
    from base
),

ranked as (
    select
        *,
        row_number() over (
            partition by currency
            order by rate_date desc
        ) as rn
    from with_variation
)

select
    rate_date,
    currency,
    rate,
    variation_pct
from ranked
where rn = 1
