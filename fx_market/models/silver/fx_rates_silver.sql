{{ config(
    materialized='incremental',
    unique_key='fx_rate_id'
) }}

with silver_base as (
    select
        raw_payload:"date"::date              as rate_date,
        raw_payload:"base"::string            as base,
        rates.key::string                     as currency,
        rates.value::float                    as rate,
        md5(
          concat(
            raw_payload:"date"::string,
            raw_payload:"base"::string,
            rates.key::string
          )
        ) as fx_rate_id,
        ingestion_ts
    from {{ source('fx_market', 'fx_rates_raw') }},
         lateral flatten(input => raw_payload:"rates") rates
    {% if is_incremental() %}
        where rate_date > (select max(rate_date) from {{ this }})
    {% endif %}
),

silver_with_lag as (
    select
        *,
        lag(rate) over (partition by currency order by rate_date) as rate_prev
    from silver_base
)

select
    *,
    coalesce((rate - rate_prev) * 100 / rate_prev, 0) as variation_pct
from silver_with_lag
