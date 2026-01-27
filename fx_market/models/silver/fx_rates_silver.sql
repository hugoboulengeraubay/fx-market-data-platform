{{ config(materialized='table') }}

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
