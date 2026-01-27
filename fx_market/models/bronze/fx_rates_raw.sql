{{ config(materialized='view') }}

select
    raw_payload,
    ingestion_ts,
    source
from {{ source('fx_market', 'fx_rates_raw') }}
