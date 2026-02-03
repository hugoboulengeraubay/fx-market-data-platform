{{config(materialized='table')}}

WITH last_7_days AS (
    SELECT *
    FROM {{ref('fx_rates_silver')}}
    WHERE rate_date >= (SELECT MAX(rate_date) - 6 FROM {{ref('fx_rates_silver')}})
)
SELECT
    currency,
    STDDEV_SAMP(variation_pct) AS volatility_7d
FROM last_7_days
GROUP BY currency