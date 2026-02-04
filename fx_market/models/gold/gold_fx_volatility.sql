{{ config(materialized='table') }}

WITH last_7_days AS (
    SELECT
        currency,
        rate_date,
        rate
    FROM {{ ref('fx_rates_silver') }}
    WHERE rate_date >= (
        SELECT MAX(rate_date) - 7
        FROM {{ ref('fx_rates_silver') }}
    )
),

variation_calc AS (
    SELECT
        currency,
        rate_date,
        (rate - LAG(rate) OVER (PARTITION BY currency ORDER BY rate_date))
        / LAG(rate) OVER (PARTITION BY currency ORDER BY rate_date) * 100
        AS variation_pct
    FROM last_7_days
)

SELECT
    currency,
    STDDEV_SAMP(variation_pct) AS volatility_7d
FROM variation_calc
WHERE variation_pct IS NOT NULL
GROUP BY currency
