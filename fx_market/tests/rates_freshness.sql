select 
    max(rate_date) as max_date
from 
    {{ref('fx_rates_silver')}}
having max(rate_date) < CURRENT_DATE - 3