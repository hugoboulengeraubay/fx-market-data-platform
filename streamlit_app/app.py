import streamlit as st
import pandas as pd
from snowflake_connection import get_connection
import altair as alt

st.set_page_config(page_title="FX Dashboard", layout="centered")

st.title("FX Dashboard")

# --- Sélecteur de devise
currency = st.selectbox(
    "Choisir une devise",
    ["USD", "GBP", "JPY"]
)

conn = get_connection()
print("connexion ok")

# --- Dernier taux + variation
daily_query = f"""
SELECT rate_date, rate, variation_pct
FROM GOLD_FX_DAILY
WHERE currency = '{currency}'
ORDER BY rate_date DESC
LIMIT 1
"""
daily = pd.read_sql(daily_query, conn)
print("-----------------")
print(daily)
print(daily["RATE"])
# --- Min / Max 30j
minmax_query = f"""
SELECT min_rate, max_rate
FROM GOLD_FX_MIN_MAX
WHERE currency = '{currency}'
"""
minmax = pd.read_sql(minmax_query, conn)

# --- Volatilité 7j
vol_query = f"""
SELECT volatility_7d
FROM GOLD_FX_VOLATILITY
WHERE currency = '{currency}'
"""
vol = pd.read_sql(vol_query, conn)

# --- Affichage
st.subheader(f"Currency : {currency}")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Dernier taux",
    round(daily.loc[0, "RATE"], 4)
)

col2.metric(
    "Variation (%)",
    f"{round(daily.loc[0, 'VARIATION_PCT'], 2)} %"
)

col3.metric(
    "Min 30j",
    round(minmax.loc[0, "MIN_RATE"], 4)
)

col4.metric(
    "Max 30j",
    round(minmax.loc[0, "MAX_RATE"], 4)
)

st.metric(
    "Volatilité 7 jours",
    round(vol.loc[0, "VOLATILITY_7D"], 2)
)

st.caption(f"Dernière mise à jour : {daily.loc[0, 'RATE_DATE']}")

ts_query = f"""
SELECT rate_date, rate
FROM GOLD_TIMESERIES_30D
WHERE currency = '{currency}'
ORDER BY rate_date
"""

ts = pd.read_sql(ts_query, conn) 

min_rate = ts["RATE"].min()
max_rate = ts["RATE"].max()

chart = (
    alt.Chart(ts)
    .mark_line()
    .encode(
        x="RATE_DATE:T",
        y=alt.Y(
            "RATE:Q",
            scale=alt.Scale(
                domain=[min_rate * 0.995, max_rate * 1.005]
            )
        ),
        tooltip=["RATE_DATE:T", "RATE:Q"]
    )
)

st.altair_chart(chart, use_container_width=True)


