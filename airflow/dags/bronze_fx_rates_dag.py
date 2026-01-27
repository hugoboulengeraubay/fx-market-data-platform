from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

# permet à Airflow de trouver ton script
sys.path.append("/workspaces/fx-market-data-platform")

from ingestion.fetch_fx_rates import insert_if_not_exists, fetch_rates, build_url

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "JPY", "GBP"]

def run_bronze_ingestion():
    url = build_url(BASE_CURRENCY, TARGET_CURRENCIES)
    data, _ = fetch_rates(url)
    insert_if_not_exists(data)

with DAG(
    dag_id="bronze_fx_rates_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "fx"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_fx_rates_bronze",
        python_callable=run_bronze_ingestion,
    )


    dbt_silver_task = BashOperator(
        task_id="run_dbt_silver",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt run --select fx_rates_silver",
    )

    ingest_bronze >> dbt_silver_task

