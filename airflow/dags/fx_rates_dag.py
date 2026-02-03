from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

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
    dag_id="fx_rates_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "fx"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_fx_rates_bronze",
        python_callable=run_bronze_ingestion,
    )


    silver_task = BashOperator(
        task_id="run_silver",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt run --select fx_rates_silver",
    )

    silver_dbt_tests_task = BashOperator(
        task_id="run_dbt_tests_silver",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select fx_rates_silver",
    )

    silver_test_positive_task = BashOperator(
        task_id="run_test_positive_silver",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select rate_not_negative",
    )

    silver_test_freshness_task = BashOperator(
        task_id="run_test_freshness_silver",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select rates_freshness",
    )

    silver_tests_done = EmptyOperator(task_id="silver_tests_done")


    gold_daily_task = BashOperator(
        task_id="run_gold_daily",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt run --select gold_fx_daily",
    )

    gold_min_max_task = BashOperator(
        task_id="run_gold_min_max",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt run --select gold_fx_min_max",
    )

    gold_volatility_task = BashOperator(
        task_id="run_gold_volatility",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt run --select gold_fx_volatility",
    )

    gold_test_daily_task = BashOperator(
        task_id="run_test_daily_gold",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select gold_fx_daily",
    )

    gold_test_min_max_task = BashOperator(
        task_id="run_test_min_max_gold",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select gold_fx_min_max",
    )

    gold_test_volatility_task = BashOperator(
        task_id="run_test_volatility_gold",
        bash_command="cd /workspaces/fx-market-data-platform/fx_market && dbt test --select gold_fx_volatility",
    )
    

    ingest_bronze >> silver_task
    silver_task >> [silver_dbt_tests_task, silver_test_freshness_task, silver_test_positive_task]
    [silver_dbt_tests_task, silver_test_positive_task] >> silver_tests_done
    silver_tests_done >> [gold_daily_task, gold_min_max_task, gold_volatility_task]
    gold_daily_task >> gold_test_daily_task
    gold_min_max_task >> gold_test_min_max_task
    gold_volatility_task >> gold_test_volatility_task
