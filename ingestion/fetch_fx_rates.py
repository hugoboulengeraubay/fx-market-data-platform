import requests
import json
import datetime
import time
import os
import hashlib
import pandas as pd

from ingestion.snowflake_client import get_connection

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "JPY", "GBP"]

def build_url(base, target):
    target = ",".join(target)
    return f"https://api.frankfurter.app/latest?from={base}&to={target}"

def fetch_rates(url):
    response = requests.get(url)
    return response.json(), response.status_code

def save_raw_json(data):
    output_dir = "data/raw"
    today_date = datetime.date.today()
    file_name = f"fx_rates_{today_date}.json"
    file_path = os.path.join(output_dir, file_name)

    with open(file_path, "w") as f:
        json.dump(data, f)
    
def json_to_pandas(data):
    records = []
    rate_date = data['date']
    base = data['base']

    for currency, rate in data['rates'].items():
        records.append(
            {
                'rate_date': rate_date,
                'base': base,
                'currency': currency,
                'rate': rate,
                'ingestion_ts': time.time(),
                'source': "frankfurter"
            }
        )
        
    return pd.DataFrame(records)

def save_silver_df(df):
    output_dir = "data/silver"
    file_name = f"fx_rates_{datetime.date.today()}.parquet"
    file_path = os.path.join(output_dir, file_name)

    df.to_parquet(file_path, index=False)

def compute_payload_hash(payload):
    payload_str = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

def insert_if_not_exists(payload: dict):
    conn = get_connection()
    cur = conn.cursor()

    payload_str = json.dumps(payload, sort_keys=True)
    payload_hash = compute_payload_hash(payload)

    check_sql = """
        SELECT COUNT(*)
        FROM FX_RATES_RAW
        WHERE PAYLOAD_HASH = %s
    """
    cur.execute(check_sql, (payload_hash,))
    exists = cur.fetchone()[0] > 0

    if exists:
        print("Payload déjà présent → aucune insertion")
    else:
        insert_sql = """
            INSERT INTO FX_RATES_RAW (
                RAW_PAYLOAD,
                PAYLOAD_HASH,
                INGESTION_TS,
                SOURCE
            )
            SELECT
                PARSE_JSON(%s),
                %s,
                CURRENT_TIMESTAMP(),
                %s
        """
        cur.execute(insert_sql, (payload_str, payload_hash, "frankfurter_api"))
        print("Payload inséré en Bronze")

    cur.close()
    conn.close()


def main():
    url = build_url(BASE_CURRENCY, TARGET_CURRENCIES)
    data, code = fetch_rates(url)
    save_raw_json(data)
    df = json_to_pandas(data)
    save_silver_df(df)

if __name__ == "__main__":
    url = build_url(BASE_CURRENCY, TARGET_CURRENCIES)
    data, code = fetch_rates(url)
    insert_if_not_exists(data)