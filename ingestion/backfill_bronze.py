import datetime
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.fetch_fx_rates import build_url, fetch_rates, insert_if_not_exists, BASE_CURRENCY, TARGET_CURRENCIES

def backfill_bronze(start_date: str, end_date: str):
    """
    Remplit FX_RATES_RAW pour toutes les dates entre start_date et end_date incluses.
    start_date et end_date au format 'YYYY-MM-DD'
    """
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    current = start
    while current <= end:
        url = f"https://api.frankfurter.app/{current}?from={BASE_CURRENCY}&to={','.join(TARGET_CURRENCIES)}"
        print(f"Fetching rates for {current} ...")
        data, code = fetch_rates(url)

        if code == 200:
            insert_if_not_exists(data)
        else:
            print(f"Erreur API pour {current}, code {code}")
        
        time.sleep(0.5)  # petit délai pour ne pas spammer l'API
        current += datetime.timedelta(days=1)

if __name__ == "__main__":
    backfill_bronze("2025-01-01", str(datetime.date.today()))
