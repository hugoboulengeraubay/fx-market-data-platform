from snowflake_client import get_connection

def create_bronze_table():
    conn = get_connection()
    cur = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS FX_RATES_RAW (
        RAW_PAYLOAD VARIANT,
        PAYLOAD_HASH STRING,
        INGESTION_TS TIMESTAMP_NTZ,
        SOURCE STRING
    );
    """

    cur.execute(create_table_sql)
    print("Table FX_RATES_RAW créée (ou déjà existante)")

    cur.close()
    conn.close()

if __name__ == "__main__":
    create_bronze_table()
