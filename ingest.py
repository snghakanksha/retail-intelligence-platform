"""
Retail Intelligence Platform — Fast Ingestion via Snowflake Stage
Uses PUT + COPY INTO instead of row-by-row inserts.
Completes in 2-3 minutes instead of hours.
"""

import logging
from datetime import datetime
from pathlib import Path
import snowflake.connector
from config import SNOWFLAKE_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")


def get_connection():
    return snowflake.connector.connect(
        **SNOWFLAKE_CONFIG,
        client_session_keep_alive=True
    )


def run_pipeline():
    conn = get_connection()
    cur  = conn.cursor()
    log.info("Connected to Snowflake.")

    cur.execute("USE WAREHOUSE RETAIL_WH")
    cur.execute("USE DATABASE RAW_DB")
    cur.execute("USE SCHEMA RAW")

    # Create a temporary internal stage
    cur.execute("CREATE TEMP STAGE IF NOT EXISTS fast_stage")
    log.info("Stage ready.")

    # ── 1. Load orders.csv ─────────────────────────────────────
    log.info("Uploading orders.csv...")
    cur.execute(f"PUT file://{DATA_DIR.absolute()}/orders.csv @fast_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute("""
        COPY INTO RAW_DB.RAW.raw_orders
            (order_id, customer_id, product_id, order_date, order_status,
             quantity, unit_price, discount_pct, shipping_city, shipping_country)
        FROM @fast_stage/orders.csv.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL')
        )
        PURGE = TRUE
    """)
    cur.execute("SELECT COUNT(*) FROM RAW_DB.RAW.raw_orders")
    log.info(f"  raw_orders → {cur.fetchone()[0]:,} rows")

    # ── 2. Load customers.csv ──────────────────────────────────
    log.info("Uploading customers.csv...")
    cur.execute(f"PUT file://{DATA_DIR.absolute()}/customers.csv @fast_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute("""
        COPY INTO RAW_DB.RAW.raw_customers
            (customer_id, customer_name, email, phone,
             city, country, segment, signup_date)
        FROM @fast_stage/customers.csv.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL')
        )
        PURGE = TRUE
    """)
    cur.execute("SELECT COUNT(*) FROM RAW_DB.RAW.raw_customers")
    log.info(f"  raw_customers → {cur.fetchone()[0]:,} rows")

    # ── 3. Load products.json ──────────────────────────────────
    log.info("Uploading products.json...")
    cur.execute(f"PUT file://{DATA_DIR.absolute()}/products.json @fast_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute("""
        COPY INTO RAW_DB.RAW.raw_products (raw_data)
        FROM (SELECT $1 FROM @fast_stage/products.json.gz)
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
        PURGE = TRUE
    """)
    cur.execute("SELECT COUNT(*) FROM RAW_DB.RAW.raw_products")
    log.info(f"  raw_products → {cur.fetchone()[0]:,} rows")

    # ── 4. Load web_events.json ────────────────────────────────
    log.info("Uploading web_events.json...")
    cur.execute(f"PUT file://{DATA_DIR.absolute()}/web_events.json @fast_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute("""
        COPY INTO RAW_DB.RAW.raw_web_events (raw_data)
        FROM (SELECT $1 FROM @fast_stage/web_events.json.gz)
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
        PURGE = TRUE
    """)
    cur.execute("SELECT COUNT(*) FROM RAW_DB.RAW.raw_web_events")
    log.info(f"  raw_web_events → {cur.fetchone()[0]:,} rows")

    log.info("\nAll tables loaded successfully!")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run_pipeline()