"""
JSON to DWH ETL Pipeline
========================
Loads nested JSON data into MSSQL and/or PostgreSQL Data Warehouses.

Data Source Options:
  1. Local JSON file (default, for testing)
  2. REST API (recommended for production pipelines)
  3. Other sources: S3, SFTP, database query — see comments below

Automation:
  - Run manually: python json_to_dwh_etl.py
  - Schedule via Windows Task Scheduler or Linux cron
  - See README or bottom of this file for scheduling instructions
"""

import pyodbc
import pandas as pd
import json
import logging
import os
import time
from datetime import datetime
from sqlalchemy import create_engine

# ─── Optional: for API source ─────────────────────────────────────────────────
import requests                     # pip install requests
# ─── Optional: for email alerts ───────────────────────────────────────────────
# import smtplib
# from email.mime.text import MIMEText

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION  ← change these values to match your environment
# ══════════════════════════════════════════════════════════════════════════════

# ── MSSQL ─────────────────────────────────────────────────────────────────────
MSSQL_SERVER   = 'DESKTOP-I8DIQH8\\SQLEXPRESS'   # ← change to your server
MSSQL_DATABASE = 'Json Practice'                   # ← change to your database

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
PG_HOST     = 'localhost'       # ← change to your PostgreSQL host
PG_PORT     = '5432'
PG_DATABASE = 'json_practice'   # ← change to your PostgreSQL database name
PG_USER     = 'postgres'        # ← change to your PostgreSQL username
PG_PASSWORD = 'your_password'   # ← change or load from env variable (see below)
# Recommended: load password from environment variable instead of hardcoding:
# PG_PASSWORD = os.environ.get('PG_PASSWORD', '')

# ── Target table name ─────────────────────────────────────────────────────────
TABLE_NAME = 'json_file'        # ← name of the table created in the database

# ── Data source ───────────────────────────────────────────────────────────────
# Set DATA_SOURCE to:
#   'file' — read from a local JSON file (good for testing)
#   'api'  — fetch live data from a REST API  (recommended for production)
DATA_SOURCE = 'api'

# ── Local file path (used only when DATA_SOURCE = 'file') ─────────────────────
LOCAL_JSON_PATH = r'C:\path\to\your\file.json'   # ← change to your file path

# ── API settings (used only when DATA_SOURCE = 'api') ─────────────────────────
#
#  Replace this URL with any REST endpoint that returns a JSON array, e.g.:
#    - Internal company API:  'https://api.yourcompany.com/v1/companies'
#    - Public sample API:     'https://randomuser.me/api/?results=100'
#    - Any paginated API:     implement pagination in fetch_from_api() below
#
API_URL     = 'https://jsonplaceholder.typicode.com/users'   # ← change this
API_HEADERS = {
    # 'Authorization': f"Bearer {os.environ.get('API_TOKEN', '')}",  # ← uncomment if needed
    'Content-Type': 'application/json',
}
API_PARAMS  = {}   # ← add query parameters if needed, e.g. {'page': 1, 'limit': 100}

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()         # also print to console
    ]
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DATA EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def fetch_from_api(url: str, headers: dict, params: dict) -> list:
    """
    Fetch JSON data from a REST API endpoint.

    For paginated APIs, extend this function with a loop, e.g.:
        page = 1
        while True:
            params['page'] = page
            response = requests.get(url, ...)
            batch = response.json()
            if not batch: break
            all_data.extend(batch)
            page += 1
    """
    logger.info(f"Fetching data from API: {url}")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()         # raises exception for 4xx / 5xx
        data = response.json()

        # The API might return a list directly, or wrap it in a key like {"data": [...]}
        if isinstance(data, list):
            pass                            # already a list — use as-is
        elif isinstance(data, dict):
            # ↓ adjust the key name to match your API's response structure
            data = data.get('data', data.get('results', data.get('items', [data])))

        logger.info(f"Fetched {len(data)} records from API")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


def load_from_file(path: str) -> list:
    """Load JSON records from a local file (one JSON object per line, or a JSON array)."""
    logger.info(f"Loading data from file: {path}")
    data = []
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
        if content.startswith('['):
            data = json.loads(content)      # JSON array
        else:
            for line in content.splitlines():
                if line.strip():
                    data.append(json.loads(line))   # NDJSON (one object per line)
    logger.info(f"Loaded {len(data)} records from file")
    return data


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORMATION
# ══════════════════════════════════════════════════════════════════════════════

def detect_nested_columns(data: list) -> dict:
    """Return a dict of column_name → list of sub-keys for list-of-dict columns."""
    nested_columns = {}
    if not data:
        return nested_columns
    sample = data[0]
    for key, value in sample.items():
        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            nested_columns[key] = list(value[0].keys())
    return nested_columns


def flatten_array_column(df: pd.DataFrame, column_name: str, fields: list) -> pd.DataFrame:
    """Expand the first element of a list-of-dict column into individual columns."""
    if column_name in df.columns:
        for field in fields:
            df[f'{column_name}_{field}'] = df[column_name].apply(
                lambda x: x[0].get(field) if isinstance(x, list) and len(x) > 0 and x[0] is not None else None
            )
        df = df.drop(columns=[column_name])
    return df


def transform(data: list) -> pd.DataFrame:
    """Normalize and flatten nested JSON into a flat DataFrame."""
    logger.info("Starting data transformation...")

    df = pd.json_normalize(data, sep='_')
    logger.info(f"Shape after normalization: {df.shape}")

    nested_columns_fields = detect_nested_columns(data)
    logger.info(f"Detected nested columns: {list(nested_columns_fields.keys())}")

    for column, fields in nested_columns_fields.items():
        df = flatten_array_column(df, column, fields)
        logger.info(f"Flattened column: '{column}'")

    # Handle 'specialties' (or similar) — list of strings → comma-separated string
    if 'specialties' in df.columns:
        df['specialties'] = df['specialties'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else None
        )

    # Add a pipeline metadata column so you know when each row was loaded
    df['_etl_loaded_at'] = datetime.now()

    logger.info(f"Transformation complete. Final shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# LOADING
# ══════════════════════════════════════════════════════════════════════════════

def get_mssql_engine():
    """Create a SQLAlchemy engine for MSSQL with Windows Authentication."""
    connection_string = (
        f'mssql+pyodbc://@{MSSQL_SERVER}/{MSSQL_DATABASE}'
        f'?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'
    )
    return create_engine(connection_string)


def get_postgres_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    connection_string = (
        f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}'
    )
    # pip install psycopg2-binary
    return create_engine(connection_string)


def load_to_database(df: pd.DataFrame, engine, db_label: str) -> None:
    """Load DataFrame into the target database."""
    logger.info(f"Loading {len(df)} rows into [{db_label}].{TABLE_NAME} ...")
    start = time.time()
    df.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
    elapsed = round(time.time() - start, 2)
    logger.info(f"✓ Loaded to {db_label} in {elapsed}s")


# ══════════════════════════════════════════════════════════════════════════════
# OPTIONAL: EMAIL ALERT ON FAILURE
# ══════════════════════════════════════════════════════════════════════════════
# def send_failure_alert(error_message: str) -> None:
#     """Send an email alert when the pipeline fails."""
#     msg = MIMEText(f"ETL pipeline failed:\n\n{error_message}")
#     msg['Subject'] = 'ETL Pipeline Failure'
#     msg['From']    = 'etl@yourcompany.com'
#     msg['To']      = 'you@yourcompany.com'
#     with smtplib.SMTP('smtp.yourcompany.com', 587) as server:
#         server.login('user', 'password')
#         server.send_message(msg)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 60)

    try:
        # ── 1. EXTRACT ──────────────────────────────────────────────────────
        if DATA_SOURCE == 'api':
            data = fetch_from_api(API_URL, API_HEADERS, API_PARAMS)
        elif DATA_SOURCE == 'file':
            data = load_from_file(LOCAL_JSON_PATH)
        else:
            raise ValueError(f"Unknown DATA_SOURCE: '{DATA_SOURCE}'. Use 'api' or 'file'.")

        if not data:
            logger.warning("No data returned. Pipeline exiting early.")
            return

        # ── 2. TRANSFORM ─────────────────────────────────────────────────────
        df = transform(data)

        # ── 3. LOAD ──────────────────────────────────────────────────────────
        # Load to MSSQL
        mssql_engine = get_mssql_engine()
        load_to_database(df, mssql_engine, 'MSSQL')

        # Load to PostgreSQL
        pg_engine = get_postgres_engine()
        load_to_database(df, pg_engine, 'PostgreSQL')

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        # send_failure_alert(str(e))   # ← uncomment to enable email alerts
        raise


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    run_pipeline()


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULING / AUTOMATION NOTES
# ══════════════════════════════════════════════════════════════════════════════
#
# ── Windows Task Scheduler ───────────────────────────────────────────────────
#   1. Open Task Scheduler → Create Basic Task
#   2. Set trigger (e.g. Daily at 06:00)
#   3. Action: Start a Program
#      Program:   C:\Users\<you>\AppData\Local\Programs\Python\Python311\python.exe
#      Arguments: C:\path\to\json_to_dwh_etl.py
#
# ── Linux / macOS cron ───────────────────────────────────────────────────────
#   Run `crontab -e` and add one of these lines:
#
#   Every day at 6 AM:
#     0 6 * * * /usr/bin/python3 /home/user/json_to_dwh_etl.py >> /home/user/etl_cron.log 2>&1
#
#   Every hour:
#     0 * * * * /usr/bin/python3 /home/user/json_to_dwh_etl.py
#
# ── Airflow / Prefect (advanced orchestration) ───────────────────────────────
#   For production pipelines, consider wrapping run_pipeline() inside an
#   Apache Airflow DAG or a Prefect flow for retries, monitoring, and alerts.
#
# ── Other data source ideas ──────────────────────────────────────────────────
#   Replace fetch_from_api() with:
#   - boto3 (AWS S3):   s3.get_object(Bucket='...', Key='...')
#   - paramiko (SFTP):  sftp.get('remote/path', 'local/path')
#   - pysftp, ftplib
#   - psycopg2 / pyodbc query result → pd.read_sql(...)
#   - Google Sheets:    gspread library