"""
dag_backup_quarterly.py — Quarterly cold archival: PostgreSQL → Parquet (Snappy).

Runs on a quarterly schedule (@quarterly) to archive the previous quarter's
inspection data from PostgreSQL into compressed Parquet files, then removes
the source rows from the database partition-by-partition to reclaim disk space.

For each table, the pipeline runs three sequential steps inside a TaskGroup:
  1. export   — Rust binary extracts data from PostgreSQL, writes Parquet (Snappy)
  2. validate — Verifies the Parquet output directory exists and is non-empty
  3. drop     — Deletes source rows day-by-day with individual commits

Tables processed:
  - inspection_data_eagle_mirror1   (Eagle Vision mirror1 line)
  - inspection_data_eagle_cut1  (Eagle Vision cut1 line)
  - inspection_data_cut2         (cut2 inspection line)
  - p2_mirror2_defects             (mirror2 defect records)
  - p2_mirror2_panel               (mirror2 panel measurements)

Architecture role:
  This DAG manages the transition from the hot (PostgreSQL) to the cold
  (Parquet) storage layer of the inspection data lake. The Parquet files
  produced here are also the historical data source for Digital Twin
  simulation models — enabling physics-based replay of past production
  conditions against current asset state.

Engineering notes:
  - All task IDs are fully static (no dynamic dates at module level).
    The Airflow scheduler re-parses this file every ~30s; dynamic dates
    at import time cause Zombie Tasks and scheduler deadlocks.
  - The quarter calculation always runs inside a function.
  - Day-by-day DELETE with individual commits avoids table-level locks
    and WAL explosion that a single 90-day DELETE would cause.
  - The Rust binary uses a server-side cursor (FETCH_SIZE rows at a time)
    to keep Python memory footprint near zero regardless of table size.
"""

import datetime
import json
import os
import shutil
import subprocess
from contextlib import closing

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# =============================================================================
# CONFIGURATION — all sensitive values from environment variables
# =============================================================================

RUST_BACKUP_BINARY = os.environ.get("BACKUP_ETL_BINARY", "/opt/airflow/bin/backup_etl")

POSTGRES_CONFIG = {
    "dbname":   os.environ.get("PG_DBNAME", "dbc"),
    "user":     os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASSWORD"),
    "host":     os.environ.get("PG_HOST", "postgres_dc2"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
}

# Tables to archive and the timestamp column used for range filtering
TABLES = {
    "inspection_data_eagle_mirror1":  "date",
    "inspection_data_eagle_cut1": "date",
    "inspection_data_cut2":        "time",
    "p2_mirror2_defects":            "Time",
    "p2_mirror2_panel":              "starttime",
}

# Base output path for Parquet cold storage
# Final path per table: PARQUET_BASE_DIR/{table}/{YYYY-MM-DD_a_YYYY-MM-DD}/
PARQUET_BASE_DIR = os.environ.get(
    "PARQUET_BASE_DIR", "/media/dc2/data/parquet_dc2"
)

# Number of rows fetched per batch by the Rust server-side cursor
FETCH_SIZE = int(os.environ.get("BACKUP_FETCH_SIZE", "50000"))

# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner":   "data-engineering",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    dag_id="backup_quarterly_rust",
    default_args=default_args,
    schedule_interval="@quarterly",
    catchup=False,
    tags=["backup", "parquet", "cold-storage", "digital-twin"],
)

# =============================================================================
# PYTHON ORCHESTRATION: DATE CALCULATION, VALIDATION, DAY-BY-DAY DELETE
# Heavy processing is delegated to the Rust binary (export task).
# =============================================================================

def _calculate_quarter_range() -> tuple[datetime.date, datetime.date]:
    """
    Calculates the start and end dates of the previous quarter.

    Uses a 15-day lookback from today to determine which quarter to archive,
    providing a safety margin so the DAG is not affected by @quarterly
    schedule drift.

    IMPORTANT: This function must always be called inside a task function,
    never at module level. The Airflow scheduler re-executes this module
    every ~30s; top-level datetime calls produce ever-changing DAG
    definitions, causing Zombie Tasks and scheduler instability.
    """
    today     = datetime.date.today()
    base_date = today - datetime.timedelta(days=15)
    quarter   = (base_date.month - 1) // 3 + 1
    year      = base_date.year

    start = datetime.date(year, 3 * (quarter - 1) + 1, 1)
    end_month = 3 * quarter
    end = (
        datetime.date(year, end_month, 1) + datetime.timedelta(days=31)
    ).replace(day=1) - datetime.timedelta(days=1)

    return start, end


def validate_parquet_output(table: str, **context) -> None:
    """
    Verifies that the Parquet output directory for `table` exists and contains files.

    Receives the exported period string via XCom from the export task.
    If no period was exported (e.g. no data in range), validation is skipped.
    Raises AirflowFailException if the directory exists but is empty —
    that indicates a silent export failure.
    """
    period = context["ti"].xcom_pull(
        task_ids=f"group_{table}.export_{table}_parquet",
        key="exported_period",
    )

    if not period:
        print(f"[{table}] No period exported. Validation skipped.")
        return

    output_dir = os.path.join(PARQUET_BASE_DIR, table, period)
    print(f"[{table}] Validating: {output_dir}")

    if not os.path.isdir(output_dir):
        print(f"[{table}] Output directory not found — no data in period. Skipping drop.")
        return

    if not os.listdir(output_dir):
        raise AirflowFailException(
            f"[{table}] Directory '{output_dir}' exists but is empty. Export failed."
        )

    print(f"[{table}] Parquet output validated.")


def drop_quarter_rows(table: str, **context) -> None:
    """
    Deletes the quarter's rows from `table` one day at a time, each in its own
    transaction. This pattern avoids:
      - Table-level locks from a single large DELETE
      - WAL explosion from a 90-day atomic transaction
      - Replication lag on any downstream consumers

    The date column used for filtering is read from the TABLES mapping.
    """
    start, end = _calculate_quarter_range()

    if end < start:
        print(f"[{table}] Invalid period. Drop skipped.")
        return

    date_col    = TABLES[table]
    total_deleted = 0

    print(f"[{table}] Dropping rows from {start} to {end} (day by day)...")

    with closing(psycopg2.connect(**POSTGRES_CONFIG)) as conn:
        current = start
        while current <= end:
            with conn.cursor() as cur:
                cur.execute(
                    f'DELETE FROM {table} WHERE CAST("{date_col}" AS DATE) = %s',
                    (current,),
                )
                deleted = cur.rowcount
                conn.commit()
            total_deleted += deleted
            print(f"  [{table}] {current}: {deleted} rows deleted.")
            current += datetime.timedelta(days=1)

    print(f"[{table}] Total deleted: {total_deleted} rows.")


# =============================================================================
# EXPORT TASK — delegates to the Rust binary
# =============================================================================

def export_to_parquet(table: str, **context) -> None:
    """
    Calculates the quarter range, builds the output path, and calls the
    Rust backup_etl binary. The binary connects to PostgreSQL, streams
    data via a server-side cursor (FETCH_SIZE rows at a time), and writes
    Snappy-compressed Parquet files to PARQUET_BASE_DIR/{table}/{period}/.

    On success: pushes the period string to XCom for the validation task.
    On failure: raises AirflowFailException with the binary's stderr output.
    """
    if table not in TABLES:
        raise AirflowFailException(f"Unrecognized table: '{table}'.")

    start, end = _calculate_quarter_range()

    if end < start:
        print("Invalid quarter range. Exiting.")
        return

    period_str  = f"{start}_a_{end}"
    output_dir  = os.path.join(PARQUET_BASE_DIR, table, period_str)
    date_col    = TABLES[table]

    # Clean up any partial output from a previous failed run
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    print(f"[{table}] Quarter: {start} → {end}")
    print(f"[{table}] Output: {output_dir}")

    cmd = [
        RUST_BACKUP_BINARY,
        "--tabela",      table,
        "--coluna-data", date_col,
        "--inicio",      str(start),
        "--fim",         str(end),
        "--pasta-base",  output_dir,
        "--fetch-size",  str(FETCH_SIZE),
        "--pg-host",     POSTGRES_CONFIG["host"],
        "--pg-port",     str(POSTGRES_CONFIG["port"]),
        "--pg-user",     POSTGRES_CONFIG["user"],
        "--pg-password", POSTGRES_CONFIG["password"],
        "--pg-dbname",   POSTGRES_CONFIG["dbname"],
    ]

    print(f"[{table}] Calling Rust backup_etl ...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=14400)  # 4h max

    if result.stderr:
        for line in result.stderr.strip().splitlines():
            print(f"[rust] {line}")

    if result.returncode != 0:
        raise AirflowFailException(
            f"backup_etl failed for '{table}' (exit {result.returncode}): "
            f"{result.stderr[:500]}"
        )

    try:
        payload = json.loads(result.stdout.strip())
    except json.JSONDecodeError as exc:
        raise AirflowFailException(
            f"Invalid JSON from backup_etl for '{table}': "
            f"{result.stdout[:200]} — {exc}"
        ) from exc

    if payload.get("status") != "success":
        raise AirflowFailException(
            f"backup_etl reported error for '{table}': "
            f"{payload.get('error', 'unknown')}"
        )

    rows       = payload.get("rows", 0)
    partitions = payload.get("partitions", 0)
    print(f"[{table}] Export complete — {rows} rows, {partitions} Parquet partitions.")

    # Push period string for the validation task
    context["ti"].xcom_push(key="exported_period", value=period_str)


# =============================================================================
# TASK WIRING — fully static IDs (no dynamic values at module level)
# =============================================================================

start_dag = EmptyOperator(task_id="start", dag=dag)
end_dag   = EmptyOperator(task_id="end",   dag=dag)

for table in TABLES:
    with TaskGroup(group_id=f"group_{table}", dag=dag) as group:

        export = PythonOperator(
            task_id=f"export_{table}_parquet",
            python_callable=export_to_parquet,
            op_args=[table],
            provide_context=True,
            dag=dag,
        )

        validate = PythonOperator(
            task_id=f"validate_{table}_parquet",
            python_callable=validate_parquet_output,
            op_args=[table],
            provide_context=True,
            dag=dag,
        )

        drop = PythonOperator(
            task_id=f"drop_{table}_rows",
            python_callable=drop_quarter_rows,
            op_args=[table],
            provide_context=True,
            dag=dag,
        )

        export >> validate >> drop

    start_dag >> group >> end_dag
