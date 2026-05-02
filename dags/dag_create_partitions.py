"""
dag_create_partitions.py — Daily partition provisioning for all inspection tables.

Runs at 23:30 every day to create tomorrow's date partitions before midnight,
ensuring zero-downtime at the day rollover for all active ingestion DAGs.

Tables provisioned:
  - inspection_data_eagle_mirror1   (Eagle Vision mirror1 line)
  - inspection_data_eagle_cut1  (Eagle Vision cut1 line)
  - inspection_data_cut2         (cut2 inspection line)
  - p2_mirror2_panel               (mirror2 panel measurements)
  - p2_mirror2_defects             (mirror2 defect records)

All tables use PostgreSQL native range partitioning by timestamp (daily).
Partition range: [tomorrow 00:00:00, day-after-tomorrow 00:00:00) — open-ended
upper bound avoids the "no partition found" error at 23:59:59.xxx.

CREATE TABLE IF NOT EXISTS makes this DAG fully idempotent: safe to re-run,
backfill, or retry without risk of errors or duplicate structures.

Architecture role:
  Maintenance task in the bronze layer of the inspection data lake.
  Without it, ingestion DAGs fail at midnight when they attempt to insert
  into a partition that has not been created yet.
"""

import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# CONFIGURATION — credentials from environment variables
# =============================================================================

POSTGRES_CONFIG = {
    "dbname":   os.environ.get("PG_DBNAME", "dbc"),
    "user":     os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASSWORD"),
    "host":     os.environ.get("PG_HOST", "postgres_dc2"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
}

# All partitioned tables mapped to their inspection line label (for logging)
PARTITIONED_TABLES = {
    "inspection_data_eagle_mirror1":  "mirror1",
    "inspection_data_eagle_cut1": "cut1",
    "inspection_data_cut2":        "cut2",
    "p2_mirror2_panel":              "mirror2 Panel",
    "p2_mirror2_defects":            "mirror2 Defects",
}

# =============================================================================
# PARTITION LOGIC
# =============================================================================

def create_daily_partitions(**context) -> None:
    """
    Creates one date partition per inspection table for tomorrow.

    Partition naming convention : {parent_table}_{YYYY_MM_DD}
    Partition range             : [tomorrow 00:00:00, day-after 00:00:00)

    The open-ended upper bound (day-after 00:00:00 instead of 23:59:59)
    is intentional — it avoids the "no partition of relation found" error
    that occurs when a record arrives at exactly 23:59:59.xxx.

    All five partitions are committed individually so a failure on one
    table does not roll back already-created partitions.
    """
    tomorrow  = datetime.today().date() + timedelta(days=1)
    day_after = tomorrow + timedelta(days=1)

    tomorrow_fmt  = tomorrow.strftime("%Y-%m-%d")
    day_after_fmt = day_after.strftime("%Y-%m-%d")
    tomorrow_safe = tomorrow_fmt.replace("-", "_")

    print(f"Provisioning partitions for: {tomorrow_fmt}")
    print(f"Range: [{tomorrow_fmt} 00:00:00, {day_after_fmt} 00:00:00)")

    conn   = None
    cursor = None

    try:
        conn   = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        for parent_table, line_label in PARTITIONED_TABLES.items():
            partition_name = f"{parent_table}_{tomorrow_safe}"

            sql = f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF {parent_table}
                FOR VALUES FROM ('{tomorrow_fmt} 00:00:00')
                             TO ('{day_after_fmt} 00:00:00');
            """

            print(f"[{line_label}] Creating: {partition_name} ...")
            cursor.execute(sql)
            conn.commit()
            print(f"[{line_label}] OK")

        print(f"\nAll {len(PARTITIONED_TABLES)} partitions provisioned for {tomorrow_fmt}.")

    except Exception as exc:
        if conn:
            conn.rollback()
        print(f"Partition creation failed: {exc}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Database connection closed.")


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    "create_inspection_partitions",
    default_args=default_args,
    description=(
        "Daily partition provisioning for all inspection line tables "
        "(mirror1, cut1, cut2, mirror2 Panel, mirror2 Defects). "
        "Runs at 23:30 to prepare for the next day's ingestion."
    ),
    schedule_interval="30 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "partitions", "bronze", "digital-twin"],
) as dag:

    create_partition_task = PythonOperator(
        task_id="create_daily_partitions",
        python_callable=create_daily_partitions,
        provide_context=True,
    )
