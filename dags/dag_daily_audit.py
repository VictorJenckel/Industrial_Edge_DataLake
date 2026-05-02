"""
dag_daily_audit.py — Daily data integrity audit for all inspection lines.

Runs at 07:00 every day to audit the previous day's data, comparing
the raw machine log files against the PostgreSQL database records.

For each inspection line, the audit:
  1. Downloads the machine log files (via SMB for Eagle lines, SSH for CUT2)
  2. Reads the logs line-by-line (no pandas — controlled memory footprint)
  3. Queries the database for the same day's records
  4. Compares the two sets of unique record identifiers
  5. Writes the result to the auditoria_logs table
  6. Cleans up the downloaded files

Lines audited in parallel:
  - MIRROR1   (Eagle Vision, SMB protocol)
  - CUT1  (Eagle Vision, SMB protocol)
  - CUT2   (CSV over SSH/rsync, composite key audit)

A final consolidation task collects all results and raises an exception
if any divergence is detected, surfacing the alert in the Airflow UI.

Architecture role:
  This DAG is the integrity guardian of the bronze layer. It ensures that
  the PostgreSQL data feeding the Digital Twin telemetry submodels is a
  faithful mirror of what the physical machines actually measured.
  Divergences are flagged before they propagate to the AAS layer.

Memory design:
  All log reading uses csv.DictReader (line-by-line, never pandas).
  All DB reads use direct cursor iteration (never fetchall()).
  Only the set of unique IDs for each day is kept in RAM at any time.
"""

import csv
import glob
import os
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# CONFIGURATION — all sensitive values from environment variables
# =============================================================================

# MIRROR1 line (Eagle Vision, SMB)
_MIRROR1_HOST  = os.environ.get("MIRROR1_SMB_HOST")
_MIRROR1_SHARE = os.environ.get("MIRROR1_SMB_SHARE", "Logs")
_MIRROR1_USER  = os.environ.get("MIRROR1_SMB_USER")
_MIRROR1_PASS  = os.environ.get("MIRROR1_SMB_PASS")

# CUT1 line (Eagle Vision, SMB)
_CUT1_HOST  = os.environ.get("CUT1_SMB_HOST")
_CUT1_SHARE = os.environ.get("CUT1_SMB_SHARE", "Logs")
_CUT1_USER  = os.environ.get("CUT1_SMB_USER")
_CUT1_PASS  = os.environ.get("CUT1_SMB_PASS")

# CUT2 line (SSH/rsync)
_CUT2_IP      = os.environ.get("CUT2_MACHINE_IP")
_CUT2_USER    = os.environ.get("CUT2_MACHINE_USER")
_CUT2_PASS    = os.environ.get("CUT2_MACHINE_PASS")
_CUT2_DATADIR = os.environ.get("CUT2_REMOTE_DATA_DIR", "/home/operator/CUT2_data/history")

# Staging base dir for downloaded audit files
_AUDIT_STAGING = os.environ.get("AUDIT_STAGING_DIR", "/home/dc2/temp/auditoria")

LINES = {
    "MIRROR1": {
        "type":           "smb",
        "server":         _MIRROR1_HOST,
        "share":          _MIRROR1_SHARE,
        "user":           _MIRROR1_USER,
        "password":       _MIRROR1_PASS,
        "local_dir":      os.path.join(_AUDIT_STAGING, "MIRROR1"),
        "table":          "inspection_data_eagle_MIRROR1",
        "log_key":        "GrpID",
        "db_key":         "grpid",
        "log_date_col":   "Date",
        "db_date_col":    "date",
        "sep":            "\t",
        "date_format":    "%Y/%m/%d %H:%M:%S",
    },
    "CUT1": {
        "type":           "smb",
        "server":         _CUT1_HOST,
        "share":          _CUT1_SHARE,
        "user":           _CUT1_USER,
        "password":       _CUT1_PASS,
        "local_dir":      os.path.join(_AUDIT_STAGING, "CUT1"),
        "table":          "inspection_data_eagle_CUT1",
        "log_key":        "GrpID",
        "db_key":         "grpid",
        "log_date_col":   "Date",
        "db_date_col":    "date",
        "sep":            "\t",
        "date_format":    "%Y/%m/%d %H:%M:%S",
    },
    "CUT2": {
        "type":           "ssh",
        "ip":             _CUT2_IP,
        "user":           _CUT2_USER,
        "password":       _CUT2_PASS,
        "remote_dir":     _CUT2_DATADIR,
        "local_dir":      os.path.join(_AUDIT_STAGING, "CUT2"),
        "table":          "inspection_data_CUT2",
        "log_key":        "LiteID",
        "db_key":         "lite_id",
        "log_date_col":   "Time",
        "db_date_col":    "time",
        "sep":            ";",
        "date_format":    "%Y-%m-%d %H:%M:%S",  # reconstructed: date_ref + ' ' + Time
    },
}

POSTGRES_CONFIG = {
    "dbname":   os.environ.get("PG_DBNAME", "dbc"),
    "user":     os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASSWORD"),
    "host":     os.environ.get("PG_HOST", "postgres_dc2"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
}

# =============================================================================
# FILE DOWNLOAD HELPERS — SMB (MIRROR1, CUT1)
# =============================================================================

def _download_smb(server, share, user, password, remote_folder, filename, local_dir) -> str | None:
    """
    Downloads a single PlateLog file via smbclient.
    Returns the local file path, or None if the download failed.
    """
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)

    cmd = [
        "smbclient", f"//{server}/{share}",
        "-U", f"{user}%{password}",
        "-c", f"cd {remote_folder}; get {filename} {filename}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=local_dir)

    if result.returncode == 0 and os.path.exists(local_path):
        print(f"  [SMB] '{filename}' downloaded.")
        return local_path

    print(f"  [SMB] Failed to download '{filename}': {result.stderr.strip()}")
    return None


def _collect_smb_logs(cfg: dict, audited_date: datetime, next_date: datetime) -> list:
    """
    Downloads PlateLog files for the audited day and the following day via SMB.

    The next day's file is required because records from the last minutes of
    the audited day may be physically written to the next day's file.

    Returns a list of (local_path, date_str) tuples.
    """
    files = []
    for date in [audited_date, next_date]:
        remote_folder = date.strftime("%Y-%m")
        filename      = f"{date.strftime('%Y-%m-%d')}-PlateLog.txt"
        path = _download_smb(
            cfg["server"], cfg["share"], cfg["user"], cfg["password"],
            remote_folder, filename, cfg["local_dir"],
        )
        if path:
            files.append((path, date.strftime("%Y-%m-%d")))
    return files


# =============================================================================
# FILE DOWNLOAD HELPERS — SSH/rsync (CUT2)
# =============================================================================

def _download_ssh(cfg: dict, date: datetime, hour_str: str) -> str | None:
    """
    Downloads a single hourly CUT2 CSV file via rsync/SSH.
    Returns the local file path, or None if the transfer failed.
    """
    os.makedirs(cfg["local_dir"], exist_ok=True)

    remote_path = (
        f"{cfg['remote_dir']}/"
        f"{date.strftime('%Y')}/"
        f"{date.strftime('%m')}/"
        f"{date.strftime('%d')}/"
        f"{hour_str}.csv"
    )
    local_name = f"CUT2_{date.strftime('%Y-%m-%d')}_{hour_str}.csv"
    local_path = os.path.join(cfg["local_dir"], local_name)

    cmd = [
        "sshpass", "-p", cfg["password"],
        "rsync", "-az", "--checksum",
        "-e", "ssh -o StrictHostKeyChecking=no",
        f"{cfg['user']}@{cfg['ip']}:{remote_path}",
        local_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0 and os.path.exists(local_path):
        return local_path

    print(f"  [SSH] Failed to download '{remote_path}': {result.stderr.strip()}")
    return None


def _collect_CUT2_logs(cfg: dict, audited_date: datetime, next_date: datetime) -> list:
    """
    Downloads all 24 hourly CSV files for the audited day plus
    the 00:00 file of the next day (captures records at the midnight boundary).

    Returns a list of (local_path, date_str, hour_str) tuples.
    """
    files = []

    for hour in range(24):
        hour_str = f"{hour:02d}"
        path = _download_ssh(cfg, audited_date, hour_str)
        if path:
            files.append((path, audited_date.strftime("%Y-%m-%d"), hour_str))

    path = _download_ssh(cfg, next_date, "00")
    if path:
        files.append((path, next_date.strftime("%Y-%m-%d"), "00"))

    return files


# =============================================================================
# LOG ID EXTRACTION — memory-controlled 
# =============================================================================

def _extract_ids_smb(files_with_meta: list, cfg: dict, audited_date: datetime) -> tuple:
    """
    Extracts unique GrpIDs for the audited day from PlateLog files (MIRROR1/CUT1).

    Memory design: reads line-by-line via csv.DictReader — only the two
    required columns (Date and GrpID) are accessed per row, and only the
    set of unique IDs accumulates in RAM (proportional to unique records
    in the day, not to total file size or column count).
    """
    ids_in_day   = set()
    target_date  = audited_date.date()
    col_date     = cfg["log_date_col"]
    col_key      = cfg["log_key"]
    date_format  = cfg["date_format"]
    sep          = cfg["sep"]
    total_read   = 0

    for file_path, _ in files_with_meta:
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter=sep)
                for row in reader:
                    raw_date = row.get(col_date, "").strip()
                    raw_key  = row.get(col_key,  "").strip()
                    if not raw_date or not raw_key:
                        continue
                    try:
                        clean_date = raw_date.split(".")[0]
                        dt = datetime.strptime(clean_date, date_format)
                        if dt.date() == target_date:
                            # Normalize: remove decimal part (e.g. "123.0" → "123")
                            clean_key = str(raw_key).split(".")[0].strip()
                            ids_in_day.add(clean_key)
                            total_read += 1
                    except (ValueError, TypeError):
                        continue
            print(f"  {os.path.basename(file_path)}: {total_read} IDs accumulated so far")
        except Exception as exc:
            print(f"  Error reading '{file_path}': {exc}")

    print(f"  Total unique IDs in log: {len(ids_in_day)}")
    return len(ids_in_day), ids_in_day


def _extract_ids_CUT2(files_with_meta: list, cfg: dict, audited_date: datetime) -> tuple:
    """
    Extracts unique composite keys for the audited day from CUT2 hourly CSV files.

    The composite key mirrors the ETL's event_seq logic:
      cap_id | lite_id | HH:MM:SS | cumcount(cap_id, lite_id, time)

    Memory design: same as _extract_ids_smb — line-by-line, no DataFrames,
    no concat. The event counter dict is the only auxiliary structure in RAM.
    """
    ids_in_day    = set()
    target_date   = audited_date.date()
    sep           = cfg["sep"]
    total_read    = 0
    event_counter = {}  # Simulates df.groupby().cumcount() from the ETL

    for file_path, date_ref_str, _ in files_with_meta:
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter=sep)
                for row in reader:
                    raw_time = row.get("Time",   "").strip()
                    raw_lite = row.get("Lite id", "").strip()
                    raw_cap  = row.get("Cap id",  "").strip()

                    if not raw_time or not raw_lite:
                        continue

                    try:
                        clean_time = raw_time.split(".")[0]
                        dt = datetime.strptime(
                            f"{date_ref_str} {clean_time}", "%Y-%m-%d %H:%M:%S"
                        )

                        if dt.date() == target_date:
                            lite = str(raw_lite).split(".")[0].strip()
                            cap  = str(raw_cap).split(".")[0].strip()
                            time_str = dt.strftime("%H:%M:%S")

                            group_key = (cap, lite, time_str)
                            seq = event_counter.get(group_key, 0)
                            event_counter[group_key] = seq + 1

                            # Composite key: "cap|lite|HH:MM:SS|seq"
                            composite_key = f"{cap}|{lite}|{time_str}|{seq}"
                            ids_in_day.add(composite_key)
                            total_read += 1
                    except (ValueError, TypeError):
                        continue
            print(f"  {os.path.basename(file_path)}: {total_read} valid records accumulated")
        except Exception as exc:
            print(f"  Error reading '{file_path}': {exc}")

    print(f"  Total unique composite keys in CUT2 log: {len(ids_in_day)}")
    return len(ids_in_day), ids_in_day


# =============================================================================
# DATABASE QUERIES — memory-controlled (cursor iteration, no fetchall)
# =============================================================================

def _count_db_records(table: str, date_col: str, audited_date: datetime) -> int:
    """
    Counts records in `table` for the audited day using a server-side COUNT(*).
    No rows are transferred to Python RAM — only the integer result.
    """
    date_str = audited_date.strftime("%Y-%m-%d")
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {date_col}::date = %s;",
                (date_str,),
            )
            total = cur.fetchone()[0]
    print(f"  DB records ({table}) for {date_str}: {total}")
    return total


def _fetch_db_ids(table: str, key_col: str, date_col: str, audited_date: datetime) -> set:
    """
    Returns the set of unique key values from `table` for the audited day.

    Memory design: iterates the cursor directly instead of fetchall().
    fetchall() would allocate a list of all rows in Python RAM before
    building the set. Direct iteration builds the set incrementally,
    keeping only one row at a time outside the driver buffer.
    """
    date_str = audited_date.strftime("%Y-%m-%d")
    ids = set()
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {key_col} FROM {table} WHERE {date_col}::date = %s;",
                (date_str,),
            )
            for (id_val,) in cur:
                if id_val is not None:
                    ids.add(str(id_val).strip())
    return ids


def _fetch_db_ids_CUT2(table: str, audited_date: datetime) -> set:
    """
    Returns composite key strings from the CUT2 table for the audited day.
    Mirrors the format generated by _extract_ids_CUT2: "cap|lite|HH:MM:SS|seq"
    """
    date_str = audited_date.strftime("%Y-%m-%d")
    ids = set()
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT cap_id, lite_id, to_char(time, 'HH24:MI:SS'), event_seq
                FROM {table}
                WHERE time::date = %s;
                """,
                (date_str,),
            )
            for cap, lite, time_str, seq in cur:
                if lite is not None:
                    ids.add(f"{cap}|{lite}|{time_str}|{seq}")
    return ids


# =============================================================================
# AUDIT RESULT PERSISTENCE
# =============================================================================

def _write_audit_result(
    line: str,
    audited_date: datetime,
    total_log: int,
    total_db: int,
    missing_ids: set,
    status: str,
    note: str = "",
) -> None:
    """
    Writes the audit result to the auditoria_logs table using an upsert.
    Re-running the audit for the same day safely overwrites the previous result.
    """
    divergence = total_log - total_db
    ids_str    = ",".join(str(i) for i in sorted(missing_ids)) if missing_ids else ""
    if len(ids_str) > 2000:
        ids_str = ids_str[:2000] + "...(truncated)"

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO auditoria_logs
                    (linha, data_auditada, total_log, total_banco,
                     divergencia, ids_faltando, status, observacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (linha, data_auditada) DO UPDATE SET
                    total_log    = EXCLUDED.total_log,
                    total_banco  = EXCLUDED.total_banco,
                    divergencia  = EXCLUDED.divergencia,
                    ids_faltando = EXCLUDED.ids_faltando,
                    status       = EXCLUDED.status,
                    observacao   = EXCLUDED.observacao,
                    auditado_em  = NOW();
                """,
                (
                    line,
                    audited_date.strftime("%Y-%m-%d"),
                    total_log, total_db, divergence,
                    ids_str, status, note[:1000],
                ),
            )
            conn.commit()

    print(
        f"  [{line.upper()}] Result written — "
        f"Log: {total_log} | DB: {total_db} | Δ: {divergence:+d} | {status}"
    )


def _cleanup_audit_files(local_dir: str) -> None:
    """Removes all downloaded audit files after processing."""
    for fpath in glob.glob(os.path.join(local_dir, "*")):
        try:
            os.remove(fpath)
        except Exception as exc:
            print(f"  Warning: could not remove '{fpath}': {exc}")


# =============================================================================
# COMPARISON LOGIC
# =============================================================================

def _calculate_status(
    total_log: int,
    total_db: int,
    ids_log: set,
    ids_db: set,
    key_label: str = "GrpID",
) -> tuple:
    """
    Compares log and database ID sets and returns (status, note, missing_ids).

    Status "ok" is only emitted when both the count AND the ID sets match exactly.
    Equal counts with different IDs (e.g. log=[1,2,3], db=[1,2,4]) would
    otherwise produce a false "ok" — this guard prevents that.

    Possible statuses:
      ok              — no divergence
      missing_in_db   — records present in log but not in database
      excess_in_db    — records present in database but not in log
    """
    missing_ids = ids_log - ids_db
    extra_ids   = ids_db  - ids_log
    divergence  = total_log - total_db

    if divergence == 0 and not missing_ids and not extra_ids:
        return "ok", "Audit passed with no divergences.", set()

    if divergence >= 0:
        sample = sorted(missing_ids)[:20]
        status = "missing_in_db"
        note   = (
            f"Divergence of {divergence} record(s). "
            f"{len(missing_ids)} IDs in log missing from DB. "
            f"{key_label} sample: {sample}"
        )
    else:
        sample = sorted(extra_ids)[:20]
        status = "excess_in_db"
        note   = (
            f"DB has {abs(divergence)} extra record(s). "
            f"{len(extra_ids)} IDs in DB absent from log. "
            f"{key_label} sample: {sample}"
        )

    return status, note, missing_ids


# =============================================================================
# AUDIT TASKS
# =============================================================================

def audit_smb_line(line_name: str, **context) -> None:
    """
    Audit task for Eagle Vision lines (MIRROR1, CUT1) using SMB protocol.

    Memory-controlled flow:
      1. Download PlateLog files (audited day + next day) via smbclient
      2. Read line-by-line → set of GrpIDs for the audited day
      3. Iterate DB cursor → set of DB IDs (no fetchall)
      4. Compare sets, write result, clean up staging files
    """
    cfg           = LINES[line_name]
    now           = datetime.now()
    audited_date  = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_date     = now.replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"\n{'='*60}")
    print(f"AUDIT {line_name.upper()} — date: {audited_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")

    try:
        print("\n[1/4] Downloading log files ...")
        files = _collect_smb_logs(cfg, audited_date, next_date)

        if not files:
            msg = "No log files found for this audit date."
            print(f"  WARNING: {msg}")
            _write_audit_result(line_name, audited_date, 0, 0, set(), "no_log", msg)
            return

        print("\n[2/4] Extracting IDs from log (line-by-line, no pandas) ...")
        total_log, ids_log = _extract_ids_smb(files, cfg, audited_date)

        print("\n[3/4] Querying database ...")
        total_db = _count_db_records(cfg["table"], cfg["db_date_col"], audited_date)
        ids_db   = _fetch_db_ids(
            cfg["table"], cfg["db_key"], cfg["db_date_col"], audited_date
        )

        print("\n[4/4] Calculating divergences ...")
        status, note, missing = _calculate_status(
            total_log, total_db, ids_log, ids_db, key_label="GrpID"
        )
        _write_audit_result(line_name, audited_date, total_log, total_db, missing, status, note)

    except Exception as exc:
        msg = f"Audit error: {exc}"
        print(f"  ERROR: {msg}")
        _write_audit_result(line_name, audited_date, 0, 0, set(), "error", msg)
        raise

    finally:
        _cleanup_audit_files(cfg["local_dir"])


def audit_CUT2_line(**context) -> None:
    """
    Audit task for the CUT2 line (SSH/rsync protocol, composite key).

    Memory-controlled flow:
      1. Download 24 hourly CSV files + next day's 00:00 file via rsync
      2. Read each file line-by-line → set of composite keys
         (mirrors the ETL's event_seq cumcount logic)
      3. Iterate DB cursor → set of composite keys from DB
      4. Compare sets, write result, clean up staging files
    """
    cfg          = LINES["CUT2"]
    now          = datetime.now()
    audited_date = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_date    = now.replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"\n{'='*60}")
    print(f"AUDIT CUT2 — date: {audited_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")

    try:
        print("\n[1/4] Downloading CUT2 log files (24h + next day 00h) ...")
        files = _collect_CUT2_logs(cfg, audited_date, next_date)

        if not files:
            msg = "No CUT2 log files found for this audit date."
            print(f"  WARNING: {msg}")
            _write_audit_result("CUT2", audited_date, 0, 0, set(), "no_log", msg)
            return

        print("\n[2/4] Extracting composite keys from CUT2 log ...")
        total_log, ids_log = _extract_ids_CUT2(files, cfg, audited_date)

        print("\n[3/4] Querying database ...")
        total_db = _count_db_records(cfg["table"], cfg["db_date_col"], audited_date)
        ids_db   = _fetch_db_ids_CUT2(cfg["table"], audited_date)

        print("\n[4/4] Calculating divergences ...")
        status, note, missing = _calculate_status(
            total_log, total_db, ids_log, ids_db,
            key_label="CompositeKey(Cap|Lite|Time|Seq)",
        )
        _write_audit_result("CUT2", audited_date, total_log, total_db, missing, status, note)

    except Exception as exc:
        msg = f"CUT2 audit error: {exc}"
        print(f"  ERROR: {msg}")
        _write_audit_result("CUT2", audited_date, 0, 0, set(), "error", msg)
        raise

    finally:
        _cleanup_audit_files(cfg["local_dir"])


# =============================================================================
# CONSOLIDATION TASK
# =============================================================================

def generate_audit_summary(**context) -> None:
    """
    Reads the audit results for all three lines and prints a consolidated report.
    Raises ValueError if any divergence is detected — surfaces the alert in
    the Airflow UI so operators are notified without checking the DB directly.

    Uses trigger_rule='all_done' so it runs even if individual audit tasks failed,
    giving a complete picture of all lines in a single report.
    """
    audited_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"AUDIT SUMMARY — {audited_date}")
    print(f"{'='*60}")

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT linha, total_log, total_banco, divergencia, status, observacao
                FROM auditoria_logs
                WHERE data_auditada = %s
                ORDER BY linha;
                """,
                (audited_date,),
            )
            results = cur.fetchall()

    has_divergence = False
    for line, total_log, total_db, divergence, status, note in results:
        symbol = "✓" if status == "ok" else "✗"
        print(
            f"  {symbol} {line.upper():6s} | "
            f"Log: {total_log:5d} | DB: {total_db:5d} | "
            f"Δ: {divergence:+4d} | {status}"
        )
        if note and status != "ok":
            print(f"         ↳ {note}")
        if status != "ok":
            has_divergence = True

    print(f"\n{'='*60}")
    if has_divergence:
        print("  WARNING: Divergences detected. Check the auditoria_logs table.")
        raise ValueError(
            f"Daily audit {audited_date}: divergences found in one or more lines."
        )
    else:
        print("✓  All lines audited with no divergences.")


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    "daily_inspection_audit",
    default_args=default_args,
    description=(
        "Daily data integrity audit: machine log files vs PostgreSQL records "
        "for MIRROR1, CUT1, and CUT2 inspection lines. "
        "Runs at 07:00 auditing the previous day."
    ),
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["audit", "integrity", "bronze", "digital-twin"],
)

# Three audit tasks run in parallel; summary waits for all of them
task_MIRROR1 = PythonOperator(
    task_id="audit_MIRROR1",
    python_callable=lambda **ctx: audit_smb_line("MIRROR1", **ctx),
    provide_context=True,
    dag=dag,
)

task_CUT1 = PythonOperator(
    task_id="audit_CUT1",
    python_callable=lambda **ctx: audit_smb_line("CUT1", **ctx),
    provide_context=True,
    dag=dag,
)

task_CUT2 = PythonOperator(
    task_id="audit_CUT2",
    python_callable=audit_CUT2_line,
    provide_context=True,
    dag=dag,
)

task_summary = PythonOperator(
    task_id="audit_summary",
    python_callable=generate_audit_summary,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done",
)

[task_MIRROR1, task_CUT1, task_CUT2] >> task_summary
