/// CUT2_etl — High-performance CSV → PostgreSQL ETL for the CUT2 inspection line
///
/// This binary is the processing core (muscle) of the CUT2 ingestion pipeline.
/// Orchestration concerns (SSH stat, rsync, watermark management, XCom, audit
/// logging, and hour-transition windows) remain in Python/Airflow.
///
/// Responsibilities:
///   1.  Read the local CSV (sep=';') row by row
///   2.  Compute scan_position = (cap_id - lite_id) % 4096 (original column names)
///   3.  Build `time` column = data_str + Time column from CSV
///   4.  Normalize column names to snake_case
///   5.  Replace machine-emitted invalid strings with NULL
///   6.  Force numeric coercion on non-key columns
///   7.  Compute event_seq = cumcount per (cap_id, lite_id, time)
///   8.  Append extraction_timestamp
///   9.  Validate mandatory schema before any DB write
///  10.  Bulk COPY to staging table → atomic upsert into destination table
///  11.  Write JSON to stdout → {"rows": <n>, "status": "success"}
///
/// Why Rust here:
///   Glass inspection lines produce continuous high-frequency CSV logs.
///   At production speed this binary processes files with near-zero memory
///   footprint, keeping ingestion latency within the 2-minute Airflow schedule.

use std::collections::HashMap;
use std::io::Write;

use chrono::{NaiveDateTime, Utc};
use clap::Parser;
use postgres::{Client, NoTls};
use serde_json::json;

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(about = "CUT2 inspection line ETL: local CSV → PostgreSQL")]
struct Args {
    /// Full path to the local CSV file (copied by rsync from the machine host)
    #[arg(long)]
    arquivo_path: String,

    /// Reference date for this file (YYYY-MM-DD) — derived from the remote path
    #[arg(long)]
    data_str: String,

    /// Destination table in PostgreSQL
    #[arg(long, default_value = "inspection_data_CUT2")]
    dest_table: String,

    // --- PostgreSQL connection (passed by Airflow DAG, never hardcoded) ---
    #[arg(long)]
    pg_host: String,
    #[arg(long, default_value = "5432")]
    pg_port: u16,
    #[arg(long)]
    pg_user: String,
    #[arg(long)]
    pg_password: String,
    #[arg(long)]
    pg_dbname: String,
}

// =============================================================================
// CONSTANTS
// =============================================================================

/// Invalid strings emitted by the CUT2 machine hardware — mapped to NULL.
/// These are floating-point edge cases that the machine firmware does not filter.
const INVALID_STRINGS: &[&str] = &[
    "-1.#J", "1.#J", "NaN", "1.#QNAN", "-1.#IND", "-1.#INF", "1.#INF",
];

/// Mandatory columns after transformation (snake_case).
/// Schema validation runs before any DB write to prevent partial inserts.
const EXPECTED_COLUMNS: &[&str] = &[
    "time", "lite_id", "cap_id", "event_seq", "scan_position", "convoy_id",
    "tolerance_nbr", "nominal_length", "nominal_width", "length_diff", "width_diff",
    "diagonal", "rectangularity", "rotation", "position", "leading_corner_left",
    "leading_edge", "leading_corner_right", "right_edge", "trailing_corner_right",
    "trailing_edge", "trailing_corner_left", "left_edge", "markings", "total_result",
    "top_curv", "top_dev", "rgt_curv", "rgt_dev", "btm_curv", "btm_dev",
    "lft_curv", "lft_dev",
];

/// Insert column order (extraction_timestamp appended last).
/// Must match the PostgreSQL table definition exactly.
const INSERT_COLUMNS: &[&str] = &[
    "time", "lite_id", "cap_id", "event_seq", "scan_position", "convoy_id",
    "tolerance_nbr", "nominal_length", "nominal_width", "length_diff", "width_diff",
    "diagonal", "rectangularity", "rotation", "position", "leading_corner_left",
    "leading_edge", "leading_corner_right", "right_edge", "trailing_corner_right",
    "trailing_edge", "trailing_corner_left", "left_edge", "markings", "total_result",
    "top_curv", "top_dev", "rgt_curv", "rgt_dev", "btm_curv", "btm_dev",
    "lft_curv", "lft_dev", "extraction_timestamp",
];

// =============================================================================
// TRANSFORMATION HELPERS
// =============================================================================

/// Normalize a column name to snake_case, mirroring the pandas behavior:
///   .str.strip().str.lower()
///   .str.replace(r'[^\w]+', '_')
///   .str.replace(r'_+', '_')
///   .str.strip('_')
fn normalize_col(s: &str) -> String {
    let lower = s.trim().to_lowercase();
    let mut result = String::new();
    let mut last_underscore = false;

    for c in lower.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
            last_underscore = c == '_';
        } else if !last_underscore {
            result.push('_');
            last_underscore = true;
        }
    }

    result.trim_matches('_').to_string()
}

/// Returns true if the string is one of the machine's known invalid markers.
fn is_invalid(s: &str) -> bool {
    INVALID_STRINGS.contains(&s)
}

/// Attempts to parse a string as f64. Returns None for invalid/empty values.
fn parse_f64(s: &str) -> Option<f64> {
    if is_invalid(s) || s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

/// Attempts to parse a string as i64. Returns None for invalid/empty values.
fn parse_i64(s: &str) -> Option<i64> {
    if is_invalid(s) || s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

/// Escapes a field for PostgreSQL COPY CSV format.
/// NULL is represented as an empty field (COPY ... NULL '').
fn csv_escape(s: &str) -> String {
    if s.contains('"') || s.contains(',') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

// =============================================================================
//  ROW STRUCTURE
// =============================================================================

struct CUT2Row {
    // Keys / temporal
    time:                  Option<NaiveDateTime>,
    lite_id:               Option<i64>,
    cap_id:                Option<i64>,
    event_seq:             i64,
    scan_position:         Option<i64>,
    // Geometry measurements
    convoy_id:             Option<f64>,
    tolerance_nbr:         Option<f64>,
    nominal_length:        Option<f64>,
    nominal_width:         Option<f64>,
    length_diff:           Option<f64>,
    width_diff:            Option<f64>,
    diagonal:              Option<f64>,
    rectangularity:        Option<f64>,
    rotation:              Option<f64>,
    position:              Option<f64>,
    // Edge measurements (leading/trailing/left/right)
    leading_corner_left:   Option<f64>,
    leading_edge:          Option<f64>,
    leading_corner_right:  Option<f64>,
    right_edge:            Option<f64>,
    trailing_corner_right: Option<f64>,
    trailing_edge:         Option<f64>,
    trailing_corner_left:  Option<f64>,
    left_edge:             Option<f64>,
    markings:              Option<f64>,
    total_result:          Option<String>, // categorical: 'OK', 'X', etc.
    // Curvature / deviation per edge
    top_curv:              Option<f64>,
    top_dev:               Option<f64>,
    rgt_curv:              Option<f64>,
    rgt_dev:               Option<f64>,
    btm_curv:              Option<f64>,
    btm_dev:               Option<f64>,
    lft_curv:              Option<f64>,
    lft_dev:               Option<f64>,
    extraction_timestamp:  String,
}

impl CUT2Row {
    /// Serializes the row as a CSV line for PostgreSQL COPY.
    /// Column order must exactly match INSERT_COLUMNS.
    fn to_csv_line(&self) -> String {
        let time_str = self.time
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();

        let fields: Vec<String> = vec![
            time_str,
            self.lite_id.map(|v| v.to_string()).unwrap_or_default(),
            self.cap_id.map(|v| v.to_string()).unwrap_or_default(),
            self.event_seq.to_string(),
            self.scan_position.map(|v| v.to_string()).unwrap_or_default(),
            self.convoy_id.map(|v| v.to_string()).unwrap_or_default(),
            self.tolerance_nbr.map(|v| v.to_string()).unwrap_or_default(),
            self.nominal_length.map(|v| v.to_string()).unwrap_or_default(),
            self.nominal_width.map(|v| v.to_string()).unwrap_or_default(),
            self.length_diff.map(|v| v.to_string()).unwrap_or_default(),
            self.width_diff.map(|v| v.to_string()).unwrap_or_default(),
            self.diagonal.map(|v| v.to_string()).unwrap_or_default(),
            self.rectangularity.map(|v| v.to_string()).unwrap_or_default(),
            self.rotation.map(|v| v.to_string()).unwrap_or_default(),
            self.position.map(|v| v.to_string()).unwrap_or_default(),
            self.leading_corner_left.map(|v| v.to_string()).unwrap_or_default(),
            self.leading_edge.map(|v| v.to_string()).unwrap_or_default(),
            self.leading_corner_right.map(|v| v.to_string()).unwrap_or_default(),
            self.right_edge.map(|v| v.to_string()).unwrap_or_default(),
            self.trailing_corner_right.map(|v| v.to_string()).unwrap_or_default(),
            self.trailing_edge.map(|v| v.to_string()).unwrap_or_default(),
            self.trailing_corner_left.map(|v| v.to_string()).unwrap_or_default(),
            self.left_edge.map(|v| v.to_string()).unwrap_or_default(),
            self.markings.map(|v| v.to_string()).unwrap_or_default(),
            self.total_result.as_deref().map(csv_escape).unwrap_or_default(),
            self.top_curv.map(|v| v.to_string()).unwrap_or_default(),
            self.top_dev.map(|v| v.to_string()).unwrap_or_default(),
            self.rgt_curv.map(|v| v.to_string()).unwrap_or_default(),
            self.rgt_dev.map(|v| v.to_string()).unwrap_or_default(),
            self.btm_curv.map(|v| v.to_string()).unwrap_or_default(),
            self.btm_dev.map(|v| v.to_string()).unwrap_or_default(),
            self.lft_curv.map(|v| v.to_string()).unwrap_or_default(),
            self.lft_dev.map(|v| v.to_string()).unwrap_or_default(),
            self.extraction_timestamp.clone(),
        ];

        fields.join(",") + "\n"
    }
}

// =============================================================================
// CSV READING AND TRANSFORMATION
// =============================================================================

fn read_and_transform(
    file_path: &str,
    data_str: &str,
) -> Result<Vec<CUT2Row>, Box<dyn std::error::Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_path(file_path)?;

    // Capture original headers for index-based access before normalization
    let raw_headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();

    // Index map: original name → column index
    // Needed to access "Cap id" and "Lite id" before renaming
    let idx: HashMap<String, usize> = raw_headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.trim().to_string(), i))
        .collect();

    let idx_cap  = idx.get("Cap id").copied();
    let idx_lite = idx.get("Lite id").copied();
    let idx_time = idx.get("Time").copied();

    // Normalized headers (snake_case) — mirrors the pandas rename logic
    let norm_headers: Vec<String> = raw_headers.iter().map(|h| normalize_col(h)).collect();

    let extraction_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();

    // First pass: collect raw rows.
    // event_seq (cumcount) requires seeing the full group before numbering,
    // so we buffer all rows before the second pass.
    struct RawRow {
        cap_id:        Option<i64>,
        lite_id:       Option<i64>,
        time_parsed:   Option<NaiveDateTime>,
        scan_position: Option<i64>,
        fields:        Vec<String>,
    }

    let mut raw_rows: Vec<RawRow> = Vec::new();

    for result in reader.records() {
        let record = result?;

        // scan_position uses original column names (before rename)
        let cap_raw  = idx_cap.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let lite_raw = idx_lite.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let cap_val:  Option<i64> = parse_i64(&cap_raw);
        let lite_val: Option<i64> = parse_i64(&lite_raw);

        // scan_position = (cap_id - lite_id) % 4096 using Euclidean modulo
        // to handle the 12-bit PLC counter rollover correctly
        let scan_pos: Option<i64> = match (cap_val, lite_val) {
            (Some(c), Some(l)) => Some((c - l).rem_euclid(4096)),
            _                  => None,
        };

        // Build 'time' = data_str + Time column value
        let time_raw = idx_time.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();
        let time_parsed = if time_raw.is_empty() {
            None
        } else {
            let combined = format!("{} {}", data_str, time_raw);
            NaiveDateTime::parse_from_str(&combined, "%Y-%m-%d %H:%M:%S").ok()
        };

        // Normalize field values: replace machine invalid strings with empty (→ NULL)
        let fields: Vec<String> = record.iter()
            .map(|v| {
                let v = v.trim();
                if is_invalid(v) { String::new() } else { v.to_string() }
            })
            .collect();

        raw_rows.push(RawRow {
            cap_id: cap_val,
            lite_id: lite_val,
            time_parsed,
            scan_position: scan_pos,
            fields,
        });
    }

    // Second pass: compute event_seq (cumcount per group).
    // Group key: (cap_id, lite_id, time string)
    // This deterministic key replaces extraction_timestamp for deduplication,
    // making re-reads of the same file fully idempotent.
    let mut counters: HashMap<(Option<i64>, Option<i64>, String), i64> = HashMap::new();
    let mut rows: Vec<CUT2Row> = Vec::with_capacity(raw_rows.len());

    for raw in raw_rows {
        let time_key = raw.time_parsed
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();

        let group_key = (raw.cap_id, raw.lite_id, time_key);
        let seq = counters.entry(group_key).or_insert(0);
        let event_seq = *seq;
        *seq += 1;

        let field = |name: &str| -> String {
            norm_headers.iter()
                .position(|h| h == name)
                .and_then(|i| raw.fields.get(i))
                .cloned()
                .unwrap_or_default()
        };

        let f = |name: &str| parse_f64(&field(name));

        let total_result = {
            let v = field("total_result");
            if v.is_empty() { None } else { Some(v) }
        };

        rows.push(CUT2Row {
            time:                  raw.time_parsed,
            lite_id:               raw.lite_id,
            cap_id:                raw.cap_id,
            event_seq,
            scan_position:         raw.scan_position,
            convoy_id:             f("convoy_id"),
            tolerance_nbr:         f("tolerance_nbr"),
            nominal_length:        f("nominal_length"),
            nominal_width:         f("nominal_width"),
            length_diff:           f("length_diff"),
            width_diff:            f("width_diff"),
            diagonal:              f("diagonal"),
            rectangularity:        f("rectangularity"),
            rotation:              f("rotation"),
            position:              f("position"),
            leading_corner_left:   f("leading_corner_left"),
            leading_edge:          f("leading_edge"),
            leading_corner_right:  f("leading_corner_right"),
            right_edge:            f("right_edge"),
            trailing_corner_right: f("trailing_corner_right"),
            trailing_edge:         f("trailing_edge"),
            trailing_corner_left:  f("trailing_corner_left"),
            left_edge:             f("left_edge"),
            markings:              f("markings"),
            total_result,
            top_curv:              f("top_curv"),
            top_dev:               f("top_dev"),
            rgt_curv:              f("rgt_curv"),
            rgt_dev:               f("rgt_dev"),
            btm_curv:              f("btm_curv"),
            btm_dev:               f("btm_dev"),
            lft_curv:              f("lft_curv"),
            lft_dev:               f("lft_dev"),
            extraction_timestamp:  extraction_timestamp.clone(),
        });
    }

    Ok(rows)
}

// =============================================================================
// SCHEMA VALIDATION
// =============================================================================

/// Validates that all mandatory columns are present in the normalized CSV headers.
/// Runs before any DB connection to prevent partial inserts on malformed files.
fn validate_schema(norm_headers: &[String]) -> Result<(), String> {
    let present: std::collections::HashSet<&str> =
        norm_headers.iter().map(|s| s.as_str()).collect();

    // event_seq and scan_position are computed, not read from CSV
    let missing: Vec<&str> = EXPECTED_COLUMNS
        .iter()
        .filter(|&&c| !present.contains(c) && c != "event_seq" && c != "scan_position")
        .copied()
        .collect();

    if !missing.is_empty() {
        return Err(format!("Missing columns in CSV: {:?}", missing));
    }
    Ok(())
}

// =============================================================================
// POSTGRESQL LOAD: COPY → staging → upsert
// =============================================================================

fn load_postgres(
    rows: &[CUT2Row],
    pg: &mut Client,
    dest_table: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Step 1: Create temporary staging table (same schema as destination)
    pg.execute(
        &format!(
            "CREATE TEMP TABLE IF NOT EXISTS CUT2_stage AS SELECT * FROM {} LIMIT 0",
            dest_table
        ),
        &[],
    )?;
    pg.execute("TRUNCATE TABLE CUT2_stage", &[])?;

    // Step 2: Build in-memory CSV buffer
    let cols_str = INSERT_COLUMNS.join(", ");
    let mut csv_buf: Vec<u8> = Vec::with_capacity(rows.len() * 128);
    for row in rows {
        csv_buf.extend_from_slice(row.to_csv_line().as_bytes());
    }

    // Step 3: Bulk COPY into staging (fastest PostgreSQL ingestion path)
    let copy_sql = format!(
        "COPY CUT2_stage ({}) FROM STDIN WITH (FORMAT csv, NULL '')",
        cols_str
    );
    let mut writer = pg.copy_in(&copy_sql)?;
    writer.write_all(&csv_buf)?;
    writer.finish()?;

    // Step 4: Atomic upsert from staging → destination.
    // ON CONFLICT (cap_id, lite_id, time, event_seq) DO UPDATE ensures
    // that re-reading the same file updates changed fields without
    // creating duplicate rows. The natural key is fully deterministic.
    let update_cols: String = INSERT_COLUMNS
        .iter()
        .filter(|&&c| !["cap_id", "lite_id", "time", "event_seq"].contains(&c))
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect::<Vec<_>>()
        .join(", ");

    let upsert_sql = format!(
        "INSERT INTO {dest} ({cols}) \
         SELECT {cols} FROM CUT2_stage \
         ON CONFLICT (cap_id, lite_id, time, event_seq) DO UPDATE SET {upd}",
        dest = dest_table,
        cols = cols_str,
        upd  = update_cols,
    );
    let rows_affected = pg.execute(&upsert_sql, &[])?;

    Ok(rows_affected)
}

// =============================================================================
// ENTRY POINT
// =============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    eprintln!(
        "[CUT2_etl] Starting: file='{}' date='{}'",
        args.arquivo_path, args.data_str
    );

    // Schema validation — runs before connecting to PostgreSQL
    // to detect malformed files early without consuming DB resources
    {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(true)
            .from_path(&args.arquivo_path)?;

        let norm_headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|h| normalize_col(h))
            .collect();

        validate_schema(&norm_headers).map_err(|e| {
            eprintln!("[CUT2_etl] Schema error: {}", e);
            e
        })?;
    }

    // Transform
    let rows = read_and_transform(&args.arquivo_path, &args.data_str)?;
    eprintln!("[CUT2_etl] {} rows transformed.", rows.len());

    // Load
    let pg_connstr = format!(
        "host={} port={} user={} password={} dbname={}",
        args.pg_host, args.pg_port, args.pg_user, args.pg_password, args.pg_dbname
    );
    let mut pg = Client::connect(&pg_connstr, NoTls)?;

    let rows_affected = load_postgres(&rows, &mut pg, &args.dest_table)?;
    eprintln!(
        "[CUT2_etl] {} rows inserted/updated in '{}'.",
        rows_affected, args.dest_table
    );

    // JSON output consumed by the Python/Airflow orchestration layer
    println!(
        "{}",
        json!({
            "rows":   rows_affected,
            "status": "success"
        })
    );

    Ok(())
}
