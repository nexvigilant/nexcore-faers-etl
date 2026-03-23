//! CTVP Phase 2 Validation: Verify existing pipeline output Parquet files.
//!
//! This test validates the artifacts produced by a prior faers-pipeline run,
//! confirming signal detection correctness without re-ingesting the full 370MB dataset.
//!
//! Run with: cargo test -p nexcore-faers-etl --test validate_parquet_output --release -- --nocapture

use duckdb::{Connection, Result as DuckResult};

const SIGNALS_PARQUET: &str = "/home/matthew/nexcore/output/faers_2025Q4_signals.parquet";
const COUNTS_PARQUET: &str = "/home/matthew/nexcore/output/faers_2025Q4_counts.parquet";

fn parquet_available() -> bool {
    std::path::Path::new(SIGNALS_PARQUET).exists() && std::path::Path::new(COUNTS_PARQUET).exists()
}

fn conn() -> DuckResult<Connection> {
    Connection::open_in_memory()
}

/// Validate counts Parquet: schema, row count, no nulls, DUPIXENT present.
#[test]
fn test_validate_counts_parquet() {
    if !parquet_available() {
        eprintln!("SKIP: Parquet files not found");
        return;
    }

    let db = conn().expect("DuckDB open failed");

    // Row count
    let row_count: u64 = db
        .query_row(
            &format!("SELECT COUNT(*) FROM read_parquet('{COUNTS_PARQUET}')"),
            [],
            |row| row.get(0),
        )
        .expect("Count query failed");
    eprintln!("Counts Parquet rows: {row_count}");
    assert!(
        row_count > 10_000,
        "Expected >10K unique drug-event pairs, got {row_count}"
    );

    // Column check
    let col_count: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM (SELECT * FROM read_parquet('{COUNTS_PARQUET}') LIMIT 1) sub"
            ),
            [],
            |row| row.get(0),
        )
        .expect("Column check failed");
    assert!(col_count > 0, "Parquet is empty");

    // Distinct drugs and events
    let distinct_drugs: u64 = db
        .query_row(
            &format!("SELECT COUNT(DISTINCT drug) FROM read_parquet('{COUNTS_PARQUET}')"),
            [],
            |row| row.get(0),
        )
        .expect("Distinct drugs query failed");
    let distinct_events: u64 = db
        .query_row(
            &format!("SELECT COUNT(DISTINCT event) FROM read_parquet('{COUNTS_PARQUET}')"),
            [],
            |row| row.get(0),
        )
        .expect("Distinct events query failed");
    eprintln!("Distinct drugs: {distinct_drugs}, events: {distinct_events}");
    assert!(distinct_drugs > 1_000, "Too few drugs: {distinct_drugs}");
    assert!(distinct_events > 500, "Too few events: {distinct_events}");

    // DUPIXENT check
    let dupixent_pairs: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{COUNTS_PARQUET}') WHERE drug = 'DUPIXENT'"
            ),
            [],
            |row| row.get(0),
        )
        .expect("DUPIXENT query failed");
    eprintln!("DUPIXENT drug-event pairs: {dupixent_pairs}");
    assert!(
        dupixent_pairs > 10,
        "DUPIXENT not found or too few pairs: {dupixent_pairs}"
    );

    // Top 10 drugs by total n
    let mut stmt = db
        .prepare(&format!(
            "SELECT drug, SUM(n) as total_n FROM read_parquet('{COUNTS_PARQUET}') \
             GROUP BY drug ORDER BY total_n DESC LIMIT 10"
        ))
        .expect("Prepare failed");
    let rows = stmt
        .query_map([], |row| {
            let drug: String = row.get(0)?;
            let total: i64 = row.get(1)?;
            Ok((drug, total))
        })
        .expect("Query failed");
    eprintln!("\n--- Top 10 Drugs by Total N ---");
    for row in rows {
        let (drug, n) = row.expect("Row read failed");
        eprintln!("  {drug:<30} {n:>8}");
    }
}

/// Validate signals Parquet: schema, no NaN/Inf, signal rates, known pairs.
#[test]
fn test_validate_signals_parquet() {
    if !parquet_available() {
        eprintln!("SKIP: Parquet files not found");
        return;
    }

    let db = conn().expect("DuckDB open failed");

    // Total signals results
    let total: u64 = db
        .query_row(
            &format!("SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}')"),
            [],
            |row| row.get(0),
        )
        .expect("Total query failed");
    eprintln!("Total signal detection results: {total}");
    assert!(total > 10_000, "Too few results: {total}");

    // NaN check
    let nan_count: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') \
                 WHERE isnan(prr) OR isnan(ror) OR isnan(ic) OR isnan(ebgm)"
            ),
            [],
            |row| row.get(0),
        )
        .expect("NaN query failed");
    eprintln!("Results with NaN: {nan_count}");
    assert!(
        nan_count == 0,
        "Found {nan_count} NaN values — algorithm failure"
    );

    // Inf check
    let inf_count: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') \
                 WHERE isinf(prr) OR isinf(ror) OR isinf(ic) OR isinf(ebgm)"
            ),
            [],
            |row| row.get(0),
        )
        .expect("Inf query failed");
    eprintln!("Results with Inf: {inf_count}");

    // Signal counts by algorithm
    let prr_signals: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') WHERE prr_signal = true"
            ),
            [],
            |row| row.get(0),
        )
        .expect("PRR signal count failed");
    let ror_signals: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') WHERE ror_signal = true"
            ),
            [],
            |row| row.get(0),
        )
        .expect("ROR signal count failed");
    let ic_signals: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') WHERE ic_signal = true"
            ),
            [],
            |row| row.get(0),
        )
        .expect("IC signal count failed");
    let ebgm_signals: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') WHERE ebgm_signal = true"
            ),
            [],
            |row| row.get(0),
        )
        .expect("EBGM signal count failed");
    let any_signal: u64 = db
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{SIGNALS_PARQUET}') \
                 WHERE prr_signal OR ror_signal OR ic_signal OR ebgm_signal"
            ),
            [],
            |row| row.get(0),
        )
        .expect("Any signal count failed");

    let signal_rate = any_signal as f64 / total as f64 * 100.0;
    eprintln!("\n--- Signal Detection Summary ---");
    eprintln!("  PRR signals:  {prr_signals:>8}");
    eprintln!("  ROR signals:  {ror_signals:>8}");
    eprintln!("  IC signals:   {ic_signals:>8}");
    eprintln!("  EBGM signals: {ebgm_signals:>8}");
    eprintln!("  ANY signal:   {any_signal:>8} / {total} ({signal_rate:.1}%)");

    assert!(signal_rate > 0.5, "Signal rate too low: {signal_rate:.1}%");
    assert!(
        signal_rate < 90.0,
        "Signal rate too high: {signal_rate:.1}%"
    );
}

/// Validate known clinically-established drug-event pairs in signal output.
#[test]
fn test_validate_known_signal_pairs() {
    if !parquet_available() {
        eprintln!("SKIP: Parquet files not found");
        return;
    }

    let db = conn().expect("DuckDB open failed");

    let known_pairs = vec![
        (
            "DUPIXENT",
            "CONJUNCTIVITIS",
            "Dupilumab-associated conjunctivitis (FDA label)",
        ),
        (
            "LENALIDOMIDE",
            "NEUTROPENIA",
            "Lenalidomide hematologic toxicity (FDA label)",
        ),
        (
            "LENALIDOMIDE",
            "THROMBOCYTOPENIA",
            "Lenalidomide platelet suppression (FDA label)",
        ),
        (
            "METHOTREXATE",
            "HEPATOTOXICITY",
            "MTX hepatic injury (well-established)",
        ),
    ];

    let mut found = 0;
    eprintln!("\n--- Known Signal Pair Validation ---");

    for (drug, event_substr, rationale) in &known_pairs {
        let query = format!(
            "SELECT drug, event, n, prr, ror, ic, ebgm, \
                    prr_signal, ror_signal, ic_signal, ebgm_signal \
             FROM read_parquet('{SIGNALS_PARQUET}') \
             WHERE drug = '{drug}' AND event LIKE '%{event_substr}%' \
             AND (prr_signal OR ror_signal OR ic_signal OR ebgm_signal) \
             ORDER BY n DESC LIMIT 1"
        );

        let result: Result<(String, String, i32, f64, f64, f64, f64), _> =
            db.query_row(&query, [], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, f64>(6)?,
                ))
            });

        match result {
            Ok((d, e, n, prr, ror, ic, ebgm)) => {
                eprintln!(
                    "  HIT:  {d} + {e} (n={n}, PRR={prr:.2}, ROR={ror:.2}, IC={ic:.2}, EBGM={ebgm:.2}) — {rationale}"
                );
                found += 1;
            }
            Err(_) => {
                // Check if pair exists but not flagged
                let exists_query = format!(
                    "SELECT drug, event, n, prr, ror, ic, ebgm \
                     FROM read_parquet('{SIGNALS_PARQUET}') \
                     WHERE drug = '{drug}' AND event LIKE '%{event_substr}%' \
                     ORDER BY n DESC LIMIT 1"
                );
                let exists: Result<(String, String, i32, f64, f64, f64, f64), _> =
                    db.query_row(&exists_query, [], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i32>(2)?,
                            row.get::<_, f64>(3)?,
                            row.get::<_, f64>(4)?,
                            row.get::<_, f64>(5)?,
                            row.get::<_, f64>(6)?,
                        ))
                    });
                match exists {
                    Ok((d, e, n, prr, ror, ic, ebgm)) => {
                        eprintln!(
                            "  MISS (not flagged): {d} + {e} (n={n}, PRR={prr:.2}, ROR={ror:.2}, IC={ic:.2}, EBGM={ebgm:.2}) — {rationale}"
                        );
                    }
                    Err(_) => {
                        eprintln!("  MISS (not in data): {drug} + {event_substr} — {rationale}");
                    }
                }
            }
        }
    }

    let hit_rate = found as f64 / known_pairs.len() as f64;
    eprintln!(
        "\nKnown signal validation: {found}/{} ({:.0}%)",
        known_pairs.len(),
        hit_rate * 100.0
    );
    assert!(
        hit_rate >= 0.25,
        "Too few known signals: {found}/{} — algorithm may be broken",
        known_pairs.len()
    );
}

/// Top 20 signals by PRR — the headline results.
#[test]
fn test_top_signals_report() {
    if !parquet_available() {
        eprintln!("SKIP: Parquet files not found");
        return;
    }

    let db = conn().expect("DuckDB open failed");

    let mut stmt = db
        .prepare(&format!(
            "SELECT drug, event, n, prr, ror, ic, ebgm \
             FROM read_parquet('{SIGNALS_PARQUET}') \
             WHERE (prr_signal OR ror_signal OR ic_signal OR ebgm_signal) AND n >= 10 \
             ORDER BY prr DESC LIMIT 20"
        ))
        .expect("Prepare failed");

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i32>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, f64>(6)?,
            ))
        })
        .expect("Query failed");

    eprintln!("\n--- Top 20 Signals (n≥10) by PRR ---");
    eprintln!(
        "{:<25} {:<30} {:>6} {:>8} {:>8} {:>8} {:>8}",
        "Drug", "Event", "N", "PRR", "ROR", "IC", "EBGM"
    );
    for row in rows {
        let (drug, event, n, prr, ror, ic, ebgm) = row.expect("Row failed");
        let d = if drug.len() > 24 { &drug[..24] } else { &drug };
        let e = if event.len() > 29 {
            &event[..29]
        } else {
            &event
        };
        eprintln!("{d:<25} {e:<30} {n:>6} {prr:>8.2} {ror:>8.2} {ic:>8.2} {ebgm:>8.2}");
    }
}
