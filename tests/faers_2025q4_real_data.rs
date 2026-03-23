//! FAERS 2025Q4 Real-Data Integration Tests
//!
//! CTVP Phase 2 (Efficacy): Validates the full ETL→signal detection pipeline
//! against real FDA Adverse Event Reporting System data from 2025 Q4.
//!
//! Run with: cargo test -p nexcore-faers-etl --test faers_2025q4_real_data -- --nocapture

use nexcore_faers_etl::{
    SignalDetectionResult, columns, filter_signals, ingest_faers_quarterly_with_options,
    run_signal_detection_pipeline, transform_count_drug_events, transform_filter_minimum,
};
use std::path::Path;
use std::time::Instant;

const FAERS_DIR: &str = "/home/matthew/data/faers/faers_ascii_2025Q4/ASCII";

fn faers_data_available() -> bool {
    Path::new(FAERS_DIR).join("DEMO25Q4.txt").exists()
}

#[test]
fn test_ingest_real_faers_2025q4() {
    if !faers_data_available() {
        eprintln!("SKIP: FAERS 2025Q4 data not found at {FAERS_DIR}");
        return;
    }

    let start = Instant::now();
    let df = ingest_faers_quarterly_with_options(Path::new(FAERS_DIR), false)
        .expect("Failed to ingest FAERS 2025Q4");
    eprintln!("Ingest completed in {:.2?}", start.elapsed());
    eprintln!("Shape: {} rows × {} cols", df.height(), df.width());

    assert_eq!(df.width(), 3, "Expected 3 columns");
    assert!(df.column(columns::CASE_ID).is_ok());
    assert!(df.column(columns::DRUG).is_ok());
    assert!(df.column(columns::EVENT).is_ok());

    let row_count = df.height();
    assert!(row_count > 100_000, "Expected >100K pairs, got {row_count}");
    assert!(row_count < 10_000_000, "Unexpectedly high: {row_count}");
}

#[test]
fn test_count_aggregation_real_data() {
    if !faers_data_available() {
        eprintln!("SKIP: FAERS 2025Q4 data not found at {FAERS_DIR}");
        return;
    }

    let df =
        ingest_faers_quarterly_with_options(Path::new(FAERS_DIR), false).expect("Ingest failed");
    let counts = transform_count_drug_events(df).expect("Count failed");

    assert!(counts.height() > 10_000, "Expected >10K pairs");

    // Verify DUPIXENT exists in counted data
    let dupixent_found = (0..counts.height()).any(|i| {
        counts
            .column(columns::DRUG)
            .ok()
            .and_then(|col| col.get_str(i).ok().flatten())
            == Some("DUPIXENT")
    });
    assert!(dupixent_found, "DUPIXENT not found");
}

#[test]
fn test_end_to_end_signal_detection() {
    if !faers_data_available() {
        eprintln!("SKIP: FAERS 2025Q4 data not found at {FAERS_DIR}");
        return;
    }

    let start = Instant::now();
    let df =
        ingest_faers_quarterly_with_options(Path::new(FAERS_DIR), false).expect("Ingest failed");
    let counts = transform_count_drug_events(df).expect("Count failed");
    let filtered = transform_filter_minimum(counts).expect("Filter failed");
    let results = run_signal_detection_pipeline(&filtered).expect("Signal detection failed");
    eprintln!(
        "Pipeline done in {:.2?}: {} results",
        start.elapsed(),
        results.len()
    );

    assert!(!results.is_empty());
    assert_no_nan(&results);

    let signals = filter_signals(&results);
    let rate = signals.len() as f64 / results.len() as f64 * 100.0;
    eprintln!(
        "Signals: {} / {} ({rate:.1}%)",
        signals.len(),
        results.len()
    );
    assert!(
        rate > 0.5 && rate < 90.0,
        "Signal rate {rate:.1}% out of range"
    );

    print_top_signals(&signals);
}

#[test]
fn test_known_signal_pairs() {
    if !faers_data_available() {
        eprintln!("SKIP: FAERS 2025Q4 data not found at {FAERS_DIR}");
        return;
    }

    let df =
        ingest_faers_quarterly_with_options(Path::new(FAERS_DIR), false).expect("Ingest failed");
    let counts = transform_count_drug_events(df).expect("Count failed");
    let filtered = transform_filter_minimum(counts).expect("Filter failed");
    let results = run_signal_detection_pipeline(&filtered).expect("Signal detection failed");

    let known: Vec<(&str, &str, &str)> = vec![
        (
            "DUPIXENT",
            "CONJUNCTIVITIS",
            "Dupilumab conjunctivitis (label)",
        ),
        (
            "LENALIDOMIDE",
            "NEUTROPENIA",
            "Lenalidomide hematox (label)",
        ),
        (
            "LENALIDOMIDE",
            "THROMBOCYTOPENIA",
            "Lenalidomide platelet (label)",
        ),
        ("METHOTREXATE", "HEPATOTOXICITY", "MTX hepatic injury"),
    ];

    let found = check_known_pairs(&results, &known);
    let hit_rate = found as f64 / known.len() as f64;
    assert!(hit_rate >= 0.25, "Too few known: {found}/{}", known.len());
}

#[test]
fn test_pipeline_performance() {
    if !faers_data_available() {
        eprintln!("SKIP: FAERS 2025Q4 data not found at {FAERS_DIR}");
        return;
    }
    let start = Instant::now();
    let df =
        ingest_faers_quarterly_with_options(Path::new(FAERS_DIR), false).expect("Ingest failed");
    let counts = transform_count_drug_events(df).expect("Count failed");
    let filtered = transform_filter_minimum(counts).expect("Filter failed");
    let _r = run_signal_detection_pipeline(&filtered).expect("Signal detection failed");
    let t = start.elapsed();
    eprintln!("TOTAL: {t:.2?}");
    assert!(t.as_secs() < 120, "Too slow: {t:.2?}");
}

// --- Helpers ---

fn assert_no_nan(results: &[SignalDetectionResult]) {
    let nan = results
        .iter()
        .filter(|r| {
            r.prr.point.value().is_nan()
                || r.ror.point.value().is_nan()
                || r.ic.point.value().is_nan()
                || r.ebgm.point.value().is_nan()
        })
        .count();
    assert!(nan == 0, "Found {nan} NaN results");
}

fn print_top_signals(signals: &[&SignalDetectionResult]) {
    let mut sorted: Vec<&SignalDetectionResult> = signals.to_vec();
    sorted.sort_by(|a, b| {
        b.prr
            .point
            .value()
            .partial_cmp(&a.prr.point.value())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    eprintln!("\n--- Top 20 by PRR ---");
    for s in sorted.iter().take(20) {
        let d = s.drug.as_str();
        let e = s.event.as_str();
        let n = s.case_count.value();
        let prr = s.prr.point.value();
        let ror = s.ror.point.value();
        let ic = s.ic.point.value();
        let ebgm = s.ebgm.point.value();
        eprintln!(
            "{d:<25} {e:<30} n={n:>6} PRR={prr:>8.2} ROR={ror:>8.2} IC={ic:>8.2} EBGM={ebgm:>8.2}"
        );
    }
}

fn check_known_pairs(results: &[SignalDetectionResult], known: &[(&str, &str, &str)]) -> usize {
    let mut found = 0;
    for (drug, event_sub, rationale) in known {
        let matching: Vec<&SignalDetectionResult> = results
            .iter()
            .filter(|r| {
                r.drug.as_str() == *drug
                    && r.event.as_str().contains(event_sub)
                    && r.is_any_signal()
            })
            .collect();
        if matching.is_empty() {
            eprintln!("MISS: {drug} + {event_sub} — {rationale}");
        } else {
            let r = matching[0];
            eprintln!(
                "HIT:  {drug} + {} (n={}) — {rationale}",
                r.event, r.case_count
            );
            found += 1;
        }
    }
    found
}
