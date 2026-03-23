//! FAERS ETL Pipeline CLI
//!
//! Stream FAERS quarterly ASCII files and generate drug-event contingency
//! tables for signal detection.
//!
//! # Usage
//!
//! ```bash
//! # Set FAERS data directory
//! export FAERS_DATA_DIR=/path/to/faers/2024q4
//!
//! # Run pipeline
//! nexcore-faers-etl
//!
//! # Dry run (validate configuration only)

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
//! nexcore-faers-etl --dry-run
//!
//! # Verbose output
//! nexcore-faers-etl -vvv
//! ```

use clap::Parser;
use nexcore_error::{Context, Result};
#[allow(unused_imports)]
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use nexcore_faers_etl::*;

/// Pipeline CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "nexcore-faers-etl")]
#[command(version = "0.1.0")]
#[command(
    about = "Stream FAERS quarterly ASCII files and generate drug-event contingency tables for signal detection"
)]
struct Args {
    /// Run once and exit (default behavior).
    #[arg(long)]
    once: bool,

    /// Dry run - validate configuration only.
    #[arg(long)]
    dry_run: bool,

    /// Verbose output (-v, -vv, -vvv).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Output path for JSON file.
    #[arg(long, default_value = "output/drug_event_counts.json")]
    output: String,

    /// Minimum case count threshold (default: 3 per Evans criteria).
    #[arg(long, default_value = "3")]
    min_cases: i64,
}

/// Pipeline execution statistics.
#[derive(Debug, Default)]
struct PipelineStats {
    /// Records processed during ingest
    records_processed: u64,
    /// Records written to output
    records_written: u64,
    /// Total pipeline duration in seconds
    duration_secs: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Configure logging level based on verbosity
    let level = match args.verbose {
        0 => tracing::Level::INFO,
        1 => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .with(tracing_subscriber::filter::LevelFilter::from_level(level))
        .init();

    info!(
        pipeline = "nexcore-faers-etl",
        version = "0.1.0",
        "Starting pipeline"
    );

    if args.dry_run {
        info!("Dry run mode - validation only");
        // In dry run mode, just verify the FAERS directory exists
        let faers_dir =
            std::env::var("FAERS_DATA_DIR").unwrap_or_else(|_| "./data/faers".to_string());
        let path = std::path::Path::new(&faers_dir);
        if path.exists() {
            info!(path = %path.display(), "FAERS data directory exists");
        } else {
            warn!(path = %path.display(), "FAERS data directory does not exist");
        }
        return Ok(());
    }

    let result = run_pipeline(&args).await;

    match result {
        Ok(stats) => {
            info!(
                records_processed = stats.records_processed,
                records_written = stats.records_written,
                duration_secs = stats.duration_secs,
                "Pipeline completed successfully"
            );
            Ok(())
        }
        Err(e) => {
            error!(error = %e, "Pipeline failed");
            Err(e)
        }
    }
}

/// Run the full ETL pipeline.
#[instrument]
async fn run_pipeline(args: &Args) -> Result<PipelineStats> {
    let start = std::time::Instant::now();
    let mut stats = PipelineStats::default();

    // Stage 1: Ingest
    info!(
        stage = "faers-quarterly",
        source_type = "FileWatch",
        "Running ingest"
    );

    let df = ingest_faers_quarterly().context("Ingest failed: faers-quarterly")?;

    stats.records_processed = df.height() as u64;
    info!(
        stage = "faers-quarterly",
        records = stats.records_processed,
        "Ingest complete"
    );

    // Stage 2: Normalize names
    info!(
        stage = "normalize-names",
        operation = "Map",
        "Running transform"
    );
    let df = transform_normalize_names(df).context("Transform failed: normalize-names")?;

    // Stage 3: Count drug-event pairs
    info!(
        stage = "count-drug-events",
        operation = "Aggregate",
        "Running transform"
    );
    let df = transform_count_drug_events(df).context("Transform failed: count-drug-events")?;

    // Stage 4: Filter by minimum case count
    info!(
        stage = "filter-minimum",
        operation = "Filter",
        min_cases = args.min_cases,
        "Running transform"
    );
    let df = transform_filter_minimum_n(df, args.min_cases)
        .context("Transform failed: filter-minimum")?;

    // Stage 5: Sink to JSON
    info!(
        stage = "json-output",
        sink_type = "Json",
        path = %args.output,
        "Writing to JSON"
    );
    let written = sink_output_to(df, &args.output).context("JSON sink failed: json-output")?;

    stats.records_written = written.value();
    stats.duration_secs = start.elapsed().as_secs_f64();

    Ok(stats)
}
