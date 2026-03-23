use clap::Parser;
use nexcore_dataframe::{Agg, Column, DataFrame, Scalar};
use nexcore_error::{Context, Result};
use nexcore_faers_etl::{RowCount, SignalDetectionResult, columns};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "faers-pipeline")]
#[command(about = "Run FAERS ETL -> counts -> signal detection")]
struct Args {
    #[arg(long)]
    faers_dir: PathBuf,
    #[arg(long, default_value = "output/faers_counts.json")]
    counts_out: PathBuf,
    #[arg(long, default_value = "output/faers_signals.json")]
    signals_out: PathBuf,
    #[arg(long, default_value = "3")]
    min_cases: i64,
    #[arg(long)]
    include_all_roles: bool,
    #[arg(long)]
    no_summary: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let (counts, counts_written) = run_etl_phase(&args)?;
    if !args.no_summary {
        print_summary(&counts)?;
    }
    run_signal_phase(&args, &counts)?;
    println!("counts_rows: {counts_written}");
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if !args.faers_dir.exists() {
        nexcore_error::bail!("FAERS directory not found: {}", args.faers_dir.display());
    }
    Ok(())
}

fn run_etl_phase(args: &Args) -> Result<(DataFrame, RowCount)> {
    let raw = nexcore_faers_etl::ingest_faers_quarterly_with_options(
        &args.faers_dir,
        args.include_all_roles,
    )
    .context("Ingest failed")?;

    let df = nexcore_faers_etl::transform_normalize_names(raw)?;
    let df = nexcore_faers_etl::transform_count_drug_events(df)?;
    let df = nexcore_faers_etl::transform_filter_minimum_n(df, args.min_cases)?;

    let out_str = args.counts_out.display().to_string();
    let written = nexcore_faers_etl::sink_output_to(df.clone(), &out_str)?;
    println!("counts_json: {}", args.counts_out.display());
    Ok((df, written))
}

fn run_signal_phase(args: &Args, counts: &DataFrame) -> Result<()> {
    let signals = nexcore_faers_etl::run_signal_detection_pipeline(counts)?;
    let written = nexcore_faers_etl::sink_signals(&signals, &args.signals_out)?;
    print_signal_stats(&signals, &args.signals_out, written);
    Ok(())
}

fn print_signal_stats(signals: &[SignalDetectionResult], path: &PathBuf, written: RowCount) {
    let prr = signals.iter().filter(|r| r.prr.is_signal).count();
    let ror = signals.iter().filter(|r| r.ror.is_signal).count();
    let ic = signals.iter().filter(|r| r.ic.is_signal).count();
    let ebgm = signals.iter().filter(|r| r.ebgm.is_signal).count();
    println!("signals_json: {}", path.display());
    println!("signals_total: {}", signals.len());
    println!("signals_prr: {prr}");
    println!("signals_ror: {ror}");
    println!("signals_ic: {ic}");
    println!("signals_ebgm: {ebgm}");
    println!("signals_rows_written: {written}");
}

fn print_summary(counts: &DataFrame) -> Result<()> {
    print_basic_info(counts);
    print_n_stats(counts)?;
    print_distinct_counts(counts)?;
    print_top_pairs(counts)?;
    print_top_by_column(counts, columns::DRUG, "drugs")?;
    print_top_by_column(counts, columns::EVENT, "events")?;
    Ok(())
}

fn print_basic_info(counts: &DataFrame) {
    println!("counts_columns: {:?}", counts.column_names());
    println!("counts_rows: {}", counts.height());
}

fn print_n_stats(counts: &DataFrame) -> Result<()> {
    let n_col = counts.column(columns::N).context("Missing 'n'")?;
    let min = n_col.min();
    let max = n_col.max();
    let mean = n_col.mean();
    let p50 = n_col.quantile(0.50).unwrap_or(Scalar::Null);
    let p90 = n_col.quantile(0.90).unwrap_or(Scalar::Null);
    let p99 = n_col.quantile(0.99).unwrap_or(Scalar::Null);
    println!("\nfield:n stats:");
    println!("  min={min} max={max} mean={mean} p50={p50} p90={p90} p99={p99}");
    Ok(())
}

fn print_distinct_counts(counts: &DataFrame) -> Result<()> {
    let drug_col = counts.column(columns::DRUG).context("Missing 'drug'")?;
    let event_col = counts.column(columns::EVENT).context("Missing 'event'")?;
    println!(
        "\ndistinct: drugs={} events={}",
        drug_col.n_unique(),
        event_col.n_unique()
    );
    Ok(())
}

fn print_top_pairs(counts: &DataFrame) -> Result<()> {
    let sorted = counts
        .sort(columns::N, true)
        .context("Sort failed")?
        .head(15);
    println!("\nTop 15 pairs by n:");
    for i in 0..sorted.height() {
        let drug = sorted
            .column(columns::DRUG)
            .ok()
            .and_then(|c| c.get_str(i).ok().flatten())
            .unwrap_or("?");
        let event = sorted
            .column(columns::EVENT)
            .ok()
            .and_then(|c| c.get_str(i).ok().flatten())
            .unwrap_or("?");
        let n = sorted
            .column(columns::N)
            .ok()
            .and_then(|c| c.get(i))
            .map_or("?".to_string(), |v| v.to_string());
        println!("  {drug:<30} {event:<30} n={n}");
    }
    Ok(())
}

fn print_top_by_column(counts: &DataFrame, col_name: &str, label: &str) -> Result<()> {
    let grouped = counts
        .group_by(&[col_name])
        .context("GroupBy failed")?
        .agg(&[Agg::Sum(columns::N.into())])
        .context("Agg failed")?;
    // Find the sum column name — group_by + Sum produces "{col}_sum"
    let sum_col_name = format!("{}_sum", columns::N);
    let sorted = grouped
        .sort(&sum_col_name, true)
        .context("Sort failed")?
        .head(15);
    println!("\nTop 15 {label} by total_n:");
    for i in 0..sorted.height() {
        let name = sorted
            .column(col_name)
            .ok()
            .and_then(|c| c.get_str(i).ok().flatten())
            .unwrap_or("?");
        let total = sorted
            .column(&sum_col_name)
            .ok()
            .and_then(|c| c.get(i))
            .map_or("?".to_string(), |v| v.to_string());
        println!("  {name:<30} total_n={total}");
    }
    Ok(())
}
