//! FDA Data Bridge - ETL pipeline for pharmacovigilance intelligence.
//!
//! Provides comprehensive data integration with FDA sources:
//!
//! # Modules
//!
//! - **FAERS ETL** - Quarterly ASCII file ingestion and signal detection
//! - **OpenFDA API** ([`api`]) - Real-time adverse event queries with caching
//! - **NDC Bridge** ([`ndc`]) - National Drug Code directory lookups
//! - **Deduplication** ([`dedup`]) - Report clustering and duplicate removal
//! - **Types** ([`types`]) - T2-P newtypes and T2-C composites (Primitive Codex)
//!
//! # Configuration
//!
//! The ingest stage reads from the `FAERS_DATA_DIR` environment variable.
//! If not set, it will look for FAERS data in the default location:
//! `./data/faers` relative to the current working directory.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod analytics;
pub mod api;
pub mod dedup;
pub mod grounding;
pub mod ndc;
pub mod spatial_bridge;
pub mod types;

use nexcore_dataframe::{Column, Counter, DataFrame, DataFrameError, DataType, Scalar};
use nexcore_error::{Context, Result};
use nexcore_vigilance::pv::faers::parse_quarterly_linked;
use nexcore_vigilance::pv::signals::batch::{
    BatchContingencyTables, CompleteSignalResult, batch_complete_parallel,
};
use nexcore_vigilance::pv::signals::core::newtypes::{Ebgm, Ic, Prr, Ror};
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

pub use analytics::{
    CascadeConfig,
    CaseSeriousness,
    // A78 — Polypharmacy Interaction Signal
    CountrySignal,
    DrugCharacterization,
    GeographicCase,
    GeographicConfig,
    GeographicDivergence,
    MonthBucket,
    OutcomeCase,
    OutcomeConditionedConfig,
    OutcomeConditionedSignal,
    PolypharmacyCase,
    PolypharmacyConfig,
    PolypharmacySignal,
    ReactionOutcome,
    ReporterCase,
    ReporterQualification,
    ReporterWeightedConfig,
    ReporterWeightedSignal,
    SeriousnessCascade,
    SeriousnessCase,
    SeriousnessFlag,
    SignalVelocity,
    TemporalCase,
    VelocityConfig,
    compute_geographic_divergence,
    compute_outcome_conditioned,
    compute_polypharmacy_signals,
    compute_reporter_weighted,
    compute_seriousness_cascade,
    compute_signal_velocity,
};
pub use types::{
    CaseCount, ContingencyBatch, DrugName, DrugRole, EventName, MetricAssessment, RowCount, columns,
};

const DEFAULT_FAERS_DIR: &str = "./data/faers";
const FAERS_DATA_DIR_ENV: &str = "FAERS_DATA_DIR";

fn get_faers_data_dir() -> PathBuf {
    env::var(FAERS_DATA_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_FAERS_DIR))
}

fn format_vec<T: std::fmt::Display>(v: &[T]) -> Option<String> {
    if v.is_empty() {
        return None;
    }
    Some(
        v.iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("|"),
    )
}

// =============================================================================
// INGEST
// =============================================================================

/// Ingest FAERS quarterly files and return a DataFrame of drug-event pairs.
pub fn ingest_faers_quarterly() -> Result<DataFrame> {
    let faers_dir = get_faers_data_dir();
    ingest_faers_quarterly_with_options(&faers_dir, false)
}

/// Ingest FAERS quarterly files with custom options.
pub fn ingest_faers_quarterly_with_options(
    faers_dir: &Path,
    include_all_roles: bool,
) -> Result<DataFrame> {
    tracing::info!(stage = "faers-quarterly", path = %faers_dir.display(), "Starting FAERS ingest");
    let (reports, _parse_errors) = parse_quarterly_linked(faers_dir);
    if reports.is_empty() {
        return Ok(DataFrame::empty());
    }
    let rows = flatten_reports(&reports, include_all_roles);
    build_ingest_dataframe(&reports, &rows)
}

/// A flattened row: (report_index, case_id, drug_name, role, event_name).
type FlatRow = (usize, u64, String, String, String);

/// Flatten all reports into per-(drug, event) rows, filtering by role.
fn flatten_reports(
    reports: &[nexcore_vigilance::pv::faers::LinkedReport],
    include_all_roles: bool,
) -> Vec<FlatRow> {
    let mut rows = Vec::new();
    for (idx, report) in reports.iter().enumerate() {
        let Ok(case_id) = report.primary_id.parse::<u64>() else {
            tracing::warn!(primary_id = %report.primary_id, "Skipping non-numeric ID");
            continue;
        };
        flatten_single_report(&mut rows, idx, case_id, report, include_all_roles);
    }
    rows
}

/// Flatten a single report's drugs × reactions into rows.
fn flatten_single_report(
    rows: &mut Vec<FlatRow>,
    idx: usize,
    case_id: u64,
    report: &nexcore_vigilance::pv::faers::LinkedReport,
    include_all_roles: bool,
) {
    let selected = select_drugs(report, include_all_roles);
    for (drug_name, role, _seq) in selected {
        append_drug_events(rows, idx, case_id, &drug_name, &role, &report.reactions);
    }
}

/// Append one drug × all reactions to the rows vec.
fn append_drug_events(
    rows: &mut Vec<FlatRow>,
    idx: usize,
    case_id: u64,
    drug: &str,
    role: &str,
    reactions: &[String],
) {
    for event in reactions {
        rows.push((
            idx,
            case_id,
            drug.to_uppercase(),
            role.to_string(),
            event.to_uppercase(),
        ));
    }
}

/// Build the full DataFrame from flattened rows.
fn build_ingest_dataframe(
    reports: &[nexcore_vigilance::pv::faers::LinkedReport],
    rows: &[FlatRow],
) -> Result<DataFrame> {
    let n = rows.len();
    let mut acc = IngestColumns::with_capacity(n);

    for (idx, case_id, drug, role, event) in rows {
        let report = &reports[*idx];
        acc.push(report, *case_id, drug.clone(), role.clone(), event.clone());
    }

    acc.into_dataframe()
}

/// Column vectors accumulated during ingest.
struct IngestColumns {
    case_ids: Vec<u64>,
    drugs: Vec<String>,
    events: Vec<String>,
    age_years: Vec<Option<f64>>,
    age_groups: Vec<Option<String>>,
    sexes: Vec<Option<String>>,
    weights: Vec<Option<f64>>,
    reporter_countries: Vec<Option<String>>,
    occr_countries: Vec<Option<String>>,
    mfr_sndrs: Vec<Option<String>>,
    occp_cods: Vec<Option<String>>,
    mfr_nums: Vec<Option<String>>,
    fda_dts: Vec<Option<String>>,
    event_dts: Vec<Option<String>>,
    role_codes: Vec<Option<String>>,
    report_sources: Vec<Option<String>>,
    outcomes: Vec<Option<String>>,
    therapy: Vec<Option<String>>,
}

impl IngestColumns {
    fn with_capacity(n: usize) -> Self {
        Self {
            case_ids: Vec::with_capacity(n),
            drugs: Vec::with_capacity(n),
            events: Vec::with_capacity(n),
            age_years: Vec::with_capacity(n),
            age_groups: Vec::with_capacity(n),
            sexes: Vec::with_capacity(n),
            weights: Vec::with_capacity(n),
            reporter_countries: Vec::with_capacity(n),
            occr_countries: Vec::with_capacity(n),
            mfr_sndrs: Vec::with_capacity(n),
            occp_cods: Vec::with_capacity(n),
            mfr_nums: Vec::with_capacity(n),
            fda_dts: Vec::with_capacity(n),
            event_dts: Vec::with_capacity(n),
            role_codes: Vec::with_capacity(n),
            report_sources: Vec::with_capacity(n),
            outcomes: Vec::with_capacity(n),
            therapy: Vec::with_capacity(n),
        }
    }

    fn push(
        &mut self,
        r: &nexcore_vigilance::pv::faers::LinkedReport,
        case_id: u64,
        drug: String,
        role: String,
        event: String,
    ) {
        self.case_ids.push(case_id);
        self.drugs.push(drug);
        self.events.push(event);
        self.age_years.push(r.age_years);
        self.age_groups.push(r.age_group.clone());
        self.sexes.push(r.sex.clone());
        self.weights.push(r.weight_kg);
        self.reporter_countries.push(r.reporter_country.clone());
        self.occr_countries.push(r.occr_country.clone());
        self.mfr_sndrs.push(r.mfr_sndr.clone());
        self.occp_cods.push(r.occp_cod.clone());
        self.mfr_nums.push(r.mfr_num.clone());
        self.fda_dts.push(r.fda_dt.clone());
        self.event_dts.push(r.event_dt.clone());
        self.role_codes.push(Some(role));
        self.report_sources.push(format_vec(&r.report_sources));
        self.outcomes.push(format_vec(&r.outcomes));
        self.therapy.push(format_therapy(&r.therapy));
    }

    fn into_dataframe(self) -> Result<DataFrame> {
        DataFrame::new(vec![
            Column::from_u64s(columns::CASE_ID, self.case_ids),
            Column::from_strings(columns::DRUG, self.drugs),
            Column::from_strings(columns::EVENT, self.events),
            Column::new_f64(columns::AGE_YEARS, self.age_years),
            Column::new_string(columns::AGE_GROUP, self.age_groups),
            Column::new_string(columns::SEX, self.sexes),
            Column::new_f64(columns::WEIGHT_KG, self.weights),
            Column::new_string(columns::REPORTER_COUNTRY, self.reporter_countries),
            Column::new_string(columns::OCCR_COUNTRY, self.occr_countries),
            Column::new_string(columns::MFR_SNDR, self.mfr_sndrs),
            Column::new_string(columns::OCCP_COD, self.occp_cods),
            Column::new_string(columns::MFR_NUM, self.mfr_nums),
            Column::new_string(columns::FDA_DT, self.fda_dts),
            Column::new_string(columns::EVENT_DT, self.event_dts),
            Column::new_string(columns::ROLE_CODE, self.role_codes),
            Column::new_string(columns::REPORT_SOURCES, self.report_sources),
            Column::new_string(columns::OUTCOMES, self.outcomes),
            Column::new_string(columns::THERAPY_SUMMARY, self.therapy),
        ])
        .context("Failed to create high-resolution DataFrame")
    }
}

/// Format therapy entries into a pipe-separated string.
fn format_therapy(therapy: &[(Option<String>, Option<String>, u32)]) -> Option<String> {
    if therapy.is_empty() {
        return None;
    }
    let parts: Vec<String> = therapy
        .iter()
        .map(|(s, e, seq)| format_therapy_entry(s, e, *seq))
        .collect();
    Some(parts.join("||"))
}

/// Format a single therapy entry.
fn format_therapy_entry(start: &Option<String>, end: &Option<String>, seq: u32) -> String {
    format!(
        "{}|{}|{}",
        start.as_deref().unwrap_or(""),
        end.as_deref().unwrap_or(""),
        seq
    )
}

/// Select drugs from a report based on role filter.
fn select_drugs(
    report: &nexcore_vigilance::pv::faers::LinkedReport,
    include_all_roles: bool,
) -> Vec<(String, String, u32)> {
    if include_all_roles {
        return report.drugs.clone();
    }
    report
        .drugs
        .iter()
        .filter(|(_, role, _)| DrugRole::from(role.as_str()).is_suspect())
        .cloned()
        .collect()
}

// =============================================================================
// TRANSFORMS
// =============================================================================

/// Transform: normalize drug and event names (no-op, already uppercased).
pub fn transform_normalize_names(df: DataFrame) -> Result<DataFrame> {
    Ok(df)
}

/// Transform: aggregate drug-event pairs into counts.
pub fn transform_count_drug_events(df: DataFrame) -> Result<DataFrame> {
    transform_count_drug_events_stratified(df, vec![])
}

/// Transform: aggregate with stratification.
pub fn transform_count_drug_events_stratified(
    df: DataFrame,
    strata: Vec<&str>,
) -> Result<DataFrame> {
    let mut group_cols: Vec<&str> = vec![columns::DRUG, columns::EVENT];
    group_cols.extend(strata);

    let counter =
        Counter::from_dataframe(&df, &group_cols).context("Failed to count drug-event pairs")?;
    let counted = counter
        .into_dataframe()
        .context("Failed to build count DataFrame")?;
    // Counter produces "count" column — rename to match pipeline convention "n"
    counted
        .rename_column("count", columns::N)
        .context("Failed to rename count column")
}

/// Transform: filter to minimum case count (default 3).
pub fn transform_filter_minimum(df: DataFrame) -> Result<DataFrame> {
    transform_filter_minimum_n(df, 3)
}

/// Transform: filter to minimum case count with custom threshold.
pub fn transform_filter_minimum_n(df: DataFrame, min_cases: i64) -> Result<DataFrame> {
    let threshold = if min_cases < 0 {
        0u64
    } else {
        min_cases as u64
    };
    df.filter_by(columns::N, |v| v.as_u64().is_some_and(|n| n >= threshold))
        .context("Failed to filter by minimum count")
}

// =============================================================================
// SINKS
// =============================================================================

/// Sink: Write DataFrame to JSON file (default path).
pub fn sink_output(df: DataFrame) -> Result<RowCount> {
    sink_output_to(df, "output/drug_event_counts.json")
}

/// Sink: Write DataFrame to specified JSON file path.
pub fn sink_output_to(df: DataFrame, path_template: &str) -> Result<RowCount> {
    let row_count = RowCount(df.height() as u64);
    if row_count.value() == 0 {
        return Ok(RowCount(0));
    }
    let path = nexcore_chrono::DateTime::now()
        .format(path_template)
        .unwrap_or_default();
    write_json(&df, &path)?;
    Ok(row_count)
}

/// Sink: Write signal detection results to JSON.
pub fn sink_signals(results: &[SignalDetectionResult], path: &Path) -> Result<RowCount> {
    let df = signals_to_dataframe(results)?;
    let row_count = RowCount(df.height() as u64);
    if row_count.value() == 0 {
        return Ok(RowCount(0));
    }
    write_json(&df, &path.display().to_string())?;
    Ok(row_count)
}

/// Shared JSON writer — creates parent dirs, writes row-oriented JSON.
fn write_json(df: &DataFrame, path: &str) -> Result<()> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create dir: {}", parent.display()))?;
        }
    }
    df.to_json_file(p)
        .with_context(|| format!("Failed to write: {path}"))
}

// =============================================================================
// SIGNAL DETECTION
// =============================================================================

/// Tier: T3 — Signal detection result for a drug-event pair.
#[derive(Debug, Clone)]
pub struct SignalDetectionResult {
    /// Drug name (T2-P)
    pub drug: DrugName,
    /// Event name / MedDRA PT (T2-P)
    pub event: EventName,
    /// Co-occurrence count — cell "a" (T2-P)
    pub case_count: CaseCount,
    /// PRR assessment (T2-C)
    pub prr: MetricAssessment<Prr>,
    /// ROR assessment (T2-C)
    pub ror: MetricAssessment<Ror>,
    /// IC assessment (T2-C)
    pub ic: MetricAssessment<Ic>,
    /// EBGM assessment (T2-C)
    pub ebgm: MetricAssessment<Ebgm>,
}

impl SignalDetectionResult {
    /// Returns true if any algorithm flagged this pair as a signal.
    #[must_use]
    pub fn is_any_signal(&self) -> bool {
        self.prr.is_signal || self.ror.is_signal || self.ic.is_signal || self.ebgm.is_signal
    }
}

/// Build contingency tables from aggregated drug-event counts.
pub fn build_contingency_tables_from_counts(df: &DataFrame) -> Result<ContingencyBatch> {
    let drugs = extract_str_column(df, columns::DRUG)?;
    let events = extract_str_column(df, columns::EVENT)?;
    let counts = extract_counts_column(df)?;
    let (dt, et, total) = compute_marginal_totals(drugs, events, &counts, df.height());
    Ok(build_tables_from_marginals(
        drugs,
        events,
        &counts,
        &dt,
        &et,
        total,
        df.height(),
    ))
}

fn extract_str_column<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Column> {
    let col = df
        .column(name)
        .with_context(|| format!("Missing '{name}'"))?;
    if col.dtype() != DataType::Utf8 {
        nexcore_error::bail!("'{name}' is not a string column");
    }
    Ok(col)
}

fn extract_counts_column(df: &DataFrame) -> Result<Vec<u64>> {
    let n_col = df.column(columns::N).context("Missing 'n'")?;
    let mut counts = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let val = match n_col.get(i) {
            Some(Scalar::UInt64(n)) => n,
            Some(Scalar::Int64(n)) => n as u64,
            Some(Scalar::Float64(n)) => n as u64,
            _ => 0,
        };
        counts.push(val);
    }
    Ok(counts)
}

fn compute_marginal_totals(
    drugs: &Column,
    events: &Column,
    counts: &[u64],
    n: usize,
) -> (HashMap<String, u64>, HashMap<String, u64>, u64) {
    let mut dt: HashMap<String, u64> = HashMap::new();
    let mut et: HashMap<String, u64> = HashMap::new();
    let mut total: u64 = 0;
    for i in 0..n {
        let d = drugs
            .get_str(i)
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_string();
        let e = events
            .get_str(i)
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_string();
        *dt.entry(d).or_insert(0) += counts[i];
        *et.entry(e).or_insert(0) += counts[i];
        total += counts[i];
    }
    (dt, et, total)
}

fn build_tables_from_marginals(
    drugs: &Column,
    events: &Column,
    counts: &[u64],
    dt: &HashMap<String, u64>,
    et: &HashMap<String, u64>,
    total: u64,
    n: usize,
) -> ContingencyBatch {
    let mut dn = Vec::with_capacity(n);
    let mut en = Vec::with_capacity(n);
    let mut av = Vec::with_capacity(n);
    let mut bv = Vec::with_capacity(n);
    let mut cv = Vec::with_capacity(n);
    let mut dv = Vec::with_capacity(n);

    for i in 0..n {
        let d = drugs
            .get_str(i)
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_string();
        let e = events
            .get_str(i)
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_string();
        let a = counts[i];
        let d_tot = *dt.get(&d).unwrap_or(&0);
        let e_tot = *et.get(&e).unwrap_or(&0);
        av.push(a);
        bv.push(d_tot.saturating_sub(a));
        cv.push(e_tot.saturating_sub(a));
        dv.push(total.saturating_sub(d_tot + e_tot - a));
        dn.push(DrugName(d));
        en.push(EventName(e));
    }

    ContingencyBatch {
        drugs: dn,
        events: en,
        tables: BatchContingencyTables::new(av, bv, cv, dv),
    }
}

/// Run signal detection on a [`ContingencyBatch`].
pub fn run_signal_detection(batch: &ContingencyBatch) -> Result<Vec<SignalDetectionResult>> {
    let complete = batch_complete_parallel(&batch.tables);
    let results: Vec<SignalDetectionResult> = (0..batch.tables.len())
        .into_par_iter()
        .map(|i| map_complete_to_result(batch, &complete[i], i))
        .collect();
    Ok(results)
}

fn map_complete_to_result(
    batch: &ContingencyBatch,
    r: &CompleteSignalResult,
    i: usize,
) -> SignalDetectionResult {
    SignalDetectionResult {
        drug: batch.drugs[i].clone(),
        event: batch.events[i].clone(),
        case_count: CaseCount(batch.tables.a[i]),
        prr: make_assessment(Prr::new_unchecked(r.prr.point_estimate), &r.prr),
        ror: make_assessment(Ror::new_unchecked(r.ror.point_estimate), &r.ror),
        ic: make_assessment(Ic::new_unchecked(r.ic.point_estimate), &r.ic),
        ebgm: make_assessment(Ebgm::new_unchecked(r.ebgm.point_estimate), &r.ebgm),
    }
}

fn make_assessment<M>(
    point: M,
    br: &nexcore_vigilance::pv::signals::batch::BatchResult,
) -> MetricAssessment<M> {
    MetricAssessment {
        point,
        lower_ci: br.lower_ci,
        upper_ci: Some(br.upper_ci),
        is_signal: br.is_signal,
    }
}

/// Filter results to only include detected signals.
pub fn filter_signals(results: &[SignalDetectionResult]) -> Vec<&SignalDetectionResult> {
    results.iter().filter(|r| r.is_any_signal()).collect()
}

/// Convert signal detection results to DataFrame.
pub fn signals_to_dataframe(results: &[SignalDetectionResult]) -> Result<DataFrame> {
    DataFrame::new(vec![
        Column::from_strings(
            columns::DRUG,
            results.iter().map(|r| r.drug.0.clone()).collect(),
        ),
        Column::from_strings(
            columns::EVENT,
            results.iter().map(|r| r.event.0.clone()).collect(),
        ),
        Column::from_u64s(
            columns::N,
            results.iter().map(|r| r.case_count.value()).collect(),
        ),
        Column::from_f64s(
            columns::PRR,
            results.iter().map(|r| r.prr.point.value()).collect(),
        ),
        Column::from_f64s(
            columns::PRR_LOWER_CI,
            results.iter().map(|r| r.prr.lower_ci).collect(),
        ),
        Column::from_f64s(
            columns::PRR_UPPER_CI,
            results
                .iter()
                .map(|r| r.prr.upper_ci.unwrap_or(0.0))
                .collect(),
        ),
        Column::from_bools(
            columns::PRR_SIGNAL,
            results.iter().map(|r| r.prr.is_signal).collect(),
        ),
        Column::from_f64s(
            columns::ROR,
            results.iter().map(|r| r.ror.point.value()).collect(),
        ),
        Column::from_f64s(
            columns::ROR_LOWER_CI,
            results.iter().map(|r| r.ror.lower_ci).collect(),
        ),
        Column::from_bools(
            columns::ROR_SIGNAL,
            results.iter().map(|r| r.ror.is_signal).collect(),
        ),
        Column::from_f64s(
            columns::IC,
            results.iter().map(|r| r.ic.point.value()).collect(),
        ),
        Column::from_f64s(
            columns::IC025,
            results.iter().map(|r| r.ic.lower_ci).collect(),
        ),
        Column::from_bools(
            columns::IC_SIGNAL,
            results.iter().map(|r| r.ic.is_signal).collect(),
        ),
        Column::from_f64s(
            columns::EBGM,
            results.iter().map(|r| r.ebgm.point.value()).collect(),
        ),
        Column::from_f64s(
            columns::EB05,
            results.iter().map(|r| r.ebgm.lower_ci).collect(),
        ),
        Column::from_bools(
            columns::EBGM_SIGNAL,
            results.iter().map(|r| r.ebgm.is_signal).collect(),
        ),
    ])
    .context("Failed to create signal results DataFrame")
}

/// Run complete signal detection pipeline on aggregated counts.
pub fn run_signal_detection_pipeline(counts_df: &DataFrame) -> Result<Vec<SignalDetectionResult>> {
    let batch = build_contingency_tables_from_counts(counts_df)?;
    run_signal_detection(&batch)
}

/// End-to-end pipeline result.
pub struct PipelineOutput {
    /// All signal detection results
    pub results: Vec<SignalDetectionResult>,
    /// Total drug-event pairs evaluated
    pub total_pairs: usize,
}

/// Run the full ETL pipeline: ingest → normalize → count → filter → detect.
///
/// This is the main entry point for callers that don't need intermediate DataFrames.
pub fn run_full_pipeline(
    faers_dir: &Path,
    include_all_roles: bool,
    min_cases: i64,
) -> Result<PipelineOutput> {
    let df = ingest_faers_quarterly_with_options(faers_dir, include_all_roles)?;
    if df.height() == 0 {
        return Ok(PipelineOutput {
            results: Vec::new(),
            total_pairs: 0,
        });
    }

    let df = transform_normalize_names(df)?;
    let df = transform_count_drug_events(df)?;
    let df = transform_filter_minimum_n(df, min_cases)?;
    let total_pairs = df.height();
    let results = run_signal_detection_pipeline(&df)?;

    Ok(PipelineOutput {
        results,
        total_pairs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_signal(drug: &str, is_prr: bool) -> SignalDetectionResult {
        let pv = if is_prr { 3.0 } else { 1.0 };
        let lo = if is_prr { 2.0 } else { 0.5 };
        let zero_ror = MetricAssessment {
            point: Ror::new_unchecked(0.0),
            lower_ci: 0.0,
            upper_ci: Some(0.0),
            is_signal: false,
        };
        let zero_ic = MetricAssessment {
            point: Ic::new_unchecked(0.0),
            lower_ci: 0.0,
            upper_ci: Some(0.0),
            is_signal: false,
        };
        let zero_ebgm = MetricAssessment {
            point: Ebgm::new_unchecked(0.0),
            lower_ci: 0.0,
            upper_ci: Some(0.0),
            is_signal: false,
        };
        SignalDetectionResult {
            drug: DrugName(drug.to_string()),
            event: EventName("EVENT".to_string()),
            case_count: CaseCount(10),
            prr: MetricAssessment {
                point: Prr::new_unchecked(pv),
                lower_ci: lo,
                upper_ci: Some(4.0),
                is_signal: is_prr,
            },
            ror: zero_ror,
            ic: zero_ic,
            ebgm: zero_ebgm,
        }
    }

    #[test]
    fn test_count_drug_events() {
        let df = DataFrame::new(vec![
            Column::from_u64s(columns::CASE_ID, vec![1, 2, 3, 4]),
            Column::from_strs(columns::DRUG, &["ASP", "ASP", "ASP", "MET"]),
            Column::from_strs(columns::EVENT, &["HA", "HA", "HA", "NA"]),
        ])
        .unwrap_or_else(|e| panic!("{e}"));

        let c = transform_count_drug_events(df).unwrap_or_else(|e| panic!("{e}"));

        // ASP+HA should have count 3 — find the row
        let mut found = false;
        for i in 0..c.height() {
            let d = c
                .column(columns::DRUG)
                .ok()
                .and_then(|col| col.get_str(i).ok().flatten().map(|s| s.to_string()));
            let e = c
                .column(columns::EVENT)
                .ok()
                .and_then(|col| col.get_str(i).ok().flatten().map(|s| s.to_string()));
            if d.as_deref() == Some("ASP") && e.as_deref() == Some("HA") {
                found = true;
            }
        }
        assert!(found, "ASP+HA pair not found in counted DataFrame");
    }

    #[test]
    fn test_filter_minimum() {
        let df = DataFrame::new(vec![
            Column::from_strs(columns::DRUG, &["A", "B"]),
            Column::from_strs(columns::EVENT, &["X", "Y"]),
            Column::from_u64s(columns::N, vec![5, 2]),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
        let c = transform_filter_minimum(df).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(c.height(), 1);
    }

    #[test]
    fn test_contingency_tables() {
        let df = DataFrame::new(vec![
            Column::from_strs(columns::DRUG, &["DA", "DA", "DB"]),
            Column::from_strs(columns::EVENT, &["EX", "EY", "EX"]),
            Column::from_u64s(columns::N, vec![10, 5, 8]),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
        let b = build_contingency_tables_from_counts(&df).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(b.drugs.len(), 3);
        assert_eq!(b.tables.a[0], 10);
        assert_eq!(b.tables.b[0], 5);
        assert_eq!(b.tables.c[0], 8);
        assert_eq!(b.tables.d[0], 0);
    }

    #[test]
    fn test_pipeline_end_to_end() {
        let df = DataFrame::new(vec![
            Column::from_strs(columns::DRUG, &["A", "A", "B", "B", "C", "C"]),
            Column::from_strs(columns::EVENT, &["X", "Y", "X", "Y", "X", "Y"]),
            Column::from_u64s(columns::N, vec![50, 5, 10, 100, 20, 500]),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
        let r = run_signal_detection_pipeline(&df).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(r.len(), 6);
        let ax = r
            .iter()
            .find(|r| r.drug.as_str() == "A" && r.event.as_str() == "X");
        assert!(ax.is_some());
        assert_eq!(
            ax.unwrap_or_else(|| panic!("missing")).case_count.value(),
            50
        );
    }

    #[test]
    fn test_to_dataframe() {
        let r = test_signal("D", true);
        let df = signals_to_dataframe(&[r]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 16);
    }

    #[test]
    fn test_filter_signals() {
        let r = vec![test_signal("SIG", true), test_signal("NO", false)];
        let s = filter_signals(&r);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].drug.as_str(), "SIG");
    }
}
