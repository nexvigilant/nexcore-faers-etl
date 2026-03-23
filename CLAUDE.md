# AI Guidance — nexcore-faers-etl

FDA Data Bridge and large-scale ETL pipeline.

## Use When
- Ingesting quarterly bulk data files from the FDA (ASCII/FAERS).
- Performing population-scale signal detection (millions of records).
- Stratifying safety signals by demographic or geographic factors.
- Normalizing drug or event names against large-scale reference sets.
- Generating JSON outputs for downstream analysis.

## Architecture (Post-Directive 006A)

**DataFrame engine:** `nexcore-dataframe` (sovereign, zero external deps). NO polars.

| Operation | API |
|-----------|-----|
| Count drug-event pairs | `Counter::from_dataframe(&df, &["drug", "event"]).into_dataframe()` |
| Filter by threshold | `df.filter_by("n", \|v\| v.as_u64().is_some_and(\|n\| n >= min))` |
| Create columns | `Column::from_u64s`, `Column::from_strings`, `Column::new_f64`, `Column::new_string` |
| Aggregate | `df.group_by(&[col]).agg(&[Agg::Sum("n".into())])` |
| Sort + head | `df.sort("n", true)?.head(15)` |
| Statistics | `col.min()`, `col.max()`, `col.mean()` return `Scalar`; `col.quantile(q)` returns `Result<Scalar>` |
| Output | `df.to_json_file(path)` — JSON via serde_json |

## Grounding Patterns
- **DataFrame Priority**: Use `nexcore-dataframe` DataFrame API directly (no lazy evaluation).
- **Counter Pattern**: Use `Counter` for counting aggregations — semantically superior to generic group_by.
- **Batch Processing**: Use the `rayon`-enabled batch functions in the `signals` module for O(N) detection across large contingency sets.
- **T1 Primitives**:
  - `μ + Σ`: Root primitives for mapping and aggregating bulk data.
  - `σ + π`: Root primitives for sequential pipeline flow and durable storage.

## Maintenance SOPs
- **JSON Output**: All sinks produce JSON (not Parquet). Functions: `sink_output`, `sink_output_to`, `sink_signals`.
- **Role Filtering**: Suspect drugs (Primary/Secondary) are the default; only include "Concomitant" drugs if `include_all_roles` is explicitly requested.
- **Memory Management**: When processing files >10GB, ensure the `FAERS_DATA_DIR` is on a fast SSD and monitor RSS usage during the aggregation phase.
- **No polars**: polars was removed (Directive 006A, 2026-02-24) due to transitive CVEs. Do NOT re-introduce it.

## Key Entry Points
- `src/lib.rs`: The main `run_full_pipeline` coordinator.
- `src/bin/faers_pipeline.rs`: CLI binary with summary statistics.
- `src/analytics.rs`: Specialized algorithms (Velocity, Cascade, Polypharmacy).
- `src/api.rs`: Real-time bridge to the OpenFDA API.

## Dependencies
- `nexcore-dataframe` — sovereign columnar DataFrame engine
- `nexcore-vigilance` — signal detection algorithms (PRR/ROR/IC/EBGM)
- `nexcore-lex-primitiva` — T1 primitive grounding
- `stem-math` — mathematical primitives
- `duckdb` — local FAERS storage
- `reqwest` — OpenFDA HTTP client
