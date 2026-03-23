# nexcore-faers-etl

The FDA Data Bridge and ETL pipeline for the NexVigilant platform. This crate provides the heavy-lifting required to ingest large-scale quarterly FAERS (FDA Adverse Event Reporting System) ASCII files, transform them into high-resolution DataFrames, and execute batch signal detection.

## Intent
To automate the ingestion and analysis of bulk regulatory data. It bridges the gap between raw, messy ASCII files and structured, queryable intelligence, enabling high-performance signal detection across millions of reports using Polars and Rayon.

## T1 Grounding (Lex Primitiva)
Dominant Primitives:
- **μ (Mapping)**: The primary primitive for mapping raw ASCII rows to structured `Icsr` and `DrugEventPair` types.
- **σ (Sequence)**: Manages the ETL pipeline sequence: Ingest → Normalize → Transform → Filter → Detect.
- **Σ (Sum)**: Aggregates millions of individual reports into stratified contingency tables.
- **N (Quantity)**: Computes population-scale counts and numeric signal metrics (PRR, ROR, etc.).
- **π (Persistence)**: Manages the long-term storage of transformed data in Parquet format.

## Core Modules
- **Ingest**: Fast parser for quarterly ASCII files with linked report support.
- **Transform**: Polars-based logic for normalization, stratification, and aggregation.
- **Signals**: Batch implementation of detection algorithms (PRR, ROR, IC, EBGM).
- **Deduplication**: Heuristics for clustering and removing duplicate reports.
- **NDC Bridge**: National Drug Code directory mapping for standardized drug identification.

## SOPs for Use
### Running the Full Pipeline
```rust
use nexcore_faers_etl::run_full_pipeline;
use std::path::Path;

let output = run_full_pipeline(Path::new("./data/faers"), false, 3)?;
println!("Evaluated {} drug-event pairs.", output.total_pairs);
```

### STRATIFIED Aggregation
Use `transform_count_drug_events_stratified` to aggregate data by specific cohorts (e.g., Age, Gender, Country).

## License
Proprietary. Copyright (c) 2026 NexVigilant LLC. All Rights Reserved.
