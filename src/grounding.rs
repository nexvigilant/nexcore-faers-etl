//! # Lex Primitiva Grounding for nexcore-faers-etl
//!
//! GroundsTo implementations for all nexcore-faers-etl public types.
//! This crate is the FDA FAERS ETL pipeline: ingest, transform, signal detect,
//! with advanced analytics (A77, A80, A82), deduplication, NDC bridge, and OpenFDA API.
//!
//! ## Type Grounding Table
//!
//! | Type | Primitives | Dominant | Tier | Rationale |
//! |------|-----------|----------|------|-----------|
//! | DrugName | ∃ μ | ∃ | T2-P | Normalized drug identity via uppercase mapping |
//! | EventName | ∃ μ | ∃ | T2-P | Normalized event identity via uppercase mapping |
//! | CaseCount | N | N | T1 | Pure numeric count |
//! | RowCount | N | N | T1 | Pure numeric count |
//! | DrugRole | Σ | Σ | T1 | Sum enum of FAERS role codes |
//! | MetricAssessment | N ∂ κ ∃ | ∂ | T2-C | Boundary test: is_signal via threshold comparison |
//! | ContingencyBatch | N σ × | N | T2-P | Batch of numeric contingency tables |
//! | SignalDetectionResult | ∃ ∂ N κ × | ∂ | T2-C | Signal existence via boundary tests across 4 metrics |
//! | PipelineOutput | σ N | σ | T2-P | Sequential pipeline output with count |
//! | ReactionOutcome | Σ κ ∝ | Σ | T2-P | Ordered outcome with irreversibility semantics |
//! | OutcomeConditionedSignal | N ∂ κ ∝ → | ∂ | T2-C | Outcome-conditioned signal with boundary test |
//! | OutcomeCase | × ∃ | × | T2-P | Product of drug/event/outcome |
//! | OutcomeConditionedConfig | ς N ∂ | ς | T2-P | Configuration state with thresholds |
//! | MonthBucket | ∃ ν | ∃ | T2-P | Temporal identity with frequency semantics |
//! | SignalVelocity | N ν σ ∂ | ν | T2-C | Temporal rate of signal change |
//! | TemporalCase | × ∃ ν | × | T2-P | Product of drug/event/time |
//! | VelocityConfig | ς N | ς | T2-P | Configuration state |
//! | SeriousnessFlag | Σ ∂ ∝ | Σ | T2-P | Sum enum of seriousness flags with boundary |
//! | CaseSeriousness | × ∃ ∂ | × | T2-P | Product of case/seriousness |
//! | SeriousnessCase | × ∃ ∂ | × | T2-P | Product of drug/event/seriousness |
//! | SeriousnessCascade | ∂ ∝ N κ → | ∂ | T2-C | Cascade scoring with irreversibility |
//! | CascadeConfig | ς N ∂ | ς | T2-P | Configuration state with thresholds |
//! | DrugCharacterization | Σ | Σ | T1 | Sum enum of drug characterization codes |
//! | PolypharmacyCase | × ∃ σ | × | T2-P | Product with drug list sequence |
//! | PolypharmacySignal | N ∂ κ × | ∂ | T2-C | Interaction signal with boundary test |
//! | PolypharmacyConfig | ς N ∂ | ς | T2-P | Configuration state |
//! | ReporterQualification | Σ κ | Σ | T2-P | Ordered sum of reporter qualifications |
//! | ReporterCase | × ∃ | × | T2-P | Product of drug/event/reporter |
//! | ReporterWeightedSignal | N ∂ κ | ∂ | T2-P | Reporter-weighted signal with boundary |
//! | ReporterWeightedConfig | ς N ∂ | ς | T2-P | Configuration state |
//! | GeographicCase | × ∃ λ | × | T2-P | Product with geographic location |
//! | CountrySignal | N λ | N | T2-P | Numeric signal with location context |
//! | GeographicDivergence | N κ λ ∂ | ∂ | T2-C | Geographic divergence with boundary |
//! | GeographicConfig | ς N ∂ | ς | T2-P | Configuration state |
//! | FaersReport | ς ∃ × | ς | T2-P | Stateful report record |
//! | ReportFingerprint | ∃ κ × | ∃ | T2-P | Identity fingerprint for comparison |
//! | DeduplicationResult | N σ ∃ | N | T2-P | Numeric dedup metrics |
//! | DuplicateCluster | σ ∃ κ | σ | T2-P | Ordered cluster of similar reports |
//! | DeduplicatorConfig | ς N ∂ | ς | T2-P | Configuration with thresholds |
//! | FaersDeduplicator | ς μ σ | ς | T2-P | Stateful dedup processor |
//! | NdcProduct | ς ∃ × | ς | T2-P | Stateful product record |
//! | NdcBridge | μ ς | μ | T2-P | Drug code lookup mapping |
//! | NdcMatch | ∃ κ N | ∃ | T2-P | Match with confidence score |
//! | NdcMatchType | Σ κ | Σ | T2-P | Ordered match classification |
//! | OpenFdaError | Σ | Σ | T1 | Sum enum of API error variants |
//! | DrugEventResponse | ς σ | ς | T2-P | API response state |
//! | DrugEventQuery | ς ∂ | ς | T2-P | Query state with filter boundaries |
//! | OpenFdaClient | ς μ π | ς | T2-P | Stateful API client with caching |

use nexcore_lex_primitiva::grounding::GroundsTo;
use nexcore_lex_primitiva::primitiva::{LexPrimitiva, PrimitiveComposition};

use crate::types::{CaseCount, ContingencyBatch, DrugName, DrugRole, EventName, RowCount};
use crate::{PipelineOutput, SignalDetectionResult};

use crate::analytics::{
    CascadeConfig, CaseSeriousness, CountrySignal, DrugCharacterization, GeographicCase,
    GeographicConfig, GeographicDivergence, MonthBucket, OutcomeCase, OutcomeConditionedConfig,
    OutcomeConditionedSignal, PolypharmacyCase, PolypharmacyConfig, PolypharmacySignal,
    ReactionOutcome, ReporterCase, ReporterQualification, ReporterWeightedConfig,
    ReporterWeightedSignal, SeriousnessCascade, SeriousnessCase, SeriousnessFlag, SignalVelocity,
    TemporalCase, VelocityConfig,
};

use crate::dedup::{
    DeduplicationResult, DeduplicatorConfig, DuplicateCluster, FaersDeduplicator, FaersReport,
    ReportFingerprint,
};

use crate::ndc::{NdcBridge, NdcMatch, NdcMatchType, NdcProduct};

use crate::api::{DrugEventQuery, DrugEventResponse, OpenFdaClient, OpenFdaError};

// ============================================================================
// T1 Universal (1 unique primitive)
// ============================================================================

/// CaseCount: Newtype wrapping u32 for co-occurrence count.
/// Tier: T1Universal. Dominant: N Quantity.
/// WHY: Pure numeric wrapper -- no other structure.
impl GroundsTo for CaseCount {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity, // N -- numeric count
        ])
        .with_dominant(LexPrimitiva::Quantity, 1.0)
    }
}

/// RowCount: Newtype wrapping u64 for row count.
/// Tier: T1Universal. Dominant: N Quantity.
/// WHY: Pure numeric wrapper.
impl GroundsTo for RowCount {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity, // N -- numeric count
        ])
        .with_dominant(LexPrimitiva::Quantity, 1.0)
    }
}

/// DrugRole: Sum enum of FAERS drug role codes (PS, SS, C, I).
/// Tier: T1Universal. Dominant: Σ Sum.
/// WHY: One-of-5 exclusive classification.
impl GroundsTo for DrugRole {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum, // Σ -- one-of-5 variant
        ])
        .with_dominant(LexPrimitiva::Sum, 1.0)
    }
}

/// DrugCharacterization: Sum enum of drug characterization codes.
/// Tier: T1Universal. Dominant: Σ Sum.
/// WHY: One-of-N exclusive classification.
impl GroundsTo for DrugCharacterization {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum, // Σ -- one-of-N variant
        ])
        .with_dominant(LexPrimitiva::Sum, 1.0)
    }
}

/// OpenFdaError: Sum enum of API error variants.
/// Tier: T1Universal. Dominant: Σ Sum.
/// WHY: One-of-N error classification.
impl GroundsTo for OpenFdaError {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum, // Σ -- one-of-N error variant
        ])
        .with_dominant(LexPrimitiva::Sum, 1.0)
    }
}

// ============================================================================
// T2-P (2-3 unique primitives)
// ============================================================================

/// DrugName: Uppercase-normalized drug identifier.
/// Tier: T2Primitive. Dominant: ∃ Existence.
/// WHY: Identity of a drug entity, created via uppercase normalization mapping.
impl GroundsTo for DrugName {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence, // ∃ -- drug identity
            LexPrimitiva::Mapping,   // μ -- uppercase normalization
        ])
        .with_dominant(LexPrimitiva::Existence, 0.80)
    }
}

/// EventName: Uppercase-normalized MedDRA Preferred Term.
/// Tier: T2Primitive. Dominant: ∃ Existence.
/// WHY: Identity of an adverse event, created via uppercase normalization.
impl GroundsTo for EventName {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence, // ∃ -- event identity
            LexPrimitiva::Mapping,   // μ -- uppercase normalization
        ])
        .with_dominant(LexPrimitiva::Existence, 0.80)
    }
}

/// ContingencyBatch: Batch of contingency tables with drug/event names.
/// Tier: T2Primitive. Dominant: N Quantity.
/// WHY: Batch of numeric 2x2 tables in SoA layout with identity columns.
impl GroundsTo for ContingencyBatch {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity, // N -- numeric contingency values
            LexPrimitiva::Sequence, // σ -- ordered batch of tables
            LexPrimitiva::Product,  // × -- (drugs, events, tables) product
        ])
        .with_dominant(LexPrimitiva::Quantity, 0.70)
    }
}

/// PipelineOutput: End-to-end ETL pipeline result.
/// Tier: T2Primitive. Dominant: σ Sequence.
/// WHY: Sequential pipeline result containing ordered signal detection results.
impl GroundsTo for PipelineOutput {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sequence, // σ -- pipeline output sequence
            LexPrimitiva::Quantity, // N -- total_pairs count
        ])
        .with_dominant(LexPrimitiva::Sequence, 0.80)
    }
}

/// ReactionOutcome: FAERS reaction outcome code (1-6).
/// Tier: T2Primitive. Dominant: Σ Sum.
/// WHY: One-of-6 ordered classification with irreversibility weighting.
impl GroundsTo for ReactionOutcome {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum,             // Σ -- one-of-6 outcome variant
            LexPrimitiva::Comparison,      // κ -- severity ordering
            LexPrimitiva::Irreversibility, // ∝ -- Fatal is irreversible
        ])
        .with_dominant(LexPrimitiva::Sum, 0.70)
    }
}

/// OutcomeCase: Drug-event-outcome triple for A82 analysis.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug + event + outcome identities.
impl GroundsTo for OutcomeCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (drug, event, outcome) product
            LexPrimitiva::Existence, // ∃ -- case identity
        ])
        .with_dominant(LexPrimitiva::Product, 0.80)
    }
}

/// OutcomeConditionedConfig: Configuration for A82 analysis.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with numeric threshold boundaries.
impl GroundsTo for OutcomeConditionedConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- numeric thresholds
            LexPrimitiva::Boundary, // ∂ -- detection threshold
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// MonthBucket: Temporal identity for time-series bucketing.
/// Tier: T2Primitive. Dominant: ∃ Existence.
/// WHY: Identity of a time bucket (YYYYMM string) with frequency semantics.
impl GroundsTo for MonthBucket {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence, // ∃ -- bucket identity
            LexPrimitiva::Frequency, // ν -- temporal frequency
        ])
        .with_dominant(LexPrimitiva::Existence, 0.75)
    }
}

/// TemporalCase: Drug-event-month triple for A77 velocity analysis.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug + event + month identities.
impl GroundsTo for TemporalCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (drug, event, month) product
            LexPrimitiva::Existence, // ∃ -- case identity
            LexPrimitiva::Frequency, // ν -- temporal frequency context
        ])
        .with_dominant(LexPrimitiva::Product, 0.70)
    }
}

/// VelocityConfig: Configuration for A77 velocity detector.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with numeric parameters.
impl GroundsTo for VelocityConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- numeric parameters
        ])
        .with_dominant(LexPrimitiva::State, 0.80)
    }
}

/// SeriousnessFlag: Sum enum of FAERS seriousness outcome flags.
/// Tier: T2Primitive. Dominant: Σ Sum.
/// WHY: Classification with boundary and irreversibility semantics.
impl GroundsTo for SeriousnessFlag {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum,             // Σ -- one-of-N variant
            LexPrimitiva::Boundary,        // ∂ -- seriousness boundary
            LexPrimitiva::Irreversibility, // ∝ -- Death/Disability are irreversible
        ])
        .with_dominant(LexPrimitiva::Sum, 0.70)
    }
}

/// CaseSeriousness: Case identity with seriousness classification.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of case_id + seriousness flags.
impl GroundsTo for CaseSeriousness {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (case_id, flags) product
            LexPrimitiva::Existence, // ∃ -- case identity
            LexPrimitiva::Boundary,  // ∂ -- seriousness boundary
        ])
        .with_dominant(LexPrimitiva::Product, 0.70)
    }
}

/// SeriousnessCase: Drug-event-seriousness triple for A80 analysis.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug + event + seriousness identities.
impl GroundsTo for SeriousnessCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (drug, event, seriousness) product
            LexPrimitiva::Existence, // ∃ -- case identity
            LexPrimitiva::Boundary,  // ∂ -- seriousness boundary
        ])
        .with_dominant(LexPrimitiva::Product, 0.70)
    }
}

/// CascadeConfig: Configuration for A80 seriousness cascade.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with boundary thresholds.
impl GroundsTo for CascadeConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- numeric weights
            LexPrimitiva::Boundary, // ∂ -- cascade thresholds
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// PolypharmacyCase: Case with multiple co-prescribed drugs.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug list + event + characterization.
impl GroundsTo for PolypharmacyCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- multi-drug product
            LexPrimitiva::Existence, // ∃ -- case identity
            LexPrimitiva::Sequence,  // σ -- ordered drug list
        ])
        .with_dominant(LexPrimitiva::Product, 0.70)
    }
}

/// PolypharmacyConfig: Configuration for polypharmacy analysis.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with boundary thresholds.
impl GroundsTo for PolypharmacyConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- numeric thresholds
            LexPrimitiva::Boundary, // ∂ -- detection thresholds
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// ReporterQualification: Ordered reporter role classification.
/// Tier: T2Primitive. Dominant: Σ Sum.
/// WHY: One-of-N classification with ordered credibility weighting.
impl GroundsTo for ReporterQualification {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum,        // Σ -- one-of-N variant
            LexPrimitiva::Comparison, // κ -- credibility ordering
        ])
        .with_dominant(LexPrimitiva::Sum, 0.80)
    }
}

/// ReporterCase: Drug-event-reporter triple.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug + event + reporter qualification.
impl GroundsTo for ReporterCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (drug, event, reporter) product
            LexPrimitiva::Existence, // ∃ -- case identity
        ])
        .with_dominant(LexPrimitiva::Product, 0.80)
    }
}

/// ReporterWeightedSignal: Reporter-weighted signal with boundary test.
/// Tier: T2Primitive. Dominant: ∂ Boundary.
/// WHY: Weighted signal that crosses detection boundary threshold.
impl GroundsTo for ReporterWeightedSignal {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,   // N -- weighted PRR value
            LexPrimitiva::Boundary,   // ∂ -- signal detection threshold
            LexPrimitiva::Comparison, // κ -- threshold comparison
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.70)
    }
}

/// ReporterWeightedConfig: Configuration for reporter weighting.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with weight parameters.
impl GroundsTo for ReporterWeightedConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- weight values
            LexPrimitiva::Boundary, // ∂ -- threshold boundary
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// GeographicCase: Case with geographic location data.
/// Tier: T2Primitive. Dominant: × Product.
/// WHY: Product of drug + event + country location.
impl GroundsTo for GeographicCase {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Product,   // × -- (drug, event, country) product
            LexPrimitiva::Existence, // ∃ -- case identity
            LexPrimitiva::Location,  // λ -- geographic location
        ])
        .with_dominant(LexPrimitiva::Product, 0.70)
    }
}

/// CountrySignal: Signal strength for a specific country.
/// Tier: T2Primitive. Dominant: N Quantity.
/// WHY: Numeric signal value associated with a geographic location.
impl GroundsTo for CountrySignal {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity, // N -- PRR/count values
            LexPrimitiva::Location, // λ -- country location
        ])
        .with_dominant(LexPrimitiva::Quantity, 0.80)
    }
}

/// GeographicConfig: Configuration for geographic analysis.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with boundary thresholds.
impl GroundsTo for GeographicConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- numeric parameters
            LexPrimitiva::Boundary, // ∂ -- threshold boundary
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// FaersReport: A FAERS adverse event report.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Stateful record with identity and numerous demographic fields.
impl GroundsTo for FaersReport {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,     // ς -- report demographic state
            LexPrimitiva::Existence, // ∃ -- safety_report_id identity
            LexPrimitiva::Product,   // × -- product of multiple fields
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// ReportFingerprint: Demographic fingerprint for deduplication.
/// Tier: T2Primitive. Dominant: ∃ Existence.
/// WHY: Identity hash for clustering -- determines if two reports are the same case.
impl GroundsTo for ReportFingerprint {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence,  // ∃ -- fingerprint identity
            LexPrimitiva::Comparison, // κ -- similarity comparison
            LexPrimitiva::Product,    // × -- product of demographic features
        ])
        .with_dominant(LexPrimitiva::Existence, 0.75)
    }
}

/// DeduplicationResult: Aggregate metrics from deduplication.
/// Tier: T2Primitive. Dominant: N Quantity.
/// WHY: Numeric counts (total, unique, duplicates, clusters).
impl GroundsTo for DeduplicationResult {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,  // N -- numeric counts
            LexPrimitiva::Sequence,  // σ -- ordered cluster list
            LexPrimitiva::Existence, // ∃ -- unique case existence
        ])
        .with_dominant(LexPrimitiva::Quantity, 0.70)
    }
}

/// DuplicateCluster: Cluster of similar/duplicate reports.
/// Tier: T2Primitive. Dominant: σ Sequence.
/// WHY: Ordered list of report IDs within a cluster, identified by canonical.
impl GroundsTo for DuplicateCluster {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sequence,   // σ -- ordered member list
            LexPrimitiva::Existence,  // ∃ -- canonical report identity
            LexPrimitiva::Comparison, // κ -- similarity threshold
        ])
        .with_dominant(LexPrimitiva::Sequence, 0.70)
    }
}

/// DeduplicatorConfig: Configuration for deduplication.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Configuration state with similarity threshold boundaries.
impl GroundsTo for DeduplicatorConfig {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- configuration state
            LexPrimitiva::Quantity, // N -- threshold values
            LexPrimitiva::Boundary, // ∂ -- similarity threshold
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// FaersDeduplicator: Stateful deduplication processor.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Maintains index state, maps reports to clusters.
impl GroundsTo for FaersDeduplicator {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- index state
            LexPrimitiva::Mapping,  // μ -- report -> cluster mapping
            LexPrimitiva::Sequence, // σ -- sequential processing
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// NdcProduct: NDC product record from FDA directory.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Stateful product record with identity and numerous attributes.
impl GroundsTo for NdcProduct {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,     // ς -- product attribute state
            LexPrimitiva::Existence, // ∃ -- NDC code identity
            LexPrimitiva::Product,   // × -- product of multiple fields
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

/// NdcBridge: Drug code lookup and matching service.
/// Tier: T2Primitive. Dominant: μ Mapping.
/// WHY: Core function is mapping drug names to NDC codes across multiple indices.
impl GroundsTo for NdcBridge {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Mapping, // μ -- name -> NDC mapping
            LexPrimitiva::State,   // ς -- multi-index state
        ])
        .with_dominant(LexPrimitiva::Mapping, 0.80)
    }
}

/// NdcMatch: Result of NDC lookup with confidence.
/// Tier: T2Primitive. Dominant: ∃ Existence.
/// WHY: Existence of a match with confidence score and match type.
impl GroundsTo for NdcMatch {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence,  // ∃ -- match existence
            LexPrimitiva::Comparison, // κ -- match quality comparison
            LexPrimitiva::Quantity,   // N -- confidence score
        ])
        .with_dominant(LexPrimitiva::Existence, 0.70)
    }
}

/// NdcMatchType: Classification of NDC match quality.
/// Tier: T2Primitive. Dominant: Σ Sum.
/// WHY: One-of-5 ordered match classification.
impl GroundsTo for NdcMatchType {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Sum,        // Σ -- one-of-5 variant
            LexPrimitiva::Comparison, // κ -- quality ordering
        ])
        .with_dominant(LexPrimitiva::Sum, 0.80)
    }
}

/// DrugEventResponse: OpenFDA API response container.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Stateful response with results and metadata.
impl GroundsTo for DrugEventResponse {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- response state
            LexPrimitiva::Sequence, // σ -- ordered results list
        ])
        .with_dominant(LexPrimitiva::State, 0.80)
    }
}

/// DrugEventQuery: Query builder for OpenFDA API.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Stateful query with filter boundary parameters.
impl GroundsTo for DrugEventQuery {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- query parameter state
            LexPrimitiva::Boundary, // ∂ -- filter boundaries (date range, limit)
        ])
        .with_dominant(LexPrimitiva::State, 0.80)
    }
}

/// OpenFdaClient: Async HTTP client with caching.
/// Tier: T2Primitive. Dominant: ς State.
/// WHY: Stateful client with cache persistence and request mapping.
impl GroundsTo for OpenFdaClient {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,       // ς -- client + cache state
            LexPrimitiva::Mapping,     // μ -- request -> response mapping
            LexPrimitiva::Persistence, // π -- response caching
        ])
        .with_dominant(LexPrimitiva::State, 0.70)
    }
}

// ============================================================================
// T2-C (4-5 unique primitives)
// ============================================================================

/// MetricAssessment<M>: Assessment of a single disproportionality metric.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: The is_signal flag is a boundary test: does the metric exceed the threshold?
/// Boundary enforcement is the defining purpose.
/// NOTE: Cannot implement GroundsTo for generic MetricAssessment<M> directly
/// because GroundsTo requires concrete types. Implemented for the concept.
///
/// (Skipped due to generic type parameter -- GroundsTo is for concrete types.)

/// SignalDetectionResult: Complete signal detection for a drug-event pair.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: Tests signal existence across 4 metric boundaries (PRR, ROR, IC, EBGM).
/// Patient safety boundary enforcement is the defining purpose.
impl GroundsTo for SignalDetectionResult {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Existence,  // ∃ -- signal existence determination
            LexPrimitiva::Boundary,   // ∂ -- threshold boundary tests
            LexPrimitiva::Quantity,   // N -- numeric metric values
            LexPrimitiva::Comparison, // κ -- threshold comparison
            LexPrimitiva::Product,    // × -- (drug, event, metrics) product
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.65)
    }
}

/// OutcomeConditionedSignal: A82 outcome-conditioned signal result.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: Signal weighted by outcome severity, crossing detection boundary.
impl GroundsTo for OutcomeConditionedSignal {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,        // N -- weighted PRR and severity score
            LexPrimitiva::Boundary,        // ∂ -- detection boundary
            LexPrimitiva::Comparison,      // κ -- threshold comparison
            LexPrimitiva::Irreversibility, // ∝ -- outcome severity weighting
            LexPrimitiva::Causality,       // → -- drug causes outcome
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.60)
    }
}

/// SignalVelocity: A77 signal velocity measurement.
/// Tier: T2Composite. Dominant: ν Frequency.
/// WHY: Rate of change of reporting frequency over time.
impl GroundsTo for SignalVelocity {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,  // N -- velocity and acceleration values
            LexPrimitiva::Frequency, // ν -- temporal frequency change rate
            LexPrimitiva::Sequence,  // σ -- time-series ordering
            LexPrimitiva::Boundary,  // ∂ -- detection threshold
        ])
        .with_dominant(LexPrimitiva::Frequency, 0.65)
    }
}

/// SeriousnessCascade: A80 seriousness cascade score.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: Cascade scoring tests whether seriousness escalation crosses safety boundary.
impl GroundsTo for SeriousnessCascade {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Boundary,        // ∂ -- cascade detection boundary
            LexPrimitiva::Irreversibility, // ∝ -- seriousness irreversibility
            LexPrimitiva::Quantity,        // N -- cascade score
            LexPrimitiva::Comparison,      // κ -- severity comparison
            LexPrimitiva::Causality,       // → -- drug causes escalation
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.60)
    }
}

/// PolypharmacySignal: Drug interaction signal from polypharmacy analysis.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: Interaction signal tests if drug combination exceeds safety boundary.
impl GroundsTo for PolypharmacySignal {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,   // N -- interaction PRR and lift
            LexPrimitiva::Boundary,   // ∂ -- signal detection threshold
            LexPrimitiva::Comparison, // κ -- threshold comparison
            LexPrimitiva::Product,    // × -- drug combination product
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.65)
    }
}

/// GeographicDivergence: Geographic signal divergence result.
/// Tier: T2Composite. Dominant: ∂ Boundary.
/// WHY: Detects geographic signal divergence exceeding a boundary.
impl GroundsTo for GeographicDivergence {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Quantity,   // N -- divergence score
            LexPrimitiva::Comparison, // κ -- country comparison
            LexPrimitiva::Location,   // λ -- geographic context
            LexPrimitiva::Boundary,   // ∂ -- divergence threshold
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexcore_lex_primitiva::tier::Tier;

    // T1 Universal
    #[test]
    fn case_count_is_t1() {
        assert_eq!(CaseCount::tier(), Tier::T1Universal);
        assert_eq!(
            CaseCount::dominant_primitive(),
            Some(LexPrimitiva::Quantity)
        );
    }

    #[test]
    fn row_count_is_t1() {
        assert_eq!(RowCount::tier(), Tier::T1Universal);
        assert_eq!(RowCount::dominant_primitive(), Some(LexPrimitiva::Quantity));
    }

    #[test]
    fn drug_role_is_t1() {
        assert_eq!(DrugRole::tier(), Tier::T1Universal);
        assert_eq!(DrugRole::dominant_primitive(), Some(LexPrimitiva::Sum));
    }

    #[test]
    fn drug_characterization_is_t1() {
        assert_eq!(DrugCharacterization::tier(), Tier::T1Universal);
    }

    #[test]
    fn openfda_error_is_t1() {
        assert_eq!(OpenFdaError::tier(), Tier::T1Universal);
    }

    // T2-P
    #[test]
    fn drug_name_is_t2p_existence() {
        assert_eq!(DrugName::tier(), Tier::T2Primitive);
        assert_eq!(
            DrugName::dominant_primitive(),
            Some(LexPrimitiva::Existence)
        );
    }

    #[test]
    fn event_name_is_t2p_existence() {
        assert_eq!(EventName::tier(), Tier::T2Primitive);
        assert_eq!(
            EventName::dominant_primitive(),
            Some(LexPrimitiva::Existence)
        );
    }

    #[test]
    fn contingency_batch_is_t2p() {
        assert_eq!(ContingencyBatch::tier(), Tier::T2Primitive);
        assert_eq!(
            ContingencyBatch::dominant_primitive(),
            Some(LexPrimitiva::Quantity)
        );
    }

    #[test]
    fn pipeline_output_is_t2p() {
        assert_eq!(PipelineOutput::tier(), Tier::T2Primitive);
        assert_eq!(
            PipelineOutput::dominant_primitive(),
            Some(LexPrimitiva::Sequence)
        );
    }

    #[test]
    fn reaction_outcome_is_t2p() {
        assert_eq!(ReactionOutcome::tier(), Tier::T2Primitive);
        assert_eq!(
            ReactionOutcome::dominant_primitive(),
            Some(LexPrimitiva::Sum)
        );
    }

    #[test]
    fn month_bucket_is_t2p() {
        assert_eq!(MonthBucket::tier(), Tier::T2Primitive);
    }

    #[test]
    fn reporter_qualification_is_t2p() {
        assert_eq!(ReporterQualification::tier(), Tier::T2Primitive);
    }

    #[test]
    fn ndc_product_is_t2p() {
        assert_eq!(NdcProduct::tier(), Tier::T2Primitive);
    }

    #[test]
    fn ndc_bridge_is_t2p_mapping() {
        assert_eq!(NdcBridge::tier(), Tier::T2Primitive);
        assert_eq!(NdcBridge::dominant_primitive(), Some(LexPrimitiva::Mapping));
    }

    #[test]
    fn ndc_match_is_t2p_existence() {
        assert_eq!(NdcMatch::tier(), Tier::T2Primitive);
        assert_eq!(
            NdcMatch::dominant_primitive(),
            Some(LexPrimitiva::Existence)
        );
    }

    #[test]
    fn ndc_match_type_is_t2p() {
        assert_eq!(NdcMatchType::tier(), Tier::T2Primitive);
    }

    #[test]
    fn faers_report_is_t2p() {
        assert_eq!(FaersReport::tier(), Tier::T2Primitive);
    }

    #[test]
    fn report_fingerprint_is_t2p() {
        assert_eq!(ReportFingerprint::tier(), Tier::T2Primitive);
    }

    #[test]
    fn dedup_result_is_t2p() {
        assert_eq!(DeduplicationResult::tier(), Tier::T2Primitive);
    }

    #[test]
    fn duplicate_cluster_is_t2p() {
        assert_eq!(DuplicateCluster::tier(), Tier::T2Primitive);
    }

    #[test]
    fn faers_deduplicator_is_t2p() {
        assert_eq!(FaersDeduplicator::tier(), Tier::T2Primitive);
    }

    #[test]
    fn openfda_client_is_t2p() {
        assert_eq!(OpenFdaClient::tier(), Tier::T2Primitive);
    }

    // T2-C
    #[test]
    fn signal_detection_result_is_t2c_boundary() {
        assert_eq!(SignalDetectionResult::tier(), Tier::T2Composite);
        assert_eq!(
            SignalDetectionResult::dominant_primitive(),
            Some(LexPrimitiva::Boundary)
        );
    }

    #[test]
    fn outcome_conditioned_signal_is_t2c() {
        assert_eq!(OutcomeConditionedSignal::tier(), Tier::T2Composite);
        assert_eq!(
            OutcomeConditionedSignal::dominant_primitive(),
            Some(LexPrimitiva::Boundary)
        );
    }

    #[test]
    fn signal_velocity_is_t2c_frequency() {
        assert_eq!(SignalVelocity::tier(), Tier::T2Composite);
        assert_eq!(
            SignalVelocity::dominant_primitive(),
            Some(LexPrimitiva::Frequency)
        );
    }

    #[test]
    fn seriousness_cascade_is_t2c_boundary() {
        assert_eq!(SeriousnessCascade::tier(), Tier::T2Composite);
        assert_eq!(
            SeriousnessCascade::dominant_primitive(),
            Some(LexPrimitiva::Boundary)
        );
    }

    #[test]
    fn polypharmacy_signal_is_t2c_boundary() {
        assert_eq!(PolypharmacySignal::tier(), Tier::T2Composite);
        assert_eq!(
            PolypharmacySignal::dominant_primitive(),
            Some(LexPrimitiva::Boundary)
        );
    }

    #[test]
    fn geographic_divergence_is_t2c_boundary() {
        assert_eq!(GeographicDivergence::tier(), Tier::T2Composite);
        assert_eq!(
            GeographicDivergence::dominant_primitive(),
            Some(LexPrimitiva::Boundary)
        );
    }
}
