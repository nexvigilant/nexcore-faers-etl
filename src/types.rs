//! Type-safe domain types for the FAERS ETL pipeline.
//!
//! Eliminates naked primitives per the Primitive Codex:
//! - T2-P newtypes wrap `String`, `u32`, `u64`
//! - T2-C composites group related metrics
//! - Column constants eliminate magic strings

use nexcore_vigilance::pv::signals::batch::BatchContingencyTables;
use std::fmt;

// =============================================================================
// T2-P NEWTYPES
// =============================================================================

/// Tier: T2-P — Uppercase-normalized drug name from FAERS.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DrugName(pub String);

impl DrugName {
    /// Create a new DrugName, uppercasing the input.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into().to_uppercase())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DrugName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for DrugName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Tier: T2-P — Uppercase-normalized MedDRA Preferred Term.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventName(pub String);

impl EventName {
    /// Create a new EventName, uppercasing the input.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into().to_uppercase())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for EventName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Tier: T2-P — Co-occurrence count (cell "a" of the 2×2 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CaseCount(pub u64);

impl CaseCount {
    /// Extract the raw count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for CaseCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Tier: T2-P — Number of rows written to a sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowCount(pub u64);

impl RowCount {
    /// Extract the raw count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RowCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// =============================================================================
// T2-P ENUM
// =============================================================================

/// Tier: T2-P — FAERS drug role code.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DrugRole {
    /// Primary Suspect ("PS")
    PrimarySuspect,
    /// Secondary Suspect ("SS")
    SecondarySuspect,
    /// Concomitant ("C")
    Concomitant,
    /// Interacting ("I")
    Interacting,
    /// Unknown or unrecognized role code
    Unknown(String),
}

impl DrugRole {
    /// Returns true if this role is a suspect (PS or SS).
    #[must_use]
    pub fn is_suspect(&self) -> bool {
        matches!(self, Self::PrimarySuspect | Self::SecondarySuspect)
    }

    /// Returns the FAERS role code string.
    #[must_use]
    pub fn as_code(&self) -> &str {
        match self {
            Self::PrimarySuspect => "PS",
            Self::SecondarySuspect => "SS",
            Self::Concomitant => "C",
            Self::Interacting => "I",
            Self::Unknown(s) => s.as_str(),
        }
    }
}

impl From<&str> for DrugRole {
    fn from(s: &str) -> Self {
        match s {
            "PS" => Self::PrimarySuspect,
            "SS" => Self::SecondarySuspect,
            "C" => Self::Concomitant,
            "I" => Self::Interacting,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl fmt::Display for DrugRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_code())
    }
}

// =============================================================================
// T2-C COMPOSITES
// =============================================================================

/// Tier: T2-C — Assessment result for a single disproportionality metric.
///
/// Generic over the metric type (Prr, Ror, Ic, Ebgm) to prevent mixing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricAssessment<M> {
    /// Point estimate (T2-P newtype)
    pub point: M,
    /// Lower 95% confidence/credibility bound
    pub lower_ci: f64,
    /// Upper 95% confidence/credibility bound (some metrics have lower only)
    pub upper_ci: Option<f64>,
    /// Whether the signal threshold was exceeded
    pub is_signal: bool,
}

/// Tier: T2-C — Batch of contingency tables with associated drug/event names.
#[derive(Debug, Clone)]
pub struct ContingencyBatch {
    /// Drug names for each table
    pub drugs: Vec<DrugName>,
    /// Event names for each table
    pub events: Vec<EventName>,
    /// Batch contingency tables (SoA layout)
    pub tables: BatchContingencyTables,
}

// =============================================================================
// COLUMN NAME CONSTANTS
// =============================================================================

/// Column name constants — eliminates magic strings in Polars operations.
pub mod columns {
    /// Case identifier column
    pub const CASE_ID: &str = "case_id";
    /// Drug name column
    pub const DRUG: &str = "drug";
    /// Event/reaction name column
    pub const EVENT: &str = "event";
    /// Co-occurrence count column
    pub const N: &str = "n";
    /// Age in years column
    pub const AGE_YEARS: &str = "age_years";
    /// Age group column
    pub const AGE_GROUP: &str = "age_group";
    /// Sex column
    pub const SEX: &str = "sex";
    /// Weight in kg column
    pub const WEIGHT_KG: &str = "weight_kg";
    /// Reporter country column
    pub const REPORTER_COUNTRY: &str = "reporter_country";
    /// Occurrence country column
    pub const OCCR_COUNTRY: &str = "occr_country";
    /// Manufacturer/sender column
    pub const MFR_SNDR: &str = "mfr_sndr";
    /// Occupation code column
    pub const OCCP_COD: &str = "occp_cod";
    /// Manufacturer number column
    pub const MFR_NUM: &str = "mfr_num";
    /// FDA date column
    pub const FDA_DT: &str = "fda_dt";
    /// Event date column
    pub const EVENT_DT: &str = "event_dt";
    /// Drug role code column
    pub const ROLE_CODE: &str = "role_code";
    /// Report sources column
    pub const REPORT_SOURCES: &str = "report_sources";
    /// Outcomes column
    pub const OUTCOMES: &str = "outcomes";
    /// Therapy summary column
    pub const THERAPY_SUMMARY: &str = "therapy_summary";

    // Signal result columns
    /// PRR point estimate
    pub const PRR: &str = "prr";
    /// PRR lower CI
    pub const PRR_LOWER_CI: &str = "prr_lower_ci";
    /// PRR upper CI
    pub const PRR_UPPER_CI: &str = "prr_upper_ci";
    /// PRR signal flag
    pub const PRR_SIGNAL: &str = "prr_signal";
    /// ROR point estimate
    pub const ROR: &str = "ror";
    /// ROR lower CI
    pub const ROR_LOWER_CI: &str = "ror_lower_ci";
    /// ROR signal flag
    pub const ROR_SIGNAL: &str = "ror_signal";
    /// IC point estimate
    pub const IC: &str = "ic";
    /// IC025 (lower credibility bound)
    pub const IC025: &str = "ic025";
    /// IC signal flag
    pub const IC_SIGNAL: &str = "ic_signal";
    /// EBGM point estimate
    pub const EBGM: &str = "ebgm";
    /// EB05 (lower credibility bound)
    pub const EB05: &str = "eb05";
    /// EBGM signal flag
    pub const EBGM_SIGNAL: &str = "ebgm_signal";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drug_name_uppercases() {
        let name = DrugName::new("aspirin");
        assert_eq!(name.as_str(), "ASPIRIN");
    }

    #[test]
    fn test_event_name_uppercases() {
        let name = EventName::new("headache");
        assert_eq!(name.as_str(), "HEADACHE");
    }

    #[test]
    fn test_drug_role_from_str() {
        assert_eq!(DrugRole::from("PS"), DrugRole::PrimarySuspect);
        assert_eq!(DrugRole::from("SS"), DrugRole::SecondarySuspect);
        assert_eq!(DrugRole::from("C"), DrugRole::Concomitant);
        assert_eq!(DrugRole::from("I"), DrugRole::Interacting);
        assert!(matches!(DrugRole::from("X"), DrugRole::Unknown(_)));
    }

    #[test]
    fn test_drug_role_is_suspect() {
        assert!(DrugRole::PrimarySuspect.is_suspect());
        assert!(DrugRole::SecondarySuspect.is_suspect());
        assert!(!DrugRole::Concomitant.is_suspect());
        assert!(!DrugRole::Interacting.is_suspect());
    }

    #[test]
    fn test_case_count_value() {
        let c = CaseCount(42);
        assert_eq!(c.value(), 42);
    }

    #[test]
    fn test_row_count_value() {
        let r = RowCount(1000);
        assert_eq!(r.value(), 1000);
    }

    #[test]
    fn test_drug_role_as_code() {
        assert_eq!(DrugRole::PrimarySuspect.as_code(), "PS");
        assert_eq!(DrugRole::SecondarySuspect.as_code(), "SS");
        assert_eq!(DrugRole::Concomitant.as_code(), "C");
        assert_eq!(DrugRole::Interacting.as_code(), "I");
    }
}
