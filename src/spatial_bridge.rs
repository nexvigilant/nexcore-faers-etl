//! # Spatial Bridge: nexcore-faers-etl → stem-math
//!
//! Implements `Metric` for FAERS report fingerprint similarity
//! and `Embed` for the report → fingerprint dimension reduction.
//!
//! ## Primitive Foundation
//!
//! FAERS deduplication is fundamentally spatial:
//! - Two reports are "duplicates" when their fingerprint distance is below a threshold
//! - The fingerprint is a dimension-reducing embedding: 13 report fields → 7 fingerprint fields
//! - The similarity threshold (default 0.85) defines a deduplication Neighborhood
//!
//! ## Architecture Decision
//!
//! The `FingerprintMetric` wraps the existing `FaersDeduplicator::similarity()` method,
//! converting it from similarity (1.0 = identical) to distance (0.0 = identical).

use stem_math::spatial::{Dimension, Distance, Embed, Metric, Neighborhood};

use crate::dedup::{FaersDeduplicator, FaersReport, ReportFingerprint};

// ============================================================================
// FingerprintMetric: Distance between two FAERS reports
// ============================================================================

/// Metric over FAERS reports using weighted fingerprint similarity.
///
/// Distance = 1.0 - similarity(a, b), where similarity is the weighted
/// field-matching score from `FaersDeduplicator`.
///
/// Weights: primary_drug (3), primary_reaction (2), event_month (2),
///          age_bucket (1), sex (1), country (1).
///
/// Tier: T2-C (N Quantity + κ Comparison + μ Mapping + ∂ Boundary)
pub struct FingerprintMetric {
    deduplicator: FaersDeduplicator,
}

impl FingerprintMetric {
    /// Create a fingerprint metric with default deduplication config.
    pub fn new() -> Self {
        Self {
            deduplicator: FaersDeduplicator::new(),
        }
    }

    /// Create with a custom similarity threshold.
    pub fn with_threshold(threshold: f64) -> Self {
        Self {
            deduplicator: FaersDeduplicator::with_threshold(threshold),
        }
    }
}

impl Default for FingerprintMetric {
    fn default() -> Self {
        Self::new()
    }
}

impl Metric for FingerprintMetric {
    type Element = FaersReport;

    fn distance(&self, a: &FaersReport, b: &FaersReport) -> Distance {
        let fp_a = self.deduplicator.fingerprint(a);
        let fp_b = self.deduplicator.fingerprint(b);
        let sim = self.deduplicator.similarity(&fp_a, &fp_b);
        // Convert similarity [0,1] to distance [0,1]: distance = 1 - similarity
        Distance::new(1.0 - sim)
    }
}

// ============================================================================
// Embed: FaersReport → ReportFingerprint (dimension reduction)
// ============================================================================

/// Embedding from full FAERS reports to compressed fingerprints.
///
/// This is a structure-preserving dimension reduction:
/// - Source: 13 fields (FaersReport)
/// - Target: 7 fields (ReportFingerprint)
/// - Codimension: 6 (fields lost in projection)
///
/// The embedding preserves deduplication-relevant structure:
/// similar reports map to similar fingerprints.
///
/// Tier: T2-C (μ Mapping + N Quantity + Σ Sum + ∂ Boundary)
pub struct ReportToFingerprint {
    deduplicator: FaersDeduplicator,
}

impl ReportToFingerprint {
    /// Create a new report-to-fingerprint embedding.
    pub fn new() -> Self {
        Self {
            deduplicator: FaersDeduplicator::new(),
        }
    }
}

impl Default for ReportToFingerprint {
    fn default() -> Self {
        Self::new()
    }
}

impl Embed for ReportToFingerprint {
    type Source = FaersReport;
    type Target = ReportFingerprint;

    fn embed(&self, source: &FaersReport) -> ReportFingerprint {
        self.deduplicator.fingerprint(source)
    }

    fn in_image(&self, _target: &ReportFingerprint) -> bool {
        // Every fingerprint is reachable from some report
        true
    }

    fn codimension(&self) -> Dimension {
        // 13 report fields → 7 fingerprint fields = 6 lost dimensions
        REPORT_CODIMENSION
    }
}

// ============================================================================
// Dimension constants
// ============================================================================

/// Full FAERS report dimensionality (13 fields).
pub const REPORT_DIMENSION: Dimension = Dimension::new(13);

/// Fingerprint dimensionality (7 fields).
pub const FINGERPRINT_DIMENSION: Dimension = Dimension::new(7);

/// Codimension of the report → fingerprint embedding (6 fields lost).
pub const REPORT_CODIMENSION: Dimension = Dimension::new(6);

// ============================================================================
// Deduplication neighborhoods
// ============================================================================

/// Default deduplication neighborhood (similarity >= 0.85, distance <= 0.15).
pub fn default_dedup_neighborhood() -> Neighborhood {
    Neighborhood::closed(Distance::new(0.15))
}

/// Strict deduplication (similarity >= 0.95, distance <= 0.05).
pub fn strict_dedup_neighborhood() -> Neighborhood {
    Neighborhood::closed(Distance::new(0.05))
}

/// Lenient deduplication (similarity >= 0.70, distance <= 0.30).
pub fn lenient_dedup_neighborhood() -> Neighborhood {
    Neighborhood::closed(Distance::new(0.30))
}

/// Check if two reports are potential duplicates under the given neighborhood.
pub fn is_potential_duplicate(
    metric: &FingerprintMetric,
    a: &FaersReport,
    b: &FaersReport,
    neighborhood: &Neighborhood,
) -> bool {
    metric.within(a, b, neighborhood)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_report(drug: &str, reaction: &str, sex: &str) -> FaersReport {
        FaersReport {
            safety_report_id: "TEST001".to_string(),
            case_version: 1,
            receipt_date: "20240101".to_string(),
            patient_age: Some(45),
            patient_sex: Some(sex.to_string()),
            patient_weight: Some(70.0),
            occur_country: Some("US".to_string()),
            primary_drug: Some(drug.to_string()),
            primary_reaction: Some(reaction.to_string()),
            event_date: Some("20240101".to_string()),
            report_type: Some(1),
            serious: true,
            mfr_num: None,
        }
    }

    // ===== Metric axiom tests =====

    #[test]
    fn metric_non_negativity() {
        let m = FingerprintMetric::new();
        let r1 = make_report("ASPIRIN", "HEADACHE", "M");
        let r2 = make_report("IBUPROFEN", "NAUSEA", "F");
        assert!(m.distance(&r1, &r2).value() >= 0.0);
    }

    #[test]
    fn metric_identity() {
        let m = FingerprintMetric::new();
        let r = make_report("ASPIRIN", "HEADACHE", "M");
        // Same report → similarity = 1.0 → distance = 0.0
        assert!(m.distance(&r, &r).approx_eq(&Distance::ZERO, 1e-10));
    }

    #[test]
    fn metric_symmetry() {
        let m = FingerprintMetric::new();
        let r1 = make_report("ASPIRIN", "HEADACHE", "M");
        let r2 = make_report("IBUPROFEN", "NAUSEA", "F");
        assert!(m.is_symmetric(&r1, &r2, 1e-10));
    }

    // ===== Embed tests =====

    #[test]
    fn embed_produces_fingerprint() {
        let e = ReportToFingerprint::new();
        let report = make_report("ASPIRIN", "HEADACHE", "M");
        let fp = e.embed(&report);
        assert_eq!(fp.primary_drug.as_deref(), Some("ASPIRIN"));
        assert_eq!(fp.primary_reaction.as_deref(), Some("HEADACHE"));
    }

    #[test]
    fn embed_codimension() {
        let e = ReportToFingerprint::new();
        assert_eq!(e.codimension().rank(), 6);
    }

    #[test]
    fn embed_all_in_image() {
        let e = ReportToFingerprint::new();
        let fp = ReportFingerprint {
            age_bucket: Some(9),
            sex: Some("M".to_string()),
            country: Some("US".to_string()),
            event_month: Some("202401".to_string()),
            primary_drug: Some("ASPIRIN".to_string()),
            primary_reaction: Some("HEADACHE".to_string()),
            mfr_num: None,
        };
        assert!(e.in_image(&fp));
    }

    // ===== Neighborhood tests =====

    #[test]
    fn duplicate_detection_within_neighborhood() {
        let m = FingerprintMetric::new();
        let r1 = make_report("ASPIRIN", "HEADACHE", "M");
        let r2 = make_report("ASPIRIN", "HEADACHE", "M");
        let n = default_dedup_neighborhood();
        // Identical content → distance ≈ 0 → within neighborhood
        assert!(is_potential_duplicate(&m, &r1, &r2, &n));
    }

    #[test]
    fn different_reports_outside_strict_neighborhood() {
        let m = FingerprintMetric::new();
        let r1 = make_report("ASPIRIN", "HEADACHE", "M");
        let r2 = make_report("IBUPROFEN", "NAUSEA", "F");
        let n = strict_dedup_neighborhood();
        // Very different reports → high distance → outside strict neighborhood
        assert!(!is_potential_duplicate(&m, &r1, &r2, &n));
    }

    // ===== Dimension constants =====

    #[test]
    fn dimension_constants_consistent() {
        assert_eq!(REPORT_DIMENSION.rank(), 13);
        assert_eq!(FINGERPRINT_DIMENSION.rank(), 7);
        assert_eq!(REPORT_CODIMENSION.rank(), 6);
        // codimension = source - target dimension
        assert_eq!(
            REPORT_DIMENSION.rank() - FINGERPRINT_DIMENSION.rank(),
            REPORT_CODIMENSION.rank()
        );
    }
}
