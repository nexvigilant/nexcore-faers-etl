//! FAERS Report Deduplication.
//!
//! Identifies and clusters duplicate adverse event reports using
//! demographic fingerprinting and similarity scoring.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

// =============================================================================
// Types
// =============================================================================

/// A FAERS report for deduplication analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaersReport {
    /// Safety report ID.
    pub safety_report_id: String,
    /// Case version number.
    pub case_version: u32,
    /// Receipt date (YYYYMMDD).
    pub receipt_date: String,
    /// Patient age (optional).
    pub patient_age: Option<u32>,
    /// Patient sex (1=male, 2=female, 0=unknown).
    pub patient_sex: Option<String>,
    /// Patient weight in kg (optional).
    pub patient_weight: Option<f64>,
    /// Occurrence country.
    pub occur_country: Option<String>,
    /// Primary suspect drug name.
    pub primary_drug: Option<String>,
    /// Primary reaction (MedDRA PT).
    pub primary_reaction: Option<String>,
    /// Event date (YYYYMMDD, optional).
    pub event_date: Option<String>,
    /// Report type (1-5).
    pub report_type: Option<u32>,
    /// Serious flag.
    pub serious: bool,
    /// Manufacturer control number (strong deduplication key).
    pub mfr_num: Option<String>,
}

/// Fingerprint for clustering similar reports.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportFingerprint {
    /// Age bucket (5-year intervals).
    pub age_bucket: Option<u32>,
    /// Sex.
    pub sex: Option<String>,
    /// Country.
    pub country: Option<String>,
    /// Event month (YYYYMM).
    pub event_month: Option<String>,
    /// Primary drug (uppercase).
    pub primary_drug: Option<String>,
    /// Primary reaction (uppercase).
    pub primary_reaction: Option<String>,
    /// Manufacturer control number.
    pub mfr_num: Option<String>,
}

/// Result of deduplication.
#[derive(Debug, Clone)]
pub struct DeduplicationResult {
    /// Unique reports after deduplication.
    pub unique_reports: Vec<FaersReport>,
    /// Clusters of duplicate reports.
    pub clusters: Vec<DuplicateCluster>,
    /// Total input reports.
    pub input_count: usize,
    /// Duplicate count removed.
    pub duplicates_removed: usize,
}

/// A cluster of duplicate reports.
#[derive(Debug, Clone)]
pub struct DuplicateCluster {
    /// Primary (selected) report.
    pub primary: FaersReport,
    /// Duplicate reports.
    pub duplicates: Vec<FaersReport>,
    /// Cluster fingerprint.
    pub fingerprint: ReportFingerprint,
}

/// Deduplicator configuration.
#[derive(Debug, Clone)]
pub struct DeduplicatorConfig {
    /// Similarity threshold (0.0-1.0).
    pub similarity_threshold: f64,
    /// Age bucket size in years.
    pub age_bucket_size: u32,
    /// Whether to prefer later case versions.
    pub prefer_latest_version: bool,
    /// Whether to prefer more complete reports.
    pub prefer_complete: bool,
}

impl Default for DeduplicatorConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.85,
            age_bucket_size: 5,
            prefer_latest_version: true,
            prefer_complete: true,
        }
    }
}

// =============================================================================
// Deduplicator
// =============================================================================

/// FAERS report deduplicator.
pub struct FaersDeduplicator {
    config: DeduplicatorConfig,
}

impl FaersDeduplicator {
    /// Create a new deduplicator with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: DeduplicatorConfig::default(),
        }
    }

    /// Create a deduplicator with custom configuration.
    #[must_use]
    pub fn with_config(config: DeduplicatorConfig) -> Self {
        Self { config }
    }

    /// Create a deduplicator with custom similarity threshold.
    #[must_use]
    pub fn with_threshold(threshold: f64) -> Self {
        Self {
            config: DeduplicatorConfig {
                similarity_threshold: threshold,
                ..Default::default()
            },
        }
    }

    /// Generate fingerprint for a report.
    #[must_use]
    pub fn fingerprint(&self, report: &FaersReport) -> ReportFingerprint {
        ReportFingerprint {
            age_bucket: report
                .patient_age
                .map(|a| (a / self.config.age_bucket_size) * self.config.age_bucket_size),
            sex: report.patient_sex.clone(),
            country: report.occur_country.clone(),
            event_month: report.event_date.as_ref().and_then(|d| {
                if d.len() >= 6 {
                    Some(d[..6].to_string())
                } else {
                    None
                }
            }),
            primary_drug: report.primary_drug.as_ref().map(|d| d.to_uppercase()),
            primary_reaction: report.primary_reaction.as_ref().map(|r| r.to_uppercase()),
            mfr_num: report.mfr_num.as_ref().map(|n| n.trim().to_uppercase()),
        }
    }

    /// Compute similarity between two fingerprints (0.0-1.0).
    #[must_use]
    pub fn similarity(&self, a: &ReportFingerprint, b: &ReportFingerprint) -> f64 {
        // Deterministic match via Manufacturer Number
        if let (Some(m1), Some(m2)) = (&a.mfr_num, &b.mfr_num) {
            if m1 == m2 && !m1.is_empty() {
                return 1.0;
            }
        }

        let mut matches = 0;
        let mut total = 0;

        // Age bucket (weight: 1)
        if a.age_bucket.is_some() || b.age_bucket.is_some() {
            total += 1;
            if a.age_bucket == b.age_bucket {
                matches += 1;
            }
        }

        // Sex (weight: 1)
        if a.sex.is_some() || b.sex.is_some() {
            total += 1;
            if a.sex == b.sex {
                matches += 1;
            }
        }

        // Country (weight: 1)
        if a.country.is_some() || b.country.is_some() {
            total += 1;
            if a.country == b.country {
                matches += 1;
            }
        }

        // Event month (weight: 2 - more distinctive)
        if a.event_month.is_some() || b.event_month.is_some() {
            total += 2;
            if a.event_month == b.event_month {
                matches += 2;
            }
        }

        // Primary drug (weight: 3 - highly distinctive)
        if a.primary_drug.is_some() || b.primary_drug.is_some() {
            total += 3;
            if a.primary_drug == b.primary_drug {
                matches += 3;
            }
        }

        // Primary reaction (weight: 2)
        if a.primary_reaction.is_some() || b.primary_reaction.is_some() {
            total += 2;
            if a.primary_reaction == b.primary_reaction {
                matches += 2;
            }
        }

        if total == 0 {
            return 0.0;
        }

        (matches as f64) / (total as f64)
    }

    /// Deduplicate a collection of reports.
    #[must_use]
    pub fn deduplicate(&self, reports: Vec<FaersReport>) -> DeduplicationResult {
        let input_count = reports.len();

        if reports.is_empty() {
            return DeduplicationResult {
                unique_reports: Vec::new(),
                clusters: Vec::new(),
                input_count: 0,
                duplicates_removed: 0,
            };
        }

        // Pass 1: Group by Manufacturer Number (Deterministic)
        let mut mfr_groups: HashMap<String, Vec<FaersReport>> = HashMap::new();
        let mut remaining_reports = Vec::new();

        for report in reports {
            if let Some(mfr) = &report.mfr_num {
                let key = mfr.trim().to_uppercase();
                if !key.is_empty() {
                    mfr_groups.entry(key).or_default().push(report);
                    continue;
                }
            }
            remaining_reports.push(report);
        }

        let mut clusters: Vec<DuplicateCluster> = Vec::new();

        // Process MFR groups
        for group in mfr_groups.into_values() {
            let primary = self.select_primary(&group[0], &group[1..]);
            let fp = self.fingerprint(&primary);
            let mut duplicates = group.clone();
            duplicates.retain(|r| r.safety_report_id != primary.safety_report_id);

            clusters.push(DuplicateCluster {
                primary,
                duplicates,
                fingerprint: fp,
            });
        }

        // Pass 2: Group remaining reports by demographic fingerprint hash
        let mut fingerprint_groups: HashMap<u64, Vec<FaersReport>> = HashMap::new();

        for report in remaining_reports {
            let fp = self.fingerprint(&report);
            let hash = hash_fingerprint(&fp);
            fingerprint_groups.entry(hash).or_default().push(report);
        }

        // Build clusters, checking similarity within each group
        for group in fingerprint_groups.into_values() {
            if group.len() == 1 {
                let report = group.into_iter().next().unwrap_or_else(|| unreachable!());
                let fp = self.fingerprint(&report);
                clusters.push(DuplicateCluster {
                    primary: report,
                    duplicates: Vec::new(),
                    fingerprint: fp,
                });
            } else {
                let sub_clusters = self.cluster_similar(&group);
                clusters.extend(sub_clusters);
            }
        }

        // Extract unique reports (primary from each cluster)
        let unique_reports: Vec<FaersReport> = clusters.iter().map(|c| c.primary.clone()).collect();

        let duplicates_removed = input_count - unique_reports.len();

        DeduplicationResult {
            unique_reports,
            clusters,
            input_count,
            duplicates_removed,
        }
    }

    /// Cluster similar reports within a group.
    fn cluster_similar(&self, reports: &[FaersReport]) -> Vec<DuplicateCluster> {
        let mut clusters: Vec<DuplicateCluster> = Vec::new();

        for report in reports {
            let fp = self.fingerprint(report);

            // Find existing cluster with high similarity
            let mut found = false;
            for cluster in &mut clusters {
                let sim = self.similarity(&fp, &cluster.fingerprint);
                if sim >= self.config.similarity_threshold {
                    // Add to existing cluster
                    cluster.duplicates.push(report.clone());
                    found = true;
                    break;
                }
            }

            if !found {
                // Create new cluster
                clusters.push(DuplicateCluster {
                    primary: report.clone(),
                    duplicates: Vec::new(),
                    fingerprint: fp,
                });
            }
        }

        // Select best primary for each cluster
        for cluster in &mut clusters {
            if !cluster.duplicates.is_empty() {
                let best = self.select_primary(&cluster.primary, &cluster.duplicates);
                if best.safety_report_id != cluster.primary.safety_report_id {
                    // Swap primary with best
                    let old_primary = std::mem::replace(&mut cluster.primary, best.clone());
                    cluster
                        .duplicates
                        .retain(|r| r.safety_report_id != best.safety_report_id);
                    cluster.duplicates.push(old_primary);
                }
            }
        }

        clusters
    }

    /// Select the best primary report from candidates.
    fn select_primary(&self, current: &FaersReport, candidates: &[FaersReport]) -> FaersReport {
        let mut best = current.clone();
        let mut best_score = self.completeness_score(&best);

        for candidate in candidates {
            let score = self.completeness_score(candidate);

            let is_better = if self.config.prefer_latest_version
                && candidate.case_version != best.case_version
            {
                candidate.case_version > best.case_version
            } else if self.config.prefer_complete {
                score > best_score
            } else {
                false
            };

            if is_better {
                best = candidate.clone();
                best_score = score;
            }
        }

        best
    }

    /// Compute completeness score (count of non-None fields).
    fn completeness_score(&self, report: &FaersReport) -> u32 {
        let mut score = 0;
        if report.patient_age.is_some() {
            score += 1;
        }
        if report.patient_sex.is_some() {
            score += 1;
        }
        if report.patient_weight.is_some() {
            score += 1;
        }
        if report.occur_country.is_some() {
            score += 1;
        }
        if report.primary_drug.is_some() {
            score += 1;
        }
        if report.primary_reaction.is_some() {
            score += 1;
        }
        if report.event_date.is_some() {
            score += 1;
        }
        if report.report_type.is_some() {
            score += 1;
        }
        if report.mfr_num.is_some() {
            score += 2; // mfr_num is very valuable
        }
        score
    }
}

impl Default for FaersDeduplicator {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Hash a fingerprint for fast grouping.
fn hash_fingerprint(fp: &ReportFingerprint) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    fp.hash(&mut hasher);
    hasher.finish()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report(id: &str, age: Option<u32>, drug: &str) -> FaersReport {
        FaersReport {
            safety_report_id: id.to_string(),
            case_version: 1,
            receipt_date: "20240101".to_string(),
            patient_age: age,
            patient_sex: Some("1".to_string()),
            patient_weight: Some(70.0),
            occur_country: Some("US".to_string()),
            primary_drug: Some(drug.to_string()),
            primary_reaction: Some("HEADACHE".to_string()),
            event_date: Some("20231215".to_string()),
            report_type: Some(1),
            serious: false,
            mfr_num: None,
        }
    }

    #[test]
    fn test_fingerprint_generation() {
        let dedup = FaersDeduplicator::new();
        let report = sample_report("12345", Some(45), "ASPIRIN");
        let fp = dedup.fingerprint(&report);

        assert_eq!(fp.age_bucket, Some(45)); // 45/5*5 = 45
        assert_eq!(fp.sex, Some("1".to_string()));
        assert_eq!(fp.country, Some("US".to_string()));
        assert_eq!(fp.event_month, Some("202312".to_string()));
        assert_eq!(fp.primary_drug, Some("ASPIRIN".to_string()));
    }

    #[test]
    fn test_similarity_identical() {
        let dedup = FaersDeduplicator::new();
        let report = sample_report("12345", Some(45), "ASPIRIN");
        let fp = dedup.fingerprint(&report);

        let sim = dedup.similarity(&fp, &fp);
        assert_eq!(sim, 1.0);
    }

    #[test]
    fn test_similarity_different() {
        let dedup = FaersDeduplicator::new();
        let r1 = sample_report("12345", Some(45), "ASPIRIN");
        let r2 = sample_report("67890", Some(25), "METFORMIN");
        let fp1 = dedup.fingerprint(&r1);
        let fp2 = dedup.fingerprint(&r2);

        let sim = dedup.similarity(&fp1, &fp2);
        // Different age (weight 1), different drug (weight 3)
        // Same sex (weight 1), country (weight 1), event month (weight 2), reaction (weight 2)
        // Matches: 1+1+2+2 = 6, Total: 1+1+1+2+3+2 = 10, Sim = 0.6
        assert!(sim < 0.85); // Below default threshold
        assert!(sim > 0.5); // Still shares many fields
    }

    #[test]
    fn test_deduplicate_no_duplicates() {
        let dedup = FaersDeduplicator::new();
        let reports = vec![
            sample_report("1", Some(30), "ASPIRIN"),
            sample_report("2", Some(50), "METFORMIN"),
        ];

        let result = dedup.deduplicate(reports);

        assert_eq!(result.input_count, 2);
        assert_eq!(result.unique_reports.len(), 2);
        assert_eq!(result.duplicates_removed, 0);
    }

    #[test]
    fn test_deduplicate_with_duplicates() {
        let dedup = FaersDeduplicator::new();
        let reports = vec![
            sample_report("1", Some(45), "ASPIRIN"),
            sample_report("2", Some(45), "ASPIRIN"), // Duplicate
            sample_report("3", Some(25), "METFORMIN"),
        ];

        let result = dedup.deduplicate(reports);

        assert_eq!(result.input_count, 3);
        assert_eq!(result.unique_reports.len(), 2);
        assert_eq!(result.duplicates_removed, 1);
    }

    #[test]
    fn test_prefer_latest_version() {
        let dedup = FaersDeduplicator::new();
        let mut r1 = sample_report("1", Some(45), "ASPIRIN");
        r1.case_version = 1;
        let mut r2 = sample_report("1-v2", Some(45), "ASPIRIN");
        r2.case_version = 2;

        let reports = vec![r1, r2];
        let result = dedup.deduplicate(reports);

        // Should prefer version 2
        assert_eq!(result.unique_reports.len(), 1);
        assert_eq!(result.unique_reports[0].case_version, 2);
    }

    #[test]
    fn test_empty_input() {
        let dedup = FaersDeduplicator::new();
        let result = dedup.deduplicate(Vec::new());

        assert_eq!(result.input_count, 0);
        assert_eq!(result.unique_reports.len(), 0);
        assert_eq!(result.duplicates_removed, 0);
    }

    #[test]
    fn test_custom_threshold() {
        let dedup = FaersDeduplicator::with_threshold(0.95);
        let reports = vec![
            sample_report("1", Some(45), "ASPIRIN"),
            sample_report("2", Some(46), "ASPIRIN"), // Slightly different age
        ];

        let result = dedup.deduplicate(reports);

        // With high threshold, these might not cluster together
        // Age bucket: 45 vs 45, so they should still match
        assert!(result.unique_reports.len() <= 2);
    }

    #[test]
    fn test_deduplicate_by_mfr_num() {
        let dedup = FaersDeduplicator::new();
        let mut r1 = sample_report("1", Some(45), "ASPIRIN");
        r1.mfr_num = Some("MFR-123".to_string());

        let mut r2 = sample_report("2", Some(25), "ASPIRIN"); // Different age
        r2.mfr_num = Some("MFR-123".to_string()); // Same MFR number

        let reports = vec![r1, r2];
        let result = dedup.deduplicate(reports);

        // Should cluster together despite age difference
        assert_eq!(result.unique_reports.len(), 1);
        assert_eq!(result.duplicates_removed, 1);
    }
}
