//! FAERS Advanced Analytics — Novel Signal Detection Algorithms.
//!
//! Three algorithms that exploit previously unused FAERS data dimensions:
//!
//! - **A82** — Outcome-Conditioned Signal Strength (→+∝+ς+κ)
//! - **A77** — Signal Velocity Detector (σ+ν+→+N)
//! - **A80** — Seriousness Cascade Detector (∂+κ+∝+→+ς)
//!
//! # Primitive Foundation
//!
//! | Algorithm | Dominant | Full T1 Set | MW (Da) | Transfer |
//! |-----------|----------|-------------|---------|----------|
//! | A82       | →+∝      | →, ∝, ς, κ | ~15     | T2-C     |
//! | A77       | σ+ν      | σ, ν, →, N | ~13     | T2-C     |
//! | A80       | ∂+∝      | ∂, κ, ∝, →, ς | ~17  | T2-C     |

use std::collections::HashMap;
use std::fmt;

// =============================================================================
// A82 — OUTCOME-CONDITIONED SIGNAL STRENGTH
// =============================================================================

/// Tier: T2-P — FAERS reaction outcome code.
///
/// Maps to `patient.reaction.reactionoutcome` in OpenFDA API.
/// Previously ignored by all disproportionality algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReactionOutcome {
    /// Code 1 — Recovered/Resolved
    Recovered,
    /// Code 2 — Recovering/Resolving
    Recovering,
    /// Code 3 — Not recovered/Not resolved
    NotRecovered,
    /// Code 4 — Recovered/Resolved with sequelae
    RecoveredWithSequelae,
    /// Code 5 — Fatal
    Fatal,
    /// Code 6 — Unknown
    Unknown,
}

impl ReactionOutcome {
    /// Parse from FAERS outcome code string.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code.trim() {
            "1" => Some(Self::Recovered),
            "2" => Some(Self::Recovering),
            "3" => Some(Self::NotRecovered),
            "4" => Some(Self::RecoveredWithSequelae),
            "5" => Some(Self::Fatal),
            "6" => Some(Self::Unknown),
            _ => None,
        }
    }

    /// Severity weight for outcome-conditioned scoring.
    ///
    /// Fatal outcomes weigh 5x a recovered outcome.
    /// This is the core insight of A82: **not all signals are equal**.
    #[must_use]
    pub const fn severity_weight(self) -> f64 {
        match self {
            Self::Fatal => 5.0,
            Self::NotRecovered => 3.0,
            Self::RecoveredWithSequelae => 2.0,
            Self::Recovering => 1.0,
            Self::Recovered => 0.0,
            Self::Unknown => 0.5,
        }
    }

    /// FAERS numeric code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Recovered => "1",
            Self::Recovering => "2",
            Self::NotRecovered => "3",
            Self::RecoveredWithSequelae => "4",
            Self::Fatal => "5",
            Self::Unknown => "6",
        }
    }
}

impl fmt::Display for ReactionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recovered => write!(f, "Recovered"),
            Self::Recovering => write!(f, "Recovering"),
            Self::NotRecovered => write!(f, "Not Recovered"),
            Self::RecoveredWithSequelae => write!(f, "Recovered with Sequelae"),
            Self::Fatal => write!(f, "Fatal"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Tier: T2-C — Outcome-conditioned signal assessment (Algorithm A82).
///
/// Adjusts standard disproportionality by weighting each case by its
/// reaction outcome severity. A drug-event pair where 80% of cases are
/// fatal is fundamentally different from one where 80% recover.
#[derive(Debug, Clone)]
pub struct OutcomeConditionedSignal {
    /// Drug name
    pub drug: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Total cases for this drug-event pair
    pub total_cases: u32,
    /// Outcome severity index: weighted average of outcome severities [0.0, 5.0]
    pub outcome_severity_index: f64,
    /// Standard PRR (unadjusted)
    pub standard_prr: f64,
    /// Adjusted PRR = standard_prr * (1 + outcome_severity_index / 5)
    pub adjusted_prr: f64,
    /// Adjustment factor applied (1.0 to 2.0)
    pub adjustment_factor: f64,
    /// Outcome distribution: count per outcome type
    pub outcome_distribution: HashMap<ReactionOutcome, u32>,
    /// Proportion of cases with fatal outcome
    pub fatality_rate: f64,
    /// Whether adjusted PRR exceeds signal threshold (default 2.0)
    pub is_signal: bool,
}

/// Input case for A82 analysis.
#[derive(Debug, Clone)]
pub struct OutcomeCase {
    /// Drug name (will be uppercased)
    pub drug: String,
    /// Event name (will be uppercased)
    pub event: String,
    /// Reaction outcome code ("1"-"6")
    pub outcome_code: String,
}

/// Configuration for A82 outcome-conditioned analysis.
#[derive(Debug, Clone)]
pub struct OutcomeConditionedConfig {
    /// PRR threshold for adjusted signal (default 2.0)
    pub prr_threshold: f64,
    /// Minimum cases required (default 3)
    pub min_cases: u32,
}

impl Default for OutcomeConditionedConfig {
    fn default() -> Self {
        Self {
            prr_threshold: 2.0,
            min_cases: 3,
        }
    }
}

/// Compute outcome-conditioned signals from raw case data (Algorithm A82).
///
/// # Arguments
///
/// * `cases` — Individual case records with drug, event, and outcome
/// * `standard_prrs` — Pre-computed standard PRR for each (drug, event) pair
/// * `config` — Thresholds and minimum case requirements
///
/// # Returns
///
/// Vector of outcome-conditioned signal assessments, sorted by adjusted PRR descending.
#[must_use]
pub fn compute_outcome_conditioned(
    cases: &[OutcomeCase],
    standard_prrs: &HashMap<(String, String), f64>,
    config: &OutcomeConditionedConfig,
) -> Vec<OutcomeConditionedSignal> {
    // Group cases by (drug, event)
    let mut groups: HashMap<(String, String), Vec<&OutcomeCase>> = HashMap::new();
    for case in cases {
        let key = (case.drug.to_uppercase(), case.event.to_uppercase());
        groups.entry(key).or_default().push(case);
    }

    let mut results: Vec<OutcomeConditionedSignal> = groups
        .into_iter()
        .filter_map(|((drug, event), group_cases)| {
            let total = group_cases.len() as u32;
            if total < config.min_cases {
                return None;
            }

            // Count outcomes
            let mut distribution: HashMap<ReactionOutcome, u32> = HashMap::new();
            let mut severity_sum = 0.0;
            let mut known_outcomes = 0u32;

            for case in &group_cases {
                if let Some(outcome) = ReactionOutcome::from_code(&case.outcome_code) {
                    *distribution.entry(outcome).or_insert(0) += 1;
                    severity_sum += outcome.severity_weight();
                    known_outcomes += 1;
                }
            }

            // Skip if no known outcomes
            if known_outcomes == 0 {
                return None;
            }

            let osi = severity_sum / f64::from(known_outcomes);
            let fatal_count = distribution
                .get(&ReactionOutcome::Fatal)
                .copied()
                .unwrap_or(0);
            let fatality_rate = f64::from(fatal_count) / f64::from(known_outcomes);

            // Look up standard PRR
            let key = (drug.clone(), event.clone());
            let standard_prr = standard_prrs.get(&key).copied().unwrap_or(1.0);

            // A82 core formula: adjust PRR by outcome severity
            let adjustment_factor = 1.0 + (osi / 5.0);
            let adjusted_prr = standard_prr * adjustment_factor;

            Some(OutcomeConditionedSignal {
                drug,
                event,
                total_cases: total,
                outcome_severity_index: osi,
                standard_prr,
                adjusted_prr,
                adjustment_factor,
                outcome_distribution: distribution,
                fatality_rate,
                is_signal: adjusted_prr >= config.prr_threshold,
            })
        })
        .collect();

    // Sort by adjusted PRR descending (most concerning first)
    results.sort_by(|a, b| {
        b.adjusted_prr
            .partial_cmp(&a.adjusted_prr)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

// =============================================================================
// A77 — SIGNAL VELOCITY DETECTOR
// =============================================================================

/// Tier: T2-P — Month bucket for temporal analysis.
///
/// Format: "YYYYMM" (e.g., "202401" for January 2024).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MonthBucket(pub String);

impl MonthBucket {
    /// Parse from FAERS date string (YYYYMMDD → YYYYMM).
    #[must_use]
    pub fn from_faers_date(date: &str) -> Option<Self> {
        if date.len() >= 6 {
            Some(Self(date[..6].to_string()))
        } else {
            None
        }
    }

    /// Get the YYYYMM string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MonthBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Tier: T2-C — Signal velocity result for a drug-event pair (Algorithm A77).
///
/// Detects *emerging* signals by measuring the rate of change (velocity)
/// and rate of acceleration in reporting frequency over time.
#[derive(Debug, Clone)]
pub struct SignalVelocity {
    /// Drug name
    pub drug: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Monthly case counts (sorted chronologically)
    pub monthly_counts: Vec<(MonthBucket, u32)>,
    /// Total cases across all months
    pub total_cases: u32,
    /// Current velocity: Δ(case_count) / Δt for the most recent period
    pub current_velocity: f64,
    /// Current acceleration: Δ(velocity) / Δt for the most recent period
    pub current_acceleration: f64,
    /// Mean velocity across all periods
    pub mean_velocity: f64,
    /// Peak velocity observed
    pub peak_velocity: f64,
    /// Month of peak velocity
    pub peak_month: Option<MonthBucket>,
    /// Number of months with data
    pub active_months: usize,
    /// Whether acceleration is positive (emerging signal)
    pub is_accelerating: bool,
    /// Whether this is an early warning (accelerating but below PRR threshold)
    pub is_early_warning: bool,
}

/// Input case for A77 velocity analysis.
#[derive(Debug, Clone)]
pub struct TemporalCase {
    /// Drug name (will be uppercased)
    pub drug: String,
    /// Event name (will be uppercased)
    pub event: String,
    /// Receipt date in YYYYMMDD format
    pub receipt_date: String,
}

/// Configuration for A77 velocity detection.
#[derive(Debug, Clone)]
pub struct VelocityConfig {
    /// Minimum months of data required (default 3)
    pub min_months: usize,
    /// Minimum total cases required (default 3)
    pub min_cases: u32,
    /// Acceleration threshold for early warning (default 0.5 cases/month^2)
    pub acceleration_threshold: f64,
    /// PRR values for cross-referencing early warnings (drug, event) -> PRR
    pub known_prrs: HashMap<(String, String), f64>,
    /// PRR threshold below which an accelerating signal is "early warning"
    pub prr_early_warning_threshold: f64,
}

impl Default for VelocityConfig {
    fn default() -> Self {
        Self {
            min_months: 3,
            min_cases: 3,
            acceleration_threshold: 0.5,
            known_prrs: HashMap::new(),
            prr_early_warning_threshold: 2.0,
        }
    }
}

/// Compute signal velocities from temporal case data (Algorithm A77).
///
/// # Arguments
///
/// * `cases` — Individual case records with drug, event, and receipt date
/// * `config` — Minimum months, case thresholds, and PRR cross-references
///
/// # Returns
///
/// Vector of velocity assessments, sorted by acceleration descending.
#[must_use]
pub fn compute_signal_velocity(
    cases: &[TemporalCase],
    config: &VelocityConfig,
) -> Vec<SignalVelocity> {
    // Group by (drug, event, month)
    let mut groups: HashMap<(String, String), HashMap<MonthBucket, u32>> = HashMap::new();

    for case in cases {
        if let Some(month) = MonthBucket::from_faers_date(&case.receipt_date) {
            let key = (case.drug.to_uppercase(), case.event.to_uppercase());
            *groups.entry(key).or_default().entry(month).or_insert(0) += 1;
        }
    }

    let mut results: Vec<SignalVelocity> = groups
        .into_iter()
        .filter_map(|((drug, event), month_counts)| {
            // Sort months chronologically
            let mut sorted: Vec<(MonthBucket, u32)> = month_counts.into_iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));

            let active_months = sorted.len();
            if active_months < config.min_months {
                return None;
            }

            let total_cases: u32 = sorted.iter().map(|(_, c)| c).sum();
            if total_cases < config.min_cases {
                return None;
            }

            // Compute velocities (Δcount/Δt) for each consecutive pair
            let velocities: Vec<f64> = sorted
                .windows(2)
                .map(|w| f64::from(w[1].1) - f64::from(w[0].1))
                .collect();

            if velocities.is_empty() {
                return None;
            }

            // Compute accelerations (Δvelocity/Δt) for each consecutive velocity pair
            let accelerations: Vec<f64> = velocities.windows(2).map(|w| w[1] - w[0]).collect();

            let current_velocity = velocities.last().copied().unwrap_or(0.0);
            let current_acceleration = accelerations.last().copied().unwrap_or(0.0);
            let mean_velocity = velocities.iter().sum::<f64>() / velocities.len() as f64;

            // Find peak velocity and its month
            let (peak_idx, peak_velocity) = velocities
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(i, &v)| (i, v))
                .unwrap_or((0, 0.0));

            let peak_month = sorted.get(peak_idx + 1).map(|(m, _)| m.clone());

            let is_accelerating = current_acceleration > config.acceleration_threshold;

            // Early warning: accelerating but below PRR threshold
            let pair_key = (drug.clone(), event.clone());
            let current_prr = config.known_prrs.get(&pair_key).copied().unwrap_or(0.0);
            let is_early_warning =
                is_accelerating && current_prr < config.prr_early_warning_threshold;

            Some(SignalVelocity {
                drug,
                event,
                monthly_counts: sorted,
                total_cases,
                current_velocity,
                current_acceleration,
                mean_velocity,
                peak_velocity,
                peak_month,
                active_months,
                is_accelerating,
                is_early_warning,
            })
        })
        .collect();

    // Sort by acceleration descending (fastest-growing first)
    results.sort_by(|a, b| {
        b.current_acceleration
            .partial_cmp(&a.current_acceleration)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

// =============================================================================
// A80 — SERIOUSNESS CASCADE DETECTOR
// =============================================================================

/// Tier: T2-P — FAERS seriousness flags.
///
/// Each case can have multiple seriousness flags set simultaneously.
/// Previously treated as binary (serious/not-serious) by all algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SeriousnessFlag {
    /// Patient died
    Death,
    /// Required hospitalization
    Hospitalization,
    /// Resulted in persistent disability
    Disabling,
    /// Caused congenital anomaly
    CongenitalAnomaly,
    /// Was life-threatening
    LifeThreatening,
    /// Other medically important condition
    OtherMedicallyImportant,
}

impl SeriousnessFlag {
    /// Severity weight per the P0-P5 patient safety hierarchy.
    ///
    /// Death = 5.0 (P0 supreme), Life-threatening = 4.0,
    /// Congenital = 4.5 (irreversible harm to vulnerable population),
    /// Disabling = 3.0, Hospitalization = 2.0, Other = 1.0.
    #[must_use]
    pub const fn weight(self) -> f64 {
        match self {
            Self::Death => 5.0,
            Self::CongenitalAnomaly => 4.5,
            Self::LifeThreatening => 4.0,
            Self::Disabling => 3.0,
            Self::Hospitalization => 2.0,
            Self::OtherMedicallyImportant => 1.0,
        }
    }

    /// All flags in descending severity order.
    #[must_use]
    pub const fn all() -> [Self; 6] {
        [
            Self::Death,
            Self::CongenitalAnomaly,
            Self::LifeThreatening,
            Self::Disabling,
            Self::Hospitalization,
            Self::OtherMedicallyImportant,
        ]
    }
}

impl fmt::Display for SeriousnessFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Death => write!(f, "Death"),
            Self::Hospitalization => write!(f, "Hospitalization"),
            Self::Disabling => write!(f, "Disabling"),
            Self::CongenitalAnomaly => write!(f, "Congenital Anomaly"),
            Self::LifeThreatening => write!(f, "Life-Threatening"),
            Self::OtherMedicallyImportant => write!(f, "Other Medically Important"),
        }
    }
}

/// Tier: T2-P — Seriousness profile for a single case.
#[derive(Debug, Clone, Default)]
pub struct CaseSeriousness {
    /// Set of active seriousness flags for this case
    pub flags: Vec<SeriousnessFlag>,
}

impl CaseSeriousness {
    /// Create from OpenFDA-style flag strings ("1" = present).
    #[must_use]
    pub fn from_openfda(
        death: Option<&str>,
        hospitalization: Option<&str>,
        disabling: Option<&str>,
        congenital: Option<&str>,
        life_threatening: Option<&str>,
        other: Option<&str>,
    ) -> Self {
        let mut flags = Vec::new();
        let is_set = |v: Option<&str>| v.is_some_and(|s| s == "1");

        if is_set(death) {
            flags.push(SeriousnessFlag::Death);
        }
        if is_set(hospitalization) {
            flags.push(SeriousnessFlag::Hospitalization);
        }
        if is_set(disabling) {
            flags.push(SeriousnessFlag::Disabling);
        }
        if is_set(congenital) {
            flags.push(SeriousnessFlag::CongenitalAnomaly);
        }
        if is_set(life_threatening) {
            flags.push(SeriousnessFlag::LifeThreatening);
        }
        if is_set(other) {
            flags.push(SeriousnessFlag::OtherMedicallyImportant);
        }

        Self { flags }
    }

    /// Cascade score: sum of severity weights for all active flags.
    ///
    /// Range: 0.0 (non-serious) to 19.5 (all flags active).
    #[must_use]
    pub fn cascade_score(&self) -> f64 {
        self.flags.iter().map(|f| f.weight()).sum()
    }

    /// Highest severity flag present.
    #[must_use]
    pub fn max_severity(&self) -> Option<SeriousnessFlag> {
        self.flags
            .iter()
            .max_by(|a, b| {
                a.weight()
                    .partial_cmp(&b.weight())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
    }
}

/// Input case for A80 cascade analysis.
#[derive(Debug, Clone)]
pub struct SeriousnessCase {
    /// Drug name (will be uppercased)
    pub drug: String,
    /// Event name (will be uppercased)
    pub event: String,
    /// Seriousness profile
    pub seriousness: CaseSeriousness,
    /// Receipt date in YYYYMMDD format (for escalation trend)
    pub receipt_date: String,
}

/// Tier: T2-C — Seriousness cascade result for a drug-event pair (Algorithm A80).
///
/// Detects signals *escalating in severity* even when total case counts
/// are stable. A drug-event pair shifting from hospitalization to death
/// reports is a P0 patient safety alarm.
#[derive(Debug, Clone)]
pub struct SeriousnessCascade {
    /// Drug name
    pub drug: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Total cases
    pub total_cases: u32,
    /// Mean cascade score across all cases [0.0, 19.5]
    pub mean_cascade_score: f64,
    /// Flag distribution: count per seriousness flag
    pub flag_distribution: HashMap<SeriousnessFlag, u32>,
    /// Flag rates: proportion per seriousness flag [0.0, 1.0]
    pub flag_rates: HashMap<SeriousnessFlag, f64>,
    /// Death rate (P0 metric)
    pub death_rate: f64,
    /// Cascade velocity: Δ(cascade_score) / Δt for the most recent period
    pub cascade_velocity: f64,
    /// Monthly cascade scores (chronological)
    pub monthly_cascade_scores: Vec<(MonthBucket, f64)>,
    /// Whether seriousness is escalating (positive cascade velocity)
    pub is_escalating: bool,
    /// Highest severity flag observed
    pub max_observed_severity: Option<SeriousnessFlag>,
    /// Whether this signal requires immediate human review (P0 criteria)
    pub requires_immediate_review: bool,
}

/// Configuration for A80 cascade detection.
#[derive(Debug, Clone)]
pub struct CascadeConfig {
    /// Minimum cases required (default 3)
    pub min_cases: u32,
    /// Cascade score threshold for concern (default 3.0)
    pub cascade_threshold: f64,
    /// Death rate threshold for immediate review (default 0.1 = 10%)
    pub death_rate_review_threshold: f64,
    /// Minimum months for escalation trend (default 2)
    pub min_months_for_trend: usize,
}

impl Default for CascadeConfig {
    fn default() -> Self {
        Self {
            min_cases: 3,
            cascade_threshold: 3.0,
            death_rate_review_threshold: 0.1,
            min_months_for_trend: 2,
        }
    }
}

/// Compute seriousness cascades from case data (Algorithm A80).
///
/// # Arguments
///
/// * `cases` — Individual case records with drug, event, seriousness, and date
/// * `config` — Thresholds for cascade scoring and escalation detection
///
/// # Returns
///
/// Vector of cascade assessments, sorted by cascade velocity descending.
#[must_use]
pub fn compute_seriousness_cascade(
    cases: &[SeriousnessCase],
    config: &CascadeConfig,
) -> Vec<SeriousnessCascade> {
    // Group by (drug, event)
    let mut groups: HashMap<(String, String), Vec<&SeriousnessCase>> = HashMap::new();
    for case in cases {
        let key = (case.drug.to_uppercase(), case.event.to_uppercase());
        groups.entry(key).or_default().push(case);
    }

    let mut results: Vec<SeriousnessCascade> = groups
        .into_iter()
        .filter_map(|((drug, event), group_cases)| {
            let total = group_cases.len() as u32;
            if total < config.min_cases {
                return None;
            }

            // Aggregate flag distribution
            let mut flag_distribution: HashMap<SeriousnessFlag, u32> = HashMap::new();
            let mut cascade_scores_sum = 0.0;
            let mut max_severity: Option<SeriousnessFlag> = None;

            for case in &group_cases {
                let score = case.seriousness.cascade_score();
                cascade_scores_sum += score;

                for flag in &case.seriousness.flags {
                    *flag_distribution.entry(*flag).or_insert(0) += 1;
                }

                if let Some(case_max) = case.seriousness.max_severity() {
                    match max_severity {
                        None => max_severity = Some(case_max),
                        Some(current) => {
                            if case_max.weight() > current.weight() {
                                max_severity = Some(case_max);
                            }
                        }
                    }
                }
            }

            let mean_cascade = cascade_scores_sum / f64::from(total);

            // Flag rates
            let flag_rates: HashMap<SeriousnessFlag, f64> = flag_distribution
                .iter()
                .map(|(&flag, &count)| (flag, f64::from(count) / f64::from(total)))
                .collect();

            let death_rate = flag_rates
                .get(&SeriousnessFlag::Death)
                .copied()
                .unwrap_or(0.0);

            // Compute monthly cascade scores for trend detection
            let mut monthly: HashMap<MonthBucket, (f64, u32)> = HashMap::new();
            for case in &group_cases {
                if let Some(month) = MonthBucket::from_faers_date(&case.receipt_date) {
                    let entry = monthly.entry(month).or_insert((0.0, 0));
                    entry.0 += case.seriousness.cascade_score();
                    entry.1 += 1;
                }
            }

            let mut monthly_cascade_scores: Vec<(MonthBucket, f64)> = monthly
                .into_iter()
                .map(|(m, (sum, count))| (m, sum / f64::from(count)))
                .collect();
            monthly_cascade_scores.sort_by(|a, b| a.0.cmp(&b.0));

            // Cascade velocity (trend in seriousness over time)
            let cascade_velocity = if monthly_cascade_scores.len() >= config.min_months_for_trend {
                let scores: Vec<f64> = monthly_cascade_scores.iter().map(|(_, s)| *s).collect();
                compute_trend_slope(&scores)
            } else {
                0.0
            };

            let is_escalating = cascade_velocity > 0.0
                && monthly_cascade_scores.len() >= config.min_months_for_trend;

            // P0 immediate review: death rate above threshold OR escalating with deaths
            let requires_immediate_review = death_rate >= config.death_rate_review_threshold
                || (is_escalating
                    && flag_distribution
                        .get(&SeriousnessFlag::Death)
                        .copied()
                        .unwrap_or(0)
                        > 0);

            Some(SeriousnessCascade {
                drug,
                event,
                total_cases: total,
                mean_cascade_score: mean_cascade,
                flag_distribution,
                flag_rates,
                death_rate,
                cascade_velocity,
                monthly_cascade_scores,
                is_escalating,
                max_observed_severity: max_severity,
                requires_immediate_review,
            })
        })
        .collect();

    // Sort by cascade velocity descending (fastest escalation first)
    results.sort_by(|a, b| {
        b.cascade_velocity
            .partial_cmp(&a.cascade_velocity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

// =============================================================================
// A78 — POLYPHARMACY INTERACTION SIGNAL
// =============================================================================

/// Tier: T2-P — Drug characterization in FAERS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DrugCharacterization {
    /// Code 1 — Primary Suspect
    Suspect,
    /// Code 2 — Concomitant
    Concomitant,
    /// Code 3 — Interacting
    Interacting,
}

impl DrugCharacterization {
    /// Parse from FAERS characterization code string.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code.trim() {
            "1" => Some(Self::Suspect),
            "2" => Some(Self::Concomitant),
            "3" => Some(Self::Interacting),
            _ => None,
        }
    }
}

impl fmt::Display for DrugCharacterization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Suspect => write!(f, "Suspect"),
            Self::Concomitant => write!(f, "Concomitant"),
            Self::Interacting => write!(f, "Interacting"),
        }
    }
}

/// Input case for A78 polypharmacy analysis.
///
/// Each case represents a single report with multiple drugs and one event.
#[derive(Debug, Clone)]
pub struct PolypharmacyCase {
    /// Case identifier (safety report ID or similar)
    pub case_id: String,
    /// List of drugs in this case, each with name and characterization
    pub drugs: Vec<(String, DrugCharacterization)>,
    /// Event name (MedDRA PT)
    pub event: String,
}

/// Tier: T2-C — Polypharmacy interaction signal result (Algorithm A78).
///
/// Computes whether a drug *pair* co-occurring in a case has disproportionate
/// reporting compared to either drug alone — revealing synergistic toxicity.
#[derive(Debug, Clone)]
pub struct PolypharmacySignal {
    /// First drug in the pair
    pub drug_a: String,
    /// Second drug in the pair
    pub drug_b: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Cases where both drugs + event co-occur
    pub pair_count: u32,
    /// Cases where drug A alone + event (excluding pair cases)
    pub drug_a_only_count: u32,
    /// Cases where drug B alone + event (excluding pair cases)
    pub drug_b_only_count: u32,
    /// Total cases with this event (across all drugs)
    pub total_event_cases: u32,
    /// Pair PRR: disproportionality of the combination
    pub pair_prr: f64,
    /// Individual PRR for drug A
    pub individual_prr_a: f64,
    /// Individual PRR for drug B
    pub individual_prr_b: f64,
    /// Interaction signal: pair_prr - max(prr_a, prr_b)
    /// Positive = synergistic toxicity, Negative = no interaction effect
    pub interaction_signal: f64,
    /// Whether synergistic interaction is detected (interaction_signal > threshold)
    pub is_synergistic: bool,
}

/// Configuration for A78 polypharmacy analysis.
#[derive(Debug, Clone)]
pub struct PolypharmacyConfig {
    /// Minimum pair co-occurrence count (default 3)
    pub min_pair_count: u32,
    /// Interaction signal threshold (default 1.0)
    pub interaction_threshold: f64,
}

impl Default for PolypharmacyConfig {
    fn default() -> Self {
        Self {
            min_pair_count: 3,
            interaction_threshold: 1.0,
        }
    }
}

/// Compute polypharmacy interaction signals (Algorithm A78).
///
/// For each drug pair (A, B) co-occurring in cases with a given event,
/// computes the pair PRR and compares to individual PRRs. A positive
/// interaction signal indicates synergistic toxicity invisible to
/// single-drug analysis.
#[must_use]
pub fn compute_polypharmacy_signals(
    cases: &[PolypharmacyCase],
    config: &PolypharmacyConfig,
) -> Vec<PolypharmacySignal> {
    // Step 1: Build case-level drug-event index
    // For each case, extract all drug names (uppercased)
    let total_cases = cases.len() as u32;
    if total_cases == 0 {
        return Vec::new();
    }

    // Count: (drug, event) → set of case_ids
    let mut drug_event_cases: HashMap<(String, String), Vec<&str>> = HashMap::new();
    // Count: (drugA, drugB, event) → set of case_ids (A < B lexically)
    let mut pair_event_cases: HashMap<(String, String, String), Vec<&str>> = HashMap::new();
    // Count: event → total cases
    let mut event_totals: HashMap<String, u32> = HashMap::new();

    for case in cases {
        let event = case.event.to_uppercase();
        *event_totals.entry(event.clone()).or_insert(0) += 1;

        let drug_names: Vec<String> = case
            .drugs
            .iter()
            .map(|(name, _)| name.to_uppercase())
            .collect();

        // Single drug-event pairs
        for drug in &drug_names {
            drug_event_cases
                .entry((drug.clone(), event.clone()))
                .or_default()
                .push(&case.case_id);
        }

        // Drug pairs (ordered lexically to avoid duplicates)
        let mut unique_drugs: Vec<&String> = drug_names.iter().collect();
        unique_drugs.sort();
        unique_drugs.dedup();

        for i in 0..unique_drugs.len() {
            for j in (i + 1)..unique_drugs.len() {
                let a = unique_drugs[i].clone();
                let b = unique_drugs[j].clone();
                pair_event_cases
                    .entry((a, b, event.clone()))
                    .or_default()
                    .push(&case.case_id);
            }
        }
    }

    // Step 2: For each pair, compute PRRs and interaction signal
    let mut results: Vec<PolypharmacySignal> = pair_event_cases
        .iter()
        .filter_map(|((drug_a, drug_b, event), pair_cases)| {
            let pair_count = pair_cases.len() as u32;
            if pair_count < config.min_pair_count {
                return None;
            }

            let event_total = event_totals.get(event).copied().unwrap_or(1).max(1);
            let a_event_count = drug_event_cases
                .get(&(drug_a.clone(), event.clone()))
                .map(|v| v.len() as u32)
                .unwrap_or(0);
            let b_event_count = drug_event_cases
                .get(&(drug_b.clone(), event.clone()))
                .map(|v| v.len() as u32)
                .unwrap_or(0);

            // PRR approximation: (pair_count / total_cases) / (event_total / total_cases)
            // Simplified: pair_count * total_cases / (pair_total * event_total)
            // Using simple ratio for tractability
            let pair_rate = f64::from(pair_count) / f64::from(total_cases);
            let event_rate = f64::from(event_total) / f64::from(total_cases);
            let pair_prr = if event_rate > 0.0 {
                pair_rate / event_rate * f64::from(total_cases) / f64::from(pair_count).max(1.0)
            } else {
                0.0
            };

            // Individual PRRs (simplified)
            let a_rate = f64::from(a_event_count) / f64::from(total_cases);
            let individual_prr_a = if event_rate > 0.0 && a_rate > 0.0 {
                a_rate / event_rate * f64::from(total_cases) / f64::from(a_event_count).max(1.0)
            } else {
                0.0
            };

            let b_rate = f64::from(b_event_count) / f64::from(total_cases);
            let individual_prr_b = if event_rate > 0.0 && b_rate > 0.0 {
                b_rate / event_rate * f64::from(total_cases) / f64::from(b_event_count).max(1.0)
            } else {
                0.0
            };

            // A78 core: interaction signal = excess disproportionality from combination
            let max_individual = individual_prr_a.max(individual_prr_b);
            let interaction_signal = pair_prr - max_individual;

            let drug_a_only = a_event_count.saturating_sub(pair_count);
            let drug_b_only = b_event_count.saturating_sub(pair_count);

            Some(PolypharmacySignal {
                drug_a: drug_a.clone(),
                drug_b: drug_b.clone(),
                event: event.clone(),
                pair_count,
                drug_a_only_count: drug_a_only,
                drug_b_only_count: drug_b_only,
                total_event_cases: event_total,
                pair_prr,
                individual_prr_a,
                individual_prr_b,
                interaction_signal,
                is_synergistic: interaction_signal > config.interaction_threshold,
            })
        })
        .collect();

    results.sort_by(|a, b| {
        b.interaction_signal
            .partial_cmp(&a.interaction_signal)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

// =============================================================================
// A79 — REPORTER-WEIGHTED DISPROPORTIONALITY
// =============================================================================

/// Tier: T2-P — FAERS reporter qualification.
///
/// Maps to `primarysource.qualification` in OpenFDA API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReporterQualification {
    /// Code 1 — Physician
    Physician,
    /// Code 2 — Pharmacist
    Pharmacist,
    /// Code 3 — Other health professional
    OtherHealthProfessional,
    /// Code 4 — Lawyer
    Lawyer,
    /// Code 5 — Consumer or non-health professional
    Consumer,
}

impl ReporterQualification {
    /// Parse from FAERS qualification code string.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code.trim() {
            "1" => Some(Self::Physician),
            "2" => Some(Self::Pharmacist),
            "3" => Some(Self::OtherHealthProfessional),
            "4" => Some(Self::Lawyer),
            "5" => Some(Self::Consumer),
            _ => None,
        }
    }

    /// Evidential weight for this reporter type.
    ///
    /// Physicians carry highest weight (clinical observation).
    /// Consumers carry less (self-report, potential misattribution).
    /// Lawyers carry least (litigation bias).
    #[must_use]
    pub const fn weight(self) -> f64 {
        match self {
            Self::Physician => 1.0,
            Self::Pharmacist => 0.9,
            Self::OtherHealthProfessional => 0.8,
            Self::Consumer => 0.6,
            Self::Lawyer => 0.5,
        }
    }

    /// All qualifications.
    #[must_use]
    pub const fn all() -> [Self; 5] {
        [
            Self::Physician,
            Self::Pharmacist,
            Self::OtherHealthProfessional,
            Self::Lawyer,
            Self::Consumer,
        ]
    }
}

impl fmt::Display for ReporterQualification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Physician => write!(f, "Physician"),
            Self::Pharmacist => write!(f, "Pharmacist"),
            Self::OtherHealthProfessional => write!(f, "Other Health Professional"),
            Self::Lawyer => write!(f, "Lawyer"),
            Self::Consumer => write!(f, "Consumer"),
        }
    }
}

/// Input case for A79 reporter-weighted analysis.
#[derive(Debug, Clone)]
pub struct ReporterCase {
    /// Drug name
    pub drug: String,
    /// Event name (MedDRA PT)
    pub event: String,
    /// Reporter qualification code ("1"-"5")
    pub qualification_code: String,
}

/// Tier: T2-C — Reporter-weighted signal result (Algorithm A79).
#[derive(Debug, Clone)]
pub struct ReporterWeightedSignal {
    /// Drug name
    pub drug: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Total raw case count
    pub raw_count: u32,
    /// Weighted case count (sum of reporter weights)
    pub weighted_count: f64,
    /// Reporter distribution: count per qualification
    pub reporter_distribution: HashMap<ReporterQualification, u32>,
    /// Reporter diversity index (Shannon entropy, 0.0-ln(5))
    pub reporter_diversity_index: f64,
    /// Normalized diversity (0.0-1.0, 1.0 = all reporter types equally represented)
    pub normalized_diversity: f64,
    /// Mean reporter weight for this signal
    pub mean_reporter_weight: f64,
    /// Whether signal has multi-source confirmation (diversity > 0.5)
    pub is_multi_source_confirmed: bool,
    /// Confidence bonus from reporter diversity
    pub confidence_bonus: f64,
}

/// Configuration for A79 reporter-weighted analysis.
#[derive(Debug, Clone)]
pub struct ReporterWeightedConfig {
    /// Minimum raw cases (default 3)
    pub min_cases: u32,
    /// Diversity threshold for multi-source confirmation (default 0.5)
    pub diversity_threshold: f64,
}

impl Default for ReporterWeightedConfig {
    fn default() -> Self {
        Self {
            min_cases: 3,
            diversity_threshold: 0.5,
        }
    }
}

/// Compute reporter-weighted signals (Algorithm A79).
///
/// Weights each case by reporter qualification and computes reporter
/// diversity index. High diversity = signal confirmed across independent
/// reporter types (more credible).
#[must_use]
pub fn compute_reporter_weighted(
    cases: &[ReporterCase],
    config: &ReporterWeightedConfig,
) -> Vec<ReporterWeightedSignal> {
    let mut groups: HashMap<(String, String), Vec<&ReporterCase>> = HashMap::new();
    for case in cases {
        let key = (case.drug.to_uppercase(), case.event.to_uppercase());
        groups.entry(key).or_default().push(case);
    }

    let max_entropy = (ReporterQualification::all().len() as f64).ln();

    let mut results: Vec<ReporterWeightedSignal> = groups
        .into_iter()
        .filter_map(|((drug, event), group_cases)| {
            let raw_count = group_cases.len() as u32;
            if raw_count < config.min_cases {
                return None;
            }

            let mut distribution: HashMap<ReporterQualification, u32> = HashMap::new();
            let mut weighted_sum = 0.0;
            let mut known_reporters = 0u32;

            for case in &group_cases {
                if let Some(qual) = ReporterQualification::from_code(&case.qualification_code) {
                    *distribution.entry(qual).or_insert(0) += 1;
                    weighted_sum += qual.weight();
                    known_reporters += 1;
                }
            }

            if known_reporters == 0 {
                return None;
            }

            let mean_weight = weighted_sum / f64::from(known_reporters);

            // Shannon entropy for reporter diversity
            let total_f = f64::from(known_reporters);
            let entropy: f64 = distribution
                .values()
                .filter(|&&c| c > 0)
                .map(|&c| {
                    let p = f64::from(c) / total_f;
                    -p * p.ln()
                })
                .sum();

            let normalized_diversity = if max_entropy > 0.0 {
                entropy / max_entropy
            } else {
                0.0
            };

            let is_multi_source = normalized_diversity >= config.diversity_threshold;
            // Confidence bonus: 0% to 20% based on diversity
            let confidence_bonus = normalized_diversity * 0.2;

            Some(ReporterWeightedSignal {
                drug,
                event,
                raw_count,
                weighted_count: weighted_sum,
                reporter_distribution: distribution,
                reporter_diversity_index: entropy,
                normalized_diversity,
                mean_reporter_weight: mean_weight,
                is_multi_source_confirmed: is_multi_source,
                confidence_bonus,
            })
        })
        .collect();

    // Sort by weighted count descending
    results.sort_by(|a, b| {
        b.weighted_count
            .partial_cmp(&a.weighted_count)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

// =============================================================================
// A81 — GEOGRAPHIC SIGNAL DIVERGENCE
// =============================================================================

/// Input case for A81 geographic analysis.
#[derive(Debug, Clone)]
pub struct GeographicCase {
    /// Drug name
    pub drug: String,
    /// Event name (MedDRA PT)
    pub event: String,
    /// Occurrence country (ISO 2-letter code)
    pub country: String,
}

/// Per-country signal data within a geographic divergence result.
#[derive(Debug, Clone)]
pub struct CountrySignal {
    /// Country code
    pub country: String,
    /// Case count in this country
    pub count: u32,
    /// Country-specific reporting rate (count / country_total)
    pub reporting_rate: f64,
}

/// Tier: T2-C — Geographic signal divergence result (Algorithm A81).
///
/// Detects drug-event pairs with significantly different reporting rates
/// across countries — suggesting pharmacogenomic effects, regulatory
/// gaps, or reporting biases.
#[derive(Debug, Clone)]
pub struct GeographicDivergence {
    /// Drug name
    pub drug: String,
    /// Event (MedDRA PT)
    pub event: String,
    /// Total cases across all countries
    pub total_cases: u32,
    /// Number of countries reporting this pair
    pub reporting_countries: usize,
    /// Per-country signal data (sorted by rate descending)
    pub country_signals: Vec<CountrySignal>,
    /// Divergence ratio: max_rate / min_rate
    pub divergence_ratio: f64,
    /// Country with highest reporting rate
    pub highest_country: String,
    /// Country with lowest reporting rate
    pub lowest_country: String,
    /// Chi-squared heterogeneity statistic
    pub chi_squared: f64,
    /// Chi-squared p-value approximation (1 - chi2_cdf)
    pub heterogeneity_p: f64,
    /// Whether statistically significant heterogeneity exists
    pub is_heterogeneous: bool,
    /// Whether divergence exceeds threshold
    pub is_divergent: bool,
}

/// Configuration for A81 geographic divergence analysis.
#[derive(Debug, Clone)]
pub struct GeographicConfig {
    /// Minimum total cases (default 5)
    pub min_cases: u32,
    /// Minimum countries required (default 2)
    pub min_countries: usize,
    /// Divergence ratio threshold for flagging (default 3.0)
    pub divergence_threshold: f64,
    /// P-value threshold for heterogeneity (default 0.05)
    pub p_value_threshold: f64,
    /// Minimum cases per country to include (default 2)
    pub min_country_cases: u32,
}

impl Default for GeographicConfig {
    fn default() -> Self {
        Self {
            min_cases: 5,
            min_countries: 2,
            divergence_threshold: 3.0,
            p_value_threshold: 0.05,
            min_country_cases: 2,
        }
    }
}

/// Compute geographic signal divergences (Algorithm A81).
///
/// For each drug-event pair, computes per-country reporting rates and
/// tests for significant geographic heterogeneity using chi-squared.
#[must_use]
pub fn compute_geographic_divergence(
    cases: &[GeographicCase],
    config: &GeographicConfig,
) -> Vec<GeographicDivergence> {
    // Count total cases per country (denominator for rates)
    let mut country_totals: HashMap<String, u32> = HashMap::new();
    for case in cases {
        let country = case.country.to_uppercase();
        *country_totals.entry(country).or_insert(0) += 1;
    }

    // Group by (drug, event)
    let mut groups: HashMap<(String, String), HashMap<String, u32>> = HashMap::new();
    for case in cases {
        let key = (case.drug.to_uppercase(), case.event.to_uppercase());
        let country = case.country.to_uppercase();
        *groups.entry(key).or_default().entry(country).or_insert(0) += 1;
    }

    let total_all = cases.len() as f64;

    let mut results: Vec<GeographicDivergence> = groups
        .into_iter()
        .filter_map(|((drug, event), country_counts)| {
            let total_cases: u32 = country_counts.values().sum();
            if total_cases < config.min_cases {
                return None;
            }

            // Filter to countries with enough cases
            let filtered: Vec<(String, u32)> = country_counts
                .into_iter()
                .filter(|(_, count)| *count >= config.min_country_cases)
                .collect();

            if filtered.len() < config.min_countries {
                return None;
            }

            // Compute per-country reporting rates
            let mut country_signals: Vec<CountrySignal> = filtered
                .iter()
                .map(|(country, count)| {
                    let country_total = country_totals.get(country).copied().unwrap_or(1).max(1);
                    let rate = f64::from(*count) / f64::from(country_total);
                    CountrySignal {
                        country: country.clone(),
                        count: *count,
                        reporting_rate: rate,
                    }
                })
                .collect();

            country_signals.sort_by(|a, b| {
                b.reporting_rate
                    .partial_cmp(&a.reporting_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let max_rate = country_signals
                .first()
                .map(|s| s.reporting_rate)
                .unwrap_or(0.0);
            let min_rate = country_signals
                .last()
                .map(|s| s.reporting_rate)
                .unwrap_or(0.0);

            let highest = country_signals
                .first()
                .map(|s| s.country.clone())
                .unwrap_or_default();
            let lowest = country_signals
                .last()
                .map(|s| s.country.clone())
                .unwrap_or_default();

            let divergence_ratio = if min_rate > f64::EPSILON {
                max_rate / min_rate
            } else if max_rate > f64::EPSILON {
                f64::INFINITY
            } else {
                1.0
            };

            // Chi-squared heterogeneity test
            // H0: reporting rate is the same across all countries
            let overall_rate = f64::from(total_cases) / total_all;
            let chi_sq =
                compute_chi_squared_heterogeneity(&filtered, &country_totals, overall_rate);

            let df = (filtered.len() as f64 - 1.0).max(1.0);
            let p_value = chi_squared_p_value(chi_sq, df);

            let is_heterogeneous = p_value < config.p_value_threshold;
            let is_divergent = divergence_ratio >= config.divergence_threshold && is_heterogeneous;

            Some(GeographicDivergence {
                drug,
                event,
                total_cases,
                reporting_countries: filtered.len(),
                country_signals,
                divergence_ratio,
                highest_country: highest,
                lowest_country: lowest,
                chi_squared: chi_sq,
                heterogeneity_p: p_value,
                is_heterogeneous,
                is_divergent,
            })
        })
        .collect();

    results.sort_by(|a, b| {
        b.divergence_ratio
            .partial_cmp(&a.divergence_ratio)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

/// Compute chi-squared heterogeneity statistic across countries.
fn compute_chi_squared_heterogeneity(
    country_counts: &[(String, u32)],
    country_totals: &HashMap<String, u32>,
    overall_rate: f64,
) -> f64 {
    let mut chi_sq = 0.0;
    for (country, observed) in country_counts {
        let total = f64::from(country_totals.get(country).copied().unwrap_or(1).max(1));
        let expected = overall_rate * total;
        if expected > 0.0 {
            let diff = f64::from(*observed) - expected;
            chi_sq += (diff * diff) / expected;
        }
    }
    chi_sq
}

/// Approximate chi-squared p-value using Wilson-Hilferty transformation.
///
/// For df >= 1, approximates 1 - CDF(chi_sq, df) via normal approximation.
/// Adequate for signal detection thresholds (not regulatory submission).
fn chi_squared_p_value(chi_sq: f64, df: f64) -> f64 {
    if chi_sq <= 0.0 || df <= 0.0 {
        return 1.0;
    }

    // Wilson-Hilferty normal approximation
    let term = (chi_sq / df).powf(1.0 / 3.0);
    let z = (term - (1.0 - 2.0 / (9.0 * df))) / (2.0 / (9.0 * df)).sqrt();

    // Standard normal CDF approximation (Abramowitz & Stegun)
    let p = standard_normal_cdf(z);
    (1.0 - p).clamp(0.0, 1.0)
}

/// Standard normal CDF approximation (Abramowitz & Stegun, formula 7.1.26).
fn standard_normal_cdf(z: f64) -> f64 {
    if z < -8.0 {
        return 0.0;
    }
    if z > 8.0 {
        return 1.0;
    }

    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if z < 0.0 { -1.0 } else { 1.0 };
    let x = z.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    0.5 * (1.0 + sign * y)
}

// =============================================================================
// SHARED UTILITIES
// =============================================================================

/// Compute linear trend slope using least-squares regression.
///
/// Given y-values at equally-spaced x-values (0, 1, 2, ...),
/// returns the slope of the best-fit line.
fn compute_trend_slope(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    if n < 2.0 {
        return 0.0;
    }

    let x_mean = (n - 1.0) / 2.0;
    let y_mean: f64 = values.iter().sum::<f64>() / n;

    let mut numerator = 0.0;
    let mut denominator = 0.0;

    for (i, &y) in values.iter().enumerate() {
        let x = i as f64;
        numerator += (x - x_mean) * (y - y_mean);
        denominator += (x - x_mean) * (x - x_mean);
    }

    if denominator.abs() < f64::EPSILON {
        return 0.0;
    }

    numerator / denominator
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // A82 Tests
    // =========================================================================

    #[test]
    fn test_reaction_outcome_parsing() {
        assert_eq!(
            ReactionOutcome::from_code("1"),
            Some(ReactionOutcome::Recovered)
        );
        assert_eq!(
            ReactionOutcome::from_code("5"),
            Some(ReactionOutcome::Fatal)
        );
        assert_eq!(ReactionOutcome::from_code("7"), None);
        assert_eq!(ReactionOutcome::from_code(""), None);
    }

    #[test]
    fn test_outcome_severity_weights() {
        assert!((ReactionOutcome::Fatal.severity_weight() - 5.0).abs() < f64::EPSILON);
        assert!((ReactionOutcome::Recovered.severity_weight() - 0.0).abs() < f64::EPSILON);
        assert!(
            ReactionOutcome::NotRecovered.severity_weight()
                > ReactionOutcome::Recovering.severity_weight()
        );
    }

    #[test]
    fn test_outcome_conditioned_basic() {
        let cases = vec![
            OutcomeCase {
                drug: "ASPIRIN".into(),
                event: "HEADACHE".into(),
                outcome_code: "5".into(), // Fatal
            },
            OutcomeCase {
                drug: "ASPIRIN".into(),
                event: "HEADACHE".into(),
                outcome_code: "5".into(), // Fatal
            },
            OutcomeCase {
                drug: "ASPIRIN".into(),
                event: "HEADACHE".into(),
                outcome_code: "1".into(), // Recovered
            },
        ];

        let mut prrs = HashMap::new();
        prrs.insert(("ASPIRIN".into(), "HEADACHE".into()), 2.0);

        let results =
            compute_outcome_conditioned(&cases, &prrs, &OutcomeConditionedConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.total_cases, 3);
        // OSI = (5.0 + 5.0 + 0.0) / 3 = 3.333...
        assert!((r.outcome_severity_index - 10.0 / 3.0).abs() < 0.01);
        // Adjustment = 1 + (3.333/5) = 1.667
        // Adjusted PRR = 2.0 * 1.667 = 3.333
        assert!(r.adjusted_prr > 3.0);
        assert!(r.is_signal);
    }

    #[test]
    fn test_outcome_conditioned_all_recovered() {
        let cases = vec![
            OutcomeCase {
                drug: "DRUG_A".into(),
                event: "NAUSEA".into(),
                outcome_code: "1".into(),
            },
            OutcomeCase {
                drug: "DRUG_A".into(),
                event: "NAUSEA".into(),
                outcome_code: "1".into(),
            },
            OutcomeCase {
                drug: "DRUG_A".into(),
                event: "NAUSEA".into(),
                outcome_code: "1".into(),
            },
        ];

        let mut prrs = HashMap::new();
        prrs.insert(("DRUG_A".into(), "NAUSEA".into()), 2.5);

        let results =
            compute_outcome_conditioned(&cases, &prrs, &OutcomeConditionedConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        // OSI = 0.0 (all recovered)
        assert!(r.outcome_severity_index.abs() < f64::EPSILON);
        // Adjustment = 1 + 0/5 = 1.0 (no adjustment)
        assert!((r.adjusted_prr - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_outcome_conditioned_below_min_cases() {
        let cases = vec![OutcomeCase {
            drug: "RARE".into(),
            event: "EVENT".into(),
            outcome_code: "5".into(),
        }];

        let prrs = HashMap::new();
        let config = OutcomeConditionedConfig {
            min_cases: 3,
            ..Default::default()
        };

        let results = compute_outcome_conditioned(&cases, &prrs, &config);
        assert!(results.is_empty());
    }

    #[test]
    fn test_outcome_conditioned_fatality_rate() {
        let cases = vec![
            OutcomeCase {
                drug: "D".into(),
                event: "E".into(),
                outcome_code: "5".into(),
            },
            OutcomeCase {
                drug: "D".into(),
                event: "E".into(),
                outcome_code: "5".into(),
            },
            OutcomeCase {
                drug: "D".into(),
                event: "E".into(),
                outcome_code: "1".into(),
            },
            OutcomeCase {
                drug: "D".into(),
                event: "E".into(),
                outcome_code: "2".into(),
            },
        ];

        let prrs = HashMap::new();
        let results =
            compute_outcome_conditioned(&cases, &prrs, &OutcomeConditionedConfig::default());

        assert_eq!(results.len(), 1);
        assert!((results[0].fatality_rate - 0.5).abs() < f64::EPSILON);
    }

    // =========================================================================
    // A77 Tests
    // =========================================================================

    #[test]
    fn test_month_bucket_parsing() {
        assert_eq!(
            MonthBucket::from_faers_date("20240115"),
            Some(MonthBucket("202401".into()))
        );
        assert_eq!(MonthBucket::from_faers_date("20241"), None);
        assert_eq!(
            MonthBucket::from_faers_date("202412"),
            Some(MonthBucket("202412".into()))
        );
    }

    #[test]
    fn test_signal_velocity_basic() {
        // Accelerating signal: 1, 2, 4, 8 cases per month
        let mut cases = Vec::new();
        for _ in 0..1 {
            cases.push(TemporalCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                receipt_date: "20240101".into(),
            });
        }
        for _ in 0..2 {
            cases.push(TemporalCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                receipt_date: "20240201".into(),
            });
        }
        for _ in 0..4 {
            cases.push(TemporalCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                receipt_date: "20240301".into(),
            });
        }
        for _ in 0..8 {
            cases.push(TemporalCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                receipt_date: "20240401".into(),
            });
        }

        let results = compute_signal_velocity(&cases, &VelocityConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.total_cases, 15);
        assert_eq!(r.active_months, 4);
        // Velocities: 1, 2, 4 → accelerations: 1, 2 → current acceleration = 2
        assert!(r.current_acceleration > 0.0);
        assert!(r.is_accelerating);
    }

    #[test]
    fn test_signal_velocity_decelerating() {
        // Decelerating signal: 8, 4, 2, 1 cases per month
        let mut cases = Vec::new();
        for _ in 0..8 {
            cases.push(TemporalCase {
                drug: "DRUG".into(),
                event: "EVT".into(),
                receipt_date: "20240101".into(),
            });
        }
        for _ in 0..4 {
            cases.push(TemporalCase {
                drug: "DRUG".into(),
                event: "EVT".into(),
                receipt_date: "20240201".into(),
            });
        }
        for _ in 0..2 {
            cases.push(TemporalCase {
                drug: "DRUG".into(),
                event: "EVT".into(),
                receipt_date: "20240301".into(),
            });
        }
        for _ in 0..1 {
            cases.push(TemporalCase {
                drug: "DRUG".into(),
                event: "EVT".into(),
                receipt_date: "20240401".into(),
            });
        }

        let results = compute_signal_velocity(&cases, &VelocityConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        // Velocities: -4, -2, -1 → accelerations: 2, 1 → technically accelerating
        // but velocity is negative (signal is fading, just slower)
        assert!(r.current_velocity < 0.0);
    }

    #[test]
    fn test_signal_velocity_early_warning() {
        let mut cases = Vec::new();
        for _ in 0..1 {
            cases.push(TemporalCase {
                drug: "NEW_DRUG".into(),
                event: "RARE_EVENT".into(),
                receipt_date: "20240101".into(),
            });
        }
        for _ in 0..2 {
            cases.push(TemporalCase {
                drug: "NEW_DRUG".into(),
                event: "RARE_EVENT".into(),
                receipt_date: "20240201".into(),
            });
        }
        for _ in 0..4 {
            cases.push(TemporalCase {
                drug: "NEW_DRUG".into(),
                event: "RARE_EVENT".into(),
                receipt_date: "20240301".into(),
            });
        }

        let mut config = VelocityConfig::default();
        // PRR is below threshold — this should be an early warning
        config
            .known_prrs
            .insert(("NEW_DRUG".into(), "RARE_EVENT".into()), 1.2);
        config.prr_early_warning_threshold = 2.0;

        let results = compute_signal_velocity(&cases, &config);

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.is_accelerating);
        assert!(r.is_early_warning);
    }

    #[test]
    fn test_signal_velocity_insufficient_months() {
        let cases = vec![
            TemporalCase {
                drug: "D".into(),
                event: "E".into(),
                receipt_date: "20240101".into(),
            },
            TemporalCase {
                drug: "D".into(),
                event: "E".into(),
                receipt_date: "20240101".into(),
            },
            TemporalCase {
                drug: "D".into(),
                event: "E".into(),
                receipt_date: "20240201".into(),
            },
        ];

        let results = compute_signal_velocity(&cases, &VelocityConfig::default());
        // Only 2 months, default min is 3
        assert!(results.is_empty());
    }

    // =========================================================================
    // A80 Tests
    // =========================================================================

    #[test]
    fn test_seriousness_flag_weights() {
        assert!(SeriousnessFlag::Death.weight() > SeriousnessFlag::LifeThreatening.weight());
        assert!(
            SeriousnessFlag::CongenitalAnomaly.weight() > SeriousnessFlag::LifeThreatening.weight()
        );
        assert!(SeriousnessFlag::LifeThreatening.weight() > SeriousnessFlag::Disabling.weight());
        assert!(SeriousnessFlag::Disabling.weight() > SeriousnessFlag::Hospitalization.weight());
        assert!(
            SeriousnessFlag::Hospitalization.weight()
                > SeriousnessFlag::OtherMedicallyImportant.weight()
        );
    }

    #[test]
    fn test_case_seriousness_from_openfda() {
        let cs = CaseSeriousness::from_openfda(
            Some("1"), // death
            None,
            None,
            None,
            Some("1"), // life-threatening
            None,
        );
        assert_eq!(cs.flags.len(), 2);
        assert!((cs.cascade_score() - 9.0).abs() < f64::EPSILON); // 5.0 + 4.0
    }

    #[test]
    fn test_case_seriousness_cascade_score() {
        let cs = CaseSeriousness {
            flags: vec![SeriousnessFlag::Death, SeriousnessFlag::Hospitalization],
        };
        assert!((cs.cascade_score() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_case_seriousness_max_severity() {
        let cs = CaseSeriousness {
            flags: vec![
                SeriousnessFlag::Hospitalization,
                SeriousnessFlag::LifeThreatening,
            ],
        };
        assert_eq!(cs.max_severity(), Some(SeriousnessFlag::LifeThreatening));
    }

    #[test]
    fn test_seriousness_cascade_basic() {
        let cases = vec![
            SeriousnessCase {
                drug: "DRUG_A".into(),
                event: "EVENT_X".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Hospitalization],
                },
                receipt_date: "20240101".into(),
            },
            SeriousnessCase {
                drug: "DRUG_A".into(),
                event: "EVENT_X".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::LifeThreatening],
                },
                receipt_date: "20240201".into(),
            },
            SeriousnessCase {
                drug: "DRUG_A".into(),
                event: "EVENT_X".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Death],
                },
                receipt_date: "20240301".into(),
            },
        ];

        let results = compute_seriousness_cascade(&cases, &CascadeConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.total_cases, 3);
        // Mean cascade: (2.0 + 4.0 + 5.0) / 3 = 3.667
        assert!(r.mean_cascade_score > 3.0);
        // Escalating: monthly scores 2.0 → 4.0 → 5.0 (positive slope)
        assert!(r.cascade_velocity > 0.0);
        assert!(r.is_escalating);
        assert_eq!(r.max_observed_severity, Some(SeriousnessFlag::Death));
    }

    #[test]
    fn test_seriousness_cascade_requires_review() {
        let cases = vec![
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Death],
                },
                receipt_date: "20240101".into(),
            },
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Death],
                },
                receipt_date: "20240201".into(),
            },
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Hospitalization],
                },
                receipt_date: "20240301".into(),
            },
        ];

        let config = CascadeConfig {
            death_rate_review_threshold: 0.5,
            ..Default::default()
        };
        let results = compute_seriousness_cascade(&cases, &config);

        assert_eq!(results.len(), 1);
        let r = &results[0];
        // Death rate = 2/3 = 0.667 > 0.5 threshold
        assert!(r.death_rate > 0.5);
        assert!(r.requires_immediate_review);
    }

    #[test]
    fn test_seriousness_cascade_non_escalating() {
        let cases = vec![
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Death],
                },
                receipt_date: "20240101".into(),
            },
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::LifeThreatening],
                },
                receipt_date: "20240201".into(),
            },
            SeriousnessCase {
                drug: "D".into(),
                event: "E".into(),
                seriousness: CaseSeriousness {
                    flags: vec![SeriousnessFlag::Hospitalization],
                },
                receipt_date: "20240301".into(),
            },
        ];

        let results = compute_seriousness_cascade(&cases, &CascadeConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        // Seriousness is decreasing: 5.0 → 4.0 → 2.0
        assert!(r.cascade_velocity < 0.0);
        assert!(!r.is_escalating);
    }

    // =========================================================================
    // Utility Tests
    // =========================================================================

    #[test]
    fn test_trend_slope_flat() {
        let values = vec![5.0, 5.0, 5.0, 5.0];
        assert!(compute_trend_slope(&values).abs() < f64::EPSILON);
    }

    #[test]
    fn test_trend_slope_positive() {
        let values = vec![1.0, 2.0, 3.0, 4.0];
        assert!((compute_trend_slope(&values) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_trend_slope_negative() {
        let values = vec![4.0, 3.0, 2.0, 1.0];
        assert!((compute_trend_slope(&values) - (-1.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_trend_slope_single_value() {
        let values = vec![42.0];
        assert!(compute_trend_slope(&values).abs() < f64::EPSILON);
    }

    #[test]
    fn test_trend_slope_empty() {
        let values: Vec<f64> = Vec::new();
        assert!(compute_trend_slope(&values).abs() < f64::EPSILON);
    }

    #[test]
    fn test_outcome_display() {
        assert_eq!(format!("{}", ReactionOutcome::Fatal), "Fatal");
        assert_eq!(format!("{}", ReactionOutcome::Recovered), "Recovered");
    }

    #[test]
    fn test_seriousness_flag_display() {
        assert_eq!(format!("{}", SeriousnessFlag::Death), "Death");
        assert_eq!(
            format!("{}", SeriousnessFlag::Hospitalization),
            "Hospitalization"
        );
    }

    #[test]
    fn test_seriousness_flag_all_count() {
        assert_eq!(SeriousnessFlag::all().len(), 6);
    }

    #[test]
    fn test_outcome_case_normalization() {
        let cases = vec![
            OutcomeCase {
                drug: "aspirin".into(),
                event: "headache".into(),
                outcome_code: "1".into(),
            },
            OutcomeCase {
                drug: "ASPIRIN".into(),
                event: "HEADACHE".into(),
                outcome_code: "1".into(),
            },
            OutcomeCase {
                drug: "Aspirin".into(),
                event: "Headache".into(),
                outcome_code: "1".into(),
            },
        ];
        let prrs = HashMap::new();
        let results =
            compute_outcome_conditioned(&cases, &prrs, &OutcomeConditionedConfig::default());
        // All should group into one (drug, event) pair
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].total_cases, 3);
    }

    // =========================================================================
    // A78 Tests — Polypharmacy Interaction Signal
    // =========================================================================

    #[test]
    fn test_drug_characterization_parsing() {
        assert_eq!(
            DrugCharacterization::from_code("1"),
            Some(DrugCharacterization::Suspect)
        );
        assert_eq!(
            DrugCharacterization::from_code("2"),
            Some(DrugCharacterization::Concomitant)
        );
        assert_eq!(
            DrugCharacterization::from_code("3"),
            Some(DrugCharacterization::Interacting)
        );
        assert_eq!(DrugCharacterization::from_code("4"), None);
        assert_eq!(DrugCharacterization::from_code(""), None);
    }

    #[test]
    fn test_drug_characterization_display() {
        assert_eq!(format!("{}", DrugCharacterization::Suspect), "Suspect");
        assert_eq!(
            format!("{}", DrugCharacterization::Concomitant),
            "Concomitant"
        );
        assert_eq!(
            format!("{}", DrugCharacterization::Interacting),
            "Interacting"
        );
    }

    #[test]
    fn test_polypharmacy_basic_synergistic() {
        // Cases where DRUG_A + DRUG_B co-occur more than either alone with EVENT_X
        let cases = vec![
            // 3 cases with both drugs → pair
            PolypharmacyCase {
                case_id: "1".into(),
                drugs: vec![
                    ("DRUG_A".into(), DrugCharacterization::Suspect),
                    ("DRUG_B".into(), DrugCharacterization::Concomitant),
                ],
                event: "EVENT_X".into(),
            },
            PolypharmacyCase {
                case_id: "2".into(),
                drugs: vec![
                    ("DRUG_A".into(), DrugCharacterization::Suspect),
                    ("DRUG_B".into(), DrugCharacterization::Concomitant),
                ],
                event: "EVENT_X".into(),
            },
            PolypharmacyCase {
                case_id: "3".into(),
                drugs: vec![
                    ("DRUG_A".into(), DrugCharacterization::Suspect),
                    ("DRUG_B".into(), DrugCharacterization::Concomitant),
                ],
                event: "EVENT_X".into(),
            },
            // 1 case with DRUG_A alone
            PolypharmacyCase {
                case_id: "4".into(),
                drugs: vec![("DRUG_A".into(), DrugCharacterization::Suspect)],
                event: "EVENT_X".into(),
            },
            // 1 case with DRUG_B alone
            PolypharmacyCase {
                case_id: "5".into(),
                drugs: vec![("DRUG_B".into(), DrugCharacterization::Suspect)],
                event: "EVENT_X".into(),
            },
            // Background noise: unrelated drug-event
            PolypharmacyCase {
                case_id: "6".into(),
                drugs: vec![("DRUG_C".into(), DrugCharacterization::Suspect)],
                event: "OTHER_EVENT".into(),
            },
        ];

        let results = compute_polypharmacy_signals(&cases, &PolypharmacyConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.pair_count, 3);
        assert_eq!(r.drug_a.as_str(), "DRUG_A");
        assert_eq!(r.drug_b.as_str(), "DRUG_B");
    }

    #[test]
    fn test_polypharmacy_below_min_pair_count() {
        let cases = vec![
            PolypharmacyCase {
                case_id: "1".into(),
                drugs: vec![
                    ("A".into(), DrugCharacterization::Suspect),
                    ("B".into(), DrugCharacterization::Suspect),
                ],
                event: "E".into(),
            },
            PolypharmacyCase {
                case_id: "2".into(),
                drugs: vec![
                    ("A".into(), DrugCharacterization::Suspect),
                    ("B".into(), DrugCharacterization::Suspect),
                ],
                event: "E".into(),
            },
        ];

        let results = compute_polypharmacy_signals(&cases, &PolypharmacyConfig::default());
        // Only 2 pair co-occurrences, default min is 3
        assert!(results.is_empty());
    }

    #[test]
    fn test_polypharmacy_empty_cases() {
        let results = compute_polypharmacy_signals(&[], &PolypharmacyConfig::default());
        assert!(results.is_empty());
    }

    #[test]
    fn test_polypharmacy_normalization() {
        let cases = vec![
            PolypharmacyCase {
                case_id: "1".into(),
                drugs: vec![
                    ("drug_a".into(), DrugCharacterization::Suspect),
                    ("drug_b".into(), DrugCharacterization::Suspect),
                ],
                event: "event_x".into(),
            },
            PolypharmacyCase {
                case_id: "2".into(),
                drugs: vec![
                    ("DRUG_A".into(), DrugCharacterization::Suspect),
                    ("DRUG_B".into(), DrugCharacterization::Suspect),
                ],
                event: "EVENT_X".into(),
            },
            PolypharmacyCase {
                case_id: "3".into(),
                drugs: vec![
                    ("Drug_A".into(), DrugCharacterization::Suspect),
                    ("Drug_B".into(), DrugCharacterization::Suspect),
                ],
                event: "Event_X".into(),
            },
        ];

        let results = compute_polypharmacy_signals(&cases, &PolypharmacyConfig::default());
        // All normalize to same pair → 1 result
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].pair_count, 3);
    }

    // =========================================================================
    // A79 Tests — Reporter-Weighted Disproportionality
    // =========================================================================

    #[test]
    fn test_reporter_qualification_parsing() {
        assert_eq!(
            ReporterQualification::from_code("1"),
            Some(ReporterQualification::Physician)
        );
        assert_eq!(
            ReporterQualification::from_code("2"),
            Some(ReporterQualification::Pharmacist)
        );
        assert_eq!(
            ReporterQualification::from_code("3"),
            Some(ReporterQualification::OtherHealthProfessional)
        );
        assert_eq!(
            ReporterQualification::from_code("4"),
            Some(ReporterQualification::Lawyer)
        );
        assert_eq!(
            ReporterQualification::from_code("5"),
            Some(ReporterQualification::Consumer)
        );
        assert_eq!(ReporterQualification::from_code("6"), None);
    }

    #[test]
    fn test_reporter_qualification_weights() {
        assert!(
            ReporterQualification::Physician.weight() > ReporterQualification::Pharmacist.weight()
        );
        assert!(
            ReporterQualification::Pharmacist.weight()
                > ReporterQualification::OtherHealthProfessional.weight()
        );
        assert!(ReporterQualification::Consumer.weight() > ReporterQualification::Lawyer.weight());
    }

    #[test]
    fn test_reporter_qualification_display() {
        assert_eq!(format!("{}", ReporterQualification::Physician), "Physician");
        assert_eq!(format!("{}", ReporterQualification::Lawyer), "Lawyer");
    }

    #[test]
    fn test_reporter_weighted_physician_heavy() {
        let cases = vec![
            ReporterCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                qualification_code: "1".into(), // Physician
            },
            ReporterCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                qualification_code: "1".into(), // Physician
            },
            ReporterCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                qualification_code: "1".into(), // Physician
            },
        ];

        let results = compute_reporter_weighted(&cases, &ReporterWeightedConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.raw_count, 3);
        // All physicians: weight = 1.0 each
        assert!((r.weighted_count - 3.0).abs() < f64::EPSILON);
        assert!((r.mean_reporter_weight - 1.0).abs() < f64::EPSILON);
        // Single source type: diversity = 0
        assert!(r.reporter_diversity_index.abs() < f64::EPSILON);
        assert!(!r.is_multi_source_confirmed);
    }

    #[test]
    fn test_reporter_weighted_diverse_sources() {
        let cases = vec![
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "1".into(), // Physician
            },
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "2".into(), // Pharmacist
            },
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "5".into(), // Consumer
            },
        ];

        let results = compute_reporter_weighted(&cases, &ReporterWeightedConfig::default());

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.raw_count, 3);
        // 3 distinct sources → high entropy
        assert!(r.reporter_diversity_index > 0.5);
        assert!(r.normalized_diversity > 0.5);
        assert!(r.is_multi_source_confirmed);
        assert!(r.confidence_bonus > 0.0);
    }

    #[test]
    fn test_reporter_weighted_below_min_cases() {
        let cases = vec![ReporterCase {
            drug: "D".into(),
            event: "E".into(),
            qualification_code: "1".into(),
        }];

        let results = compute_reporter_weighted(&cases, &ReporterWeightedConfig::default());
        assert!(results.is_empty());
    }

    #[test]
    fn test_reporter_weighted_unknown_codes() {
        let cases = vec![
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "9".into(), // Unknown
            },
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "8".into(), // Unknown
            },
            ReporterCase {
                drug: "D".into(),
                event: "E".into(),
                qualification_code: "7".into(), // Unknown
            },
        ];

        let results = compute_reporter_weighted(&cases, &ReporterWeightedConfig::default());
        // All codes unknown → no known reporters → filtered out
        assert!(results.is_empty());
    }

    // =========================================================================
    // A81 Tests — Geographic Signal Divergence
    // =========================================================================

    #[test]
    fn test_geographic_basic_divergent() {
        let mut cases = Vec::new();
        // Country US: 10 cases of DRUG+EVENT out of 20 total US cases (rate = 0.5)
        for _ in 0..10 {
            cases.push(GeographicCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                country: "US".into(),
            });
        }
        for _ in 0..10 {
            cases.push(GeographicCase {
                drug: "OTHER".into(),
                event: "OTHER".into(),
                country: "US".into(),
            });
        }
        // Country JP: 2 cases of DRUG+EVENT out of 50 total JP cases (rate = 0.04)
        for _ in 0..2 {
            cases.push(GeographicCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                country: "JP".into(),
            });
        }
        for _ in 0..48 {
            cases.push(GeographicCase {
                drug: "OTHER".into(),
                event: "OTHER".into(),
                country: "JP".into(),
            });
        }

        let results = compute_geographic_divergence(&cases, &GeographicConfig::default());

        // Should find DRUG_X + EVENT_Y divergent between US and JP
        let target = results
            .iter()
            .find(|r| r.drug == "DRUG_X" && r.event == "EVENT_Y");
        assert!(target.is_some());
        let r = target.unwrap_or_else(|| panic!("missing target result"));
        assert_eq!(r.total_cases, 12);
        assert_eq!(r.reporting_countries, 2);
        assert!(r.divergence_ratio > 3.0);
        assert_eq!(r.highest_country, "US");
        assert_eq!(r.lowest_country, "JP");
    }

    #[test]
    fn test_geographic_homogeneous() {
        let mut cases = Vec::new();
        // US: 5 cases out of 10 total (rate = 0.5)
        for _ in 0..5 {
            cases.push(GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            });
        }
        for _ in 0..5 {
            cases.push(GeographicCase {
                drug: "OTHER".into(),
                event: "OTHER".into(),
                country: "US".into(),
            });
        }
        // UK: 5 cases out of 10 total (rate = 0.5, same as US)
        for _ in 0..5 {
            cases.push(GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "UK".into(),
            });
        }
        for _ in 0..5 {
            cases.push(GeographicCase {
                drug: "OTHER".into(),
                event: "OTHER".into(),
                country: "UK".into(),
            });
        }

        let results = compute_geographic_divergence(&cases, &GeographicConfig::default());

        let target = results.iter().find(|r| r.drug == "D" && r.event == "E");
        assert!(target.is_some());
        let r = target.unwrap_or_else(|| panic!("missing"));
        // Rates are similar → divergence ratio near 1.0
        assert!(r.divergence_ratio < 2.0);
        assert!(!r.is_divergent);
    }

    #[test]
    fn test_geographic_below_min_countries() {
        let cases = vec![
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
        ];

        let results = compute_geographic_divergence(&cases, &GeographicConfig::default());
        // Only 1 country → filtered out (need min 2)
        assert!(results.is_empty());
    }

    #[test]
    fn test_geographic_below_min_cases() {
        let cases = vec![
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "D".into(),
                event: "E".into(),
                country: "JP".into(),
            },
        ];

        let config = GeographicConfig {
            min_cases: 5,
            ..Default::default()
        };
        let results = compute_geographic_divergence(&cases, &config);
        // Only 2 total cases, min is 5
        assert!(results.is_empty());
    }

    #[test]
    fn test_chi_squared_p_value_zero() {
        let p = chi_squared_p_value(0.0, 1.0);
        assert!((p - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_chi_squared_p_value_large() {
        // Very large chi-squared → p-value near 0
        let p = chi_squared_p_value(100.0, 1.0);
        assert!(p < 0.001);
    }

    #[test]
    fn test_standard_normal_cdf_symmetry() {
        let p_neg = standard_normal_cdf(-1.96);
        let p_pos = standard_normal_cdf(1.96);
        // CDF(-1.96) ≈ 0.025, CDF(1.96) ≈ 0.975
        assert!((p_neg - 0.025).abs() < 0.01);
        assert!((p_pos - 0.975).abs() < 0.01);
        // Symmetry: CDF(-z) + CDF(z) ≈ 1
        assert!((p_neg + p_pos - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_standard_normal_cdf_extremes() {
        assert!(standard_normal_cdf(-10.0).abs() < f64::EPSILON);
        assert!((standard_normal_cdf(10.0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_geographic_normalization() {
        let cases = vec![
            GeographicCase {
                drug: "drug_x".into(),
                event: "event_y".into(),
                country: "us".into(),
            },
            GeographicCase {
                drug: "drug_x".into(),
                event: "event_y".into(),
                country: "us".into(),
            },
            GeographicCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "DRUG_X".into(),
                event: "EVENT_Y".into(),
                country: "US".into(),
            },
            GeographicCase {
                drug: "Drug_X".into(),
                event: "Event_Y".into(),
                country: "JP".into(),
            },
            GeographicCase {
                drug: "Drug_X".into(),
                event: "Event_Y".into(),
                country: "JP".into(),
            },
        ];

        let config = GeographicConfig {
            min_cases: 3,
            min_countries: 2,
            min_country_cases: 2,
            ..Default::default()
        };
        let results = compute_geographic_divergence(&cases, &config);
        // All should normalize to same drug/event with 2 countries
        let target = results
            .iter()
            .find(|r| r.drug == "DRUG_X" && r.event == "EVENT_Y");
        assert!(target.is_some());
        let r = target.unwrap_or_else(|| panic!("missing"));
        assert_eq!(r.total_cases, 7);
        assert_eq!(r.reporting_countries, 2);
    }
}
