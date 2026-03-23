//! OpenFDA API Client for real-time adverse event queries.
//!
//! Complements the batch ETL pipeline with live API access to FDA data.
//! Implements V33 contingency pattern: async fetch with fallback cache.
//!
//! # Example
//!
//! ```rust,ignore
//! use nexcore_faers_etl::api::{OpenFdaClient, DrugEventQuery};
//!
//! let client = OpenFdaClient::new()?;
//! let query = DrugEventQuery::new("aspirin").with_limit(10);
//! let events = client.drug_events(&query).await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use nexcore_chrono::DateTime;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// =============================================================================
// Constants
// =============================================================================

/// OpenFDA API base URL
const OPENFDA_BASE_URL: &str = "https://api.fda.gov";

/// Default request timeout in seconds
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default results limit per request
const DEFAULT_LIMIT: u32 = 100;

/// Maximum results per request (FDA limit)
const MAX_LIMIT: u32 = 1000;

/// Cache TTL in seconds (15 minutes)
const CACHE_TTL_SECS: u64 = 900;

// =============================================================================
// Error Types
// =============================================================================

/// Errors from OpenFDA API operations.
#[derive(Debug, nexcore_error::Error)]
pub enum OpenFdaError {
    /// Failed to build HTTP client.
    #[error("Failed to build HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),

    /// Network request failed.
    #[error("OpenFDA API request failed: {0}")]
    NetworkError(#[source] reqwest::Error),

    /// Invalid HTTP response status.
    #[error("OpenFDA returned HTTP {status}: {message}")]
    InvalidResponse { status: u16, message: String },

    /// Failed to parse response JSON.
    #[error("Failed to parse OpenFDA response: {0}")]
    ParseError(#[source] reqwest::Error),

    /// Rate limited by OpenFDA.
    #[error("OpenFDA rate limit exceeded, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    /// API unavailable and no cache available.
    #[error("OpenFDA unavailable: {reason} (no cached fallback)")]
    Unavailable { reason: String },
}

// =============================================================================
// API Response Types
// =============================================================================

/// OpenFDA drug event response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrugEventResponse {
    /// Response metadata.
    pub meta: ResponseMeta,
    /// Array of adverse event results.
    pub results: Vec<AdverseEvent>,
}

/// Response metadata from OpenFDA.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMeta {
    /// Disclaimer text.
    #[serde(default)]
    pub disclaimer: String,
    /// Terms of use URL.
    #[serde(default)]
    pub terms: String,
    /// License information.
    #[serde(default)]
    pub license: String,
    /// Last updated timestamp.
    #[serde(default)]
    pub last_updated: String,
    /// Result count information.
    #[serde(default)]
    pub results: ResultsMeta,
}

/// Results metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultsMeta {
    /// Number of results skipped.
    #[serde(default)]
    pub skip: u32,
    /// Number of results returned.
    #[serde(default)]
    pub limit: u32,
    /// Total results available.
    #[serde(default)]
    pub total: u64,
}

/// A single adverse event record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdverseEvent {
    /// Safety report ID.
    #[serde(default)]
    pub safetyreportid: String,
    /// Receipt date (YYYYMMDD format).
    #[serde(default)]
    pub receiptdate: String,
    /// Report type (1=spontaneous, 2=study, etc.).
    #[serde(default)]
    pub reporttype: String,
    /// Serious flag (1=yes, 2=no).
    #[serde(default)]
    pub serious: String,

    // Granular seriousness flags
    #[serde(default)]
    pub seriousnessdeath: Option<String>,
    #[serde(default)]
    pub seriousnesshospitalization: Option<String>,
    #[serde(default)]
    pub seriousnessdisabling: Option<String>,
    #[serde(default)]
    pub seriousnesscongenitalanomali: Option<String>,
    #[serde(default)]
    pub seriousnesslifethreatening: Option<String>,
    #[serde(default)]
    pub seriousnessother: Option<String>,

    /// Reporter metadata
    #[serde(default)]
    pub primarysource: Option<PrimarySource>,

    /// Patient information.
    #[serde(default)]
    pub patient: Option<Patient>,
}

/// Information about the primary source of the report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrimarySource {
    /// Reporter country
    pub reportercountry: Option<String>,
    /// Qualification (1=Physician, 2=Pharmacist, etc.)
    pub qualification: Option<String>,
}

/// Patient information in an adverse event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Patient {
    /// Patient age at event.
    #[serde(default)]
    pub patientonsetage: Option<String>,
    /// Age unit (801=year, 802=month, etc.).
    #[serde(default)]
    pub patientonsetageunit: Option<String>,
    /// Patient sex (1=male, 2=female, 0=unknown).
    #[serde(default)]
    pub patientsex: Option<String>,
    /// Patient weight in kg.
    #[serde(default)]
    pub patientweight: Option<String>,
    /// Drug information.
    #[serde(default)]
    pub drug: Vec<Drug>,
    /// Reaction information.
    #[serde(default)]
    pub reaction: Vec<Reaction>,
}

/// Drug information in a patient record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drug {
    /// Drug characterization (1=suspect, 2=concomitant, 3=interacting).
    #[serde(default)]
    pub drugcharacterization: String,
    /// Medicinal product name.
    #[serde(default)]
    pub medicinalproduct: String,
    /// OpenFDA enriched data.
    #[serde(default)]
    pub openfda: Option<OpenFdaData>,
}

/// OpenFDA enriched drug data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenFdaData {
    /// Brand names.
    #[serde(default)]
    pub brand_name: Vec<String>,
    /// Generic names.
    #[serde(default)]
    pub generic_name: Vec<String>,
    /// Manufacturer names.
    #[serde(default)]
    pub manufacturer_name: Vec<String>,
    /// Active ingredients.
    #[serde(default)]
    pub substance_name: Vec<String>,
    /// Route of administration.
    #[serde(default)]
    pub route: Vec<String>,
}

/// Reaction (adverse event) information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reaction {
    /// MedDRA preferred term.
    #[serde(default)]
    pub reactionmeddrapt: String,
    /// Reaction outcome (1=recovered, 2=recovering, etc.).
    #[serde(default)]
    pub reactionoutcome: Option<String>,
}

// =============================================================================
// Query Builder
// =============================================================================

/// Builder for drug event queries.
#[derive(Debug, Clone)]
pub struct DrugEventQuery {
    /// Drug name to search (brand or generic).
    drug: String,
    /// Optional event (MedDRA PT) to filter.
    event: Option<String>,
    /// Maximum results to return.
    limit: u32,
    /// Number of results to skip.
    skip: u32,
    /// Filter to serious reports only.
    serious_only: bool,
    /// Minimum receipt date (YYYYMMDD).
    from_date: Option<String>,
    /// Maximum receipt date (YYYYMMDD).
    to_date: Option<String>,
}

impl DrugEventQuery {
    /// Create a new query for a drug.
    #[must_use]
    pub fn new(drug: impl Into<String>) -> Self {
        Self {
            drug: drug.into(),
            event: None,
            limit: DEFAULT_LIMIT,
            skip: 0,
            serious_only: false,
            from_date: None,
            to_date: None,
        }
    }

    /// Filter to a specific adverse event (MedDRA PT).
    #[must_use]
    pub fn with_event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    /// Set maximum results to return.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit.min(MAX_LIMIT);
        self
    }

    /// Skip a number of results (for pagination).
    #[must_use]
    pub fn with_skip(mut self, skip: u32) -> Self {
        self.skip = skip;
        self
    }

    /// Filter to serious reports only.
    #[must_use]
    pub fn serious_only(mut self) -> Self {
        self.serious_only = true;
        self
    }

    /// Filter by date range.
    #[must_use]
    pub fn with_date_range(mut self, from: &str, to: &str) -> Self {
        self.from_date = Some(from.to_string());
        self.to_date = Some(to.to_string());
        self
    }

    /// Build the search query string.
    fn build_search(&self) -> String {
        let mut parts = Vec::new();

        // Drug search (brand or generic name)
        parts.push(format!(
            "(patient.drug.openfda.brand_name:\"{}\" OR patient.drug.openfda.generic_name:\"{}\")",
            self.drug, self.drug
        ));

        // Event filter
        if let Some(ref event) = self.event {
            parts.push(format!("patient.reaction.reactionmeddrapt:\"{}\"", event));
        }

        // Serious only filter
        if self.serious_only {
            parts.push("serious:1".to_string());
        }

        // Date range filter
        if let (Some(from), Some(to)) = (&self.from_date, &self.to_date) {
            parts.push(format!("receiptdate:[{}+TO+{}]", from, to));
        }

        parts.join("+AND+")
    }

    /// Build the full URL for this query.
    fn build_url(&self) -> String {
        format!(
            "{}/drug/event.json?search={}&limit={}&skip={}",
            OPENFDA_BASE_URL,
            self.build_search(),
            self.limit,
            self.skip
        )
    }
}

// =============================================================================
// Cache Entry
// =============================================================================

/// Cached response with TTL.
#[derive(Debug, Clone)]
struct CacheEntry {
    response: DrugEventResponse,
    fetched_at: DateTime,
}

impl CacheEntry {
    fn is_valid(&self) -> bool {
        let age = DateTime::now().signed_duration_since(self.fetched_at);
        age.num_seconds() < (CACHE_TTL_SECS as i64)
    }
}

// =============================================================================
// OpenFDA Client
// =============================================================================

/// Async client for OpenFDA API with V33 contingency (fallback cache).
pub struct OpenFdaClient {
    /// HTTP client.
    client: reqwest::Client,
    /// Response cache (query URL -> response).
    cache: Arc<RwLock<std::collections::HashMap<String, CacheEntry>>>,
    /// Optional API key for higher rate limits.
    api_key: Option<String>,
}

impl OpenFdaClient {
    /// Create a new OpenFDA client.
    ///
    /// # Errors
    ///
    /// Returns `OpenFdaError::ClientBuild` if the HTTP client cannot be created.
    pub fn new() -> Result<Self, OpenFdaError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .user_agent("nexcore-faers-etl/1.0")
            .build()
            .map_err(OpenFdaError::ClientBuild)?;

        Ok(Self {
            client,
            cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            api_key: None,
        })
    }

    /// Create a new client with an API key for higher rate limits.
    ///
    /// # Errors
    ///
    /// Returns `OpenFdaError::ClientBuild` if the HTTP client cannot be created.
    pub fn with_api_key(api_key: impl Into<String>) -> Result<Self, OpenFdaError> {
        let mut client = Self::new()?;
        client.api_key = Some(api_key.into());
        Ok(client)
    }

    /// Query drug adverse events.
    ///
    /// Implements V33 contingency: returns cached data on API failure.
    ///
    /// # Errors
    ///
    /// Returns error only if API fails AND no valid cache exists.
    pub async fn drug_events(
        &self,
        query: &DrugEventQuery,
    ) -> Result<DrugEventResponse, OpenFdaError> {
        let url = self.build_url_with_key(&query.build_url());

        // Try cache first if valid
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&url) {
                if entry.is_valid() {
                    tracing::debug!(cache_hit = true, "Using cached OpenFDA response");
                    return Ok(entry.response.clone());
                }
            }
        }

        // Fetch from API
        match self.fetch(&url).await {
            Ok(response) => {
                // Update cache
                let mut cache = self.cache.write().await;
                cache.insert(
                    url,
                    CacheEntry {
                        response: response.clone(),
                        fetched_at: DateTime::now(),
                    },
                );
                Ok(response)
            }
            Err(e) => {
                // V33 contingency: fallback to stale cache
                let cache = self.cache.read().await;
                if let Some(entry) = cache.get(&url) {
                    tracing::warn!(
                        error = %e,
                        cache_age_secs = (DateTime::now() - entry.fetched_at).num_seconds(),
                        "API failed, using stale cache (V33 contingency)"
                    );
                    return Ok(entry.response.clone());
                }
                Err(e)
            }
        }
    }

    /// Fetch response from API.
    async fn fetch(&self, url: &str) -> Result<DrugEventResponse, OpenFdaError> {
        tracing::debug!(url = url, "Fetching from OpenFDA");

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(OpenFdaError::NetworkError)?;

        let status = response.status();

        // Handle rate limiting
        if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);

            return Err(OpenFdaError::RateLimited {
                retry_after_secs: retry_after,
            });
        }

        // Handle other errors
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(OpenFdaError::InvalidResponse {
                status: status.as_u16(),
                message,
            });
        }

        // Parse response
        response
            .json::<DrugEventResponse>()
            .await
            .map_err(OpenFdaError::ParseError)
    }

    /// Build URL with API key if configured.
    fn build_url_with_key(&self, base_url: &str) -> String {
        match &self.api_key {
            Some(key) => format!("{}&api_key={}", base_url, key),
            None => base_url.to_string(),
        }
    }

    /// Get count of drug-event pairs for signal detection.
    ///
    /// Returns (drug, event, count) tuples from FDA data.
    ///
    /// # Errors
    ///
    /// Returns error if API unavailable and no cache.
    pub async fn get_drug_event_counts(
        &self,
        drug: &str,
        limit: u32,
    ) -> Result<Vec<(String, String, u32)>, OpenFdaError> {
        let query = DrugEventQuery::new(drug).with_limit(limit);
        let response = self.drug_events(&query).await?;

        let mut counts: std::collections::HashMap<(String, String), u32> =
            std::collections::HashMap::new();

        for event in &response.results {
            if let Some(ref patient) = event.patient {
                // Get suspect drugs only (characterization = 1)
                let suspect_drugs: Vec<&str> = patient
                    .drug
                    .iter()
                    .filter(|d| d.drugcharacterization == "1")
                    .map(|d| d.medicinalproduct.as_str())
                    .collect();

                // Get reactions
                let reactions: Vec<&str> = patient
                    .reaction
                    .iter()
                    .map(|r| r.reactionmeddrapt.as_str())
                    .collect();

                // Count drug-event pairs
                for drug_name in &suspect_drugs {
                    for reaction in &reactions {
                        let key = (drug_name.to_uppercase(), reaction.to_uppercase());
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }

        let result: Vec<_> = counts.into_iter().map(|((d, e), c)| (d, e, c)).collect();

        Ok(result)
    }

    /// Clear the response cache.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_builder_basic() {
        let query = DrugEventQuery::new("aspirin");
        let url = query.build_url();

        assert!(url.contains("api.fda.gov"));
        assert!(url.contains("aspirin"));
        assert!(url.contains("limit=100"));
    }

    #[test]
    fn test_query_builder_with_event() {
        let query = DrugEventQuery::new("metformin").with_event("HEADACHE");
        let search = query.build_search();

        assert!(search.contains("metformin"));
        assert!(search.contains("HEADACHE"));
    }

    #[test]
    fn test_query_builder_serious_only() {
        let query = DrugEventQuery::new("drug").serious_only();
        let search = query.build_search();

        assert!(search.contains("serious:1"));
    }

    #[test]
    fn test_query_builder_date_range() {
        let query = DrugEventQuery::new("drug").with_date_range("20230101", "20231231");
        let search = query.build_search();

        assert!(search.contains("receiptdate"));
        assert!(search.contains("20230101"));
        assert!(search.contains("20231231"));
    }

    #[test]
    fn test_query_builder_limit_capped() {
        let query = DrugEventQuery::new("drug").with_limit(5000);
        let url = query.build_url();

        // Should be capped at MAX_LIMIT (1000)
        assert!(url.contains("limit=1000"));
    }

    #[test]
    fn test_client_creation() {
        let result = OpenFdaClient::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_cache_entry_validity() {
        let entry = CacheEntry {
            response: DrugEventResponse {
                meta: ResponseMeta {
                    disclaimer: String::new(),
                    terms: String::new(),
                    license: String::new(),
                    last_updated: String::new(),
                    results: ResultsMeta::default(),
                },
                results: Vec::<AdverseEvent>::new(),
            },
            fetched_at: DateTime::now(),
        };

        assert!(entry.is_valid());
    }

    #[test]
    fn test_error_display() {
        let err = OpenFdaError::RateLimited {
            retry_after_secs: 60,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("rate limit"));
        assert!(msg.contains("60"));
    }

    #[test]
    fn test_error_unavailable() {
        let err = OpenFdaError::Unavailable {
            reason: "API down".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("unavailable"));
        assert!(msg.contains("no cached fallback"));
    }
}
