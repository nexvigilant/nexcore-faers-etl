//! NDC (National Drug Code) Bridge for drug identification.
//!
//! Provides lookup and fuzzy matching against the FDA NDC Directory.
//! Enables linking FAERS drug names to standardized product codes.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

// =============================================================================
// Types
// =============================================================================

/// NDC product record from FDA directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NdcProduct {
    /// 10 or 11 digit NDC code (format varies: 4-4-2, 5-3-2, 5-4-1).
    pub ndc_code: String,
    /// Proprietary (brand) name.
    pub proprietary_name: String,
    /// Non-proprietary (generic) name.
    pub nonproprietary_name: String,
    /// Labeler (manufacturer) name.
    pub labeler_name: String,
    /// Dosage form (tablet, capsule, etc.).
    pub dosage_form: String,
    /// Route of administration.
    pub route: String,
    /// Active ingredients.
    pub active_ingredients: Vec<String>,
    /// Pharmacological class (e.g., ACE Inhibitor).
    pub pharm_class: Vec<String>,
    /// Product type (HUMAN PRESCRIPTION DRUG, OTC, etc.).
    pub product_type: String,
    /// Marketing status.
    pub marketing_status: String,
    /// Marketing start date (YYYYMMDD).
    pub marketing_start_date: Option<String>,
    /// Marketing end date (YYYYMMDD).
    pub marketing_end_date: Option<String>,
}

/// NDC Bridge for drug code lookups.
pub struct NdcBridge {
    /// Index by NDC code.
    by_ndc: HashMap<String, NdcProduct>,
    /// Index by proprietary name (lowercase).
    by_brand: HashMap<String, Vec<NdcProduct>>,
    /// Index by nonproprietary name (lowercase).
    by_generic: HashMap<String, Vec<NdcProduct>>,
    /// Index by active ingredient (lowercase).
    by_ingredient: HashMap<String, Vec<NdcProduct>>,
}

/// Match result from NDC lookup.
#[derive(Debug, Clone)]
pub struct NdcMatch {
    /// The matched product.
    pub product: NdcProduct,
    /// Match type (exact NDC, brand, generic, ingredient, fuzzy).
    pub match_type: NdcMatchType,
    /// Confidence score (1.0 = exact, lower for fuzzy).
    pub confidence: f64,
}

/// Type of NDC match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdcMatchType {
    /// Exact NDC code match.
    ExactNdc,
    /// Exact brand name match.
    ExactBrand,
    /// Exact generic name match.
    ExactGeneric,
    /// Exact ingredient match.
    ExactIngredient,
    /// Fuzzy (Levenshtein) match.
    Fuzzy,
}

// =============================================================================
// Implementation
// =============================================================================

impl NdcBridge {
    /// Create an empty NDC bridge.
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_ndc: HashMap::new(),
            by_brand: HashMap::new(),
            by_generic: HashMap::new(),
            by_ingredient: HashMap::new(),
        }
    }

    /// Load NDC directory from FDA JSON file.
    ///
    /// # Errors
    ///
    /// Returns error if file cannot be read or parsed.
    pub fn load_from_file(path: &Path) -> nexcore_error::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let products: Vec<NdcProduct> = serde_json::from_str(&content)?;
        Ok(Self::from_products(products))
    }

    /// Build bridge from product list.
    #[must_use]
    pub fn from_products(products: Vec<NdcProduct>) -> Self {
        let mut bridge = Self::new();

        for product in products {
            // Index by NDC
            let ndc_normalized = normalize_ndc(&product.ndc_code);
            bridge.by_ndc.insert(ndc_normalized, product.clone());

            // Index by brand name
            let brand_key = product.proprietary_name.to_lowercase();
            bridge
                .by_brand
                .entry(brand_key)
                .or_default()
                .push(product.clone());

            // Index by generic name
            let generic_key = product.nonproprietary_name.to_lowercase();
            bridge
                .by_generic
                .entry(generic_key)
                .or_default()
                .push(product.clone());

            // Index by each active ingredient
            for ingredient in &product.active_ingredients {
                let ingredient_key = ingredient.to_lowercase();
                bridge
                    .by_ingredient
                    .entry(ingredient_key)
                    .or_default()
                    .push(product.clone());
            }
        }

        bridge
    }

    /// Add a single product to the index.
    pub fn add_product(&mut self, product: NdcProduct) {
        let ndc_normalized = normalize_ndc(&product.ndc_code);
        self.by_ndc.insert(ndc_normalized, product.clone());

        let brand_key = product.proprietary_name.to_lowercase();
        self.by_brand
            .entry(brand_key)
            .or_default()
            .push(product.clone());

        let generic_key = product.nonproprietary_name.to_lowercase();
        self.by_generic
            .entry(generic_key)
            .or_default()
            .push(product.clone());

        for ingredient in &product.active_ingredients {
            let ingredient_key = ingredient.to_lowercase();
            self.by_ingredient
                .entry(ingredient_key)
                .or_default()
                .push(product.clone());
        }
    }

    /// Look up a drug by name, trying multiple match strategies.
    ///
    /// Match priority:
    /// 1. Exact NDC code
    /// 2. Exact brand name
    /// 3. Exact generic name
    /// 4. Exact ingredient
    /// 5. Fuzzy match (if enabled)
    #[must_use]
    pub fn lookup(&self, query: &str, fuzzy: bool) -> Vec<NdcMatch> {
        let mut matches = Vec::new();
        let query_lower = query.to_lowercase();
        let query_normalized = normalize_drug_name(&query_lower);

        // 1. Try exact NDC match
        if let Some(product) = self.by_ndc.get(&normalize_ndc(query)) {
            matches.push(NdcMatch {
                product: product.clone(),
                match_type: NdcMatchType::ExactNdc,
                confidence: 1.0,
            });
            return matches; // NDC is definitive
        }

        // 2. Try exact brand name match
        if let Some(products) = self.by_brand.get(&query_normalized) {
            for product in products {
                matches.push(NdcMatch {
                    product: product.clone(),
                    match_type: NdcMatchType::ExactBrand,
                    confidence: 1.0,
                });
            }
        }

        // 3. Try exact generic name match
        if let Some(products) = self.by_generic.get(&query_normalized) {
            for product in products {
                // Avoid duplicates
                if !matches
                    .iter()
                    .any(|m| m.product.ndc_code == product.ndc_code)
                {
                    matches.push(NdcMatch {
                        product: product.clone(),
                        match_type: NdcMatchType::ExactGeneric,
                        confidence: 1.0,
                    });
                }
            }
        }

        // 4. Try exact ingredient match
        if let Some(products) = self.by_ingredient.get(&query_normalized) {
            for product in products {
                if !matches
                    .iter()
                    .any(|m| m.product.ndc_code == product.ndc_code)
                {
                    matches.push(NdcMatch {
                        product: product.clone(),
                        match_type: NdcMatchType::ExactIngredient,
                        confidence: 0.95,
                    });
                }
            }
        }

        // 5. Try fuzzy matching if enabled and no exact matches
        if fuzzy && matches.is_empty() {
            let fuzzy_matches = self.fuzzy_lookup(&query_normalized, 3);
            matches.extend(fuzzy_matches);
        }

        // Sort by confidence (descending) - use total_cmp for NaN safety
        matches.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));

        matches
    }

    /// Fuzzy lookup using Levenshtein distance.
    fn fuzzy_lookup(&self, query: &str, max_distance: usize) -> Vec<NdcMatch> {
        let mut matches = Vec::new();

        // Search brand names
        for (name, products) in &self.by_brand {
            let distance = levenshtein(query, name);
            if distance <= max_distance {
                let confidence = 1.0 - (distance as f64) / (query.len().max(name.len()) as f64);
                for product in products {
                    matches.push(NdcMatch {
                        product: product.clone(),
                        match_type: NdcMatchType::Fuzzy,
                        confidence,
                    });
                }
            }
        }

        // Search generic names
        for (name, products) in &self.by_generic {
            let distance = levenshtein(query, name);
            if distance <= max_distance {
                let confidence = 1.0 - (distance as f64) / (query.len().max(name.len()) as f64);
                for product in products {
                    if !matches
                        .iter()
                        .any(|m| m.product.ndc_code == product.ndc_code)
                    {
                        matches.push(NdcMatch {
                            product: product.clone(),
                            match_type: NdcMatchType::Fuzzy,
                            confidence,
                        });
                    }
                }
            }
        }

        matches
    }

    /// Get the number of products in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_ndc.len()
    }

    /// Check if the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_ndc.is_empty()
    }

    /// Get all unique brand names.
    #[must_use]
    pub fn brand_names(&self) -> Vec<&str> {
        self.by_brand.keys().map(String::as_str).collect()
    }

    /// Get all unique generic names.
    #[must_use]
    pub fn generic_names(&self) -> Vec<&str> {
        self.by_generic.keys().map(String::as_str).collect()
    }
}

impl Default for NdcBridge {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Normalize NDC code by removing dashes and padding.
fn normalize_ndc(ndc: &str) -> String {
    ndc.chars().filter(|c| c.is_ascii_digit()).collect()
}

/// Normalize drug name for matching.
fn normalize_drug_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Compute Levenshtein edit distance between two strings.
///
/// Delegates to the canonical `nexcore-edit-distance` implementation.
fn levenshtein(a: &str, b: &str) -> usize {
    nexcore_edit_distance::classic::levenshtein_distance(a, b)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_products() -> Vec<NdcProduct> {
        vec![
            NdcProduct {
                ndc_code: "0069-0150-01".to_string(),
                proprietary_name: "Lipitor".to_string(),
                nonproprietary_name: "Atorvastatin Calcium".to_string(),
                labeler_name: "Pfizer".to_string(),
                dosage_form: "TABLET".to_string(),
                route: "ORAL".to_string(),
                active_ingredients: vec!["ATORVASTATIN CALCIUM".to_string()],
                product_type: "HUMAN PRESCRIPTION DRUG".to_string(),
                marketing_status: "Active".to_string(),
                marketing_start_date: None,
                marketing_end_date: None,
                pharm_class: Vec::new(),
            },
            NdcProduct {
                ndc_code: "0378-0127-01".to_string(),
                proprietary_name: "Atorvastatin Calcium Tablets".to_string(),
                nonproprietary_name: "Atorvastatin Calcium".to_string(),
                labeler_name: "Mylan".to_string(),
                dosage_form: "TABLET".to_string(),
                route: "ORAL".to_string(),
                active_ingredients: vec!["ATORVASTATIN CALCIUM".to_string()],
                product_type: "HUMAN PRESCRIPTION DRUG".to_string(),
                marketing_status: "Active".to_string(),
                marketing_start_date: None,
                marketing_end_date: None,
                pharm_class: Vec::new(),
            },
        ]
    }

    #[test]
    fn test_ndc_bridge_creation() {
        let bridge = NdcBridge::from_products(sample_products());
        assert_eq!(bridge.len(), 2);
    }

    #[test]
    fn test_exact_brand_lookup() {
        let bridge = NdcBridge::from_products(sample_products());
        let matches = bridge.lookup("Lipitor", false);

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_type, NdcMatchType::ExactBrand);
        assert_eq!(matches[0].confidence, 1.0);
    }

    #[test]
    fn test_exact_generic_lookup() {
        let bridge = NdcBridge::from_products(sample_products());
        let matches = bridge.lookup("Atorvastatin Calcium", false);

        assert_eq!(matches.len(), 2); // Both products have same generic
        // One matches as brand ("Atorvastatin Calcium Tablets"), one as generic
        assert!(
            matches
                .iter()
                .any(|m| m.match_type == NdcMatchType::ExactGeneric)
        );
    }

    #[test]
    fn test_ingredient_lookup() {
        let bridge = NdcBridge::from_products(sample_products());
        let matches = bridge.lookup("ATORVASTATIN CALCIUM", false);

        assert!(!matches.is_empty());
    }

    #[test]
    fn test_fuzzy_lookup() {
        let bridge = NdcBridge::from_products(sample_products());
        let matches = bridge.lookup("Liptor", true); // Typo

        assert!(!matches.is_empty());
        assert_eq!(matches[0].match_type, NdcMatchType::Fuzzy);
        assert!(matches[0].confidence < 1.0);
    }

    #[test]
    fn test_ndc_normalization() {
        assert_eq!(normalize_ndc("0069-0150-01"), "0069015001");
        assert_eq!(normalize_ndc("00690150-01"), "0069015001");
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
    }
}
