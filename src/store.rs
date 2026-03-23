//! DuckDB persistence layer for FAERS data.
//!
//! Provides local storage for parsed FAERS quarterly files with:
//! - Schema: reports, drugs, reactions, signals tables
//! - Indexes on drug_name, reaction_pt for fast queries
//! - Parquet export support

use nexcore_error::{Context, Result};
use duckdb::{Connection, params};
use std::path::Path;

/// DuckDB-backed FAERS store.
pub struct FaersStore {
    conn: Connection,
}

impl FaersStore {
    /// Create new in-memory store.
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("Failed to create in-memory DuckDB")?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    /// Open or create store at path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .with_context(|| format!("Failed to open DuckDB at {}", path.as_ref().display()))?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    /// Initialize database schema.
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Reports table (demographics)
            CREATE TABLE IF NOT EXISTS reports (
                primary_id TEXT PRIMARY KEY,
                age_years DOUBLE,
                sex TEXT,
                weight_kg DOUBLE,
                country TEXT,
                quarter TEXT,
                ingested_at TIMESTAMP DEFAULT current_timestamp
            );

            -- Drugs table
            CREATE TABLE IF NOT EXISTS drugs (
                id INTEGER PRIMARY KEY,
                primary_id TEXT NOT NULL,
                drug_name TEXT NOT NULL,
                role_code TEXT,
                FOREIGN KEY (primary_id) REFERENCES reports(primary_id)
            );
            CREATE INDEX IF NOT EXISTS idx_drugs_name ON drugs(drug_name);
            CREATE INDEX IF NOT EXISTS idx_drugs_primary_id ON drugs(primary_id);

            -- Reactions table
            CREATE TABLE IF NOT EXISTS reactions (
                id INTEGER PRIMARY KEY,
                primary_id TEXT NOT NULL,
                reaction_pt TEXT NOT NULL,
                FOREIGN KEY (primary_id) REFERENCES reports(primary_id)
            );
            CREATE INDEX IF NOT EXISTS idx_reactions_pt ON reactions(reaction_pt);
            CREATE INDEX IF NOT EXISTS idx_reactions_primary_id ON reactions(primary_id);

            -- Drug-event pairs (materialized for fast queries)
            CREATE TABLE IF NOT EXISTS drug_event_pairs (
                drug_name TEXT NOT NULL,
                reaction_pt TEXT NOT NULL,
                n INTEGER NOT NULL,
                PRIMARY KEY (drug_name, reaction_pt)
            );
            CREATE INDEX IF NOT EXISTS idx_pairs_drug ON drug_event_pairs(drug_name);
            CREATE INDEX IF NOT EXISTS idx_pairs_reaction ON drug_event_pairs(reaction_pt);

            -- Detected signals
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY,
                drug_name TEXT NOT NULL,
                reaction_pt TEXT NOT NULL,
                n INTEGER NOT NULL,
                prr DOUBLE,
                prr_lower_ci DOUBLE,
                prr_upper_ci DOUBLE,
                prr_signal BOOLEAN,
                ror DOUBLE,
                ror_lower_ci DOUBLE,
                ror_signal BOOLEAN,
                ic DOUBLE,
                ic025 DOUBLE,
                ic_signal BOOLEAN,
                ebgm DOUBLE,
                eb05 DOUBLE,
                ebgm_signal BOOLEAN,
                detected_at TIMESTAMP DEFAULT current_timestamp
            );
            CREATE INDEX IF NOT EXISTS idx_signals_drug ON signals(drug_name);
            CREATE INDEX IF NOT EXISTS idx_signals_reaction ON signals(reaction_pt);

            -- Store metadata
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            "#,
        )
        .context("Failed to initialize schema")?;

        Ok(())
    }

    /// Insert a report (demographics).
    pub fn insert_report(
        &self,
        primary_id: &str,
        age_years: Option<f64>,
        sex: Option<&str>,
        weight_kg: Option<f64>,
        country: Option<&str>,
        quarter: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO reports (primary_id, age_years, sex, weight_kg, country, quarter) VALUES (?, ?, ?, ?, ?, ?)",
            params![primary_id, age_years, sex, weight_kg, country, quarter],
        )?;
        Ok(())
    }

    /// Insert a drug record.
    pub fn insert_drug(&self, primary_id: &str, drug_name: &str, role_code: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO drugs (primary_id, drug_name, role_code) VALUES (?, ?, ?)",
            params![primary_id, drug_name.to_uppercase(), role_code],
        )?;
        Ok(())
    }

    /// Insert a reaction record.
    pub fn insert_reaction(&self, primary_id: &str, reaction_pt: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO reactions (primary_id, reaction_pt) VALUES (?, ?)",
            params![primary_id, reaction_pt.to_uppercase()],
        )?;
        Ok(())
    }

    /// Build drug-event pair counts from raw data.
    /// Only includes primary suspect drugs (PS role).
    pub fn build_pair_counts(&self) -> Result<u64> {
        // Clear existing pairs
        self.conn.execute("DELETE FROM drug_event_pairs", [])?;

        // Build from raw tables - only primary suspect drugs
        let inserted = self.conn.execute(
            r#"
            INSERT INTO drug_event_pairs (drug_name, reaction_pt, n)
            SELECT d.drug_name, r.reaction_pt, COUNT(DISTINCT d.primary_id) as n
            FROM drugs d
            JOIN reactions r ON d.primary_id = r.primary_id
            WHERE d.role_code = 'PS'
            GROUP BY d.drug_name, r.reaction_pt
            "#,
            [],
        )?;

        Ok(inserted as u64)
    }

    /// Get drug-event pair count.
    pub fn get_pair_count(&self, drug: &str, event: &str) -> Result<u32> {
        let count: Option<u32> = self.conn.query_row(
            "SELECT n FROM drug_event_pairs WHERE drug_name = ? AND reaction_pt = ?",
            params![drug.to_uppercase(), event.to_uppercase()],
            |row| row.get(0),
        ).ok();
        Ok(count.unwrap_or(0))
    }

    /// Get top events for a drug.
    pub fn get_drug_events(&self, drug: &str, limit: usize) -> Result<Vec<(String, u32)>> {
        let mut stmt = self.conn.prepare(
            "SELECT reaction_pt, n FROM drug_event_pairs WHERE drug_name = ? ORDER BY n DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![drug.to_uppercase(), limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get contingency table counts (a, b, c, d) for a drug-event pair.
    pub fn get_contingency(&self, drug: &str, event: &str) -> Result<(u32, u32, u32, u32)> {
        let drug = drug.to_uppercase();
        let event = event.to_uppercase();

        // a = drug + event
        let a = self.get_pair_count(&drug, &event)?;

        // Drug total
        let drug_total: u32 = self.conn.query_row(
            "SELECT COALESCE(SUM(n), 0) FROM drug_event_pairs WHERE drug_name = ?",
            params![&drug],
            |row| row.get(0),
        )?;

        // Event total
        let event_total: u32 = self.conn.query_row(
            "SELECT COALESCE(SUM(n), 0) FROM drug_event_pairs WHERE reaction_pt = ?",
            params![&event],
            |row| row.get(0),
        )?;

        // Total
        let total: u32 = self.conn.query_row(
            "SELECT COALESCE(SUM(n), 0) FROM drug_event_pairs",
            [],
            |row| row.get(0),
        )?;

        let b = drug_total.saturating_sub(a);
        let c = event_total.saturating_sub(a);
        let d = total.saturating_sub(drug_total + event_total - a);

        Ok((a, b, c, d))
    }

    /// Get all drug-event pairs with minimum count.
    pub fn get_all_pairs(&self, min_n: u32) -> Result<Vec<(String, String, u32)>> {
        let mut stmt = self.conn.prepare(
            "SELECT drug_name, reaction_pt, n FROM drug_event_pairs WHERE n >= ? ORDER BY n DESC",
        )?;
        let rows = stmt.query_map(params![min_n], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, u32>(2)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Insert a signal detection result.
    pub fn insert_signal(&self, signal: &crate::SignalDetectionResult) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO signals (drug_name, reaction_pt, n, prr, prr_lower_ci, prr_upper_ci, prr_signal,
                ror, ror_lower_ci, ror_signal, ic, ic025, ic_signal, ebgm, eb05, ebgm_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                signal.drug, signal.event, signal.n,
                signal.prr, signal.prr_lower_ci, signal.prr_upper_ci, signal.prr_signal,
                signal.ror, signal.ror_lower_ci, signal.ror_signal,
                signal.ic, signal.ic025, signal.ic_signal,
                signal.ebgm, signal.eb05, signal.ebgm_signal
            ],
        )?;
        Ok(())
    }

    /// Get statistics about the store.
    pub fn stats(&self) -> Result<StoreStats> {
        let report_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM reports", [], |row| row.get(0)
        )?;
        let drug_count: u64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT drug_name) FROM drugs", [], |row| row.get(0)
        )?;
        let reaction_count: u64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT reaction_pt) FROM reactions", [], |row| row.get(0)
        )?;
        let pair_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM drug_event_pairs", [], |row| row.get(0)
        )?;
        let signal_count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM signals", [], |row| row.get(0)
        )?;

        Ok(StoreStats {
            report_count,
            drug_count,
            reaction_count,
            pair_count,
            signal_count,
        })
    }

    /// Export signals to Parquet file.
    pub fn export_signals_parquet(&self, path: &str) -> Result<u64> {
        let count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM signals", [], |row| row.get(0)
        )?;

        if count == 0 {
            return Ok(0);
        }

        self.conn.execute(
            &format!("COPY signals TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY)", path),
            [],
        )?;

        Ok(count)
    }

    /// Export drug-event pairs to Parquet file.
    pub fn export_pairs_parquet(&self, path: &str) -> Result<u64> {
        let count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM drug_event_pairs", [], |row| row.get(0)
        )?;

        if count == 0 {
            return Ok(0);
        }

        self.conn.execute(
            &format!("COPY drug_event_pairs TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY)", path),
            [],
        )?;

        Ok(count)
    }

    /// Begin a transaction.
    pub fn begin_transaction(&self) -> Result<()> {
        self.conn.execute("BEGIN TRANSACTION", [])?;
        Ok(())
    }

    /// Commit a transaction.
    pub fn commit(&self) -> Result<()> {
        self.conn.execute("COMMIT", [])?;
        Ok(())
    }

    /// Rollback a transaction.
    pub fn rollback(&self) -> Result<()> {
        self.conn.execute("ROLLBACK", [])?;
        Ok(())
    }

    /// Get raw connection (for advanced queries).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// Store statistics.
#[derive(Debug, Clone)]
pub struct StoreStats {
    /// Number of unique reports
    pub report_count: u64,
    /// Number of unique drugs
    pub drug_count: u64,
    /// Number of unique reactions
    pub reaction_count: u64,
    /// Number of drug-event pairs
    pub pair_count: u64,
    /// Number of detected signals
    pub signal_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_create() {
        let store = FaersStore::new_in_memory().unwrap();
        let stats = store.stats().unwrap();
        assert_eq!(stats.report_count, 0);
    }

    #[test]
    fn test_insert_and_query() {
        let store = FaersStore::new_in_memory().unwrap();

        // Insert test data
        store.insert_report("1", Some(45.0), Some("M"), Some(80.0), Some("US"), "2025Q4").unwrap();
        store.insert_drug("1", "ASPIRIN", "PS").unwrap();
        store.insert_reaction("1", "HEADACHE").unwrap();

        store.insert_report("2", Some(50.0), Some("F"), Some(65.0), Some("US"), "2025Q4").unwrap();
        store.insert_drug("2", "ASPIRIN", "PS").unwrap();
        store.insert_reaction("2", "HEADACHE").unwrap();

        store.insert_report("3", Some(30.0), Some("M"), Some(75.0), Some("US"), "2025Q4").unwrap();
        store.insert_drug("3", "METFORMIN", "PS").unwrap();
        store.insert_reaction("3", "NAUSEA").unwrap();

        // Build pairs
        store.build_pair_counts().unwrap();

        // Query
        let count = store.get_pair_count("ASPIRIN", "HEADACHE").unwrap();
        assert_eq!(count, 2);

        let events = store.get_drug_events("ASPIRIN", 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "HEADACHE");
        assert_eq!(events[0].1, 2);
    }

    #[test]
    fn test_contingency() {
        let store = FaersStore::new_in_memory().unwrap();

        // Setup data: ASPIRIN+HEADACHE=3, ASPIRIN+NAUSEA=1, METFORMIN+HEADACHE=1
        for i in 0..3 {
            store.insert_report(&format!("A{i}"), None, None, None, None, "2025Q4").unwrap();
            store.insert_drug(&format!("A{i}"), "ASPIRIN", "PS").unwrap();
            store.insert_reaction(&format!("A{i}"), "HEADACHE").unwrap();
        }
        store.insert_report("B1", None, None, None, None, "2025Q4").unwrap();
        store.insert_drug("B1", "ASPIRIN", "PS").unwrap();
        store.insert_reaction("B1", "NAUSEA").unwrap();

        store.insert_report("C1", None, None, None, None, "2025Q4").unwrap();
        store.insert_drug("C1", "METFORMIN", "PS").unwrap();
        store.insert_reaction("C1", "HEADACHE").unwrap();

        store.build_pair_counts().unwrap();

        let (a, b, c, d) = store.get_contingency("ASPIRIN", "HEADACHE").unwrap();
        assert_eq!(a, 3); // ASPIRIN + HEADACHE
        assert_eq!(b, 1); // ASPIRIN + not HEADACHE (NAUSEA)
        assert_eq!(c, 1); // not ASPIRIN + HEADACHE (METFORMIN)
        assert_eq!(d, 0); // not ASPIRIN + not HEADACHE
    }
}
