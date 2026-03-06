// SPDX-License-Identifier: LGPL-3.0-or-later
//! IERS Earth Orientation Parameter (EOP) data for astronomical conversions.
//!
//! This crate provides access to IERS finals2000A.data — the standard
//! combined EOP file that contains dUT1 (UT1−UTC), polar motion (xp, yp),
//! and nutation corrections needed for precise astronomical coordinate
//! conversions.
//!
//! # Features
//!
//! - **Bundled data**: A recent snapshot of finals2000A.data is compiled
//!   into the binary via [`include_str!`], so EOP data is always available
//!   without network access.
//! - **Runtime loading**: Load from a local file for updated data.
//! - **Interpolation**: Linear interpolation between daily entries, with
//!   leap-second jump handling for dUT1.
//! - **Safe update** (behind `"update"` feature): Download new data,
//!   validate it, and safely swap with the old version.
//!
//! # Quick start
//!
//! ```
//! use casacore_measures_data::EopTable;
//!
//! let eop = EopTable::bundled();
//! let (start, end) = eop.mjd_range();
//! println!("EOP data covers MJD {start} to {end}");
//!
//! // Look up dUT1 at J2000.0
//! if let Some(vals) = eop.interpolate(51544.5) {
//!     println!("dUT1 = {:.4} s", vals.dut1_seconds);
//!     println!("xp = {:.6}\"", vals.x_arcsec);
//!     println!("yp = {:.6}\"", vals.y_arcsec);
//! }
//! ```

use std::fmt;
use std::path::Path;
use std::sync::OnceLock;

mod bundled;
mod interp;
mod parser;

#[cfg(feature = "update")]
pub mod update;

pub use bundled::bundled_eop_table;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from EOP data operations.
#[derive(Debug, Clone)]
pub enum EopError {
    /// A parsing error in the data file.
    ParseError(String),
    /// An I/O error (file read, network, etc.).
    IoError(String),
}

impl fmt::Display for EopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "EOP parse error: {msg}"),
            Self::IoError(msg) => write!(f, "EOP I/O error: {msg}"),
        }
    }
}

impl std::error::Error for EopError {}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A single day's Earth Orientation Parameter entry.
///
/// Each entry corresponds to one row in the IERS finals2000A.data file,
/// at a specific MJD (Modified Julian Date). Values are extracted from the
/// fixed-column ASCII format.
///
/// Corresponds to one day of data from the IERS Bulletin A combined series.
#[derive(Debug, Clone, Copy)]
pub struct EopEntry {
    /// Modified Julian Date (UTC epoch).
    pub mjd: f64,
    /// Polar motion X component in arcseconds.
    pub x_arcsec: f64,
    /// Polar motion Y component in arcseconds.
    pub y_arcsec: f64,
    /// UT1−UTC offset in seconds.
    pub dut1_seconds: f64,
    /// Excess length of day in seconds (0 if not available).
    pub lod_seconds: f64,
    /// Celestial pole offset dX (IAU 2000A nutation) in milliarcseconds.
    pub dx_mas: f64,
    /// Celestial pole offset dY (IAU 2000A nutation) in milliarcseconds.
    pub dy_mas: f64,
    /// `true` if this entry is predicted rather than measured.
    pub is_predicted: bool,
}

/// Interpolated Earth Orientation Parameter values at a specific epoch.
///
/// Produced by [`EopTable::interpolate`] for any MJD within the table range.
#[derive(Debug, Clone, Copy)]
pub struct EopValues {
    /// UT1−UTC offset in seconds.
    pub dut1_seconds: f64,
    /// Polar motion X component in arcseconds.
    pub x_arcsec: f64,
    /// Polar motion Y component in arcseconds.
    pub y_arcsec: f64,
    /// Celestial pole offset dX in milliarcseconds.
    pub dx_mas: f64,
    /// Celestial pole offset dY in milliarcseconds.
    pub dy_mas: f64,
    /// `true` if any bracketing entry is predicted.
    pub is_predicted: bool,
}

/// Summary information about an EOP table.
#[derive(Debug, Clone)]
pub struct EopSummary {
    /// Number of entries in the table.
    pub num_entries: usize,
    /// First MJD in the table.
    pub mjd_start: f64,
    /// Last MJD in the table.
    pub mjd_end: f64,
    /// Last MJD with measured (non-predicted) data.
    pub last_measured_mjd: f64,
    /// Number of measured entries.
    pub num_measured: usize,
    /// Number of predicted entries.
    pub num_predicted: usize,
}

impl fmt::Display for EopSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EOP table: {} entries, MJD {:.0}–{:.0}, last measured MJD {:.0} ({} measured, {} predicted)",
            self.num_entries,
            self.mjd_start,
            self.mjd_end,
            self.last_measured_mjd,
            self.num_measured,
            self.num_predicted
        )
    }
}

// ---------------------------------------------------------------------------
// EopTable
// ---------------------------------------------------------------------------

/// In-memory EOP table with daily entries and interpolation.
///
/// Stores a sorted array of [`EopEntry`] values (one per MJD day) and
/// provides fast O(1) interpolation for any MJD within the covered range.
///
/// # Data sources
///
/// - [`EopTable::bundled()`] — compiled-in snapshot (always available)
/// - [`EopTable::from_file()`] — load from a local file
/// - [`EopTable::from_finals2000a()`] — parse from string content
///
/// # Example
///
/// ```
/// use casacore_measures_data::EopTable;
///
/// let eop = EopTable::bundled();
/// let vals = eop.interpolate(51544.5).unwrap();
/// assert!(vals.dut1_seconds.abs() < 1.0); // dUT1 is always < 0.9s
/// ```
#[derive(Debug, Clone)]
pub struct EopTable {
    entries: Vec<EopEntry>,
    mjd_start: f64,
    mjd_end: f64,
}

impl EopTable {
    /// Parse an EOP table from finals2000A.data content.
    ///
    /// The content should be the complete text of an IERS finals2000A.data
    /// file in fixed-column ASCII format.
    pub fn from_finals2000a(content: &str) -> Result<Self, EopError> {
        let entries = parser::parse_finals2000a(content)?;
        let mjd_start = entries[0].mjd;
        let mjd_end = entries[entries.len() - 1].mjd;
        Ok(Self {
            entries,
            mjd_start,
            mjd_end,
        })
    }

    /// Load the bundled (compiled-in) EOP snapshot.
    ///
    /// Returns a `&'static` reference to the lazily-parsed table.
    /// This is an O(1) operation after the first call.
    pub fn bundled() -> &'static Self {
        bundled_eop_table()
    }

    /// Load an EOP table from a file path.
    ///
    /// The file should be in IERS finals2000A.data fixed-column format.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, EopError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| EopError::IoError(format!("{}: {e}", path.as_ref().display())))?;
        Self::from_finals2000a(&content)
    }

    /// Interpolate EOP values at a given MJD.
    ///
    /// Uses linear interpolation between daily entries. For dUT1,
    /// interpolation across leap-second boundaries is avoided (the
    /// nearest entry's value is used instead).
    ///
    /// Returns `None` if `mjd` is outside the table range.
    pub fn interpolate(&self, mjd: f64) -> Option<EopValues> {
        interp::interpolate(&self.entries, mjd)
    }

    /// Returns the MJD range covered by this table: `(start, end)`.
    pub fn mjd_range(&self) -> (f64, f64) {
        (self.mjd_start, self.mjd_end)
    }

    /// Returns summary information about this table.
    pub fn summary(&self) -> EopSummary {
        let num_measured = self.entries.iter().filter(|e| !e.is_predicted).count();
        let num_predicted = self.entries.len() - num_measured;
        let last_measured_mjd = self
            .entries
            .iter()
            .rev()
            .find(|e| !e.is_predicted)
            .map(|e| e.mjd)
            .unwrap_or(self.mjd_start);

        EopSummary {
            num_entries: self.entries.len(),
            mjd_start: self.mjd_start,
            mjd_end: self.mjd_end,
            last_measured_mjd,
            num_measured,
            num_predicted,
        }
    }

    /// Returns all entries in the table.
    pub fn entries(&self) -> &[EopEntry] {
        &self.entries
    }

    /// Returns the number of days since the last measured (non-predicted)
    /// entry, relative to the given MJD. Useful for staleness checks.
    pub fn days_since_last_measured(&self, current_mjd: f64) -> f64 {
        let last = self
            .entries
            .iter()
            .rev()
            .find(|e| !e.is_predicted)
            .map(|e| e.mjd)
            .unwrap_or(self.mjd_start);
        current_mjd - last
    }
}

/// The filename searched for in data directories.
const EOP_FILENAME: &str = "finals2000A.data";

/// Cached result from [`load_eop`].
///
/// Stores either an owned dynamically-loaded table or a marker that the
/// bundled table should be used. The first successful call to [`load_eop`]
/// initialises this; subsequent calls return the cached result.
static LOADED_EOP: OnceLock<Option<(EopTable, &'static str)>> = OnceLock::new();

/// Load EOP data with the standard search order:
///
/// 1. `$CASA_RS_DATA/finals2000A.data` (environment variable)
/// 2. `~/.casa-rs/data/finals2000A.data`
/// 3. Bundled snapshot (always available)
///
/// The result is cached after the first call. Returns a reference to
/// the loaded table and a description of the source.
pub fn load_eop() -> (&'static EopTable, &'static str) {
    let loaded = LOADED_EOP.get_or_init(|| {
        // Try environment variable
        if let Ok(dir) = std::env::var("CASA_RS_DATA") {
            let path = std::path::PathBuf::from(&dir).join(EOP_FILENAME);
            if let Ok(table) = EopTable::from_file(&path) {
                return Some((table, "$CASA_RS_DATA"));
            }
        }

        // Try ~/.casa-rs/data/
        if let Ok(home) = std::env::var("HOME") {
            let path = std::path::PathBuf::from(home)
                .join(".casa-rs")
                .join("data")
                .join(EOP_FILENAME);
            if let Ok(table) = EopTable::from_file(&path) {
                return Some((table, "~/.casa-rs/data"));
            }
        }

        // Fall through to bundled
        None
    });

    match loaded {
        Some((table, source)) => (table, source),
        None => (EopTable::bundled(), "bundled"),
    }
}

/// Maximum age (in days) of the bundled EOP data's last measured entry
/// before it is considered stale. 180 days ≈ 6 months.
pub const BUNDLED_STALENESS_THRESHOLD_DAYS: f64 = 180.0;

/// Check whether the bundled EOP data is stale relative to the given MJD.
///
/// Returns `Ok(days_old)` if the data is fresh enough, or `Err(days_old)`
/// if the last measured entry is older than [`BUNDLED_STALENESS_THRESHOLD_DAYS`].
///
/// This is intended for release-time CI checks:
///
/// ```bash
/// cargo test -p casacore-measures-data bundled_data_not_stale
/// ```
pub fn check_bundled_freshness(current_mjd: f64) -> Result<f64, f64> {
    let days_old = EopTable::bundled().days_since_last_measured(current_mjd);
    if days_old > BUNDLED_STALENESS_THRESHOLD_DAYS {
        Err(days_old)
    } else {
        Ok(days_old)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_table_loads() {
        let eop = EopTable::bundled();
        let (start, end) = eop.mjd_range();
        assert!(start < 50000.0); // Should start before year 2000
        assert!(end > 50000.0); // Should extend past year 2000
        assert!(eop.entries().len() > 10000);
    }

    #[test]
    fn bundled_j2000_interpolation() {
        let eop = EopTable::bundled();
        let vals = eop.interpolate(51544.5).unwrap();
        // dUT1 should be reasonable (< 0.9s magnitude)
        assert!(vals.dut1_seconds.abs() < 1.0);
        // Polar motion should be reasonable (< 1 arcsec)
        assert!(vals.x_arcsec.abs() < 1.0);
        assert!(vals.y_arcsec.abs() < 1.0);
    }

    #[test]
    fn bundled_summary() {
        let summary = EopTable::bundled().summary();
        assert!(summary.num_entries > 10000);
        assert!(summary.num_measured > 5000);
        println!("{summary}");
    }

    #[test]
    fn outside_range_returns_none() {
        let eop = EopTable::bundled();
        assert!(eop.interpolate(10000.0).is_none()); // way before table start
    }

    #[test]
    fn load_eop_returns_valid_table() {
        let (table, source) = load_eop();
        assert!(table.entries().len() > 1000);
        assert!(!source.is_empty());
    }

    /// This test fails when the bundled EOP data is older than 6 months.
    /// Run it as part of the release checklist to ensure data freshness.
    ///
    /// To update: `cargo run --example update_eop -p casacore-measures-data --features update`
    /// then copy the new file to `crates/casacore-measures-data/data/finals2000A.data`.
    #[test]
    fn bundled_data_not_stale() {
        // Approximate current MJD from the date 2026-03-05:
        // MJD = JD - 2400000.5; JD(2026-03-05) ≈ 2461270.5
        // Use a formula: MJD(Y,M,D) for quick approximation
        // 2026-01-01 = MJD 61041; + 31 (Jan) + 28 (Feb) + 4 (Mar 5) = 61104
        // But rather than hard-code, compute from system time.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        // Unix epoch (1970-01-01) = MJD 40587.0
        let current_mjd = 40587.0 + now / 86400.0;

        let summary = EopTable::bundled().summary();
        let days_old = current_mjd - summary.last_measured_mjd;

        assert!(
            days_old < BUNDLED_STALENESS_THRESHOLD_DAYS,
            "Bundled EOP data is {days_old:.0} days old (threshold: {BUNDLED_STALENESS_THRESHOLD_DAYS:.0} days). \
             Update with: cargo run --example update_eop -p casacore-measures-data --features update \
             then copy to crates/casacore-measures-data/data/finals2000A.data"
        );
    }
}
