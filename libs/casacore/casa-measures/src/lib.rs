// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]
//!
//! <details>
//! <summary>API details</summary>
//!
//! CASA-table-backed measures runtime data for astronomical conversions.
//!
//! [`MeasuresRuntime`] validates and lazily reads one explicit CASA measures
//! tree, or eagerly materializes every retained catalog before entering a
//! bounded operation. Discovery and packaged-snapshot installation are
//! separate, fallible operations; scientific calls never mutate a user's data
//! directory.
//!
//! </details>
#![warn(missing_docs)]

use std::fmt;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

mod igrf;
use igrf::{IgrfTable, load_igrf_from};

use casa_core::measures::{
    EopValues as ProviderEopValues, MeasuresProvider, MeasuresProviderState, NamedSourceDirection,
    ObservatoryPosition,
};
use casa_core::{ArrayValue, ScalarValue};
use casa_tables::{Table, TableOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod comet;
mod interp;
mod observatory;
mod parser;
mod planet;
mod source;

pub mod snapshot;
mod spectral_line;
pub mod table_measures;

#[cfg(feature = "update")]
pub mod update;

pub use comet::{CometDirection, CometTable};
pub use observatory::{ObservatoryCatalog, ObservatoryEntry};
pub use planet::{PlanetaryEphemeris, PlanetaryTable};
pub use snapshot::{install_packaged_snapshot, packaged_snapshot_provenance};
pub use source::{SourceCatalog, SourceEntry};
pub use spectral_line::{SpectralLineCatalog, SpectralLineEntry};

fn path_heap_bytes(path: &PathBuf) -> usize {
    path.capacity()
}

const DEFAULT_MEASURES_ENV: &str = "CASA_RS_MEASURESPATH";
const DEFAULT_MEASURES_RELATIVE: &str = ".casa/data";
const INSTALLED_PROVENANCE_FILE: &str = ".casa-rs-measures-provenance.json";
/// One shared manifest for validation and packaged-snapshot installation.
pub const REQUIRED_RELATIVE_PATHS: &[&str] = &[
    "geodetic/IERSeop97/table.dat",
    "geodetic/IERSpredict/table.dat",
    "geodetic/IERSeop2000/table.dat",
    "geodetic/IERSpredict2000/table.dat",
    "geodetic/TAI_UTC/table.dat",
    "geodetic/Observatories/table.dat",
    "geodetic/IGRF/table.dat",
    "ephemerides/DE200/table.dat",
    "ephemerides/DE405/table.dat",
    "ephemerides/VGEO/table.dat",
    "ephemerides/VTOP/table.dat",
    "ephemerides/Sources/table.dat",
    "ephemerides/Lines/table.dat",
];
/// Relative catalogue directories that must contain at least one entry.
pub const REQUIRED_NONEMPTY_DIRS: &[&str] = &["ephemerides/JPL-Horizons"];
/// Relative paths included when producing the packaged runtime snapshot.
pub const PACKAGED_SNAPSHOT_PATHS: &[&str] = &[
    "readme.txt",
    "geodetic",
    "ephemerides/DE200",
    "ephemerides/DE405",
    "ephemerides/VGEO",
    "ephemerides/VTOP",
    "ephemerides/JPL-Horizons",
    "ephemerides/Sources",
    "ephemerides/Lines",
];

/// Errors from explicitly parsing raw `finals2000A.data` input.
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

/// Errors raised while discovering or reading the standard measures runtime.
#[derive(Debug, Clone)]
pub enum MeasuresDataError {
    /// The runtime measures path is unavailable or incomplete.
    RuntimePath(String),
    /// The runtime tree exists but does not satisfy the shared manifest.
    IncompleteTree(String),
    /// A packaged snapshot bootstrap step failed.
    Bootstrap(String),
    /// Reading a CASA table failed.
    TableRead(String),
    /// The packaged provenance metadata is invalid.
    Provenance(String),
    /// The caller's explicit freshness threshold was exceeded.
    Stale {
        /// Elapsed days since the installed snapshot was generated.
        age_days: f64,
        /// Maximum age in days allowed by the caller.
        maximum_days: f64,
    },
    /// The requested data is unavailable from the runtime tree.
    MissingData(String),
    /// A checked retained-residency projection overflowed the host byte domain.
    ResidencyOverflow,
}

impl fmt::Display for MeasuresDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimePath(msg) => write!(f, "measures runtime path error: {msg}"),
            Self::IncompleteTree(msg) => write!(f, "incomplete measures runtime: {msg}"),
            Self::Bootstrap(msg) => write!(f, "measures bootstrap error: {msg}"),
            Self::TableRead(msg) => write!(f, "measures table read error: {msg}"),
            Self::Provenance(msg) => write!(f, "measures provenance error: {msg}"),
            Self::Stale {
                age_days,
                maximum_days,
            } => write!(
                f,
                "measures runtime is {age_days:.0} days old (maximum {maximum_days:.0})"
            ),
            Self::MissingData(msg) => write!(f, "measures data missing: {msg}"),
            Self::ResidencyOverflow => {
                f.write_str("measures retained-residency projection overflowed")
            }
        }
    }
}

impl std::error::Error for MeasuresDataError {}

impl From<casa_tables::TableError> for MeasuresDataError {
    fn from(value: casa_tables::TableError) -> Self {
        Self::TableRead(value.to_string())
    }
}

#[derive(Debug)]
struct RuntimeTable {
    table: Table,
}

impl RuntimeTable {
    fn open(path: &Path) -> Result<Self, MeasuresDataError> {
        Ok(Self {
            table: Table::open(TableOptions::new(path))?,
        })
    }

    fn row_count(&self) -> usize {
        self.table.row_count()
    }

    fn scalar_f64(&self, name: &str) -> Result<Vec<f64>, MeasuresDataError> {
        self.table
            .column_accessor(name)?
            .scalar_cells_owned()?
            .into_iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float64(value)) => Ok(value),
                Some(value) => Err(MeasuresDataError::TableRead(format!(
                    "column {name:?} row {row} is {:?}, expected f64",
                    value.primitive_type()
                ))),
                None => Err(MeasuresDataError::TableRead(format!(
                    "column {name:?} row {row} is undefined"
                ))),
            })
            .collect()
    }

    fn scalar_string(&self, name: &str) -> Result<Vec<String>, MeasuresDataError> {
        self.table
            .column_accessor(name)?
            .scalar_cells_owned()?
            .into_iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::String(value)) => Ok(value),
                Some(value) => Err(MeasuresDataError::TableRead(format!(
                    "column {name:?} row {row} is {:?}, expected string",
                    value.primitive_type()
                ))),
                None => Err(MeasuresDataError::TableRead(format!(
                    "column {name:?} row {row} is undefined"
                ))),
            })
            .collect()
    }

    fn array_f64_cell(&self, name: &str, row: usize) -> Result<Vec<f64>, MeasuresDataError> {
        match self.table.column_accessor(name)?.array_cell(row)? {
            ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
            values => Err(MeasuresDataError::TableRead(format!(
                "column {name:?} row {row} is {:?}, expected f64 array",
                values.primitive_type()
            ))),
        }
    }
}

/// Provenance for the explicitly installable packaged snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotProvenance {
    /// When the packaged snapshot was generated.
    pub generated_at_utc: String,
    /// The `casarundata` version used for the packaged snapshot.
    pub casarundata_version: String,
    /// The `WSRT_Measures_*` version overlaid onto the packaged snapshot.
    pub measures_version: String,
    /// The measures site actually used.
    pub measures_site: String,
    /// Relative paths intentionally included in the packaged snapshot.
    pub included_paths: Vec<String>,
}

impl SnapshotProvenance {
    fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .generated_at_utc
            .capacity()
            .checked_add(self.casarundata_version.capacity())?
            .checked_add(self.measures_version.capacity())?
            .checked_add(self.measures_site.capacity())?
            .checked_add(
                self.included_paths
                    .capacity()
                    .checked_mul(size_of::<String>())?,
            )?;
        for path in &self.included_paths {
            bytes = bytes.checked_add(path.capacity())?;
        }
        Some(bytes)
    }
}

/// A single day's Earth Orientation Parameter entry.
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
    /// Excess length of day in seconds.
    pub lod_seconds: f64,
    /// Celestial pole offset dX in milliarcseconds.
    pub dx_mas: f64,
    /// Celestial pole offset dY in milliarcseconds.
    pub dy_mas: f64,
    /// `true` if this entry is predicted rather than measured.
    pub is_predicted: bool,
}

/// Interpolated Earth Orientation Parameter values at a specific epoch.
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

/// In-memory EOP table with daily entries and interpolation.
#[derive(Debug, Clone)]
pub struct EopTable {
    entries: Vec<EopEntry>,
    mjd_start: f64,
    mjd_end: f64,
}

impl EopTable {
    /// Parse an EOP table from raw `finals2000A.data` content.
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

    /// Load a raw `finals2000A.data` file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, EopError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|error| EopError::IoError(format!("{}: {error}", path.as_ref().display())))?;
        Self::from_finals2000a(&content)
    }

    fn from_entries(entries: Vec<EopEntry>) -> Result<Self, MeasuresDataError> {
        if entries.is_empty() {
            return Err(MeasuresDataError::MissingData(
                "EOP runtime tables produced no rows".to_string(),
            ));
        }
        let mjd_start = entries[0].mjd;
        let mjd_end = entries[entries.len() - 1].mjd;
        Ok(Self {
            entries,
            mjd_start,
            mjd_end,
        })
    }

    /// Interpolate EOP values for the given MJD.
    pub fn interpolate(&self, mjd: f64) -> Option<EopValues> {
        interp::interpolate(self.entries(), mjd)
    }

    /// Returns the covered MJD range.
    pub fn mjd_range(&self) -> (f64, f64) {
        (self.mjd_start, self.mjd_end)
    }

    /// Returns a summary of the table contents.
    pub fn summary(&self) -> EopSummary {
        let num_entries = self.entries.len();
        let num_measured = self
            .entries
            .iter()
            .filter(|entry| !entry.is_predicted)
            .count();
        let num_predicted = num_entries - num_measured;
        let last_measured_mjd = self
            .entries
            .iter()
            .rev()
            .find(|entry| !entry.is_predicted)
            .map(|entry| entry.mjd)
            .unwrap_or(self.mjd_start);
        EopSummary {
            num_entries,
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

    /// Returns the number of days since the last measured entry.
    pub fn days_since_last_measured(&self, current_mjd: f64) -> f64 {
        let last = self
            .entries
            .iter()
            .rev()
            .find(|entry| !entry.is_predicted)
            .map(|entry| entry.mjd)
            .unwrap_or(self.mjd_start);
        current_mjd - last
    }

    fn retained_heap_bytes(&self) -> Option<usize> {
        self.entries.capacity().checked_mul(size_of::<EopEntry>())
    }
}

#[derive(Debug, Clone)]
struct TaiUtcEntry {
    mjd: f64,
    d_utc: f64,
    offset: f64,
    multiplier: f64,
}

#[derive(Debug, Clone)]
struct TaiUtcTable {
    entries: Vec<TaiUtcEntry>,
}

impl TaiUtcTable {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.entries
            .capacity()
            .checked_mul(size_of::<TaiUtcEntry>())
    }

    fn tai_minus_utc_seconds(&self, utc_mjd: f64) -> Result<f64, MeasuresDataError> {
        let entry = self
            .entries
            .iter()
            .rev()
            .find(|entry| utc_mjd >= entry.mjd)
            .or_else(|| self.entries.first())
            .ok_or_else(|| {
                MeasuresDataError::MissingData("TAI_UTC table contains no rows".to_string())
            })?;
        Ok(entry.d_utc + (utc_mjd - entry.offset) * entry.multiplier)
    }

    fn utc_mjd_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, MeasuresDataError> {
        let mut utc_mjd = tai_mjd;
        for _ in 0..8 {
            let offset_days = self.tai_minus_utc_seconds(utc_mjd)? / 86_400.0;
            let next = tai_mjd - offset_days;
            if (next - utc_mjd).abs() < 1e-14 {
                return Ok(next);
            }
            utc_mjd = next;
        }
        Ok(utc_mjd)
    }
}

/// Validation policy applied when opening an explicit runtime root.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimePolicy {
    /// Require installed provenance metadata.
    pub require_provenance: bool,
    /// Optional `(current_mjd, maximum_age_days)` freshness check.
    pub freshness: Option<(f64, f64)>,
}

/// A non-mutating candidate returned by runtime discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCandidate {
    /// Candidate runtime directory.
    pub path: PathBuf,
    /// Discovery source, such as the configured environment variable.
    pub source: &'static str,
    /// Whether the required files and nonempty directories are present.
    pub complete: bool,
}

/// Fallible handle for one explicitly selected measures-data tree.
#[derive(Debug)]
pub struct MeasuresRuntime {
    root: PathBuf,
    provenance: Option<SnapshotProvenance>,
    eop: OnceLock<Result<EopTable, String>>,
    observatories: OnceLock<Result<ObservatoryCatalog, String>>,
    sources: OnceLock<Result<SourceCatalog, String>>,
    spectral_lines: OnceLock<Result<SpectralLineCatalog, String>>,
    tai_utc: OnceLock<Result<TaiUtcTable, String>>,
    igrf: OnceLock<Result<IgrfTable, String>>,
    bounded_state: OnceLock<Result<MeasuresProviderState, String>>,
}

impl MeasuresRuntime {
    /// The most heap a runtime holds while it loads every catalog
    /// ([`Self::prepare_bounded_state`]) and once it has, for a caller that
    /// admits the runtime before its first lookup. The packaged catalogs
    /// peak at about 11 MB while they load (the EOP table's columns are most
    /// of it) and keep about 3.3 MB; the law
    /// `loading_every_catalog_holds_at_most_the_load_bytes` keeps the figure
    /// honest as the tables grow.
    pub const LOAD_BYTES: usize = 16 << 20;

    /// Validate and open one root. Construction and later reads never write.
    pub fn open(
        root: impl Into<PathBuf>,
        policy: RuntimePolicy,
    ) -> Result<Self, MeasuresDataError> {
        let root = root.into();
        validate_measures_tree(&root)?;
        let provenance_path = root.join(INSTALLED_PROVENANCE_FILE);
        let provenance = if provenance_path.is_file() {
            let bytes = std::fs::read(&provenance_path).map_err(|error| {
                MeasuresDataError::Provenance(format!("{}: {error}", provenance_path.display()))
            })?;
            Some(
                serde_json::from_slice::<SnapshotProvenance>(&bytes).map_err(|error| {
                    MeasuresDataError::Provenance(format!("{}: {error}", provenance_path.display()))
                })?,
            )
        } else if policy.require_provenance {
            return Err(MeasuresDataError::Provenance(format!(
                "{} is missing",
                provenance_path.display()
            )));
        } else {
            None
        };
        if let Some((current_mjd, maximum_days)) = policy.freshness {
            let provenance = provenance.as_ref().ok_or_else(|| {
                MeasuresDataError::Provenance("freshness requires installed provenance".to_string())
            })?;
            let age_days =
                parse_measures_version_mjd_age(&provenance.measures_version, current_mjd)
                    .ok_or_else(|| {
                        MeasuresDataError::Provenance(format!(
                            "invalid measures version {:?}",
                            provenance.measures_version
                        ))
                    })?;
            if age_days > maximum_days {
                return Err(MeasuresDataError::Stale {
                    age_days,
                    maximum_days,
                });
            }
        }
        Ok(Self {
            root,
            provenance,
            eop: OnceLock::new(),
            observatories: OnceLock::new(),
            sources: OnceLock::new(),
            spectral_lines: OnceLock::new(),
            tai_utc: OnceLock::new(),
            igrf: OnceLock::new(),
            bounded_state: OnceLock::new(),
        })
    }

    /// Discover the configured and conventional candidates without opening,
    /// installing, or otherwise mutating them.
    pub fn discover_candidates() -> Result<Vec<RuntimeCandidate>, MeasuresDataError> {
        let mut candidates = Vec::new();
        if let Some(path) = configured_measures_path() {
            candidates.push(RuntimeCandidate {
                complete: measures_tree_complete(&path),
                path,
                source: "$CASA_RS_MEASURESPATH",
            });
        }
        let path = default_measures_path()?;
        if candidates.iter().all(|candidate| candidate.path != path) {
            candidates.push(RuntimeCandidate {
                complete: measures_tree_complete(&path),
                path,
                source: "~/.casa/data",
            });
        }
        Ok(candidates)
    }

    /// Open the first complete discovered candidate using an explicit policy.
    pub fn open_discovered(policy: RuntimePolicy) -> Result<Self, MeasuresDataError> {
        let candidates = Self::discover_candidates()?;
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.complete)
            .ok_or_else(|| {
                MeasuresDataError::RuntimePath(format!(
                    "no complete measures runtime found; checked {}",
                    candidates
                        .iter()
                        .map(|candidate| candidate.path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
        Self::open(candidate.path.clone(), policy)
    }

    /// Directory backing this runtime and its lazily opened catalogues.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Provenance loaded when the runtime was opened, if the tree supplied it.
    pub fn provenance(&self) -> Option<&SnapshotProvenance> {
        self.provenance.as_ref()
    }

    /// Eagerly materialize every provider catalog and return its authoritative
    /// scientific-content identity plus exact retained heap residency.
    ///
    /// After this succeeds, all provider methods read immutable initialized
    /// `OnceLock` values, so later scientific calls cannot grow retained cache
    /// state behind a bounded operation's admission decision.
    pub fn prepare_bounded_state(&self) -> Result<MeasuresProviderState, MeasuresDataError> {
        match self.bounded_state.get_or_init(|| {
            self.build_bounded_state()
                .map_err(|error| error.to_string())
        }) {
            Ok(state) => Ok(*state),
            Err(error) => Err(MeasuresDataError::TableRead(error.clone())),
        }
    }

    fn build_bounded_state(&self) -> Result<MeasuresProviderState, MeasuresDataError> {
        let eop = self.eop()?;
        let observatories = self.observatories()?;
        let sources = self.sources()?;
        let spectral_lines = self.spectral_lines()?;
        let tai_utc = self.tai_utc()?;
        let igrf = self.igrf()?;

        let mut bytes = path_heap_bytes(&self.root);
        if let Some(provenance) = &self.provenance {
            bytes = bytes
                .checked_add(
                    provenance
                        .retained_heap_bytes()
                        .ok_or(MeasuresDataError::ResidencyOverflow)?,
                )
                .ok_or(MeasuresDataError::ResidencyOverflow)?;
        }
        let retained_heap_bytes = bytes
            .checked_add(
                eop.retained_heap_bytes()
                    .ok_or(MeasuresDataError::ResidencyOverflow)?,
            )
            .and_then(|bytes| bytes.checked_add(observatories.retained_heap_bytes()?))
            .and_then(|bytes| bytes.checked_add(sources.retained_heap_bytes()?))
            .and_then(|bytes| bytes.checked_add(spectral_lines.retained_heap_bytes()?))
            .and_then(|bytes| bytes.checked_add(tai_utc.retained_heap_bytes()?))
            .and_then(|bytes| bytes.checked_add(igrf.retained_heap_bytes()?))
            .ok_or(MeasuresDataError::ResidencyOverflow)?;
        let identity_sha256 = scientific_state_identity(
            self.provenance.as_ref(),
            eop,
            observatories,
            sources,
            spectral_lines,
            tai_utc,
            igrf,
        )?;
        Ok(MeasuresProviderState::new(
            identity_sha256,
            retained_heap_bytes,
        ))
    }

    /// Borrow the cached Earth orientation table, loading it on first access.
    ///
    /// Returns a table-read error if the required measured or predicted data cannot be read.
    pub fn eop(&self) -> Result<&EopTable, MeasuresDataError> {
        cached(&self.eop, || load_eop_from(&self.root))
    }

    /// Borrow the cached observatory catalogue, loading it on first access.
    ///
    /// Returns a table-read error if the Observatories table cannot be decoded.
    pub fn observatories(&self) -> Result<&ObservatoryCatalog, MeasuresDataError> {
        cached(&self.observatories, || load_observatories_from(&self.root))
    }

    /// Borrow the cached source catalogue, loading it on first access.
    ///
    /// Returns a table-read error if the Sources table cannot be decoded.
    pub fn sources(&self) -> Result<&SourceCatalog, MeasuresDataError> {
        cached(&self.sources, || load_sources_from(&self.root))
    }

    /// Borrow the cached spectral-line catalogue, whose stored frequencies are in GHz.
    ///
    /// Use [`SpectralLineEntry::frequency_hz`] for the casacore rest frequency in Hz.
    ///
    /// Returns a table-read error if the Lines table cannot be decoded.
    pub fn spectral_lines(&self) -> Result<&SpectralLineCatalog, MeasuresDataError> {
        cached(&self.spectral_lines, || {
            load_spectral_lines_from(&self.root)
        })
    }

    /// Look up TAI minus UTC in seconds at the given UTC Modified Julian Date.
    ///
    /// Returns an error if the leap-second catalogue or the requested epoch is unavailable.
    pub fn tai_minus_utc_seconds(&self, utc_mjd: f64) -> Result<f64, MeasuresDataError> {
        self.tai_utc()?.tai_minus_utc_seconds(utc_mjd)
    }

    /// Convert a TAI Modified Julian Date to UTC using the runtime's leap-second data.
    ///
    /// Returns an error if the table is unreadable or the epoch cannot be converted.
    pub fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, MeasuresDataError> {
        self.tai_utc()?.utc_mjd_from_tai_mjd(tai_mjd)
    }

    /// Evaluate IGRF Gauss coefficients at a decimal year.
    ///
    /// Returns the coefficients and maximum spherical-harmonic degree, or an error
    /// if the IGRF table or requested epoch is unavailable.
    pub fn igrf_coefficients(
        &self,
        decimal_year: f64,
    ) -> Result<(Vec<f64>, usize), MeasuresDataError> {
        self.igrf()?.coefficients_for_decimal_year(decimal_year)
    }

    fn tai_utc(&self) -> Result<&TaiUtcTable, MeasuresDataError> {
        cached(&self.tai_utc, || load_tai_utc_from(&self.root))
    }

    fn igrf(&self) -> Result<&IgrfTable, MeasuresDataError> {
        cached(&self.igrf, || load_igrf_from(&self.root))
    }
}

fn scientific_state_identity(
    provenance: Option<&SnapshotProvenance>,
    eop: &EopTable,
    observatories: &ObservatoryCatalog,
    sources: &SourceCatalog,
    spectral_lines: &SpectralLineCatalog,
    tai_utc: &TaiUtcTable,
    igrf: &IgrfTable,
) -> Result<[u8; 32], MeasuresDataError> {
    let mut state = ScientificStateHasher::new();
    state.raw(b"casa-rs-measures-provider-state-v1\0");

    state.boolean(provenance.is_some());
    if let Some(provenance) = provenance {
        state.text(&provenance.generated_at_utc)?;
        state.text(&provenance.casarundata_version)?;
        state.text(&provenance.measures_version)?;
        state.text(&provenance.measures_site)?;
        state.sequence_len(provenance.included_paths.len())?;
        for path in &provenance.included_paths {
            state.text(path)?;
        }
    }

    state.sequence_len(eop.entries.len())?;
    for entry in &eop.entries {
        state.float(entry.mjd);
        state.float(entry.x_arcsec);
        state.float(entry.y_arcsec);
        state.float(entry.dut1_seconds);
        state.float(entry.lod_seconds);
        state.float(entry.dx_mas);
        state.float(entry.dy_mas);
        state.boolean(entry.is_predicted);
    }

    state.sequence_len(observatories.entries().len())?;
    for entry in observatories.entries() {
        state.float(entry.mjd);
        state.text(&entry.name)?;
        state.text(&entry.observatory_type)?;
        state.float(entry.longitude_deg);
        state.float(entry.latitude_deg);
        state.float(entry.height_m);
        state.float(entry.x_m);
        state.float(entry.y_m);
        state.float(entry.z_m);
        state.text(&entry.source)?;
        state.text(&entry.comment)?;
        state.text(&entry.antenna_responses)?;
    }

    state.sequence_len(sources.entries().len())?;
    for entry in sources.entries() {
        state.float(entry.mjd);
        state.text(&entry.name)?;
        state.text(&entry.direction_type)?;
        state.float(entry.longitude_deg);
        state.float(entry.latitude_deg);
        state.text(&entry.source)?;
        state.text(&entry.comment)?;
    }

    state.sequence_len(spectral_lines.entries().len())?;
    for entry in spectral_lines.entries() {
        state.float(entry.mjd);
        state.text(&entry.name)?;
        state.text(&entry.frequency_type)?;
        state.float(entry.frequency_ghz);
        state.text(&entry.source)?;
        state.text(&entry.comment)?;
    }

    state.sequence_len(tai_utc.entries.len())?;
    for entry in &tai_utc.entries {
        state.float(entry.mjd);
        state.float(entry.d_utc);
        state.float(entry.offset);
        state.float(entry.multiplier);
    }

    igrf.hash_state(&mut state)?;

    Ok(state.finish())
}

struct ScientificStateHasher {
    inner: Sha256,
}

impl ScientificStateHasher {
    fn new() -> Self {
        Self {
            inner: Sha256::new(),
        }
    }

    fn raw(&mut self, value: &[u8]) {
        self.inner.update(value);
    }

    fn sequence_len(&mut self, value: usize) -> Result<(), MeasuresDataError> {
        let value = u64::try_from(value).map_err(|_| MeasuresDataError::ResidencyOverflow)?;
        self.inner.update(value.to_be_bytes());
        Ok(())
    }

    fn text(&mut self, value: &str) -> Result<(), MeasuresDataError> {
        self.sequence_len(value.len())?;
        self.inner.update(value.as_bytes());
        Ok(())
    }

    fn float(&mut self, value: f64) {
        self.inner.update(value.to_bits().to_be_bytes());
    }

    fn boolean(&mut self, value: bool) {
        self.inner.update([u8::from(value)]);
    }

    fn finish(self) -> [u8; 32] {
        self.inner.finalize().into()
    }
}

fn cached<T>(
    cell: &OnceLock<Result<T, String>>,
    load: impl FnOnce() -> Result<T, MeasuresDataError>,
) -> Result<&T, MeasuresDataError> {
    match cell.get_or_init(|| load().map_err(|error| error.to_string())) {
        Ok(value) => Ok(value),
        Err(error) => Err(MeasuresDataError::TableRead(error.clone())),
    }
}

impl MeasuresProvider for MeasuresRuntime {
    fn prepare_bounded_state(&self) -> Result<Option<MeasuresProviderState>, String> {
        MeasuresRuntime::prepare_bounded_state(self)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    fn eop_values(&self, utc_mjd: f64) -> Result<Option<ProviderEopValues>, String> {
        self.eop()
            .map(|table| {
                table.interpolate(utc_mjd).map(|value| ProviderEopValues {
                    dut1_seconds: value.dut1_seconds,
                    x_arcsec: value.x_arcsec,
                    y_arcsec: value.y_arcsec,
                    dx_mas: value.dx_mas,
                    dy_mas: value.dy_mas,
                    is_predicted: value.is_predicted,
                })
            })
            .map_err(|error| error.to_string())
    }

    fn tai_minus_utc_seconds(&self, utc_mjd: f64) -> Result<f64, String> {
        self.tai_minus_utc_seconds(utc_mjd)
            .map_err(|error| error.to_string())
    }

    fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
        self.utc_from_tai_mjd(tai_mjd)
            .map_err(|error| error.to_string())
    }

    fn igrf_coefficients(&self, decimal_year: f64) -> Result<(Vec<f64>, usize), String> {
        self.igrf_coefficients(decimal_year)
            .map_err(|error| error.to_string())
    }

    fn igrf_coefficients_at_mjd(&self, tdb_mjd: f64) -> Result<(Vec<f64>, usize), String> {
        self.igrf()
            .and_then(|table| table.coefficients_at_mjd(tdb_mjd))
            .map_err(|error| error.to_string())
    }

    fn observatory_names(&self) -> Result<Vec<String>, String> {
        self.observatories()
            .map(|catalog| catalog.iter().map(|entry| entry.name.clone()).collect())
            .map_err(|error| error.to_string())
    }

    fn source_names(&self) -> Result<Vec<String>, String> {
        self.sources()
            .map(|catalog| catalog.iter().map(|entry| entry.name.clone()).collect())
            .map_err(|error| error.to_string())
    }

    fn spectral_line_names(&self) -> Result<Vec<String>, String> {
        self.spectral_lines()
            .map(|catalog| catalog.iter().map(|entry| entry.name.clone()).collect())
            .map_err(|error| error.to_string())
    }

    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        self.observatories()
            .map(|catalog| catalog.get(name).and_then(ObservatoryEntry::position))
            .map_err(|error| error.to_string())
    }

    fn source(&self, name: &str) -> Result<Option<NamedSourceDirection>, String> {
        self.sources()
            .map(|catalog| {
                catalog.get(name).map(|entry| NamedSourceDirection {
                    reference: entry.direction_type.clone(),
                    longitude_rad: entry.longitude_rad(),
                    latitude_rad: entry.latitude_rad(),
                })
            })
            .map_err(|error| error.to_string())
    }

    fn spectral_line_hz(&self, name: &str) -> Result<Option<f64>, String> {
        self.spectral_lines()
            .map(|catalog| catalog.get(name).map(SpectralLineEntry::frequency_hz))
            .map_err(|error| error.to_string())
    }
}

fn configured_measures_path() -> Option<PathBuf> {
    std::env::var_os(DEFAULT_MEASURES_ENV).map(PathBuf::from)
}

fn default_measures_path() -> Result<PathBuf, MeasuresDataError> {
    let home = std::env::var("HOME").map_err(|error| {
        MeasuresDataError::RuntimePath(format!(
            "HOME is not set for default measures path: {error}"
        ))
    })?;
    Ok(PathBuf::from(home).join(DEFAULT_MEASURES_RELATIVE))
}

fn measures_tree_complete(root: &Path) -> bool {
    REQUIRED_RELATIVE_PATHS
        .iter()
        .all(|relative| root.join(relative).is_file())
        && REQUIRED_NONEMPTY_DIRS.iter().all(|relative| {
            root.join(relative).is_dir()
                && root
                    .join(relative)
                    .read_dir()
                    .map(|mut entries| entries.next().is_some())
                    .unwrap_or(false)
        })
}

fn validate_measures_tree(root: &Path) -> Result<(), MeasuresDataError> {
    if measures_tree_complete(root) {
        Ok(())
    } else {
        Err(MeasuresDataError::IncompleteTree(
            root.display().to_string(),
        ))
    }
}

fn load_eop_from(root: &Path) -> Result<EopTable, MeasuresDataError> {
    let measured = RuntimeTable::open(&root.join("geodetic/IERSeop97"))?;
    let predicted = RuntimeTable::open(&root.join("geodetic/IERSpredict"))?;
    let measured_corrections = RuntimeTable::open(&root.join("geodetic/IERSeop2000"))?;
    let predicted_corrections = RuntimeTable::open(&root.join("geodetic/IERSpredict2000"))?;

    let measured_rows = eop_entries_from_table(&measured, &measured_corrections, false)?;
    let mut combined = measured_rows;
    let last_measured = combined
        .last()
        .map(|entry| entry.mjd)
        .unwrap_or(f64::NEG_INFINITY);
    for entry in eop_entries_from_table(&predicted, &predicted_corrections, true)? {
        if entry.mjd > last_measured {
            combined.push(entry);
        }
    }

    EopTable::from_entries(combined)
}

fn eop_entries_from_table(
    table: &RuntimeTable,
    correction_table: &RuntimeTable,
    is_predicted: bool,
) -> Result<Vec<EopEntry>, MeasuresDataError> {
    let mjd = table.scalar_f64("MJD")?;
    let x = table.scalar_f64("x")?;
    let y = table.scalar_f64("y")?;
    let dut1 = table.scalar_f64("dUT1")?;
    let lod = table.scalar_f64("LOD")?;
    let correction_mjd = correction_table.scalar_f64("MJD")?;
    let dx = correction_table.scalar_f64("dX")?;
    let dy = correction_table.scalar_f64("dY")?;
    if correction_mjd != mjd {
        return Err(MeasuresDataError::TableRead(
            "legacy and IAU-2000 EOP tables have different MJD rows".to_string(),
        ));
    }
    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(EopEntry {
            mjd: mjd[row],
            x_arcsec: x[row],
            y_arcsec: y[row],
            dut1_seconds: dut1[row],
            lod_seconds: lod[row],
            dx_mas: dx[row] * 1000.0,
            dy_mas: dy[row] * 1000.0,
            is_predicted,
        });
    }
    Ok(entries)
}

fn load_observatories_from(root: &Path) -> Result<ObservatoryCatalog, MeasuresDataError> {
    let table = RuntimeTable::open(&root.join("geodetic/Observatories"))?;
    let mjd = table.scalar_f64("MJD")?;
    let name = table.scalar_string("Name")?;
    let observatory_type = table.scalar_string("Type")?;
    let longitude_deg = table.scalar_f64("Long")?;
    let latitude_deg = table.scalar_f64("Lat")?;
    let height_m = table.scalar_f64("Height")?;
    let x_m = table.scalar_f64("X")?;
    let y_m = table.scalar_f64("Y")?;
    let z_m = table.scalar_f64("Z")?;
    let source_col = table.scalar_string("Source")?;
    let comment = table.scalar_string("Comment")?;
    let antenna_responses = table.scalar_string("AntennaResponses")?;

    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(ObservatoryEntry {
            mjd: mjd[row],
            name: name[row].clone(),
            observatory_type: observatory_type[row].clone(),
            longitude_deg: longitude_deg[row],
            latitude_deg: latitude_deg[row],
            height_m: height_m[row],
            x_m: x_m[row],
            y_m: y_m[row],
            z_m: z_m[row],
            source: source_col[row].clone(),
            comment: comment[row].clone(),
            antenna_responses: antenna_responses[row].clone(),
        });
    }

    Ok(ObservatoryCatalog::from_entries(entries))
}

fn load_sources_from(root: &Path) -> Result<SourceCatalog, MeasuresDataError> {
    let table = RuntimeTable::open(&root.join("ephemerides/Sources"))?;
    let mjd = table.scalar_f64("MJD")?;
    let name = table.scalar_string("Name")?;
    let direction_type = table.scalar_string("Type")?;
    let longitude_deg = table.scalar_f64("Long")?;
    let latitude_deg = table.scalar_f64("Lat")?;
    let source_col = table.scalar_string("Source")?;
    let comment = table.scalar_string("Comment")?;

    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(SourceEntry {
            mjd: mjd[row],
            name: name[row].clone(),
            direction_type: direction_type[row].clone(),
            longitude_deg: longitude_deg[row],
            latitude_deg: latitude_deg[row],
            source: source_col[row].clone(),
            comment: comment[row].clone(),
        });
    }

    Ok(SourceCatalog::from_entries(entries))
}

fn load_spectral_lines_from(root: &Path) -> Result<SpectralLineCatalog, MeasuresDataError> {
    let table = RuntimeTable::open(&root.join("ephemerides/Lines"))?;
    let mjd = table.scalar_f64("MJD")?;
    let name = table.scalar_string("Name")?;
    let frequency_type = table.scalar_string("Type")?;
    let frequency_ghz = table.scalar_f64("Freq")?;
    let source_col = table.scalar_string("Source")?;
    let comment = table.scalar_string("Comment")?;

    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(SpectralLineEntry {
            mjd: mjd[row],
            name: name[row].clone(),
            frequency_type: frequency_type[row].clone(),
            frequency_ghz: frequency_ghz[row],
            source: source_col[row].clone(),
            comment: comment[row].clone(),
        });
    }

    Ok(SpectralLineCatalog::from_entries(entries))
}

fn load_tai_utc_from(root: &Path) -> Result<TaiUtcTable, MeasuresDataError> {
    let table = RuntimeTable::open(&root.join("geodetic/TAI_UTC"))?;
    let mjd = table.scalar_f64("MJD")?;
    let d_utc = table.scalar_f64("dUTC")?;
    let offset = table.scalar_f64("Offset")?;
    let multiplier = table.scalar_f64("Multiplier")?;
    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(TaiUtcEntry {
            mjd: mjd[row],
            d_utc: d_utc[row],
            offset: offset[row],
            multiplier: multiplier[row],
        });
    }
    Ok(TaiUtcTable { entries })
}

fn parse_measures_version_mjd_age(version: &str, current_mjd: f64) -> Option<f64> {
    let marker = "_Measures_";
    let idx = version.find(marker)?;
    let date = version.get(idx + marker.len()..idx + marker.len() + 8)?;
    let year = date.get(0..4)?.parse::<i32>().ok()?;
    let month = date.get(4..6)?.parse::<u32>().ok()?;
    let day = date.get(6..8)?.parse::<u32>().ok()?;
    let mjd = mjd_from_calendar(year, month, day)?;
    Some(current_mjd - mjd)
}

fn mjd_from_calendar(year: i32, month: u32, day: u32) -> Option<f64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let a = (14 - month as i32) / 12;
    let y = year + 4800 - a;
    let m = month as i32 + 12 * a - 3;
    let jdn = day as i32 + ((153 * m + 2) / 5) + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(f64::from(jdn) - 2_400_001.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    pub(super) static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn installed_runtime() -> (TempDir, MeasuresRuntime) {
        let temp = TempDir::new().expect("tempdir");
        let archive = std::env::var_os("CASA_RS_PACKAGED_SNAPSHOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join("data/casa-measures-runtime.tar.gz")
            });
        install_packaged_snapshot(&archive, temp.path()).expect("install snapshot");
        let runtime = MeasuresRuntime::open(
            temp.path(),
            RuntimePolicy {
                require_provenance: true,
                freshness: None,
            },
        )
        .expect("open runtime");
        (temp, runtime)
    }

    #[test]
    fn explicit_runtime_loads_every_catalog_through_casa_tables() {
        let (_temp, runtime) = installed_runtime();
        assert!(runtime.eop().expect("eop").entries().len() > 10_000);
        let catalog = runtime.observatories().expect("observatories");
        assert!(catalog.entries().len() > 40);
        assert!(catalog.get("ALMA").is_some());
        // casacore keeps spaces: a trailing one still agrees with ALMA over
        // ALMA's length, a leading one does not.
        assert!(catalog.get("alma ").is_some());
        assert!(catalog.get(" alma").is_none());
        let catalog = runtime.sources().expect("sources");
        assert!(catalog.entries().len() > 100);
        let source_0002 = catalog.get("0002-478").expect("catalog source");
        assert_eq!(source_0002.direction_type, "ICRS");
        assert!((source_0002.longitude_rad() - 0.020_046_3).abs() < 1.0e-6);
        assert!((source_0002.latitude_rad() - (-0.830_872)).abs() < 1.0e-6);
        let catalog = runtime.spectral_lines().expect("lines");
        assert!(catalog.entries().len() >= 18);
        let hi = catalog.get("HI").expect("HI line");
        assert!(catalog.get("hi ").is_some());
        assert!(catalog.get(" hi").is_none());
        assert_eq!(hi.frequency_type, "REST");
        // casacore's QC::HI, which the table also holds (CASA 6.7.6.14's
        // me.spectralline('HI')).
        assert_eq!(hi.frequency_hz(), 1_420_405_751.786);
        assert_eq!(hi.frequency_ghz * 1.0e9, hi.frequency_hz());
        let tai_minus_utc = runtime.tai_minus_utc_seconds(51_544.5).expect("TAI-UTC");
        assert!((tai_minus_utc - 32.0).abs() < 1e-6);
        let utc = runtime
            .utc_from_tai_mjd(51_544.5 + tai_minus_utc / 86_400.0)
            .expect("UTC");
        assert!((utc - 51_544.5).abs() < 1e-10);
        let (coeffs, nmax) = runtime.igrf_coefficients(2012.5).expect("IGRF");
        assert_eq!(nmax, 13);
        assert_eq!(coeffs.len(), 195);
    }

    #[test]
    fn bounded_preparation_eagerly_stabilizes_every_retained_catalog() {
        let (_temp, runtime) = installed_runtime();

        let state = runtime
            .prepare_bounded_state()
            .expect("prepare bounded runtime");
        assert!(state.retained_heap_bytes() > 0);
        assert_ne!(state.identity_sha256(), [0; 32]);
        assert!(runtime.eop.get().is_some());
        assert!(runtime.observatories.get().is_some());
        assert!(runtime.sources.get().is_some());
        assert!(runtime.spectral_lines.get().is_some());
        assert!(runtime.tai_utc.get().is_some());
        assert!(runtime.igrf.get().is_some());

        runtime.eop_values(51_544.5).expect("EOP lookup");
        runtime
            .tai_minus_utc_seconds(51_544.5)
            .expect("TAI-UTC lookup");
        runtime
            .utc_from_tai_mjd(51_544.5 + 32.0 / 86_400.0)
            .expect("UTC lookup");
        runtime.igrf_coefficients(2012.5).expect("IGRF lookup");
        runtime.observatory("ALMA").expect("observatory lookup");
        runtime.source("0002-478").expect("source lookup");
        runtime.spectral_line_hz("HI").expect("line lookup");

        assert_eq!(
            runtime
                .prepare_bounded_state()
                .expect("recheck bounded runtime"),
            state
        );
    }

    #[test]
    fn packaged_snapshot_provenance_loads() {
        let provenance = packaged_snapshot_provenance().expect("provenance");
        assert!(!provenance.casarundata_version.is_empty());
        assert!(!provenance.measures_version.is_empty());
        assert!(!provenance.included_paths.is_empty());
    }

    #[test]
    fn construction_never_installs_or_repairs_an_incomplete_root() {
        let temp = TempDir::new().expect("tempdir");
        let marker = temp.path().join("unchanged");
        std::fs::write(&marker, b"marker").unwrap();
        let error = MeasuresRuntime::open(temp.path(), RuntimePolicy::default())
            .expect_err("incomplete root must fail");
        assert!(matches!(error, MeasuresDataError::IncompleteTree(_)));
        assert_eq!(std::fs::read(marker).unwrap(), b"marker");
        assert!(!temp.path().join("geodetic").exists());
    }

    #[test]
    fn corrupt_catalog_is_distinct_from_incomplete_tree() {
        let (temp, _runtime) = installed_runtime();
        std::fs::write(
            temp.path().join("geodetic/Observatories/table.dat"),
            b"not a casacore table",
        )
        .expect("corrupt observatory table");
        let runtime = MeasuresRuntime::open(temp.path(), RuntimePolicy::default())
            .expect("manifest remains complete");
        assert!(matches!(
            runtime.observatories(),
            Err(MeasuresDataError::TableRead(_))
        ));
    }

    #[test]
    fn discovery_reports_missing_home_without_mutation_or_fallback() {
        let _guard = ENV_LOCK.lock().expect("environment lock");
        let old_home = std::env::var_os("HOME");
        let old_explicit = std::env::var_os(DEFAULT_MEASURES_ENV);
        unsafe {
            std::env::remove_var("HOME");
            std::env::remove_var(DEFAULT_MEASURES_ENV);
        }

        let result = MeasuresRuntime::discover_candidates();

        unsafe {
            match old_home {
                Some(value) => std::env::set_var("HOME", value),
                None => std::env::remove_var("HOME"),
            }
            match old_explicit {
                Some(value) => std::env::set_var(DEFAULT_MEASURES_ENV, value),
                None => std::env::remove_var(DEFAULT_MEASURES_ENV),
            }
        }
        assert!(matches!(result, Err(MeasuresDataError::RuntimePath(_))));
    }

    #[test]
    fn freshness_errors_preserve_invalid_provenance_and_stale_state() {
        let (temp, _runtime) = installed_runtime();
        assert!(matches!(
            MeasuresRuntime::open(
                temp.path(),
                RuntimePolicy {
                    require_provenance: true,
                    freshness: Some((70_000.0, 180.0)),
                }
            ),
            Err(MeasuresDataError::Stale { .. })
        ));
        std::fs::write(temp.path().join(INSTALLED_PROVENANCE_FILE), b"not json").unwrap();
        assert!(matches!(
            MeasuresRuntime::open(
                temp.path(),
                RuntimePolicy {
                    require_provenance: true,
                    freshness: Some((60_000.0, 180.0)),
                }
            ),
            Err(MeasuresDataError::Provenance(_))
        ));
    }

    #[test]
    fn concurrent_catalog_access_reuses_per_runtime_cache() {
        let (_temp, runtime) = installed_runtime();
        let runtime = Arc::new(runtime);
        let handles = (0..4)
            .map(|_| {
                let runtime = Arc::clone(&runtime);
                std::thread::spawn(move || runtime.eop().unwrap().entries().len())
            })
            .collect::<Vec<_>>();
        let lengths = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(lengths.iter().all(|length| *length == lengths[0]));
    }
}
