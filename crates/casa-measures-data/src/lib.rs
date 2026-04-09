// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-table-backed measures runtime data for astronomical conversions.
//!
//! The runtime model follows CASA's layout and defaults:
//!
//! - default measures path: `~/.casa/data`
//! - preferred source: an existing CASA/casaconfig-populated tree
//! - fallback for the default path only: a packaged CASA-table snapshot shipped
//!   with `casa-rs` and unpacked into `~/.casa/data` on first use
//! - explicit override: `CASA_RS_MEASURESPATH` (with deprecated compatibility
//!   alias `CASA_RS_DATA`)

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use casa_table_read::{PlainTable, TableReadError};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use tar::Archive;

mod interp;
mod observatory;
mod parser;

#[cfg(feature = "update")]
pub mod update;

pub use observatory::{ObservatoryCatalog, ObservatoryEntry};

const DEFAULT_MEASURES_ENV: &str = "CASA_RS_MEASURESPATH";
const LEGACY_MEASURES_ENV: &str = "CASA_RS_DATA";
const DEFAULT_MEASURES_RELATIVE: &str = ".casa/data";
const PACKAGED_SNAPSHOT_ARCHIVE: &[u8] = include_bytes!("../data/casa-measures-runtime.tar.gz");
const PACKAGED_SNAPSHOT_PROVENANCE_JSON: &str =
    include_str!("../data/casa-measures-runtime.provenance.json");
const REQUIRED_RELATIVE_PATHS: &[&str] = &[
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
const REQUIRED_NONEMPTY_DIRS: &[&str] = &["ephemerides/JPL-Horizons"];

/// Errors from raw `finals2000A.data` operations kept for compatibility.
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
    /// A packaged snapshot bootstrap step failed.
    Bootstrap(String),
    /// Reading a CASA table failed.
    TableRead(String),
    /// The packaged provenance metadata is invalid.
    Provenance(String),
    /// The requested data is unavailable from the runtime tree.
    MissingData(String),
}

impl fmt::Display for MeasuresDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimePath(msg) => write!(f, "measures runtime path error: {msg}"),
            Self::Bootstrap(msg) => write!(f, "measures bootstrap error: {msg}"),
            Self::TableRead(msg) => write!(f, "measures table read error: {msg}"),
            Self::Provenance(msg) => write!(f, "measures provenance error: {msg}"),
            Self::MissingData(msg) => write!(f, "measures data missing: {msg}"),
        }
    }
}

impl std::error::Error for MeasuresDataError {}

impl From<TableReadError> for MeasuresDataError {
    fn from(value: TableReadError) -> Self {
        Self::TableRead(value.to_string())
    }
}

/// Provenance for the packaged fallback snapshot.
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

    /// Return the standard runtime EOP table.
    pub fn bundled() -> &'static Self {
        load_eop().0
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

#[derive(Debug, Clone)]
struct IgrfTable {
    years: Vec<f64>,
    coeffs_by_year: Vec<Vec<f64>>,
    secular_variation: Vec<f64>,
    nmax: usize,
}

impl IgrfTable {
    fn coefficients_for_decimal_year(
        &self,
        decimal_year: f64,
    ) -> Result<(Vec<f64>, usize), MeasuresDataError> {
        let min_year = *self.years.first().ok_or_else(|| {
            MeasuresDataError::MissingData("IGRF table has no year rows".to_string())
        })?;
        let max_year = self.years.last().copied().unwrap_or(min_year) + 5.0;
        if decimal_year < min_year || decimal_year > max_year {
            return Err(MeasuresDataError::MissingData(format!(
                "IGRF date must be between {min_year:.0}-01-01 and {max_year:.0}-12-31"
            )));
        }

        let coeffs = if decimal_year >= *self.years.last().expect("years non-empty") {
            extrapolate_coefficients(
                decimal_year,
                *self.years.last().expect("years non-empty"),
                self.coeffs_by_year.last().expect("coefficients non-empty"),
                &self.secular_variation,
            )
        } else {
            let upper = self
                .years
                .iter()
                .position(|year| *year > decimal_year)
                .expect("decimal year below final row has upper interval");
            interpolate_coefficients(
                decimal_year,
                self.years[upper - 1],
                &self.coeffs_by_year[upper - 1],
                self.years[upper],
                &self.coeffs_by_year[upper],
            )
        };

        Ok((coeffs, self.nmax))
    }
}

static STANDARD_MEASURES_ROOT: OnceLock<Result<(PathBuf, &'static str), String>> = OnceLock::new();
static STANDARD_EOP: OnceLock<Result<(EopTable, &'static str), String>> = OnceLock::new();
static STANDARD_OBSERVATORIES: OnceLock<Result<(ObservatoryCatalog, &'static str), String>> =
    OnceLock::new();
static STANDARD_TAI_UTC: OnceLock<Result<TaiUtcTable, String>> = OnceLock::new();
static STANDARD_IGRF: OnceLock<Result<IgrfTable, String>> = OnceLock::new();
static PACKAGED_PROVENANCE: OnceLock<Result<SnapshotProvenance, String>> = OnceLock::new();

/// Maximum age (in days) of the packaged snapshot's measures date before it is
/// considered stale.
pub const PACKAGED_STALENESS_THRESHOLD_DAYS: f64 = 180.0;

/// Return the packaged snapshot provenance.
pub fn packaged_snapshot_provenance() -> Result<&'static SnapshotProvenance, MeasuresDataError> {
    match PACKAGED_PROVENANCE.get_or_init(|| {
        serde_json::from_str(PACKAGED_SNAPSHOT_PROVENANCE_JSON).map_err(|error| error.to_string())
    }) {
        Ok(provenance) => Ok(provenance),
        Err(error) => Err(MeasuresDataError::Provenance(error.clone())),
    }
}

/// Check whether the packaged snapshot is stale relative to the given MJD.
pub fn check_packaged_snapshot_freshness(current_mjd: f64) -> Result<f64, f64> {
    let version = packaged_snapshot_provenance()
        .ok()
        .map(|provenance| provenance.measures_version.clone())
        .unwrap_or_default();
    let days_old = parse_measures_version_mjd_age(&version, current_mjd).unwrap_or(f64::INFINITY);
    if days_old > PACKAGED_STALENESS_THRESHOLD_DAYS {
        Err(days_old)
    } else {
        Ok(days_old)
    }
}

/// Discover the standard measures path and its provenance label.
pub fn standard_measures_path() -> Result<(&'static Path, &'static str), MeasuresDataError> {
    match STANDARD_MEASURES_ROOT
        .get_or_init(|| resolve_standard_measures_root().map_err(|error| error.to_string()))
    {
        Ok((path, source)) => Ok((path.as_path(), *source)),
        Err(error) => Err(MeasuresDataError::RuntimePath(error.clone())),
    }
}

/// Load the standard runtime EOP table.
pub fn try_load_eop() -> Result<(&'static EopTable, &'static str), MeasuresDataError> {
    match STANDARD_EOP.get_or_init(|| load_standard_eop().map_err(|error| error.to_string())) {
        Ok((table, source)) => Ok((table, *source)),
        Err(error) => Err(MeasuresDataError::TableRead(error.clone())),
    }
}

/// Infallible compatibility wrapper for callers that expect a standard table.
pub fn load_eop() -> (&'static EopTable, &'static str) {
    try_load_eop().unwrap_or_else(|error| panic!("failed to load standard EOP runtime: {error}"))
}

/// Load the standard runtime observatory catalog.
pub fn try_load_observatories()
-> Result<(&'static ObservatoryCatalog, &'static str), MeasuresDataError> {
    match STANDARD_OBSERVATORIES
        .get_or_init(|| load_standard_observatories().map_err(|error| error.to_string()))
    {
        Ok((catalog, source)) => Ok((catalog, *source)),
        Err(error) => Err(MeasuresDataError::TableRead(error.clone())),
    }
}

/// Infallible compatibility wrapper for callers that expect a standard catalog.
pub fn load_observatories() -> (&'static ObservatoryCatalog, &'static str) {
    try_load_observatories()
        .unwrap_or_else(|error| panic!("failed to load standard observatory runtime: {error}"))
}

/// Compute `TAI-UTC` in seconds from the standard `geodetic/TAI_UTC` table.
pub fn tai_minus_utc_seconds(utc_mjd: f64) -> Result<f64, MeasuresDataError> {
    let table = match STANDARD_TAI_UTC
        .get_or_init(|| load_standard_tai_utc().map_err(|error| error.to_string()))
    {
        Ok(table) => table,
        Err(error) => return Err(MeasuresDataError::TableRead(error.clone())),
    };
    table.tai_minus_utc_seconds(utc_mjd)
}

/// Invert `TAI = UTC + (TAI-UTC)` using the standard `geodetic/TAI_UTC` table.
pub fn utc_from_tai_mjd(tai_mjd: f64) -> Result<f64, MeasuresDataError> {
    let table = match STANDARD_TAI_UTC
        .get_or_init(|| load_standard_tai_utc().map_err(|error| error.to_string()))
    {
        Ok(table) => table,
        Err(error) => return Err(MeasuresDataError::TableRead(error.clone())),
    };
    table.utc_mjd_from_tai_mjd(tai_mjd)
}

/// Load IGRF coefficients for a given decimal year from the standard runtime.
pub fn igrf_coefficients_for_decimal_year(
    decimal_year: f64,
) -> Result<(Vec<f64>, usize), MeasuresDataError> {
    let table = match STANDARD_IGRF
        .get_or_init(|| load_standard_igrf().map_err(|error| error.to_string()))
    {
        Ok(table) => table,
        Err(error) => return Err(MeasuresDataError::TableRead(error.clone())),
    };
    table.coefficients_for_decimal_year(decimal_year)
}

fn resolve_standard_measures_root() -> Result<(PathBuf, &'static str), MeasuresDataError> {
    let default_path = default_measures_path()?;
    resolve_standard_measures_root_inner(
        std::env::var(DEFAULT_MEASURES_ENV).ok().map(PathBuf::from),
        std::env::var(LEGACY_MEASURES_ENV).ok().map(PathBuf::from),
        default_path,
    )
}

fn resolve_standard_measures_root_inner(
    explicit_override: Option<PathBuf>,
    legacy_override: Option<PathBuf>,
    default_path: PathBuf,
) -> Result<(PathBuf, &'static str), MeasuresDataError> {
    if let Some(path) = explicit_override {
        if measures_tree_complete(&path) {
            return Ok((path, "$CASA_RS_MEASURESPATH"));
        }
        return Err(MeasuresDataError::RuntimePath(format!(
            "{DEFAULT_MEASURES_ENV} points to an incomplete measures tree"
        )));
    }

    if let Some(path) = legacy_override {
        if measures_tree_complete(&path) {
            return Ok((path, "$CASA_RS_DATA"));
        }
        return Err(MeasuresDataError::RuntimePath(format!(
            "{LEGACY_MEASURES_ENV} points to an incomplete measures tree"
        )));
    }

    let source = if measures_tree_complete(&default_path) {
        "~/.casa/data"
    } else {
        bootstrap_packaged_snapshot(&default_path)?;
        "~/.casa/data (bootstrapped)"
    };
    Ok((default_path, source))
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

fn bootstrap_packaged_snapshot(root: &Path) -> Result<(), MeasuresDataError> {
    std::fs::create_dir_all(root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("creating {}: {error}", root.display()))
    })?;

    let decoder = GzDecoder::new(PACKAGED_SNAPSHOT_ARCHIVE);
    let mut archive = Archive::new(decoder);
    archive.unpack(root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!(
            "extracting packaged snapshot into {}: {error}",
            root.display()
        ))
    })?;

    if !measures_tree_complete(root) {
        return Err(MeasuresDataError::Bootstrap(format!(
            "packaged snapshot extracted into {} but required tables are still missing",
            root.display()
        )));
    }
    Ok(())
}

fn load_standard_eop() -> Result<(EopTable, &'static str), MeasuresDataError> {
    let (root, source) = standard_measures_path()?;
    let measured = PlainTable::open(&root.join("geodetic/IERSeop2000"))?;
    let predicted = PlainTable::open(&root.join("geodetic/IERSpredict2000"))?;

    let measured_rows = eop_entries_from_table(&measured, false)?;
    let mut combined = measured_rows;
    let last_measured = combined
        .last()
        .map(|entry| entry.mjd)
        .unwrap_or(f64::NEG_INFINITY);
    for entry in eop_entries_from_table(&predicted, true)? {
        if entry.mjd > last_measured {
            combined.push(entry);
        }
    }

    Ok((EopTable::from_entries(combined)?, source))
}

fn eop_entries_from_table(
    table: &PlainTable,
    is_predicted: bool,
) -> Result<Vec<EopEntry>, MeasuresDataError> {
    let mjd = table.scalar_f64("MJD")?;
    let x = table.scalar_f64("x")?;
    let y = table.scalar_f64("y")?;
    let dut1 = table.scalar_f64("dUT1")?;
    let lod = table.scalar_f64("LOD")?;
    let dx = table.scalar_f64("dX")?;
    let dy = table.scalar_f64("dY")?;
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

fn load_standard_observatories() -> Result<(ObservatoryCatalog, &'static str), MeasuresDataError> {
    let (root, source) = standard_measures_path()?;
    let table = PlainTable::open(&root.join("geodetic/Observatories"))?;
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

    Ok((ObservatoryCatalog::from_entries(entries), source))
}

fn load_standard_tai_utc() -> Result<TaiUtcTable, MeasuresDataError> {
    let (root, _source) = standard_measures_path()?;
    let table = PlainTable::open(&root.join("geodetic/TAI_UTC"))?;
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

fn load_standard_igrf() -> Result<IgrfTable, MeasuresDataError> {
    let (root, _source) = standard_measures_path()?;
    let table = PlainTable::open(&root.join("geodetic/IGRF"))?;
    let mjd = table.scalar_f64("MJD")?;
    let mut years = Vec::with_capacity(table.row_count());
    let mut coeffs_by_year = Vec::with_capacity(table.row_count());
    let mut secular_variation = Vec::new();
    let mut nmax = 0usize;

    for (row, mjd_value) in mjd.iter().copied().enumerate().take(table.row_count()) {
        years.push(decimal_year_from_mjd(mjd_value));
        let coeffs = table.array_f64_cell("COEF", row)?.to_vec();
        nmax = solve_igrf_nmax(coeffs.len())?;
        coeffs_by_year.push(coeffs);
        secular_variation = table.array_f64_cell("dCOEF", row)?.to_vec();
    }

    Ok(IgrfTable {
        years,
        coeffs_by_year,
        secular_variation,
        nmax,
    })
}

fn decimal_year_from_mjd(mjd: f64) -> f64 {
    let jd = mjd + 2_400_000.5;
    let z = (jd + 0.5).floor();
    let f = (jd + 0.5) - z;
    let mut a = z;
    if z >= 2_299_161.0 {
        let alpha = ((z - 1_867_216.25) / 36_524.25).floor();
        a = z + 1.0 + alpha - (alpha / 4.0).floor();
    }
    let b = a + 1524.0;
    let c = ((b - 122.1) / 365.25).floor();
    let d = (365.25 * c).floor();
    let e = ((b - d) / 30.6001).floor();
    let day = b - d - (30.6001 * e).floor() + f;
    let month = if e < 14.0 { e - 1.0 } else { e - 13.0 };
    let year = if month > 2.0 { c - 4716.0 } else { c - 4715.0 };

    let year_i = year as i32;
    let month_i = month as i32;
    let day_i = day.floor() as i32;
    let leap = if (year_i % 4 == 0) && ((year_i % 100 != 0) || (year_i % 400 == 0)) {
        1
    } else {
        0
    };
    const DAYS: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut day_in_year = DAYS[(month_i - 1) as usize] + day_i;
    if month_i > 2 {
        day_in_year += leap;
    }
    f64::from(year_i) + (f64::from(day_in_year) / (365.0 + f64::from(leap)))
}

fn solve_igrf_nmax(coeff_len: usize) -> Result<usize, MeasuresDataError> {
    for nmax in 1..32 {
        if nmax * (nmax + 2) == coeff_len {
            return Ok(nmax);
        }
    }
    Err(MeasuresDataError::MissingData(format!(
        "unable to derive IGRF nmax from coefficient length {coeff_len}"
    )))
}

fn interpolate_coefficients(
    date: f64,
    year1: f64,
    coeffs1: &[f64],
    year2: f64,
    coeffs2: &[f64],
) -> Vec<f64> {
    let factor = (date - year1) / (year2 - year1);
    coeffs1
        .iter()
        .zip(coeffs2.iter())
        .map(|(lhs, rhs)| lhs + factor * (rhs - lhs))
        .collect()
}

fn extrapolate_coefficients(date: f64, base_year: f64, main: &[f64], sv: &[f64]) -> Vec<f64> {
    let factor = date - base_year;
    main.iter()
        .zip(sv.iter())
        .map(|(main_coeff, sv_coeff)| main_coeff + factor * sv_coeff)
        .collect()
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
    use tempfile::TempDir;

    fn current_mjd() -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        40_587.0 + now / 86_400.0
    }

    #[test]
    fn standard_measures_path_resolves() {
        let (path, source) = standard_measures_path().expect("measures path");
        assert!(path.is_dir());
        assert!(!source.is_empty());
    }

    #[test]
    fn standard_eop_table_loads() {
        let (table, source) = try_load_eop().expect("eop");
        assert!(table.entries().len() > 10_000);
        assert!(!source.is_empty());
    }

    #[test]
    fn standard_observatory_catalog_loads() {
        let (catalog, source) = try_load_observatories().expect("observatories");
        assert!(catalog.entries().len() > 40);
        assert!(catalog.get("ALMA").is_some());
        assert!(!source.is_empty());
    }

    #[test]
    fn tai_utc_lookup_is_reasonable() {
        let tai_minus_utc = tai_minus_utc_seconds(51_544.5).expect("TAI-UTC");
        assert!((tai_minus_utc - 32.0).abs() < 1e-6);
        let utc = utc_from_tai_mjd(51_544.5 + tai_minus_utc / 86_400.0).expect("UTC");
        assert!((utc - 51_544.5).abs() < 1e-10);
    }

    #[test]
    fn igrf_coefficients_load_from_runtime_table() {
        let (coeffs, nmax) = igrf_coefficients_for_decimal_year(2012.5).expect("IGRF");
        assert_eq!(nmax, 13);
        assert_eq!(coeffs.len(), 195);
    }

    #[test]
    fn packaged_snapshot_provenance_loads() {
        let provenance = packaged_snapshot_provenance().expect("provenance");
        assert!(!provenance.casarundata_version.is_empty());
        assert!(!provenance.measures_version.is_empty());
        assert!(!provenance.included_paths.is_empty());
    }

    #[test]
    fn packaged_snapshot_not_stale() {
        let age = check_packaged_snapshot_freshness(current_mjd()).unwrap_or_else(|age| {
            panic!(
                "packaged measures snapshot is {age:.0} days old; refresh crates/casa-measures-data/data/casa-measures-runtime.tar.gz"
            )
        });
        assert!(age <= PACKAGED_STALENESS_THRESHOLD_DAYS);
    }

    #[test]
    fn measures_path_override_wins_over_legacy_and_default() {
        let temp = TempDir::new().expect("tempdir");
        let explicit = temp.path().join("explicit");
        let legacy = temp.path().join("legacy");
        let default = temp.path().join("default");
        bootstrap_packaged_snapshot(&explicit).expect("bootstrap explicit");
        bootstrap_packaged_snapshot(&legacy).expect("bootstrap legacy");

        let (resolved, source) =
            resolve_standard_measures_root_inner(Some(explicit.clone()), Some(legacy), default)
                .expect("resolved measures path");

        assert_eq!(resolved, explicit);
        assert_eq!(source, "$CASA_RS_MEASURESPATH");
    }

    #[test]
    fn deprecated_legacy_alias_still_works() {
        let temp = TempDir::new().expect("tempdir");
        let legacy = temp.path().join("legacy");
        bootstrap_packaged_snapshot(&legacy).expect("bootstrap legacy");

        let (resolved, source) =
            resolve_standard_measures_root_inner(None, Some(legacy.clone()), temp.path().join("d"))
                .expect("resolved measures path");

        assert_eq!(resolved, legacy);
        assert_eq!(source, "$CASA_RS_DATA");
    }

    #[test]
    fn missing_default_path_bootstraps_packaged_snapshot() {
        let temp = TempDir::new().expect("tempdir");
        let default = temp.path().join("default");

        let (resolved, source) = resolve_standard_measures_root_inner(None, None, default.clone())
            .expect("resolved measures path");

        assert_eq!(resolved, default);
        assert_eq!(source, "~/.casa/data (bootstrapped)");
        assert!(measures_tree_complete(&resolved));
    }

    #[test]
    fn explicit_override_is_not_auto_populated() {
        let temp = TempDir::new().expect("tempdir");
        let explicit = temp.path().join("explicit");
        std::fs::create_dir_all(&explicit).expect("create explicit");

        let error = resolve_standard_measures_root_inner(
            Some(explicit.clone()),
            None,
            temp.path().join("default"),
        )
        .expect_err("incomplete override should fail");

        assert!(matches!(error, MeasuresDataError::RuntimePath(_)));
        assert!(!measures_tree_complete(&explicit));
    }
}
