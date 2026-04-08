// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration-table keyword and column names.
//!
//! These constants mirror the modern CASA `NewCalTable` family closely enough
//! for the first compatibility wave. The reader remains permissive, but later
//! writer work will use these names to emit canonical on-disk layout.

/// Expected `table.info` type for CASA calibration tables.
pub const TABLE_INFO_TYPE: &str = "Calibration";

/// Table keyword naming the parameter family.
pub const KEY_PAR_TYPE: &str = "ParType";
/// Table keyword naming the source MeasurementSet.
pub const KEY_MS_NAME: &str = "MSName";
/// Table keyword naming the calibration subtype (for example `G Jones`).
pub const KEY_VIS_CAL: &str = "VisCal";
/// Table keyword naming the polarization basis.
pub const KEY_POL_BASIS: &str = "PolBasis";
/// Table keyword naming the CASA writer version.
pub const KEY_CASA_VERSION: &str = "CASA_Version";

/// Main-table time column.
pub const COL_TIME: &str = "TIME";
/// Main-table field identifier column.
pub const COL_FIELD_ID: &str = "FIELD_ID";
/// Main-table spectral-window identifier column.
pub const COL_SPECTRAL_WINDOW_ID: &str = "SPECTRAL_WINDOW_ID";
/// Main-table first antenna identifier column.
pub const COL_ANTENNA1: &str = "ANTENNA1";
/// Main-table second antenna identifier column.
pub const COL_ANTENNA2: &str = "ANTENNA2";
/// Main-table solution interval column.
pub const COL_INTERVAL: &str = "INTERVAL";
/// Main-table scan number column.
pub const COL_SCAN_NUMBER: &str = "SCAN_NUMBER";
/// Main-table observation identifier column.
pub const COL_OBSERVATION_ID: &str = "OBSERVATION_ID";
/// Main-table calibration-description identifier column used by legacy tables.
pub const COL_CAL_DESC_ID: &str = "CAL_DESC_ID";
/// Main-table complex-parameter payload column.
pub const COL_CPARAM: &str = "CPARAM";
/// Main-table float-parameter payload column.
pub const COL_FPARAM: &str = "FPARAM";
/// Main-table parameter error column.
pub const COL_PARAMERR: &str = "PARAMERR";
/// Main-table per-parameter flag column.
pub const COL_FLAG: &str = "FLAG";
/// Main-table signal-to-noise column.
pub const COL_SNR: &str = "SNR";
/// Main-table per-parameter weight column.
pub const COL_WEIGHT: &str = "WEIGHT";
/// Legacy BPOLY scalar gain factor column.
pub const COL_SCALE_FACTOR: &str = "SCALE_FACTOR";
/// Legacy BPOLY valid-frequency-domain column.
pub const COL_VALID_DOMAIN: &str = "VALID_DOMAIN";
/// Legacy BPOLY amplitude polynomial degree column.
pub const COL_N_POLY_AMP: &str = "N_POLY_AMP";
/// Legacy BPOLY phase polynomial degree column.
pub const COL_N_POLY_PHASE: &str = "N_POLY_PHASE";
/// Legacy BPOLY amplitude polynomial coefficients column.
pub const COL_POLY_COEFF_AMP: &str = "POLY_COEFF_AMP";
/// Legacy BPOLY phase polynomial coefficients column.
pub const COL_POLY_COEFF_PHASE: &str = "POLY_COEFF_PHASE";
/// Legacy BPOLY phase-units column.
pub const COL_PHASE_UNITS: &str = "PHASE_UNITS";
/// Legacy BPOLY CAL_DESC keyword-subtable name.
pub const LEGACY_CAL_DESC_KEYWORD: &str = "CAL_DESC";

/// Standard keyword-subtable link names used by CASA calibration tables.
pub const STANDARD_SUBTABLE_KEYWORDS: &[&str] = &[
    "OBSERVATION",
    "ANTENNA",
    "FIELD",
    "SPECTRAL_WINDOW",
    "HISTORY",
];

/// Columns required for the first-wave complex calibration-table reader.
pub const REQUIRED_COMPLEX_COLUMNS: &[&str] = &[
    COL_TIME,
    COL_FIELD_ID,
    COL_SPECTRAL_WINDOW_ID,
    COL_ANTENNA1,
    COL_ANTENNA2,
    COL_INTERVAL,
    COL_SCAN_NUMBER,
    COL_CPARAM,
    COL_FLAG,
];

/// Columns required for narrow float-parameter support such as `K Jones`.
pub const REQUIRED_FLOAT_COLUMNS: &[&str] = &[
    COL_TIME,
    COL_FIELD_ID,
    COL_SPECTRAL_WINDOW_ID,
    COL_ANTENNA1,
    COL_ANTENNA2,
    COL_INTERVAL,
    COL_SCAN_NUMBER,
    COL_FPARAM,
    COL_FLAG,
];

/// Columns required for legacy BPOLY apply support.
pub const REQUIRED_BPOLY_COLUMNS: &[&str] = &[
    COL_TIME,
    COL_FIELD_ID,
    COL_ANTENNA1,
    COL_INTERVAL,
    COL_CAL_DESC_ID,
    COL_SCALE_FACTOR,
    COL_VALID_DOMAIN,
    COL_N_POLY_AMP,
    COL_N_POLY_PHASE,
    COL_POLY_COEFF_AMP,
    COL_POLY_COEFF_PHASE,
    COL_PHASE_UNITS,
];

/// Columns tolerated but not required when reading legacy or variant tables.
pub const TOLERATED_OPTIONAL_COLUMNS: &[&str] =
    &[COL_OBSERVATION_ID, COL_PARAMERR, COL_SNR, COL_WEIGHT];
