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
/// Main-table complex-parameter payload column.
pub const COL_CPARAM: &str = "CPARAM";
/// Main-table parameter error column.
pub const COL_PARAMERR: &str = "PARAMERR";
/// Main-table per-parameter flag column.
pub const COL_FLAG: &str = "FLAG";
/// Main-table signal-to-noise column.
pub const COL_SNR: &str = "SNR";
/// Main-table per-parameter weight column.
pub const COL_WEIGHT: &str = "WEIGHT";

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

/// Columns tolerated but not required when reading legacy or variant tables.
pub const TOLERATED_OPTIONAL_COLUMNS: &[&str] =
    &[COL_OBSERVATION_ID, COL_PARAMERR, COL_SNR, COL_WEIGHT];
