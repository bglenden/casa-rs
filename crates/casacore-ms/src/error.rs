// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for MeasurementSet operations.

use casacore_tables::TableError;
use casacore_types::measures::MeasureError;

/// Errors produced by MeasurementSet operations.
///
/// Wraps [`TableError`] for low-level table I/O and adds MS-specific variants
/// for schema validation, missing subtables, and column type mismatches.
#[derive(Debug, thiserror::Error)]
pub enum MsError {
    /// An error from the underlying table system.
    #[error(transparent)]
    Table(#[from] TableError),

    /// A required subtable is missing from the MS.
    #[error("required subtable \"{0}\" not found")]
    MissingSubtable(String),

    /// A required column is missing from a table.
    #[error("required column \"{column}\" not found in {table}")]
    MissingColumn {
        /// The missing column name.
        column: String,
        /// The table or subtable where the column was expected.
        table: String,
    },

    /// A column exists but has the wrong data type.
    #[error("column \"{column}\" in {table}: expected {expected}, found {found}")]
    ColumnTypeMismatch {
        /// The column name.
        column: String,
        /// The table or subtable name.
        table: String,
        /// Expected type description.
        expected: String,
        /// Actual type description.
        found: String,
    },

    /// The MS_VERSION keyword is missing or has an unexpected value.
    #[error("MS_VERSION: {0}")]
    VersionError(String),

    /// An optional column was requested but is not present.
    #[error("optional column \"{0}\" is not present")]
    ColumnNotPresent(String),

    /// A stored casacore measure reference code is not recognized.
    #[error("invalid measure reference code {code} in {table}.{column}")]
    InvalidMeasureCode {
        /// The table or subtable name.
        table: String,
        /// The column name.
        column: String,
        /// The invalid integer code.
        code: i32,
    },

    /// An index (antenna, field, etc.) is out of range.
    #[error("{context}: index {index} out of range (max {max})")]
    InvalidIndex {
        /// The out-of-range index value.
        index: usize,
        /// The maximum valid index (exclusive).
        max: usize,
        /// What the index represents (e.g. "antenna_id", "field_id").
        context: String,
    },

    /// A measure conversion error.
    #[error("measure conversion: {0}")]
    Measure(#[from] MeasureError),

    /// A schema construction error.
    #[error("schema error: {0}")]
    Schema(#[from] casacore_tables::SchemaError),
}

/// Convenience type alias for MS operations.
pub type MsResult<T> = Result<T, MsError>;
