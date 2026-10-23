// SPDX-License-Identifier: LGPL-3.0-or-later
//! Errors of the component model and of component-list tables.

use std::path::PathBuf;

use casa_tables::TableError;
use thiserror::Error;

/// A value the component model refuses, as CASA's components module refuses
/// it.
///
/// Each variant names the check CASA makes at run time (not the `DebugAssert`
/// checks a release build of CASA skips), so a component that casa-rs accepts
/// is one CASA accepts too.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum ComponentError {
    /// A shape name that matches none of CASA's (`ComponentType::shape`).
    #[error("unknown component shape {0:?}")]
    UnknownShape(String),
    /// A spectral-model name that matches none of CASA's
    /// (`ComponentType::spectralShape`).
    #[error("unknown spectral shape {0:?}")]
    UnknownSpectrum(String),
    /// A polarisation representation other than Stokes, linear or circular
    /// (`ComponentType::polarisation`).
    #[error("unknown flux polarisation representation {0:?}")]
    UnknownPolarisation(String),
    /// A unit string casa-rs cannot parse.
    #[error("{what}: cannot parse unit {unit:?}: {reason}")]
    Unit {
        /// The quantity whose unit failed.
        what: &'static str,
        /// The unit string.
        unit: String,
        /// The parser's message.
        reason: String,
    },
    /// A flux unit whose dimensions are not those of Jy, Jy.km/s or K.rad2
    /// (`FluxRep::_getConversionUnit`).
    #[error("the flux unit {0:?} has dimensions that are not supported")]
    FluxUnit(String),
    /// An error that is negative, or not an angle (direction errors) or a
    /// frequency (reference-frequency errors).
    #[error("{what} must be a non-negative {kind}, got {value} {unit:?}")]
    BadError {
        /// The error that failed.
        what: &'static str,
        /// The required kind of quantity (`angle` or `frequency`).
        kind: &'static str,
        /// Its value.
        value: f64,
        /// Its unit.
        unit: String,
    },
    /// Shape axes CASA refuses: a non-positive axis, a minor axis longer than
    /// the major axis, or a Gaussian position angle outside ±2π.
    #[error("invalid {shape} axes: {reason}")]
    Axes {
        /// The shape.
        shape: &'static str,
        /// What is wrong.
        reason: String,
    },
    /// A parameter or error vector whose length is not the one the shape or
    /// spectrum takes (`nParameters()`).
    #[error("{what} takes {expected} values, got {actual}")]
    ParameterCount {
        /// The vector (for example "Gaussian shape parameters").
        what: &'static str,
        /// The length CASA's model takes.
        expected: usize,
        /// The length supplied.
        actual: usize,
    },
    /// A spectral-model value CASA refuses: a negative spectral-index or
    /// power-log-polynomial error, mismatched tabulation lengths, or a tabular
    /// spectrum whose reference frequency cannot be expressed in its
    /// tabulation frame.
    #[error("invalid {spectrum} spectrum: {reason}")]
    Spectrum {
        /// The spectral model.
        spectrum: &'static str,
        /// What is wrong.
        reason: String,
    },
}

/// A failure reading or writing a CASA component-list table.
#[derive(Debug, Error)]
pub enum ComponentListError {
    /// casa-tables could not open, read or save the table.
    #[error("component list {path}: {source}")]
    Table {
        /// The table.
        path: PathBuf,
        /// The table-system error.
        #[source]
        source: TableError,
    },
    /// The table lacks a column CASA's `ComponentList::readTable` requires.
    #[error("component list {path}: no column {column} (not a CASA component list)")]
    MissingColumn {
        /// The table.
        path: PathBuf,
        /// The missing column.
        column: &'static str,
    },
    /// A column's measure or quantum keywords are missing or unusable.
    #[error("component list {path}: column {column}: {reason}")]
    ColumnKeywords {
        /// The table.
        path: PathBuf,
        /// The column.
        column: String,
        /// What is wrong.
        reason: String,
    },
    /// A cell holds a value of the wrong kind or shape.
    #[error("component list {path}: row {row}, column {column}: {reason}")]
    Cell {
        /// The table.
        path: PathBuf,
        /// The row.
        row: usize,
        /// The column.
        column: String,
        /// What is wrong.
        reason: String,
    },
    /// A row describes a component CASA's model refuses.
    #[error("component list {path}: row {row}: {source}")]
    Component {
        /// The table.
        path: PathBuf,
        /// The row.
        row: usize,
        /// The refusal.
        #[source]
        source: ComponentError,
    },
    /// Component `index` cannot be written as CASA writes it.
    #[error("component {index} cannot be written to {path}: {source}")]
    Unwritable {
        /// The table being written.
        path: PathBuf,
        /// The component's position in the list.
        index: usize,
        /// The refusal.
        #[source]
        source: ComponentError,
    },
    /// The output path exists. CASA's `rename` creates the table with
    /// `Table::NewNoReplace`, which refuses it too.
    #[error("component list {0}: the output already exists")]
    Exists(PathBuf),
    /// Preparing the staging directory or publishing the finished table
    /// failed.
    #[error("component list {path}: {reason}")]
    Publish {
        /// The output table.
        path: PathBuf,
        /// What failed.
        reason: String,
    },
}
