// SPDX-License-Identifier: LGPL-3.0-or-later
//! Normalized calibration-table summary types.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Parameter family carried by the calibration table payload.
///
/// CASA calibration tables use either complex parameters (`CPARAM`) or float
/// parameters (`FPARAM`) depending on the table family. The first-wave reader
/// accepts both on disk but only marks the complex family as supported for the
/// upcoming `applycal` v1 work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalibrationParameterFamily {
    /// `ParType=Complex` / `CPARAM`.
    Complex,
    /// `ParType=Float` / `FPARAM`.
    Float,
    /// Unable to infer a supported family from the current table.
    Unknown,
}

/// Severity assigned to a validation issue discovered while summarizing a
/// calibration table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalibrationIssueSeverity {
    /// The issue places the table outside the supported v1 apply surface.
    Error,
    /// The issue is tolerated during read and should be normalized later.
    Warning,
}

/// A single validation issue surfaced during summary generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationValidationIssue {
    /// Stable issue code for tests and future UI consumers.
    pub code: String,
    /// Human-readable severity.
    pub severity: CalibrationIssueSeverity,
    /// Human-readable explanation of the issue.
    pub message: String,
}

/// Scalar keyword values lifted into a stable summary shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationKeywordSummary {
    /// `ParType`.
    pub par_type: Option<String>,
    /// `VisCal`.
    pub vis_cal: Option<String>,
    /// `MSName`.
    pub ms_name: Option<String>,
    /// `PolBasis`.
    pub pol_basis: Option<String>,
    /// `CASA_Version`.
    pub casa_version: Option<String>,
}

/// Summary of a standard keyword-linked subtable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationSubtableSummary {
    /// Keyword name, for example `FIELD`.
    pub name: String,
    /// Stored table-reference string from the keyword record.
    pub stored_reference: Option<String>,
    /// Resolved absolute path when the link is present.
    pub resolved_path: Option<PathBuf>,
    /// Whether the resolved subtable exists on disk.
    pub exists: bool,
    /// Row count if the subtable could be opened.
    pub row_count: Option<usize>,
    /// Open error if the path existed but the subtable could not be opened.
    pub open_error: Option<String>,
}

/// Summary of the payload-carrying parameter column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationColumnSummary {
    /// Selected payload column name, usually `CPARAM`.
    pub parameter_column: Option<String>,
    /// Primitive element type reported by the table schema.
    pub parameter_primitive_type: Option<String>,
    /// Shape of the first payload cell, when present.
    pub first_cell_shape: Option<Vec<usize>>,
}

/// Coarse time-domain coverage derived from MAIN rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeCoverageSummary {
    /// Minimum row time in seconds.
    pub min_time: f64,
    /// Maximum row time in seconds.
    pub max_time: f64,
    /// Minimum row interval in seconds, if present.
    pub min_interval: Option<f64>,
    /// Maximum row interval in seconds, if present.
    pub max_interval: Option<f64>,
}

/// Machine-readable summary for one calibration table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationTableSummary {
    /// Table root path that was opened.
    pub path: PathBuf,
    /// `table.info` logical type.
    pub table_type: String,
    /// `table.info` subtype.
    pub table_subtype: String,
    /// Total MAIN row count.
    pub row_count: usize,
    /// Column names present in the MAIN table.
    pub columns: Vec<String>,
    /// Normalized scalar keywords.
    pub keywords: CalibrationKeywordSummary,
    /// Keyword-linked subtables of interest.
    pub subtables: Vec<CalibrationSubtableSummary>,
    /// Derived parameter family.
    pub parameter_family: CalibrationParameterFamily,
    /// Payload-column summary.
    pub parameter_column: CalibrationColumnSummary,
    /// Sorted unique field identifiers observed in MAIN rows.
    pub field_ids: Vec<i32>,
    /// Sorted unique spectral-window identifiers observed in MAIN rows.
    pub spectral_window_ids: Vec<i32>,
    /// Sorted unique `ANTENNA1` identifiers observed in MAIN rows.
    pub antenna1_ids: Vec<i32>,
    /// Sorted unique `ANTENNA2` identifiers observed in MAIN rows.
    pub antenna2_ids: Vec<i32>,
    /// Sorted unique observation identifiers observed in MAIN rows.
    pub observation_ids: Vec<i32>,
    /// Time-domain coverage if `TIME` cells were readable.
    pub time_coverage: Option<TimeCoverageSummary>,
    /// Validation issues discovered while summarizing the table.
    pub issues: Vec<CalibrationValidationIssue>,
}

impl CalibrationTableSummary {
    /// Returns `true` when the table lies inside the first planned `applycal`
    /// surface: a `Calibration` table carrying complex `CPARAM` rows with no
    /// error-level validation issues.
    pub fn supported_for_v1_apply(&self) -> bool {
        self.table_type == crate::constants::TABLE_INFO_TYPE
            && self.parameter_family == CalibrationParameterFamily::Complex
            && self
                .issues
                .iter()
                .all(|issue| issue.severity != CalibrationIssueSeverity::Error)
    }
}
