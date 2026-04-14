// SPDX-License-Identifier: LGPL-3.0-or-later
//! Limited `gaincal` support for the first solver wave.
//!
//! This module intentionally implements only a narrow, testable slice:
//!
//! - `gaintype=G|T`
//! - `calmode='p|ap'`
//! - `solint='inf'|'int'|<seconds>`
//! - explicit reference antenna
//! - point-source Stokes-I sky model (`smodel=[I,0,0,0]`)
//!
//! The acceptance contract is downstream behavior: the resulting caltable
//! should be CASA-compatible on disk and should yield corrected visibilities
//! close to CASA's own `gaincal` when applied.

pub(crate) mod grouping;
pub(crate) mod kernel;
mod writer;

use std::path::{Path, PathBuf};

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableError};
use casa_types::ScalarValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ApplyExecutionError, ApplyPlanError};
use grouping::{
    all_antenna_ids, build_solve_groups, collect_selected_rows, load_preapplied_rows,
    resolve_refant, validate_smodel, validate_solve_interval,
};
use kernel::solve_group;
use writer::write_gain_caltable;

/// Supported first-wave gain solve families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainType {
    /// Per-receptor complex gains.
    G,
    /// Polarization-collapsed scalar complex gains.
    T,
}

impl GainType {
    fn vis_cal(self) -> &'static str {
        match self {
            Self::G => "G Jones",
            Self::T => "T Jones",
        }
    }
}

/// Supported first-wave solve modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveMode {
    /// Phase-only solve.
    Phase,
    /// Amplitude-and-phase solve.
    AmplitudePhase,
}

/// Supported first-wave gain solution intervals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveInterval {
    /// Solve one solution per `(observation, field, spw, scan)` group.
    Infinite,
    /// Solve one solution per integration timestamp inside each scan.
    Integration,
    /// Solve one solution per contiguous time bucket of the given duration, in seconds.
    Seconds(f64),
}

/// Supported first-wave `gaincal` combine axes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveCombine {
    /// Extend solves across scan boundaries.
    pub scans: bool,
    /// Extend solves across field boundaries.
    pub fields: bool,
}

/// Reference antenna selector for solve requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RefAntSelector {
    /// Exact antenna id.
    AntennaId(i32),
    /// Exact ANTENNA.NAME match.
    AntennaName(String),
}

/// Request for a limited `gaincal` solve.
#[derive(Debug, Clone)]
pub struct GainSolveRequest {
    /// MS selection applied before solving.
    pub selection: MsSelection,
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Gain family to solve.
    pub gain_type: GainType,
    /// Solve mode.
    pub solve_mode: GainSolveMode,
    /// Solution interval.
    pub solve_interval: GainSolveInterval,
    /// Axes to combine while forming solve groups.
    pub combine: GainSolveCombine,
    /// Reference antenna.
    pub refant: RefAntSelector,
    /// Prior calibration tables to apply on the fly before solving.
    pub prior_calibration_tables: Vec<crate::ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction before solving.
    pub parang: bool,
    /// Point-source Stokes model. Only `[I,0,0,0]` is supported in this wave.
    pub smodel: [f32; 4],
}

/// Solve summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveReport {
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Gain family that was solved.
    pub gain_type: GainType,
    /// Resolved reference antenna id.
    pub refant_antenna_id: i32,
    /// Distinct fields represented in the solved table.
    pub field_ids: Vec<i32>,
    /// Distinct SPWs represented in the solved table.
    pub spectral_window_ids: Vec<i32>,
    /// Number of solution rows written.
    pub solution_row_count: usize,
}

/// Errors returned by the limited `gaincal` solver.
#[derive(Debug, Error)]
pub enum GainSolveError {
    /// Opening the MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying error.
        #[source]
        source: MsError,
    },

    /// The selection produced no rows.
    #[error("gain solve selection produced no rows")]
    EmptySelection,

    /// The solve requires a point-source Stokes-I model.
    #[error("unsupported smodel {smodel:?}; only [I,0,0,0] is supported in this wave")]
    UnsupportedSkyModel {
        /// Model vector passed by the caller.
        smodel: [f32; 4],
    },

    /// The requested solve interval is unsupported.
    #[error("unsupported solve interval {solve_interval:?}; seconds values must be > 0")]
    UnsupportedSolveInterval {
        /// Interval requested by the caller.
        solve_interval: GainSolveInterval,
    },

    /// The reference antenna could not be resolved.
    #[error("failed to resolve reference antenna {selector}: {reason}")]
    ResolveRefAnt {
        /// Caller-visible selector.
        selector: String,
        /// Additional context.
        reason: String,
    },

    /// Planning prior on-the-fly calibration application failed.
    #[error("failed to plan prior calibration application: {source}")]
    PriorCalibrationPlan {
        /// Underlying apply-planning error.
        #[source]
        source: Box<ApplyPlanError>,
    },

    /// Evaluating prior on-the-fly calibration application failed.
    #[error("failed to evaluate prior calibration application: {source}")]
    PriorCalibrationApply {
        /// Underlying apply-execution error.
        #[source]
        source: Box<ApplyExecutionError>,
    },

    /// The MS polarization layout is outside the supported diagonal surface.
    #[error(
        "unsupported correlation layout for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
    )]
    UnsupportedCorrelationLayout {
        /// Data description id.
        data_desc_id: i32,
        /// Correlation names.
        correlation_types: Vec<String>,
    },

    /// A table/column cell had an unexpected shape.
    #[error("unsupported parameter shape in {path}: {shape:?}")]
    UnsupportedParameterShape {
        /// Logical source.
        path: String,
        /// Discovered shape.
        shape: Vec<usize>,
    },

    /// The solve failed because the selected data do not connect the reference
    /// antenna to at least one solved antenna.
    #[error(
        "gain solve could not determine a phase for antenna {antenna_id} in field={field_id} spw={spw_id}"
    )]
    UnsolvableAntenna {
        /// Antenna id.
        antenna_id: i32,
        /// Field id.
        field_id: i32,
        /// Spectral window id.
        spw_id: i32,
    },

    /// Persisting the output caltable failed.
    #[error("failed to save caltable {path}: {source}")]
    SaveCalibrationTable {
        /// Output path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Copying an MS subtable into the caltable failed.
    #[error("failed to copy {subtable} subtable into {path}: {source}")]
    CopySubtable {
        /// Subtable name.
        subtable: String,
        /// Destination root.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Filesystem preparation for the output root failed.
    #[error("failed to prepare output path {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },
}

/// Solve a limited `gaincal` request from an on-disk MeasurementSet path.
pub fn solve_gain_from_path(
    path: impl AsRef<Path>,
    request: &GainSolveRequest,
) -> Result<GainSolveReport, GainSolveError> {
    let path = path.as_ref().to_path_buf();
    let ms = MeasurementSet::open(&path).map_err(|source| GainSolveError::OpenMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    solve_gain(&ms, request)
}

/// Solve a limited `gaincal` request from an already-open MeasurementSet.
pub fn solve_gain(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
) -> Result<GainSolveReport, GainSolveError> {
    validate_smodel(request.smodel)?;
    validate_solve_interval(request.solve_interval)?;
    let refant_id = resolve_refant(ms, &request.refant)?;
    let available_antennas = all_antenna_ids(ms)?;
    let rows = collect_selected_rows(ms, &request.selection)?;
    let preapplied_rows = load_preapplied_rows(ms, request)?;
    let groups = build_solve_groups(
        ms,
        &rows,
        preapplied_rows.as_ref(),
        request.gain_type,
        request.smodel[0],
        request.solve_interval,
        request.combine,
    )?;

    if groups.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    let mut solution_rows = Vec::new();
    for group in groups.into_values() {
        solution_rows.extend(solve_group(
            group,
            &available_antennas,
            request.gain_type,
            request.solve_mode,
            refant_id,
        )?);
    }
    write_gain_caltable(ms, request, refant_id, &solution_rows)
}

pub(crate) fn get_i32(table: &Table, row: usize, column: &str) -> Result<i32, GainSolveError> {
    match table
        .get_scalar_cell(row, column)
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ScalarValue::Int32(value) => Ok(*value),
        ScalarValue::Int64(value) => {
            i32::try_from(*value).map_err(|_| GainSolveError::UnsupportedParameterShape {
                path: format!("{column} scalar"),
                shape: vec![row],
            })
        }
        _ => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} scalar"),
            shape: vec![row],
        }),
    }
}

pub(crate) fn get_f64(table: &Table, row: usize, column: &str) -> Result<f64, GainSolveError> {
    match table
        .get_scalar_cell(row, column)
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        _ => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} scalar"),
            shape: vec![row],
        }),
    }
}

pub(crate) fn correlation_receptors(code: i32) -> Option<(usize, usize)> {
    match code {
        5 | 9 => Some((0, 0)),
        6 | 10 => Some((0, 1)),
        7 | 11 => Some((1, 0)),
        8 | 12 => Some((1, 1)),
        _ => None,
    }
}

pub(crate) fn stokes_name(code: i32) -> &'static str {
    match code {
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "UNKNOWN",
    }
}
