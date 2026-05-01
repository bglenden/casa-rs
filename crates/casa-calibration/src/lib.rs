// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table compatibility support for `casa-rs`.
//!
//! This crate starts the calibration workflow substrate planned for
//! `casars`. The first wave focuses on reading and validating CASA-produced
//! calibration tables, normalising the metadata needed for later apply/solve
//! work, and emitting machine-readable summaries that can anchor slow parity
//! tests against CASA.
//!
//! The public API is intentionally narrow in the first wave:
//!
//! - [`summarize_table`] opens a calibration table written by CASA or Rust and
//!   returns a normalized [`CalibrationTableSummary`].
//! - [`summarize_tables`] batches the same operation for CLI callers.
//! - [`CalibrationTableSummary::supported_for_v1_apply`] indicates whether the
//!   table falls inside the initial `Complex`/`CPARAM` family targeted by the
//!   upcoming `applycal`-class workflow.
//!
//! The implementation is built on [`casa_tables::Table`] rather than a new
//! storage stack, so every on-disk assumption made here is exercised against
//! the same table reader/writer substrate used elsewhere in the repo.

mod bandpass;
mod callib;
mod cli;
pub mod constants;
mod continuum_subtract;
mod corrected_export;
mod execute;
mod fluxscale;
mod gencal;
mod least_squares;
mod managed_output;
mod model;
mod plan;
mod plots;
mod solve;
mod stats;
mod summary;
mod task_contract;

pub use bandpass::{
    BandpassSolveCombine, BandpassSolveError, BandpassSolveReport, BandpassSolveRequest,
    BandpassType, solve_bandpass, solve_bandpass_from_path,
};
pub use callib::{CallibError, load_apply_specs_from_callib, save_apply_specs_to_callib};
pub use cli::{command_schema, run_env};
pub use continuum_subtract::{
    ContinuumSubtractionDataColumn, ContinuumSubtractionError, ContinuumSubtractionReport,
    ContinuumSubtractionRequest, continuum_subtract,
};
pub use corrected_export::{
    ExportCorrectedDataError, ExportCorrectedDataReport, ExportCorrectedDataRequest,
    export_corrected_data,
};
pub use execute::{
    ApplyExecutionError, ApplyExecutionReport, ApplyExecutionTimings, execute_apply,
    execute_apply_from_path,
};
pub use fluxscale::{
    FluxScaleError, FluxScaleFieldResult, FluxScaleReport, FluxScaleRequest, FluxScaleSpwResult,
    fluxscale,
};
pub use gencal::{GencalError, GencalReport, GencalRequest, GencalType, gencal};
pub use managed_output::{CalibrationTaskResult, ManagedCalibrationOutput};
pub use model::{
    CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
    CalibrationValidationIssue, TimeCoverageSummary,
};
pub use plan::{
    ApplyCalibrationTablePlan, ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode,
    ApplyPlan, ApplyPlanError, ApplyPlanRequest, ApplyPlanTimings, ApplyRowPlan, ApplySpwMapping,
    ApplyTableSelection, GainFieldSelector, ResolvedGainField, ResolvedNearestGainField,
    SpectralWindowPlan, plan_apply, plan_apply_from_path, plan_apply_with_timings,
};
pub use plots::{
    CalibrationPlotError, CalibrationPlotPreset, CalibrationPlotRequest,
    build_calibration_plot_payload,
};
pub use solve::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainSolveReport, GainSolveRequest, GainType, RefAntSelector, solve_gain, solve_gain_from_path,
};
pub use stats::{
    CalibrationIndexedStats, CalibrationStatsAxis, CalibrationStatsError, CalibrationStatsReport,
    CalibrationStatsRequest, CalibrationValueStats, calibration_stats,
};
pub use summary::{CalibrationTableError, summarize_table, summarize_tables};
pub use task_contract::{
    CALIBRATION_TASK_PROTOCOL_NAME, CALIBRATION_TASK_PROTOCOL_VERSION, CalibrationProtocolInfo,
    CalibrationTaskRequest, CalibrationTaskSchemaBundle, ContinuumSubtractionTaskRequest,
    ExecuteApplyTaskRequest, ExportCorrectedDataTaskRequest, GencalTaskRequest,
    PlanApplyTaskRequest, SolveBandpassTaskRequest, SolveGainTaskRequest, StatsTaskRequest,
    SummaryTaskRequest,
};
