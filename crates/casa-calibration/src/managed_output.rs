// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration task result envelope shared by launcher- and JSON-facing workflows.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ApplyExecutionReport, ApplyPlan, BandpassSolveReport, CalibrationStatsReport,
    CalibrationTableSummary, ExportCorrectedDataReport, FluxScaleReport, GainSolveReport,
    GencalReport,
};

/// Canonical structured result for one calibration task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "report", rename_all = "snake_case")]
pub enum CalibrationTaskResult {
    /// `applycal`, or `calibrate` in `apply` mode.
    Apply(ApplyExecutionReport),
    /// `calibrate` in `export_corrected_data` mode.
    ExportCorrectedData(ExportCorrectedDataReport),
    /// `calibrate` in `summary` mode.
    Summary(Vec<CalibrationTableSummary>),
    /// An apply plan, from the [`crate::CalibrationTaskRequest::PlanApply`]
    /// request.
    PlanApply(ApplyPlan),
    /// `calibrate` in `stats` mode.
    Stats(CalibrationStatsReport),
    /// `gaincal`, or `calibrate` in `solve_gain` mode.
    SolveGain(GainSolveReport),
    /// `bandpass`, or `calibrate` in `solve_bandpass` mode.
    SolveBandpass(BandpassSolveReport),
    /// `fluxscale`, or `calibrate` in `fluxscale` mode.
    FluxScale(FluxScaleReport),
    /// `gencal`, or `calibrate` in `gencal` mode.
    Gencal(GencalReport),
}

/// Backward-compatible alias for the launcher-managed calibration output type.
pub type ManagedCalibrationOutput = CalibrationTaskResult;
