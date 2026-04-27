// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration task result envelope shared by launcher- and JSON-facing workflows.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ApplyExecutionReport, ApplyPlan, BandpassSolveReport, CalibrationStatsReport,
    CalibrationTableSummary, ExportCorrectedDataReport, FluxScaleReport, GainSolveReport,
};

/// Canonical structured result for one calibration task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "report", rename_all = "snake_case")]
pub enum CalibrationTaskResult {
    /// `calibrate execute_apply`
    Apply(ApplyExecutionReport),
    /// `calibrate export-corrected`
    ExportCorrectedData(ExportCorrectedDataReport),
    /// `calibrate summary`
    Summary(Vec<CalibrationTableSummary>),
    /// `calibrate plan-apply`
    PlanApply(ApplyPlan),
    /// `calibrate stats`
    Stats(CalibrationStatsReport),
    /// `calibrate solve-gain`
    SolveGain(GainSolveReport),
    /// `calibrate solve-bandpass`
    SolveBandpass(BandpassSolveReport),
    /// `calibrate fluxscale`
    FluxScale(FluxScaleReport),
}

/// Backward-compatible alias for the launcher-managed calibration output type.
pub type ManagedCalibrationOutput = CalibrationTaskResult;
