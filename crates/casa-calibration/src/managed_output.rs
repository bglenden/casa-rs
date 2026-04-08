// SPDX-License-Identifier: LGPL-3.0-or-later
//! Managed-output envelope for launcher-facing calibration workflows.

use serde::{Deserialize, Serialize};

use crate::{
    ApplyExecutionReport, ApplyPlan, BandpassSolveReport, CalibrationStatsReport,
    CalibrationTableSummary, FluxScaleReport, GainSolveReport,
};

/// Structured CLI output used by `casars` when `calibrate` runs with managed output enabled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "report", rename_all = "snake_case")]
pub enum ManagedCalibrationOutput {
    /// `calibrate apply`
    Apply(ApplyExecutionReport),
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
