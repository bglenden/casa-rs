// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration task result envelope shared by launcher- and JSON-facing workflows.

use std::path::PathBuf;

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
    /// `applycal` on a CASA Multi-MS.
    ApplyMultiMs(ApplyMultiMsReport),
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

/// Result of applying calibration to the physical partitions of a Multi-MS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyMultiMsReport {
    /// The Multi-MS aggregate path.
    pub measurement_set: PathBuf,
    /// The backup made once for the entire Multi-MS, if requested.
    pub flag_backup_version: Option<String>,
    /// Physical partition results in concatenation order.
    pub partitions: Vec<ApplyPartitionReport>,
}

/// The result for one physical MeasurementSet in a Multi-MS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyPartitionReport {
    /// Path of the physical MeasurementSet.
    pub measurement_set: PathBuf,
    /// A failed partition is reported but does not stop the others.
    pub result: Result<ApplyExecutionReport, String>,
}
