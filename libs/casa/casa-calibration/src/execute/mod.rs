// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply execution for the first calibration workflow slice.
//!
//! This module consumes an [`ApplyPlan`](crate::apply::ApplyPlan) and applies
//! diagonal complex antenna-based calibration solutions to MS `DATA`,
//! writing `CORRECTED_DATA` and optionally propagating calibration flags.
//!
//! Supported in this wave:
//!
//! - chained complex `CPARAM` tables
//! - float `FPARAM` support for `K Jones` and `Kcross Jones` delay tables
//! - legacy `BPOLY` bandpass tables expanded onto the target SPW grid
//! - diagonal `G` / `T`-style application to `RR/RL/LR/LL` and `XX/XY/YX/YY`
//! - all six apply modes, including strict and flag-only application
//! - circular-feed and full four-correlation linear-feed parallactic angle
//! - linearized and full-matrix instrumental polarization leakage
//! - automatic `CORRECTED_DATA` creation when absent
//!
//! Explicitly deferred from this executor cut:
//!
//! - solver output beyond the existing planner surface

use crate::solve::control::SolveControl;

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use casa_core::cancellation::Cancellation;
use casa_core::{ArrayValue, Complex32, ScalarValue, Value};
use casa_flagging::{
    FlaggingError, save_next_flag_backup, save_next_flag_backup_without_replacing,
};
use casa_ms::MeasurementSet;
use casa_ms::build_table_schema;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::ResolvedMsSelection;
use casa_ms::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, MsError, MsResult,
};
use casa_tables::{ColumnSchema, Table, TableError, TableOptions, TableWriteLock};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::antpos_vla::{TROPO_KEYWORD, TropoDelay, only_vla_observations};
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CAL_DESC_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM,
    COL_N_POLY_AMP, COL_N_POLY_PHASE, COL_PHASE_UNITS, COL_POLY_COEFF_AMP, COL_POLY_COEFF_PHASE,
    COL_SCALE_FACTOR, COL_SPECTRAL_WINDOW_ID, COL_TIME, COL_VALID_DOMAIN, LEGACY_CAL_DESC_KEYWORD,
};
use crate::lookup::interpolate_time_linear;
use crate::model::CalibrationParameterFamily;
use crate::plan::{
    ApplyCalibrationTablePlan, ApplyInterpolationMode, ApplyMode, ApplyPlan, ApplyPlanError,
    ApplyPlanRequest, ApplyRowPlan, RegisteredApplySelection, plan_apply_with_timings,
    plan_with_selection,
};

mod availability;
use availability::execute as execute_apply_plan;
mod fields;
mod fringe;
mod gspline;
mod ionosphere;
mod sky;
pub(crate) use fields::load as load_interpolated_calibration_table;
mod gain_arrays;
mod gain_validity;
mod initialization;
mod ordered_gain;
mod staging;
use gain_validity::correcting_gain;
pub(crate) mod additive;
mod additive_polynomial;
mod bandpass;
pub(crate) mod baseline;
mod chain;
mod channels;
pub(crate) mod parang;
mod polarization;
pub(crate) use polarization::coherency_order;
mod solve_priors;
mod switched_power;
mod tool_activity;
mod tool_corrupt;
mod transform;
use tool_activity::ToolApplyActivity;
pub(crate) use tool_activity::{ApplyTermCounts, LibraryApplyRegistration};
pub(crate) use tool_corrupt::{ModelCorruption, execute_tool_corrupt};
pub(crate) use transform::PreparedReadOnlyApply;
pub(crate) mod tsys_weights;
mod weights;
use chain::{ApplyChain, ApplyStep};
pub(crate) use chain::{has_polarization_matrix, viscal_order};
use channels::{DataChannelGrid, apply_row};
pub use solve_priors::SolvePriorApply;
pub(crate) use solve_priors::{PreappliedRows, PriorSolveBoundary, evaluate_solve_priors};

const PERF_ENV: &str = "CASA_RS_CALIBRATION_PERF";
const PERF_DIR_ENV: &str = "CASA_RS_CALIBRATION_PERF_DIR";
pub(crate) const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

fn calibration_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CASA_RS_CALIBRATION_PROFILE") {
        Ok(value) => {
            let trimmed = value.trim();
            !trimmed.is_empty()
                && trimmed != "0"
                && !trimmed.eq_ignore_ascii_case("false")
                && !trimmed.eq_ignore_ascii_case("off")
        }
        Err(_) => false,
    })
}

fn log_calibration_profile(phase: &str, seconds: f64, detail: impl Into<Option<String>>) {
    let mut line = format!("[casa-calibration profile] phase={phase} dt={seconds:.3}s");
    if let Some(detail) = detail.into()
        && !detail.is_empty()
    {
        line.push(' ');
        line.push_str(&detail);
    }
    eprintln!("{line}");
}
mod columns;
mod gain_pairs;
mod geometry_cache;
mod grids;
mod parallactic;
mod preview;
mod row_apply;
mod row_inputs;
mod table_load;
pub use casa_numerics::legacy_bpoly_chebyshev_value;
mod workflow;

use columns::*;
use gain_pairs::*;
use geometry_cache::*;
pub(crate) use grids::AntposGrid;
pub(crate) use grids::CalibrationGrid;
pub(crate) use grids::CalibrationSolution;
pub(crate) use grids::DelayGrid;
pub(crate) use grids::GainCurveGrid;
pub(crate) use grids::GainGrid;
pub(crate) use grids::LoadedCalibrationTable;
pub(crate) use grids::OpacityGrid;
use grids::*;
pub(crate) use parallactic::ParallacticAngleState;
pub(crate) use parallactic::load_parallactic_angle_state;
pub(crate) use parallactic::single_receptor_angle;
use parallactic::*;
use preview::*;
use row_apply::*;
use row_inputs::*;
pub(crate) use table_load::load_calibration_table;
pub(crate) use table_load::load_simulator_table;
use table_load::*;
pub(crate) use workflow::ToolActivityInput;
pub(crate) use workflow::ToolCorrection;
pub use workflow::execute_apply_from_path;
pub use workflow::execute_apply_from_path_with_options;
pub use workflow::execute_apply_with_preparation_error;
pub use workflow::execute_single_dish_apply;
pub(crate) use workflow::execute_tool_apply;
pub(crate) use workflow::has_usable_apply_rows;
use workflow::*;
pub use workflow::{execute_apply, execute_apply_cancellable};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CalibrationPerfEventKind {
    ApplyPlanSummary,
    ApplyCompleted,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CalibrationPerfEvent {
    kind: CalibrationPerfEventKind,
    monotonic_ns: u64,
    ms_path: String,
    apply_mode: String,
    selected_row_count: usize,
    calibration_table_count: usize,
    parang: bool,
    created_corrected_data_column: bool,
    updated_row_count: usize,
    flagged_row_count: usize,
    flagged_sample_count: usize,
    planning_ns: u64,
    planning_selection_ns: u64,
    planning_selected_rows_ns: u64,
    planning_measurement_set_spectral_windows_ns: u64,
    planning_calibration_table_plans_ns: u64,
    open_measurement_set_ns: u64,
    row_field_index_lookup_ns: u64,
    ensure_corrected_data_ns: u64,
    correlation_lookup_ns: u64,
    calibration_load_ns: u64,
    row_loop_ns: u64,
    row_read_total_ns: u64,
    row_fetch_ns: u64,
    row_compute_ns: u64,
    row_read_overhead_ns: u64,
    row_writeback_ns: u64,
    save_ns: u64,
    execute_apply_plan_ns: u64,
    execute_apply_plan_unattributed_ns: u64,
    drop_ns: u64,
    total_ns: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExecuteApplyPlanTraceSummary {
    selected_row_count: usize,
    calibration_table_count: usize,
    parang: bool,
    created_corrected_data_column: bool,
    updated_row_count: usize,
    flagged_row_count: usize,
    flagged_sample_count: usize,
    row_field_index_lookup_ns: u64,
    ensure_corrected_data_ns: u64,
    correlation_lookup_ns: u64,
    calibration_load_ns: u64,
    row_loop_ns: u64,
    row_read_total_ns: u64,
    row_fetch_ns: u64,
    row_compute_ns: u64,
    row_read_overhead_ns: u64,
    row_writeback_ns: u64,
    save_ns: u64,
    execute_apply_plan_ns: u64,
    execute_apply_plan_unattributed_ns: u64,
}

struct CalibrationPerfTracer {
    started_at: Option<Instant>,
    json_file: Option<File>,
    log_file: Option<File>,
}

impl CalibrationPerfTracer {
    fn from_env() -> Self {
        if std::env::var_os(PERF_ENV).is_none() {
            return Self::disabled();
        }
        let output_dir = std::env::var_os(PERF_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        if create_dir_all(&output_dir).is_err() {
            return Self::disabled();
        }
        let pid = std::process::id();
        let json_path = output_dir.join(format!("casa-calibration-perf-{pid}.jsonl"));
        let log_path = output_dir.join(format!("casa-calibration-perf-{pid}.log"));
        let json_file = open_append_file(&json_path);
        let log_file = open_append_file(&log_path);
        if json_file.is_none() && log_file.is_none() {
            return Self::disabled();
        }
        Self {
            started_at: Some(Instant::now()),
            json_file,
            log_file,
        }
    }

    const fn disabled() -> Self {
        Self {
            started_at: None,
            json_file: None,
            log_file: None,
        }
    }

    fn is_enabled(&self) -> bool {
        self.started_at.is_some()
    }

    fn monotonic_ns(&self) -> u64 {
        self.started_at
            .map(|started| started.elapsed().as_nanos() as u64)
            .unwrap_or_default()
    }

    fn write_event(&mut self, event: &CalibrationPerfEvent) {
        if let Some(file) = self.json_file.as_mut() {
            let _ = serde_json::to_writer(&mut *file, event);
            let _ = writeln!(file);
            let _ = file.flush();
        }
        if let Some(file) = self.log_file.as_mut() {
            let _ = writeln!(
                file,
                "[+{:>7} ms] kind={:?} rows={} total_ms={:.2} planning_ms={:.2} row_field_index_ms={:.2} row_read_ms={:.2} row_read_overhead_ms={:.2} row_write_ms={:.2} save_ms={:.2} unattributed_ms={:.2}",
                event.monotonic_ns / 1_000_000,
                event.kind,
                event.selected_row_count,
                event.total_ns as f64 / 1_000_000.0,
                event.planning_ns as f64 / 1_000_000.0,
                event.row_field_index_lookup_ns as f64 / 1_000_000.0,
                event.row_read_total_ns as f64 / 1_000_000.0,
                event.row_read_overhead_ns as f64 / 1_000_000.0,
                event.row_writeback_ns as f64 / 1_000_000.0,
                event.save_ns as f64 / 1_000_000.0,
                event.execute_apply_plan_unattributed_ns as f64 / 1_000_000.0
            );
            let _ = file.flush();
        }
    }

    fn emit_apply_plan_summary(
        &mut self,
        ms_path: &str,
        plan: &ApplyPlan,
        summary: ExecuteApplyPlanTraceSummary,
    ) {
        if !self.is_enabled() {
            return;
        }
        self.write_event(&CalibrationPerfEvent {
            kind: CalibrationPerfEventKind::ApplyPlanSummary,
            monotonic_ns: self.monotonic_ns(),
            ms_path: ms_path.to_string(),
            apply_mode: format!("{:?}", plan.apply_mode),
            selected_row_count: summary.selected_row_count,
            calibration_table_count: summary.calibration_table_count,
            parang: summary.parang,
            created_corrected_data_column: summary.created_corrected_data_column,
            updated_row_count: summary.updated_row_count,
            flagged_row_count: summary.flagged_row_count,
            flagged_sample_count: summary.flagged_sample_count,
            planning_ns: 0,
            planning_selection_ns: 0,
            planning_selected_rows_ns: 0,
            planning_measurement_set_spectral_windows_ns: 0,
            planning_calibration_table_plans_ns: 0,
            open_measurement_set_ns: 0,
            row_field_index_lookup_ns: summary.row_field_index_lookup_ns,
            ensure_corrected_data_ns: summary.ensure_corrected_data_ns,
            correlation_lookup_ns: summary.correlation_lookup_ns,
            calibration_load_ns: summary.calibration_load_ns,
            row_loop_ns: summary.row_loop_ns,
            row_read_total_ns: summary.row_read_total_ns,
            row_fetch_ns: summary.row_fetch_ns,
            row_compute_ns: summary.row_compute_ns,
            row_read_overhead_ns: summary.row_read_overhead_ns,
            row_writeback_ns: summary.row_writeback_ns,
            save_ns: summary.save_ns,
            execute_apply_plan_ns: summary.execute_apply_plan_ns,
            execute_apply_plan_unattributed_ns: summary.execute_apply_plan_unattributed_ns,
            drop_ns: 0,
            total_ns: summary.execute_apply_plan_ns,
        });
    }

    fn emit_apply_completed(
        &mut self,
        ms_path: &str,
        report: &ApplyExecutionReport,
        drop_ns: u64,
        summary: ExecuteApplyPlanTraceSummary,
    ) {
        if !self.is_enabled() {
            return;
        }
        self.write_event(&CalibrationPerfEvent {
            kind: CalibrationPerfEventKind::ApplyCompleted,
            monotonic_ns: self.monotonic_ns(),
            ms_path: ms_path.to_string(),
            apply_mode: format!("{:?}", report.plan.apply_mode),
            selected_row_count: summary.selected_row_count,
            calibration_table_count: summary.calibration_table_count,
            parang: summary.parang,
            created_corrected_data_column: report.created_corrected_data_column,
            updated_row_count: report.updated_row_count,
            flagged_row_count: report.flagged_row_count,
            flagged_sample_count: report.flagged_sample_count,
            planning_ns: report.timings.planning_ns,
            planning_selection_ns: report.timings.planning_selection_ns,
            planning_selected_rows_ns: report.timings.planning_selected_rows_ns,
            planning_measurement_set_spectral_windows_ns: report
                .timings
                .planning_measurement_set_spectral_windows_ns,
            planning_calibration_table_plans_ns: report.timings.planning_calibration_table_plans_ns,
            open_measurement_set_ns: report.timings.open_measurement_set_ns,
            row_field_index_lookup_ns: summary.row_field_index_lookup_ns,
            ensure_corrected_data_ns: summary.ensure_corrected_data_ns,
            correlation_lookup_ns: summary.correlation_lookup_ns,
            calibration_load_ns: summary.calibration_load_ns,
            row_loop_ns: summary.row_loop_ns,
            row_read_total_ns: summary.row_read_total_ns,
            row_fetch_ns: summary.row_fetch_ns,
            row_compute_ns: summary.row_compute_ns,
            row_read_overhead_ns: summary.row_read_overhead_ns,
            row_writeback_ns: summary.row_writeback_ns,
            save_ns: summary.save_ns,
            execute_apply_plan_ns: summary.execute_apply_plan_ns,
            execute_apply_plan_unattributed_ns: summary.execute_apply_plan_unattributed_ns,
            drop_ns,
            total_ns: report.timings.total_ns,
        });
    }
}

/// Outcome summary for one executor run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyExecutionReport {
    /// The resolved apply plan used by the executor.
    pub plan: ApplyPlan,
    /// Whether `CORRECTED_DATA` was created during execution.
    pub created_corrected_data_column: bool,
    /// Whether the MeasurementSet was mutated and saved.
    pub wrote_measurement_set: bool,
    /// Number of selected rows written to `CORRECTED_DATA`.
    pub updated_row_count: usize,
    /// Number of selected rows whose individual samples all became flagged.
    pub flagged_row_count: usize,
    /// Number of individual correlation-channel samples flagged by calibration.
    pub flagged_sample_count: usize,
    /// The `applycal_N` flag version holding the flags as they were before
    /// this run, when [`ApplyExecutionOptions::flag_backup`] saved one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flag_backup_version: Option<String>,
    /// Timing breakdown for the apply workflow, in nanoseconds.
    pub timings: ApplyExecutionTimings,
}

/// Task-level choices for one apply run that are not part of its plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ApplyExecutionOptions {
    /// Save the MAIN flags to the next free `applycal_N` flag version before
    /// anything is written, as CASA's `applycal(flagbackup=True)` does.
    ///
    /// Flag-writing modes save this before selection and calibration-table
    /// validation, as CASA does; a rejected request can leave a backup.
    /// Cal-only and trial modes save none.
    pub flag_backup: bool,
}

/// Timing breakdown for one apply execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, JsonSchema)]
pub struct ApplyExecutionTimings {
    /// Time spent building the apply plan.
    pub planning_ns: u64,
    /// Time spent applying the MS selection during planning.
    pub planning_selection_ns: u64,
    /// Time spent expanding selected rows into executor-ready metadata.
    pub planning_selected_rows_ns: u64,
    /// Time spent loading selected MS spectral-window metadata.
    pub planning_measurement_set_spectral_windows_ns: u64,
    /// Time spent resolving calibration-table summaries and per-table plans.
    pub planning_calibration_table_plans_ns: u64,
    /// Time spent opening the MeasurementSet from disk.
    pub open_measurement_set_ns: u64,
    /// Time spent resolving cached field indices for the MS main-table row record.
    pub row_field_index_lookup_ns: u64,
    /// Time spent creating or seeding `CORRECTED_DATA` when needed.
    pub ensure_corrected_data_ns: u64,
    /// Time spent loading per-DDID correlation metadata.
    pub correlation_lookup_ns: u64,
    /// Time spent opening and indexing calibration tables.
    pub calibration_load_ns: u64,
    /// Time spent computing per-row corrected data / flags.
    pub row_compute_ns: u64,
    /// Time spent writing corrected rows and flags back to the MS.
    pub row_writeback_ns: u64,
    /// Time spent saving the mutated MeasurementSet.
    pub save_ns: u64,
    /// Total end-to-end apply time represented by this report.
    pub total_ns: u64,
}

/// One evaluated visibility row for dry-run/plot preview inspection.
#[derive(Debug, Clone)]
pub struct EvaluatedApplyRow {
    /// Calibrated visibilities.
    pub corrected_data: ArrayValue,
    /// Propagated flags.
    pub flags: ArrayValue,
    /// Forward diagonal gains for the sky model in a delay solve.
    pub model_gains: Option<ArrayD<Complex32>>,
    /// `[correlation, channel]` factors the `calwt` tables scale weights by,
    /// when any table applies with `calwt`.
    pub weight_factors: Option<ArrayD<f32>>,
    /// Actual corrected weights for a read-only PlotMS view.
    pub plot_weights: Option<PlotWeights>,
}

/// Scalar and optional spectral weights prepared for a PlotMS preview row.
#[derive(Debug, Clone)]
pub struct PlotWeights {
    /// Per-correlation scalar weight vector.
    pub scalar: ArrayValue,
    /// Optional per-correlation per-channel weight spectrum.
    pub spectrum: Option<ArrayValue>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreviewWeightSource {
    UnitFactors,
    FromSigma { channel_weighting: bool },
}

/// Errors returned while executing an apply plan.
#[derive(Debug, Error)]
pub enum ApplyExecutionError {
    /// Cancellation interrupted calibration application before publication.
    #[error("calibration application interrupted")]
    Interrupted,

    /// Private storage for the calculated selected rows failed.
    #[error("stage selected applycal rows: {0}")]
    StageSelectedRows(#[source] std::io::Error),
    /// Planning failed before any mutation occurred.
    #[error(transparent)]
    Plan(Box<ApplyPlanError>),

    /// Opening the MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Saving the flag backup failed; nothing was written to the MeasurementSet.
    #[error("failed to back up the flags of {path}: {source}")]
    FlagBackup {
        /// MeasurementSet path.
        path: String,
        /// Underlying flag-version error.
        #[source]
        source: Box<FlaggingError>,
    },

    /// Opening an auxiliary calibration subtable failed.
    #[error("failed to open calibration subtable {subtable} in {path}: {source}")]
    OpenCalibrationAuxiliaryTable {
        /// Calibration-table path.
        path: String,
        /// Auxiliary subtable name.
        subtable: &'static str,
        /// Underlying table error.
        #[source]
        source: TableError,
    },

    /// The on-disk calibration metadata is outside the supported executor surface.
    #[error("unsupported calibration metadata in {path}: {reason}")]
    UnsupportedCalibrationTable {
        /// Calibration-table path.
        path: String,
        /// Human-readable explanation.
        reason: String,
    },

    /// The row/table combination requested an interpolation mode not yet supported.
    #[error(
        "interpolation mode {interp:?} is not supported for calibration table {path}: {reason}"
    )]
    UnsupportedInterpolation {
        /// Table path.
        path: String,
        /// Interpolation mode.
        interp: ApplyInterpolationMode,
        /// Additional context.
        reason: String,
    },

    /// The executor only supports diagonal correlation layouts for now.
    #[error(
        "unsupported correlation layout for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
    )]
    UnsupportedCorrelationLayout {
        /// Data description id.
        data_desc_id: i32,
        /// Correlation names derived from POLARIZATION.CORR_TYPE.
        correlation_types: Vec<String>,
    },

    /// Linear-feed `parang` requires all four correlation products.
    #[error(
        "parallactic-angle application requires circular-feed correlations or the full XX/XY/YX/YY linear-feed layout for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
    )]
    UnsupportedParallacticAngleBasis {
        /// Data description id.
        data_desc_id: i32,
        /// Correlation names derived from POLARIZATION.CORR_TYPE.
        correlation_types: Vec<String>,
    },

    /// The caltable payload shape is outside the supported diagonal surface.
    #[error("unsupported calibration parameter shape {shape:?} in table {path}")]
    UnsupportedParameterShape {
        /// Table path.
        path: String,
        /// Shape discovered in the caltable.
        shape: Vec<usize>,
    },

    /// Creating or populating `CORRECTED_DATA` failed.
    #[error("failed to create CORRECTED_DATA in {path}: {source}")]
    CreateCorrectedData {
        /// MeasurementSet path.
        path: String,
        /// Underlying table error.
        #[source]
        source: TableError,
    },

    /// A table mutation failed.
    #[error("failed to mutate MeasurementSet {path}: {source}")]
    MutateMeasurementSet {
        /// MeasurementSet path.
        path: String,
        /// Underlying table/MS error.
        #[source]
        source: MsError,
    },
}

impl From<ApplyPlanError> for ApplyExecutionError {
    fn from(source: ApplyPlanError) -> Self {
        Self::Plan(Box::new(source))
    }
}

pub(crate) fn evaluate_apply_rows(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    share_input_flags: bool,
    control: SolveControl<'_>,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    evaluate_rows(
        ms,
        plan,
        share_input_flags,
        PreviewWeightSource::UnitFactors,
        control,
    )
}

/// Evaluate PlotMS-weighted calibrated visibility rows without writing to the MS.
pub fn evaluate_plot_rows(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    channel_weighting: bool,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    evaluate_rows(
        ms,
        plan,
        false,
        PreviewWeightSource::FromSigma { channel_weighting },
        SolveControl::default(),
    )
}

const FAST_GAIN_RECEPTOR_LIMIT: usize = 4;

fn opacity_gain(tau: f32, elevation: f64) -> f32 {
    let zenith_angle = std::f64::consts::FRAC_PI_2 - elevation;
    if zenith_angle < std::f64::consts::FRAC_PI_2 {
        (-f64::from(tau) / zenith_angle.cos()).exp().sqrt() as f32
    } else {
        1.0
    }
}

#[cfg(test)]
mod tests;
