// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply execution for the first calibration workflow slice.
//!
//! This module consumes an [`ApplyPlan`](crate::ApplyPlan) and applies
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

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use casa_ms::column_def::build_table_schema;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::ResolvedMsSelection;
use casa_ms::{
    FlaggingError, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MsError, MsResult,
    save_next_flag_backup,
};
use casa_tables::{ColumnSchema, Table, TableError, TableOptions, TableWriteLock};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
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
mod gain_validity;
mod initialization;
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
pub(crate) use solve_priors::{
    PreappliedRows, PriorSolveBoundary, SolvePriorApply, evaluate_solve_priors,
};

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

#[derive(Debug, Clone)]
pub(crate) struct EvaluatedApplyRow {
    pub(crate) corrected_data: ArrayValue,
    pub(crate) flags: ArrayValue,
    /// Forward diagonal gains for the sky model in a delay solve.
    pub(crate) model_gains: Option<ArrayD<Complex32>>,
    /// `[correlation, channel]` factors the `calwt` tables scale weights by,
    /// when any table applies with `calwt`.
    pub(crate) weight_factors: Option<ArrayD<f32>>,
    /// Actual corrected weights for a read-only PlotMS view.
    pub(crate) plot_weights: Option<PlotWeights>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotWeights {
    pub(crate) scalar: ArrayValue,
    pub(crate) spectrum: Option<ArrayValue>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreviewWeightSource {
    UnitFactors,
    FromSigma,
}

/// Errors returned while executing an apply plan.
#[derive(Debug, Error)]
pub enum ApplyExecutionError {
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
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    evaluate_rows(
        ms,
        plan,
        share_input_flags,
        PreviewWeightSource::UnitFactors,
    )
}

pub(crate) fn evaluate_plot_rows(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    evaluate_rows(ms, plan, false, PreviewWeightSource::FromSigma)
}

fn evaluate_rows(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    share_input_flags: bool,
    weight_source: PreviewWeightSource,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    let mut rows = HashMap::new();
    for_each_apply_row(
        ms,
        plan,
        apply_observed_column(ms, false),
        false,
        share_input_flags,
        weight_source,
        None,
        |index, row| {
            rows.insert(index, row);
        },
    )?;
    Ok(rows)
}

/// Evaluate one row at a time, allowing TRIAL validation without accumulating
/// corrected data or publishing any MS changes.
#[expect(
    clippy::too_many_arguments,
    reason = "preview and tool callers use the same row executor"
)]
fn for_each_apply_row(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    observed_column: &str,
    forward_model: bool,
    share_input_flags: bool,
    weight_source: PreviewWeightSource,
    mut tool: Option<&mut ToolApplyExecution<'_>>,
    mut consume: impl FnMut(usize, EvaluatedApplyRow),
) -> Result<(), ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let mut chain = ApplyChain::load(ms, plan, forward_model)?;
    if let Some(run) = &tool {
        chain.arrange_for_tool(plan, run.correction.parang_terms, run.additive);
    }

    let any_calwt = applies_preview_weights(plan, &chain);
    let mut columns = vec!["FLAG"];
    if !forward_model {
        columns.push(observed_column);
    }
    let has_sigma_spectrum = if weight_source == PreviewWeightSource::FromSigma {
        columns.push("SIGMA");
        let first_row = plan.selected_rows.first().map(|row| row.row_index);
        weights::has_spectrum(ms.main_table(), first_row, "SIGMA_SPECTRUM")?
    } else {
        false
    };
    if has_sigma_spectrum {
        columns.push("SIGMA_SPECTRUM");
    }
    let mut main_rows = ms
        .main_table()
        .row_accessor()
        .prepare(&columns)
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source: MsError::from(source),
        })?;
    let data_index = main_rows.column_index(observed_column);
    let flag_index = main_rows
        .column_index("FLAG")
        .expect("prepared apply reader includes FLAG");
    let sigma_slots = main_rows
        .column_index("SIGMA")
        .map(|scalar| (scalar, main_rows.column_index("SIGMA_SPECTRUM")));
    let mut geometry_cache = RowGeometryCache::default();
    for row in &plan.selected_rows {
        let correlation_types = correlation_types_by_ddid
            .get(&row.data_desc_id)
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: Vec::new(),
            })?;
        main_rows.load(row.row_index).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            }
        })?;
        let input_flags = main_rows.array_at(flag_index).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            }
        })?;
        let shared;
        let original_flags =
            if share_input_flags && let Some(flags) = shared_channel_flags(input_flags) {
                shared = flags;
                &shared
            } else {
                input_flags
            };
        let units;
        let data = if let Some(index) = data_index {
            main_rows.array_at(index).map_err(|source| {
                ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                }
            })?
        } else {
            units = ArrayValue::Complex32(ArrayD::from_elem(
                IxDyn(original_flags.shape()).f(),
                Complex32::new(1., 0.),
            ));
            &units
        };
        let complex_data;
        let data = if let ArrayValue::Float32(values) = data {
            complex_data = complex_visibility_from_float(values);
            &complex_data
        } else {
            data
        };
        let plot_weights = sigma_slots
            .map(|slots| read_plot_weights(&main_rows, slots, &ms_path))
            .transpose()?;
        if !availability::available(row, plan) {
            if plan.apply_mode == ApplyMode::Trial {
                continue;
            }
            // VisEquation::collapse2 excludes data that cannot be pre-calibrated
            // from solving, independently of the table's calwt setting.
            let shape = IxDyn(data.shape());
            consume(
                row.row_index,
                EvaluatedApplyRow {
                    corrected_data: data.clone(),
                    flags: ArrayValue::Bool(ArrayD::from_elem(shape.clone(), true)),
                    model_gains: None,
                    weight_factors: Some(ArrayD::zeros(shape)),
                    plot_weights,
                },
            );
            continue;
        }
        // Unit weights through the calwt update leave its factors.
        let unit_weight = (any_calwt && plot_weights.is_none()).then(|| {
            ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[correlation_types.len()]), 1.0))
        });

        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types,
                data,
                original_flags: Some(original_flags),
                original_weight: plot_weights
                    .as_ref()
                    .map(|weights| &weights.scalar)
                    .or(unit_weight.as_ref()),
                original_weight_spectrum: plot_weights
                    .as_ref()
                    .and_then(|weights| weights.spectrum.as_ref()),
                has_weight_spectrum: if plot_weights.is_some() {
                    has_sigma_spectrum
                } else {
                    any_calwt
                },
                preserved_corrected_data: None,
                preserved_weight_spectrum: None,
                channel_grid: None,
                forward_model,
                skip_unavailable_terms: false,
                trial: false,
                parang_calwt: false,
            },
            plan,
            &chain,
            Some(&mut geometry_cache),
            None,
            tool.as_deref_mut().map(|run| &mut *run.activity),
        )?;
        let (plot_weights, weight_factors) = match plot_weights {
            Some(weights) => (
                Some(PlotWeights {
                    scalar: result.updated_weight.unwrap_or(weights.scalar),
                    spectrum: result.updated_weight_spectrum.or(weights.spectrum),
                }),
                None,
            ),
            None => (
                None,
                match result.updated_weight_spectrum {
                    Some(ArrayValue::Float32(factors)) => Some(factors),
                    _ => None,
                },
            ),
        };
        consume(
            row.row_index,
            EvaluatedApplyRow {
                corrected_data: result.corrected_data,
                model_gains: None,
                flags: result
                    .updated_flags
                    .unwrap_or_else(|| original_flags.clone()),
                weight_factors,
                plot_weights,
            },
        );
    }

    Ok(())
}

fn applies_preview_weights(plan: &ApplyPlan, chain: &ApplyChain) -> bool {
    plan.apply_mode.writes_corrected_data()
        && plan
            .calibration_tables
            .iter()
            .zip(&chain.tables)
            .any(|(table, loaded)| table.calwt && loaded.supports_calwt)
}

fn shared_channel_flags(flags: &ArrayValue) -> Option<ArrayValue> {
    let ArrayValue::Bool(flags) = flags else {
        return None;
    };
    if flags.ndim() != 2 {
        return None;
    }
    let mut flags = flags.clone();
    for mut channel in flags.axis_iter_mut(ndarray::Axis(1)) {
        if channel.iter().any(|&flag| flag) {
            channel.fill(true);
        }
    }
    Some(ArrayValue::Bool(flags))
}

fn read_plot_weights(
    main_rows: &casa_tables::PreparedTableRow<'_>,
    (scalar, spectrum): (usize, Option<usize>),
    ms_path: &str,
) -> Result<PlotWeights, ApplyExecutionError> {
    let read = |slot, name| {
        let values = main_rows.array_at(slot).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.to_owned(),
                source: MsError::from(source),
            }
        })?;
        weights::from_sigma(values.clone(), name)
    };
    Ok(PlotWeights {
        scalar: read(scalar, "SIGMA")?,
        spectrum: spectrum
            .map(|slot| read(slot, "SIGMA_SPECTRUM"))
            .transpose()?,
    })
}

/// Plan and execute calibration application against an on-disk MeasurementSet.
///
/// This is the mechanism alone and saves no flag backup; the `applycal` task
/// calls [`execute_apply_from_path_with_options`].
pub fn execute_apply_from_path(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_with_options(path, request, ApplyExecutionOptions::default())
}

/// Plan and execute calibration application against an on-disk MeasurementSet
/// with the task-level `options`.
pub fn execute_apply_from_path_with_options(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path.as_ref(), request, options, None, false)
}

pub(crate) fn execute_apply_with_preparation_error(
    path: &Path,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
    preparation_error: Option<crate::plan::ApplyPreparationError<'_>>,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path, request, options, preparation_error, true)
}

pub(crate) fn execute_single_dish_apply(
    path: &Path,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path, request, ApplyExecutionOptions::default(), None, true)
}

fn execute_apply_from_path_inner(
    path: &Path,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
    preparation_error: Option<crate::plan::ApplyPreparationError<'_>>,
    prefer_float: bool,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let path = path.to_path_buf();
    let total_started_at = Instant::now();
    let mut perf_tracer = CalibrationPerfTracer::from_env();
    let (mut report, plan_trace_summary, pre_drop_total_ns) = {
        let open_started_at = Instant::now();
        let mut ms = MeasurementSet::open(&path).map_err(|source| {
            ApplyExecutionError::OpenMeasurementSet {
                path: path.display().to_string(),
                source,
            }
        })?;
        let observed_column = apply_observed_column(&ms, prefer_float);
        let open_measurement_set_ns = open_started_at.elapsed().as_nanos() as u64;
        let flag_backup_version = save_apply_flag_backup(&ms, request.apply_mode, options)?;
        let planning_started_at = Instant::now();
        let (plan, plan_timings) =
            match crate::plan::plan_apply_with_preparation_error(&ms, request, preparation_error) {
                Ok(planned) => planned,
                Err(error) => {
                    // CASA opens with addcorr=True before selectvis/setapply. Keep
                    // successful application on the existing single-write path;
                    // reproduce that initialization if planning rejects a request.
                    if request.apply_mode.writes_corrected_data() {
                        initialization::corrected_data(&mut ms, observed_column)?;
                    }
                    return Err(error.into());
                }
            };
        let planning_ns = planning_started_at.elapsed().as_nanos() as u64;
        if plan.apply_mode == ApplyMode::Trial {
            // Calibrater::correct2 still runs the calibration operators in
            // trial mode, including their input/basis validation.
            for_each_apply_row(
                &ms,
                &plan,
                observed_column,
                false,
                false,
                PreviewWeightSource::UnitFactors,
                None,
                |_, _| {},
            )?;
            return Ok(ApplyExecutionReport {
                plan,
                created_corrected_data_column: false,
                wrote_measurement_set: false,
                updated_row_count: 0,
                flagged_row_count: 0,
                flagged_sample_count: 0,
                flag_backup_version: None,
                timings: ApplyExecutionTimings {
                    planning_ns,
                    planning_selection_ns: plan_timings.selection_ns,
                    planning_selected_rows_ns: plan_timings.selected_rows_ns,
                    planning_measurement_set_spectral_windows_ns: plan_timings
                        .measurement_set_spectral_windows_ns,
                    planning_calibration_table_plans_ns: plan_timings.calibration_table_plans_ns,
                    open_measurement_set_ns,
                    total_ns: total_started_at.elapsed().as_nanos() as u64,
                    ..ApplyExecutionTimings::default()
                },
            });
        }

        let (mut report, trace_summary) = execute_apply_plan(
            &mut ms,
            plan,
            observed_column,
            Some(&mut perf_tracer),
            None,
            None,
        )?;
        report.flag_backup_version = flag_backup_version;
        report.timings.planning_ns = planning_ns;
        report.timings.planning_selection_ns = plan_timings.selection_ns;
        report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
        report.timings.planning_measurement_set_spectral_windows_ns =
            plan_timings.measurement_set_spectral_windows_ns;
        report.timings.planning_calibration_table_plans_ns =
            plan_timings.calibration_table_plans_ns;
        report.timings.open_measurement_set_ns = open_measurement_set_ns;
        let pre_drop_total_ns = total_started_at.elapsed().as_nanos() as u64;
        if calibration_profile_enabled() {
            log_calibration_profile(
                "execute_apply_from_path.pre_drop",
                pre_drop_total_ns as f64 / 1_000_000_000.0,
                Some(format!(
                    "rows={} report_total_so_far={:.3}s",
                    report.updated_row_count,
                    pre_drop_total_ns as f64 / 1_000_000_000.0
                )),
            );
        }
        (report, trace_summary, pre_drop_total_ns)
    };
    let after_drop_ns = total_started_at.elapsed().as_nanos() as u64;
    let drop_ns = after_drop_ns.saturating_sub(pre_drop_total_ns);
    if calibration_profile_enabled() {
        log_calibration_profile(
            "execute_apply_from_path.drop",
            drop_ns as f64 / 1_000_000_000.0,
            Some(format!(
                "total_after_drop={:.3}s",
                after_drop_ns as f64 / 1_000_000_000.0
            )),
        );
    }
    report.timings.total_ns = after_drop_ns;
    perf_tracer.emit_apply_completed(
        &path.display().to_string(),
        &report,
        drop_ns,
        plan_trace_summary,
    );
    Ok(report)
}

fn save_apply_flag_backup(
    ms: &MeasurementSet,
    mode: ApplyMode,
    options: ApplyExecutionOptions,
) -> Result<Option<String>, ApplyExecutionError> {
    if !options.flag_backup || !mode.writes_flags() {
        return Ok(None);
    }
    save_next_flag_backup(ms, "applycal")
        .map(Some)
        .map_err(|source| ApplyExecutionError::FlagBackup {
            path: display_ms_path(ms),
            source: Box::new(source),
        })
}

/// Plan and execute calibration application against an already-open MeasurementSet.
pub fn execute_apply(
    ms: &mut MeasurementSet,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let total_started_at = Instant::now();
    let mut perf_tracer = CalibrationPerfTracer::from_env();
    let planning_started_at = Instant::now();
    let (plan, plan_timings) = plan_apply_with_timings(ms, request)?;
    let planning_ns = planning_started_at.elapsed().as_nanos() as u64;
    if plan.apply_mode == ApplyMode::Trial {
        for_each_apply_row(
            ms,
            &plan,
            apply_observed_column(ms, false),
            false,
            false,
            PreviewWeightSource::UnitFactors,
            None,
            |_, _| {},
        )?;
        return Ok(ApplyExecutionReport {
            plan,
            created_corrected_data_column: false,
            wrote_measurement_set: false,
            updated_row_count: 0,
            flagged_row_count: 0,
            flagged_sample_count: 0,
            flag_backup_version: None,
            timings: ApplyExecutionTimings {
                planning_ns,
                planning_selection_ns: plan_timings.selection_ns,
                planning_selected_rows_ns: plan_timings.selected_rows_ns,
                planning_measurement_set_spectral_windows_ns: plan_timings
                    .measurement_set_spectral_windows_ns,
                planning_calibration_table_plans_ns: plan_timings.calibration_table_plans_ns,
                total_ns: total_started_at.elapsed().as_nanos() as u64,
                ..ApplyExecutionTimings::default()
            },
        });
    }
    let ms_path = display_ms_path(ms);
    let observed_column = apply_observed_column(ms, false);
    let (mut report, trace_summary) = execute_apply_plan(
        ms,
        plan,
        observed_column,
        Some(&mut perf_tracer),
        None,
        None,
    )?;
    report.timings.planning_ns = planning_ns;
    report.timings.planning_selection_ns = plan_timings.selection_ns;
    report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
    report.timings.planning_measurement_set_spectral_windows_ns =
        plan_timings.measurement_set_spectral_windows_ns;
    report.timings.planning_calibration_table_plans_ns = plan_timings.calibration_table_plans_ns;
    report.timings.total_ns = total_started_at.elapsed().as_nanos() as u64;
    perf_tracer.emit_apply_completed(&ms_path, &report, 0, trace_summary);
    Ok(report)
}

#[derive(Clone, Copy)]
pub(crate) struct ToolCorrection {
    pub(crate) has_library_registration: bool,
    pub(crate) strict: bool,
    pub(crate) trial: bool,
    pub(crate) parang_terms: usize,
    pub(crate) parang_calwt: bool,
    pub(crate) tableless_opacity: bool,
}

pub(crate) struct ToolActivityInput<'a> {
    pub(crate) names: &'a [String],
    pub(crate) libraries: &'a [LibraryApplyRegistration],
    pub(crate) opacity: &'a [(&'static str, usize)],
    pub(crate) previous: ApplyTermCounts,
    pub(crate) additive: &'a additive::ToolState,
}

fn unresolved_opacity(ms: &MeasurementSet) -> ApplyExecutionError {
    ApplyExecutionError::MutateMeasurementSet {
        path: display_ms_path(ms),
        source: MsError::InvalidInput("Error in TOpac::calcPar()".into()),
    }
}

pub(crate) fn has_usable_apply_rows(plan: &ApplyPlan) -> bool {
    plan.selected_rows
        .iter()
        .any(|row| availability::available(row, plan))
}

struct ToolApplyExecution<'a> {
    correction: ToolCorrection,
    activity: &'a mut ToolApplyActivity,
    additive: &'a additive::ToolState,
}

/// Correct the selection resolved under the same lock used for row writes.
pub(crate) fn execute_tool_apply(
    ms: &mut MeasurementSet,
    selected: &ResolvedMsSelection,
    lock: &mut TableWriteLock,
    request: &ApplyPlanRequest,
    correction: ToolCorrection,
    tracking: ToolActivityInput<'_>,
) -> Result<(bool, ToolApplyActivity), ApplyExecutionError> {
    let registration = RegisteredApplySelection {
        selected,
        has_library_registration: correction.has_library_registration,
        legacy_corruption: false,
    };
    let (mut plan, _) = plan_with_selection(ms, request, None, Some(registration))?;
    if additive::in_plan(&plan) {
        plan.selected_rows =
            availability::rows_in_buffer_order(ms, std::mem::take(&mut plan.selected_rows))?;
    }
    let mut activity = ToolApplyActivity::from_plan(
        &plan,
        tracking.names,
        tracking.libraries,
        correction.parang_terms,
        tracking.opacity,
        tracking.previous,
    );
    let mut run = ToolApplyExecution {
        correction,
        activity: &mut activity,
        additive: tracking.additive,
    };
    let all_calibrated = plan
        .selected_rows
        .iter()
        .all(|row| availability::available(row, &plan));
    let observed_column = apply_observed_column(ms, false);
    if correction.strict && !all_calibrated {
        availability::execute_strict_tool(ms, plan, observed_column, lock, &mut run)?;
        return Ok((false, activity));
    }
    if correction.tableless_opacity && has_usable_apply_rows(&plan) {
        return Err(unresolved_opacity(ms));
    }
    if plan.apply_mode == ApplyMode::Trial {
        for_each_apply_row(
            ms,
            &plan,
            observed_column,
            false,
            false,
            PreviewWeightSource::UnitFactors,
            Some(&mut run),
            |_, _| {},
        )?;
    } else {
        execute_apply_plan(ms, plan, observed_column, None, Some(lock), Some(&mut run))?;
    }
    Ok((all_calibrated, activity))
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn execute_apply_available(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
    observed_column: &str,
    perf_tracer: Option<&mut CalibrationPerfTracer>,
    lock: Option<&mut TableWriteLock>,
    mut tool: Option<&mut ToolApplyExecution<'_>>,
    retained_chain: Option<&ApplyChain>,
) -> Result<(ApplyExecutionReport, ExecuteApplyPlanTraceSummary), ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let execute_apply_plan_started_at = Instant::now();
    let calibration_load_started_at = Instant::now();
    let correction = tool.as_ref().map(|run| run.correction);
    let loaded;
    let chain = if let Some(chain) = retained_chain {
        chain
    } else {
        let mut chain = ApplyChain::load(ms, &plan, false)?;
        if let Some(tool) = &tool {
            chain.arrange_for_tool(&plan, tool.correction.parang_terms, tool.additive);
        }
        loaded = chain;
        &loaded
    };
    let trial = correction.is_some_and(|tool| tool.trial);
    let calibration_load_ns = calibration_load_started_at.elapsed().as_nanos() as u64;
    let ensure_corrected_data_started_at = Instant::now();
    if lock.is_some() && plan.requires_corrected_data_column && !plan.selected_rows.is_empty() {
        return Err(ApplyExecutionError::MutateMeasurementSet {
            path: ms_path,
            source: MsError::ColumnNotPresent(VisibilityDataColumn::CorrectedData.name().into()),
        });
    }
    let corrected_data_creation = if plan.apply_mode.writes_corrected_data() && lock.is_none() {
        ensure_corrected_data_column(ms, observed_column).map_err(|source| {
            ApplyExecutionError::CreateCorrectedData {
                path: ms_path.clone(),
                source,
            }
        })?
    } else {
        CorrectedDataCreation { created: false }
    };
    let created_corrected_data_column = corrected_data_creation.created;
    let ensure_corrected_data_ns = ensure_corrected_data_started_at.elapsed().as_nanos() as u64;

    let correlation_lookup_started_at = Instant::now();
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let correlation_lookup_ns = correlation_lookup_started_at.elapsed().as_nanos() as u64;
    let parang_calwt = correction.is_some_and(|tool| tool.parang_calwt)
        && plan.selected_rows.iter().all(|row| {
            correlation_types_by_ddid
                .get(&row.data_desc_id)
                .and_then(|types| types.first())
                == Some(&5)
        });
    let mut updated_row_count = 0;
    let mut flagged_row_count = 0;
    let mut flagged_sample_count = 0;
    let mut row_read_total_ns = 0_u64;
    let mut row_fetch_ns = 0_u64;
    let mut row_compute_ns = 0_u64;
    let mut row_writeback_ns = 0_u64;
    let any_calwt = plan.apply_mode.writes_corrected_data()
        && (parang_calwt
            || plan
                .calibration_tables
                .iter()
                .zip(&chain.tables)
                .any(|(table, loaded)| table.calwt && loaded.supports_calwt));
    let first_row = plan.selected_rows.first().map(|row| row.row_index);
    let has_weight_spectrum =
        any_calwt && weights::has_spectrum(ms.main_table(), first_row, "WEIGHT_SPECTRUM")?;
    let has_sigma_spectrum =
        has_weight_spectrum && weights::has_spectrum(ms.main_table(), first_row, "SIGMA_SPECTRUM")?;
    let mut changed_columns: Vec<&'static str> = if plan.apply_mode.writes_corrected_data() {
        vec![VisibilityDataColumn::CorrectedData.name()]
    } else {
        Vec::new()
    };
    let row_loop_started_at = Instant::now();
    let mut geometry_cache = RowGeometryCache::default();
    let mut row_compute_profile =
        calibration_profile_enabled().then(ApplyRowComputeProfile::default);
    if plan.apply_mode.writes_flags() {
        changed_columns.push("FLAG");
    }
    if any_calwt {
        changed_columns.push("WEIGHT");
        if has_weight_spectrum {
            changed_columns.push("WEIGHT_SPECTRUM");
        }
    }
    let maximum_data_elements = maximum_visibility_cells(&plan, &correlation_types_by_ddid)?;
    let maximum_weight_elements = plan
        .selected_rows
        .iter()
        .filter_map(|row| {
            correlation_types_by_ddid
                .get(&row.data_desc_id)
                .map(Vec::len)
        })
        .max()
        .unwrap_or(0);
    let write_columns = changed_columns
        .iter()
        .map(|column| MeasurementSetWriteColumnPlan {
            name: (*column).to_string(),
            bytes_per_row: match *column {
                "CORRECTED_DATA" => maximum_data_elements.saturating_mul(8),
                "FLAG" => maximum_data_elements,
                "WEIGHT" => maximum_weight_elements.saturating_mul(4),
                "WEIGHT_SPECTRUM" => maximum_data_elements.saturating_mul(4),
                _ => 0,
            },
            mode: if *column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
            {
                MeasurementSetColumnWriteMode::Create
            } else {
                MeasurementSetColumnWriteMode::Replace
            },
            storage_manager: if *column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
            {
                MeasurementSetColumnStorage::TiledShape
            } else {
                MeasurementSetColumnStorage::Persisted
            },
            tile_shape: None,
            create_source_column: (*column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
                && observed_column == VisibilityDataColumn::Data.name())
            .then(|| observed_column.to_string()),
        })
        .collect();
    let write_resources =
        MeasurementSetWriteResources::from_system_memory(2).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::InvalidInput(source.to_string()),
            }
        })?;
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        plan.selected_rows.iter().map(|row| row.row_index).collect(),
        write_columns,
        write_resources,
    )
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: ms_path.clone(),
        source: MsError::InvalidInput(source.to_string()),
    })?;
    let batch_rows = write_plan.batch_rows().max(1);
    let write_error =
        |source: casa_ms::MeasurementSetWriteError| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source: MsError::InvalidInput(source.to_string()),
        };

    let row_field_index_lookup_ns = 0;
    let (save_started_at, _) = MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        write_plan,
        lock,
        write_error,
        |write_session, ms| {
            for planned_rows in plan.selected_rows.chunks(batch_rows) {
                let selected_row_indices = planned_rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>();
                let row_read_started_at = Instant::now();
                let row_fetch_started_at = Instant::now();
                let data_values = ms
                    .main_table()
                    .column_accessor(observed_column)
                    .and_then(|column| column.array_cells_owned(&selected_row_indices))
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: ms_path.clone(),
                        source: MsError::from(source),
                    })?;
                let flag_values = if plan.apply_mode.writes_flags() || tool.is_some() {
                    Some(
                        ms.main_table()
                            .column_accessor("FLAG")
                            .and_then(|column| column.array_cells_owned(&selected_row_indices))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?,
                    )
                } else {
                    None
                };
                let weight_values = if any_calwt {
                    Some(
                        ms.main_table()
                            .column_accessor("SIGMA")
                            .and_then(|column| column.array_cells_owned(&selected_row_indices))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?,
                    )
                } else {
                    None
                };
                let weight_spectrum_values = if has_sigma_spectrum {
                    Some(
                        ms.main_table()
                            .column_accessor("SIGMA_SPECTRUM")
                            .and_then(|column| column.array_cells_owned(&selected_row_indices))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?,
                    )
                } else {
                    None
                };
                let partial_channels = planned_rows
                    .iter()
                    .any(|row| plan.channel_selections.contains_key(&row.data_spw_id));
                let preserved_corrected_values = if partial_channels
                    && plan.apply_mode.writes_corrected_data()
                    && !created_corrected_data_column
                {
                    Some(
                        ms.main_table()
                            .column_accessor("CORRECTED_DATA")
                            .and_then(|column| column.array_cells_owned(&selected_row_indices))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?,
                    )
                } else {
                    None
                };
                let preserved_spectrum_values = if partial_channels && has_weight_spectrum {
                    Some(
                        ms.main_table()
                            .column_accessor("WEIGHT_SPECTRUM")
                            .and_then(|column| column.array_cells_owned(&selected_row_indices))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?,
                    )
                } else {
                    None
                };
                row_fetch_ns += row_fetch_started_at.elapsed().as_nanos() as u64;
                row_read_total_ns += row_read_started_at.elapsed().as_nanos() as u64;
                let mut data_values = data_values.into_iter();
                let mut flag_values = flag_values.map(Vec::into_iter);
                let mut weight_values = weight_values.map(Vec::into_iter);
                let mut weight_spectrum_values = weight_spectrum_values.map(Vec::into_iter);
                let mut preserved_corrected_values = preserved_corrected_values.map(Vec::into_iter);
                let mut preserved_spectrum_values = preserved_spectrum_values.map(Vec::into_iter);
                let mut corrected_batch = Vec::with_capacity(planned_rows.len());
                let mut flag_batch = Vec::with_capacity(planned_rows.len());
                let mut weight_batch = Vec::with_capacity(planned_rows.len());
                let mut weight_spectrum_batch = Vec::with_capacity(planned_rows.len());

                for row in planned_rows {
                    let data = required_prefetched_array(
                        data_values.next().flatten(),
                        row.row_index,
                        observed_column,
                        &ms_path,
                    )?;
                    let data = match data {
                        ArrayValue::Float32(values) => complex_visibility_from_float(&values),
                        data => data,
                    };
                    let prefetched = PrefetchedExecutionRowInputs {
                        trial,
                        parang_calwt,
                        preserved_corrected_data: preserved_corrected_values
                            .as_mut()
                            .and_then(|values| values.next().flatten()),
                        preserved_weight_spectrum: preserved_spectrum_values
                            .as_mut()
                            .and_then(|values| values.next().flatten()),
                        data,
                        original_flags: flag_values
                            .as_mut()
                            .map(|values| {
                                required_prefetched_array(
                                    values.next().flatten(),
                                    row.row_index,
                                    "FLAG",
                                    &ms_path,
                                )
                            })
                            .transpose()?,
                        original_weight: weight_values
                            .as_mut()
                            .map(|values| {
                                weights::from_sigma(
                                    required_prefetched_array(
                                        values.next().flatten(),
                                        row.row_index,
                                        "SIGMA",
                                        &ms_path,
                                    )?,
                                    "SIGMA",
                                )
                            })
                            .transpose()?,
                        original_weight_spectrum: weight_spectrum_values
                            .as_mut()
                            .map(|values| {
                                weights::from_sigma(
                                    required_prefetched_array(
                                        values.next().flatten(),
                                        row.row_index,
                                        "SIGMA_SPECTRUM",
                                        &ms_path,
                                    )?,
                                    "SIGMA_SPECTRUM",
                                )
                            })
                            .transpose()?,
                    };
                    let correlation_types = correlation_types_by_ddid
                        .get(&row.data_desc_id)
                        .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                            data_desc_id: row.data_desc_id,
                            correlation_types: Vec::new(),
                        })?;
                    let row_compute_started_at = Instant::now();
                    let result = apply_row_prefetched(
                        row,
                        correlation_types,
                        &prefetched,
                        any_calwt && has_weight_spectrum,
                        &plan,
                        chain,
                        Some(&mut geometry_cache),
                        row_compute_profile.as_mut(),
                        tool.as_deref_mut().map(|run| &mut *run.activity),
                    )?;
                    row_compute_ns += row_compute_started_at.elapsed().as_nanos() as u64;
                    if plan.apply_mode.writes_corrected_data() {
                        corrected_batch.push(result.corrected_data);
                    }
                    if plan.apply_mode.writes_flags() {
                        flag_batch.push(result.updated_flags.unwrap_or_else(|| {
                            prefetched
                                .original_flags
                                .expect("CalFlag rows always prefetch FLAG")
                        }));
                        if result.row_became_fully_flagged {
                            flagged_row_count += 1;
                        }
                        flagged_sample_count += result.newly_flagged_samples;
                    }
                    if any_calwt {
                        weight_batch.push(result.updated_weight.ok_or_else(|| {
                            ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::InvalidInput(
                                    "calwt row did not produce WEIGHT output".to_string(),
                                ),
                            }
                        })?);
                        if has_weight_spectrum {
                            weight_spectrum_batch.push(result.updated_weight_spectrum.ok_or_else(
                                || {
                                    ApplyExecutionError::MutateMeasurementSet {
                                        path: ms_path.clone(),
                                        source: MsError::InvalidInput(
                                            "calwt row did not produce WEIGHT_SPECTRUM output"
                                                .to_string(),
                                        ),
                                    }
                                },
                            )?);
                        }
                    }
                    updated_row_count += 1;
                }

                let mut columns = if plan.apply_mode.writes_corrected_data() {
                    vec![MeasurementSetMutationColumnBatch {
                        name: VisibilityDataColumn::CorrectedData.name().to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(corrected_batch),
                    }]
                } else {
                    Vec::new()
                };
                if plan.apply_mode.writes_flags() {
                    columns.push(MeasurementSetMutationColumnBatch {
                        name: "FLAG".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(flag_batch),
                    });
                }
                if any_calwt {
                    columns.push(MeasurementSetMutationColumnBatch {
                        name: "WEIGHT".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(weight_batch),
                    });
                    if has_weight_spectrum {
                        columns.push(MeasurementSetMutationColumnBatch {
                            name: "WEIGHT_SPECTRUM".to_string(),
                            values: MeasurementSetMutationColumnValues::Arrays(
                                weight_spectrum_batch,
                            ),
                        });
                    }
                }
                let row_writeback_started_at = Instant::now();
                write_session
                    .write_mutation_batch(
                        ms,
                        MeasurementSetMutationBatch {
                            row_indices: selected_row_indices,
                            columns,
                        },
                    )
                    .map_err(write_error)?;
                row_writeback_ns += row_writeback_started_at.elapsed().as_nanos() as u64;
            }
            Ok(Instant::now())
        },
    )?;
    let row_loop_ns = save_started_at
        .duration_since(row_loop_started_at)
        .as_nanos() as u64;
    let save_ns = save_started_at.elapsed().as_nanos() as u64;
    let row_read_overhead_ns = row_read_total_ns.saturating_sub(row_fetch_ns);
    let execute_apply_plan_ns = execute_apply_plan_started_at.elapsed().as_nanos() as u64;
    let bucketed_ns = ensure_corrected_data_ns
        + correlation_lookup_ns
        + calibration_load_ns
        + row_field_index_lookup_ns
        + row_read_total_ns
        + row_compute_ns
        + row_writeback_ns
        + save_ns;
    let execute_apply_plan_unattributed_ns = execute_apply_plan_ns.saturating_sub(bucketed_ns);
    let trace_summary = ExecuteApplyPlanTraceSummary {
        selected_row_count: plan.selected_rows.len(),
        calibration_table_count: plan.calibration_tables.len(),
        parang: plan.parang,
        created_corrected_data_column,
        updated_row_count,
        flagged_row_count,
        flagged_sample_count,
        row_field_index_lookup_ns,
        ensure_corrected_data_ns,
        correlation_lookup_ns,
        calibration_load_ns,
        row_loop_ns,
        row_read_total_ns,
        row_fetch_ns,
        row_compute_ns,
        row_read_overhead_ns,
        row_writeback_ns,
        save_ns,
        execute_apply_plan_ns,
        execute_apply_plan_unattributed_ns,
    };
    if let Some(perf_tracer) = perf_tracer {
        perf_tracer.emit_apply_plan_summary(&ms_path, &plan, trace_summary);
    }
    if calibration_profile_enabled() {
        log_calibration_profile(
            "execute_apply_plan",
            execute_apply_plan_ns as f64 / 1_000_000_000.0,
            Some(format!(
                "rows={} row_loop={:.3}s bucketed={:.3}s unattributed={:.3}s ensure_corrected_data={:.3}s correlation_lookup={:.3}s calibration_load={:.3}s row_field_index_lookup={:.3}s row_read_total={:.3}s row_fetch={:.3}s row_compute={:.3}s row_read_overhead={:.3}s row_writeback={:.3}s save={:.3}s",
                plan.selected_rows.len(),
                row_loop_ns as f64 / 1_000_000_000.0,
                bucketed_ns as f64 / 1_000_000_000.0,
                execute_apply_plan_unattributed_ns as f64 / 1_000_000_000.0,
                ensure_corrected_data_ns as f64 / 1_000_000_000.0,
                correlation_lookup_ns as f64 / 1_000_000_000.0,
                calibration_load_ns as f64 / 1_000_000_000.0,
                row_field_index_lookup_ns as f64 / 1_000_000_000.0,
                row_read_total_ns as f64 / 1_000_000_000.0,
                row_fetch_ns as f64 / 1_000_000_000.0,
                row_compute_ns as f64 / 1_000_000_000.0,
                row_read_overhead_ns as f64 / 1_000_000_000.0,
                row_writeback_ns as f64 / 1_000_000_000.0,
                save_ns as f64 / 1_000_000_000.0
            )),
        );
        if let Some(profile) = row_compute_profile {
            log_calibration_profile(
                "apply_row_compute",
                row_compute_ns as f64 / 1_000_000_000.0,
                Some(profile.detail_string()),
            );
        }
    }

    Ok((
        ApplyExecutionReport {
            plan,
            created_corrected_data_column,
            wrote_measurement_set: true,
            updated_row_count,
            flagged_row_count,
            flagged_sample_count,
            flag_backup_version: None,
            timings: ApplyExecutionTimings {
                planning_ns: 0,
                open_measurement_set_ns: 0,
                row_field_index_lookup_ns,
                ensure_corrected_data_ns,
                correlation_lookup_ns,
                calibration_load_ns,
                row_compute_ns,
                row_writeback_ns,
                save_ns,
                total_ns: 0,
                ..ApplyExecutionTimings::default()
            },
        },
        trace_summary,
    ))
}

struct ExecutionRowResult {
    corrected_data: ArrayValue,
    updated_flags: Option<ArrayValue>,
    updated_weight: Option<ArrayValue>,
    updated_weight_spectrum: Option<ArrayValue>,
    newly_flagged_samples: usize,
    row_became_fully_flagged: bool,
}

#[derive(Clone, Copy)]
struct ExecutionRowInputs<'a> {
    correlation_types: &'a [i32],
    data: &'a ArrayValue,
    original_flags: Option<&'a ArrayValue>,
    original_weight: Option<&'a ArrayValue>,
    original_weight_spectrum: Option<&'a ArrayValue>,
    has_weight_spectrum: bool,
    preserved_corrected_data: Option<&'a ArrayValue>,
    preserved_weight_spectrum: Option<&'a ArrayValue>,
    channel_grid: Option<DataChannelGrid<'a>>,
    forward_model: bool,
    skip_unavailable_terms: bool,
    trial: bool,
    parang_calwt: bool,
}

struct PrefetchedExecutionRowInputs {
    trial: bool,
    parang_calwt: bool,
    data: ArrayValue,
    original_flags: Option<ArrayValue>,
    original_weight: Option<ArrayValue>,
    original_weight_spectrum: Option<ArrayValue>,
    preserved_corrected_data: Option<ArrayValue>,
    preserved_weight_spectrum: Option<ArrayValue>,
}

fn maximum_visibility_cells(
    plan: &ApplyPlan,
    correlation_types_by_ddid: &HashMap<i32, Vec<i32>>,
) -> Result<usize, ApplyExecutionError> {
    plan.selected_rows
        .iter()
        .try_fold(0_usize, |maximum_elements, row| {
            let correlations = correlation_types_by_ddid
                .get(&row.data_desc_id)
                .map(Vec::len)
                .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: Vec::new(),
                })?;
            let channels = plan
                .measurement_set_spectral_windows
                .iter()
                .find(|spw| spw.spw_id == row.data_spw_id)
                .and_then(|spw| usize::try_from(spw.num_chan).ok())
                .ok_or_else(|| ApplyExecutionError::UnsupportedParameterShape {
                    path: format!("<measurement-set SPW {}>", row.data_spw_id),
                    shape: Vec::new(),
                })?;
            let elements = correlations.checked_mul(channels).ok_or_else(|| {
                ApplyExecutionError::UnsupportedParameterShape {
                    path: "<measurement-set row byte overflow>".to_string(),
                    shape: vec![correlations, channels],
                }
            })?;
            Ok::<_, ApplyExecutionError>(maximum_elements.max(elements))
        })
}

fn required_prefetched_array(
    value: Option<ArrayValue>,
    row_index: usize,
    column: &str,
    path: &str,
) -> Result<ArrayValue, ApplyExecutionError> {
    value.ok_or_else(|| ApplyExecutionError::MutateMeasurementSet {
        path: path.to_string(),
        source: MsError::from(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    })
}

#[derive(Debug, Default)]
struct ApplyRowComputeProfile {
    rows: usize,
    table_applications: usize,
    setup_ns: u64,
    table_lookup_ns: u64,
    row_dependent_grid_ns: u64,
    row_dependent_fast_pairs: usize,
    row_dependent_materialized_pairs: usize,
    fast_gain_apply_ns: u64,
    generic_sample_apply_ns: u64,
    parallactic_angle_ns: u64,
    weight_finalize_ns: u64,
}

impl ApplyRowComputeProfile {
    fn add_elapsed(bucket: &mut u64, started_at: Option<Instant>) {
        if let Some(started_at) = started_at {
            *bucket += started_at.elapsed().as_nanos() as u64;
        }
    }

    fn detail_string(&self) -> String {
        format!(
            "rows={} table_apps={} setup={:.3}s lookup={:.3}s row_dependent_grid={:.3}s row_dependent_fast_pairs={} row_dependent_materialized_pairs={} fast_gain={:.3}s generic_sample={:.3}s parang={:.3}s weight_finalize={:.3}s",
            self.rows,
            self.table_applications,
            self.setup_ns as f64 / 1_000_000_000.0,
            self.table_lookup_ns as f64 / 1_000_000_000.0,
            self.row_dependent_grid_ns as f64 / 1_000_000_000.0,
            self.row_dependent_fast_pairs,
            self.row_dependent_materialized_pairs,
            self.fast_gain_apply_ns as f64 / 1_000_000_000.0,
            self.generic_sample_apply_ns as f64 / 1_000_000_000.0,
            self.parallactic_angle_ns as f64 / 1_000_000_000.0,
            self.weight_finalize_ns as f64 / 1_000_000_000.0,
        )
    }
}

#[derive(Default)]
struct RowGeometryCache {
    elevations: HashMap<RowGeometryKey, f64>,
    projected_offsets: HashMap<ProjectedOffsetKey, [f64; 3]>,
    materialized_grids: HashMap<MaterializedGridKey, Arc<CalibrationGrid>>,
    bandpass_power: bandpass::PowerCache,
    calibration_lookups: HashMap<CalibrationLookupKey, Option<fields::GridAtTime>>,
    transient_lookups: HashMap<CalibrationLookupKey, Option<fields::GridAtTime>>,
    transient_frequency_grids: HashMap<MaterializedGridKey, Arc<CalibrationGrid>>,
    transient_time: Option<u64>,
    additive_matrices: HashMap<(usize, i32), additive::CachedMatrix>,
    last_elevation_context: Option<RowGeometryContextKey>,
    last_elevations: Vec<Option<f64>>,
    last_projected_context: Option<RowGeometryContextKey>,
    last_projected_offsets: Vec<Option<LastProjectedOffset>>,
    last_fast_term_context: Option<RowGeometryContextKey>,
    last_fast_antpos_delays: Vec<Vec<Option<FastAntposDelay>>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RowGeometryKey {
    time_bits: u64,
    field_id: i32,
    antenna_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowGeometryContextKey {
    time_bits: u64,
    field_id: i32,
}

#[derive(Clone, Copy, Debug)]
struct LastProjectedOffset {
    offset_bits: [u64; 3],
    uvw: [f64; 3],
}

#[derive(Clone, Copy, Debug)]
struct FastAntposDelay {
    offset_bits: [u64; 3],
    delay_ns: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ProjectedOffsetKey {
    row: RowGeometryKey,
    offset_bits: [u64; 3],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct MaterializedGridKey {
    row: RowGeometryKey,
    kind: u8,
    data_spw_id: i32,
    grid_id: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CalibrationLookupKey {
    table_index: usize,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    time_bits: u64,
    interp: u8,
    observation_id: i32,
    scan_number: i32,
    data_spw_id: i32,
}

impl RowGeometryCache {
    fn elevation(
        &mut self,
        engine: &MsCalEngine,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
    ) -> MsResult<f64> {
        let key = RowGeometryKey::new(time_seconds, field_id, antenna_id);
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            if self.last_elevation_context == Some(context) {
                if let Some(Some(elevation)) = self.last_elevations.get(antenna_index) {
                    return Ok(*elevation);
                }
            } else {
                self.last_elevation_context = Some(context);
                self.last_elevations.clear();
            }
            if let Some(elevation) = self.elevations.get(&key).copied() {
                self.remember_last_elevation(antenna_index, elevation);
                return Ok(elevation);
            }
        } else if let Some(elevation) = self.elevations.get(&key) {
            return Ok(*elevation);
        }
        let (_az, elevation) = engine.azel(
            time_seconds,
            usize::try_from(field_id).unwrap_or(usize::MAX),
            usize::try_from(antenna_id).unwrap_or(usize::MAX),
        )?;
        self.elevations.insert(key, elevation);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            self.remember_last_elevation(antenna_index, elevation);
        }
        Ok(elevation)
    }

    fn project_itrf_offset_to_uvw(
        &mut self,
        engine: &MsCalEngine,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
        offset_m: [f64; 3],
    ) -> MsResult<[f64; 3]> {
        let offset_bits = [
            offset_m[0].to_bits(),
            offset_m[1].to_bits(),
            offset_m[2].to_bits(),
        ];
        let key = ProjectedOffsetKey {
            row: RowGeometryKey::new(time_seconds, field_id, antenna_id),
            offset_bits,
        };
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            if self.last_projected_context == Some(context) {
                if let Some(Some(projected)) = self.last_projected_offsets.get(antenna_index)
                    && projected.offset_bits == offset_bits
                {
                    return Ok(projected.uvw);
                }
            } else {
                self.last_projected_context = Some(context);
                self.last_projected_offsets.clear();
            }
            if let Some(uvw) = self.projected_offsets.get(&key).copied() {
                self.remember_last_projected_offset(antenna_index, offset_bits, uvw);
                return Ok(uvw);
            }
        } else if let Some(uvw) = self.projected_offsets.get(&key) {
            return Ok(*uvw);
        }
        let uvw = engine.project_itrf_offset_to_uvw(
            time_seconds,
            usize::try_from(field_id).unwrap_or(usize::MAX),
            usize::try_from(antenna_id).unwrap_or(usize::MAX),
            offset_m,
        )?;
        self.projected_offsets.insert(key, uvw);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            self.remember_last_projected_offset(antenna_index, offset_bits, uvw);
        }
        Ok(uvw)
    }

    fn remember_last_elevation(&mut self, antenna_index: usize, elevation: f64) {
        if self.last_elevations.len() <= antenna_index {
            self.last_elevations.resize(antenna_index + 1, None);
        }
        self.last_elevations[antenna_index] = Some(elevation);
    }

    fn remember_last_projected_offset(
        &mut self,
        antenna_index: usize,
        offset_bits: [u64; 3],
        uvw: [f64; 3],
    ) {
        if self.last_projected_offsets.len() <= antenna_index {
            self.last_projected_offsets.resize(antenna_index + 1, None);
        }
        self.last_projected_offsets[antenna_index] = Some(LastProjectedOffset { offset_bits, uvw });
    }

    fn ensure_fast_term_context(&mut self, time_seconds: f64, field_id: i32) {
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if self.last_fast_term_context != Some(context) {
            self.last_fast_term_context = Some(context);
            self.last_fast_antpos_delays.clear();
        }
    }

    fn cached_fast_antpos_delay(
        &mut self,
        table_index: usize,
        antenna_index: usize,
        offset_bits: [u64; 3],
    ) -> Option<f64> {
        self.last_fast_antpos_delays
            .get(table_index)
            .and_then(|values| values.get(antenna_index))
            .and_then(|value| *value)
            .and_then(|value| (value.offset_bits == offset_bits).then_some(value.delay_ns))
    }

    fn remember_fast_antpos_delay(
        &mut self,
        table_index: usize,
        antenna_index: usize,
        offset_bits: [u64; 3],
        delay_ns: f64,
    ) {
        let values = resize_nested_vec(
            &mut self.last_fast_antpos_delays,
            table_index,
            antenna_index,
        );
        values[antenna_index] = Some(FastAntposDelay {
            offset_bits,
            delay_ns,
        });
    }

    fn materialized_grid(
        &self,
        key: MaterializedGridKey,
        transient: bool,
    ) -> Option<Arc<CalibrationGrid>> {
        if transient {
            &self.transient_frequency_grids
        } else {
            &self.materialized_grids
        }
        .get(&key)
        .cloned()
    }

    fn insert_materialized_grid(
        &mut self,
        key: MaterializedGridKey,
        grid: Arc<CalibrationGrid>,
        transient: bool,
    ) {
        if transient {
            &mut self.transient_frequency_grids
        } else {
            &mut self.materialized_grids
        }
        .insert(key, grid);
    }

    fn calibration_lookup<F>(
        &mut self,
        key: CalibrationLookupKey,
        transient_time: Option<u64>,
        load: F,
    ) -> Result<Option<fields::GridAtTime>, ApplyExecutionError>
    where
        F: FnOnce() -> Result<Option<fields::GridAtTime>, ApplyExecutionError>,
    {
        let lookups = if let Some(time) = transient_time {
            if self.transient_time != Some(time) {
                self.transient_frequency_grids.clear();
                self.transient_lookups.clear();
                self.transient_time = Some(time);
            }
            &mut self.transient_lookups
        } else {
            &mut self.calibration_lookups
        };
        if let Some(grid) = lookups.get(&key) {
            return Ok(grid.clone());
        }
        let grid = load()?;
        lookups.insert(key, grid.clone());
        Ok(grid)
    }
}

impl RowGeometryKey {
    fn new(time_seconds: f64, field_id: i32, antenna_id: i32) -> Self {
        Self {
            time_bits: time_seconds.to_bits(),
            field_id,
            antenna_id,
        }
    }
}

impl RowGeometryContextKey {
    fn new(time_seconds: f64, field_id: i32) -> Self {
        Self {
            time_bits: time_seconds.to_bits(),
            field_id,
        }
    }
}

fn resize_nested_vec<T: Clone>(
    values: &mut Vec<Vec<Option<T>>>,
    table_index: usize,
    antenna_index: usize,
) -> &mut Vec<Option<T>> {
    if values.len() <= table_index {
        values.resize_with(table_index + 1, Vec::new);
    }
    let values = &mut values[table_index];
    if values.len() <= antenna_index {
        values.resize(antenna_index + 1, None);
    }
    values
}

fn materialized_grid_key(
    grid: &Arc<CalibrationGrid>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    data_spw_id: i32,
) -> Option<MaterializedGridKey> {
    let kind = match grid.as_ref() {
        CalibrationGrid::Antpos(_) => 1,
        CalibrationGrid::GainCurve(_) => 2,
        CalibrationGrid::Opacity(_) | CalibrationGrid::SpectralOpacity(_) => 3,
        _ => return None,
    };
    Some(MaterializedGridKey {
        row: RowGeometryKey::new(time_seconds, field_id, antenna_id),
        kind,
        data_spw_id,
        grid_id: Arc::as_ptr(grid) as usize,
    })
}

fn calibration_lookup_key(
    table_index: usize,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    interp: ApplyInterpolationMode,
) -> CalibrationLookupKey {
    CalibrationLookupKey {
        table_index,
        field_id,
        spw_id,
        antenna_id,
        time_bits: time_seconds.to_bits(),
        observation_id: -1,
        scan_number: -1,
        data_spw_id: -1,
        interp: match interp {
            ApplyInterpolationMode::Nearest => 1,
            ApplyInterpolationMode::Linear => 2,
            ApplyInterpolationMode::NearestLinear => 3,
            ApplyInterpolationMode::Casa { .. } => 4,
            ApplyInterpolationMode::Invalid => 0,
        },
    }
}

pub(crate) struct ParallacticAngleState {
    engine: MsCalEngine,
    feed_rows: HashMap<(i32, i32), Vec<FeedAngleRow>>,
}

#[derive(Debug, Clone, Copy)]
struct FeedAngleRow {
    spectral_window_id: i32,
    time_seconds: f64,
    interval_seconds: f64,
    receptor0_angle_rad: f64,
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn apply_row_all_channels(
    row: &ApplyRowPlan,
    inputs: ExecutionRowInputs<'_>,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry_cache: Option<&mut RowGeometryCache>,
    compute_profile: Option<&mut ApplyRowComputeProfile>,
    mut activity: Option<&mut ToolApplyActivity>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    let mut compute_profile = compute_profile;
    let setup_started_at = compute_profile.as_ref().map(|_| Instant::now());
    let ExecutionRowInputs {
        correlation_types,
        data,
        original_flags,
        original_weight,
        original_weight_spectrum,
        has_weight_spectrum,
        channel_grid,
        forward_model,
        skip_unavailable_terms,
        trial,
        parang_calwt,
        ..
    } = inputs;
    let ArrayValue::Complex32(data) = data else {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set DATA>".to_string(),
            shape: data.shape().to_vec(),
        });
    };
    let flag_array = match original_flags {
        Some(ArrayValue::Bool(flag_array)) => Some(flag_array),
        Some(other) => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set FLAG>".to_string(),
                shape: other.shape().to_vec(),
            });
        }
        None => None,
    };
    if data.ndim() != 2
        || flag_array
            .is_some_and(|flag_array| flag_array.ndim() != 2 || data.shape() != flag_array.shape())
    {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set row>".to_string(),
            shape: data.shape().to_vec(),
        });
    }
    if data.shape()[0] != correlation_types.len() {
        return Err(ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: row.data_desc_id,
            correlation_types: correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        });
    }

    let mut corrected = data.clone();
    let propagate_flags = plan.apply_mode.writes_flags() || activity.is_some();
    let mut flags = if propagate_flags {
        flag_array
            .ok_or_else(|| ApplyExecutionError::MutateMeasurementSet {
                path: "<measurement-set FLAG>".to_string(),
                source: MsError::from(TableError::ColumnNotFound {
                    row_index: row.row_index,
                    column: "FLAG".to_string(),
                }),
            })?
            .clone()
    } else {
        ArrayD::from_elem(IxDyn(&[0]).f(), false)
    };
    let mut newly_flagged_samples = 0;
    let initial_flags = activity
        .as_ref()
        .map_or(0, |_| flags.iter().filter(|flag| **flag).count());
    if let Some(activity) = activity.as_deref_mut() {
        activity.begin_row(data.len());
    }
    let any_calwt = plan.apply_mode.writes_corrected_data()
        && (parang_calwt
            || plan
                .calibration_tables
                .iter()
                .zip(&chain.tables)
                .any(|(table, loaded)| table.calwt && loaded.supports_calwt));
    let mut weight = match original_weight {
        Some(ArrayValue::Float32(weight)) => Some(weight.clone()),
        Some(other) => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: other.shape().to_vec(),
            });
        }
        None => None,
    };
    let mut weight_spectrum = None;
    let mut implicit_weight_spectrum =
        (any_calwt && !has_weight_spectrum).then(|| ArrayD::from_elem(data.raw_dim(), 1.0_f32));
    let data_spw = plan
        .measurement_set_spectral_windows
        .iter()
        .find(|spw| spw.spw_id == row.data_spw_id)
        .expect("planner guarantees selected MS spectral windows");
    let data_channels =
        channel_grid.unwrap_or_else(|| DataChannelGrid::all(&data_spw.channel_frequencies_hz));
    let mut geometry_cache = geometry_cache;
    let mut fast_gain_pairs: Vec<FastGainPair> = Vec::new();

    if any_calwt {
        let Some(weight_values) = weight.as_ref() else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: Vec::new(),
            });
        };
        if weight_values.ndim() != 1 || weight_values.shape()[0] != correlation_types.len() {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weight_values.shape().to_vec(),
            });
        }
        if has_weight_spectrum {
            weight_spectrum = Some(match original_weight_spectrum {
                Some(ArrayValue::Float32(values)) if values.shape() == data.shape() => {
                    values.clone()
                }
                None => expand_weight_to_spectrum(weight_values, data.shape()[1]),
                Some(other) => {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: "<measurement-set SIGMA_SPECTRUM>".into(),
                        shape: other.shape().to_vec(),
                    });
                }
            });
        }
    }
    if let Some(profile) = compute_profile.as_deref_mut() {
        profile.rows += 1;
        ApplyRowComputeProfile::add_elapsed(&mut profile.setup_ns, setup_started_at);
    }

    let mut output = ApplyBuffer {
        data_desc_id: row.data_desc_id,
        correlation_types,
        data_channels,
        corrected: &mut corrected,
        flags: &mut flags,
        propagate_flags,
        newly_flagged_samples: &mut newly_flagged_samples,
    };
    let mut pending_count = None;
    let mut parang_registration = 0;
    for &step in &chain.steps {
        if let Some((term, flags_in)) = pending_count.take() {
            // The fast gain kernel may defer this term's effect. Finish it
            // before measuring the flags that the next term receives.
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            activity.as_deref_mut().expect("tracked term").count_term(
                term,
                data.len(),
                flags_in,
                initial_flags + *output.newly_flagged_samples,
            );
        }
        let ApplyStep::Table(table_index) = step else {
            if let Some(activity) = activity.as_deref_mut() {
                pending_count = Some((
                    activity.parallactic_term(parang_registration),
                    initial_flags + *output.newly_flagged_samples,
                ));
                parang_registration += 1;
            }
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            apply_parallactic_step(
                row,
                chain.parang.as_ref().expect("planned parallactic angle"),
                &mut output,
                forward_model,
                compute_profile.as_deref_mut(),
            )?;
            continue;
        };
        let table_plan = &plan.calibration_tables[table_index];
        let loaded_table = &chain.tables[table_index];
        if !table_plan.applies_to(row)
            || ((forward_model || skip_unavailable_terms)
                && !availability::table_available(row, table_plan))
        {
            continue;
        }
        if let Some(activity) = activity.as_deref_mut() {
            pending_count = Some((
                activity.table_term(table_index),
                initial_flags + *output.newly_flagged_samples,
            ));
        }
        let calibrate_weights = !trial
            && plan.apply_mode.writes_corrected_data()
            && table_plan.calwt
            && loaded_table.supports_calwt
            // TfOpac requests sigma reset and writeback with unity scaling.
            && table_plan.summary.table_subtype != "TfOpac";
        if let Some(profile) = compute_profile.as_deref_mut() {
            profile.table_applications += 1;
        }
        let lookup_started_at = compute_profile.as_ref().map(|_| Instant::now());
        let cal_spw_id = table_plan
            .spw_mapping
            .iter()
            .find(|mapping| mapping.data_spw_id == row.data_spw_id)
            .map(|mapping| mapping.calibration_spw_id)
            .expect("planner guarantees spw mapping for selected rows");
        let cal_spw = table_plan
            .calibration_spectral_windows
            .iter()
            .find(|spw| spw.spw_id == cal_spw_id)
            .expect("planner guarantees mapped caltable spectral windows");

        let mut field_id = table_plan
            .resolved_nearest_gainfields
            .iter()
            .find(|mapping| mapping.measurement_set_field_id == row.field_id)
            .map(|mapping| mapping.calibration_field_id)
            .unwrap_or(-1);

        if let Some(library) = table_plan
            .spec
            .callibrary
            .as_ref()
            .filter(|library| !library.field_map.is_empty())
        {
            field_id = usize::try_from(row.field_id)
                .ok()
                .and_then(|id| library.field_map.get(id))
                .copied()
                .unwrap_or(-1);
        }

        let subtype = table_plan.summary.table_subtype.as_str();
        let is_spline = subtype == "GSPLINE";
        let field_id = if is_spline { row.field_id } else { field_id };
        let is_tsys = subtype == "B TSYS";
        let is_sky = crate::single_dish::is_sky_subtype(subtype);
        let is_switched_power = subtype == "G EVLASWPOW";
        let position_angle = subtype == "PosAng Jones";
        let channelized_polarization = polarization::Leakage::is_channelized(subtype)
            || matches!(subtype, "Xf Jones" | "Xfparang Jones" | "GlinXphf Jones")
            || position_angle;
        let bounded_frequency_cache = is_tsys
            || is_sky
            || channelized_polarization
            || is_spline
            || matches!(subtype, "Mf Mueller" | "TfOpac" | additive::SUBTYPE);
        let evaluation_frequency_hz = if is_spline {
            let frequencies = data_channels.frequencies_hz;
            frequencies.iter().sum::<f64>() / frequencies.len() as f64
        } else {
            cal_spw_reference_frequency_hz(cal_spw)
        };
        let lookup = |antenna, cache: Option<&mut RowGeometryCache>| {
            let mut key = calibration_lookup_key(
                table_index,
                field_id,
                cal_spw_id,
                antenna,
                row.time_seconds,
                table_plan.interp,
            );
            key.observation_id = row.observation_id;
            key.scan_number = row.scan_number;
            key.data_spw_id = row.data_spw_id;
            let calculate = || {
                fields::lookup(
                    loaded_table,
                    field_id,
                    cal_spw_id,
                    antenna,
                    row,
                    data_spw,
                    evaluation_frequency_hz,
                )
            };
            if let Some(cache) = cache {
                cache.calibration_lookup(
                    key,
                    bounded_frequency_cache.then(|| row.time_seconds.to_bits()),
                    calculate,
                )
            } else {
                calculate()
            }
        };
        if subtype == "X Mueller" {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            let solution = lookup(0, geometry_cache.as_deref_mut())?;
            polarization::apply_x_mueller(
                solution.as_ref().map(|solution| solution.grid.as_ref()),
                &loaded_table.path,
                forward_model,
                &mut output,
            )?;
            continue;
        }
        if loaded_table.baseline {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            let antenna1 = fields::mapped_antenna(loaded_table, row.antenna1);
            let antenna2 = fields::mapped_antenna(loaded_table, row.antenna2);
            let solution = match baseline::index(antenna1, antenna2) {
                Some(index) => lookup(index, geometry_cache.as_deref_mut())?,
                None => None,
            };
            if let Some(profile) = compute_profile.as_deref_mut() {
                ApplyRowComputeProfile::add_elapsed(
                    &mut profile.table_lookup_ns,
                    lookup_started_at,
                );
            }
            if subtype == "A Mueller" && cal_spw.num_chan > 1 {
                if !forward_model && (trial || !plan.apply_mode.writes_corrected_data()) {
                    return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                        path: loaded_table.path.display().to_string(),
                        reason: "trial apply is unavailable for additive polynomial tables".into(),
                    });
                }
                additive_polynomial::apply(
                    solution.as_ref().map(|solution| solution.grid.as_ref()),
                    cal_spw,
                    &loaded_table.path,
                    antenna1 > antenna2,
                    forward_model,
                    chain.legacy_corruption,
                    &mut output,
                )?;
                continue;
            }
            let grid = baseline::prepare(
                solution,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
            )?;
            if subtype == additive::SUBTYPE {
                let add = chain.additive.as_ref().map_or(forward_model, |state| {
                    state.direction(
                        (table_index, row.data_spw_id),
                        additive::Context::new(loaded_table, row, field_id, cal_spw_id),
                        loaded_table,
                        forward_model,
                        geometry_cache
                            .as_mut()
                            .map(|cache| &mut cache.additive_matrices),
                    )
                });
                additive::apply(
                    grid.as_deref(),
                    &loaded_table.path,
                    antenna1 > antenna2,
                    add,
                    forward_model && chain.legacy_corruption,
                    &mut output,
                )?;
            } else {
                baseline::apply(
                    grid.as_deref(),
                    &loaded_table.path,
                    antenna1 > antenna2,
                    forward_model,
                    &mut output,
                )?;
            }
            continue;
        }
        let ant1 = lookup(row.antenna1, geometry_cache.as_deref_mut())?;
        let ant2 = if is_sky {
            ant1.clone()
        } else {
            lookup(row.antenna2, geometry_cache.as_deref_mut())?
        };
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(&mut profile.table_lookup_ns, lookup_started_at);
        }

        let (Some(ant1), Some(ant2)) = (ant1, ant2) else {
            if propagate_flags {
                flush_fast_gain_pairs(
                    &mut fast_gain_pairs,
                    &mut output,
                    compute_profile.as_deref_mut(),
                )?;
                for corr_index in 0..data.shape()[0] {
                    for chan_index in 0..data.shape()[1] {
                        if !output.flags[[corr_index, chan_index]] {
                            output.flags[[corr_index, chan_index]] = true;
                            *output.newly_flagged_samples += 1;
                        }
                    }
                }
            }
            continue;
        };

        // CASA samples BPOLY on its CAL_DESC window and then interpolates
        // the resulting B grid onto the data window.
        let is_bandpass = is_tsys || matches!(subtype, "B Jones" | "BPOLY");
        let resample_frequency =
            is_bandpass || is_sky || channelized_polarization || subtype == "TfOpac";
        let first_receptor_only = (is_bandpass
            || is_switched_power
            || polarization::is_cross_hand_phase(subtype)
            || matches!(subtype, "GSPLINE" | "GlinXph Jones" | "GlinXphf Jones"))
            && correlation_types.len() == 1;
        let (ant1_transient, ant2_transient) = (
            bounded_frequency_cache && ant1.interpolated,
            bounded_frequency_cache && ant2.interpolated,
        );
        let (ant1, ant2) = (ant1.grid, ant2.grid);
        let bandpass_weights = if is_bandpass && !is_tsys && calibrate_weights {
            let mut scratch = bandpass::PowerCache::default();
            geometry_cache
                .as_deref_mut()
                .map_or(&mut scratch, |cache| &mut cache.bandpass_power)
                .factors(
                    table_index,
                    (row.antenna1, &ant1),
                    (row.antenna2, &ant2),
                    correlation_types,
                )
        } else {
            None
        };
        let (ant1, ant2) = if resample_frequency {
            let ant1 = bandpass::prepare(
                ant1,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
                ant1_transient,
            )?;
            let ant2 = bandpass::prepare(
                ant2,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
                ant2_transient,
            )?;
            (ant1, ant2)
        } else {
            (ant1, ant2)
        };
        let bandpass_weights = if is_tsys && calibrate_weights && !has_weight_spectrum {
            bandpass::temperature_weight_factors(&ant1, &ant2, correlation_types, data_channels)
        } else {
            bandpass_weights
        };
        let sampling_context = CalibrationSamplingContext {
            data_channels,
            cal_frequencies_hz: if resample_frequency {
                &data_spw.channel_frequencies_hz
            } else {
                &cal_spw.channel_frequencies_hz
            },
            cal_ref_frequency_hz: cal_spw_reference_frequency_hz(cal_spw),
            // Prepared solutions already occupy the full MS grid. Temporal
            // interpolation happened at lookup; sample original bins directly.
            interp: if resample_frequency {
                ApplyInterpolationMode::Nearest
            } else {
                table_plan.interp
            },
            path: &loaded_table.path,
            engine: loaded_table.engine.as_deref(),
            antpos_tropo: loaded_table.antpos_tropo.as_ref(),
        };

        if is_sky {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            sky::apply_grid(&ant1, &loaded_table.path, &mut output, forward_model)?;
            if calibrate_weights && let Some(exposures) = &loaded_table.sky_exposures {
                let factor = exposures.weight_factor(row, cal_spw_id, loaded_table.interp);
                if let Some(spectrum) = &mut weight_spectrum {
                    *spectrum *= factor;
                }
                if let Some(spectrum) = &mut implicit_weight_spectrum {
                    *spectrum *= factor;
                }
            }
            continue;
        }
        if let Some(leakage) = polarization::Leakage::from_subtype(subtype) {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            leakage.apply(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }
        if position_angle {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            polarization::apply_position_angle(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }
        if let Some(ionosphere) = &loaded_table.ionosphere {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            ionosphere.apply(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }

        let materialize_started_at = compute_profile.as_ref().map(|_| Instant::now());
        if !forward_model
            && !calibrate_weights
            && !first_receptor_only
            && let Some(pair) = build_fast_gain_pair(
                table_index,
                &ant1,
                &ant2,
                row.field_id,
                row.antenna1,
                row.antenna2,
                row.time_seconds,
                loaded_table,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
        {
            if let Some(profile) = compute_profile.as_deref_mut() {
                ApplyRowComputeProfile::add_elapsed(
                    &mut profile.row_dependent_grid_ns,
                    materialize_started_at,
                );
                profile.row_dependent_fast_pairs += 1;
            }
            fast_gain_pairs.push(pair);
            continue;
        }
        let ant1 = materialize_row_dependent_grid(
            ant1,
            row.field_id,
            row.antenna1,
            row.time_seconds,
            data_spw,
            loaded_table,
            geometry_cache.as_deref_mut(),
        )?;
        let ant2 = materialize_row_dependent_grid(
            ant2,
            row.field_id,
            row.antenna2,
            row.time_seconds,
            data_spw,
            loaded_table,
            geometry_cache.as_deref_mut(),
        )?;
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.row_dependent_grid_ns,
                materialize_started_at,
            );
            profile.row_dependent_materialized_pairs += 1;
        }

        if !forward_model
            && !first_receptor_only
            && let (CalibrationGrid::Complex(first), CalibrationGrid::Complex(second)) =
                (ant1.as_ref(), ant2.as_ref())
        {
            let scalar_weights =
                !is_bandpass && first.channel_count == 1 && second.channel_count == 1;
            if !calibrate_weights || scalar_weights {
                if calibrate_weights {
                    weights::apply_scalar_pair(
                        first,
                        second,
                        correlation_types,
                        row.data_desc_id,
                        table_plan.summary.table_subtype == "EPowerCurve",
                        weight_spectrum.as_mut(),
                        implicit_weight_spectrum.as_mut(),
                    )?;
                }
                fast_gain_pairs.push(FastGainPair::Complex(ant1, ant2));
                continue;
            }
        }

        flush_fast_gain_pairs(
            &mut fast_gain_pairs,
            &mut output,
            compute_profile.as_deref_mut(),
        )?;

        let generic_apply_started_at = compute_profile.as_ref().map(|_| Instant::now());
        for corr_index in 0..data.shape()[0] {
            let receptors = if first_receptor_only {
                bandpass::receptors(correlation_types, corr_index)
            } else {
                correlation_receptors(correlation_types[corr_index])
            }
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: correlation_types
                    .iter()
                    .map(|code| stokes_name(*code).to_string())
                    .collect(),
            })?;
            let switched_power_weight = match (ant1.as_ref(), ant2.as_ref()) {
                (CalibrationGrid::SwitchedPower(left), CalibrationGrid::SwitchedPower(right)) => {
                    Some(left.weight_scale(receptors.0) * right.weight_scale(receptors.1))
                }
                _ => None,
            };

            for chan_index in 0..data.shape()[1] {
                let gain1 = ant1.sample(
                    receptors.0,
                    chan_index,
                    row.field_id,
                    row.antenna1,
                    row.time_seconds,
                    &sampling_context,
                )?;
                let gain2 = ant2.sample(
                    receptors.1,
                    chan_index,
                    row.field_id,
                    row.antenna2,
                    row.time_seconds,
                    &sampling_context,
                )?;

                let gain = if forward_model {
                    gain_validity::forward_gain
                } else {
                    correcting_gain
                };
                let (value1, flag1) = gain(gain1.value, gain1.flagged);
                let (value2, flag2) = gain(gain2.value, gain2.flagged);
                if (flag1 || flag2) && propagate_flags && !output.flags[[corr_index, chan_index]] {
                    output.flags[[corr_index, chan_index]] = true;
                    *output.newly_flagged_samples += 1;
                }

                let factor = value1 * value2.conj();
                if forward_model {
                    output.corrected[[corr_index, chan_index]] *= factor;
                } else {
                    output.corrected[[corr_index, chan_index]] /= factor;
                }
                if calibrate_weights {
                    let factor = if is_tsys && has_weight_spectrum && (flag1 || flag2) {
                        0.0
                    } else {
                        switched_power_weight
                            .or_else(|| {
                                bandpass_weights.as_ref().map(|factors| factors[corr_index])
                            })
                            .unwrap_or_else(|| value1.norm_sqr() * value2.norm_sqr())
                    };
                    if let Some(weight_spectrum) = weight_spectrum.as_mut() {
                        weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                    if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_mut() {
                        implicit_weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                }
            }
        }
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.generic_sample_apply_ns,
                generic_apply_started_at,
            );
        }
    }

    flush_fast_gain_pairs(
        &mut fast_gain_pairs,
        &mut output,
        compute_profile.as_deref_mut(),
    )?;
    if let Some((term, flags_in)) = pending_count {
        activity.expect("tracked term").count_term(
            term,
            data.len(),
            flags_in,
            initial_flags + *output.newly_flagged_samples,
        );
    }

    if any_calwt {
        let weight_finalize_started_at = compute_profile.as_ref().map(|_| Instant::now());
        let weight_values = weight
            .as_mut()
            .expect("validated WEIGHT availability when calwt is enabled");
        if let Some(weight_spectrum_values) = weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..weight_spectrum_values.shape()[1])
                    .map(|chan_index| weight_spectrum_values[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] = weights::lower_median(samples);
            }
        } else if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..implicit_weight_spectrum.shape()[1])
                    .map(|chan_index| implicit_weight_spectrum[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] *= weights::lower_median(samples);
            }
        }
        if let Some(profile) = compute_profile {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.weight_finalize_ns,
                weight_finalize_started_at,
            );
        }
    }

    if trial && plan.apply_mode.writes_corrected_data() {
        corrected.assign(data);
    }
    let flags_changed = newly_flagged_samples > 0;
    let row_became_fully_flagged = flags_changed && flags.iter().all(|flag| *flag);
    Ok(ExecutionRowResult {
        corrected_data: ArrayValue::Complex32(corrected),
        updated_flags: (plan.apply_mode.writes_flags() && flags_changed)
            .then_some(ArrayValue::Bool(flags)),
        updated_weight: any_calwt.then(|| ArrayValue::Float32(weight.expect("calwt weight"))),
        updated_weight_spectrum: weight_spectrum.map(ArrayValue::Float32),
        newly_flagged_samples,
        row_became_fully_flagged,
    })
}

fn flush_fast_gain_pairs(
    pairs: &mut Vec<FastGainPair>,
    output: &mut ApplyBuffer<'_>,
    profile: Option<&mut ApplyRowComputeProfile>,
) -> Result<(), ApplyExecutionError> {
    if !pairs.is_empty() {
        let started = profile.as_ref().map(|_| Instant::now());
        apply_complex_gain_pairs_fast(pairs, output)?;
        if let Some(profile) = profile {
            ApplyRowComputeProfile::add_elapsed(&mut profile.fast_gain_apply_ns, started);
        }
        pairs.clear();
    }
    Ok(())
}

fn apply_parallactic_step(
    row: &ApplyRowPlan,
    state: &ParallacticAngleState,
    output: &mut ApplyBuffer<'_>,
    forward: bool,
    profile: Option<&mut ApplyRowComputeProfile>,
) -> Result<(), ApplyExecutionError> {
    let started = profile.as_ref().map(|_| Instant::now());
    let angle = |antenna, feed| {
        state.feed_parallactic_angle(
            row.time_seconds,
            row.field_id,
            antenna,
            feed,
            row.data_spw_id,
        )
    };
    let apply = if forward {
        parang::corrupt
    } else {
        parang::apply
    };
    apply(
        row.data_desc_id,
        output.correlation_types,
        angle(row.antenna1, row.feed1)?,
        angle(row.antenna2, row.feed2)?,
        output.corrected,
    )?;
    if output.propagate_flags {
        *output.newly_flagged_samples +=
            parang::propagate_flags(output.correlation_types, output.flags);
    }
    if let Some(profile) = profile {
        ApplyRowComputeProfile::add_elapsed(&mut profile.parallactic_angle_ns, started);
    }
    Ok(())
}

const FAST_GAIN_RECEPTOR_LIMIT: usize = 4;

struct ApplyBuffer<'a> {
    data_desc_id: i32,
    correlation_types: &'a [i32],
    data_channels: DataChannelGrid<'a>,
    corrected: &'a mut ArrayD<Complex32>,
    flags: &'a mut ArrayD<bool>,
    propagate_flags: bool,
    newly_flagged_samples: &'a mut usize,
}

enum FastGainPair {
    Complex(Arc<CalibrationGrid>, Arc<CalibrationGrid>),
    Antpos {
        delay_delta_ns: f64,
        flagged: bool,
    },
    GainCurve {
        ant1: FastReceptorGains,
        ant2: FastReceptorGains,
    },
    Opacity {
        denom: Complex32,
        flagged: bool,
    },
}

#[derive(Clone, Copy)]
struct FastReceptorGains {
    receptor_count: usize,
    values: [f32; FAST_GAIN_RECEPTOR_LIMIT],
    flags: [bool; FAST_GAIN_RECEPTOR_LIMIT],
}

#[derive(Clone, Copy)]
struct FastGainSample {
    denom: Complex32,
    flagged: bool,
}

impl FastReceptorGains {
    fn sample(self, receptor: usize) -> (f32, bool) {
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let (value, flag) = correcting_gain(
            Complex32::new(self.values[receptor], 0.),
            self.flags[receptor],
        );
        (value.re, flag)
    }
}

impl FastGainPair {
    fn requires_data_frequencies(&self) -> bool {
        matches!(self, Self::Antpos { .. })
    }

    fn scalar_sample(&self, receptors: (usize, usize)) -> Option<FastGainSample> {
        match self {
            Self::Complex(ant1, ant2) => {
                let (CalibrationGrid::Complex(ant1), CalibrationGrid::Complex(ant2)) =
                    (ant1.as_ref(), ant2.as_ref())
                else {
                    return None;
                };
                if ant1.channel_count > 1 || ant2.channel_count > 1 {
                    return None;
                }
                let receptor1 = receptors.0.min(ant1.receptor_count.saturating_sub(1));
                let receptor2 = receptors.1.min(ant2.receptor_count.saturating_sub(1));
                let (gain1, flag1) =
                    correcting_gain(ant1.value_at(receptor1, 0), ant1.flag_at(receptor1, 0));
                let (gain2, flag2) =
                    correcting_gain(ant2.value_at(receptor2, 0), ant2.flag_at(receptor2, 0));
                Some(FastGainSample {
                    denom: gain1 * gain2.conj(),
                    flagged: flag1 || flag2,
                })
            }
            Self::GainCurve { ant1, ant2 } => {
                let (gain1, flag1) = ant1.sample(receptors.0);
                let (gain2, flag2) = ant2.sample(receptors.1);
                Some(FastGainSample {
                    denom: Complex32::new(gain1 * gain2, 0.0),
                    flagged: flag1 || flag2,
                })
            }
            Self::Opacity { denom, flagged } => Some(FastGainSample {
                denom: *denom,
                flagged: *flagged,
            }),
            Self::Antpos { .. } => None,
        }
    }

    fn channel_sample(
        &self,
        receptors: (usize, usize),
        chan_index: usize,
        data_channels: DataChannelGrid<'_>,
    ) -> Option<FastGainSample> {
        match self {
            Self::Complex(ant1, ant2) => {
                let (CalibrationGrid::Complex(ant1), CalibrationGrid::Complex(ant2)) =
                    (ant1.as_ref(), ant2.as_ref())
                else {
                    return None;
                };
                if ant1.channel_count <= 1 && ant2.channel_count <= 1 {
                    return None;
                }
                let receptor1 = receptors.0.min(ant1.receptor_count.saturating_sub(1));
                let receptor2 = receptors.1.min(ant2.receptor_count.saturating_sub(1));
                let ant1_chan = if ant1.channel_count <= 1 {
                    0
                } else {
                    data_channels
                        .original_index(chan_index)
                        .min(ant1.channel_count.saturating_sub(1))
                };
                let ant2_chan = if ant2.channel_count <= 1 {
                    0
                } else {
                    data_channels
                        .original_index(chan_index)
                        .min(ant2.channel_count.saturating_sub(1))
                };
                let (gain1, flag1) = correcting_gain(
                    ant1.value_at(receptor1, ant1_chan),
                    ant1.flag_at(receptor1, ant1_chan),
                );
                let (gain2, flag2) = correcting_gain(
                    ant2.value_at(receptor2, ant2_chan),
                    ant2.flag_at(receptor2, ant2_chan),
                );
                Some(FastGainSample {
                    denom: gain1 * gain2.conj(),
                    flagged: flag1 || flag2,
                })
            }
            Self::Antpos {
                delay_delta_ns,
                flagged,
            } => {
                let frequency_hz = data_channels.frequencies_hz[chan_index];
                let phase_rad =
                    2.0 * std::f64::consts::PI * *delay_delta_ns * (frequency_hz / 1.0e9);
                Some(FastGainSample {
                    denom: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
                    flagged: *flagged,
                })
            }
            Self::GainCurve { .. } | Self::Opacity { .. } => None,
        }
    }
}

fn apply_complex_gain_pairs_fast(
    pairs: &[FastGainPair],
    ctx: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    if pairs.is_empty() {
        return Ok(());
    }
    let shape = ctx.corrected.shape();
    let corr_count = shape[0];
    let channel_count = shape[1];
    ensure_fast_pair_frequency_coverage(pairs, ctx.data_channels.frequencies_hz, channel_count)?;
    let corrected_is_fortran = ctx.corrected.ndim() == 2 && ctx.corrected.strides()[0] == 1;
    let flags_are_fortran =
        !ctx.propagate_flags || (ctx.flags.ndim() == 2 && ctx.flags.strides()[0] == 1);
    if corrected_is_fortran
        && flags_are_fortran
        && let Some(corrected) = ctx.corrected.as_slice_memory_order_mut()
    {
        let flags = (ctx.propagate_flags)
            .then(|| ctx.flags.as_slice_memory_order_mut())
            .flatten();
        return apply_complex_gain_pairs_fast_fortran(
            pairs,
            ctx.data_desc_id,
            ctx.correlation_types,
            ctx.data_channels,
            corr_count,
            channel_count,
            corrected,
            flags,
            ctx.newly_flagged_samples,
        );
    }

    let receptors_by_corr =
        fast_pair_receptors(ctx.data_desc_id, ctx.correlation_types, corr_count)?;

    for (corr_index, &receptors) in receptors_by_corr.iter().enumerate().take(corr_count) {
        let mut scalar_denom = Complex32::new(1.0, 0.0);
        for pair in pairs {
            let Some(sample) = pair.scalar_sample(receptors) else {
                continue;
            };
            if fast_gain_sample_invalid(sample) && ctx.propagate_flags {
                flag_all_channels_generic(
                    ctx.flags,
                    corr_index,
                    channel_count,
                    ctx.newly_flagged_samples,
                );
            }
            if sample.denom != Complex32::new(0., 0.) {
                scalar_denom *= sample.denom;
            }
        }

        for chan_index in 0..channel_count {
            let mut denom = scalar_denom;
            for pair in pairs {
                let Some(sample) = pair.channel_sample(receptors, chan_index, ctx.data_channels)
                else {
                    continue;
                };
                if fast_gain_sample_invalid(sample)
                    && ctx.propagate_flags
                    && !ctx.flags[[corr_index, chan_index]]
                {
                    ctx.flags[[corr_index, chan_index]] = true;
                    *ctx.newly_flagged_samples += 1;
                }
                if sample.denom != Complex32::new(0., 0.) {
                    denom *= sample.denom;
                }
            }
            ctx.corrected[[corr_index, chan_index]] /= denom;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_complex_gain_pairs_fast_fortran(
    pairs: &[FastGainPair],
    data_desc_id: i32,
    correlation_types: &[i32],
    data_channels: DataChannelGrid<'_>,
    corr_count: usize,
    channel_count: usize,
    corrected: &mut [Complex32],
    mut flags: Option<&mut [bool]>,
    newly_flagged_samples: &mut usize,
) -> Result<(), ApplyExecutionError> {
    let receptors_by_corr = fast_pair_receptors(data_desc_id, correlation_types, corr_count)?;

    for (corr_index, &receptors) in receptors_by_corr.iter().enumerate().take(corr_count) {
        let mut scalar_denom = Complex32::new(1.0, 0.0);
        for pair in pairs {
            let Some(sample) = pair.scalar_sample(receptors) else {
                continue;
            };
            if fast_gain_sample_invalid(sample)
                && let Some(flags) = flags.as_deref_mut()
            {
                flag_all_channels_fortran(
                    flags,
                    corr_index,
                    corr_count,
                    channel_count,
                    newly_flagged_samples,
                );
            }
            if sample.denom != Complex32::new(0., 0.) {
                scalar_denom *= sample.denom;
            }
        }

        for chan_index in 0..channel_count {
            let offset = corr_index + chan_index * corr_count;
            let mut denom = scalar_denom;
            for pair in pairs {
                let Some(sample) = pair.channel_sample(receptors, chan_index, data_channels) else {
                    continue;
                };
                if fast_gain_sample_invalid(sample)
                    && let Some(flags) = flags.as_deref_mut()
                    && !flags[offset]
                {
                    flags[offset] = true;
                    *newly_flagged_samples += 1;
                }
                if sample.denom != Complex32::new(0., 0.) {
                    denom *= sample.denom;
                }
            }
            corrected[offset] /= denom;
        }
    }

    Ok(())
}

fn fast_pair_receptors(
    data_desc_id: i32,
    correlation_types: &[i32],
    corr_count: usize,
) -> Result<Vec<(usize, usize)>, ApplyExecutionError> {
    (0..corr_count)
        .map(|corr_index| {
            correlation_receptors(correlation_types[corr_index]).ok_or_else(|| {
                ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn ensure_fast_pair_frequency_coverage(
    pairs: &[FastGainPair],
    data_frequencies_hz: &[f64],
    channel_count: usize,
) -> Result<(), ApplyExecutionError> {
    if pairs.iter().any(FastGainPair::requires_data_frequencies)
        && data_frequencies_hz.len() < channel_count
    {
        return Err(ApplyExecutionError::UnsupportedInterpolation {
            path: "<measurement-set SPECTRAL_WINDOW>".to_string(),
            interp: ApplyInterpolationMode::Nearest,
            reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
        });
    }
    Ok(())
}

fn fast_gain_sample_invalid(sample: FastGainSample) -> bool {
    sample.flagged || sample.denom == Complex32::new(0.0, 0.0)
}

fn flag_all_channels_generic(
    flags: &mut ArrayD<bool>,
    corr_index: usize,
    channel_count: usize,
    newly_flagged_samples: &mut usize,
) {
    for flag_chan_index in 0..channel_count {
        if !flags[[corr_index, flag_chan_index]] {
            flags[[corr_index, flag_chan_index]] = true;
            *newly_flagged_samples += 1;
        }
    }
}

fn flag_all_channels_fortran(
    flags: &mut [bool],
    corr_index: usize,
    corr_count: usize,
    channel_count: usize,
    newly_flagged_samples: &mut usize,
) {
    for flag_chan_index in 0..channel_count {
        let flag_offset = corr_index + flag_chan_index * corr_count;
        if !flags[flag_offset] {
            flags[flag_offset] = true;
            *newly_flagged_samples += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_fast_gain_pair(
    table_index: usize,
    ant1: &Arc<CalibrationGrid>,
    ant2: &Arc<CalibrationGrid>,
    field_id: i32,
    antenna1_id: i32,
    antenna2_id: i32,
    time_seconds: f64,
    table: &LoadedCalibrationTable,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Option<FastGainPair>, ApplyExecutionError> {
    let engine = table.engine.as_deref();
    let path = &table.path;
    let mut geometry_cache = geometry_cache;
    match (ant1.as_ref(), ant2.as_ref()) {
        (CalibrationGrid::Complex(_), CalibrationGrid::Complex(_)) => Ok(Some(
            FastGainPair::Complex(Arc::clone(ant1), Arc::clone(ant2)),
        )),
        (CalibrationGrid::Antpos(ant1), CalibrationGrid::Antpos(ant2)) => {
            let delay1_ns = fast_antpos_delay_ns(
                table_index,
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                table.antpos_tropo.as_ref(),
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let delay2_ns = fast_antpos_delay_ns(
                table_index,
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                table.antpos_tropo.as_ref(),
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            Ok(Some(FastGainPair::Antpos {
                delay_delta_ns: if ant1.flagged { 0. } else { delay1_ns }
                    - if ant2.flagged { 0. } else { delay2_ns },
                flagged: ant1.flagged || ant2.flagged,
            }))
        }
        (CalibrationGrid::GainCurve(ant1), CalibrationGrid::GainCurve(ant2)) => {
            let Some(ant1) = fast_gain_curve_values(
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
            else {
                return Ok(None);
            };
            let Some(ant2) = fast_gain_curve_values(
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
            else {
                return Ok(None);
            };
            Ok(Some(FastGainPair::GainCurve { ant1, ant2 }))
        }
        (CalibrationGrid::Opacity(ant1), CalibrationGrid::Opacity(ant2)) => {
            let gain1 = fast_opacity_gain(
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let gain2 = fast_opacity_gain(
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let (gain1, flag1) = correcting_gain(Complex32::new(gain1, 0.), ant1.flagged);
            let (gain2, flag2) = correcting_gain(Complex32::new(gain2, 0.), ant2.flagged);
            Ok(Some(FastGainPair::Opacity {
                denom: gain1 * gain2,
                flagged: flag1 || flag2,
            }))
        }
        _ => Ok(None),
    }
}

fn reborrow_geometry_cache<'a>(
    geometry_cache: &'a mut Option<&mut RowGeometryCache>,
) -> Option<&'a mut RowGeometryCache> {
    geometry_cache.as_mut().map(|cache| &mut **cache)
}

#[allow(clippy::too_many_arguments)]
fn fast_antpos_delay_ns(
    table_index: usize,
    grid: &AntposGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    tropo: Option<&TropoDelay>,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let mut geometry_cache = geometry_cache;
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
        });
    };
    let offset_m = [
        f64::from(grid.offsets_m[0]),
        f64::from(grid.offsets_m[1]),
        f64::from(grid.offsets_m[2]),
    ];
    let offset_bits = [
        offset_m[0].to_bits(),
        offset_m[1].to_bits(),
        offset_m[2].to_bits(),
    ];
    let antenna_index = usize::try_from(antenna_id).ok();
    if let Some(cache) = reborrow_geometry_cache(&mut geometry_cache) {
        cache.ensure_fast_term_context(time_seconds, field_id);
        if let Some(antenna_index) = antenna_index
            && let Some(delay_ns) =
                cache.cached_fast_antpos_delay(table_index, antenna_index, offset_bits)
        {
            return Ok(delay_ns);
        }
    }
    let delay_ns = antpos_delay_ns(
        engine,
        field_id,
        antenna_id,
        time_seconds,
        offset_m,
        path,
        tropo,
        reborrow_geometry_cache(&mut geometry_cache),
    )?;
    if let (Some(cache), Some(antenna_index)) =
        (reborrow_geometry_cache(&mut geometry_cache), antenna_index)
    {
        cache.remember_fast_antpos_delay(table_index, antenna_index, offset_bits, delay_ns);
    }
    Ok(delay_ns)
}

#[allow(clippy::too_many_arguments)]
fn antpos_delay_ns(
    engine: &MsCalEngine,
    field: i32,
    antenna: i32,
    time_seconds: f64,
    offset_m: [f64; 3],
    path: &Path,
    tropo: Option<&TropoDelay>,
    cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let field_id = usize::try_from(field).unwrap_or(usize::MAX);
    let antenna_id = usize::try_from(antenna).unwrap_or(usize::MAX);
    let failed = |source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    };
    let uvw = match cache {
        Some(cache) => {
            cache.project_itrf_offset_to_uvw(engine, time_seconds, field, antenna, offset_m)
        }
        None => engine.project_itrf_offset_to_uvw(time_seconds, field_id, antenna_id, offset_m),
    }
    .map_err(failed)?;
    let tropo_m = tropo
        .map(|tropo| tropo.metres(engine, time_seconds, field_id, antenna_id))
        .transpose()
        .map_err(failed)?
        .unwrap_or(0.0);
    Ok((uvw[2] + tropo_m) / SPEED_OF_LIGHT_M_PER_S * 1.0e9)
}

fn fast_gain_curve_values(
    grid: &GainCurveGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Option<FastReceptorGains>, ApplyExecutionError> {
    if grid.receptor_count > FAST_GAIN_RECEPTOR_LIMIT {
        return Ok(None);
    }
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
        });
    };
    // The sampled coefficients vary by calibration window and slice.
    // Only cache the geometry, which is independent of those coefficients.
    let elevation = if let Some(cache) = geometry_cache {
        cache.elevation(engine, time_seconds, field_id, antenna_id)
    } else {
        engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|(_az, elevation)| elevation)
    }
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
    let mut values = [0.0_f32; FAST_GAIN_RECEPTOR_LIMIT];
    let mut flags = [false; FAST_GAIN_RECEPTOR_LIMIT];
    for receptor in 0..grid.receptor_count {
        (values[receptor], flags[receptor]) = grid.evaluate(receptor, za_degrees);
    }
    let gains = FastReceptorGains {
        receptor_count: grid.receptor_count,
        values,
        flags,
    };
    Ok(Some(gains))
}

fn fast_opacity_gain(
    grid: &OpacityGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f32, ApplyExecutionError> {
    let elevation = opacity_elevation(
        engine,
        field_id,
        antenna_id,
        time_seconds,
        path,
        geometry_cache,
    )?;
    Ok(opacity_gain(grid.tau, elevation))
}

fn opacity_elevation(
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "TOpac apply requires MeasurementSet geometry".to_string(),
        });
    };
    let elevation = if let Some(cache) = geometry_cache {
        cache.elevation(engine, time_seconds, field_id, antenna_id)
    } else {
        engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|(_az, elevation)| elevation)
    }
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    Ok(elevation)
}

fn opacity_gain(tau: f32, elevation: f64) -> f32 {
    let zenith_angle = std::f64::consts::FRAC_PI_2 - elevation;
    if zenith_angle < std::f64::consts::FRAC_PI_2 {
        (-f64::from(tau) / zenith_angle.cos()).exp().sqrt() as f32
    } else {
        1.0
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_row_prefetched(
    row: &ApplyRowPlan,
    correlation_types: &[i32],
    inputs: &PrefetchedExecutionRowInputs,
    has_weight_spectrum: bool,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry_cache: Option<&mut RowGeometryCache>,
    compute_profile: Option<&mut ApplyRowComputeProfile>,
    activity: Option<&mut ToolApplyActivity>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    apply_row(
        row,
        ExecutionRowInputs {
            correlation_types,
            data: &inputs.data,
            original_flags: inputs.original_flags.as_ref(),
            original_weight: inputs.original_weight.as_ref(),
            original_weight_spectrum: inputs.original_weight_spectrum.as_ref(),
            has_weight_spectrum,
            preserved_corrected_data: inputs.preserved_corrected_data.as_ref(),
            preserved_weight_spectrum: inputs.preserved_weight_spectrum.as_ref(),
            channel_grid: None,
            forward_model: false,
            skip_unavailable_terms: false,
            trial: inputs.trial,
            parang_calwt: inputs.parang_calwt,
        },
        plan,
        chain,
        geometry_cache,
        compute_profile,
        activity,
    )
}

#[allow(clippy::too_many_arguments)]
fn materialize_row_dependent_grid(
    grid: Arc<CalibrationGrid>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    data_spw: &crate::plan::SpectralWindowPlan,
    table: &LoadedCalibrationTable,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let engine = table.engine.as_deref();
    let path = &table.path;
    let mut geometry_cache = geometry_cache;
    let transient = matches!(grid.as_ref(), CalibrationGrid::SpectralOpacity(_));
    let cache_key =
        materialized_grid_key(&grid, field_id, antenna_id, time_seconds, data_spw.spw_id);
    if let (Some(cache), Some(key)) = (&geometry_cache, cache_key)
        && let Some(grid) = cache.materialized_grid(key, transient)
    {
        return Ok(grid);
    }
    let result = match grid.as_ref() {
        CalibrationGrid::Antpos(grid) => {
            let Some(engine) = engine else {
                return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                    path: path.display().to_string(),
                    reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
                });
            };
            let delay_ns = antpos_delay_ns(
                engine,
                field_id,
                antenna_id,
                time_seconds,
                grid.offsets_m.map(f64::from),
                path,
                table.antpos_tropo.as_ref(),
                geometry_cache.as_deref_mut(),
            )?;
            let values = data_spw
                .channel_frequencies_hz
                .iter()
                .map(|frequency_hz| {
                    let phase_rad = 2.0 * std::f64::consts::PI * delay_ns * (*frequency_hz / 1.0e9);
                    Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32)
                })
                .collect::<Vec<_>>();
            let channel_count = values.len();
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count,
                values: ArrayD::from_shape_vec(IxDyn(&[1, channel_count]).f(), values)
                    .expect("antpos materialized grid shape is valid"),
                flags: ArrayD::from_elem(IxDyn(&[1, channel_count]).f(), grid.flagged),
            })))
        }
        CalibrationGrid::GainCurve(grid) => {
            let Some(engine) = engine else {
                return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                    path: path.display().to_string(),
                    reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
                });
            };
            let elevation = if let Some(cache) = geometry_cache.as_deref_mut() {
                cache.elevation(engine, time_seconds, field_id, antenna_id)
            } else {
                engine
                    .azel(
                        time_seconds,
                        usize::try_from(field_id).unwrap_or(usize::MAX),
                        usize::try_from(antenna_id).unwrap_or(usize::MAX),
                    )
                    .map(|(_az, elevation)| elevation)
            }
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: path.display().to_string(),
                source,
            })?;
            let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
            let mut values = Vec::with_capacity(grid.receptor_count);
            let mut flags = Vec::with_capacity(grid.receptor_count);
            for receptor in 0..grid.receptor_count {
                let (gain, flagged) = grid.evaluate(receptor, za_degrees);
                values.push(Complex32::new(gain, 0.0));
                flags.push(flagged);
            }
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: grid.receptor_count,
                channel_count: 1,
                values: ArrayD::from_shape_vec(IxDyn(&[grid.receptor_count]).f(), values)
                    .expect("gaincurve materialized grid shape is valid"),
                flags: ArrayD::from_shape_vec(IxDyn(&[grid.receptor_count]).f(), flags)
                    .expect("gaincurve materialized flag shape is valid"),
            })))
        }
        CalibrationGrid::Opacity(grid) => {
            let gain = fast_opacity_gain(
                grid,
                engine,
                field_id,
                antenna_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count: 1,
                values: ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![Complex32::new(gain, 0.0)])
                    .expect("opacity materialized grid shape is valid"),
                flags: ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![grid.flagged])
                    .expect("opacity materialized flag shape is valid"),
            })))
        }
        CalibrationGrid::SpectralOpacity(tau) => {
            let elevation = opacity_elevation(
                engine,
                field_id,
                antenna_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let mut gain = tau.clone();
            gain.values
                .mapv_inplace(|value| Complex32::new(opacity_gain(value.re, elevation), 0.));
            Ok(Arc::new(CalibrationGrid::Complex(gain)))
        }
        _ => Ok(Arc::clone(&grid)),
    };
    if let (Some(cache), Some(key), Ok(materialized)) =
        (&mut geometry_cache, cache_key, result.as_ref())
    {
        cache.insert_materialized_grid(key, Arc::clone(materialized), transient);
    }
    result
}

impl ParallacticAngleState {
    pub(crate) fn feed_parallactic_angle(
        &self,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
        feed_id: i32,
        data_spw_id: i32,
    ) -> Result<f64, ApplyExecutionError> {
        let receptor0_angle = self
            .lookup_receptor0_angle(antenna_id, feed_id, data_spw_id, time_seconds)
            .unwrap_or(0.0);
        self.engine
            .feed_parallactic_angle(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|feed_angle| feed_angle + receptor0_angle)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: "<measurement-set derived parallactic angle>".to_string(),
                source,
            })
    }

    fn lookup_receptor0_angle(
        &self,
        antenna_id: i32,
        feed_id: i32,
        data_spw_id: i32,
        time_seconds: f64,
    ) -> Option<f64> {
        let rows = self.feed_rows.get(&(antenna_id, feed_id))?;
        let mut exact_spw = rows
            .iter()
            .copied()
            .filter(|row| row.spectral_window_id == data_spw_id)
            .collect::<Vec<_>>();
        if exact_spw.is_empty() {
            exact_spw = rows
                .iter()
                .copied()
                .filter(|row| row.spectral_window_id < 0)
                .collect::<Vec<_>>();
        }
        exact_spw
            .into_iter()
            .min_by(|left, right| {
                feed_row_distance(left, time_seconds)
                    .total_cmp(&feed_row_distance(right, time_seconds))
            })
            .map(|row| row.receptor0_angle_rad)
    }
}

fn interval_distance(epoch: f64, interval: f64, time_seconds: f64) -> f64 {
    if interval > 0.0 {
        let half = interval / 2.0;
        let start = epoch - half;
        let end = epoch + half;
        if (start..=end).contains(&time_seconds) {
            return 0.0;
        }
    }
    (epoch - time_seconds).abs()
}

fn feed_row_distance(row: &FeedAngleRow, time_seconds: f64) -> f64 {
    interval_distance(row.time_seconds, row.interval_seconds, time_seconds)
}

fn receptor0_angle(value: &ArrayValue) -> f64 {
    match value {
        ArrayValue::Float64(values) if !values.is_empty() => values[[0]],
        ArrayValue::Float32(values) if !values.is_empty() => f64::from(values[[0]]),
        _ => 0.0,
    }
}

/// Find one feed angle without retaining a table-wide index. Exact spectral
/// windows precede wildcard rows, then the nearest interval wins.
pub(crate) fn single_receptor_angle(
    ms: &MeasurementSet,
    antenna: i32,
    feed_id: i32,
    spw: i32,
    time: f64,
) -> MsResult<f64> {
    let Ok(feed) = ms.feed() else {
        return Ok(0.0);
    };
    let mut best = None;
    for row in 0..feed.row_count() {
        if feed.i32(row, "ANTENNA_ID")? != antenna || feed.i32(row, "FEED_ID")? != feed_id {
            continue;
        }
        let window = feed.i32(row, "SPECTRAL_WINDOW_ID")?;
        if window != spw && window >= 0 {
            continue;
        }
        let wildcard = window != spw;
        let distance = interval_distance(feed.f64(row, "TIME")?, feed.f64(row, "INTERVAL")?, time);
        if best.is_none_or(|(old_wildcard, old_distance, _)| {
            wildcard
                .cmp(&old_wildcard)
                .then_with(|| distance.total_cmp(&old_distance))
                .is_lt()
        }) {
            best = Some((wildcard, distance, row));
        }
    }
    best.map_or(Ok(0.0), |(_, _, row)| {
        feed.array(row, "RECEPTOR_ANGLE").map(receptor0_angle)
    })
}

pub(crate) fn load_parallactic_angle_state(ms: &MeasurementSet) -> MsResult<ParallacticAngleState> {
    let engine = MsCalEngine::for_calibration(ms)?;
    let mut feed_rows = HashMap::<(i32, i32), Vec<FeedAngleRow>>::new();
    if let Ok(feed) = ms.feed() {
        for row_index in 0..feed.row_count() {
            let antenna_id = feed.i32(row_index, "ANTENNA_ID")?;
            let feed_id = feed.i32(row_index, "FEED_ID")?;
            let spectral_window_id = feed.i32(row_index, "SPECTRAL_WINDOW_ID")?;
            let time_seconds = feed.f64(row_index, "TIME")?;
            let interval_seconds = feed.f64(row_index, "INTERVAL")?;
            let receptor0_angle_rad = receptor0_angle(feed.array(row_index, "RECEPTOR_ANGLE")?);
            feed_rows
                .entry((antenna_id, feed_id))
                .or_default()
                .push(FeedAngleRow {
                    spectral_window_id,
                    time_seconds,
                    interval_seconds,
                    receptor0_angle_rad,
                });
        }
    }
    Ok(ParallacticAngleState { engine, feed_rows })
}

fn parallactic_angle_gain(
    correlation_type: i32,
    data_desc_id: i32,
    all_correlation_types: &[i32],
    ant1_feed_pa: f64,
    ant2_feed_pa: f64,
) -> Result<Complex32, ApplyExecutionError> {
    match correlation_type {
        5 => Ok(circular_parang_gain(-ant1_feed_pa + ant2_feed_pa)),
        6 => Ok(circular_parang_gain(-ant1_feed_pa - ant2_feed_pa)),
        7 => Ok(circular_parang_gain(ant1_feed_pa + ant2_feed_pa)),
        8 => Ok(circular_parang_gain(ant1_feed_pa - ant2_feed_pa)),
        9..=12 => Err(ApplyExecutionError::UnsupportedParallacticAngleBasis {
            data_desc_id,
            correlation_types: all_correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        }),
        _ => Err(ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types: all_correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        }),
    }
}

fn circular_parang_gain(angle_rad: f64) -> Complex32 {
    Complex32::new(angle_rad.cos() as f32, angle_rad.sin() as f32)
}

fn load_correlation_types_by_ddid(ms: &MeasurementSet) -> MsResult<HashMap<i32, Vec<i32>>> {
    let dd = ms.data_description()?;
    let pol = ms.polarization()?;
    let mut out = HashMap::new();
    for row in 0..dd.row_count() {
        let pol_id = dd.polarization_id(row)?;
        out.insert(
            pol_id_of_ddid(row),
            pol.corr_type(usize::try_from(pol_id).unwrap_or(usize::MAX))?,
        );
    }
    Ok(out)
}

fn pol_id_of_ddid(ddid_row: usize) -> i32 {
    ddid_row as i32
}

pub(crate) struct LoadedCalibrationTable {
    pub(crate) path: PathBuf,
    pub(crate) baseline: bool,
    callibrary: Option<crate::CallibraryOptions>,
    time_plans: HashMap<i32, crate::lookup::TimePlan>,
    missing_grids: HashMap<i32, Arc<CalibrationGrid>>,
    time_parameter_family: Option<TimeParameterFamily>,
    phase_delay: bool,
    interp: ApplyInterpolationMode,
    supports_calwt: bool,
    engine: Option<Arc<MsCalEngine>>,
    antpos_tropo: Option<TropoDelay>,
    ionosphere: Option<ionosphere::Ionosphere>,
    sky_exposures: Option<sky::SkyExposures>,
    spline: Option<gspline::SplineTable>,
    pub(crate) solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>>,
}

#[derive(Clone, Copy)]
enum TimeParameterFamily {
    Delay,
    Antpos,
    GainCurve { power: bool },
    Opacity,
    SpectralOpacity,
    Tsys,
    Sky,
    PositionAngle,
    Tec,
    Fringe,
    SwitchedPower,
}

impl TimeParameterFamily {
    fn from_subtype(subtype: &str) -> Option<Self> {
        match subtype {
            "K Jones" | "Kcross Jones" => Some(Self::Delay),
            "KAntPos Jones" => Some(Self::Antpos),
            "EGainCurve" => Some(Self::GainCurve { power: false }),
            "EPowerCurve" => Some(Self::GainCurve { power: true }),
            "TOpac" => Some(Self::Opacity),
            "TfOpac" => Some(Self::SpectralOpacity),
            "B TSYS" => Some(Self::Tsys),
            "PosAng Jones" => Some(Self::PositionAngle),
            "F Jones" => Some(Self::Tec),
            "Fringe Jones" => Some(Self::Fringe),
            "G EVLASWPOW" => Some(Self::SwitchedPower),
            subtype if crate::single_dish::is_sky_subtype(subtype) => Some(Self::Sky),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct LegacyCalDescEntry {
    spw_id: i32,
    receptor_count: usize,
}

pub(crate) struct CalibrationSolution {
    pub(crate) time_seconds: f64,
    pub(crate) grid: Arc<CalibrationGrid>,
}

#[derive(Clone)]
pub(crate) enum CalibrationGrid {
    Complex(GainGrid),
    Delay(DelayGrid),
    Fringe(GainGrid),
    Antpos(AntposGrid),
    GainCurve(GainCurveGrid),
    Opacity(OpacityGrid),
    SpectralOpacity(GainGrid),
    SwitchedPower(switched_power::SwitchedPowerGrid),
}

#[derive(Clone)]
pub(crate) struct GainGrid {
    pub(crate) receptor_count: usize,
    pub(crate) channel_count: usize,
    pub(crate) values: ArrayD<Complex32>,
    pub(crate) flags: ArrayD<bool>,
}

#[derive(Clone, Copy)]
struct GainSample {
    value: Complex32,
    flagged: bool,
}

struct CalibrationSamplingContext<'a> {
    data_channels: DataChannelGrid<'a>,
    cal_frequencies_hz: &'a [f64],
    cal_ref_frequency_hz: f64,
    interp: ApplyInterpolationMode,
    path: &'a Path,
    engine: Option<&'a MsCalEngine>,
    antpos_tropo: Option<&'a TropoDelay>,
}

#[derive(Clone)]
pub(crate) struct DelayGrid {
    pub(crate) receptor_count: usize,
    pub(crate) channel_count: usize,
    pub(crate) values_ns: ArrayD<f32>,
    pub(crate) flags: ArrayD<bool>,
}

#[derive(Clone)]
pub(crate) struct AntposGrid {
    offsets_m: [f32; 3],
    flagged: bool,
}

#[derive(Clone)]
pub(crate) struct GainCurveGrid {
    receptor_count: usize,
    power: bool,
    coefficients: ArrayD<f32>,
    flags: ArrayD<bool>,
}

#[derive(Clone)]
pub(crate) struct OpacityGrid {
    tau: f32,
    flagged: bool,
}

impl CalibrationGrid {
    #[allow(clippy::too_many_arguments)]
    fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        match self {
            Self::Complex(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_channels,
                context.cal_frequencies_hz,
                context.interp,
                context.path,
            ),
            Self::Delay(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_channels.frequencies_hz,
                context.cal_ref_frequency_hz,
                context.path,
            ),
            Self::Fringe(grid) => fringe::sample(grid, receptor, data_chan_index, context),
            Self::Antpos(grid) => {
                grid.sample(data_chan_index, field_id, antenna_id, time_seconds, context)
            }
            Self::GainCurve(grid) => {
                grid.sample(receptor, field_id, antenna_id, time_seconds, context)
            }
            Self::Opacity(grid) => grid.sample(field_id, antenna_id, time_seconds, context),
            Self::SpectralOpacity(grid) => {
                let sample = grid.sample(
                    0,
                    data_chan_index,
                    context.data_channels,
                    context.cal_frequencies_hz,
                    context.interp,
                    context.path,
                )?;
                let elevation = opacity_elevation(
                    context.engine,
                    field_id,
                    antenna_id,
                    time_seconds,
                    context.path,
                    None,
                )?;
                Ok(GainSample {
                    value: Complex32::new(opacity_gain(sample.value.re, elevation), 0.),
                    flagged: sample.flagged,
                })
            }
            Self::SwitchedPower(grid) => Ok(grid.sample(receptor)),
        }
    }
}

impl GainGrid {
    fn cross_hand_diagonal(self, path: &Path) -> Result<Self, ApplyExecutionError> {
        if self.receptor_count != 1 {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "cross-hand phase requires one CPARAM receptor, found {:?}",
                    self.values.shape()
                ),
            });
        }
        let channel_count = self.channel_count;
        let shape = IxDyn(&[2, channel_count]).f();
        let values = ArrayD::from_shape_fn(shape.clone(), |index| {
            if index[0] == 0 {
                self.value_at(0, index[1])
            } else {
                Complex32::ONE
            }
        });
        let flags = ArrayD::from_shape_fn(shape, |index| self.flag_at(0, index[1]));
        Ok(Self {
            receptor_count: 2,
            channel_count,
            values,
            flags,
        })
    }

    pub(crate) fn from_arrays(
        path: &Path,
        gains: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let ArrayValue::Complex32(values) = gains else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: gains.shape().to_vec(),
            });
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };

        if values.shape() != flags.shape() || values.is_empty() {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: "parameter and FLAG arrays must have matching, nonempty shapes".into(),
            });
        }

        match values.shape() {
            [receptor_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: 1,
                values: values.clone(),
                flags: flags.clone(),
            }),
            [receptor_count, channel_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: *channel_count,
                values: values.clone(),
                flags: flags.clone(),
            }),
            shape => Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: shape.to_vec(),
            }),
        }
    }

    fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        data_channels: DataChannelGrid<'_>,
        cal_frequencies_hz: &[f64],
        interp: ApplyInterpolationMode,
        path: &Path,
    ) -> Result<GainSample, ApplyExecutionError> {
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        if self.channel_count <= 1 {
            return Ok(GainSample {
                value: self.value_at(receptor, 0),
                flagged: self.flag_at(receptor, 0),
            });
        }

        match interp {
            ApplyInterpolationMode::Nearest
            | ApplyInterpolationMode::Linear
            | ApplyInterpolationMode::Casa { .. }
            | ApplyInterpolationMode::Invalid => {
                let chan = data_channels
                    .original_index(data_chan_index)
                    .min(self.channel_count.saturating_sub(1));
                Ok(GainSample {
                    value: self.value_at(receptor, chan),
                    flagged: self.flag_at(receptor, chan),
                })
            }
            ApplyInterpolationMode::NearestLinear => {
                if cal_frequencies_hz.len() != self.channel_count {
                    return Err(ApplyExecutionError::UnsupportedInterpolation {
                        path: path.display().to_string(),
                        interp,
                        reason: "calibration frequency grid does not match CPARAM channel axis"
                            .to_string(),
                    });
                }
                let target_frequency = *data_channels
                    .frequencies_hz
                    .get(data_chan_index)
                    .ok_or_else(|| ApplyExecutionError::UnsupportedInterpolation {
                        path: path.display().to_string(),
                        interp,
                        reason: "data channel index is outside the MeasurementSet SPW grid"
                            .to_string(),
                    })?;
                Ok(self.sample_frequency_linear(receptor, target_frequency, cal_frequencies_hz))
            }
        }
    }

    fn sample_frequency_linear(
        &self,
        receptor: usize,
        target_frequency_hz: f64,
        cal_frequencies_hz: &[f64],
    ) -> GainSample {
        if target_frequency_hz <= cal_frequencies_hz[0] {
            return GainSample {
                value: self.value_at(receptor, 0),
                flagged: self.flag_at(receptor, 0),
            };
        }
        if target_frequency_hz >= cal_frequencies_hz[self.channel_count - 1] {
            let last = self.channel_count - 1;
            return GainSample {
                value: self.value_at(receptor, last),
                flagged: self.flag_at(receptor, last),
            };
        }

        for upper in 1..self.channel_count {
            let lower = upper - 1;
            let low_freq = cal_frequencies_hz[lower];
            let high_freq = cal_frequencies_hz[upper];
            if target_frequency_hz <= high_freq {
                let fraction = (target_frequency_hz - low_freq) / (high_freq - low_freq);
                let low = self.value_at(receptor, lower);
                let high = self.value_at(receptor, upper);
                return GainSample {
                    value: low + (high - low) * fraction as f32,
                    flagged: self.flag_at(receptor, lower) || self.flag_at(receptor, upper),
                };
            }
        }

        let last = self.channel_count - 1;
        GainSample {
            value: self.value_at(receptor, last),
            flagged: self.flag_at(receptor, last),
        }
    }

    fn value_at(&self, receptor: usize, channel: usize) -> Complex32 {
        match self.values.ndim() {
            1 => self.values[[receptor]],
            2 => self.values[[receptor, channel]],
            _ => unreachable!("validated during construction"),
        }
    }

    fn flag_at(&self, receptor: usize, channel: usize) -> bool {
        match self.flags.ndim() {
            1 => self.flags[[receptor]],
            2 => self.flags[[receptor, channel]],
            _ => unreachable!("validated during construction"),
        }
    }
}

impl DelayGrid {
    fn from_arrays(
        path: &Path,
        delays: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values_ns = match delays {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };

        match values_ns.shape() {
            [receptor_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: 1,
                values_ns,
                flags: flags.clone(),
            }),
            [receptor_count, channel_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: *channel_count,
                values_ns,
                flags: flags.clone(),
            }),
            shape => Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: shape.to_vec(),
            }),
        }
    }

    fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        data_frequencies_hz: &[f64],
        cal_ref_frequency_hz: f64,
        path: &Path,
    ) -> Result<GainSample, ApplyExecutionError> {
        let target_frequency_hz = *data_frequencies_hz.get(data_chan_index).ok_or_else(|| {
            ApplyExecutionError::UnsupportedInterpolation {
                path: path.display().to_string(),
                interp: ApplyInterpolationMode::Nearest,
                reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
            }
        })?;
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let delay_ns = self.value_at(receptor, 0);
        let flagged = self.flag_at(receptor, 0);
        let phase_rad = 2.0_f64
            * std::f64::consts::PI
            * f64::from(delay_ns)
            * ((target_frequency_hz - cal_ref_frequency_hz) / 1.0e9_f64);
        Ok(GainSample {
            value: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
            flagged,
        })
    }

    fn value_at(&self, receptor: usize, channel: usize) -> f32 {
        match self.values_ns.ndim() {
            1 => self.values_ns[[receptor]],
            2 => self.values_ns[[receptor, channel.min(self.channel_count.saturating_sub(1))]],
            _ => unreachable!("validated during construction"),
        }
    }

    fn flag_at(&self, receptor: usize, channel: usize) -> bool {
        match self.flags.ndim() {
            1 => self.flags[[receptor]],
            2 => self.flags[[receptor, channel.min(self.channel_count.saturating_sub(1))]],
            _ => unreachable!("validated during construction"),
        }
    }
}

impl AntposGrid {
    fn from_arrays(
        path: &Path,
        offsets: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values = match offsets {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(_) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        if values.len() < 3 {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: values.shape().to_vec(),
            });
        }
        Ok(Self {
            offsets_m: [values[[0, 0]], values[[1, 0]], values[[2, 0]]],
            // KAntPosJones::calcAllJones marks the projected Jones term valid
            // regardless of the FPARAM flags. Missing rows are handled separately.
            flagged: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn sample(
        &self,
        data_chan_index: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let path = context.path;
        let data_frequencies_hz = context.data_channels.frequencies_hz;
        let frequency_hz = *data_frequencies_hz.get(data_chan_index).ok_or_else(|| {
            ApplyExecutionError::UnsupportedInterpolation {
                path: path.display().to_string(),
                interp: ApplyInterpolationMode::Nearest,
                reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
            }
        })?;
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
            });
        };
        let delay_ns = antpos_delay_ns(
            engine,
            field_id,
            antenna_id,
            time_seconds,
            self.offsets_m.map(f64::from),
            path,
            context.antpos_tropo,
            None,
        )?;
        let phase_rad = 2.0 * std::f64::consts::PI * delay_ns * (frequency_hz / 1.0e9);
        Ok(GainSample {
            value: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
            flagged: self.flagged,
        })
    }
}

impl GainCurveGrid {
    fn from_arrays(
        path: &Path,
        coefficients: &ArrayValue,
        flags: &ArrayValue,
        power: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let coefficients = match coefficients {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(_) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        let terms = if power {
            crate::gencal::POWER_CURVE_TERMS
        } else {
            4
        };
        if coefficients.ndim() != 2
            || coefficients.shape()[0] % terms != 0
            || (power && coefficients.shape()[0] != 2 * terms)
        {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: coefficients.shape().to_vec(),
            });
        }
        Ok(Self {
            receptor_count: coefficients.shape()[0] / terms,
            power,
            // EGainCurve::calcAllJones evaluates the coefficients and marks
            // each Jones term valid regardless of the input flags.
            flags: ArrayD::from_elem(IxDyn(coefficients.shape()), false),
            coefficients,
        })
    }

    fn sample(
        &self,
        receptor: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: context.path.display().to_string(),
                reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
            });
        };
        let (_az, elevation) = engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: context.path.display().to_string(),
                source,
            })?;
        let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let (gain, flagged) = self.evaluate(receptor, za_degrees);
        Ok(GainSample {
            value: Complex32::new(gain, 0.0),
            flagged,
        })
    }

    fn evaluate(&self, receptor: usize, za_degrees: f64) -> (f32, bool) {
        if self.power {
            let terms = crate::gencal::POWER_CURVE_TERMS;
            let base = receptor * terms;
            let mut loss = f64::from(self.coefficients[[base, 0]]);
            let mut angle_power = 1.0;
            for index in 1..terms {
                angle_power *= za_degrees;
                loss += f64::from(self.coefficients[[base + index, 0]]) * angle_power;
            }
            let missing = self.flags[[base, 0]];
            return if !missing && loss >= 0.0 {
                (loss.sqrt() as f32, false)
            } else {
                (0.0, true)
            };
        }
        let za_degrees = za_degrees as f32;
        let base = receptor * 4;
        let mut gain = self.coefficients[[base, 0]];
        let mut angle_power = 1.0_f32;
        for index in 1..4 {
            angle_power *= za_degrees;
            gain += self.coefficients[[base + index, 0]] * angle_power;
        }
        let flagged = (0..4).any(|index| self.flags[[base + index, 0]]);
        (gain, flagged)
    }
}

impl OpacityGrid {
    fn from_arrays(
        path: &Path,
        tau: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values = match tau {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        if values.is_empty() {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: values.shape().to_vec(),
            });
        }
        Ok(Self {
            tau: values[[0, 0]],
            flagged: flags.iter().any(|flag| *flag),
        })
    }

    fn sample(
        &self,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: context.path.display().to_string(),
                reason: "TOpac apply requires MeasurementSet geometry".to_string(),
            });
        };
        let (_az, elevation) = engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: context.path.display().to_string(),
                source,
            })?;
        let zenith_angle = std::f64::consts::FRAC_PI_2 - elevation;
        let gain = if zenith_angle < std::f64::consts::FRAC_PI_2 {
            (-f64::from(self.tau) / zenith_angle.cos()).exp().sqrt() as f32
        } else {
            1.0
        };
        Ok(GainSample {
            value: Complex32::new(gain, 0.0),
            flagged: self.flagged,
        })
    }
}

impl LoadedCalibrationTable {
    pub(crate) fn interpolation_windows(&self) -> Vec<i32> {
        let mut windows = self.missing_grids.keys().copied().collect::<Vec<_>>();
        windows.sort_unstable();
        windows
    }

    pub(crate) fn complex_solution_for_field(
        &self,
        field: i32,
        query: crate::lookup::TimeQuery,
    ) -> Result<Option<crate::ComplexSolution>, ApplyExecutionError> {
        self.time_plans
            .get(&field)
            .map(|plan| plan.solution(&self.path, query))
            .transpose()
            .map(Option::flatten)
    }

    pub(crate) fn lookup(
        &self,
        field_id: i32,
        spw_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        interp: ApplyInterpolationMode,
    ) -> Result<Option<Arc<CalibrationGrid>>, ApplyExecutionError> {
        if let Some(spline) = &self.spline {
            return Ok(gspline::grid(
                spline,
                field_id,
                antenna_id,
                time_seconds,
                None,
            ));
        }
        if self.interp != interp {
            return Err(ApplyExecutionError::UnsupportedInterpolation {
                path: self.path.display().to_string(),
                interp,
                reason: "loaded-table interpolation state diverged from the plan".to_string(),
            });
        }
        let candidates = self
            .solutions
            .get(&(field_id, spw_id, antenna_id))
            .or_else(|| self.solutions.get(&(field_id, spw_id, -1)))
            .map(Vec::as_slice)
            .or_else(|| {
                // CASA applies a sole-field table across selected MS fields by
                // default when no explicit gainfield override is required.
                self.lookup_single_field_candidates(spw_id, antenna_id)
            });

        let Some(candidates) = candidates else {
            return Ok(None);
        };

        match interp {
            ApplyInterpolationMode::Nearest | ApplyInterpolationMode::NearestLinear => Ok(Some(
                candidates
                    .iter()
                    .min_by(|a, b| {
                        (a.time_seconds - time_seconds)
                            .abs()
                            .total_cmp(&(b.time_seconds - time_seconds).abs())
                    })
                    .expect("non-empty candidates")
                    .grid
                    .clone(),
            )),
            ApplyInterpolationMode::Linear => {
                interpolate_time_linear(&self.path, candidates, time_seconds).map(Some)
            }
            other => Err(ApplyExecutionError::UnsupportedInterpolation {
                path: self.path.display().to_string(),
                interp: other,
                reason: "contextual interpolation requires the common apply time engine (#1518)"
                    .to_string(),
            }),
        }
    }

    fn lookup_single_field_candidates(
        &self,
        spw_id: i32,
        antenna_id: i32,
    ) -> Option<&[CalibrationSolution]> {
        let matching_fields = self
            .solutions
            .keys()
            .filter(|(_, key_spw_id, key_antenna_id)| {
                *key_spw_id == spw_id && (*key_antenna_id == antenna_id || *key_antenna_id == -1)
            })
            .map(|(key_field_id, _, _)| *key_field_id)
            .collect::<BTreeSet<_>>();
        let sole_field_id = match matching_fields.len() {
            1 => *matching_fields.first().expect("one matching field"),
            _ => return None,
        };
        self.solutions
            .get(&(sole_field_id, spw_id, antenna_id))
            .or_else(|| self.solutions.get(&(sole_field_id, spw_id, -1)))
            .map(Vec::as_slice)
    }
}

fn geometry_engine_for_plan(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
) -> Result<Option<Arc<MsCalEngine>>, ApplyExecutionError> {
    let needs_geometry = plan.calibration_tables.iter().any(|table| {
        matches!(
            table.summary.table_subtype.as_str(),
            "KAntPos Jones" | "EGainCurve" | "EPowerCurve" | "TOpac" | "TfOpac" | "F Jones"
        )
    });
    if !needs_geometry {
        return Ok(None);
    }
    let ms_path = display_ms_path(ms);
    MsCalEngine::for_calibration(ms)
        .map(Arc::new)
        .map(Some)
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path,
            source,
        })
}

pub(crate) fn load_calibration_table(
    table_plan: &ApplyCalibrationTablePlan,
    geometry_engine: Option<&Arc<MsCalEngine>>,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    load_parameters(table_plan, geometry_engine, false, None)
}

pub(crate) fn load_simulator_table(
    table_plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    load_parameters(table_plan, None, true, None)
}

fn load_parameters(
    table_plan: &ApplyCalibrationTablePlan,
    geometry_engine: Option<&Arc<MsCalEngine>>,
    real_parameters: bool,
    ms: Option<&MeasurementSet>,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    match table_plan.summary.table_subtype.as_str() {
        "BPOLY" => return load_bpoly_calibration_table(table_plan),
        "GSPLINE" => return gspline::load(table_plan),
        _ => {}
    }
    let table = Table::open(TableOptions::new(&table_plan.spec.path)).map_err(|source| {
        ApplyExecutionError::OpenMeasurementSet {
            path: table_plan.spec.path.display().to_string(),
            source: MsError::from(source),
        }
    })?;
    let leakage = polarization::Leakage::from_subtype(&table_plan.summary.table_subtype);
    let ionosphere = ionosphere::Ionosphere::for_plan(&table, table_plan)?;
    let cross_hand_phase = polarization::is_cross_hand_phase(&table_plan.summary.table_subtype);
    let sky_term = crate::single_dish::is_sky_subtype(&table_plan.summary.table_subtype);
    let sky_exposures = if sky_term && table_plan.calwt {
        ms.map(|ms| sky::SkyExposures::read(ms, &table, &table_plan.spec.path))
            .transpose()?
    } else {
        None
    };
    let supports_calwt =
        weights::supports_calwt(&table_plan.summary, leakage.is_some(), cross_hand_phase);
    let antpos_tropo = match (
        table_plan.summary.table_subtype.as_str(),
        ms,
        geometry_engine,
    ) {
        ("KAntPos Jones", Some(ms), Some(engine)) => load_antpos_tropo(&table, ms, engine)?,
        _ => None,
    };

    let baseline = ms.is_some() && baseline::is_subtype(&table_plan.summary.table_subtype);
    let x_mueller = table_plan.summary.table_subtype == "X Mueller";
    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for row_index in 0..table.row_count() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let spw_id = get_i32(&table, row_index, COL_SPECTRAL_WINDOW_ID)?;
        let Some((antenna_id, reversed)) = load_parameter_antenna(
            &table,
            row_index,
            &table_plan.spec.path,
            baseline,
            x_mueller,
        )?
        else {
            continue;
        };
        let time_seconds = get_f64(&table, row_index, COL_TIME)?;
        let flags = table
            .cell_accessor(row_index, COL_FLAG)
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: table_plan.spec.path.display().to_string(),
                source: MsError::from(source),
            })?;
        let read_fparam = || {
            table
                .cell_accessor(row_index, COL_FPARAM)
                .and_then(|cell| cell.array())
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: table_plan.spec.path.display().to_string(),
                    source: MsError::from(source),
                })
        };
        let grid = match table_plan.summary.parameter_family {
            CalibrationParameterFamily::Complex => load_complex_parameters(
                &table,
                row_index,
                &table_plan.spec.path,
                flags,
                if baseline && table_plan.summary.table_subtype != "A Mueller" {
                    Some("baseline calibration")
                } else {
                    leakage.map(|_| "leakage")
                },
                cross_hand_phase,
                reversed,
            )?,
            CalibrationParameterFamily::Float
                if real_parameters || table_plan.summary.table_subtype == "G EVLASWPOW" =>
            {
                let values = read_fparam()?;
                let path = &table_plan.spec.path;
                if (matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "PosAng Jones" | "TfOpac"
                ) || ionosphere.is_some())
                    && values.shape().first() != Some(&1)
                {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: path.display().to_string(),
                        shape: values.shape().to_vec(),
                    });
                }
                if table_plan.summary.table_subtype == "Fringe Jones" && values.shape() != [8, 1] {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: path.display().to_string(),
                        shape: values.shape().to_vec(),
                    });
                }
                CalibrationGrid::Complex(GainGrid::from_real_arrays(path, values, flags)?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "K Jones" | "Kcross Jones"
                ) =>
            {
                let delays = read_fparam()?;
                CalibrationGrid::Delay(DelayGrid::from_arrays(
                    &table_plan.spec.path,
                    delays,
                    flags,
                )?)
            }
            CalibrationParameterFamily::Float
                if table_plan.summary.table_subtype.as_str() == "KAntPos Jones" =>
            {
                let offsets = read_fparam()?;
                CalibrationGrid::Antpos(AntposGrid::from_arrays(
                    &table_plan.spec.path,
                    offsets,
                    flags,
                )?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "EGainCurve" | "EPowerCurve"
                ) =>
            {
                let coefficients = read_fparam()?;
                CalibrationGrid::GainCurve(GainCurveGrid::from_arrays(
                    &table_plan.spec.path,
                    coefficients,
                    flags,
                    table_plan.summary.table_subtype == "EPowerCurve",
                )?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "TOpac" | "TfOpac"
                ) =>
            {
                let tau = read_fparam()?;
                let path = &table_plan.spec.path;
                if table_plan.summary.table_subtype == "TfOpac" {
                    CalibrationGrid::SpectralOpacity(GainGrid::from_real_arrays(path, tau, flags)?)
                } else {
                    CalibrationGrid::Opacity(OpacityGrid::from_arrays(path, tau, flags)?)
                }
            }
            _ => {
                return Err(ApplyExecutionError::UnsupportedInterpolation {
                    path: table_plan.spec.path.display().to_string(),
                    interp: table_plan.interp,
                    reason: "unsupported calibration parameter family for executor".to_string(),
                });
            }
        };
        solutions
            .entry((field_id, spw_id, antenna_id))
            .or_default()
            .push(CalibrationSolution {
                time_seconds,
                grid: Arc::new(grid),
            });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        baseline,
        interp: table_plan.interp,
        supports_calwt,
        engine: geometry_engine.cloned(),
        antpos_tropo,
        ionosphere,
        sky_exposures,
        spline: None,
        solutions,
        callibrary: None,
        time_plans: HashMap::new(),
        missing_grids: HashMap::new(),
        time_parameter_family: None,
        phase_delay: false,
    })
}

fn load_parameter_antenna(
    table: &Table,
    row_index: usize,
    path: &Path,
    baseline: bool,
    x_mueller: bool,
) -> Result<Option<(i32, bool)>, ApplyExecutionError> {
    let first = get_i32(table, row_index, COL_ANTENNA1)?;
    if x_mueller {
        // XMueller uses the first baseline parameter for every baseline.
        let is_first = first == 0 && get_i32(table, row_index, COL_ANTENNA2)? == 0;
        Ok(is_first.then_some((0, false)))
    } else if baseline {
        let second = get_i32(table, row_index, COL_ANTENNA2)?;
        let index = baseline::require_index(first, second, path)?;
        Ok(Some((index, first > second)))
    } else {
        Ok(Some((first, false)))
    }
}

fn load_complex_parameters(
    table: &Table,
    row_index: usize,
    path: &Path,
    flags: &ArrayValue,
    two_receptor_family: Option<&str>,
    cross_hand_phase: bool,
    reversed: bool,
) -> Result<CalibrationGrid, ApplyExecutionError> {
    let gains = table
        .cell_accessor(row_index, COL_CPARAM)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: MsError::from(source),
        })?;
    if let Some(family) = two_receptor_family
        && gains.shape().first() != Some(&2)
    {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{family} requires two CPARAM receptors, found shape {:?} at row {row_index}",
                gains.shape()
            ),
        });
    }
    let mut grid = GainGrid::from_arrays(path, gains, flags)?;
    if reversed {
        grid.values.mapv_inplace(|value| value.conj());
        if grid.receptor_count == 4 && grid.values.ndim() == 2 {
            for coefficient in 0..grid.channel_count {
                grid.values.swap([1, coefficient], [2, coefficient]);
                grid.flags.swap([1, coefficient], [2, coefficient]);
            }
        }
    }
    if cross_hand_phase {
        grid = grid.cross_hand_diagonal(path)?;
    }
    Ok(CalibrationGrid::Complex(grid))
}

fn load_antpos_tropo(
    table: &Table,
    ms: &MeasurementSet,
    engine: &MsCalEngine,
) -> Result<Option<TropoDelay>, ApplyExecutionError> {
    let Some(value) = table.keywords().get(TROPO_KEYWORD) else {
        return Ok(None);
    };
    let failed = |source| ApplyExecutionError::MutateMeasurementSet {
        path: display_ms_path(ms),
        source,
    };
    if !only_vla_observations(ms).map_err(failed)? {
        return Ok(None);
    }
    let scale = match value {
        Value::Scalar(ScalarValue::Float64(scale)) => *scale,
        Value::Scalar(ScalarValue::Float32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::UInt8(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int16(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::UInt32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int64(scale)) => *scale as f64,
        _ => {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: table
                    .path()
                    .map_or_else(String::new, |path| path.display().to_string()),
                reason: format!("{TROPO_KEYWORD} must be a numeric keyword"),
            });
        }
    };
    (scale != 0.0)
        .then(|| TropoDelay::new(ms, engine, scale))
        .transpose()
        .map_err(failed)
}

fn load_bpoly_calibration_table(
    table_plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    let table = Table::open(TableOptions::new(&table_plan.spec.path)).map_err(|source| {
        ApplyExecutionError::OpenMeasurementSet {
            path: table_plan.spec.path.display().to_string(),
            source: MsError::from(source),
        }
    })?;
    let cal_desc = Table::open(TableOptions::new(
        table_plan.spec.path.join(LEGACY_CAL_DESC_KEYWORD),
    ))
    .map_err(
        |source| ApplyExecutionError::OpenCalibrationAuxiliaryTable {
            path: table_plan.spec.path.display().to_string(),
            subtable: LEGACY_CAL_DESC_KEYWORD,
            source,
        },
    )?;
    let cal_desc_map = load_bpoly_cal_desc_map(&table_plan.spec.path, &cal_desc)?;
    let spw_plans = table_plan
        .calibration_spectral_windows
        .iter()
        .map(|spw| (spw.spw_id, spw))
        .collect::<HashMap<_, _>>();

    let rows = (0..table.row_count())
        .map(|row| {
            Ok((
                get_i32(&table, row, COL_CAL_DESC_ID)?,
                get_f64(&table, row, COL_TIME)?,
            ))
        })
        .collect::<Result<Vec<_>, ApplyExecutionError>>()?;
    let first = rows
        .iter()
        .copied()
        .min_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for (row_index, (cal_desc_id, time_seconds)) in rows.into_iter().enumerate() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let antenna_id = get_i32(&table, row_index, COL_ANTENNA1)?;
        let cal_desc_entry = cal_desc_map.get(&cal_desc_id).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: table_plan.spec.path.display().to_string(),
                reason: format!("CAL_DESC_ID {cal_desc_id} was not present in CAL_DESC"),
            }
        })?;
        let Some(spw_plan) = spw_plans.get(&cal_desc_entry.spw_id) else {
            // Only mapped, applicable windows have a target sampling grid.
            // Unavailable rows are skipped or strictly flagged by the caller.
            continue;
        };
        let grid = sample_bpoly_row(
            &table,
            row_index,
            &table_plan.spec.path,
            spw_plan,
            *cal_desc_entry,
            first == Some((cal_desc_id, time_seconds)),
        )?;
        solutions
            .entry((field_id, cal_desc_entry.spw_id, antenna_id))
            .or_default()
            .push(CalibrationSolution {
                time_seconds,
                grid: Arc::new(grid),
            });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        baseline: false,
        interp: table_plan.interp,
        supports_calwt: false,
        engine: None,
        antpos_tropo: None,
        ionosphere: None,
        sky_exposures: None,
        spline: None,
        solutions,
        callibrary: None,
        time_plans: HashMap::new(),
        missing_grids: HashMap::new(),
        time_parameter_family: None,
        phase_delay: false,
    })
}

fn load_bpoly_cal_desc_map(
    path: &Path,
    cal_desc: &Table,
) -> Result<HashMap<i32, LegacyCalDescEntry>, ApplyExecutionError> {
    let mut entries = HashMap::new();
    for row_index in 0..cal_desc.row_count() {
        let spw_ids = cal_desc
            .cell_accessor(row_index, COL_SPECTRAL_WINDOW_ID)
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "failed to read CAL_DESC SPECTRAL_WINDOW_ID row {row_index}: {source}"
                ),
            })?;
        let ArrayValue::Int32(spw_ids) = spw_ids else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC SPECTRAL_WINDOW_ID row {row_index} had unexpected type {:?}",
                    spw_ids.primitive_type()
                ),
            });
        };
        let spw_values = spw_ids.iter().copied().collect::<Vec<_>>();
        let [spw_id] = spw_values.as_slice() else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC row {row_index} must reference exactly one spectral window"
                ),
            });
        };
        let receptor_count = get_i32(cal_desc, row_index, "NUM_RECEPTORS")?;
        let receptor_count = usize::try_from(receptor_count).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC row {row_index} had invalid NUM_RECEPTORS value {receptor_count}"
                ),
            }
        })?;
        entries.insert(
            row_index as i32,
            LegacyCalDescEntry {
                spw_id: *spw_id,
                receptor_count,
            },
        );
    }
    Ok(entries)
}

fn sample_bpoly_row(
    table: &Table,
    row_index: usize,
    path: &Path,
    spw_plan: &crate::plan::SpectralWindowPlan,
    cal_desc_entry: LegacyCalDescEntry,
    first_interval: bool,
) -> Result<CalibrationGrid, ApplyExecutionError> {
    let scale_factor = get_complex32(table, row_index, COL_SCALE_FACTOR)?;
    let valid_domain = get_f64_array(table, row_index, COL_VALID_DOMAIN, path)?;
    let [domain_start_hz, domain_end_hz] = valid_domain.as_slice() else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("BPOLY VALID_DOMAIN row {row_index} must contain exactly two values"),
        });
    };
    let amp_coeff_count =
        usize::try_from(get_i32(table, row_index, COL_N_POLY_AMP)?).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("BPOLY N_POLY_AMP row {row_index} was negative"),
            }
        })?;
    let phase_coeff_count =
        usize::try_from(get_i32(table, row_index, COL_N_POLY_PHASE)?).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("BPOLY N_POLY_PHASE row {row_index} was negative"),
            }
        })?;
    let amp_values = get_numeric_array(table, row_index, COL_POLY_COEFF_AMP, path)?;
    let phase_values = get_numeric_array(table, row_index, COL_POLY_COEFF_PHASE, path)?;
    let amp_receptor_count = infer_bpoly_receptor_count(
        amp_values.len(),
        cal_desc_entry.receptor_count,
        amp_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_AMP,
    )?;
    let phase_receptor_count = infer_bpoly_receptor_count(
        phase_values.len(),
        cal_desc_entry.receptor_count,
        phase_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_PHASE,
    )?;
    if amp_coeff_count > 0 && phase_coeff_count > 0 && amp_receptor_count != phase_receptor_count {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "BPOLY row {row_index} inferred mismatched receptor counts: amp={amp_receptor_count}, phase={phase_receptor_count}"
            ),
        });
    }
    let receptor_count = if amp_coeff_count == 0 {
        phase_receptor_count
    } else {
        amp_receptor_count
    };
    let amp_coefficients = split_bpoly_coefficients(
        amp_values,
        receptor_count,
        amp_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_AMP,
    )?;
    let phase_coefficients = split_bpoly_coefficients(
        phase_values,
        receptor_count,
        phase_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_PHASE,
    )?;
    let phase_unit_scale = match get_string(table, row_index, COL_PHASE_UNITS)?
        .to_ascii_uppercase()
        .as_str()
    {
        "RAD" | "RADIAN" | "RADIANS" => 1.0_f64,
        "DEG" | "DEGREE" | "DEGREES" => std::f64::consts::PI / 180.0_f64,
        other => {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("unsupported BPOLY PHASE_UNITS value {other:?}"),
            });
        }
    };

    let channel_count = spw_plan.channel_frequencies_hz.len().max(1);
    let mut values = ArrayD::from_elem(
        IxDyn(&[receptor_count, channel_count]).f(),
        Complex32::new(1.0, 0.0),
    );
    let mut flags = ArrayD::from_elem(IxDyn(&[receptor_count, channel_count]).f(), false);
    let domain_start_hz = *domain_start_hz;
    let domain_end_hz = *domain_end_hz;

    for receptor in 0..receptor_count {
        let amp_coeff = amp_coefficients.get(receptor).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("missing BPOLY amplitude coefficients for receptor {receptor}"),
            }
        })?;
        let phase_coeff = phase_coefficients.get(receptor).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("missing BPOLY phase coefficients for receptor {receptor}"),
            }
        })?;
        // The CASA-layout loader starts with valid unity gains only for the
        // first stored interval. Later intervals flag receptor groups whose
        // coefficients never replace that interval's initial unity value.
        if cal_desc_entry.receptor_count == 0
            && !first_interval
            && amp_coeff
                .iter()
                .chain(phase_coeff)
                .all(|&coefficient| coefficient == 0.0)
        {
            flags.index_axis_mut(ndarray::Axis(0), receptor).fill(true);
            continue;
        }

        for (channel, frequency_hz) in spw_plan.channel_frequencies_hz.iter().copied().enumerate() {
            if frequency_hz < domain_start_hz || frequency_hz > domain_end_hz {
                values[[receptor, channel]] = Complex32::new(1.0, 0.0);
                continue;
            }

            let amp_value = legacy_bpoly_chebyshev_value(
                amp_coeff,
                domain_start_hz,
                domain_end_hz,
                frequency_hz,
            );
            let phase_rad = legacy_bpoly_chebyshev_value(
                phase_coeff,
                domain_start_hz,
                domain_end_hz,
                frequency_hz,
            ) * phase_unit_scale;
            let amp_scale = amp_value.exp() as f32;
            let polynomial_gain =
                Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32) * amp_scale;
            values[[receptor, channel]] = scale_factor * polynomial_gain;
        }
    }

    Ok(CalibrationGrid::Complex(GainGrid {
        receptor_count,
        channel_count,
        values,
        flags,
    }))
}

fn split_bpoly_coefficients(
    coefficients: Vec<f64>,
    receptor_count: usize,
    coefficients_per_receptor: usize,
    path: &Path,
    row_index: usize,
    column: &str,
) -> Result<Vec<Vec<f64>>, ApplyExecutionError> {
    let expected = receptor_count.saturating_mul(coefficients_per_receptor);
    if coefficients.len() != expected {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "BPOLY {column} row {row_index} contained {} coefficients but expected {expected}",
                coefficients.len()
            ),
        });
    }
    if coefficients_per_receptor == 0 {
        return Ok(vec![Vec::new(); receptor_count]);
    }
    Ok(coefficients
        .chunks(coefficients_per_receptor)
        .map(|chunk| chunk.to_vec())
        .collect())
}

fn infer_bpoly_receptor_count(
    total_coefficients: usize,
    nominal_receptor_count: usize,
    coefficients_per_receptor: usize,
    path: &Path,
    row_index: usize,
    column: &str,
) -> Result<usize, ApplyExecutionError> {
    if coefficients_per_receptor == 0 {
        return Ok(nominal_receptor_count.max(1));
    }
    let nominal_total = nominal_receptor_count.saturating_mul(coefficients_per_receptor);
    if total_coefficients == nominal_total {
        return Ok(nominal_receptor_count.max(1));
    }
    if total_coefficients.is_multiple_of(coefficients_per_receptor) {
        return Ok((total_coefficients / coefficients_per_receptor).max(1));
    }
    Err(ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: format!(
            "BPOLY {column} row {row_index} contained {total_coefficients} coefficients, which is not divisible by the per-receptor coefficient count {coefficients_per_receptor}"
        ),
    })
}

pub(crate) use casa_numerics::legacy_bpoly_chebyshev_value;

fn cal_spw_reference_frequency_hz(cal_spw: &crate::plan::SpectralWindowPlan) -> f64 {
    cal_spw
        .channel_frequencies_hz
        .get(cal_spw.channel_frequencies_hz.len() / 2)
        .copied()
        .unwrap_or(cal_spw.ref_frequency_hz)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CorrectedDataCreation {
    created: bool,
}

fn ensure_corrected_data_column(
    ms: &mut MeasurementSet,
    observed_column: &str,
) -> Result<CorrectedDataCreation, TableError> {
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(VisibilityDataColumn::CorrectedData.name()))
    {
        return Ok(CorrectedDataCreation { created: false });
    }
    if observed_column == "FLOAT_DATA"
        && let Some(path) = ms.path()
    {
        let lock = casa_tables::TableWriteLock::acquire_to_reread(path, 1)?;
        crate::model_columns::initialize_calibrater_columns(ms, false, true, false, lock).map_err(
            |error| match error {
                MsError::Table(error) => error,
                other => TableError::Storage(other.to_string()),
            },
        )?;
        return Ok(CorrectedDataCreation { created: true });
    }

    // CASA's `addScrCols` clears the cached sort keywords first.
    casa_ms::scratch_columns::prepare_write(ms, VisibilityDataColumn::CorrectedData, true)
        .map_err(|error| match error {
            MsError::Table(error) => error,
            other => TableError::Storage(other.to_string()),
        })?;
    let column_def = *VisibilityDataColumn::CorrectedData
        .optional_column()
        .column_def();
    let schema = build_table_schema(&[column_def]).expect("single optional column schema");
    let column: ColumnSchema = schema
        .column(VisibilityDataColumn::CorrectedData.name())
        .expect("corrected data column present")
        .clone();
    ms.main_table_mut().add_column(column, None)?;
    if let Some(data_keywords) = ms.main_table().column_keywords(observed_column).cloned() {
        ms.main_table_mut()
            .set_column_keywords(VisibilityDataColumn::CorrectedData.name(), data_keywords);
    }

    if ms.path().is_none() {
        let data_rows = (0..ms.row_count())
            .map(|row_index| {
                let data = ms
                    .main_table()
                    .column_accessor(observed_column)?
                    .array_cell(row_index)?
                    .clone();
                Ok(match data {
                    ArrayValue::Float32(values) => complex_visibility_from_float(&values),
                    data => data,
                })
            })
            .collect::<Result<Vec<_>, casa_tables::TableError>>()?;
        let mut writer = ms
            .main_table_mut()
            .row_accessor_mut()
            .prepare(&[VisibilityDataColumn::CorrectedData.name()])?;
        let slot = writer
            .column_index(VisibilityDataColumn::CorrectedData.name())
            .expect("prepared CORRECTED_DATA slot");
        for (row_index, data) in data_rows.into_iter().enumerate() {
            writer.seek(row_index)?;
            writer.set_value_at(slot, Value::Array(data))?;
        }
    }

    Ok(CorrectedDataCreation { created: true })
}

fn apply_observed_column(ms: &MeasurementSet, prefer_float: bool) -> &'static str {
    if prefer_float || !crate::model_columns::has_column(ms, VisibilityDataColumn::Data) {
        crate::model_columns::observed_column(ms)
    } else {
        VisibilityDataColumn::Data.name()
    }
}

fn complex_visibility_from_float(values: &ArrayD<f32>) -> ArrayValue {
    ArrayValue::Complex32(values.mapv(|value| Complex32::new(value, 0.)))
}

fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<in-memory>".to_string())
}

fn open_append_file(path: &Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

fn expand_weight_to_spectrum(weight: &ArrayD<f32>, channel_count: usize) -> ArrayD<f32> {
    let mut expanded = ArrayD::from_elem(IxDyn(&[weight.shape()[0], channel_count]).f(), 0.0_f32);
    for corr_index in 0..weight.shape()[0] {
        for chan_index in 0..channel_count {
            expanded[[corr_index, chan_index]] = weight[[corr_index]];
        }
    }
    expanded
}

fn stokes_name(code: i32) -> &'static str {
    match code {
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}

fn correlation_receptors(code: i32) -> Option<(usize, usize)> {
    match code {
        5 | 9 => Some((0, 0)),
        6 | 10 => Some((0, 1)),
        7 | 11 => Some((1, 0)),
        8 | 12 => Some((1, 1)),
        _ => None,
    }
}

fn get_i32(table: &Table, row_index: usize, column: &str) -> Result<i32, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

fn get_f64(table: &Table, row_index: usize, column: &str) -> Result<f64, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Float64(value) => Ok(*value),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

fn get_complex32(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<Complex32, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Complex32(value) => Ok(*value),
        ScalarValue::Complex64(value) => Ok(Complex32::new(value.re as f32, value.im as f32)),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

fn get_string(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<String, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::String(value) => Ok(value.clone()),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

fn get_numeric_array(
    table: &Table,
    row_index: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, ApplyExecutionError> {
    let values = table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        })?;
    match values {
        ArrayValue::Float32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Int32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        other => Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{column} row {row_index} had unexpected array type {:?}",
                other.primitive_type()
            ),
        }),
    }
}

fn get_f64_array(
    table: &Table,
    row_index: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, ApplyExecutionError> {
    let values = table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        })?;
    match values {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Float32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        other => Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{column} row {row_index} had unexpected array type {:?}",
                other.primitive_type()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup::interpolate_gain_amplitude_phase;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    use casa_ms::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};
    use casa_tables::{ArrayShapeContract, ColumnSchema, ColumnType, Table};
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::position::MPosition;
    use casa_types::{
        ArrayValue, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    };
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};
    use tempfile::TempDir;

    use crate::model::{
        CalibrationColumnSummary, CalibrationKeywordSummary, CalibrationSubtableSummary,
        CalibrationTableSummary,
    };

    fn perf_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn row(fields: Vec<RecordField>) -> RecordValue {
        RecordValue::new(fields)
    }

    fn scalar_table(fields: Vec<RecordField>) -> Table {
        Table::from_rows_memory(vec![row(fields)])
    }

    fn assert_complex_close(actual: Complex32, expected: Complex32, tolerance: f32) {
        assert!(
            (actual - expected).norm() <= tolerance,
            "actual={actual:?} expected={expected:?}"
        );
    }

    fn empty_cal_summary(path: PathBuf) -> CalibrationTableSummary {
        CalibrationTableSummary {
            path,
            table_type: "Calibration".to_string(),
            table_subtype: "G Jones".to_string(),
            row_count: 0,
            columns: Vec::new(),
            keywords: CalibrationKeywordSummary {
                par_type: None,
                vis_cal: None,
                ms_name: None,
                pol_basis: None,
                casa_version: None,
            },
            subtables: Vec::<CalibrationSubtableSummary>::new(),
            parameter_family: CalibrationParameterFamily::Complex,
            parameter_column: CalibrationColumnSummary {
                parameter_column: None,
                parameter_primitive_type: None,
                first_cell_shape: None,
            },
            field_ids: Vec::new(),
            spectral_window_ids: Vec::new(),
            antenna1_ids: Vec::new(),
            antenna2_ids: Vec::new(),
            observation_ids: Vec::new(),
            time_coverage: None,
            issues: Vec::new(),
        }
    }

    #[test]
    fn calibration_perf_tracer_from_env_writes_jsonl_and_summary_log() {
        let _guard = perf_env_lock().lock().expect("perf env lock");
        let tempdir = TempDir::new().expect("tempdir");
        unsafe {
            std::env::set_var(PERF_ENV, "1");
            std::env::set_var(PERF_DIR_ENV, tempdir.path());
        }

        let mut tracer = CalibrationPerfTracer::from_env();
        assert!(tracer.is_enabled());
        tracer.write_event(&CalibrationPerfEvent {
            kind: CalibrationPerfEventKind::ApplyCompleted,
            monotonic_ns: 42,
            ms_path: "/tmp/test.ms".to_string(),
            apply_mode: "CalFlag".to_string(),
            selected_row_count: 8,
            calibration_table_count: 1,
            parang: false,
            created_corrected_data_column: true,
            updated_row_count: 8,
            flagged_row_count: 2,
            flagged_sample_count: 5,
            planning_ns: 11,
            planning_selection_ns: 12,
            planning_selected_rows_ns: 13,
            planning_measurement_set_spectral_windows_ns: 14,
            planning_calibration_table_plans_ns: 15,
            open_measurement_set_ns: 16,
            row_field_index_lookup_ns: 17,
            ensure_corrected_data_ns: 18,
            correlation_lookup_ns: 19,
            calibration_load_ns: 20,
            row_loop_ns: 21,
            row_read_total_ns: 22,
            row_fetch_ns: 23,
            row_compute_ns: 24,
            row_read_overhead_ns: 25,
            row_writeback_ns: 26,
            save_ns: 27,
            execute_apply_plan_ns: 28,
            execute_apply_plan_unattributed_ns: 29,
            drop_ns: 30,
            total_ns: 31,
        });
        drop(tracer);

        unsafe {
            std::env::remove_var(PERF_ENV);
            std::env::remove_var(PERF_DIR_ENV);
        }

        let mut json_paths = fs::read_dir(tempdir.path())
            .expect("read perf dir")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .collect::<Vec<_>>();
        json_paths.sort();
        let mut log_paths = fs::read_dir(tempdir.path())
            .expect("read perf dir")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
            .collect::<Vec<_>>();
        log_paths.sort();
        assert_eq!(json_paths.len(), 1);
        assert_eq!(log_paths.len(), 1);

        let json = fs::read_to_string(json_paths[0].clone()).expect("json trace");
        assert!(json.contains("\"kind\":\"apply_completed\""));
        assert!(json.contains("\"row_read_overhead_ns\":25"));

        let log = fs::read_to_string(log_paths[0].clone()).expect("summary log");
        assert!(log.contains("kind=ApplyCompleted"));
        assert!(log.contains("row_read_overhead_ms=0.00"));
    }

    #[test]
    fn calibration_perf_tracer_emits_plan_and_completion_metadata() {
        let _guard = perf_env_lock().lock().expect("perf env lock");
        let tempdir = TempDir::new().expect("tempdir");
        unsafe {
            std::env::set_var(PERF_ENV, "1");
            std::env::set_var(PERF_DIR_ENV, tempdir.path());
        }

        let cal_path = tempdir.path().join("gain.cal");
        let spw_plan = crate::plan::SpectralWindowPlan {
            spw_id: 2,
            num_chan: 3,
            ref_frequency_hz: 1.4e9,
            channel_frequencies_hz: vec![1.399e9, 1.4e9, 1.401e9],
        };
        let plan = ApplyPlan {
            measurement_set_path: Some(tempdir.path().join("input.ms")),
            apply_mode: ApplyMode::CalFlag,
            requires_corrected_data_column: true,
            channel_selections: Default::default(),
            selected_rows: vec![ApplyRowPlan {
                row_index: 4,
                field_id: 5,
                observation_id: 6,
                calibration_observation_id: 6,
                scan_number: 1,
                data_desc_id: 7,
                data_spw_id: 2,
                polarization_id: 8,
                antenna1: 9,
                antenna2: 10,
                feed1: 0,
                feed2: 0,
                time_seconds: 123.0,
                interval_seconds: 2.0,
            }],
            selected_row_count: 1,
            parang: true,
            selected_field_ids: vec![5],
            selected_data_desc_ids: vec![7],
            selected_data_spw_ids: vec![2],
            measurement_set_spectral_windows: vec![spw_plan.clone()],
            calibration_tables: vec![ApplyCalibrationTablePlan {
                applicable_row_indices: None,
                unavailable_row_indices: Vec::new(),
                spec: crate::plan::ApplyCalibrationTableSpec {
                    callibrary: None,
                    path: cal_path.clone(),
                    apply_to: Default::default(),
                    gainfield: None,
                    spwmap: vec![2],
                    interp: ApplyInterpolationMode::Linear,
                    calwt: true,
                },
                applicable_selected_row_count: 1,
                summary: empty_cal_summary(cal_path.clone()),
                resolved_gainfields: Vec::new(),
                resolved_nearest_gainfields: Vec::new(),
                spw_mapping: vec![crate::plan::ApplySpwMapping {
                    data_spw_id: 2,
                    calibration_spw_id: 2,
                }],
                calibration_spectral_windows: vec![spw_plan],
                interp: ApplyInterpolationMode::Linear,
                calwt: true,
            }],
        };
        let summary = ExecuteApplyPlanTraceSummary {
            selected_row_count: 1,
            calibration_table_count: 1,
            parang: true,
            created_corrected_data_column: true,
            updated_row_count: 1,
            flagged_row_count: 0,
            flagged_sample_count: 2,
            row_field_index_lookup_ns: 11,
            ensure_corrected_data_ns: 12,
            correlation_lookup_ns: 13,
            calibration_load_ns: 14,
            row_loop_ns: 15,
            row_read_total_ns: 16,
            row_fetch_ns: 17,
            row_compute_ns: 18,
            row_read_overhead_ns: 19,
            row_writeback_ns: 20,
            save_ns: 21,
            execute_apply_plan_ns: 22,
            execute_apply_plan_unattributed_ns: 23,
        };
        let report = ApplyExecutionReport {
            plan,
            created_corrected_data_column: true,
            wrote_measurement_set: true,
            updated_row_count: 1,
            flagged_row_count: 0,
            flagged_sample_count: 2,
            flag_backup_version: None,
            timings: ApplyExecutionTimings {
                planning_ns: 31,
                planning_selection_ns: 32,
                planning_selected_rows_ns: 33,
                planning_measurement_set_spectral_windows_ns: 34,
                planning_calibration_table_plans_ns: 35,
                open_measurement_set_ns: 36,
                row_field_index_lookup_ns: 37,
                ensure_corrected_data_ns: 38,
                correlation_lookup_ns: 39,
                calibration_load_ns: 40,
                row_compute_ns: 41,
                row_writeback_ns: 42,
                save_ns: 43,
                total_ns: 44,
            },
        };

        let mut tracer = CalibrationPerfTracer::from_env();
        tracer.emit_apply_plan_summary("/tmp/input.ms", &report.plan, summary);
        tracer.emit_apply_completed("/tmp/input.ms", &report, 55, summary);
        drop(tracer);

        unsafe {
            std::env::remove_var(PERF_ENV);
            std::env::remove_var(PERF_DIR_ENV);
        }

        let json_path = fs::read_dir(tempdir.path())
            .expect("read perf dir")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .expect("json trace");
        let json = fs::read_to_string(json_path).expect("json trace");
        assert!(json.contains("\"kind\":\"apply_plan_summary\""));
        assert!(json.contains("\"kind\":\"apply_completed\""));
        assert!(json.contains("\"apply_mode\":\"CalFlag\""));
        assert!(json.contains("\"parang\":true"));
        assert!(json.contains("\"planning_calibration_table_plans_ns\":35"));
        assert!(json.contains("\"drop_ns\":55"));
        assert!(json.contains("\"execute_apply_plan_unattributed_ns\":23"));
    }

    fn default_main_value(column: &ColumnSchema) -> Value {
        match column.column_type() {
            ColumnType::Scalar => match column.data_type().unwrap_or(PrimitiveType::Int32) {
                PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
                PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
                PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
                PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
                _ => Value::Scalar(ScalarValue::Float64(0.0)),
            },
            ColumnType::Record => Value::Record(RecordValue::new(vec![])),
            ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
                let total: usize = shape.iter().product();
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
                ))
            }
            ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
                let shape: Vec<usize> = vec![1; ndim.unwrap_or(1)];
                let total: usize = shape.iter().product();
                match column.data_type().unwrap_or(PrimitiveType::Float64) {
                    PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                    )),
                    PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                    )),
                    _ => Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                    )),
                }
            }
        }
    }

    #[test]
    fn circular_parang_gain_matches_expected_rr_rl_lr_ll_phases() {
        let ant1 = 0.3_f64;
        let ant2 = -0.2_f64;
        let all = vec![5, 6, 7, 8];

        let rr = parallactic_angle_gain(5, 0, &all, ant1, ant2).expect("RR correction");
        let rl = parallactic_angle_gain(6, 0, &all, ant1, ant2).expect("RL correction");
        let lr = parallactic_angle_gain(7, 0, &all, ant1, ant2).expect("LR correction");
        let ll = parallactic_angle_gain(8, 0, &all, ant1, ant2).expect("LL correction");

        assert_eq!(rr, circular_parang_gain(-ant1 + ant2));
        assert_eq!(rl, circular_parang_gain(-ant1 - ant2));
        assert_eq!(lr, circular_parang_gain(ant1 + ant2));
        assert_eq!(ll, circular_parang_gain(ant1 - ant2));
    }

    #[test]
    fn linear_feed_parang_basis_is_rejected() {
        let all = vec![9, 10, 11, 12];
        let err = parallactic_angle_gain(9, 7, &all, 0.1, 0.2).expect_err("linear basis error");
        match err {
            ApplyExecutionError::UnsupportedParallacticAngleBasis {
                data_desc_id,
                correlation_types,
            } => {
                assert_eq!(data_desc_id, 7);
                assert_eq!(correlation_types, vec!["XX", "XY", "YX", "YY"]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn feed_angle_lookup_prefers_exact_spw_then_wildcard_and_nearest_time() {
        let state = ParallacticAngleState {
            engine: MsCalEngine::from_parts(
                vec![MPosition::new_itrf(0.0, 0.0, 0.0)],
                vec![MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)],
                MPosition::new_itrf(0.0, 0.0, 0.0),
                casa_test_support::deterministic_measures_provider(),
            ),
            feed_rows: HashMap::from([(
                (0, 0),
                vec![
                    FeedAngleRow {
                        spectral_window_id: -1,
                        time_seconds: 10.0,
                        interval_seconds: 0.0,
                        receptor0_angle_rad: 0.25,
                    },
                    FeedAngleRow {
                        spectral_window_id: 3,
                        time_seconds: 20.0,
                        interval_seconds: 5.0,
                        receptor0_angle_rad: 0.5,
                    },
                ],
            )]),
        };

        assert_eq!(state.lookup_receptor0_angle(0, 0, 3, 20.0), Some(0.5));
        assert_eq!(state.lookup_receptor0_angle(0, 0, 2, 20.0), Some(0.25));
    }

    #[test]
    fn feed_parallactic_angle_adds_receptor0_angle() {
        let time_seconds = 59000.5 * 86400.0;
        let state = ParallacticAngleState {
            engine: MsCalEngine::from_parts(
                vec![
                    MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9),
                    MPosition::new_itrf(-1_601_085.4, -5_041_977.5, 3_554_875.9),
                ],
                vec![MDirection::from_angles(
                    0.0,
                    std::f64::consts::FRAC_PI_4,
                    DirectionRef::J2000,
                )],
                MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9),
                casa_test_support::deterministic_measures_provider(),
            ),
            feed_rows: HashMap::from([(
                (0, 0),
                vec![FeedAngleRow {
                    spectral_window_id: -1,
                    time_seconds,
                    interval_seconds: 0.0,
                    receptor0_angle_rad: 0.25,
                }],
            )]),
        };

        let base = state
            .engine
            .feed_parallactic_angle(time_seconds, 0, 0)
            .unwrap();
        let feed = state
            .feed_parallactic_angle(time_seconds, 0, 0, 0, 1)
            .unwrap();

        assert!((feed - (base + 0.25)).abs() < 1.0e-12);
    }

    #[test]
    fn row_geometry_cache_reuses_row_dependent_grids_and_fast_terms() {
        let mut cache = RowGeometryCache::default();
        let offset_a = [1_u64, 2, 3];
        let offset_b = [4_u64, 5, 6];
        cache.ensure_fast_term_context(12.5, 3);

        assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), None);
        cache.remember_fast_antpos_delay(0, 2, offset_a, 4.25);
        assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), Some(4.25));
        assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_b), None);

        cache.ensure_fast_term_context(12.5, 4);
        assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), None);

        let antpos = Arc::new(CalibrationGrid::Antpos(AntposGrid {
            offsets_m: [1.0, 2.0, 3.0],
            flagged: false,
        }));
        let gain_curve = Arc::new(CalibrationGrid::GainCurve(GainCurveGrid {
            power: false,
            receptor_count: 1,
            coefficients: ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![1.0_f32, 0.0, 0.0, 0.0])
                .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap(),
        }));
        let opacity = Arc::new(CalibrationGrid::Opacity(OpacityGrid {
            tau: 0.1,
            flagged: true,
        }));
        let complex = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 1,
            channel_count: 1,
            values: ArrayD::from_shape_vec(IxDyn(&[1, 1]).f(), vec![Complex32::new(1.0, 0.0)])
                .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[1, 1]).f(), vec![false]).unwrap(),
        }));

        let antpos_key = materialized_grid_key(&antpos, 3, 2, 12.5, 7).expect("antpos key");
        let gain_curve_key =
            materialized_grid_key(&gain_curve, 3, 2, 12.5, 7).expect("gain curve key");
        let opacity_key = materialized_grid_key(&opacity, 3, 2, 12.5, 7).expect("opacity key");
        assert_eq!(antpos_key.kind, 1);
        assert_eq!(gain_curve_key.kind, 2);
        assert_eq!(opacity_key.kind, 3);
        assert!(materialized_grid_key(&complex, 3, 2, 12.5, 7).is_none());

        assert!(cache.materialized_grid(antpos_key, false).is_none());
        cache.insert_materialized_grid(antpos_key, antpos.clone(), false);
        assert!(Arc::ptr_eq(
            &cache
                .materialized_grid(antpos_key, false)
                .expect("materialized grid"),
            &antpos
        ));

        let lookup_key =
            calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::NearestLinear);
        assert_eq!(lookup_key.interp, 3);
        assert_eq!(
            calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::Nearest).interp,
            1
        );
        assert_eq!(
            calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::Linear).interp,
            2
        );

        let mut load_count = 0;
        let first = cache
            .calibration_lookup(lookup_key, None, || {
                load_count += 1;
                Ok(Some(fields::GridAtTime {
                    grid: gain_curve.clone(),
                    interpolated: false,
                }))
            })
            .expect("first lookup")
            .expect("grid");
        let second = cache
            .calibration_lookup(lookup_key, None, || {
                load_count += 1;
                Ok(Some(fields::GridAtTime {
                    grid: opacity.clone(),
                    interpolated: false,
                }))
            })
            .expect("cached lookup")
            .expect("grid");
        assert_eq!(load_count, 1);
        assert!(Arc::ptr_eq(&first.grid, &second.grid));

        let missing_key = calibration_lookup_key(6, 3, 7, 2, 12.5, ApplyInterpolationMode::Nearest);
        assert!(
            cache
                .calibration_lookup(missing_key, None, || Ok(None))
                .expect("missing lookup")
                .is_none()
        );
        assert!(
            cache
                .calibration_lookup(missing_key, None, || panic!(
                    "cached None should not reload"
                ))
                .expect("cached missing lookup")
                .is_none()
        );
    }

    #[test]
    fn interpolate_time_linear_covers_complex_delay_and_error_cases() {
        let path = Path::new("/tmp/interp.cal");
        let complex_pair = [
            CalibrationSolution {
                time_seconds: 30.0,
                grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(5.0, 4.0), Complex32::new(7.0, 6.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
                })),
            },
            CalibrationSolution {
                time_seconds: 10.0,
                grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                })),
            },
        ];

        match interpolate_time_linear(path, &complex_pair, 20.0)
            .unwrap()
            .as_ref()
        {
            CalibrationGrid::Complex(grid) => {
                assert_complex_close(
                    grid.values[[0, 0]],
                    interpolate_gain_amplitude_phase(
                        Complex32::new(1.0, 0.0),
                        Complex32::new(5.0, 4.0),
                        0.5,
                    ),
                    1.0e-6,
                );
                assert_complex_close(
                    grid.values[[0, 1]],
                    interpolate_gain_amplitude_phase(
                        Complex32::new(3.0, 2.0),
                        Complex32::new(7.0, 6.0),
                        0.5,
                    ),
                    1.0e-6,
                );
                assert!(grid.flags[[0, 0]]);
                assert!(!grid.flags[[0, 1]]);
            }
            _ => panic!("expected complex interpolation"),
        }

        let wrapped = interpolate_gain_amplitude_phase(
            Complex32::new((-3.0_f32).cos(), (-3.0_f32).sin()),
            Complex32::new(3.0_f32.cos(), 3.0_f32.sin()),
            0.5,
        );
        assert!(wrapped.arg().abs() > 3.0);

        let delay_pair = [
            CalibrationSolution {
                time_seconds: 1.0,
                grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 3.0])
                        .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, true]).unwrap(),
                })),
            },
            CalibrationSolution {
                time_seconds: 3.0,
                grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![5.0_f32, 7.0])
                        .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
                })),
            },
        ];
        match interpolate_time_linear(path, &delay_pair, 2.0)
            .unwrap()
            .as_ref()
        {
            CalibrationGrid::Delay(grid) => {
                assert_eq!(grid.values_ns[[0, 0]], 3.0);
                assert_eq!(grid.values_ns[[0, 1]], 5.0);
                assert!(grid.flags[[0, 0]]);
                assert!(grid.flags[[0, 1]]);
            }
            _ => panic!("expected delay interpolation"),
        }

        match interpolate_time_linear(
            path,
            &[CalibrationSolution {
                time_seconds: 10.0,
                grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                })),
            }],
            0.0,
        )
        .unwrap()
        .as_ref()
        {
            CalibrationGrid::Complex(grid) => {
                assert_eq!(grid.values[[0, 0]], Complex32::new(1.0, 0.0))
            }
            _ => panic!("expected complex interpolation"),
        }
        match interpolate_time_linear(path, &[], 0.0) {
            Ok(_) => panic!("expected empty interpolation to fail"),
            Err(ApplyExecutionError::UnsupportedInterpolation { .. }) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
        }
        match interpolate_time_linear(
            path,
            &[
                CalibrationSolution {
                    time_seconds: 10.0,
                    grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                        receptor_count: 1,
                        channel_count: 2,
                        values: ArrayD::from_shape_vec(
                            IxDyn(&[1, 2]).f(),
                            vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                        )
                        .unwrap(),
                        flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false])
                            .unwrap(),
                    })),
                },
                CalibrationSolution {
                    time_seconds: 20.0,
                    grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                        receptor_count: 1,
                        channel_count: 2,
                        values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 2.0])
                            .unwrap(),
                        flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false])
                            .unwrap(),
                    })),
                },
            ],
            15.0,
        ) {
            Ok(_) => panic!("expected mixed-family interpolation to fail"),
            Err(ApplyExecutionError::UnsupportedInterpolation { .. }) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn fast_gain_pair_application_covers_layouts_flags_and_frequency_errors() {
        let scalar_ant1 = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 2,
            channel_count: 1,
            values: ArrayD::from_shape_vec(
                IxDyn(&[2, 1]).f(),
                vec![Complex32::new(2.0, 0.0), Complex32::new(3.0, 0.0)],
            )
            .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![false, false]).unwrap(),
        }));
        let scalar_ant2 = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 2,
            channel_count: 1,
            values: ArrayD::from_shape_vec(
                IxDyn(&[2, 1]).f(),
                vec![Complex32::new(4.0, 0.0), Complex32::new(5.0, 0.0)],
            )
            .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![false, false]).unwrap(),
        }));
        let channel_ant1 = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 2,
            channel_count: 2,
            values: ArrayD::from_shape_vec(
                IxDyn(&[2, 2]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(2.0, 0.0),
                    Complex32::new(3.0, 0.0),
                    Complex32::new(4.0, 0.0),
                ],
            )
            .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, false, false, true])
                .unwrap(),
        }));
        let channel_ant2 = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 2,
            channel_count: 2,
            values: ArrayD::from_shape_vec(
                IxDyn(&[2, 2]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(1.0, 0.0),
                    Complex32::new(1.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
        }));

        let mut corrected =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![Complex32::new(80.0, 0.0); 4]).unwrap();
        let mut flags =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![false, false, false, false]).unwrap();
        let mut newly_flagged_samples = 0;
        apply_complex_gain_pairs_fast(
            &[
                FastGainPair::Complex(scalar_ant1.clone(), scalar_ant2.clone()),
                FastGainPair::Complex(channel_ant1.clone(), channel_ant2.clone()),
            ],
            &mut ApplyBuffer {
                data_desc_id: 0,
                correlation_types: &[5, 8],
                data_channels: DataChannelGrid::all(&[1.0e9, 1.1e9]),
                corrected: &mut corrected,
                flags: &mut flags,
                propagate_flags: true,
                newly_flagged_samples: &mut newly_flagged_samples,
            },
        )
        .unwrap();
        assert_complex_close(corrected[[0, 0]], Complex32::new(10.0, 0.0), 1.0e-6);
        assert_complex_close(corrected[[1, 0]], Complex32::new(80.0 / 30.0, 0.0), 1.0e-6);
        assert_complex_close(corrected[[0, 1]], Complex32::new(80.0 / 24.0, 0.0), 1.0e-6);
        assert!(flags[[1, 1]]);
        assert_eq!(newly_flagged_samples, 1);

        let mut fortran_corrected =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![Complex32::new(90.0, 0.0); 4]).unwrap();
        let mut fortran_flags =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, false, true, false]).unwrap();
        let mut fortran_new_flags = 0;
        apply_complex_gain_pairs_fast(
            &[
                FastGainPair::GainCurve {
                    ant1: FastReceptorGains {
                        receptor_count: 2,
                        values: [2.0, 3.0, 0.0, 0.0],
                        flags: [false, false, false, false],
                    },
                    ant2: FastReceptorGains {
                        receptor_count: 2,
                        values: [5.0, 7.0, 0.0, 0.0],
                        flags: [false, false, false, false],
                    },
                },
                FastGainPair::Opacity {
                    denom: Complex32::new(0.5, 0.0),
                    flagged: false,
                },
            ],
            &mut ApplyBuffer {
                data_desc_id: 0,
                correlation_types: &[5, 8],
                data_channels: DataChannelGrid::all(&[1.0e9, 1.1e9]),
                corrected: &mut fortran_corrected,
                flags: &mut fortran_flags,
                propagate_flags: true,
                newly_flagged_samples: &mut fortran_new_flags,
            },
        )
        .unwrap();
        assert_complex_close(fortran_corrected[[0, 0]], Complex32::new(18.0, 0.0), 1.0e-6);
        assert_complex_close(
            fortran_corrected[[1, 0]],
            Complex32::new(90.0 / 21.0 / 0.5, 0.0),
            1.0e-6,
        );
        assert_eq!(fortran_new_flags, 0);

        let mut antpos_corrected =
            ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![Complex32::new(1.0, 0.0); 2]).unwrap();
        let mut antpos_flags = ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![false; 2]).unwrap();
        let mut antpos_new_flags = 0;
        assert!(matches!(
            apply_complex_gain_pairs_fast(
                &[FastGainPair::Antpos {
                    delay_delta_ns: 1.0,
                    flagged: false,
                }],
                &mut ApplyBuffer {
                    data_desc_id: 0,
                    correlation_types: &[5],
                    data_channels: DataChannelGrid::all(&[1.0e9]),
                    corrected: &mut antpos_corrected,
                    flags: &mut antpos_flags,
                    propagate_flags: false,
                    newly_flagged_samples: &mut antpos_new_flags,
                },
            )
            .unwrap_err(),
            ApplyExecutionError::UnsupportedInterpolation { .. }
        ));

        assert!(matches!(
            fast_pair_receptors(9, &[42], 1).unwrap_err(),
            ApplyExecutionError::UnsupportedCorrelationLayout { .. }
        ));
    }

    #[test]
    fn bpoly_helpers_and_typed_accessors_cover_branchy_paths() {
        let path = Path::new("/tmp/bpoly.cal");
        assert_eq!(
            infer_bpoly_receptor_count(4, 2, 2, path, 0, "AMP").unwrap(),
            2
        );
        assert_eq!(
            infer_bpoly_receptor_count(0, 0, 0, path, 0, "AMP").unwrap(),
            1
        );
        assert!(infer_bpoly_receptor_count(5, 2, 3, path, 0, "AMP").is_err());

        assert_eq!(
            split_bpoly_coefficients(vec![1.0, 2.0, 3.0, 4.0], 2, 2, path, 1, "AMP").unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );
        assert!(split_bpoly_coefficients(vec![1.0, 2.0], 2, 2, path, 1, "AMP").is_err());

        assert_eq!(legacy_bpoly_chebyshev_value(&[], 0.0, 1.0, 0.5), 0.0);
        assert_eq!(legacy_bpoly_chebyshev_value(&[4.0], 0.0, 1.0, 0.5), 2.0);
        assert!((legacy_bpoly_chebyshev_value(&[2.0, 1.0], 0.0, 10.0, 10.0) - 2.0).abs() < 1.0e-12);

        let table = scalar_table(vec![
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(12.5))),
            RecordField::new(
                "SCALE_FACTOR",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(1.0, -2.0))),
            ),
            RecordField::new(
                "PHASE_UNITS",
                Value::Scalar(ScalarValue::String("DEG".to_string())),
            ),
            RecordField::new(
                "F32S",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![5.0_f32, 6.0]).unwrap(),
                )),
            ),
            RecordField::new(
                "I32S",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![3_i32, 4]).unwrap(),
                )),
            ),
        ]);
        assert_eq!(get_i32(&table, 0, "FIELD_ID").unwrap(), 7);
        assert_eq!(get_f64(&table, 0, "TIME").unwrap(), 12.5);
        assert_eq!(
            get_complex32(&table, 0, "SCALE_FACTOR").unwrap(),
            Complex32::new(1.0, -2.0)
        );
        assert_eq!(get_string(&table, 0, "PHASE_UNITS").unwrap(), "DEG");
        assert_eq!(
            get_numeric_array(&table, 0, "I32S", path).unwrap(),
            vec![3.0, 4.0]
        );
        assert_eq!(
            get_f64_array(&table, 0, "F32S", path).unwrap(),
            vec![5.0, 6.0]
        );
        assert!(matches!(
            get_i32(&table, 0, "PHASE_UNITS").unwrap_err(),
            ApplyExecutionError::UnsupportedParameterShape { .. }
        ));

        let cal_desc = Table::from_rows_memory(vec![
            row(vec![
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![9_i32]).unwrap(),
                    )),
                ),
                RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(2))),
            ]),
            row(vec![
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![3_i32]).unwrap(),
                    )),
                ),
                RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(1))),
            ]),
        ]);
        let entries = load_bpoly_cal_desc_map(path, &cal_desc).unwrap();
        assert_eq!(entries.get(&0).unwrap().spw_id, 9);
        assert_eq!(entries.get(&1).unwrap().receptor_count, 1);
        let invalid_cal_desc = Table::from_rows_memory(vec![row(vec![
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![3_i32]).unwrap(),
                )),
            ),
            RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(-1))),
        ])]);
        assert!(load_bpoly_cal_desc_map(path, &invalid_cal_desc).is_err());

        assert_eq!(weights::lower_median(vec![3.0, 1.0, 2.0]), 2.0);
        assert_eq!(weights::lower_median(vec![4.0, 1.0, 3.0, 2.0]), 2.0);
        assert_eq!(
            expand_weight_to_spectrum(
                &ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.5_f32, 2.5]).unwrap(),
                2,
            )
            .iter()
            .copied()
            .collect::<Vec<_>>(),
            vec![1.5, 1.5, 2.5, 2.5]
        );
        assert_eq!(stokes_name(5), "RR");
        assert_eq!(stokes_name(99), "??");
        assert_eq!(correlation_receptors(10), Some((0, 1)));
        assert_eq!(correlation_receptors(42), None);
    }

    #[test]
    fn sample_bpoly_row_and_corrected_data_column_cover_remaining_helpers() {
        let bpoly = scalar_table(vec![
            RecordField::new(
                COL_SCALE_FACTOR,
                Value::Scalar(ScalarValue::Complex32(Complex32::new(2.0, 0.0))),
            ),
            RecordField::new(
                COL_VALID_DOMAIN,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.0_f64, 3.0]).unwrap(),
                )),
            ),
            RecordField::new(COL_N_POLY_AMP, Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(COL_N_POLY_PHASE, Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                COL_POLY_COEFF_AMP,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 0.0]).unwrap(),
                )),
            ),
            RecordField::new(
                COL_POLY_COEFF_PHASE,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 90.0]).unwrap(),
                )),
            ),
            RecordField::new(
                COL_PHASE_UNITS,
                Value::Scalar(ScalarValue::String("DEG".to_string())),
            ),
        ]);
        let plan = crate::plan::SpectralWindowPlan {
            spw_id: 5,
            num_chan: 3,
            ref_frequency_hz: 1.2e9,
            channel_frequencies_hz: vec![0.5, 1.5, 2.5],
        };
        assert_eq!(cal_spw_reference_frequency_hz(&plan), 1.5);
        match sample_bpoly_row(
            &bpoly,
            0,
            Path::new("/tmp/bpoly"),
            &plan,
            LegacyCalDescEntry {
                spw_id: 5,
                receptor_count: 2,
            },
            true,
        )
        .unwrap()
        {
            CalibrationGrid::Complex(grid) => {
                assert_eq!(grid.receptor_count, 2);
                assert_eq!(grid.channel_count, 3);
                assert_eq!(grid.values[[0, 0]], Complex32::new(1.0, 0.0));
                assert!((grid.values[[0, 1]].norm() - 2.0).abs() < 1.0e-5);
                assert_ne!(grid.values[[0, 1]], Complex32::new(1.0, 0.0));
            }
            _ => panic!("expected complex BPOLY output"),
        }

        let bad_phase_units = scalar_table(vec![
            RecordField::new(
                COL_SCALE_FACTOR,
                Value::Scalar(ScalarValue::Complex32(Complex32::new(2.0, 0.0))),
            ),
            RecordField::new(
                COL_VALID_DOMAIN,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.0_f64, 3.0]).unwrap(),
                )),
            ),
            RecordField::new(COL_N_POLY_AMP, Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(COL_N_POLY_PHASE, Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                COL_POLY_COEFF_AMP,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 0.0]).unwrap(),
                )),
            ),
            RecordField::new(
                COL_POLY_COEFF_PHASE,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 90.0]).unwrap(),
                )),
            ),
            RecordField::new(
                COL_PHASE_UNITS,
                Value::Scalar(ScalarValue::String("TURNS".to_string())),
            ),
        ]);
        assert!(
            sample_bpoly_row(
                &bad_phase_units,
                0,
                Path::new("/tmp/bpoly"),
                &plan,
                LegacyCalDescEntry {
                    spw_id: 5,
                    receptor_count: 2,
                },
                true,
            )
            .is_err()
        );

        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        let schema = ms.main_table().schema().unwrap().clone();
        let fields = schema
            .columns()
            .iter()
            .map(|column| {
                if column.name() == VisibilityDataColumn::Data.name() {
                    RecordField::new(
                        column.name(),
                        Value::Array(ArrayValue::Complex32(
                            ArrayD::from_shape_vec(
                                IxDyn(&[2, 1]).f(),
                                vec![Complex32::new(1.0, 0.0), Complex32::new(0.0, 1.0)],
                            )
                            .unwrap(),
                        )),
                    )
                } else {
                    RecordField::new(column.name(), default_main_value(column))
                }
            })
            .collect::<Vec<_>>();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();

        assert_eq!(
            ensure_corrected_data_column(&mut ms, "DATA").unwrap(),
            CorrectedDataCreation { created: true }
        );
        assert!(
            ms.main_table()
                .schema()
                .unwrap()
                .contains_column(VisibilityDataColumn::CorrectedData.name())
        );
        assert!(
            ms.main_table()
                .row_accessor()
                .row(0)
                .unwrap()
                .get(VisibilityDataColumn::CorrectedData.name())
                .is_some()
        );
        assert_eq!(
            ensure_corrected_data_column(&mut ms, "DATA").unwrap(),
            CorrectedDataCreation { created: false }
        );
        assert_eq!(display_ms_path(&ms), "<in-memory>");
    }
}
