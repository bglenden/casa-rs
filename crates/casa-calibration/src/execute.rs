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
//! - narrow float `FPARAM` support for `K Jones` delay tables
//! - legacy `BPOLY` bandpass tables expanded onto the target SPW grid
//! - diagonal `G` / `T`-style application to `RR/RL/LR/LL` and `XX/XY/YX/YY`
//! - `ApplyMode::Trial`, `ApplyMode::CalOnly`, and `ApplyMode::CalFlag`
//! - automatic `CORRECTED_DATA` creation when absent
//!
//! Explicitly deferred from this executor cut:
//!
//! - non-diagonal Jones terms / polarization leakage
//! - linear-feed parallactic-angle rotation, which requires general Jones support
//! - solver output beyond the existing planner surface

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

use casa_ms::column_def::build_table_schema;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{MsError, MsResult};
use casa_tables::{ColumnSchema, Table, TableError, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_CAL_DESC_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM, COL_N_POLY_AMP,
    COL_N_POLY_PHASE, COL_PHASE_UNITS, COL_POLY_COEFF_AMP, COL_POLY_COEFF_PHASE, COL_SCALE_FACTOR,
    COL_SPECTRAL_WINDOW_ID, COL_TIME, COL_VALID_DOMAIN, LEGACY_CAL_DESC_KEYWORD,
};
use crate::model::CalibrationParameterFamily;
use crate::plan::{
    ApplyCalibrationTablePlan, ApplyInterpolationMode, ApplyMode, ApplyPlan, ApplyPlanError,
    ApplyPlanRequest, ApplyRowPlan, plan_apply_with_timings,
};

const PERF_ENV: &str = "CASA_RS_CALIBRATION_PERF";
const PERF_DIR_ENV: &str = "CASA_RS_CALIBRATION_PERF_DIR";

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
    if let Some(detail) = detail.into() {
        if !detail.is_empty() {
            line.push(' ');
            line.push_str(&detail);
        }
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
    /// Number of selected rows newly marked `FLAG_ROW=true`.
    pub flagged_row_count: usize,
    /// Number of individual correlation-channel samples flagged by calibration.
    pub flagged_sample_count: usize,
    /// Timing breakdown for the apply workflow, in nanoseconds.
    pub timings: ApplyExecutionTimings,
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

    /// `parang` currently supports only circular-feed correlation layouts.
    #[error(
        "parallactic-angle application currently supports only circular-feed layouts for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
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
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let loaded_tables = plan
        .calibration_tables
        .iter()
        .map(load_calibration_table)
        .collect::<Result<Vec<_>, _>>()?;
    let parang_state = if plan.parang {
        Some(load_parallactic_angle_state(ms).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source,
            }
        })?)
    } else {
        None
    };

    let mut evaluated_rows = HashMap::new();
    let mut main_rows = ms
        .main_table()
        .row_accessor()
        .prepare(&[VisibilityDataColumn::Data.name(), "FLAG"])
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source: MsError::from(source),
        })?;
    let data_index = main_rows
        .column_index(VisibilityDataColumn::Data.name())
        .expect("prepared apply reader includes DATA");
    let flag_index = main_rows
        .column_index("FLAG")
        .expect("prepared apply reader includes FLAG");
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
        let data = main_rows.array_at(data_index).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            }
        })?;
        let original_flags = main_rows.array_at(flag_index).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            }
        })?;

        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types,
                data,
                original_flags,
                original_weight: None,
                has_weight_spectrum: false,
            },
            plan,
            &loaded_tables,
            parang_state.as_ref(),
        )?;
        evaluated_rows.insert(
            row.row_index,
            EvaluatedApplyRow {
                corrected_data: result.corrected_data,
                flags: result
                    .updated_flags
                    .unwrap_or_else(|| original_flags.clone()),
            },
        );
    }

    Ok(evaluated_rows)
}

/// Plan and execute calibration application against an on-disk MeasurementSet.
pub fn execute_apply_from_path(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let path = path.as_ref().to_path_buf();
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
        let open_measurement_set_ns = open_started_at.elapsed().as_nanos() as u64;
        let planning_started_at = Instant::now();
        let (plan, plan_timings) = plan_apply_with_timings(&ms, request)?;
        let planning_ns = planning_started_at.elapsed().as_nanos() as u64;
        if plan.apply_mode == ApplyMode::Trial {
            return Ok(ApplyExecutionReport {
                plan,
                created_corrected_data_column: false,
                wrote_measurement_set: false,
                updated_row_count: 0,
                flagged_row_count: 0,
                flagged_sample_count: 0,
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

        let (mut report, trace_summary) =
            execute_apply_plan(&mut ms, plan, Some(&mut perf_tracer))?;
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
        return Ok(ApplyExecutionReport {
            plan,
            created_corrected_data_column: false,
            wrote_measurement_set: false,
            updated_row_count: 0,
            flagged_row_count: 0,
            flagged_sample_count: 0,
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
    let (mut report, trace_summary) = execute_apply_plan(ms, plan, Some(&mut perf_tracer))?;
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

fn execute_apply_plan(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
    perf_tracer: Option<&mut CalibrationPerfTracer>,
) -> Result<(ApplyExecutionReport, ExecuteApplyPlanTraceSummary), ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let execute_apply_plan_started_at = Instant::now();
    let ensure_corrected_data_started_at = Instant::now();
    let created_corrected_data_column = ensure_corrected_data_column(ms).map_err(|source| {
        ApplyExecutionError::CreateCorrectedData {
            path: ms_path.clone(),
            source,
        }
    })?;
    let ensure_corrected_data_ns = ensure_corrected_data_started_at.elapsed().as_nanos() as u64;

    let correlation_lookup_started_at = Instant::now();
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let correlation_lookup_ns = correlation_lookup_started_at.elapsed().as_nanos() as u64;
    let calibration_load_started_at = Instant::now();
    let loaded_tables = plan
        .calibration_tables
        .iter()
        .map(load_calibration_table)
        .collect::<Result<Vec<_>, _>>()?;
    let parang_state = if plan.parang {
        Some(load_parallactic_angle_state(ms).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source,
            }
        })?)
    } else {
        None
    };
    let calibration_load_ns = calibration_load_started_at.elapsed().as_nanos() as u64;

    let mut updated_row_count = 0;
    let mut flagged_row_count = 0;
    let mut flagged_sample_count = 0;
    let mut row_read_total_ns = 0_u64;
    let mut row_fetch_ns = 0_u64;
    let mut row_compute_ns = 0_u64;
    let mut row_writeback_ns = 0_u64;
    let any_calwt = plan
        .calibration_tables
        .iter()
        .zip(&loaded_tables)
        .any(|(table, loaded)| table.calwt && loaded.supports_calwt);
    let has_weight_spectrum = ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
    let use_partial_main_save = !created_corrected_data_column;
    let mut changed_columns: Vec<&'static str> = vec![VisibilityDataColumn::CorrectedData.name()];
    let row_loop_started_at = Instant::now();
    if use_partial_main_save {
        let anticipated_updates = plan.selected_rows.len();
        ms.main_table_mut().reserve_array_cell_updates(
            VisibilityDataColumn::CorrectedData.name(),
            anticipated_updates,
        );
        ms.main_table_mut()
            .reserve_array_cell_updates("FLAG", anticipated_updates);
        ms.main_table_mut()
            .reserve_array_cell_updates("WEIGHT", anticipated_updates);
        ms.main_table_mut()
            .reserve_array_cell_updates("WEIGHT_SPECTRUM", anticipated_updates);
    }

    let prefetched_inputs = {
        let selected_row_indices: Vec<usize> =
            plan.selected_rows.iter().map(|row| row.row_index).collect();
        let row_read_started_at = Instant::now();
        let row_fetch_started_at = Instant::now();
        let data_values = ms
            .main_table()
            .get_array_cells_owned(VisibilityDataColumn::Data.name(), &selected_row_indices)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;
        let flag_values = ms
            .main_table()
            .get_array_cells_owned("FLAG", &selected_row_indices)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;
        let weight_values = if any_calwt {
            Some(
                ms.main_table()
                    .get_array_cells_owned("WEIGHT", &selected_row_indices)
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
        let mut prefetched_inputs = Vec::with_capacity(plan.selected_rows.len());

        for (prefetch_idx, row) in plan.selected_rows.iter().enumerate() {
            let data = data_values[prefetch_idx].clone().ok_or_else(|| {
                ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(TableError::ColumnNotFound {
                        row_index: row.row_index,
                        column: VisibilityDataColumn::Data.name().to_string(),
                    }),
                }
            })?;
            let original_flags = flag_values[prefetch_idx].clone().ok_or_else(|| {
                ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(TableError::ColumnNotFound {
                        row_index: row.row_index,
                        column: "FLAG".to_string(),
                    }),
                }
            })?;
            let original_weight = weight_values
                .as_ref()
                .map(|weights| {
                    weights[prefetch_idx].clone().ok_or_else(|| {
                        ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(TableError::ColumnNotFound {
                                row_index: row.row_index,
                                column: "WEIGHT".to_string(),
                            }),
                        }
                    })
                })
                .transpose()?;
            prefetched_inputs.push(PrefetchedExecutionRowInputs {
                data,
                original_flags,
                original_weight,
            });
        }

        prefetched_inputs
    };

    let row_field_index_lookup_ns;
    if use_partial_main_save {
        row_field_index_lookup_ns = 0;
        for (row, prefetched_inputs) in plan.selected_rows.iter().zip(&prefetched_inputs) {
            let correlation_types = correlation_types_by_ddid
                .get(&row.data_desc_id)
                .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: Vec::new(),
                })?;
            let row_compute_started_at = Instant::now();
            let ExecutionRowResult {
                corrected_data,
                updated_flags,
                updated_weight,
                updated_weight_spectrum,
                newly_flagged_samples,
                row_became_fully_flagged,
            } = apply_row_prefetched(
                row,
                correlation_types,
                prefetched_inputs,
                any_calwt && has_weight_spectrum,
                &plan,
                &loaded_tables,
                parang_state.as_ref(),
            )?;
            row_compute_ns += row_compute_started_at.elapsed().as_nanos() as u64;

            let row_writeback_started_at = Instant::now();
            ms.main_table_mut()
                .set_array_cell_assuming_valid(
                    row.row_index,
                    VisibilityDataColumn::CorrectedData.name(),
                    corrected_data,
                )
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                })?;

            if let Some(updated_flags) = updated_flags {
                if !changed_columns.contains(&"FLAG") {
                    changed_columns.push("FLAG");
                }
                ms.main_table_mut()
                    .set_array_cell_assuming_valid(row.row_index, "FLAG", updated_flags)
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: ms_path.clone(),
                        source: MsError::from(source),
                    })?;
                if row_became_fully_flagged {
                    if !changed_columns.contains(&"FLAG_ROW") {
                        changed_columns.push("FLAG_ROW");
                    }
                    ms.main_table_mut()
                        .set_scalar_cell_assuming_valid(
                            row.row_index,
                            "FLAG_ROW",
                            ScalarValue::Bool(true),
                        )
                        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(source),
                        })?;
                    flagged_row_count += 1;
                }
                flagged_sample_count += newly_flagged_samples;
            }
            if let Some(updated_weight) = updated_weight {
                if !changed_columns.contains(&"WEIGHT") {
                    changed_columns.push("WEIGHT");
                }
                ms.main_table_mut()
                    .set_array_cell_assuming_valid(row.row_index, "WEIGHT", updated_weight)
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: ms_path.clone(),
                        source: MsError::from(source),
                    })?;
            }
            if let Some(updated_weight_spectrum) = updated_weight_spectrum {
                if !changed_columns.contains(&"WEIGHT_SPECTRUM") {
                    changed_columns.push("WEIGHT_SPECTRUM");
                }
                ms.main_table_mut()
                    .set_array_cell_assuming_valid(
                        row.row_index,
                        "WEIGHT_SPECTRUM",
                        updated_weight_spectrum,
                    )
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: ms_path.clone(),
                        source: MsError::from(source),
                    })?;
            }
            updated_row_count += 1;
            row_writeback_ns += row_writeback_started_at.elapsed().as_nanos() as u64;
        }
    } else {
        let row_field_index_lookup_started_at = Instant::now();
        let mut prepared_columns = vec![VisibilityDataColumn::CorrectedData.name()];
        if ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("FLAG"))
        {
            prepared_columns.push("FLAG");
        }
        if ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("FLAG_ROW"))
        {
            prepared_columns.push("FLAG_ROW");
        }
        if ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT"))
        {
            prepared_columns.push("WEIGHT");
        }
        if has_weight_spectrum {
            prepared_columns.push("WEIGHT_SPECTRUM");
        }
        let mut prepared_main_rows = ms
            .main_table_mut()
            .row_accessor_mut()
            .prepare(&prepared_columns)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;
        let row_field_indices = ApplyRowFieldIndices {
            corrected_data: prepared_main_rows
                .column_index(VisibilityDataColumn::CorrectedData.name()),
            flag: prepared_main_rows.column_index("FLAG"),
            flag_row: prepared_main_rows.column_index("FLAG_ROW"),
            weight: prepared_main_rows.column_index("WEIGHT"),
            weight_spectrum: prepared_main_rows.column_index("WEIGHT_SPECTRUM"),
        };
        row_field_index_lookup_ns = row_field_index_lookup_started_at.elapsed().as_nanos() as u64;

        for (row, prefetched_inputs) in plan.selected_rows.iter().zip(&prefetched_inputs) {
            let correlation_types = correlation_types_by_ddid
                .get(&row.data_desc_id)
                .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: Vec::new(),
                })?;
            let row_compute_started_at = Instant::now();
            let ExecutionRowResult {
                corrected_data,
                updated_flags,
                updated_weight,
                updated_weight_spectrum,
                newly_flagged_samples,
                row_became_fully_flagged,
            } = apply_row_prefetched(
                row,
                correlation_types,
                prefetched_inputs,
                any_calwt && has_weight_spectrum,
                &plan,
                &loaded_tables,
                parang_state.as_ref(),
            )?;
            row_compute_ns += row_compute_started_at.elapsed().as_nanos() as u64;

            let row_writeback_started_at = Instant::now();
            prepared_main_rows.seek(row.row_index).map_err(|source| {
                ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                }
            })?;
            if let Some(slot_index) = row_field_indices.corrected_data {
                prepared_main_rows
                    .set_value_at(slot_index, Value::Array(corrected_data))
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: ms_path.clone(),
                        source: MsError::from(source),
                    })?;
            }

            if let Some(updated_flags) = updated_flags {
                if let Some(slot_index) = row_field_indices.flag {
                    prepared_main_rows
                        .set_value_at(slot_index, Value::Array(updated_flags))
                        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(source),
                        })?;
                }
                if row_became_fully_flagged {
                    if let Some(slot_index) = row_field_indices.flag_row {
                        prepared_main_rows
                            .set_value_at(slot_index, Value::Scalar(ScalarValue::Bool(true)))
                            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::from(source),
                            })?;
                    }
                    flagged_row_count += 1;
                }
                flagged_sample_count += newly_flagged_samples;
            }
            if let Some(updated_weight) = updated_weight {
                if let Some(slot_index) = row_field_indices.weight {
                    prepared_main_rows
                        .set_value_at(slot_index, Value::Array(updated_weight))
                        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(source),
                        })?;
                }
            }
            if let Some(updated_weight_spectrum) = updated_weight_spectrum {
                if let Some(slot_index) = row_field_indices.weight_spectrum {
                    prepared_main_rows
                        .set_value_at(slot_index, Value::Array(updated_weight_spectrum))
                        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(source),
                        })?;
                }
            }
            updated_row_count += 1;
            row_writeback_ns += row_writeback_started_at.elapsed().as_nanos() as u64;
        }

        prepared_main_rows
            .flush()
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;
    }
    let row_loop_ns = row_loop_started_at.elapsed().as_nanos() as u64;

    let save_started_at = Instant::now();
    if use_partial_main_save {
        let changed_row_indices: Vec<usize> =
            plan.selected_rows.iter().map(|row| row.row_index).collect();
        ms.main_table()
            .save_selected_rows_in_place_assuming_valid(&changed_columns, &changed_row_indices)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;
    } else {
        ms.save_main_table_only_assuming_valid().map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source,
            }
        })?;
    }
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
    }

    Ok((
        ApplyExecutionReport {
            plan,
            created_corrected_data_column,
            wrote_measurement_set: true,
            updated_row_count,
            flagged_row_count,
            flagged_sample_count,
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

struct ExecutionRowInputs<'a> {
    correlation_types: &'a [i32],
    data: &'a ArrayValue,
    original_flags: &'a ArrayValue,
    original_weight: Option<&'a ArrayValue>,
    has_weight_spectrum: bool,
}

struct PrefetchedExecutionRowInputs {
    data: ArrayValue,
    original_flags: ArrayValue,
    original_weight: Option<ArrayValue>,
}

struct ParallacticAngleState {
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

fn apply_row(
    row: &ApplyRowPlan,
    inputs: ExecutionRowInputs<'_>,
    plan: &ApplyPlan,
    loaded_tables: &[LoadedCalibrationTable],
    parang_state: Option<&ParallacticAngleState>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    let ExecutionRowInputs {
        correlation_types,
        data,
        original_flags,
        original_weight,
        has_weight_spectrum,
    } = inputs;
    let ArrayValue::Complex32(data) = data else {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set DATA>".to_string(),
            shape: data.shape().to_vec(),
        });
    };
    let ArrayValue::Bool(flag_array) = original_flags else {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set FLAG>".to_string(),
            shape: original_flags.shape().to_vec(),
        });
    };
    if data.ndim() != 2 || flag_array.ndim() != 2 || data.shape() != flag_array.shape() {
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
    let mut flags = flag_array.clone();
    let mut newly_flagged_samples = 0;
    let any_calwt = plan.calibration_tables.iter().any(|table| table.calwt);
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
            weight_spectrum = Some(expand_weight_to_spectrum(weight_values, data.shape()[1]));
        }
    }

    for (table_plan, loaded_table) in plan.calibration_tables.iter().zip(loaded_tables) {
        if !table_plan.spec.apply_to.matches(row) {
            continue;
        }
        let cal_spw_id = table_plan
            .spw_mapping
            .iter()
            .find(|mapping| mapping.data_spw_id == row.data_spw_id)
            .map(|mapping| mapping.calibration_spw_id)
            .expect("planner guarantees spw mapping for selected rows");
        let data_spw = plan
            .measurement_set_spectral_windows
            .iter()
            .find(|spw| spw.spw_id == row.data_spw_id)
            .expect("planner guarantees selected MS spectral windows");
        let cal_spw = table_plan
            .calibration_spectral_windows
            .iter()
            .find(|spw| spw.spw_id == cal_spw_id)
            .expect("planner guarantees mapped caltable spectral windows");

        let field_id = table_plan
            .resolved_nearest_gainfields
            .iter()
            .find(|mapping| mapping.measurement_set_field_id == row.field_id)
            .map(|mapping| mapping.calibration_field_id)
            .or_else(|| {
                table_plan
                    .resolved_gainfield
                    .as_ref()
                    .map(|field| field.field_id)
            })
            .unwrap_or(row.field_id);

        let ant1 = loaded_table.lookup(
            field_id,
            cal_spw_id,
            row.antenna1,
            row.time_seconds,
            table_plan.interp,
        )?;
        let ant2 = loaded_table.lookup(
            field_id,
            cal_spw_id,
            row.antenna2,
            row.time_seconds,
            table_plan.interp,
        )?;

        let (Some(ant1), Some(ant2)) = (ant1, ant2) else {
            if plan.apply_mode == ApplyMode::CalFlag {
                for corr_index in 0..data.shape()[0] {
                    for chan_index in 0..data.shape()[1] {
                        if !flags[[corr_index, chan_index]] {
                            flags[[corr_index, chan_index]] = true;
                            newly_flagged_samples += 1;
                        }
                    }
                }
            }
            continue;
        };

        let sampling_context = CalibrationSamplingContext {
            data_frequencies_hz: &data_spw.channel_frequencies_hz,
            cal_frequencies_hz: &cal_spw.channel_frequencies_hz,
            cal_ref_frequency_hz: cal_spw_reference_frequency_hz(cal_spw),
            interp: table_plan.interp,
            path: &loaded_table.path,
        };

        for corr_index in 0..data.shape()[0] {
            let receptors =
                correlation_receptors(correlation_types[corr_index]).ok_or_else(|| {
                    ApplyExecutionError::UnsupportedCorrelationLayout {
                        data_desc_id: row.data_desc_id,
                        correlation_types: correlation_types
                            .iter()
                            .map(|code| stokes_name(*code).to_string())
                            .collect(),
                    }
                })?;

            for chan_index in 0..data.shape()[1] {
                let gain1 = ant1.sample(receptors.0, chan_index, &sampling_context)?;
                let gain2 = ant2.sample(receptors.1, chan_index, &sampling_context)?;

                if gain1.flagged
                    || gain2.flagged
                    || gain1.value == Complex32::new(0.0, 0.0)
                    || gain2.value == Complex32::new(0.0, 0.0)
                {
                    if plan.apply_mode == ApplyMode::CalFlag && !flags[[corr_index, chan_index]] {
                        flags[[corr_index, chan_index]] = true;
                        newly_flagged_samples += 1;
                    }
                    continue;
                }

                let denom = gain1.value * gain2.value.conj();
                corrected[[corr_index, chan_index]] /= denom;
                if table_plan.calwt && loaded_table.supports_calwt {
                    let factor = gain1.value.norm_sqr() * gain2.value.norm_sqr();
                    if let Some(weight_spectrum) = weight_spectrum.as_mut() {
                        weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                    if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_mut() {
                        implicit_weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                }
            }
        }
    }

    if let Some(parang_state) = parang_state {
        let ant1_feed_pa = parang_state.feed_parallactic_angle(
            row.time_seconds,
            row.field_id,
            row.antenna1,
            row.feed1,
            row.data_spw_id,
        )?;
        let ant2_feed_pa = parang_state.feed_parallactic_angle(
            row.time_seconds,
            row.field_id,
            row.antenna2,
            row.feed2,
            row.data_spw_id,
        )?;

        for corr_index in 0..data.shape()[0] {
            let correction = parallactic_angle_gain(
                correlation_types[corr_index],
                row.data_desc_id,
                correlation_types,
                ant1_feed_pa,
                ant2_feed_pa,
            )?;
            for chan_index in 0..data.shape()[1] {
                corrected[[corr_index, chan_index]] /= correction;
            }
        }
    }

    if any_calwt {
        let weight_values = weight
            .as_mut()
            .expect("validated WEIGHT availability when calwt is enabled");
        if let Some(weight_spectrum_values) = weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..weight_spectrum_values.shape()[1])
                    .map(|chan_index| weight_spectrum_values[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] = median_f32(&samples);
            }
        } else if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..implicit_weight_spectrum.shape()[1])
                    .map(|chan_index| implicit_weight_spectrum[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] *= median_f32(&samples);
            }
        }
    }

    let row_became_fully_flagged = flags.iter().all(|flag| *flag);
    Ok(ExecutionRowResult {
        corrected_data: ArrayValue::Complex32(corrected),
        updated_flags: (plan.apply_mode == ApplyMode::CalFlag).then_some(ArrayValue::Bool(flags)),
        updated_weight: any_calwt.then(|| ArrayValue::Float32(weight.expect("calwt weight"))),
        updated_weight_spectrum: weight_spectrum.map(ArrayValue::Float32),
        newly_flagged_samples,
        row_became_fully_flagged,
    })
}

fn apply_row_prefetched(
    row: &ApplyRowPlan,
    correlation_types: &[i32],
    inputs: &PrefetchedExecutionRowInputs,
    has_weight_spectrum: bool,
    plan: &ApplyPlan,
    loaded_tables: &[LoadedCalibrationTable],
    parang_state: Option<&ParallacticAngleState>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    apply_row(
        row,
        ExecutionRowInputs {
            correlation_types,
            data: &inputs.data,
            original_flags: &inputs.original_flags,
            original_weight: inputs.original_weight.as_ref(),
            has_weight_spectrum,
        },
        plan,
        loaded_tables,
        parang_state,
    )
}

impl ParallacticAngleState {
    fn feed_parallactic_angle(
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
            .parallactic_angle(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|parallactic_angle| parallactic_angle + receptor0_angle)
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

fn feed_row_distance(row: &FeedAngleRow, time_seconds: f64) -> f64 {
    if row.interval_seconds > 0.0 {
        let half = row.interval_seconds / 2.0;
        let start = row.time_seconds - half;
        let end = row.time_seconds + half;
        if (start..=end).contains(&time_seconds) {
            return 0.0;
        }
    }
    (row.time_seconds - time_seconds).abs()
}

fn load_parallactic_angle_state(ms: &MeasurementSet) -> MsResult<ParallacticAngleState> {
    let engine = MsCalEngine::new(ms)?;
    let mut feed_rows = HashMap::<(i32, i32), Vec<FeedAngleRow>>::new();
    if let Ok(feed) = ms.feed() {
        for row_index in 0..feed.row_count() {
            let antenna_id = feed.i32(row_index, "ANTENNA_ID")?;
            let feed_id = feed.i32(row_index, "FEED_ID")?;
            let spectral_window_id = feed.i32(row_index, "SPECTRAL_WINDOW_ID")?;
            let time_seconds = feed.f64(row_index, "TIME")?;
            let interval_seconds = feed.f64(row_index, "INTERVAL")?;
            let receptor0_angle_rad = match feed.array(row_index, "RECEPTOR_ANGLE")? {
                ArrayValue::Float64(values) if !values.is_empty() => values[[0]],
                ArrayValue::Float32(values) if !values.is_empty() => f64::from(values[[0]]),
                _ => 0.0,
            };
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

struct LoadedCalibrationTable {
    path: PathBuf,
    interp: ApplyInterpolationMode,
    supports_calwt: bool,
    solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>>,
}

#[derive(Clone, Copy)]
struct LegacyCalDescEntry {
    spw_id: i32,
    receptor_count: usize,
}

struct CalibrationSolution {
    time_seconds: f64,
    grid: CalibrationGrid,
}

#[derive(Clone)]
enum CalibrationGrid {
    Complex(GainGrid),
    Delay(DelayGrid),
}

#[derive(Clone)]
struct GainGrid {
    receptor_count: usize,
    channel_count: usize,
    values: ArrayD<Complex32>,
    flags: ArrayD<bool>,
}

#[derive(Clone, Copy)]
struct GainSample {
    value: Complex32,
    flagged: bool,
}

struct CalibrationSamplingContext<'a> {
    data_frequencies_hz: &'a [f64],
    cal_frequencies_hz: &'a [f64],
    cal_ref_frequency_hz: f64,
    interp: ApplyInterpolationMode,
    path: &'a Path,
}

#[derive(Clone)]
struct DelayGrid {
    receptor_count: usize,
    channel_count: usize,
    values_ns: ArrayD<f32>,
    flags: ArrayD<bool>,
}

impl CalibrationGrid {
    fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        match self {
            Self::Complex(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_frequencies_hz,
                context.cal_frequencies_hz,
                context.interp,
                context.path,
            ),
            Self::Delay(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_frequencies_hz,
                context.cal_ref_frequency_hz,
                context.path,
            ),
        }
    }
}

impl GainGrid {
    fn from_arrays(
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
        data_frequencies_hz: &[f64],
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
            ApplyInterpolationMode::Nearest | ApplyInterpolationMode::Linear => {
                let chan = data_chan_index.min(self.channel_count.saturating_sub(1));
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
                let target_frequency =
                    *data_frequencies_hz.get(data_chan_index).ok_or_else(|| {
                        ApplyExecutionError::UnsupportedInterpolation {
                            path: path.display().to_string(),
                            interp,
                            reason: "data channel index is outside the MeasurementSet SPW grid"
                                .to_string(),
                        }
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

impl LoadedCalibrationTable {
    fn lookup(
        &self,
        field_id: i32,
        spw_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        interp: ApplyInterpolationMode,
    ) -> Result<Option<CalibrationGrid>, ApplyExecutionError> {
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

fn interpolate_time_linear(
    path: &Path,
    candidates: &[CalibrationSolution],
    time_seconds: f64,
) -> Result<CalibrationGrid, ApplyExecutionError> {
    let mut sorted = candidates.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| a.time_seconds.total_cmp(&b.time_seconds));

    let lower = sorted
        .iter()
        .rev()
        .find(|solution| solution.time_seconds <= time_seconds)
        .copied();
    let upper = sorted
        .iter()
        .find(|solution| solution.time_seconds >= time_seconds)
        .copied();

    match (lower, upper) {
        (Some(lower), Some(upper))
            if (upper.time_seconds - lower.time_seconds).abs() > f64::EPSILON =>
        {
            let fraction = ((time_seconds - lower.time_seconds)
                / (upper.time_seconds - lower.time_seconds)) as f32;
            match (&lower.grid, &upper.grid) {
                (CalibrationGrid::Complex(lower), CalibrationGrid::Complex(upper)) => {
                    if lower.values.shape() != upper.values.shape() {
                        return Err(ApplyExecutionError::UnsupportedInterpolation {
                            path: path.display().to_string(),
                            interp: ApplyInterpolationMode::Linear,
                            reason: "time interpolation requires matching parameter shapes"
                                .to_string(),
                        });
                    }
                    let mut values = lower.values.clone();
                    for (value, upper_value) in values.iter_mut().zip(upper.values.iter()) {
                        *value = *value + (*upper_value - *value) * fraction;
                    }
                    let mut flags = lower.flags.clone();
                    for (flag, upper_flag) in flags.iter_mut().zip(upper.flags.iter()) {
                        *flag = *flag || *upper_flag;
                    }
                    Ok(CalibrationGrid::Complex(GainGrid {
                        receptor_count: lower.receptor_count,
                        channel_count: lower.channel_count,
                        values,
                        flags,
                    }))
                }
                (CalibrationGrid::Delay(lower), CalibrationGrid::Delay(upper)) => {
                    if lower.values_ns.shape() != upper.values_ns.shape() {
                        return Err(ApplyExecutionError::UnsupportedInterpolation {
                            path: path.display().to_string(),
                            interp: ApplyInterpolationMode::Linear,
                            reason: "time interpolation requires matching parameter shapes"
                                .to_string(),
                        });
                    }
                    let mut values_ns = lower.values_ns.clone();
                    for (value, upper_value) in values_ns.iter_mut().zip(upper.values_ns.iter()) {
                        *value = *value + (*upper_value - *value) * fraction;
                    }
                    let mut flags = lower.flags.clone();
                    for (flag, upper_flag) in flags.iter_mut().zip(upper.flags.iter()) {
                        *flag = *flag || *upper_flag;
                    }
                    Ok(CalibrationGrid::Delay(DelayGrid {
                        receptor_count: lower.receptor_count,
                        channel_count: lower.channel_count,
                        values_ns,
                        flags,
                    }))
                }
                _ => Err(ApplyExecutionError::UnsupportedInterpolation {
                    path: path.display().to_string(),
                    interp: ApplyInterpolationMode::Linear,
                    reason: "time interpolation requires matching calibration parameter families"
                        .to_string(),
                }),
            }
        }
        (Some(lower), _) => Ok(lower.grid.clone()),
        (_, Some(upper)) => Ok(upper.grid.clone()),
        _ => Err(ApplyExecutionError::UnsupportedInterpolation {
            path: path.display().to_string(),
            interp: ApplyInterpolationMode::Linear,
            reason: "no calibration rows available for linear interpolation".to_string(),
        }),
    }
}

fn load_calibration_table(
    table_plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    if table_plan.summary.table_subtype == "BPOLY" {
        return load_bpoly_calibration_table(table_plan);
    }
    let table = Table::open(TableOptions::new(&table_plan.spec.path)).map_err(|source| {
        ApplyExecutionError::OpenMeasurementSet {
            path: table_plan.spec.path.display().to_string(),
            source: MsError::from(source),
        }
    })?;
    let supports_calwt = table_plan.summary.parameter_family == CalibrationParameterFamily::Complex;

    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for row_index in 0..table.row_count() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let spw_id = get_i32(&table, row_index, COL_SPECTRAL_WINDOW_ID)?;
        let antenna_id = get_i32(&table, row_index, COL_ANTENNA1)?;
        let time_seconds = get_f64(&table, row_index, COL_TIME)?;
        let flags = table
            .get_array_cell(row_index, COL_FLAG)
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: table_plan.spec.path.display().to_string(),
                source: MsError::from(source),
            })?;
        let grid = match table_plan.summary.parameter_family {
            CalibrationParameterFamily::Complex => {
                let gains = table
                    .get_array_cell(row_index, COL_CPARAM)
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: table_plan.spec.path.display().to_string(),
                        source: MsError::from(source),
                    })?;
                CalibrationGrid::Complex(GainGrid::from_arrays(
                    &table_plan.spec.path,
                    gains,
                    flags,
                )?)
            }
            CalibrationParameterFamily::Float
                if table_plan.summary.table_subtype.as_str() == "K Jones" =>
            {
                let delays = table
                    .get_array_cell(row_index, COL_FPARAM)
                    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                        path: table_plan.spec.path.display().to_string(),
                        source: MsError::from(source),
                    })?;
                CalibrationGrid::Delay(DelayGrid::from_arrays(
                    &table_plan.spec.path,
                    delays,
                    flags,
                )?)
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
            .push(CalibrationSolution { time_seconds, grid });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        interp: table_plan.interp,
        supports_calwt,
        solutions,
    })
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

    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for row_index in 0..table.row_count() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let antenna_id = get_i32(&table, row_index, COL_ANTENNA1)?;
        let time_seconds = get_f64(&table, row_index, COL_TIME)?;
        let cal_desc_id = get_i32(&table, row_index, COL_CAL_DESC_ID)?;
        let cal_desc_entry = cal_desc_map.get(&cal_desc_id).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: table_plan.spec.path.display().to_string(),
                reason: format!("CAL_DESC_ID {cal_desc_id} was not present in CAL_DESC"),
            }
        })?;
        let spw_plan = spw_plans.get(&cal_desc_entry.spw_id).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: table_plan.spec.path.display().to_string(),
                reason: format!(
                    "planner did not provide a target SPW grid for CAL_DESC_ID {cal_desc_id} -> SPW {}",
                    cal_desc_entry.spw_id
                ),
            }
        })?;
        let grid = sample_bpoly_row(
            &table,
            row_index,
            &table_plan.spec.path,
            spw_plan,
            *cal_desc_entry,
        )?;
        solutions
            .entry((field_id, cal_desc_entry.spw_id, antenna_id))
            .or_default()
            .push(CalibrationSolution { time_seconds, grid });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        interp: table_plan.interp,
        supports_calwt: false,
        solutions,
    })
}

fn load_bpoly_cal_desc_map(
    path: &Path,
    cal_desc: &Table,
) -> Result<HashMap<i32, LegacyCalDescEntry>, ApplyExecutionError> {
    let mut entries = HashMap::new();
    for row_index in 0..cal_desc.row_count() {
        let spw_ids = cal_desc
            .get_array_cell(row_index, COL_SPECTRAL_WINDOW_ID)
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
                receptor_count: receptor_count.max(1),
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
    if amp_receptor_count != phase_receptor_count {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "BPOLY row {row_index} inferred mismatched receptor counts: amp={amp_receptor_count}, phase={phase_receptor_count}"
            ),
        });
    }
    let receptor_count = amp_receptor_count.max(1);
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
    let flags = ArrayD::from_elem(IxDyn(&[receptor_count, channel_count]).f(), false);
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
    if total_coefficients % coefficients_per_receptor == 0 {
        return Ok((total_coefficients / coefficients_per_receptor).max(1));
    }
    Err(ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: format!(
            "BPOLY {column} row {row_index} contained {total_coefficients} coefficients, which is not divisible by the per-receptor coefficient count {coefficients_per_receptor}"
        ),
    })
}

fn legacy_bpoly_chebyshev_value(coefficients: &[f64], x_start: f64, x_end: f64, x: f64) -> f64 {
    if coefficients.is_empty() {
        return 0.0;
    }
    if coefficients.len() == 1 || (x_end - x_start).abs() <= f64::EPSILON {
        return 0.5_f64 * coefficients[0];
    }

    let xcap = ((x - x_start) - (x_end - x)) / (x_end - x_start);
    let mut sum = 0.5_f64 * coefficients[0];
    let mut t_prev = 1.0_f64;
    let mut t_curr = xcap;
    sum += coefficients[1] * t_curr;
    for coefficient in coefficients.iter().copied().skip(2) {
        let t_next = 2.0_f64 * xcap * t_curr - t_prev;
        sum += coefficient * t_next;
        t_prev = t_curr;
        t_curr = t_next;
    }
    sum
}

fn cal_spw_reference_frequency_hz(cal_spw: &crate::plan::SpectralWindowPlan) -> f64 {
    cal_spw
        .channel_frequencies_hz
        .get(cal_spw.channel_frequencies_hz.len() / 2)
        .copied()
        .unwrap_or(cal_spw.ref_frequency_hz)
}

fn ensure_corrected_data_column(ms: &mut MeasurementSet) -> Result<bool, TableError> {
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(VisibilityDataColumn::CorrectedData.name()))
    {
        return Ok(false);
    }

    let column_def = *VisibilityDataColumn::CorrectedData
        .optional_column()
        .column_def();
    let schema = build_table_schema(&[column_def]).expect("single optional column schema");
    let column: ColumnSchema = schema
        .column(VisibilityDataColumn::CorrectedData.name())
        .expect("corrected data column present")
        .clone();
    let empty = Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[0, 0]).f(), Vec::<Complex32>::new()).unwrap(),
    ));
    ms.main_table_mut().add_column(column, Some(empty))?;

    let row_count = ms.row_count();
    for row_index in 0..row_count {
        let data = ms
            .main_table()
            .get_array_cell(row_index, VisibilityDataColumn::Data.name())?
            .clone();
        ms.main_table_mut().set_cell(
            row_index,
            VisibilityDataColumn::CorrectedData.name(),
            Value::Array(data),
        )?;
    }
    Ok(true)
}

#[derive(Clone, Copy, Default)]
struct ApplyRowFieldIndices {
    corrected_data: Option<usize>,
    flag: Option<usize>,
    flag_row: Option<usize>,
    weight: Option<usize>,
    weight_spectrum: Option<usize>,
}

fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<in-memory>".to_string())
}

fn open_append_file(path: &Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

fn median_f32(values: &[f32]) -> f32 {
    assert!(!values.is_empty(), "median requires at least one value");
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[middle]
    } else {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    }
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
    match table.get_scalar_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        }
    })? {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

fn get_f64(table: &Table, row_index: usize, column: &str) -> Result<f64, ApplyExecutionError> {
    match table.get_scalar_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        }
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
    match table.get_scalar_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        }
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
    match table.get_scalar_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        }
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
    let values = table.get_array_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        }
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
    let values = table.get_array_cell(row_index, column).map_err(|source| {
        ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        }
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

        let base = state.engine.parallactic_angle(time_seconds, 0, 0).unwrap();
        let feed = state
            .feed_parallactic_angle(time_seconds, 0, 0, 0, 1)
            .unwrap();

        assert!((feed - (base + 0.25)).abs() < 1.0e-12);
    }

    #[test]
    fn interpolate_time_linear_covers_complex_delay_and_error_cases() {
        let path = Path::new("/tmp/interp.cal");
        let complex_pair = [
            CalibrationSolution {
                time_seconds: 30.0,
                grid: CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(5.0, 4.0), Complex32::new(7.0, 6.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
                }),
            },
            CalibrationSolution {
                time_seconds: 10.0,
                grid: CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                }),
            },
        ];

        match interpolate_time_linear(path, &complex_pair, 20.0).unwrap() {
            CalibrationGrid::Complex(grid) => {
                assert_eq!(grid.values[[0, 0]], Complex32::new(3.0, 2.0));
                assert_eq!(grid.values[[0, 1]], Complex32::new(5.0, 4.0));
                assert!(grid.flags[[0, 0]]);
                assert!(!grid.flags[[0, 1]]);
            }
            CalibrationGrid::Delay(_) => panic!("expected complex interpolation"),
        }

        let delay_pair = [
            CalibrationSolution {
                time_seconds: 1.0,
                grid: CalibrationGrid::Delay(DelayGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 3.0])
                        .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, true]).unwrap(),
                }),
            },
            CalibrationSolution {
                time_seconds: 3.0,
                grid: CalibrationGrid::Delay(DelayGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![5.0_f32, 7.0])
                        .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
                }),
            },
        ];
        match interpolate_time_linear(path, &delay_pair, 2.0).unwrap() {
            CalibrationGrid::Delay(grid) => {
                assert_eq!(grid.values_ns[[0, 0]], 3.0);
                assert_eq!(grid.values_ns[[0, 1]], 5.0);
                assert!(grid.flags[[0, 0]]);
                assert!(grid.flags[[0, 1]]);
            }
            CalibrationGrid::Complex(_) => panic!("expected delay interpolation"),
        }

        match interpolate_time_linear(
            path,
            &[CalibrationSolution {
                time_seconds: 10.0,
                grid: CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                }),
            }],
            0.0,
        )
        .unwrap()
        {
            CalibrationGrid::Complex(grid) => {
                assert_eq!(grid.values[[0, 0]], Complex32::new(1.0, 0.0))
            }
            CalibrationGrid::Delay(_) => panic!("expected complex interpolation"),
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
                    grid: CalibrationGrid::Complex(GainGrid {
                        receptor_count: 1,
                        channel_count: 2,
                        values: ArrayD::from_shape_vec(
                            IxDyn(&[1, 2]).f(),
                            vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                        )
                        .unwrap(),
                        flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false])
                            .unwrap(),
                    }),
                },
                CalibrationSolution {
                    time_seconds: 20.0,
                    grid: CalibrationGrid::Delay(DelayGrid {
                        receptor_count: 1,
                        channel_count: 2,
                        values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 2.0])
                            .unwrap(),
                        flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false])
                            .unwrap(),
                    }),
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

        assert_eq!(median_f32(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median_f32(&[4.0, 1.0, 3.0, 2.0]), 2.5);
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
            CalibrationGrid::Delay(_) => panic!("expected complex BPOLY output"),
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

        assert!(ensure_corrected_data_column(&mut ms).unwrap());
        let corrected = ms
            .main_table()
            .get_array_cell(0, VisibilityDataColumn::CorrectedData.name())
            .unwrap()
            .clone();
        assert_eq!(
            corrected,
            *ms.main_table()
                .get_array_cell(0, VisibilityDataColumn::Data.name())
                .unwrap()
        );
        assert!(!ensure_corrected_data_column(&mut ms).unwrap());
        assert_eq!(display_ms_path(&ms), "<in-memory>");
    }
}
