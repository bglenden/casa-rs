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
use std::path::{Path, PathBuf};
use std::time::Instant;

use casacore_ms::column_def::build_table_schema;
use casacore_ms::derived::engine::MsCalEngine;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::main_table::VisibilityDataColumn;
use casacore_ms::{MsError, MsResult};
use casacore_tables::{ColumnSchema, Table, TableError, TableOptions};
use casacore_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
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

/// Outcome summary for one executor run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    for row in &plan.selected_rows {
        let correlation_types = correlation_types_by_ddid
            .get(&row.data_desc_id)
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: Vec::new(),
            })?;

        let data = ms
            .main_table()
            .get_array_cell(row.row_index, VisibilityDataColumn::Data.name())
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?
            .clone();
        let original_flags = ms
            .main_table()
            .get_array_cell(row.row_index, "FLAG")
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?
            .clone();

        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types,
                data: &data,
                original_flags: &original_flags,
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
                flags: result.updated_flags.unwrap_or(original_flags),
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
    let open_started_at = Instant::now();
    let mut ms =
        MeasurementSet::open(&path).map_err(|source| ApplyExecutionError::OpenMeasurementSet {
            path: path.display().to_string(),
            source,
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

    let mut report = execute_apply_plan(&mut ms, plan)?;
    report.timings.planning_ns = planning_ns;
    report.timings.planning_selection_ns = plan_timings.selection_ns;
    report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
    report.timings.planning_measurement_set_spectral_windows_ns =
        plan_timings.measurement_set_spectral_windows_ns;
    report.timings.planning_calibration_table_plans_ns = plan_timings.calibration_table_plans_ns;
    report.timings.open_measurement_set_ns = open_measurement_set_ns;
    report.timings.total_ns = total_started_at.elapsed().as_nanos() as u64;
    Ok(report)
}

/// Plan and execute calibration application against an already-open MeasurementSet.
pub fn execute_apply(
    ms: &mut MeasurementSet,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let total_started_at = Instant::now();
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
    let mut report = execute_apply_plan(ms, plan)?;
    report.timings.planning_ns = planning_ns;
    report.timings.planning_selection_ns = plan_timings.selection_ns;
    report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
    report.timings.planning_measurement_set_spectral_windows_ns =
        plan_timings.measurement_set_spectral_windows_ns;
    report.timings.planning_calibration_table_plans_ns = plan_timings.calibration_table_plans_ns;
    report.timings.total_ns = total_started_at.elapsed().as_nanos() as u64;
    Ok(report)
}

fn execute_apply_plan(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
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

    for row in &plan.selected_rows {
        let correlation_types = correlation_types_by_ddid
            .get(&row.data_desc_id)
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: Vec::new(),
            })?;

        let data = ms
            .main_table()
            .get_array_cell(row.row_index, VisibilityDataColumn::Data.name())
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?
            .clone();
        let original_flags = ms
            .main_table()
            .get_array_cell(row.row_index, "FLAG")
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?
            .clone();
        let original_weight = any_calwt.then(|| {
            ms.main_table()
                .get_array_cell(row.row_index, "WEIGHT")
                .cloned()
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                })
        });
        let original_weight = match original_weight {
            Some(result) => Some(result?),
            None => None,
        };
        let row_compute_started_at = Instant::now();
        let ExecutionRowResult {
            corrected_data,
            updated_flags,
            updated_weight,
            updated_weight_spectrum,
            newly_flagged_samples,
            row_became_fully_flagged,
        } = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types,
                data: &data,
                original_flags: &original_flags,
                original_weight: original_weight.as_ref(),
                has_weight_spectrum: any_calwt && has_weight_spectrum,
            },
            &plan,
            &loaded_tables,
            parang_state.as_ref(),
        )?;
        row_compute_ns += row_compute_started_at.elapsed().as_nanos() as u64;

        let row_writeback_started_at = Instant::now();
        ms.main_table_mut()
            .set_cell(
                row.row_index,
                VisibilityDataColumn::CorrectedData.name(),
                Value::Array(corrected_data),
            )
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::from(source),
            })?;

        if let Some(updated_flags) = updated_flags {
            ms.main_table_mut()
                .set_cell(row.row_index, "FLAG", Value::Array(updated_flags))
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                })?;
            if row_became_fully_flagged {
                ms.main_table_mut()
                    .set_cell(
                        row.row_index,
                        "FLAG_ROW",
                        Value::Scalar(ScalarValue::Bool(true)),
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
            ms.main_table_mut()
                .set_cell(row.row_index, "WEIGHT", Value::Array(updated_weight))
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                })?;
        }
        if let Some(updated_weight_spectrum) = updated_weight_spectrum {
            ms.main_table_mut()
                .set_cell(
                    row.row_index,
                    "WEIGHT_SPECTRUM",
                    Value::Array(updated_weight_spectrum),
                )
                .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                    path: ms_path.clone(),
                    source: MsError::from(source),
                })?;
        }

        updated_row_count += 1;
        row_writeback_ns += row_writeback_started_at.elapsed().as_nanos() as u64;
    }

    let save_started_at = Instant::now();
    ms.save_main_table_only_assuming_valid().map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path,
            source,
        }
    })?;
    let save_ns = save_started_at.elapsed().as_nanos() as u64;

    Ok(ApplyExecutionReport {
        plan,
        created_corrected_data_column,
        wrote_measurement_set: true,
        updated_row_count,
        flagged_row_count,
        flagged_sample_count,
        timings: ApplyExecutionTimings {
            planning_ns: 0,
            open_measurement_set_ns: 0,
            ensure_corrected_data_ns,
            correlation_lookup_ns,
            calibration_load_ns,
            row_compute_ns,
            row_writeback_ns,
            save_ns,
            total_ns: 0,
            ..ApplyExecutionTimings::default()
        },
    })
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
            .feed_parallactic_angle(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
                receptor0_angle,
            )
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

fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<in-memory>".to_string())
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
    use casacore_types::measures::direction::{DirectionRef, MDirection};
    use casacore_types::measures::position::MPosition;

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
}
