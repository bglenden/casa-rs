// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prior calibration and packed visibility inputs shared by calibration solvers.

use casa_core::ArrayValue;
use casa_ms::{MeasurementSet, MsSelection};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, main_cell, ms_read_error};

use crate::apply::ApplyCalibrationTableSpec;
use crate::execute::{
    PreappliedRows, PriorSolveBoundary, evaluate_apply_rows, evaluate_solve_priors,
    has_polarization_matrix,
};
use crate::plan::{ApplyPlanRequest, plan_apply};

pub(crate) enum PriorCalibrationMode {
    Data,
    Baseline {
        share_input_flags: bool,
    },
    Accor {
        share_input_flags: bool,
    },
    Gain {
        scalar: bool,
        rotate_model: bool,
        share_input_flags: bool,
    },
    GainSpline,
    Bandpass {
        share_input_flags: bool,
        rotate_model: bool,
    },
    Delay {
        share_input_flags: bool,
        rotate_model: bool,
    },
    Leakage {
        share_input_flags: bool,
        parang_terms: usize,
    },
    CrossHand {
        share_input_flags: bool,
        parang_terms: usize,
    },
    PositionAngle {
        share_input_flags: bool,
        parang_terms: usize,
    },
}

pub(crate) fn load_preapplied_rows(
    ms: &MeasurementSet,
    selection: &MsSelection,
    prior_calibration_tables: &[ApplyCalibrationTableSpec],
    parang: bool,
    mode: PriorCalibrationMode,
) -> Result<Option<PreappliedRows>, GainSolveError> {
    let legacy_rotate = match mode {
        PriorCalibrationMode::Gain { rotate_model, .. }
        | PriorCalibrationMode::Bandpass { rotate_model, .. }
        | PriorCalibrationMode::Delay { rotate_model, .. } => rotate_model,
        _ => false,
    };
    if prior_calibration_tables.is_empty() && (!parang || legacy_rotate) {
        return Ok(None);
    }
    let mut plan = plan_apply(
        ms,
        &ApplyPlanRequest {
            selection: selection.clone(),
            apply_mode: crate::apply::ApplyMode::CalFlag,
            parang,
            calibration_tables: prior_calibration_tables.to_vec(),
        },
    )
    .map_err(|source| GainSolveError::PriorCalibrationPlan {
        source: Box::new(source),
    })?;
    if legacy_rotate && !has_polarization_matrix(&plan) {
        plan.parang = false;
    }
    let preapplied = match mode {
        PriorCalibrationMode::Data => evaluate_apply_rows(ms, &plan, false).map(Into::into),
        PriorCalibrationMode::Baseline { share_input_flags } => {
            evaluate_solve_priors(ms, plan, PriorSolveBoundary::Baseline, share_input_flags)
        }
        PriorCalibrationMode::Accor { share_input_flags } => {
            evaluate_solve_priors(ms, plan, PriorSolveBoundary::Accor, share_input_flags)
        }
        PriorCalibrationMode::Gain {
            scalar,
            share_input_flags,
            ..
        } => {
            let boundary = if scalar {
                PriorSolveBoundary::ScalarGain
            } else {
                PriorSolveBoundary::Gain
            };
            evaluate_solve_priors(ms, plan, boundary, share_input_flags)
        }
        PriorCalibrationMode::Bandpass {
            share_input_flags, ..
        } => evaluate_solve_priors(ms, plan, PriorSolveBoundary::Bandpass, share_input_flags),
        PriorCalibrationMode::GainSpline => {
            evaluate_solve_priors(ms, plan, PriorSolveBoundary::GainSpline, true)
        }
        PriorCalibrationMode::Delay {
            share_input_flags, ..
        } => evaluate_solve_priors(ms, plan, PriorSolveBoundary::Delay, share_input_flags),
        PriorCalibrationMode::Leakage {
            share_input_flags,
            parang_terms,
        } => evaluate_solve_priors(
            ms,
            plan,
            PriorSolveBoundary::Leakage { parang_terms },
            share_input_flags,
        ),
        PriorCalibrationMode::CrossHand {
            share_input_flags,
            parang_terms,
        } => evaluate_solve_priors(
            ms,
            plan,
            PriorSolveBoundary::CrossHand { parang_terms },
            share_input_flags,
        ),
        PriorCalibrationMode::PositionAngle {
            share_input_flags,
            parang_terms,
        } => evaluate_solve_priors(
            ms,
            plan,
            PriorSolveBoundary::PositionAngle { parang_terms },
            share_input_flags,
        ),
    };
    preapplied
        .map(Some)
        .map_err(|source| GainSolveError::PriorCalibrationApply {
            source: Box::new(source),
        })
}

/// Array payloads live for one batch, without filling the table's row cache.
pub(crate) struct SolveInputBatch {
    pub(super) columns: Vec<(&'static str, Vec<Option<ArrayValue>>)>,
    pub(super) spectral_weights: Vec<bool>,
}

impl SolveInputBatch {
    pub(super) fn polynomial_bin_weights(&self, index: usize) -> Option<&ArrayValue> {
        self.cell("WEIGHT_SPECTRUM", index)
    }

    pub(super) fn cell(&self, name: &str, index: usize) -> Option<&ArrayValue> {
        self.columns
            .iter()
            .find(|(column, _)| *column == name)
            .and_then(|(_, rows)| rows[index].as_ref())
    }
}

/// Read selected arrays together so headers and tiles are shared by a batch;
/// release the payloads after accumulation instead of retaining the whole MS.
pub(super) fn read_input_batch(
    ms: &MeasurementSet,
    rows: &[usize],
    (read_visibilities, read_model_flags): (bool, bool),
    read_model_column: bool,
    read_uvw: bool,
    spectral_weights: Vec<bool>,
    read_polynomial_bin_weights: bool,
) -> Result<SolveInputBatch, GainSolveError> {
    let schema = ms.main_table().schema();
    let mut columns = Vec::new();
    if spectral_weights.contains(&true) {
        columns.push("SIGMA_SPECTRUM");
    }
    if spectral_weights.contains(&false) {
        columns.push("SIGMA");
    }
    if read_polynomial_bin_weights
        && schema.is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
    {
        columns.push("WEIGHT_SPECTRUM");
    }
    if read_visibilities {
        columns.push("DATA");
    }
    if read_visibilities || read_model_flags {
        columns.push("FLAG");
    }
    if read_model_column {
        columns.push("MODEL_DATA");
    }
    if read_uvw {
        columns.push("UVW");
    }
    let mut inputs = SolveInputBatch {
        columns: Vec::with_capacity(columns.len()),
        spectral_weights,
    };
    for column in columns {
        let values = ms
            .main_table()
            .column_accessor(column)
            .and_then(|accessor| {
                read_column_inputs(
                    &accessor,
                    column,
                    schema
                        .and_then(|s| s.column(column))
                        .and_then(|c| c.data_type()),
                    rows,
                )
            })
            .map_err(|source| ms_read_error(ms, source))?;
        inputs.columns.push((column, values));
    }
    Ok(inputs)
}

/// Homogeneous cells use the typed selected reader, avoiding reconstruction
/// of complete tiled cubes. Mixed shapes and pending edits use the ordinary
/// owned-cell reader, preserving their existing validation and semantics.
fn read_column_inputs(
    column: &casa_tables::TableColumn<'_>,
    name: &str,
    kind: Option<casa_core::PrimitiveType>,
    rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, casa_tables::TableError> {
    if !matches!(name, "SIGMA" | "UVW")
        && let Ok(shapes) = column.array_shapes_uncached(rows)
        && let Some(Some(shape)) = shapes.first()
        && shape.len() == 2
        && shape.iter().all(|n| *n > 0)
        && shapes.iter().all(|other| other.as_ref() == Some(shape))
    {
        use casa_core::PrimitiveType;
        use casa_tables::SelectedArray2DCellsMut;
        let values = match kind {
            Some(PrimitiveType::Complex32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelComplex32(values),
                ArrayValue::Complex32,
            )?,
            Some(PrimitiveType::Bool) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelBool(values),
                ArrayValue::Bool,
            )?,
            Some(PrimitiveType::Float32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelFloat32(values),
                ArrayValue::Float32,
            )?,
            _ => None,
        };
        if let Some(values) = values {
            return Ok(values);
        }
    }
    column.array_cells_owned_uncached(rows)
}

/// Request contiguous row cells directly, avoiding a channel/row transpose.
fn read_row_packed<T: Clone>(
    column: &casa_tables::TableColumn<'_>,
    rows: &[usize],
    channels: usize,
    destination: for<'a> fn(&'a mut Vec<T>) -> casa_tables::SelectedArray2DCellsMut<'a>,
    wrap: fn(ArrayD<T>) -> ArrayValue,
) -> Result<Option<Vec<Option<ArrayValue>>>, casa_tables::TableError> {
    let mut values = Vec::new();
    let packed = column.fill_array_cells_2d_channel_range_typed_uncached(
        rows,
        0,
        channels,
        destination(&mut values),
    );
    // Old storage managers can provide an owned cell batch through their
    // whole-column reader. The typed channel reader deliberately declines it.
    if matches!(&packed, Err(casa_tables::TableError::Storage(reason))
        if reason.contains("its data manager can read this layout only as a whole column"))
    {
        return Ok(None);
    }
    let Some(shape) = packed? else {
        return Ok(None);
    };
    let shape = [shape.axis0_count, shape.channel_count];
    let capacity = shape[0].checked_mul(shape[1]).ok_or_else(|| {
        casa_tables::TableError::Storage("solve input cell size overflows".to_string())
    })?;
    values
        .chunks_exact(capacity)
        .map(|row| {
            ArrayD::from_shape_vec(IxDyn(&shape).f(), row.to_vec())
                .map(|array| Some(wrap(array)))
                .map_err(|e| casa_tables::TableError::Storage(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn input_cell<'a>(
    ms: &'a MeasurementSet,
    row: usize,
    inputs: (&'a SolveInputBatch, usize),
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    if let Some((_, values)) = inputs.0.columns.iter().find(|(name, _)| *name == column) {
        return values[inputs.1].as_ref().ok_or_else(|| {
            ms_read_error(
                ms,
                casa_tables::TableError::ColumnNotFound {
                    row_index: row,
                    column: column.to_string(),
                },
            )
        });
    }
    main_cell(ms, row, column)
}
