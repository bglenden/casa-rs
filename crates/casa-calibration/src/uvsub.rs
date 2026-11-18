// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-domain subtraction using CASA's visible model precedence.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
use casa_ms::write_session::{
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteSession,
};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_tables::{Table, TableInfo, TableWriteLock};
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, Axis, IxDyn};

use crate::model_columns::{has_column, observed_column, scratch_write_plan};
use crate::session::resolve_casa_calibration_selection;
use crate::solve::grouping::VisibilityModels;

const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

/// Add or subtract the visible sky model in `CORRECTED_DATA` for every row.
/// Create the corrected column from observed data when it is absent.
/// An existing virtual model takes precedence over the scratch model.
pub fn subtract_visibility_model(path: impl AsRef<Path>, reverse: bool) -> MsResult<()> {
    let path = path.as_ref();
    let lock = TableWriteLock::acquire(path, 0)?;
    subtract_visibility_model_holding(path, reverse, lock)?.release()?;
    Ok(())
}

/// Subtract while retaining a write lock already owned by a tool session.
#[doc(hidden)]
pub fn subtract_visibility_model_holding(
    path: &Path,
    reverse: bool,
    lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let concatenated = std::fs::read_to_string(path.join("table.info"))
        .is_ok_and(|info| TableInfo::parse(&info).sub_type == "CONCATENATED");
    if concatenated {
        let sources = Table::concatenated_source_paths(path)?
            .ok_or_else(|| invalid("Multi-MS has no concatenation"))?;
        // Check all references before modifying the first partition.
        for source in &sources {
            MeasurementSet::open(source)?;
        }
        for source in sources {
            subtract_visibility_model(source, reverse)?;
        }
        return Ok(lock);
    }
    let mut ms = MeasurementSet::open(path)?;
    let create = !has_column(&ms, CORRECTED);
    let input = if create {
        observed_column(&ms)
    } else {
        CORRECTED.name()
    };
    let selected =
        resolve_casa_calibration_selection(&ms, &MsSelection::default()).map_err(invalid)?;
    let models = VisibilityModels::new(&ms, &selected.selected_rows).map_err(invalid)?;
    let polarizations = ms.polarization()?;
    let correlations = (0..polarizations.row_count())
        .map(|id| Ok((id as i32, polarizations.corr_type(id)?)))
        .collect::<MsResult<_>>()?;
    let mut lock = lock;
    let plan = scratch_write_plan(
        &mut ms,
        selected.row_indices().collect(),
        &[CORRECTED],
        create,
        &mut lock,
    )?;
    let batch_rows = plan.batch_rows().max(1);
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation_holding(&mut ms, plan, lock)
            .map_err(invalid)?;
    for rows in selected.selected_rows.chunks(batch_rows) {
        let batch = subtract_batch(&ms, rows, input, &models, &correlations, reverse)?;
        session
            .write_mutation_batch(&mut ms, batch)
            .map_err(invalid)?;
    }
    let (_, lock) = session.finish_mutation_holding().map_err(invalid)?;
    Ok(lock)
}

fn subtract_batch(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    input: &str,
    models: &VisibilityModels,
    correlations: &BTreeMap<i32, Vec<i32>>,
    reverse: bool,
) -> MsResult<MeasurementSetMutationBatch> {
    let row_indices: Vec<_> = rows.iter().map(|row| row.row_index).collect();
    let main = ms.main_table();
    let read = |name| {
        main.column_accessor(name)?
            .array_cells_owned_uncached(&row_indices)
    };
    let input_cells = read(input)?;
    let mut scratch = rows
        .iter()
        .any(|row| models.reads_column(row.field_id))
        .then(|| read(MODEL.name()))
        .transpose()?;
    let flags = rows
        .iter()
        .any(|row| models.uses_fourier(row.field_id))
        .then(|| read("FLAG"))
        .transpose()?;
    let no_flags = ArrayD::<bool>::default(IxDyn(&[0, 0]));
    let mut values = Vec::with_capacity(rows.len());
    for (offset, (row, data)) in rows.iter().zip(input_cells).enumerate() {
        let mut data = complex_cell(data, input)?;
        let codes = correlations
            .get(&row.polarization_id)
            .ok_or_else(|| invalid("invalid POLARIZATION_ID"))?;
        if data.ndim() != 2 || data.shape()[0] != codes.len() {
            return Err(invalid("visibility array and correlation layout disagree"));
        }
        let model = if models.reads_column(row.field_id) {
            let cell = scratch.as_mut().and_then(|cells| cells[offset].take());
            Some(complex_cell(cell, MODEL.name())?)
        } else {
            let row_flags = if models.uses_fourier(row.field_id) {
                match flags.as_ref().and_then(|cells| cells[offset].as_ref()) {
                    Some(ArrayValue::Bool(flags)) => flags,
                    _ => return Err(invalid("FLAG is undefined")),
                }
            } else {
                &no_flags
            };
            models
                .predict(ms, row.row_index, codes, row_flags)
                .map_err(invalid)?
        };
        add_or_subtract(&mut data, model, codes, reverse)?;
        values.push(ArrayValue::Complex32(data));
    }
    Ok(MeasurementSetMutationBatch {
        row_indices,
        columns: vec![MeasurementSetMutationColumnBatch {
            name: CORRECTED.name().to_owned(),
            values: MeasurementSetMutationColumnValues::Arrays(values),
        }],
    })
}

fn complex_cell(value: Option<ArrayValue>, column: &str) -> MsResult<ArrayD<Complex32>> {
    match value {
        Some(ArrayValue::Complex32(array)) => Ok(array),
        Some(ArrayValue::Float32(array)) => Ok(array.mapv(|value| Complex32::new(value, 0.0))),
        _ => Err(invalid(format!("{column} has no defined visibility array"))),
    }
}

fn add_or_subtract(
    data: &mut ArrayD<Complex32>,
    model: Option<ArrayD<Complex32>>,
    correlations: &[i32],
    reverse: bool,
) -> MsResult<()> {
    if let Some(model) = model {
        if data.shape() != model.shape() {
            return Err(invalid("model and corrected visibility shapes differ"));
        }
        if reverse {
            *data += &model;
        } else {
            *data -= &model;
        }
    } else {
        let sign = if reverse { 1.0 } else { -1.0 };
        for (mut data, &code) in data.axis_iter_mut(Axis(0)).zip(correlations) {
            data += initial_model_visibility(code) * sign;
        }
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
