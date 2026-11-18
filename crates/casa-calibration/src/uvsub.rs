// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-domain subtraction using CASA's visible model precedence.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
use casa_ms::write_session::{
    MeasurementSetMutationBatch, MeasurementSetWriteError, MeasurementSetWriteSession,
};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_tables::{Table, TableInfo, TableWriteLock};
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, Axis, IxDyn};

use crate::model_columns::{ScratchDestination, has_column, observed_column, scratch_write_plan};
use crate::session::resolve_casa_calibration_selection;
use crate::solve::grouping::VisibilityModels;

const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

/// Add or subtract the visible sky model in `CORRECTED_DATA` for every row.
/// Create the corrected column from observed data when it is absent.
/// An existing virtual model takes precedence over the scratch model.
pub fn subtract_visibility_model(path: impl AsRef<Path>, reverse: bool) -> MsResult<()> {
    for path in visibility_model_source_paths(path.as_ref())? {
        let mut lock = TableWriteLock::acquire_to_reread(&path, 0)?;
        subtract_visibility_model_holding(&path, reverse, &mut lock)?;
        lock.release()?;
    }
    Ok(())
}

/// Open an ordinary MS or a Multi-MS's physical parts before changing any.
#[doc(hidden)]
pub fn visibility_model_source_paths(path: &Path) -> MsResult<Vec<PathBuf>> {
    let concatenated = std::fs::read_to_string(path.join("table.info"))
        .is_ok_and(|info| TableInfo::parse(&info).sub_type == "CONCATENATED");
    let paths = if concatenated {
        Table::concatenated_source_paths(path)?
            .ok_or_else(|| invalid("Multi-MS has no concatenation"))?
    } else {
        vec![path.to_owned()]
    };
    paths
        .into_iter()
        .map(|path| {
            let path = path.canonicalize().map_err(invalid)?;
            let ms = MeasurementSet::open(&path)?;
            if ms.ms_version() != Some(2.0) {
                return Err(invalid(format!(
                    "{} is not an MSv2 MeasurementSet",
                    path.display()
                )));
            }
            Ok(path)
        })
        .collect()
}

/// Subtract in one physical MS while retaining an already held MAIN lock.
#[doc(hidden)]
pub fn subtract_visibility_model_holding(
    path: &Path,
    reverse: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
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
    let (plan, destinations) = scratch_write_plan(
        &mut ms,
        selected.row_indices().collect(),
        &[CORRECTED],
        create,
        false,
        lock,
    )?;
    let batch_rows = plan.batch_rows().max(1);
    MeasurementSetWriteSession::mutate_selected_rows_with_lock(
        &mut ms,
        plan,
        lock,
        |session, ms| {
            for rows in selected.selected_rows.chunks(batch_rows) {
                let batch = subtract_batch(
                    ms,
                    rows,
                    input,
                    &models,
                    &correlations,
                    &destinations[0],
                    reverse,
                )
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                session.write_mutation_batch(ms, batch)?;
            }
            Ok(())
        },
    )
    .map_err(invalid)?;
    Ok(())
}

fn subtract_batch(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    input: &str,
    models: &VisibilityModels,
    correlations: &BTreeMap<i32, Vec<i32>>,
    destination: &ScratchDestination,
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
    let columns = destination.batch(ms, &row_indices, rows, &BTreeMap::new(), values)?;
    Ok(MeasurementSetMutationBatch {
        row_indices,
        columns,
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
