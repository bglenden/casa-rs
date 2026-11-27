// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-domain subtraction using CASA's visible model precedence.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, Complex32, ScalarValue};
use casa_ms::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_ms::{MeasurementSetMutationBatch, MeasurementSetWriteError, MeasurementSetWriteSession};
use casa_tables::{Table, TableInfo, TableWriteLock};
use ndarray::{ArrayD, Axis, IxDyn};

use crate::model_columns::{
    ScratchDestination, ScratchWriteMode, has_column, observed_column, scratch_write_plan,
};
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
    subtract_visibility_model_selected(&mut ms, reverse, None, lock)
}

/// Subtract in one physical MS using an optional sorted list of selected MAIN rows.
#[doc(hidden)]
pub fn subtract_visibility_model_selected(
    ms: &mut MeasurementSet,
    reverse: bool,
    selection: Option<&[usize]>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if selection.is_some_and(<[usize]>::is_empty) {
        return Ok(());
    }
    let create = !has_column(ms, CORRECTED);
    let mut subtraction = Subtraction {
        selection,
        reverse,
        initializing: create,
        refusal: None,
    };
    let mut resolved =
        resolve_casa_calibration_selection(ms, &MsSelection::default()).map_err(invalid)?;
    if !create {
        resolved
            .selected_rows
            .retain(|row| subtraction.selected(row.row_index));
    }
    let model_rows;
    let rows_with_models = if create && selection.is_some() {
        model_rows = resolved
            .selected_rows
            .iter()
            .filter(|row| subtraction.selected(row.row_index))
            .cloned()
            .collect::<Vec<_>>();
        &model_rows
    } else {
        &resolved.selected_rows
    };
    let models = VisibilityModels::for_uvsub(ms, rows_with_models).map_err(invalid)?;
    let polarizations = ms.polarization()?;
    let correlations = (0..polarizations.row_count())
        .map(|id| Ok((id as i32, polarizations.corr_type(id)?)))
        .collect::<MsResult<_>>()?;
    if models.has_virtual_refusals() {
        let rows = &resolved.selected_rows;
        if create {
            let mut baseline = Subtraction {
                selection: Some(&[]),
                ..subtraction
            };
            write_subtraction(ms, rows, &models, &correlations, &mut baseline, lock)?;
        }
        return subtract_valid_buffers(ms, rows_with_models, &models, &correlations, reverse, lock);
    }
    write_subtraction(
        ms,
        &resolved.selected_rows,
        &models,
        &correlations,
        &mut subtraction,
        lock,
    )
}

fn write_subtraction(
    ms: &mut MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    models: &VisibilityModels,
    correlations: &BTreeMap<i32, Vec<i32>>,
    subtraction: &mut Subtraction<'_>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let create = subtraction.initializing;
    let input = if create {
        observed_column(ms)
    } else {
        CORRECTED.name()
    };
    let (plan, destinations) = scratch_write_plan(
        ms,
        rows.iter().map(|row| row.row_index).collect(),
        &[CORRECTED],
        input,
        if create {
            ScratchWriteMode::Create
        } else {
            ScratchWriteMode::Replace
        },
        false,
        lock,
    )?;
    let batch_rows = plan.batch_rows().max(1);
    MeasurementSetWriteSession::mutate_selected_rows_with_lock(ms, plan, lock, |session, ms| {
        for rows in rows.chunks(batch_rows) {
            let batch = subtract_batch(
                ms,
                rows,
                input,
                models,
                correlations,
                &destinations[0],
                subtraction,
            )
            .map_err(visibility_column_error)?;
            session.write_mutation_batch(ms, batch)?;
        }
        Ok(())
    })
    .map_err(invalid)?;
    if let Some(error) = subtraction.refusal.take() {
        return Err(invalid(visibility_column_error(error)));
    }
    Ok(())
}

fn subtract_valid_buffers(
    ms: &mut MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    models: &VisibilityModels,
    correlations: &BTreeMap<i32, Vec<i32>>,
    reverse: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let main = ms.main_table();
    let mut ordered = rows
        .iter()
        .map(|row| {
            let ScalarValue::Int32(array) =
                main.cell_accessor(row.row_index, "ARRAY_ID")?.scalar()?
            else {
                return Err(invalid("ARRAY_ID must be Int32"));
            };
            Ok(((*array, row.field_id, row.data_desc_id), row))
        })
        .collect::<MsResult<Vec<_>>>()?;
    ordered.sort_unstable_by(|(a, left), (b, right)| {
        a.cmp(b)
            .then_with(|| left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds))
            .then_with(|| left.row_index.cmp(&right.row_index))
    });
    let mut subtraction = Subtraction {
        selection: None,
        reverse,
        initializing: false,
        refusal: None,
    };
    for buffer in
        ordered.chunk_by(|a, b| a.0 == b.0 && a.1.time_mjd_seconds == b.1.time_mjd_seconds)
    {
        // The old CASA reader reaches recipes in visibility-buffer order.
        // A refused buffer never touches CORRECTED_DATA or its compression.
        if let Some(error) = models.virtual_refusal(buffer[0].1.field_id) {
            return Err(invalid(visibility_column_error(invalid(error))));
        }
        let rows = buffer
            .iter()
            .map(|(_, row)| (**row).clone())
            .collect::<Vec<_>>();
        write_subtraction(ms, &rows, models, correlations, &mut subtraction, lock)?;
    }
    Ok(())
}

struct Subtraction<'a> {
    selection: Option<&'a [usize]>,
    reverse: bool,
    initializing: bool,
    refusal: Option<MsError>,
}

impl Subtraction<'_> {
    fn selected(&self, row: usize) -> bool {
        self.selection
            .is_none_or(|rows| rows.binary_search(&row).is_ok())
    }

    fn predicts(&self, row: usize) -> bool {
        self.refusal.is_none() && self.selected(row)
    }
}

fn visibility_column_error(error: MsError) -> MeasurementSetWriteError {
    MeasurementSetWriteError::Column(error.to_string())
}

fn subtract_batch(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    input: &str,
    models: &VisibilityModels,
    correlations: &BTreeMap<i32, Vec<i32>>,
    destination: &ScratchDestination<'_>,
    subtraction: &mut Subtraction<'_>,
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
        .any(|row| subtraction.predicts(row.row_index) && models.reads_column(row.field_id))
        .then(|| read(MODEL.name()))
        .transpose()?;
    let flags = rows
        .iter()
        .any(|row| subtraction.predicts(row.row_index) && models.uses_fourier(row.field_id))
        .then(|| read("FLAG"))
        .transpose()?;
    let no_flags = ArrayD::<bool>::default(IxDyn(&[0, 0]));
    let mut values = Vec::with_capacity(rows.len());
    for (offset, (row, data)) in rows.iter().zip(input_cells).enumerate() {
        let mut data = complex_cell(data, input)?;
        if subtraction.predicts(row.row_index) {
            let result = (|| {
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
                add_or_subtract(&mut data, model, codes, subtraction.reverse)
            })();
            if let Err(error) = result {
                if !subtraction.initializing {
                    return Err(error);
                }
                // Finish initializing the new column with observed values
                // from the refused row onward before returning the error.
                subtraction.refusal = Some(error);
            }
        }
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
