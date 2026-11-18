// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA scratch-column initialization and model removal.

mod compression;

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::schema::main_table::{
    OptionalMainColumn, VisibilityDataColumn, initial_model_visibility,
};
use casa_ms::scratch_columns;
use casa_ms::selection::{ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_ms::write_session::{
    MeasurementSetMutationBatch, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, maximum_visibility_cell_elements,
};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection};
use casa_tables::TableWriteLock;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::session::resolve_casa_calibration_selection;
pub(crate) use compression::ScratchDestination;

const DATA: VisibilityDataColumn = VisibilityDataColumn::Data;
const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

pub(crate) fn reset_scratch_columns(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    add_model: bool,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let has_corrected = has_column(ms, CORRECTED);
    let has_model = has_column(ms, MODEL);
    let mut new = Vec::new();
    if !has_corrected {
        new.push(CORRECTED);
    }
    if add_model && !has_model {
        new.push(MODEL);
    }
    if !new.is_empty() {
        // CASA adds and initializes missing columns across the entire MS
        // before resolving the user's selectors.
        let all =
            resolve_casa_calibration_selection(ms, &MsSelection::default()).map_err(invalid)?;
        lock = reset(ms, all, &new, true, lock)?;
    }
    if has_corrected {
        let selected = resolve_casa_calibration_selection(ms, selection).map_err(invalid)?;
        if selected.selected_rows.is_empty() {
            return Err(invalid("selection matches no rows"));
        }
        let columns = if has_model {
            &[CORRECTED, MODEL][..]
        } else {
            &[CORRECTED]
        };
        lock = reset(ms, selected, columns, false, lock)?;
    }
    Ok(lock)
}

pub(crate) fn delete_model(
    ms: &mut MeasurementSet,
    field: Option<&str>,
    virtual_model: bool,
    scratch: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if virtual_model {
        let field = field.filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("false"));
        let fields = field.map_or_else(
            || Ok(Vec::new()),
            |text| ms.resolve_field_selector(if text.trim().is_empty() { "*" } else { text }),
        )?;
        casa_ms::virtual_model::clear_virtual_model_holding(ms, &fields, &[], lock)?;
    }
    if scratch {
        scratch_columns::remove_scratch_model_holding(ms, lock)?;
    }
    Ok(())
}

pub(crate) fn has_column(ms: &MeasurementSet, column: VisibilityDataColumn) -> bool {
    ms.main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column.name()))
}

pub(crate) fn observed_column(ms: &MeasurementSet) -> &'static str {
    let float = OptionalMainColumn::FloatData.name();
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(float))
    {
        float
    } else {
        DATA.name()
    }
}

fn reset(
    ms: &mut MeasurementSet,
    selected: ResolvedMsSelection,
    columns: &[VisibilityDataColumn],
    create: bool,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let (plan, destinations) = scratch_write_plan(
        ms,
        selected.row_indices().collect(),
        columns,
        create,
        &mut lock,
    )?;
    let batch_rows = plan.batch_rows().max(1);
    let polarizations = if columns.contains(&MODEL) {
        model_polarizations(ms)?
    } else {
        BTreeMap::new()
    };
    let ranges = selected
        .channel_selections
        .iter()
        .filter_map(|(&spw, selection)| {
            let first = selection
                .segments
                .iter()
                .map(|segment| segment.start.min(segment.end))
                .min()?;
            let last = selection
                .segments
                .iter()
                .map(|segment| segment.start.max(segment.end))
                .max()?;
            Some((spw, first as usize..last as usize + 1))
        })
        .collect::<BTreeMap<_, _>>();
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation_holding(ms, plan, lock)
            .map_err(invalid)?;
    for rows in selected.selected_rows.chunks(batch_rows) {
        let batch = reset_batch(ms, rows, &destinations, &ranges, &polarizations)?;
        session.write_mutation_batch(ms, batch).map_err(invalid)?;
    }
    let (_, lock) = session.finish_mutation_holding().map_err(invalid)?;
    Ok(lock)
}

pub(crate) fn scratch_write_plan(
    ms: &mut MeasurementSet,
    row_indices: Vec<usize>,
    columns: &[VisibilityDataColumn],
    create: bool,
    lock: &mut TableWriteLock,
) -> MsResult<(MeasurementSetWritePlan, Vec<ScratchDestination>)> {
    let destinations = columns
        .iter()
        .map(|&column| ScratchDestination::new(ms, column))
        .collect::<MsResult<Vec<_>>>()?;
    for &column in columns {
        scratch_columns::prepare_write_holding(ms, column, create, lock)?;
    }
    if create {
        for &column in columns {
            let keyword = ms
                .main_table()
                .column_keywords(observed_column(ms))
                .cloned();
            let schema =
                casa_ms::column_def::build_table_schema(&[*column.optional_column().column_def()])
                    .map_err(invalid)?;
            let definition = schema
                .column(column.name())
                .expect("scratch column schema")
                .clone();
            ms.main_table_mut().add_column(definition, None)?;
            if let Some(keyword) = keyword {
                ms.main_table_mut()
                    .set_column_keywords(column.name(), keyword);
            }
        }
    }
    let sizes = maximum_visibility_cell_elements(ms).map_err(invalid)?;
    let write_columns = destinations
        .iter()
        .flat_map(|column| column.plans(sizes, create))
        .collect();
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(invalid)?;
    let plan =
        MeasurementSetWritePlan::selected_row_mutation(row_indices, write_columns, resources)
            .map_err(invalid)?;
    Ok((plan, destinations))
}

fn model_polarizations(ms: &MeasurementSet) -> MsResult<BTreeMap<i32, Vec<Complex32>>> {
    let polarization = ms.polarization()?;
    (0..polarization.row_count())
        .map(|id| {
            let values = polarization
                .corr_type(id)?
                .into_iter()
                .map(initial_model_visibility)
                .collect();
            Ok((id as i32, values))
        })
        .collect()
}

fn reset_batch(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    columns: &[ScratchDestination],
    ranges: &BTreeMap<i32, Range<usize>>,
    polarizations: &BTreeMap<i32, Vec<Complex32>>,
) -> MsResult<MeasurementSetMutationBatch> {
    let row_indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let main = ms.main_table();
    let data = main.column_accessor(observed_column(ms))?;
    let shapes = data.array_shapes_uncached(&row_indices)?;
    let mut data = columns
        .iter()
        .any(|column| column.visibility == CORRECTED)
        .then(|| data.array_cells_owned_uncached(&row_indices))
        .transpose()?;
    let needs_preserved_channels = rows
        .iter()
        .any(|row| ranges.contains_key(&row.spectral_window_id));
    let mut batch = Vec::with_capacity(columns.len());
    for destination in columns {
        let column = destination.visibility;
        let mut previous = needs_preserved_channels
            .then(|| {
                main.column_accessor(column.name())?
                    .array_cells_owned_uncached(&row_indices)
            })
            .transpose()?;
        let mut values = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let shape = shapes[index]
                .as_deref()
                .ok_or_else(|| invalid("DATA is undefined"))?;
            let &[correlations, channels] = shape else {
                return Err(invalid("visibility data must have two axes"));
            };
            let replacement = if column == MODEL {
                let model = polarizations
                    .get(&row.polarization_id)
                    .ok_or_else(|| invalid("invalid POLARIZATION_ID"))?;
                if model.len() != correlations {
                    return Err(invalid("POLARIZATION does not agree with DATA"));
                }
                ArrayValue::Complex32(ArrayD::from_shape_fn(IxDyn(shape).f(), |position| {
                    model[position[0]]
                }))
            } else {
                let observed = data
                    .as_mut()
                    .and_then(|rows| rows[index].take())
                    .ok_or_else(|| invalid("observed visibility is undefined"))?;
                match observed {
                    ArrayValue::Float32(array) => {
                        ArrayValue::Complex32(array.mapv(|value| Complex32::new(value, 0.0)))
                    }
                    complex => complex,
                }
            };
            if let Some(range) = ranges.get(&row.spectral_window_id) {
                if range.end > channels {
                    return Err(invalid("spectral-window selection exceeds DATA shape"));
                }
                let mut previous = previous
                    .as_mut()
                    .and_then(|rows| rows[index].take())
                    .ok_or_else(|| invalid("scratch column is undefined"))?;
                copy_channels(&mut previous, &replacement, range.clone())?;
                values.push(previous);
            } else {
                values.push(replacement);
            }
        }
        batch.extend(destination.batch(ms, &row_indices, rows, ranges, values)?);
    }
    Ok(MeasurementSetMutationBatch {
        row_indices,
        columns: batch,
    })
}

fn copy_channels(
    current: &mut ArrayValue,
    replacement: &ArrayValue,
    channels: Range<usize>,
) -> MsResult<()> {
    let (ArrayValue::Complex32(current), ArrayValue::Complex32(replacement)) =
        (current, replacement)
    else {
        return Err(invalid("expected complex scratch visibilities"));
    };
    if current.shape() != replacement.shape() {
        return Err(invalid("scratch and DATA shapes do not match"));
    }
    for channel in channels {
        current
            .index_axis_mut(ndarray::Axis(1), channel)
            .assign(&replacement.index_axis(ndarray::Axis(1), channel));
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
