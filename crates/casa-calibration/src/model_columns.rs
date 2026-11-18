// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA scratch-column initialization and model removal.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::schema::main_table::{
    OptionalMainColumn, VisibilityDataColumn, initial_model_visibility,
};
use casa_ms::scratch_columns;
use casa_ms::selection::{ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_ms::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, maximum_visibility_cell_elements,
};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection};
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::session::resolve_casa_calibration_selection;

const DATA: VisibilityDataColumn = VisibilityDataColumn::Data;
const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

pub(crate) fn reset_scratch_columns(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    add_model: bool,
) -> MsResult<()> {
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
        reset(ms, all, &new, true)?;
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
        reset(ms, selected, columns, false)?;
    }
    Ok(())
}

pub(crate) fn delete_model(
    ms: &mut MeasurementSet,
    field: Option<&str>,
    virtual_model: bool,
    scratch: bool,
) -> MsResult<()> {
    if virtual_model {
        let (fields, _) = casa_ms::virtual_model::virtual_model_clear_ids(ms, field, None)?;
        casa_ms::virtual_model::clear_virtual_model(ms, &fields, &[])?;
    }
    if scratch {
        scratch_columns::remove_scratch_model(ms)?;
    }
    Ok(())
}

fn has_column(ms: &MeasurementSet, column: VisibilityDataColumn) -> bool {
    ms.main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column.name()))
}

fn observed_column(ms: &MeasurementSet) -> &'static str {
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
) -> MsResult<()> {
    for &column in columns {
        scratch_columns::prepare_write(ms, column, create)?;
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
    let write_columns = columns
        .iter()
        .map(|column| MeasurementSetWriteColumnPlan {
            name: column.name().to_string(),
            bytes_per_row: sizes.saturating_mul(std::mem::size_of::<Complex32>()),
            mode: if create {
                MeasurementSetColumnWriteMode::Create
            } else {
                MeasurementSetColumnWriteMode::Replace
            },
            storage_manager: if create {
                MeasurementSetColumnStorage::TiledShape
            } else {
                MeasurementSetColumnStorage::Persisted
            },
            tile_shape: None,
            create_source_column: None,
        })
        .collect();
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(invalid)?;
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        selected.row_indices().collect(),
        write_columns,
        resources,
    )
    .map_err(invalid)?;
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
        MeasurementSetWriteSession::start_selected_row_mutation(ms, plan).map_err(invalid)?;
    for rows in selected.selected_rows.chunks(batch_rows) {
        let batch = reset_batch(ms, rows, columns, &ranges, &polarizations)?;
        session.write_mutation_batch(ms, batch).map_err(invalid)?;
    }
    session.finish_mutation().map_err(invalid)?;
    Ok(())
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
    columns: &[VisibilityDataColumn],
    ranges: &BTreeMap<i32, Range<usize>>,
    polarizations: &BTreeMap<i32, Vec<Complex32>>,
) -> MsResult<MeasurementSetMutationBatch> {
    let row_indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let main = ms.main_table();
    let data = main.column_accessor(observed_column(ms))?;
    let shapes = data.array_shapes_uncached(&row_indices)?;
    let mut data = columns
        .contains(&CORRECTED)
        .then(|| data.array_cells_owned_uncached(&row_indices))
        .transpose()?;
    let needs_preserved_channels = rows
        .iter()
        .any(|row| ranges.contains_key(&row.spectral_window_id));
    let mut batch = Vec::with_capacity(columns.len());
    for &column in columns {
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
        batch.push(MeasurementSetMutationColumnBatch {
            name: column.name().to_string(),
            values: MeasurementSetMutationColumnValues::Arrays(values),
        });
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
