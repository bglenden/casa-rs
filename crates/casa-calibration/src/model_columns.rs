// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA scratch-column initialization and model removal.

mod compression;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::path::Path;

use casa_ms::schema::main_table::{
    OptionalMainColumn, VisibilityDataColumn, initial_model_visibility,
};
use casa_ms::scratch_columns;
use casa_ms::selection::syntax::ChannelSelection;
use casa_ms::selection::{ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_ms::write_session::{
    MeasurementSetMutationBatch, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, maximum_visibility_cell_elements,
};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection};
use casa_tables::{
    RequiredScalarColumnValues, StreamedTiledShapeColumn, StreamedTiledShapeCubeLayout,
    StreamedTiledShapeValueType, StreamingTiledShapeWriter, TableWriteLock, TileLayoutPlanner,
};
use casa_types::{ArrayValue, Complex32, PrimitiveType};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::session::resolve_casa_calibration_selection;
pub(crate) use compression::ScratchDestination;

const DATA: VisibilityDataColumn = VisibilityDataColumn::Data;
const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScratchWriteMode<'a> {
    Replace,
    Create,
    Hanning { corrected: &'a str, model: &'a str },
}

impl<'a> ScratchWriteMode<'a> {
    fn physical_name(self, column: VisibilityDataColumn) -> &'a str {
        match (self, column) {
            (Self::Hanning { corrected, .. }, CORRECTED) => corrected,
            (Self::Hanning { model, .. }, MODEL) => model,
            _ => column.name(),
        }
    }
}

pub fn reset_scratch_columns(
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
        lock = reset(ms, all, &new, true, false, lock)?;
    }
    if has_corrected {
        lock = reset_existing_scratch(ms, selection, has_model, true, lock)?;
    }
    Ok(lock)
}

pub(crate) fn initialize_calibrater_columns(
    ms: &mut MeasurementSet,
    compress: bool,
    add_corrected: bool,
    add_model: bool,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let missing = [(MODEL, add_model), (CORRECTED, add_corrected)]
        .into_iter()
        .filter(|&(column, requested)| requested && !has_column(ms, column))
        .map(|(column, _)| column)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        // Calibrater preserves both existing scratch cells and virtual
        // recipes on open. Adding a missing MODEL_DATA clears the recipes.
        return Ok(lock);
    }
    let all = resolve_casa_calibration_selection(ms, &MsSelection::default()).map_err(invalid)?;
    if compress {
        compression::initialize_columns(ms, all, &missing, &mut lock)?;
        return Ok(lock);
    }
    reset(ms, all, &missing, true, false, lock)
}

/// Reset existing scratch columns for `calibrater.initcalset`.
///
/// `calset=1` removes virtual models from the entire MeasurementSet before
/// initializing the selected scratch cells. Other values keep virtual models.
/// Like CASA, the reset requires the legacy visibility iterator mode.
///
/// # Errors
///
/// Returns an error for the modern iterator mode, a missing corrected column,
/// an empty or invalid selection, or a failed read or write.
pub fn reset_calibration_set(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    calset: i64,
    old_iterator: bool,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    check_main_lock(ms, &lock)?;
    let clear_virtual = calset == 1;
    if !old_iterator || !has_column(ms, CORRECTED) {
        if clear_virtual {
            casa_ms::virtual_model::clear_virtual_model_holding(ms, &[], &[], &mut lock)?;
        }
        return Err(invalid(if old_iterator {
            "the MeasurementSet has no CORRECTED_DATA column"
        } else {
            "Calibrater::initCalSet not implemented"
        }));
    }
    reset_existing_scratch(ms, selection, has_column(ms, MODEL), clear_virtual, lock)
}

fn reset_existing_scratch(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    has_model: bool,
    clear_virtual: bool,
    lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let selected = resolve_casa_calibration_selection(ms, selection).map_err(invalid)?;
    if selected.selected_rows.is_empty() {
        return Err(invalid("selection matches no rows"));
    }
    let columns = if has_model {
        &[CORRECTED, MODEL][..]
    } else {
        &[CORRECTED]
    };
    reset(ms, selected, columns, false, clear_virtual, lock)
}

/// Prepare a visibility scratch column at attachment, before data selection.
pub(crate) fn initialize_scratch_column(
    ms: &mut MeasurementSet,
    column: VisibilityDataColumn,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    if has_column(ms, column) {
        if column == MODEL {
            // CASA invalidates virtual recipes when it attaches a scratch
            // model iterator, even if the scratch samples already exist.
            scratch_columns::prepare_write_holding(ms, column, false, &mut lock)?;
        }
        return Ok(lock);
    }
    let all = resolve_casa_calibration_selection(ms, &MsSelection::default()).map_err(invalid)?;
    reset(ms, all, &[column], true, false, lock)
}

/// Delete selected virtual models and/or the entire scratch model column.
///
/// CASA resolves `field` and `spw` when `virtual_model` is true. An empty
/// field selection clears every virtual model. The scratch deletion includes
/// any physical backing columns used for compression.
///
/// # Errors
///
/// Returns an error for an invalid selector or a failed table update.
pub fn delete_calibration_model(
    ms: &mut MeasurementSet,
    field: Option<&str>,
    spw: Option<&str>,
    virtual_model: bool,
    scratch: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    check_main_lock(ms, lock)?;
    if virtual_model {
        let field = field.filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("false"));
        let spw = spw.filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("false"));
        let fields = field.map_or_else(
            || Ok(Vec::new()),
            |text| ms.resolve_field_selector(if text.trim().is_empty() { "*" } else { text }),
        )?;
        let mut windows = Vec::new();
        if !fields.is_empty()
            && let Some(spw) = spw
        {
            windows.extend(
                ms.resolve_spw_selector(spw)?
                    .spw_ids()
                    .iter()
                    .map(|&id| id as i32),
            );
            windows.sort_unstable();
            windows.dedup();
        }
        casa_ms::virtual_model::clear_virtual_model_holding(ms, &fields, &windows, lock)?;
    }
    if scratch {
        scratch_columns::remove_scratch_model_holding(ms, lock)?;
    }
    Ok(())
}

fn check_main_lock(ms: &MeasurementSet, lock: &TableWriteLock) -> MsResult<()> {
    let path = ms
        .path()
        .ok_or_else(|| invalid("the MeasurementSet has no on-disk MAIN table"))?;
    if path.canonicalize().map_err(invalid)? != lock.path().canonicalize().map_err(invalid)? {
        return Err(invalid(format!(
            "the write lock held on {} is not on MAIN of {}",
            lock.path().display(),
            path.display()
        )));
    }
    Ok(())
}

pub(crate) fn has_column(ms: &MeasurementSet, column: VisibilityDataColumn) -> bool {
    has_column_named(ms, column.name())
}

fn has_column_named(ms: &MeasurementSet, column: &str) -> bool {
    ms.main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column))
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

/// Legacy calibration iterators visit the inclusive span of all selected segments.
pub(crate) fn calibration_channel_span(selection: &ChannelSelection) -> Option<Range<usize>> {
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
    Some(first as usize..last as usize + 1)
}

fn reset(
    ms: &mut MeasurementSet,
    selected: ResolvedMsSelection,
    columns: &[VisibilityDataColumn],
    create: bool,
    clear_virtual: bool,
    mut lock: TableWriteLock,
) -> MsResult<TableWriteLock> {
    let observed = observed_column(ms);
    reset_holding(
        ms,
        selected,
        columns,
        observed,
        if create {
            ScratchWriteMode::Create
        } else {
            ScratchWriteMode::Replace
        },
        clear_virtual,
        &mut lock,
    )?;
    Ok(lock)
}

/// Initialize Hanning scratch columns for the physical rows in the current
/// attachment. Existing model cells and virtual model recipes are preserved.
///
/// # Errors
/// The caller's MAIN lock is stale, or a required column cannot be filled.
pub fn initialize_hanning_scratch_holding(
    ms: &mut MeasurementSet,
    rows: &[usize],
    observed: &str,
    [corrected, model]: [&str; 2],
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if !ms.path().is_some_and(|path| lock.matches_table_path(path)) {
        return Err(invalid(
            "the Hanning write lock does not protect physical MAIN",
        ));
    }
    let has_corrected = has_column_named(ms, corrected);
    let has_model = has_column_named(ms, model);
    if (!has_corrected && corrected != CORRECTED.name()) || (!has_model && model != MODEL.name()) {
        return Err(invalid(
            "a physical column named by the scratch projection is missing",
        ));
    }
    let mode = ScratchWriteMode::Hanning { corrected, model };
    scratch_columns::prepare_write_holding(ms, CORRECTED, true, lock)?;
    let whole_table = !rows.is_empty() && rows.iter().copied().eq(0..ms.row_count());
    if whole_table && !has_corrected && can_clone_tiled_observed(ms, observed) {
        add_scratch_schema(ms, CORRECTED, observed)?;
        lock.record_write();
        ms.main_table_mut().prepare_write().add_tiled_column_clone(
            observed,
            CORRECTED.name(),
            "TiledCORRECTED_DATA",
        )?;
        if !has_model {
            let staging = tempfile::tempdir_in(lock.path()).map_err(invalid)?;
            let model = stream_hanning_model(ms, rows, observed, &staging.path().join("MODEL"))?;
            add_scratch_schema(ms, MODEL, observed)?;
            lock.record_write();
            ms.main_table_mut()
                .prepare_write()
                .add_streamed_tiled_shape_column(MODEL.name(), model)?;
        }
        return Ok(());
    }
    let mut columns = Vec::with_capacity(2);
    if !has_corrected || (!rows.is_empty() && observed != corrected) {
        columns.push(CORRECTED);
    }
    if !has_model {
        columns.push(MODEL);
    }
    if columns.is_empty() {
        return Ok(());
    }
    if rows.is_empty() {
        let (plan, _) = scratch_write_plan(ms, Vec::new(), &columns, observed, mode, false, lock)?;
        MeasurementSetWriteSession::mutate_selected_rows(ms, plan, Some(lock), invalid, |_, _| {
            Ok(())
        })?;
        return Ok(());
    }
    let mut selection =
        resolve_casa_calibration_selection(ms, &MsSelection::default()).map_err(invalid)?;
    if !whole_table {
        let selected: HashSet<_> = rows.iter().copied().collect();
        selection
            .selected_rows
            .retain(|row| selected.contains(&row.row_index));
    }
    reset_holding(ms, selection, &columns, observed, mode, false, lock)
}

fn stream_hanning_model(
    ms: &MeasurementSet,
    rows: &[usize],
    observed: &str,
    temporary: &Path,
) -> MsResult<StreamedTiledShapeColumn> {
    let table = ms.main_table();
    let shapes = table
        .column_accessor(observed)?
        .array_shapes_uncached(rows)?
        .into_iter()
        .map(|shape| {
            let Some([correlations, channels]) = shape.as_deref() else {
                return Err(invalid("observed data must have a defined rank-2 shape"));
            };
            Ok([*correlations, *channels])
        })
        .collect::<MsResult<Vec<_>>>()?;
    let mut rows_per_shape = BTreeMap::<_, usize>::new();
    for &shape in &shapes {
        *rows_per_shape.entry(shape).or_default() += 1;
    }
    let layouts = rows_per_shape
        .into_iter()
        .map(|(shape, count)| {
            let tiles = TileLayoutPlanner::repository_default()
                .plan_column(&shape, count, std::mem::size_of::<Complex32>())
                .map_err(invalid)?;
            Ok(StreamedTiledShapeCubeLayout::new(
                shape.to_vec(),
                count,
                tiles.tile_shape().to_vec(),
            ))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let mut scalar = table.required_scalar_columns_owned_for_rows(&["DATA_DESC_ID"], rows)?;
    let Some(RequiredScalarColumnValues::Int32(descriptions)) = scalar.remove("DATA_DESC_ID")
    else {
        return Err(invalid("DATA_DESC_ID must be Int32"));
    };
    let dd = ms.data_description()?;
    let pol_ids = (0..dd.row_count())
        .map(|row| dd.polarization_id(row))
        .collect::<MsResult<Vec<_>>>()?;
    let polarizations = model_polarizations(ms)?;
    let big_endian = table
        .stored_endian_format()?
        .is_none_or(casa_tables::EndianFormat::is_big_endian);
    let resources = MeasurementSetWriteResources::from_system_memory(1).map_err(invalid)?;
    let mut writer = StreamingTiledShapeWriter::create(
        temporary,
        StreamedTiledShapeValueType::Complex32,
        layouts,
        resources.available_bytes,
        big_endian,
    )
    .map_err(invalid)?;
    let mut models = HashMap::new();
    for (shape, description) in shapes.into_iter().zip(descriptions) {
        let pol = *pol_ids
            .get(description as usize)
            .ok_or_else(|| invalid("invalid DATA_DESC_ID"))?;
        let values = polarizations
            .get(&pol)
            .ok_or_else(|| invalid("invalid POLARIZATION_ID"))?;
        if values.len() != shape[0] {
            return Err(invalid("POLARIZATION does not agree with DATA"));
        }
        let cell = models.entry((shape, pol)).or_insert_with(|| {
            values
                .iter()
                .copied()
                .cycle()
                .take(shape[0] * shape[1])
                .collect::<Vec<_>>()
        });
        writer.push_complex32_row(&shape, cell).map_err(invalid)?;
    }
    writer.finish().map_err(invalid)
}

fn can_clone_tiled_observed(ms: &MeasurementSet, observed: &str) -> bool {
    let table = ms.main_table();
    table.schema().is_some_and(|schema| {
        schema.column(observed).and_then(|col| col.data_type()) == Some(PrimitiveType::Complex32)
            && !schema
                .columns()
                .iter()
                .any(|col| table.is_virtual_column(col.name()))
    }) && table
        .data_manager_info()
        .iter()
        .any(|manager| manager.dm_type == "TiledShapeStMan" && manager.columns == [observed])
}

fn reset_holding(
    ms: &mut MeasurementSet,
    selected: ResolvedMsSelection,
    columns: &[VisibilityDataColumn],
    observed: &str,
    mode: ScratchWriteMode<'_>,
    clear_virtual: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let (plan, destinations) = scratch_write_plan(
        ms,
        selected.row_indices().collect(),
        columns,
        observed,
        mode,
        clear_virtual,
        lock,
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
        .filter_map(|(&spw, selection)| Some((spw, calibration_channel_span(selection)?)))
        .collect::<BTreeMap<_, _>>();
    MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        plan,
        Some(lock),
        invalid,
        |session, ms| {
            for rows in selected.selected_rows.chunks(batch_rows) {
                let batch =
                    reset_batch(ms, observed, rows, &destinations, &ranges, &polarizations)?;
                session.write_mutation_batch(ms, batch).map_err(invalid)?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

pub(crate) fn scratch_write_plan<'a>(
    ms: &mut MeasurementSet,
    row_indices: Vec<usize>,
    columns: &[VisibilityDataColumn],
    observed: &str,
    mode: ScratchWriteMode<'a>,
    clear_virtual: bool,
    lock: &mut TableWriteLock,
) -> MsResult<(MeasurementSetWritePlan, Vec<ScratchDestination<'a>>)> {
    let destinations = columns
        .iter()
        .map(|&column| ScratchDestination::new(ms, column, mode.physical_name(column)))
        .collect::<MsResult<Vec<_>>>()?;
    if clear_virtual {
        casa_ms::virtual_model::clear_virtual_model_holding(ms, &[], &[], lock)?;
    }
    let creates = columns
        .iter()
        .map(|&column| match mode {
            ScratchWriteMode::Replace => false,
            ScratchWriteMode::Create => true,
            ScratchWriteMode::Hanning { .. } => !has_column_named(ms, mode.physical_name(column)),
        })
        .collect::<Vec<_>>();
    if mode == ScratchWriteMode::Create {
        for &column in columns {
            scratch_columns::prepare_write_holding(ms, column, true, lock)?;
        }
    }
    for (&column, &create) in columns.iter().zip(&creates) {
        if create {
            add_scratch_schema(ms, column, observed)?;
        }
    }
    let sizes = maximum_visibility_cell_elements(ms).map_err(invalid)?;
    let write_columns = destinations
        .iter()
        .zip(creates)
        .flat_map(|(column, create)| column.plans(sizes, create))
        .collect();
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(invalid)?;
    let plan =
        MeasurementSetWritePlan::selected_row_mutation(row_indices, write_columns, resources)
            .map_err(invalid)?;
    Ok((plan, destinations))
}

fn add_scratch_schema(
    ms: &mut MeasurementSet,
    column: VisibilityDataColumn,
    observed: &str,
) -> MsResult<()> {
    let keyword = (column == CORRECTED)
        .then(|| ms.main_table().column_keywords(observed))
        .flatten()
        .cloned();
    let schema = casa_ms::column_def::build_table_schema(&[*column.optional_column().column_def()])
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
    observed: &str,
    rows: &[ResolvedMsSelectionRow],
    columns: &[ScratchDestination<'_>],
    ranges: &BTreeMap<i32, Range<usize>>,
    polarizations: &BTreeMap<i32, Vec<Complex32>>,
) -> MsResult<MeasurementSetMutationBatch> {
    let row_indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let main = ms.main_table();
    let data = main.column_accessor(observed)?;
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
                main.column_accessor(destination.name())?
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
