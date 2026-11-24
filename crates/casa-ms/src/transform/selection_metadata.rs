// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna and time references in selected MeasurementSet subtables.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_tables::{ColumnType, Table, TableError, TableOptions};
use casa_types::{RecordValue, ScalarValue, Value};

use crate::selection::array_expr::ArraySelection;
use crate::selection::parser::parse_state_selector;
use crate::selection::syntax::parse_numeric_row_id_ranges;
use crate::selection::{ResolvedMsSelectionRow, TimeSelection};
use crate::subtables::{get_f64, get_i32};
use crate::{MeasurementSet, MsError, SubtableId};

use super::{
    MsTransformError, MsTransformRequest, rewrite_selected_table_rows, scalar_f64, scalar_i32,
};

pub(super) struct OrderedMainRows {
    pub(super) rows: Vec<usize>,
    pub(super) descriptions: Vec<i32>,
    pub(super) starts_visibility_buffer: Vec<bool>,
}

#[derive(Clone, Copy)]
pub(super) enum MainRowOrder {
    Transform,
    SingleDish,
    Baseline,
    Atmosphere,
}

/// CASA's visibility iterator groups ordinary MS rows by the original IDs
/// before ordering each group by TIME (MSTransformManager::setIterationApproach).
pub(super) fn sort_selected_rows(
    table: &Table,
    input_path: &Path,
    rows: Vec<usize>,
    descriptions: Vec<i32>,
    order: MainRowOrder,
) -> Result<OrderedMainRows, MsTransformError> {
    const GROUPS: [&str; 5] = [
        "OBSERVATION_ID",
        "ARRAY_ID",
        "SCAN_NUMBER",
        "STATE_ID",
        "FIELD_ID",
    ];
    const SINGLE_DISH_GROUPS: [&str; 4] = ["OBSERVATION_ID", "ARRAY_ID", "ANTENNA1", "FEED1"];
    let read = |name| {
        table
            .column_accessor(name)
            .and_then(|column| column.scalar_cells_owned_for_rows(&rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: input_path.display().to_string(),
                source: Box::new(source),
            })
    };
    let mut groups = vec![[0; GROUPS.len()]; rows.len()];
    let columns: &[&str] = match order {
        MainRowOrder::Transform => &GROUPS,
        MainRowOrder::SingleDish => &SINGLE_DISH_GROUPS,
        MainRowOrder::Baseline => &[],
        MainRowOrder::Atmosphere => &["OBSERVATION_ID", "ARRAY_ID", "FEED1"],
    };
    for (slot, &name) in columns.iter().enumerate() {
        for ((group, value), &row) in groups.iter_mut().zip(read(name)?).zip(&rows) {
            group[slot] = scalar_i32(value.as_ref(), name, row)?;
        }
    }
    let times = read("TIME")?;
    let mut ordering = groups
        .into_iter()
        .zip(times)
        .zip(rows)
        .zip(descriptions)
        .map(|(((group, time), row), ddid)| {
            Ok((group, ddid, scalar_f64(time.as_ref(), "TIME", row)?, row))
        })
        .collect::<Result<Vec<_>, MsTransformError>>()?;
    ordering.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| {
                if matches!(order, MainRowOrder::Baseline | MainRowOrder::Atmosphere) {
                    std::cmp::Ordering::Equal
                } else {
                    left.2.total_cmp(&right.2)
                }
            })
            .then_with(|| left.3.cmp(&right.3))
    });
    let mut starts_visibility_buffer = Vec::with_capacity(ordering.len());
    for (index, &(keys, description, time, _)) in ordering.iter().enumerate() {
        let start = index == 0 || {
            let (previous_keys, previous_description, previous_time, _) = ordering[index - 1];
            (keys, description, time) != (previous_keys, previous_description, previous_time)
        };
        starts_visibility_buffer.push(start);
    }
    let (rows, descriptions) = ordering
        .into_iter()
        .map(|(_, ddid, _, row)| (row, ddid))
        .unzip();
    Ok(OrderedMainRows {
        rows,
        descriptions,
        starts_visibility_buffer,
    })
}

/// Return selected MAIN rows and buffer starts in ordinary PlotMS iterator order.
/// Each input pair contains a row number and its data-description ID.
pub(crate) fn selected_plotms_integration_order(
    ms: &MeasurementSet,
    selected: impl Iterator<Item = (usize, i32)>,
) -> Result<(Vec<usize>, Vec<bool>), MsTransformError> {
    let (rows, descriptions) = selected.unzip();
    let ordered = sort_selected_rows(
        ms.main_table(),
        ms.path().unwrap_or_else(|| Path::new("<memory>")),
        rows,
        descriptions,
        MainRowOrder::Transform,
    )?;
    Ok((ordered.rows, ordered.starts_visibility_buffer))
}

#[derive(Default)]
pub(super) struct RowIdMaps {
    antenna: Option<BTreeMap<i32, i32>>,
    observation: Option<BTreeMap<i32, i32>>,
    state: Option<BTreeMap<i32, i32>>,
    pub(super) array: Option<ArraySelection>,
}

impl RowIdMaps {
    pub(super) fn for_column(&self, column: &str) -> Option<&BTreeMap<i32, i32>> {
        match column {
            "ANTENNA1" | "ANTENNA2" => self.antenna.as_ref(),
            "OBSERVATION_ID" => self.observation.as_ref(),
            "STATE_ID" => self.state.as_ref(),
            _ => None,
        }
    }
}

pub(super) fn reindex_selected_row_ids(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    antenna: Option<BTreeMap<i32, i32>>,
    reindex_array: bool,
) -> Result<RowIdMaps, MsTransformError> {
    let observation = request
        .selection
        .observation
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .map(|text| {
            let table = input
                .observation()
                .map_err(|source| metadata_error(request, source))?;
            let ranges = parse_numeric_row_id_ranges(text, "observation")
                .map_err(|source| metadata_error(request, source))?;
            let selected = (0..table.row_count() as i32)
                .filter(|&id| {
                    ranges
                        .iter()
                        .any(|&(first, last)| (first..=last).contains(&id))
                })
                .collect::<Vec<_>>();
            reindex_table(request, SubtableId::Observation, selected)
        })
        .transpose()?;
    let state = request
        .selection
        .intent
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .map(|text| {
            let selected = parse_state_selector(input, text)
                .map_err(|source| metadata_error(request, source))?;
            reindex_table(request, SubtableId::State, selected)
        })
        .transpose()?;
    let array = request
        .selection
        .array
        .as_deref()
        .filter(|text| reindex_array && !text.is_empty())
        .map(ArraySelection::parse)
        .transpose()
        .map_err(|source| metadata_error(request, source))?;
    Ok(RowIdMaps {
        antenna,
        observation,
        state,
        array,
    })
}

fn reindex_table(
    request: &MsTransformRequest,
    id: SubtableId,
    selected: Vec<i32>,
) -> Result<BTreeMap<i32, i32>, MsTransformError> {
    let name = id.name();
    let rows = selected.iter().map(|&row| row as usize).collect::<Vec<_>>();
    rewrite_selected_table_rows(
        &request.input_ms.join(name),
        &request.output_ms.join(name),
        &rows,
        |_, row| Ok(row),
    )?;
    Ok(selected
        .into_iter()
        .enumerate()
        .map(|(out, input)| (input, out as i32))
        .collect())
}

pub(super) fn reindex_fields_and_sources(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    field: Option<&str>,
    spw_ids: &BTreeMap<i32, i32>,
) -> Result<Option<BTreeMap<i32, i32>>, MsTransformError> {
    let fields = input
        .field()
        .map_err(|source| metadata_error(request, source))?;
    let selected = field
        .filter(|text| !text.trim().is_empty())
        .map(|text| {
            input
                .resolve_field_selector(text)
                .map_err(|source| metadata_error(request, source))
        })
        .transpose()?;
    let field_rows = match &selected {
        Some(ids) => ids.iter().map(|&id| id as usize).collect::<Vec<_>>(),
        None => (0..fields.row_count()).collect(),
    };
    let mut source_map = BTreeMap::new();
    let has_source = input.subtable(SubtableId::Source).is_some();
    let mut field_sources = BTreeMap::new();
    let mut sources_changed = false;
    for &index in &field_rows {
        let old_id = fields
            .source_id(index)
            .map_err(|source| metadata_error(request, source))?;
        let new_id = if has_source && old_id >= 0 {
            let next_id = source_map.len() as i32;
            *source_map.entry(old_id).or_insert(next_id)
        } else {
            -1
        };
        sources_changed |= new_id != old_id;
        field_sources.insert(index, new_id);
    }
    if selected.is_some() || sources_changed {
        let name = SubtableId::Field.name();
        rewrite_selected_table_rows(
            &request.input_ms.join(name),
            &request.output_ms.join(name),
            &field_rows,
            |index, mut row| {
                row.upsert(
                    "SOURCE_ID",
                    Value::Scalar(ScalarValue::Int32(field_sources[&index])),
                );
                Ok(row)
            },
        )?;
    }
    if let Some(source) = input.subtable(SubtableId::Source) {
        reindex_sources(source, request, &source_map, spw_ids)?;
    }
    Ok(selected.map(|ids| {
        ids.into_iter()
            .enumerate()
            .map(|(new, old)| (old, new as i32))
            .collect()
    }))
}

/// CASA copies SOURCE_MODEL records to a new transform product without
/// publishing links to the input iterator's field IDs or model paths.
pub(super) fn remove_virtual_model_links(
    input: &MeasurementSet,
    main: &mut Table,
    output: &Path,
) -> Result<(), MsTransformError> {
    let originals = [Some(input.main_table()), input.subtable(SubtableId::Source)];
    let mut keys = BTreeSet::new();
    for original in originals.into_iter().flatten() {
        for field in original.keywords().fields() {
            if field.name.starts_with("definedmodel_field_") {
                keys.insert(field.name.as_str());
                if let Value::Scalar(ScalarValue::String(key)) = &field.value {
                    keys.insert(key.as_str());
                }
            }
        }
    }
    if keys.is_empty() {
        return Ok(());
    }
    for &key in &keys {
        main.keywords_mut().remove(key);
    }
    let source_path = output.join(SubtableId::Source.name());
    if source_path.is_dir() {
        let error = |source| MsTransformError::MutateMeasurementSet {
            path: source_path.display().to_string(),
            source: Box::new(source),
        };
        let mut source =
            Table::open_metadata_only(TableOptions::new(&source_path)).map_err(error)?;
        for key in keys {
            source.keywords_mut().remove(key);
        }
        source
            .save_keywords_atomically(&source_path)
            .map_err(error)?;
    }
    Ok(())
}

fn reindex_sources(
    source: &Table,
    request: &MsTransformRequest,
    source_map: &BTreeMap<i32, i32>,
    spw_map: &BTreeMap<i32, i32>,
) -> Result<(), MsTransformError> {
    let mut selected = BTreeMap::new();
    let mut changed = false;
    for index in 0..source.row_count() {
        let id =
            get_i32(source, index, "SOURCE_ID").map_err(|error| metadata_error(request, error))?;
        let spw = get_i32(source, index, "SPECTRAL_WINDOW_ID")
            .map_err(|error| metadata_error(request, error))?;
        let new_spw = if spw == -1 {
            Some(-1)
        } else {
            spw_map.get(&spw).copied()
        };
        if let (Some(&new_id), Some(new_spw)) = (source_map.get(&id), new_spw) {
            selected.insert(index, (new_id, new_spw));
            changed |= (new_id, new_spw) != (id, spw);
        }
    }
    if changed || selected.len() != source.row_count() {
        let name = SubtableId::Source.name();
        let rows = selected.keys().copied().collect::<Vec<_>>();
        rewrite_selected_table_rows(
            &request.input_ms.join(name),
            &request.output_ms.join(name),
            &rows,
            |index, mut row| {
                let (id, spw) = selected[&index];
                row.upsert("SOURCE_ID", Value::Scalar(ScalarValue::Int32(id)));
                row.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(spw)));
                Ok(row)
            },
        )?;
    }
    Ok(())
}

pub(super) fn selected_antenna_ids(
    input: &MeasurementSet,
    antenna: Option<&str>,
    rows: &[ResolvedMsSelectionRow],
) -> Result<Option<Vec<i32>>, MsTransformError> {
    let Some(text) = antenna.filter(|text| !text.trim().is_empty()) else {
        return Ok(None);
    };
    let error = |source| MsTransformError::SelectRows {
        path: input
            .main_table()
            .path()
            .map_or_else(String::new, |path| path.display().to_string()),
        source,
    };
    let antenna_count = input.antenna().map_err(error)?.row_count();
    let ids = input.transform_antenna_ids(text).map_err(error)?;
    // Expressions with negation or a length/regex term can select rows using
    // antennas absent from the parser's positive ID lists. Keep their references
    // valid even when CASA's remapping would yield a negative MAIN antenna ID.
    let compact = ids.len() < antenna_count
        && rows.iter().all(|row| {
            ids.binary_search(&row.antenna1_id).is_ok()
                && ids.binary_search(&row.antenna2_id).is_ok()
        });
    Ok(compact.then_some(ids))
}

pub(super) fn reindex_antenna_subtables(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    antenna_ids: Option<&[i32]>,
    spw_ids: &BTreeMap<i32, i32>,
    time: Option<&TimeSelection>,
) -> Result<Option<BTreeMap<i32, i32>>, MsTransformError> {
    let antenna_map = antenna_ids.map(|ids| {
        ids.iter()
            .enumerate()
            .map(|(new, &old)| (old, new as i32))
            .collect::<BTreeMap<_, _>>()
    });
    if let Some(ids) = antenna_ids {
        let name = SubtableId::Antenna.name();
        let rows = ids.iter().map(|&id| id as usize).collect::<Vec<_>>();
        rewrite_selected_table_rows(
            &request.input_ms.join(name),
            &request.output_ms.join(name),
            &rows,
            |_, row| Ok(row),
        )?;
    }
    let spw_count = input
        .spectral_window()
        .map_err(|source| metadata_error(request, source))?
        .row_count();
    if antenna_map.is_some() || spw_ids.len() != spw_count {
        for name in ["FEED", "SYSCAL", "CALDEVICE", "SYSPOWER"] {
            reindex_subtable(request, name, antenna_map.as_ref(), Some(spw_ids), None)?;
        }
    }
    if antenna_map.is_some() {
        reindex_subtable(request, "WEATHER", antenna_map.as_ref(), None, None)?;
    }
    if antenna_map.is_some() || time.is_some() {
        reindex_subtable(request, "POINTING", antenna_map.as_ref(), None, time)?;
    }
    Ok(antenna_map)
}

fn reindex_subtable(
    request: &MsTransformRequest,
    name: &str,
    antenna_map: Option<&BTreeMap<i32, i32>>,
    spw_map: Option<&BTreeMap<i32, i32>>,
    time: Option<&TimeSelection>,
) -> Result<(), MsTransformError> {
    let source = request.input_ms.join(name);
    if !source.exists() {
        return Ok(());
    }
    let table = Table::open(TableOptions::new(&source)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: request.input_ms.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let mut ids = BTreeMap::new();
    for row in 0..table.row_count() {
        let antenna =
            get_i32(&table, row, "ANTENNA_ID").map_err(|source| metadata_error(request, source))?;
        let new_antenna = match (antenna_map, antenna >= 0) {
            (Some(map), true) => map.get(&antenna).copied(),
            (_, false) if name != "WEATHER" => None,
            _ => Some(antenna),
        };
        let Some(new_antenna) = new_antenna else {
            continue;
        };
        let new_spw = if let Some(map) = spw_map {
            let spw = get_i32(&table, row, "SPECTRAL_WINDOW_ID")
                .map_err(|source| metadata_error(request, source))?;
            if spw < 0 {
                Some(spw)
            } else if let Some(&new_spw) = map.get(&spw) {
                Some(new_spw)
            } else {
                continue;
            }
        } else {
            None
        };
        if let Some(time) = time {
            let epoch =
                get_f64(&table, row, "TIME").map_err(|source| metadata_error(request, source))?;
            // CASA's POINTING filter uses the recorded interval endpoints.
            // Those can differ from the predicate that selects MAIN rows.
            if !time
                .time_list()
                .any(|term| epoch >= term.start_s && epoch <= term.end_s)
            {
                continue;
            }
        }
        ids.insert(row, (new_antenna, new_spw));
    }
    let rows = ids.keys().copied().collect::<Vec<_>>();
    rewrite_selected_table_rows(
        &source,
        &request.output_ms.join(name),
        &rows,
        |index, mut row| {
            let (antenna, spw) = ids[&index];
            row.upsert("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna)));
            if let Some(spw) = spw {
                row.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(spw)));
            }
            Ok(row)
        },
    )
}

fn metadata_error(request: &MsTransformRequest, source: MsError) -> MsTransformError {
    MsTransformError::SpectralMetadata {
        path: request.input_ms.display().to_string(),
        reason: source.to_string(),
    }
}

/// Project only selected source cells. A narrow POINTING selection can have
/// very few output rows even when the input covers an entire observation.
pub(super) fn read_subtable_rows(
    table: &Table,
    indices: &[usize],
) -> Result<Vec<RecordValue>, TableError> {
    let schema = table
        .schema()
        .ok_or_else(|| TableError::Schema("missing subtable schema".into()))?;
    let mut rows = vec![RecordValue::new(Vec::new()); indices.len()];
    for column in schema.columns() {
        let name = column.name();
        let cells = table.column_accessor(name)?;
        let values = match column.column_type() {
            ColumnType::Scalar => cells
                .scalar_cells_owned_for_rows(indices)?
                .into_iter()
                .map(|cell| cell.map(Value::Scalar))
                .collect(),
            ColumnType::Array(_) => cells
                .array_cells_owned_uncached(indices)?
                .into_iter()
                .map(|cell| cell.map(Value::Array))
                .collect(),
            ColumnType::Record => indices
                .iter()
                .map(|&index| {
                    cells
                        .record_cell(index)
                        .map(|cell| Some(Value::Record(cell)))
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        for (row, value) in rows.iter_mut().zip(values) {
            if let Some(value) = value {
                row.upsert(name, value);
            }
        }
    }
    Ok(rows)
}
