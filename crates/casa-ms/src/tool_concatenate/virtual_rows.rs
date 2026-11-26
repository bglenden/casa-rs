// SPDX-License-Identifier: LGPL-3.0-or-later
//! Remap donor MAIN in place so it can be used with the attached MS's subtables.

use std::collections::{BTreeMap, HashMap};

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::{ArrayValue, ScalarValue, Value};
use rayon::prelude::*;

use super::{
    MainRowMap,
    main_rows::{self, ArrayTransform, RowOrder, lookup, optional_id, sparse_id},
    scan::ScanBounds,
};
use crate::{
    MeasurementSet, MsError, MsResult,
    write_session::{MeasurementSetMutationColumnValues, VisibilityWriteDestination},
};

const SCALARS: &[&str] = &["ANTENNA1", "ANTENNA2", "DATA_DESC_ID", "FIELD_ID"];
pub(super) const SCAN_COLUMNS: &[&str] = &["OBSERVATION_ID", "SCAN_NUMBER"];

type RequiredScalars = HashMap<String, RequiredScalarColumnValues>;

pub(super) fn int32_column<'a>(scalar: &'a RequiredScalars, name: &str) -> MsResult<&'a [i32]> {
    match scalar.get(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(invalid(format!("column {name} must contain int32 values"))),
    }
}

pub(super) struct MainReplacement {
    pub(super) columns: BTreeMap<String, Vec<Option<Value>>>,
    pub(super) channel_selection: Option<Value>,
}

pub(super) fn attached_replacements(
    table: &Table,
    map: &MainRowMap,
    selected: Option<&[usize]>,
) -> MsResult<MainReplacement> {
    let mut columns = BTreeMap::new();
    for (name, mapping) in [
        ("OBSERVATION_ID", &map.attached_observations),
        ("PROCESSOR_ID", &map.attached_processors),
    ] {
        remap_ids(table, name, mapping, &mut columns)?;
    }
    if map.attached_has_state && !map.donor_has_state && table.row_count() > 0 {
        let missing = Some(Value::Scalar(ScalarValue::Int32(-1)));
        let states = if let Some(selected) = selected {
            let mut persisted = table.scalar_columns_owned(&["STATE_ID"])?;
            let mut states = persisted
                .remove("STATE_ID")
                .expect("requested STATE_ID")
                .into_iter()
                .map(|value| value.map(Value::Scalar))
                .collect::<Vec<_>>();
            for &row in selected {
                let state = states
                    .get_mut(row)
                    .ok_or_else(|| invalid("attached selection changed during concatenation"))?;
                *state = missing.clone();
            }
            states
        } else {
            vec![missing; table.row_count()]
        };
        columns.insert("STATE_ID".into(), states);
    }
    Ok(MainReplacement {
        columns,
        channel_selection: None,
    })
}

pub(super) fn remap_ids(
    table: &Table,
    name: &str,
    mapping: &[usize],
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
) -> MsResult<()> {
    if mapping.is_empty() || table.row_count() == 0 {
        return Ok(());
    }
    let scalars = table.required_scalar_columns_owned(&[name])?;
    let original = int32_column(&scalars, name)?;
    let updated = original
        .iter()
        .map(|&id| sparse_id(mapping, id))
        .collect::<MsResult<Vec<_>>>()?;
    replace(columns, name, original, updated);
    Ok(())
}

pub(super) fn prepare(
    attached: &MeasurementSet,
    donor: &MeasurementSet,
    map: &MainRowMap,
    scale: f32,
    read_bounds: Option<impl FnOnce() -> MsResult<ScanBounds> + Send>,
) -> MsResult<(MainReplacement, Option<String>)> {
    let table = donor.main_table();
    let mut names = SCALARS.to_vec();
    if map.attached_has_state && map.donor_has_state {
        names.push("STATE_ID");
    }
    if read_bounds.is_some() {
        names.extend(SCAN_COLUMNS);
        names.push("PROCESSOR_ID");
    }
    let read_scalar = || table.required_scalar_columns_owned(&names);
    // Attached scan bounds and donor scalar columns come from separate,
    // already captured and locked inputs.
    let (scalar, bounds) = if let Some(read_bounds) = read_bounds {
        let (scalar, bounds) = super::join_with_tracing(read_scalar, read_bounds);
        (scalar?, Some(bounds?))
    } else {
        (read_scalar()?, None)
    };
    let first = int32_column(&scalar, "ANTENNA1")?;
    let second = int32_column(&scalar, "ANTENNA2")?;
    let descriptions = int32_column(&scalar, "DATA_DESC_ID")?;
    let reorder = map.antennas.iter().copied().ne(0..map.antennas.len());
    let order = first
        .iter()
        .zip(second)
        .zip(descriptions)
        .map(|((&first, &second), &description)| {
            main_rows::row_order(map, first, second, description, reorder)
        })
        .collect::<MsResult<Vec<_>>>()?;
    let mut output = scalar_replacements(&scalar, &order, map)?;
    let auxiliary = bounds
        .map(|bounds| prepare_scans(bounds, &scalar, &mut output.columns, map))
        .transpose()?;
    let kernels = ArrayTransform::new(donor, &order, scale)?;
    if kernels.any_flipped {
        replace_feeds(table, &order, &mut output.columns)?;
    }
    let columns = if table.row_count() == 0 {
        Vec::new()
    } else {
        main_rows::array_names(attached.main_table(), table, table, |name| {
            kernels.changes(name)
        })?
    };
    if columns.contains(&"MODEL_DATA") {
        output.channel_selection = Some(main_rows::model_channel_selection(donor, None)?);
    }
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let transformed = columns
        .into_par_iter()
        .filter(|name| kernels.changes(name))
        .map(|name| {
            tracing::dispatcher::with_default(&dispatch, || {
                transform_column(table, name, &order, &kernels)
            })
        })
        .collect::<MsResult<Vec<_>>>()?;
    for group in transformed {
        for (name, cells) in group {
            if output.columns.insert(name.clone(), cells).is_some() {
                return Err(invalid(format!("several virtual edits change MAIN {name}")));
            }
        }
    }
    Ok((output, auxiliary))
}

fn replace_feeds(
    table: &Table,
    order: &[RowOrder],
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
) -> MsResult<()> {
    let feeds = table.required_scalar_columns_owned(&["FEED1", "FEED2"])?;
    let first = int32_column(&feeds, "FEED1")?;
    let second = int32_column(&feeds, "FEED2")?;
    let (new_first, new_second) = first
        .iter()
        .zip(second)
        .zip(order)
        .map(|((&first, &second), row)| {
            if row.flipped {
                (second, first)
            } else {
                (first, second)
            }
        })
        .unzip();
    replace(columns, "FEED1", first, new_first);
    replace(columns, "FEED2", second, new_second);
    Ok(())
}

fn scalar_replacements(
    scalar: &RequiredScalars,
    order: &[RowOrder],
    map: &MainRowMap,
) -> MsResult<MainReplacement> {
    let mut columns = BTreeMap::new();
    replace(
        &mut columns,
        "ANTENNA1",
        int32_column(scalar, "ANTENNA1")?,
        order.iter().map(|order| order.first).collect(),
    );
    replace(
        &mut columns,
        "ANTENNA2",
        int32_column(scalar, "ANTENNA2")?,
        order.iter().map(|order| order.second).collect(),
    );
    replace(
        &mut columns,
        "DATA_DESC_ID",
        int32_column(scalar, "DATA_DESC_ID")?,
        order.iter().map(|order| order.description).collect(),
    );
    let old_fields = int32_column(scalar, "FIELD_ID")?;
    let fields = old_fields
        .iter()
        .map(|&id| lookup(&map.fields, id, "FIELD_ID").copied())
        .collect::<MsResult<_>>()?;
    replace(&mut columns, "FIELD_ID", old_fields, fields);
    if map.attached_has_state && map.donor_has_state {
        let old_states = int32_column(scalar, "STATE_ID")?;
        let states = old_states
            .iter()
            .map(|&id| optional_id(&map.states, id, "STATE_ID"))
            .collect::<MsResult<_>>()?;
        replace(&mut columns, "STATE_ID", old_states, states);
    }
    Ok(MainReplacement {
        columns,
        channel_selection: None,
    })
}

fn prepare_scans(
    mut bounds: ScanBounds,
    scalar: &RequiredScalars,
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
    map: &MainRowMap,
) -> MsResult<String> {
    let old_observations = int32_column(scalar, "OBSERVATION_ID")?;
    let old_scans = int32_column(scalar, "SCAN_NUMBER")?;
    let old_processors = int32_column(scalar, "PROCESSOR_ID")?;
    let minimum = old_scans.iter().copied().min().unwrap_or(i32::MAX);
    let offsets = bounds.offsets_from_minimum(minimum, &map.observations);
    let count = old_observations.len();
    let mut observations = Vec::with_capacity(count);
    let mut scans = Vec::with_capacity(count);
    let mut processors = Vec::with_capacity(count);
    let mut max_scan = 0;
    for ((&old, &scan), &processor) in old_observations.iter().zip(old_scans).zip(old_processors) {
        let new = sparse_id(&map.observations, old)?;
        let scan = offsets.remap(old, new, scan)?;
        bounds.observe(new, scan);
        max_scan = max_scan.max(scan);
        observations.push(new);
        scans.push(scan);
        processors.push(sparse_id(&map.processors, processor)?);
    }
    replace(columns, "OBSERVATION_ID", old_observations, observations);
    replace(columns, "SCAN_NUMBER", old_scans, scans);
    replace(columns, "PROCESSOR_ID", old_processors, processors);
    Ok(bounds.render(max_scan))
}

fn replace(
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
    column: &str,
    original: &[i32],
    updated: Vec<i32>,
) {
    if updated != original {
        columns.insert(
            column.into(),
            updated
                .into_iter()
                .map(|value| Some(Value::Scalar(ScalarValue::Int32(value))))
                .collect(),
        );
    }
}

type ColumnReplacements = Vec<(String, Vec<Option<Value>>)>;

fn transform_column(
    table: &Table,
    column: &str,
    order: &[RowOrder],
    kernels: &ArrayTransform,
) -> MsResult<ColumnReplacements> {
    let rows = (0..order.len()).collect::<Vec<_>>();
    let arrays = table
        .column_accessor(column)?
        .array_cells_owned_uncached(&rows)?;
    if arrays.iter().all(Option::is_none) {
        return Ok(Vec::new());
    }
    let arrays = arrays
        .into_iter()
        .zip(order)
        .map(|(array, &order)| {
            array
                .map(|array| kernels.apply(column, order, array))
                .transpose()
        })
        .collect::<MsResult<Vec<Option<ArrayValue>>>>()?;
    if table.is_virtual_column(column) {
        let cells = arrays
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid(format!("undefined compressed MAIN {column} cell")))?;
        let updates = VisibilityWriteDestination::for_append(table, column)?
            .batch(table, &rows, cells, None)?;
        return Ok(updates
            .into_iter()
            .map(|update| {
                let values = match update.values {
                    MeasurementSetMutationColumnValues::Scalars(values) => values
                        .into_iter()
                        .map(|value| Some(Value::Scalar(value)))
                        .collect(),
                    MeasurementSetMutationColumnValues::Arrays(values) => values
                        .into_iter()
                        .map(|value| Some(Value::Array(value)))
                        .collect(),
                };
                (update.name, values)
            })
            .collect());
    }
    Ok(vec![(
        column.into(),
        arrays
            .into_iter()
            .map(|array| array.map(Value::Array))
            .collect(),
    )])
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
