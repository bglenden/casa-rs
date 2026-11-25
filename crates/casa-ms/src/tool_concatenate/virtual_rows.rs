// SPDX-License-Identifier: LGPL-3.0-or-later
//! Remap donor MAIN in place so it can be used with the attached MS's subtables.

use std::collections::BTreeMap;

use casa_tables::Table;
use casa_types::{ArrayValue, ScalarValue, Value};
use rayon::prelude::*;

use super::{
    MainRowMap,
    main_rows::{
        self, ArrayTransform, RowOrder, ScalarColumns, integer, lookup, optional_id, sparse_id,
    },
    scan::ScanBounds,
};
use crate::{
    MeasurementSet, MsError, MsResult,
    write_session::{MeasurementSetMutationColumnValues, VisibilityWriteDestination},
};

const SCALARS: &[&str] = &[
    "ANTENNA1",
    "ANTENNA2",
    "DATA_DESC_ID",
    "FIELD_ID",
    "STATE_ID",
];
pub(super) const SCAN_COLUMNS: &[&str] = &["OBSERVATION_ID", "SCAN_NUMBER"];
const AUX_SCALARS: &[&str] = &["OBSERVATION_ID", "SCAN_NUMBER", "PROCESSOR_ID"];

pub(super) struct MainReplacement {
    pub(super) columns: BTreeMap<String, Vec<Option<Value>>>,
    pub(super) channel_selection: Option<Value>,
}

pub(super) fn prepare(
    attached: &MeasurementSet,
    donor: &MeasurementSet,
    map: &MainRowMap,
    scale: f32,
    bounds: Option<ScanBounds>,
) -> MsResult<(MainReplacement, Option<String>)> {
    let table = donor.main_table();
    let mut names = SCALARS.to_vec();
    if bounds.is_some() {
        names.extend(AUX_SCALARS);
    }
    let scalar = main_rows::scalar_columns(table, &names)?;
    let reorder = map.antennas.iter().copied().ne(0..map.antennas.len());
    let order = main_rows::row_orders(table.row_count(), &scalar, map, reorder)?;
    let mut output = scalar_replacements(&scalar, &order, map)?;
    let auxiliary = bounds
        .map(|bounds| prepare_scans(bounds, &scalar, &mut output.columns, map, table.row_count()))
        .transpose()?;
    let columns = if table.row_count() == 0 {
        Vec::new()
    } else {
        main_rows::array_names(attached.main_table(), table, table)?
    };
    if columns.contains(&"MODEL_DATA") {
        output.channel_selection = Some(main_rows::model_channel_selection(donor, None)?);
    }
    let kernels = ArrayTransform::new(donor, &order, scale)?;
    let transformed = columns
        .into_par_iter()
        .filter(|name| kernels.changes(name))
        .map(|name| transform_column(table, name, &order, &kernels))
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

fn scalar_replacements(
    scalar: &ScalarColumns,
    order: &[RowOrder],
    map: &MainRowMap,
) -> MsResult<MainReplacement> {
    let mut columns = BTreeMap::new();
    replace(
        &mut columns,
        scalar,
        "ANTENNA1",
        order.iter().map(|order| order.first).collect(),
    );
    replace(
        &mut columns,
        scalar,
        "ANTENNA2",
        order.iter().map(|order| order.second).collect(),
    );
    replace(
        &mut columns,
        scalar,
        "DATA_DESC_ID",
        order.iter().map(|order| order.description).collect(),
    );
    let fields = (0..order.len())
        .map(|row| {
            Ok(*lookup(
                &map.fields,
                integer(scalar, "FIELD_ID", row)?,
                "FIELD_ID",
            )?)
        })
        .collect::<MsResult<_>>()?;
    replace(&mut columns, scalar, "FIELD_ID", fields);
    if map.attached_has_state && map.donor_has_state {
        let states = (0..order.len())
            .map(|row| optional_id(&map.states, integer(scalar, "STATE_ID", row)?, "STATE_ID"))
            .collect::<MsResult<_>>()?;
        replace(&mut columns, scalar, "STATE_ID", states);
    }
    Ok(MainReplacement {
        columns,
        channel_selection: None,
    })
}

fn prepare_scans(
    mut bounds: ScanBounds,
    scalar: &ScalarColumns,
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
    map: &MainRowMap,
    count: usize,
) -> MsResult<String> {
    let offsets = bounds.offsets(scalar, &map.observations)?;
    let mut observations = Vec::with_capacity(count);
    let mut scans = Vec::with_capacity(count);
    let mut processors = Vec::with_capacity(count);
    let mut max_scan = 0;
    for row in 0..count {
        let old = integer(scalar, "OBSERVATION_ID", row)?;
        let new = sparse_id(&map.observations, old)?;
        let scan = offsets.remap(old, new, integer(scalar, "SCAN_NUMBER", row)?)?;
        bounds.observe(new, scan);
        max_scan = max_scan.max(scan);
        observations.push(new);
        scans.push(scan);
        processors.push(sparse_id(
            &map.processors,
            integer(scalar, "PROCESSOR_ID", row)?,
        )?);
    }
    replace(columns, scalar, "OBSERVATION_ID", observations);
    replace(columns, scalar, "SCAN_NUMBER", scans);
    replace(columns, scalar, "PROCESSOR_ID", processors);
    Ok(bounds.render(max_scan))
}

fn replace(
    columns: &mut BTreeMap<String, Vec<Option<Value>>>,
    scalar: &ScalarColumns,
    column: &str,
    updated: Vec<i32>,
) {
    if updated
        .iter()
        .zip(&scalar[column])
        .any(|(&new, old)| *old != Some(ScalarValue::Int32(new)))
    {
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
