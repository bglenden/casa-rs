// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::ArrayValue;

use super::cube::{SampleAxes, read_cells, required};
use super::reduction::Moments;
use super::rows::Row;
use super::spec::Column;
use crate::selection::ResolvedMsSelection;
use crate::{MeasurementSet, MsError, MsResult};

pub(super) fn reduce(
    table: &Table,
    rows: &[Row],
    column: Column,
    mask: bool,
    legacy: bool,
    dataset: usize,
    moments: &mut Moments,
) -> MsResult<()> {
    let mut extra = Vec::new();
    let mut uvw = Vec::new();
    match column {
        Column::Scalar(name @ ("FEED1" | "FEED2")) => {
            let physical: Vec<_> = rows.iter().map(|row| row.physical).collect();
            let mut values = table.required_scalar_columns_owned_for_rows(&[name], &physical)?;
            let Some(RequiredScalarColumnValues::Int32(values)) = values.remove(name) else {
                return Err(invalid_column(name));
            };
            extra = values;
        }
        Column::UvDistance => {
            let physical: Vec<_> = rows.iter().map(|row| row.physical).collect();
            uvw = read_cells(table, "UVW", &physical)?;
        }
        _ => {}
    }
    let mut sample_rows = rows;
    if legacy
        && matches!(
            column,
            Column::Scalar("ARRAY_ID" | "FIELD_ID" | "DATA_DESC_ID")
        )
    {
        sample_rows = &rows[..1];
    }
    for (index, row) in sample_rows.iter().enumerate() {
        if mask && row.flag {
            continue;
        }
        let value = match column {
            Column::FlagRow => f64::from(u8::from(row.flag)),
            Column::Scalar("ARRAY_ID") => f64::from(row.array),
            Column::Scalar("FIELD_ID") => f64::from(row.field),
            Column::Scalar("DATA_DESC_ID") => f64::from(row.data_description),
            Column::Scalar("SCAN_NUMBER") => f64::from(row.scan),
            Column::Scalar("TIME") => row.time,
            Column::Scalar("INTERVAL") => row.interval,
            Column::Scalar("ANTENNA1") => f64::from(row.antenna1),
            Column::Scalar("ANTENNA2") => f64::from(row.antenna2),
            Column::Scalar("FEED1" | "FEED2") => f64::from(extra[index]),
            Column::UvDistance => {
                let ArrayValue::Float64(values) = required(&uvw, index, row.physical, "UVW")?
                else {
                    return Err(invalid_column("UVW"));
                };
                let Some([u, v, _]) = values.as_slice() else {
                    return Err(invalid_column("UVW"));
                };
                (u * u + v * v).sqrt()
            }
            _ => unreachable!("this reducer handles scalar statistics columns"),
        };
        push_sample(moments, value, legacy, dataset, index);
    }
    Ok(())
}

fn push_sample(moments: &mut Moments, value: f64, legacy: bool, dataset: usize, index: usize) {
    let (value, square) = if legacy {
        let narrow = value as f32;
        (f64::from(narrow), f64::from(narrow * narrow))
    } else {
        (value, value * value)
    };
    moments.push(value, square, 1.0, dataset, index);
}

pub(super) fn reduce_vector(
    ms: &MeasurementSet,
    selection: Option<&ResolvedMsSelection>,
    rows: &[Row],
    column: &str,
) -> MsResult<Vec<Moments>> {
    let physical: Vec<_> = rows.iter().map(|row| row.physical).collect();
    let cells = read_cells(ms.main_table(), column, &physical)?;
    let mut result = Vec::new();
    let mut grid = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let cell = required(&cells, index, row.physical, column)?;
        match (column, cell) {
            ("UVW", ArrayValue::Float64(values)) if values.len() == 3 => {
                add_components(&mut result, values.iter().copied(), index)?;
            }
            ("WEIGHT" | "SIGMA", ArrayValue::Float32(values)) => {
                let axes = match grid.entry(row.data_description) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        SampleAxes::for_description(ms, selection, row.data_description)?,
                    ),
                };
                let projections = axes
                    .correlations
                    .iter()
                    .map(|&correlation| {
                        values
                            .get([correlation])
                            .copied()
                            .map(f64::from)
                            .ok_or_else(|| invalid_column(column))
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                add_components(&mut result, projections.into_iter(), index)?;
            }
            _ => return Err(invalid_column(column)),
        }
    }
    Ok(result)
}

fn add_components(
    moments: &mut Vec<Moments>,
    values: impl ExactSizeIterator<Item = f64>,
    index: usize,
) -> MsResult<()> {
    if index == 0 {
        moments.resize_with(values.len(), || Moments::new(true));
    } else if moments.len() != values.len() {
        return Err(MsError::InvalidInput(
            "statisticsold requires a uniform selected vector shape".into(),
        ));
    }
    for (moments, value) in moments.iter_mut().zip(values) {
        push_sample(moments, value, true, 0, index);
    }
    Ok(())
}

fn invalid_column(column: &str) -> MsError {
    MsError::InvalidInput(format!(
        "unexpected MeasurementSet cell type or shape for {column}"
    ))
}
