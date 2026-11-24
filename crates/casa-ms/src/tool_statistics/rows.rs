// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cmp::Ordering;
use std::collections::HashMap;

use casa_tables::{RequiredScalarColumnValues, Table};

use super::spec::Axis;
use crate::{MsError, MsResult};

pub(super) struct Row {
    pub(super) physical: usize,
    pub(super) array: i32,
    pub(super) field: i32,
    pub(super) data_description: i32,
    pub(super) scan: i32,
    pub(super) state: i32,
    pub(super) time: f64,
    pub(super) interval: f64,
    pub(super) flag: bool,
    pub(super) antenna1: i32,
    pub(super) antenna2: i32,
}

impl Row {
    fn axis(&self, axis: Axis) -> i32 {
        match axis {
            Axis::Array => self.array,
            Axis::Field => self.field,
            Axis::DataDescription => self.data_description,
            Axis::Scan => self.scan,
            Axis::Subscan => self.state,
            Axis::Integration => unreachable!("TIME uses its stored double precision"),
        }
    }

    pub(super) fn compare(&self, other: &Self, axis: Axis) -> Ordering {
        if axis == Axis::Integration {
            self.time.total_cmp(&other.time)
        } else {
            self.axis(axis).cmp(&other.axis(axis))
        }
    }

    pub(super) fn group_item(&self, axis: Axis) -> String {
        let column = axis.column();
        if axis == Axis::Integration {
            format!("{column}={:.6}", self.time - self.interval / 2.0)
        } else {
            format!("{column}={}", self.axis(axis))
        }
    }
}

pub(super) fn read_rows(table: &Table, selected: Option<&[usize]>) -> MsResult<Vec<Row>> {
    let columns = [
        "ARRAY_ID",
        "FIELD_ID",
        "DATA_DESC_ID",
        "SCAN_NUMBER",
        "STATE_ID",
        "TIME",
        "INTERVAL",
        "FLAG_ROW",
        "ANTENNA1",
        "ANTENNA2",
    ];
    let values = match selected {
        Some(rows) => table.required_scalar_columns_owned_for_rows(&columns, rows)?,
        None => table.required_scalar_columns_owned(&columns)?,
    };
    let arrays = integer_column(&values, "ARRAY_ID")?;
    let fields = integer_column(&values, "FIELD_ID")?;
    let data_descriptions = integer_column(&values, "DATA_DESC_ID")?;
    let scans = integer_column(&values, "SCAN_NUMBER")?;
    let states = integer_column(&values, "STATE_ID")?;
    let times = double_column(&values, "TIME")?;
    if times.iter().any(|time| !time.is_finite()) {
        return Err(MsError::InvalidInput(
            "TIME contains a nonfinite value".into(),
        ));
    }
    let intervals = double_column(&values, "INTERVAL")?;
    let antenna1 = integer_column(&values, "ANTENNA1")?;
    let antenna2 = integer_column(&values, "ANTENNA2")?;
    let Some(RequiredScalarColumnValues::Bool(flags)) = values.get("FLAG_ROW") else {
        return Err(column_type("FLAG_ROW", "booleans"));
    };
    Ok((0..times.len())
        .map(|index| Row {
            physical: selected.map_or(index, |rows| rows[index]),
            array: arrays[index],
            field: fields[index],
            data_description: data_descriptions[index],
            scan: scans[index],
            state: states[index],
            time: times[index],
            interval: intervals[index],
            flag: flags[index],
            antenna1: antenna1[index],
            antenna2: antenna2[index],
        })
        .collect())
}

fn integer_column<'a>(
    values: &'a HashMap<String, RequiredScalarColumnValues>,
    name: &str,
) -> MsResult<&'a [i32]> {
    match values.get(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(column_type(name, "Int32")),
    }
}

fn double_column<'a>(
    values: &'a HashMap<String, RequiredScalarColumnValues>,
    name: &str,
) -> MsResult<&'a [f64]> {
    match values.get(name) {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values),
        _ => Err(column_type(name, "Float64")),
    }
}

fn column_type(name: &str, expected: &str) -> MsError {
    MsError::InvalidInput(format!("MAIN.{name} must contain {expected}"))
}
