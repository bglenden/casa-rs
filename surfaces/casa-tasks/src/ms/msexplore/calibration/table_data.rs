// SPDX-License-Identifier: LGPL-3.0-or-later
//! Casacore table metadata and parameter cells for calibration plots.

use std::collections::BTreeSet;
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions, add_directory};
use num_complex::Complex64;

use super::axis::SampleCoordinates;

pub(super) struct CalibrationTable {
    pub(super) table: Table,
    pub(super) windows: Vec<Window>,
    pub(super) antennas: Vec<Antenna>,
    pub(super) fields: Vec<Field>,
    pub(super) subtype: String,
    pub(super) parameter_column: &'static str,
}

pub(super) struct Window {
    pub(super) name: String,
    pub(super) frequencies: Vec<f64>,
    pub(super) widths: Vec<f64>,
    pub(super) reference: f64,
    pub(super) bandwidth: f64,
    pub(super) flagged: bool,
}

pub(super) struct Antenna {
    pub(super) name: String,
    pub(super) station: String,
    pub(super) position: [f64; 3],
}

pub(super) struct Field {
    pub(super) name: String,
    pub(super) code: String,
    pub(super) flagged: bool,
}

pub(super) struct Solution<'a> {
    pub(super) coordinates: SampleCoordinates,
    pub(super) values: &'a ArrayValue,
    pub(super) flags: &'a ArrayValue,
    pub(super) snr: Option<&'a ArrayValue>,
    weights: Option<&'a ArrayValue>,
}

impl CalibrationTable {
    pub(super) fn read(table: Table, path: &Path) -> Result<Self, String> {
        let parameter_column = if has_column(&table, "CPARAM") {
            "CPARAM"
        } else {
            "FPARAM"
        };
        if !has_column(&table, parameter_column) {
            return Err("calibration plot requires CPARAM or FPARAM".into());
        }
        let subtype = table
            .keywords()
            .get("VisCal")
            .and_then(|value| match value {
                Value::Scalar(ScalarValue::String(value)) => Some(value.as_str()),
                _ => None,
            })
            .unwrap_or(&table.info().sub_type)
            .to_string();
        let window_table = subtable(&table, path, "SPECTRAL_WINDOW")?;
        let antenna_table = subtable(&table, path, "ANTENNA")?;
        let field_table = subtable(&table, path, "FIELD")?;
        let windows = (0..window_table.row_count())
            .map(|row| Window::read(&window_table, row))
            .collect::<Result<_, _>>()?;
        let antennas = (0..antenna_table.row_count())
            .map(|row| Antenna::read(&antenna_table, row))
            .collect::<Result<_, _>>()?;
        let fields = (0..field_table.row_count())
            .map(|row| Field::read(&field_table, row))
            .collect();
        Ok(Self {
            table,
            windows,
            antennas,
            fields,
            subtype,
            parameter_column,
        })
    }

    pub(super) fn rows(&self) -> usize {
        self.table.row_count()
    }

    pub(super) fn position_parameters(&self) -> bool {
        self.subtype.starts_with("KAntPos")
    }

    pub(super) fn position_label(component: i32) -> &'static str {
        match component {
            0 => "X",
            1 => "Y",
            2 => "Z",
            _ => "*",
        }
    }

    pub(super) fn taql_rows(&self, expression: &str) -> Result<Option<BTreeSet<usize>>, String> {
        let expression = expression.trim();
        if expression.is_empty() {
            return Ok(None);
        }
        let rows = self
            .table
            .query(&format!("SELECT * WHERE ({expression})"))
            .map_err(|error| error.to_string())?;
        Ok(Some(rows.row_numbers().iter().copied().collect()))
    }

    pub(super) fn coordinates(&self, row: usize) -> Result<SampleCoordinates, String> {
        let table = &self.table;
        Ok(SampleCoordinates {
            row,
            channel: 0,
            receptor: 0,
            frequency_ghz: 0.0,
            time: number(table, row, "TIME").ok_or("calibration row has no TIME")?,
            interval: number(table, row, "INTERVAL").unwrap_or_default(),
            scan: integer(table, row, "SCAN_NUMBER").unwrap_or(-1),
            field: integer(table, row, "FIELD_ID").unwrap_or(-1),
            window: integer(table, row, "SPECTRAL_WINDOW_ID")
                .ok_or("calibration row has no SPECTRAL_WINDOW_ID")?,
            antennas: [
                integer(table, row, "ANTENNA1").unwrap_or(-1),
                integer(table, row, "ANTENNA2").unwrap_or(-1),
            ],
            feed1: integer(table, row, "FEED1").unwrap_or(-1),
            observation: integer(table, row, "OBSERVATION_ID").unwrap_or(-1),
        })
    }

    pub(super) fn solution(&self, coordinates: SampleCoordinates) -> Result<Solution<'_>, String> {
        let table = &self.table;
        let row = coordinates.row;
        let values = array(table, row, self.parameter_column)?;
        let flags = array(table, row, "FLAG")?;
        if values.shape().len() != 2 || flags.shape() != values.shape() {
            return Err(format!(
                "calibration row {row} needs matching receptor/channel parameter and FLAG axes"
            ));
        }
        Ok(Solution {
            coordinates,
            values,
            flags,
            snr: array(table, row, "SNR").ok(),
            weights: array(table, row, "WEIGHT").ok(),
        })
    }
}

impl Solution<'_> {
    pub(super) fn value(&self, receptor: usize, channel: usize) -> Option<Complex64> {
        let index = [receptor, channel];
        match self.values {
            ArrayValue::Complex32(values) => values
                .get(index)
                .copied()
                .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im))),
            ArrayValue::Complex64(values) => values.get(index).copied(),
            ArrayValue::Float32(values) => values
                .get(index)
                .map(|&value| Complex64::new(f64::from(value), 0.0)),
            ArrayValue::Float64(values) => {
                values.get(index).map(|&value| Complex64::new(value, 0.0))
            }
            _ => None,
        }
    }

    pub(super) fn snr(&self, receptor: usize, channel: usize) -> Option<f64> {
        let index = [receptor, channel];
        match self.snr? {
            ArrayValue::Float32(values) => values.get(index).map(|&value| f64::from(value)),
            ArrayValue::Float64(values) => values.get(index).copied(),
            _ => None,
        }
    }

    pub(super) fn weight(&self, receptor: usize, channel: usize) -> f64 {
        let index = [receptor, channel];
        match self.weights {
            Some(ArrayValue::Float32(values)) => values.get(index).map(|&value| f64::from(value)),
            Some(ArrayValue::Float64(values)) => values.get(index).copied(),
            _ => None,
        }
        .unwrap_or(1.0)
        .max(f64::from(f32::MIN_POSITIVE))
    }

    pub(super) fn flagged(&self, receptor: usize, channel: usize) -> bool {
        let ArrayValue::Bool(flags) = self.flags else {
            return true;
        };
        flags.get([receptor, channel]).copied().unwrap_or(true)
    }
}

impl Window {
    fn read(table: &Table, row: usize) -> Result<Self, String> {
        let frequencies = double_array(table, row, "CHAN_FREQ")?;
        Ok(Self {
            name: text(table, row, "NAME").unwrap_or_default().into(),
            reference: number(table, row, "REF_FREQUENCY")
                .or_else(|| frequencies.first().copied())
                .unwrap_or_default(),
            bandwidth: number(table, row, "TOTAL_BANDWIDTH").unwrap_or_default(),
            widths: double_array(table, row, "CHAN_WIDTH")?,
            frequencies,
            flagged: boolean(table, row, "FLAG_ROW").unwrap_or(false),
        })
    }
}

impl Antenna {
    fn read(table: &Table, row: usize) -> Result<Self, String> {
        let position = double_array(table, row, "POSITION")?
            .try_into()
            .map_err(|_| format!("ANTENNA row {row} needs a three-element POSITION"))?;
        Ok(Self {
            name: text(table, row, "NAME").unwrap_or_default().into(),
            station: text(table, row, "STATION").unwrap_or_default().into(),
            position,
        })
    }

    pub(super) fn label(&self) -> String {
        format!("{}@{}", self.name, self.station)
    }
}

impl Field {
    fn read(table: &Table, row: usize) -> Self {
        Self {
            name: text(table, row, "NAME").unwrap_or_default().into(),
            code: text(table, row, "CODE").unwrap_or_default().into(),
            flagged: boolean(table, row, "FLAG_ROW").unwrap_or(false),
        }
    }
}

pub(super) fn subtable(table: &Table, owner: &Path, name: &str) -> Result<Table, String> {
    let child = table
        .keywords()
        .get(name)
        .and_then(Value::as_table_ref)
        .map_or_else(
            || owner.join(name),
            |location| add_directory(location, owner),
        );
    Table::open(TableOptions::new(child)).map_err(|error| error.to_string())
}

fn has_column(table: &Table, column: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.column(column).is_some())
}

pub(super) fn array<'a>(
    table: &'a Table,
    row: usize,
    column: &str,
) -> Result<&'a ArrayValue, String> {
    table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(|error| error.to_string())
}

fn double_array(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, String> {
    match array(table, row, column)? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        _ => Err(format!("row {row} has a non-double {column} array")),
    }
}

fn scalar<'a>(table: &'a Table, row: usize, column: &str) -> Option<&'a ScalarValue> {
    table.cell_accessor(row, column).ok()?.scalar().ok()
}

pub(super) fn text<'a>(table: &'a Table, row: usize, column: &str) -> Option<&'a str> {
    match scalar(table, row, column)? {
        ScalarValue::String(value) => Some(value),
        _ => None,
    }
}

pub(super) fn number(table: &Table, row: usize, column: &str) -> Option<f64> {
    match scalar(table, row, column)? {
        ScalarValue::Float64(value) => Some(*value),
        ScalarValue::Float32(value) => Some(f64::from(*value)),
        _ => None,
    }
}

pub(super) fn integer(table: &Table, row: usize, column: &str) -> Option<i32> {
    match scalar(table, row, column)? {
        ScalarValue::Int32(value) => Some(*value),
        _ => None,
    }
}

fn boolean(table: &Table, row: usize, column: &str) -> Option<bool> {
    match scalar(table, row, column)? {
        ScalarValue::Bool(value) => Some(*value),
        _ => None,
    }
}
