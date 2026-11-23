// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::{MeasurementSet, MsError, MsResult, MsSource};
use casa_tables::Table;
use casa_tables::table_measures::{ArrayMeasColumn, ScalarMeasColumn};
use casa_types::measures::frequency::MFrequency;
use casa_types::measures::record::frequency_to_record;
use casa_types::quanta::casacore::casacore_unit;
use casa_types::{ArrayValue, ScalarValue, Value};
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::scan_queries::int_array;
use super::spw_membership::checked_spw;
use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::value::CasaValue;

fn quantities_unit<'a>(table: &'a Table, column: &str, component: usize) -> MsResult<&'a str> {
    let units = table
        .column_keywords(column)
        .and_then(|keywords| keywords.get("QuantumUnits"));
    match units {
        Some(Value::Array(ArrayValue::String(values))) => values
            .iter()
            .nth(component)
            .or_else(|| values.first())
            .map(String::as_str),
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.as_str()),
        _ => None,
    }
    .ok_or_else(|| MsError::InvalidInput(format!("{column} has no quantum unit")))
}

fn quantity(value: f64, unit: &str) -> CasaValue {
    CasaValue::record([
        ("unit", CasaValue::Text(unit.to_owned())),
        ("value", CasaValue::Float(value)),
    ])
}

fn fields_by_source(
    ms: &MeasurementSet,
    selected: Option<i64>,
) -> MsResult<BTreeMap<i32, Vec<i32>>> {
    let fields = ms.field()?;
    let mut groups = BTreeMap::<i32, Vec<i32>>::new();
    for row in 0..fields.row_count() {
        let source = fields.source_id(row)?;
        if selected.is_none_or(|id| id == i64::from(source)) {
            groups.entry(source).or_default().push(row as i32);
        }
    }
    Ok(groups)
}

fn source_fields(ms: &MeasurementSet, fields: Vec<i32>, asnames: bool) -> MsResult<CasaValue> {
    if asnames {
        let table = ms.field()?;
        let names = fields
            .into_iter()
            .map(|id| table.name(id as usize))
            .collect::<MsResult<BTreeSet<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();
        Ok(CasaValue::TextArray {
            shape: vec![names.len()],
            values: names,
        })
    } else {
        Ok(int_array(fields))
    }
}

fn source_line_row(
    ms: &MeasurementSet,
    source: &MsSource<'_>,
    sourceid: i64,
    spw: i64,
) -> MsResult<usize> {
    checked_spw(ms, spw)?;
    if sourceid < 0 {
        return Err(MsError::InvalidInput("sourceid cannot be negative".into()));
    }
    let mut selected = None;
    let mut keys = BTreeSet::new();
    for row in 0..source.row_count() {
        let key = (
            source.i32(row, "SOURCE_ID")?,
            source.i32(row, "SPECTRAL_WINDOW_ID")?,
        );
        if !keys.insert(key) {
            return Err(MsError::InvalidInput(
                "Too few source keys found, there are duplicate SOURCE table keys".into(),
            ));
        }
        if (i64::from(key.0), i64::from(key.1)) == (sourceid, spw) {
            selected = Some(row);
        }
    }
    selected.ok_or_else(|| MsError::InvalidInput(format!(
        "SOURCE table does not contain a row with SOURCE_ID={sourceid} and SPECTRAL_WINDOW_ID={spw}"
    )))
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (source = -1, asnames = false), text_signature = "($self, source=-1, asnames=False)")]
    fn fieldsforsource(&self, py: Python<'_>, source: i64, asnames: bool) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let fields = fields_by_source(ms, Some(source))?
                .pop_first()
                .map(|(_, fields)| fields)
                .unwrap_or_default();
            source_fields(ms, fields, asnames)
        })?
        .into_python(py)
    }

    #[pyo3(signature = (asnames = false), text_signature = "($self, asnames=False)")]
    fn fieldsforsources(&self, py: Python<'_>, asnames: bool) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let fields = fields_by_source(ms, None)?
                .into_iter()
                .map(|(id, fields)| Ok((id.to_string(), source_fields(ms, fields, asnames)?)))
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::record(fields))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcedirs(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let source = ms.source()?;
            let column = ScalarMeasColumn::new(source.table(), "DIRECTION")?;
            let values = (0..source.row_count())
                .map(|row| {
                    let direction = column.get_direction(row)?;
                    Ok((
                        row.to_string(),
                        Measure::from_casa(&CasaMeasure::Direction(direction)).to_value(),
                    ))
                })
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::record(values))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcetimes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let source = ms.source()?;
            let unit = quantities_unit(source.table(), "TIME", 0)?;
            let values = (0..source.row_count())
                .map(|row| Ok((row.to_string(), quantity(source.f64(row, "TIME")?, unit))))
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::record(values))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn propermotions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let source = ms.source()?;
            let column = "PROPER_MOTION";
            let units = [
                quantities_unit(source.table(), column, 0)?,
                quantities_unit(source.table(), column, 1)?,
            ];
            let motions = (0..source.row_count())
                .map(|row| {
                    let ArrayValue::Float64(values) = source.array(row, column)? else {
                        return Err(MsError::InvalidInput(
                            "SOURCE.PROPER_MOTION must be a floating array".into(),
                        ));
                    };
                    let mut values = values.iter();
                    let (Some(&longitude), Some(&latitude)) = (values.next(), values.next()) else {
                        return Err(MsError::InvalidInput(
                            "SOURCE.PROPER_MOTION has fewer than two values".into(),
                        ));
                    };
                    Ok((
                        row.to_string(),
                        CasaValue::record([
                            ("longitude", quantity(longitude, units[0])),
                            ("latitude", quantity(latitude, units[1])),
                        ]),
                    ))
                })
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::record(motions))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (sourceid = 0, spw = 0), text_signature = "($self, sourceid=0, spw=0)")]
    fn restfreqs(&self, py: Python<'_>, sourceid: i64, spw: i64) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let source = ms.source()?;
            let row = source_line_row(ms, &source, sourceid, spw)?;
            let table = source.table();
            table.column_accessor("TRANSITION")?;
            let frequencies = ArrayMeasColumn::new(table, "REST_FREQUENCY")?;
            if !table.is_cell_defined(row, "REST_FREQUENCY")? {
                return Ok(CasaValue::Bool(false));
            }
            let unit = quantities_unit(table, "REST_FREQUENCY", 0)?;
            let unit =
                casacore_unit(unit).map_err(|error| MsError::InvalidInput(error.to_string()))?;
            let hz =
                casacore_unit("Hz").map_err(|error| MsError::InvalidInput(error.to_string()))?;
            if !unit.conformant(&hz) {
                return Err(MsError::InvalidInput(format!(
                    "REST_FREQUENCY unit {} is not a frequency",
                    unit.name()
                )));
            }
            let scale = unit.val().factor / hz.val().factor;
            let values = frequencies
                .get(row)?
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    let raw = value.as_frequency()?;
                    let frequency = MFrequency::new(raw.hz() * scale, raw.refer());
                    Ok((
                        index.to_string(),
                        CasaValue::from_record(&frequency_to_record(&frequency)),
                    ))
                })
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::record(values))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (sourceid = 0, spw = 0), text_signature = "($self, sourceid=0, spw=0)")]
    fn transitions(&self, py: Python<'_>, sourceid: i64, spw: i64) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let source = ms.source()?;
            let row = source_line_row(ms, &source, sourceid, spw)?;
            ArrayMeasColumn::new(source.table(), "REST_FREQUENCY")?;
            if !source.table().is_cell_defined(row, "TRANSITION")? {
                return Ok(CasaValue::Bool(false));
            }
            let ArrayValue::String(transitions) = source.array(row, "TRANSITION")? else {
                return Err(MsError::InvalidInput(
                    "SOURCE.TRANSITION must be a string array".into(),
                ));
            };
            Ok(CasaValue::TextArray {
                shape: vec![transitions.len()],
                values: transitions.iter().cloned().collect(),
            })
        })?
        .into_python(py)
    }
}
