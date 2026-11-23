// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_tables::Table;
use casa_types::ScalarValue;
use casa_types::measures::MEpoch;
use casa_types::measures::direction::MDirection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::names::checked_row;
use super::scan_queries::{FieldSelector, field_ids};
use crate::convert::RecordArg;
use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::value::CasaValue;
use crate::tools::measures::observation_epoch;

fn epoch_selection(epoch: &RecordArg) -> PyResult<Option<MEpoch>> {
    if epoch.0.is_empty() {
        return Ok(None);
    }
    let mut log = Vec::new();
    observation_epoch(epoch, &mut log)
        .map_err(PyRuntimeError::new_err)?
        .map(Some)
        .ok_or_else(|| PyRuntimeError::new_err("Epoch cannot be converted to an MEpoch"))
}

fn direction_record(direction: MDirection) -> CasaValue {
    Measure::from_casa(&CasaMeasure::Direction(direction)).to_value()
}

fn selected_value(table: &Table, row: usize, column: &str) -> MsResult<ScalarValue> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(&[row])?
        .pop()
        .flatten()
        .ok_or_else(|| MsError::InvalidInput(format!("{column} is undefined at MAIN row {row}")))
}

fn baseline_sample(ms: &MeasurementSet, row: usize) -> MsResult<([i32; 2], f64, f64)> {
    let main = ms.main_table();
    let (a, b, time, interval) = (
        selected_value(main, row, "ANTENNA1")?,
        selected_value(main, row, "ANTENNA2")?,
        selected_value(main, row, "TIME")?,
        selected_value(main, row, "INTERVAL")?,
    );
    match (a, b, time, interval) {
        (
            ScalarValue::Int32(a),
            ScalarValue::Int32(b),
            ScalarValue::Float64(time),
            ScalarValue::Float64(interval),
        ) => Ok(([a, b], time, interval)),
        _ => Err(MsError::InvalidInput(
            "Invalid MAIN baseline or time metadata".into(),
        )),
    }
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (fieldid = 0, epoch = RecordArg::empty()), text_signature = "($self, fieldid=0, epoch={})")]
    fn phasecenter(&self, py: Python<'_>, fieldid: i64, epoch: RecordArg) -> PyResult<Py<PyAny>> {
        let epoch = epoch_selection(&epoch)?;
        self.with_ms(py, |ms| {
            let row = checked_row(fieldid, ms.field()?.row_count(), "Field ID")?;
            Ok(direction_record(ms.metadata_field_direction(
                row,
                false,
                epoch.as_ref(),
            )?))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (field = FieldSelector::Id(0), epoch = RecordArg::empty()), text_signature = "($self, field=0, epoch={})")]
    fn refdir(
        &self,
        py: Python<'_>,
        field: FieldSelector,
        epoch: RecordArg,
    ) -> PyResult<Py<PyAny>> {
        let epoch = epoch_selection(&epoch)?;
        self.with_ms(py, |ms| {
            let row = match field {
                FieldSelector::Id(id) => checked_row(id, ms.field()?.row_count(), "Field ID")?,
                FieldSelector::Name(name) => {
                    let fields = field_ids(ms, &name)?;
                    *fields.first().expect("field_ids returns a nonempty match") as usize
                }
                FieldSelector::Unsupported => {
                    return Err(MsError::InvalidInput(
                        "Field must be an integer or name".into(),
                    ));
                }
            };
            Ok(direction_record(ms.metadata_field_direction(
                row,
                true,
                epoch.as_ref(),
            )?))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (rownum = 0, interpolate = false, initialrow = 0), text_signature = "($self, rownum=0, interpolate=False, initialrow=0)")]
    fn pointingdirection(
        &self,
        py: Python<'_>,
        rownum: i64,
        interpolate: bool,
        initialrow: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let row = checked_row(rownum, ms.row_count(), "MAIN row")?;
            let (ids, time, interval) = baseline_sample(ms, row)?;
            let [first, last] = ms.pointing()?.metadata_baseline_directions(
                ids,
                time,
                interval,
                initialrow.max(0) as usize,
                interpolate,
            )?;
            let antenna = |id, direction| {
                CasaValue::record([
                    ("id", CasaValue::Int(i64::from(id))),
                    ("pointingdirection", direction_record(direction)),
                ])
            };
            Ok(CasaValue::record([
                ("time", CasaValue::Float(time)),
                ("antenna1", antenna(ids[0], first)),
                ("antenna2", antenna(ids[1], last)),
            ]))
        })?
        .into_python(py)
    }
}
