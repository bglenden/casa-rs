// SPDX-License-Identifier: LGPL-3.0-or-later
//! Extra Cartesian values of scalar measures.

use std::collections::HashSet;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::RecordArg;
use crate::convert::holder::{Measure, MeasureValue};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

enum Visit<'py> {
    Enter(Bound<'py, PyAny>, usize),
    Leave(usize),
}

/// Guard recursive records before the shared Python-to-record conversion.
pub(super) fn read_record(value: &Bound<'_, PyAny>) -> PyResult<RecordArg> {
    if !value.is_instance_of::<PyDict>() {
        return Err(PyTypeError::new_err("_a is not a dictionary"));
    }
    let mut pending = vec![Visit::Enter(value.clone(), 0)];
    let mut active = HashSet::new();
    while let Some(visit) = pending.pop() {
        let (value, depth) = match visit {
            Visit::Leave(identity) => {
                active.remove(&identity);
                continue;
            }
            Visit::Enter(value, depth) => (value, depth),
        };
        let Ok(record) = value.cast::<PyDict>() else {
            continue;
        };
        if depth > 128 {
            return Err(PyValueError::new_err(
                "measure record is too deeply nested (#1053)",
            ));
        }
        let identity = value.as_ptr() as usize;
        if !active.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic measure record refused (#1053)",
            ));
        }
        pending.push(Visit::Leave(identity));
        pending.extend(record.iter().map(|(_, item)| Visit::Enter(item, depth + 1)));
    }
    value.extract()
}

/// `me.addxvalue`: the direction cosines or the position vector.
pub(super) fn addxvalue(record: &RecordArg, lines: &mut Vec<LogLine>) -> PyResult<CasaValue> {
    let read = match Measure::from_record(&record.0) {
        Ok(read) => read,
        Err(error) => {
            let message = error.message();
            if message.contains("not yet implemented") {
                return Err(PyNotImplementedError::new_err(format!("{message} (#1053)")));
            }
            lines.push(LogLine::error(format!(
                "{message}Non-measure type a in measure conversion\n"
            )));
            return Ok(CasaValue::empty_record());
        }
    };
    if let Some(warning) = read.warning {
        lines.push(LogLine::warning(warning));
    }
    let (unit, values) = match read.measure.value {
        MeasureValue::Direction(values) => ("", values),
        MeasureValue::Position(values) => ("m", values),
        MeasureValue::Epoch(_) | MeasureValue::Frequency(_) => return Ok(CasaValue::empty_record()),
    };
    Ok(CasaValue::record([
        ("unit", CasaValue::Text(unit.to_string())),
        ("value", CasaValue::floats(values.to_vec())),
    ]))
}
