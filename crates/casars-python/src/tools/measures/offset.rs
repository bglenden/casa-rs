// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inspect scalar measure offsets without losing record values.

use std::collections::HashSet;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::record::Measures;
use super::{PyMeasures, State};
use crate::convert::holder::{Measure, MeasureKind};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use crate::tools::LogLine;

/// Apply the constructor's declared reference origin, following the CASA
/// wrappers that attach some offsets to both the input and output references.
pub(super) fn finish_definition(
    state: &mut State,
    base: Measure,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    if off.0.is_empty() {
        return Ok(base.to_value());
    }
    let kind = base.kind();
    let both = matches!(kind, MeasureKind::Direction | MeasureKind::Frequency);
    let origin = match Measures::read(off) {
        Ok(origin) if origin.values[0].kind() == kind => origin.into_reference_origin(),
        Ok(_) if kind == MeasureKind::Epoch => return Ok(base.to_value()),
        Ok(_) => {
            match kind {
                MeasureKind::Direction => {
                    lines.push(LogLine::warning("Non-matching measure offset\n"))
                }
                MeasureKind::Frequency => lines.push(LogLine::warning(
                    "Illegal offset type specified, not used\n",
                )),
                _ => {}
            }
            lines.push(LogLine::warning(
                "Non-conforming offset measure type\nCall to measures::measure() failed\n",
            ));
            return Ok(CasaValue::empty_record());
        }
        Err(error) => {
            if both {
                lines.push(LogLine::warning(format!(
                    "{}Non-measure type offset in measure conversion\n",
                    error.message()
                )));
            }
            return Ok(base.to_value());
        }
    };
    let mut source = Measures::scalar(base);
    if both {
        source.offset = Some(Box::new(origin.clone()));
    }
    match source.resolve_relative(state, base.refer, Some(origin))? {
        Ok(converted) => Ok(converted.to_value()),
        Err(_) => {
            let message = if kind == MeasureKind::Epoch {
                "Cannot convert due to missing frame information\nEpoch definition failed!"
            } else {
                "Cannot convert due to missing frame information\nCall to measures::measure() failed\n"
            };
            lines.push(LogLine::warning(message));
            Ok(CasaValue::empty_record())
        }
    }
}

enum Visit<'py> {
    Enter(Bound<'py, PyAny>, usize),
    Leave(usize),
}

/// Detect recursive records before the shared Python-to-record conversion.
fn validate_containers(value: &Bound<'_, PyAny>) -> PyResult<()> {
    let numpy = value.py().import("numpy")?;
    let ndarray = numpy.getattr("ndarray")?;
    let mut active = HashSet::new();
    let mut pending = vec![Visit::Enter(value.clone(), 0)];
    while let Some(visit) = pending.pop() {
        let (value, depth) = match visit {
            Visit::Leave(id) => {
                active.remove(&id);
                continue;
            }
            Visit::Enter(value, depth) => (value, depth),
        };
        if depth > 128 {
            return Err(PyValueError::new_err(
                "measure record is too deeply nested (#1252)",
            ));
        }
        if value.is_instance(&ndarray)? {
            // Numeric arrays contain no recursive Python containers.
            if value
                .getattr("dtype")?
                .getattr("hasobject")?
                .extract::<bool>()?
            {
                return Err(PyValueError::new_err(
                    "object measure arrays refused (#1252)",
                ));
            }
            continue;
        }
        let entries: Vec<Bound<'_, PyAny>> = if let Ok(dict) = value.cast::<PyDict>() {
            dict.iter().map(|(_, item)| item).collect()
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            value.try_iter()?.collect::<PyResult<Vec<_>>>()?
        } else {
            continue;
        };
        let id = value.as_ptr() as usize;
        if !active.insert(id) {
            return Err(PyValueError::new_err(
                "cyclic measure record refused (#1252)",
            ));
        }
        pending.push(Visit::Leave(id));
        pending.extend(
            entries
                .into_iter()
                .map(|entry| Visit::Enter(entry, depth + 1)),
        );
    }
    Ok(())
}

/// Copy the supported scalar record without converting integer values to f64.
fn copy_record<'py>(value: &Bound<'py, PyDict>, depth: usize) -> PyResult<Bound<'py, PyDict>> {
    if depth > 128 {
        return Err(PyValueError::new_err(
            "measure record is too deeply nested (#1252)",
        ));
    }
    let out = PyDict::new(value.py());
    let mut entries = value
        .iter()
        .map(|(key, item)| Ok((key.extract::<String>()?, item)))
        .collect::<PyResult<Vec<_>>>()?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, item) in entries {
        out.set_item(key, copy_value(&item, depth + 1)?)?;
    }
    Ok(out)
}

fn copy_value<'py>(value: &Bound<'py, PyAny>, depth: usize) -> PyResult<Bound<'py, PyAny>> {
    if depth > 128 {
        return Err(PyValueError::new_err(
            "measure record is too deeply nested (#1252)",
        ));
    }
    if let Ok(record) = value.cast::<PyDict>() {
        return Ok(copy_record(record, depth)?.into_any());
    }
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(PyList::new(
            value.py(),
            list.iter()
                .map(|item| copy_value(&item, depth + 1))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return Ok(PyTuple::new(
            value.py(),
            tuple
                .iter()
                .map(|item| copy_value(&item, depth + 1))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any());
    }
    if value.is_instance(&value.py().import("numpy")?.getattr("ndarray")?)? {
        return value.call_method0("copy");
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        let text: String = bytes.call_method1("decode", ("utf-8",))?.extract()?;
        return Ok(pyo3::types::PyString::new(
            value.py(),
            text.split('\0').next().unwrap_or_default(),
        )
        .into_any());
    }
    if value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyBool>()
        || value.is_instance_of::<PyInt>()
        || value.is_instance_of::<PyFloat>()
        || value.is_instance_of::<PyComplex>()
        || value.is_instance(&value.py().import("numpy")?.getattr("generic")?)?
    {
        return Ok(value.clone());
    }
    Err(PyTypeError::new_err(
        "unsupported value in measure offset record",
    ))
}

#[pymethods]
impl PyMeasures {
    /// Return a supported scalar measure's offset, or an empty record.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn getoffset(&self, py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let record = v
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("_v is not a dictionary"))?;
        validate_containers(v)?;
        // Snapshot the offset before self.call releases the GIL, so concurrent
        // mutation cannot change the record after its validation.
        let copied_offset = match record.get_item("offset")? {
            Some(value) => match value.cast::<PyDict>() {
                Ok(offset) => Some(copy_record(offset, 0)?.into_any().unbind()),
                Err(_) => None,
            },
            None => None,
        };
        let RecordArg(mut fields) = v.extract()?;
        let offset = fields.remove("offset");
        let result = self.call(py, move |_, lines| {
            let base = match Measures::read(&RecordArg(fields)) {
                Ok(value) => value.values[0],
                Err(error) => {
                    let message = error.message();
                    if message.contains("not yet implemented") {
                        return Err(PyNotImplementedError::new_err(format!("{message} (#1252)")));
                    }
                    lines.push(LogLine::warning(format!("{message}Incorrect input type for getoffset()")));
                    return Ok(CasaValue::Bool(false));
                }
            };
            let Some(offset) = offset else {
                lines.push(LogLine::info("Measures does not have an offset"));
                return Ok(CasaValue::Bool(false));
            };
            let Variant::Record(offset) = offset else {
                lines.push(LogLine::error("Incorrect data type for offset"));
                return Ok(CasaValue::Bool(false));
            };
            match Measures::read(&RecordArg((*offset).clone())) {
                Ok(value) if value.values[0].kind() == base.kind() => Ok(CasaValue::Bool(true)),
                Ok(_) => { lines.push(LogLine::warning("Unmatched offset type in MeasureHolder::fromRecord\nIncorrect input type for getoffset()")); Ok(CasaValue::Bool(false)) }
                Err(error) => { lines.push(LogLine::warning(format!("{}Incorrect input type for getoffset()",error.message()))); Ok(CasaValue::Bool(false)) }
            }
        })?;
        if !result.bind(py).extract::<bool>()? {
            return Ok(PyDict::new(py).into_any().unbind());
        }
        copied_offset.ok_or_else(|| PyTypeError::new_err("offset is not a dictionary"))
    }
}
