// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-library records and on-the-fly apply methods.

use std::io::ErrorKind;

use casa_calibration::{
    CalibraterLifecycleError, CallibError, load_callibrary_record, validate_callibrary_record,
};
use casa_provider_contracts::{
    ParameterValue, casacore_value_to_casa, parameter_value_to_casacore,
};
use casa_types::{RecordValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::{prelude::*, types::PyDict};

use super::{
    LOGGER, PyCalibrater,
    arguments::{supplied, text},
    runtime_error,
};
use crate::convert::reply_value::reply_value;
use crate::parameters::parameter_value;
use crate::tools::{LogLine, emit};

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (filein), text_signature = "($self, filein)")]
    fn parsecallibfile(&self, py: Python<'_>, filein: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut filename = text(Some(filein.clone()), "filein", "")?;
        if let Some(end) = filename.find('\0') {
            filename.truncate(end);
        }
        if filename.is_empty() {
            return Err(PyRuntimeError::new_err(
                "Cal Library parser failed to open file",
            ));
        }
        let record = py
            .detach(|| load_callibrary_record(&filename))
            .map_err(|error| {
                PyRuntimeError::new_err(match error {
                    CallibError::ReadFile { source, .. }
                        if source.kind() == ErrorKind::NotFound =>
                    {
                        format!("Specified cal library file ('{filename}') does not exist!")
                    }
                    CallibError::ReadFile { .. } => "Cal Library parser failed to open file".into(),
                    CallibError::ParseLine { line_number, .. } => {
                        format!("Cal Library file: Parse error on line {line_number}: syntax error")
                    }
                    error => error.to_string(),
                })
            })?;
        let value =
            casacore_value_to_casa(&Value::Record(record)).map_err(PyRuntimeError::new_err)?;
        reply_value(py, &value)
    }
    #[pyo3(signature = (), text_signature = "($self)")]
    fn returndict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.with_lifecycle(py, |state| match state.configuration_record() {
            Ok(record) => Ok(Some(record)),
            Err(CalibraterLifecycleError::Unattached) => Ok(None),
            Err(error) => Err(runtime_error(error)),
        })?;
        if let Some(record) = record {
            return reply_value(py, &record);
        }
        emit(
            py,
            LOGGER,
            &[LogLine::error("Must first open a MeasurementSet.")],
        )?;
        Ok(PyDict::new(py).into_any().unbind())
    }

    #[pyo3(signature = (callib = None), text_signature = "($self, callib={})")]
    fn setcallib(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] callib: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let record = library_argument(callib.as_ref())?;
        self.attached(py, move |state| state.set_callibrary(&record))
    }

    #[pyo3(signature = (callib = None), text_signature = "($self, callib={})")]
    fn validatecallib(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] callib: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let record = library_argument(callib.as_ref())?;
        let diagnostics = py
            .detach(|| validate_callibrary_record(&record))
            .map_err(PyRuntimeError::new_err)?;
        let lines = diagnostics
            .into_iter()
            .map(LogLine::error)
            .collect::<Vec<_>>();
        emit(py, LOGGER, &lines)?;
        Ok(true)
    }
}

fn library_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<RecordValue> {
    let Some(value) = value else {
        return Ok(RecordValue::default());
    };
    let record = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("_callib is not a dictionary"))?;
    let converted = parameter_value_to_casacore(&library_parameters(record, 0)?)
        .map_err(PyRuntimeError::new_err)?;
    let Value::Record(record) = converted else {
        unreachable!("library argument was converted as a record");
    };
    Ok(record)
}

fn library_parameters(record: &Bound<'_, PyDict>, depth: usize) -> PyResult<ParameterValue> {
    if depth > 32 {
        return Err(PyRuntimeError::new_err("record nesting is too deep"));
    }
    let fields = record
        .iter()
        .filter(|(_, value)| !value.is_none())
        .map(|(key, value)| {
            let value = match value.cast::<PyDict>() {
                Ok(nested) => library_parameters(nested, depth + 1)?,
                Err(_) => parameter_value(&value)?,
            };
            Ok((key.extract::<String>()?, value))
        })
        .collect::<PyResult<_>>()?;
    Ok(ParameterValue::Table(fields))
}
