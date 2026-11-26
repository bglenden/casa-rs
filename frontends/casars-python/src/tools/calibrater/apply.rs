// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-library records and on-the-fly apply methods.

use std::io::ErrorKind;

use casa_calibration::{
    ApplyCalibrationTableSpec, CalibraterLifecycleError, CallibError, load_callibrary_record,
    validate_callibrary_record,
};
use casa_contracts::{ParameterValue, casacore_value_to_casa, parameter_value_to_casacore};
use casa_core::{RecordValue, Value};
use pyo3::exceptions::{PyRuntimeError, PySystemError, PyTypeError};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyTuple},
};

use super::{
    LOGGER, PyCalibrater,
    arguments::{
        append_double_vector, listing_selector, number, strict_bool, supplied, text,
        window_map_with_status,
    },
    runtime_error,
};
use crate::convert::reply_value::reply_value;
use crate::parameters::parameter_value;
use crate::tools::{LogLine, emit};

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn activityrec(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.with_lifecycle(py, |state| Ok(state.activity_record()))?;
        let value =
            casacore_value_to_casa(&Value::Record(record)).map_err(PyRuntimeError::new_err)?;
        reply_value(py, &value)
    }

    #[pyo3(signature = (r#type = None, t = None, table = None, field = None, interp = None, select = None, calwt = None, spwmap = None, opacity = None), text_signature = "($self, type='', t=0.0, table='', field=[], interp='linear', select='', calwt=False, spwmap=[-1], opacity=[0.0])")]
    #[allow(clippy::too_many_arguments)]
    fn setapply(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] r#type: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] t: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] table: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] field: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] interp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] select: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] calwt: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] spwmap: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] opacity: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let kind = text(r#type, "type", "")?;
        let pending_time = number(t, "setapply", 3, 0.0).is_err();
        let mut spec = ApplyCalibrationTableSpec::new(text(table, "table", "")?);
        let field = listing_selector(field.as_ref())?;
        let interpolation = text(interp, "interp", "linear")?;
        // The tool wrapper forwards an empty SPW selector for traditional apply.
        let _selection = text(select, "select", "")?;
        if let Some(value) = calwt {
            spec.calwt = strict_bool(&value, "setapply", 8)?;
        }
        let (windows, pending_windows) = window_map_with_status(spwmap, &[-1])?;
        spec.spwmap = windows;
        let tableless_opacity = spec.path.as_os_str().is_empty()
            && (kind.eq_ignore_ascii_case("TOpac") || kind.eq_ignore_ascii_case("TfOpac"));
        let mut opacity_values = Vec::new();
        let pending_opacity = opacity.is_some_and(|value| {
            if tableless_opacity && let Ok(list) = value.cast::<PyList>() {
                for item in list.iter() {
                    let start = opacity_values.len();
                    if append_double_vector(&item, &mut opacity_values, 1).is_err() {
                        // CASA treats an unsupported list element as zero.
                        opacity_values.truncate(start);
                        opacity_values.push(0.0);
                    }
                }
                return false;
            }
            value.is_instance_of::<PyTuple>()
                || append_double_vector(&value, &mut opacity_values, 0).is_err()
        });
        let mut warn_about_opacity = false;
        let warning = &mut warn_about_opacity;
        let configured = self.attached(py, move |state| {
            if tableless_opacity {
                let result = state.set_tableless_opacity(&kind, opacity_values);
                *warning = !matches!(result, Err(CalibraterLifecycleError::Unattached));
                result
            } else {
                state.set_traditional_apply(spec, &kind, &field, &interpolation)
            }
        });
        if warn_about_opacity {
            emit(
                py,
                LOGGER,
                &[
                    LogLine::warning("Invoking opacity application without a caltable (e.g.,"),
                    LogLine::warning(
                        " via opacity=[...] in calibration tasks) will soon be disabled.",
                    ),
                    LogLine::warning(" Please begin using gencal to generate an opacity caltable,"),
                    LogLine::warning(" and then supply that table in the standard manner."),
                ],
            )?;
        }
        let configured = configured?;
        // CASA's SWIG numerics can set an exception and still call C++ with a
        // fallback value. Registration is visible when Python reports it.
        if pending_time || pending_windows || pending_opacity {
            return Err(setapply_conversion_error());
        }
        Ok(configured)
    }

    #[pyo3(signature = (applymode = None), text_signature = "($self, applymode='')")]
    fn correct(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] applymode: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let mode = text(applymode, "applymode", "")?;
        self.attached_result(py, move |state| state.correct_visibilities(&mode))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn corrupt(&self, py: Python<'_>) -> PyResult<bool> {
        self.attached_result(py, |state| state.corrupt_model())
    }

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
            let value =
                casacore_value_to_casa(&Value::Record(record)).map_err(PyRuntimeError::new_err)?;
            return reply_value(py, &value);
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

fn setapply_conversion_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function calibrater_setapply> returned a result with an exception set",
    )
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
