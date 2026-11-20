// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public weight initialization on a live calibration tool.

use std::path::Path;

use numpy::PyUntypedArray;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyFloat, PyInt, PyList, PyString};

use crate::convert::integer_vector;

use super::PyCalibrater;

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (wtmode = None, dowtsp = None, tsystable = None, gainfield = None, interp = None, spwmap = None), text_signature = "($self, wtmode='nyq', dowtsp=False, tsystable='', gainfield='', interp='', spwmap=[])")]
    #[allow(clippy::too_many_arguments)]
    fn initweights(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] wtmode: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] dowtsp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tsystable: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] gainfield: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] interp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] spwmap: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let wtmode = text(wtmode, "wtmode", "nyq")?;
        let dowtsp = dowtsp
            .as_ref()
            .map(strict_bool)
            .transpose()?
            .unwrap_or(false);
        let tsystable = text(tsystable, "tsystable", "")?;
        let gainfield = text(gainfield, "gainfield", "")?;
        let interp = text(interp, "interp", "")?;
        let spwmap = window_map(spwmap)?;
        self.attached(py, |state| {
            state.init_weights(
                &wtmode,
                dowtsp,
                Path::new(&tsystable),
                &gainfield,
                &interp,
                spwmap,
            )
        })
    }
}

fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

fn strict_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|value| value.is_true())
        .map_err(|_| {
            PyTypeError::new_err("in method 'calibrater_initweights', argument 3 of type 'bool'")
        })
}

fn text(value: Option<Bound<'_, PyAny>>, name: &str, default: &str) -> PyResult<String> {
    match value {
        None => Ok(default.to_owned()),
        Some(value) if value.is_instance_of::<PyString>() => value.extract(),
        Some(value) if value.is_instance_of::<PyBytes>() => {
            value.call_method1("decode", ("utf-8",))?.extract()
        }
        Some(_) => Err(PyTypeError::new_err(format!(
            "argument _{name} must be a string"
        ))),
    }
}

fn window_map(value: Option<Bound<'_, PyAny>>) -> PyResult<Vec<i32>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(
            "argument _spwmap must not be a string",
        ));
    }
    let system_error = || {
        PySystemError::new_err(
            "<built-in function calibrater_initweights> returned a result with an exception set",
        )
    };
    if !value.is_instance_of::<PyList>()
        && !value.is_instance_of::<PyUntypedArray>()
        && !value.is_instance_of::<PyInt>()
        && !value.is_instance_of::<PyFloat>()
    {
        return Err(system_error());
    }
    let (windows, pending) = integer_vector::flatten(
        &value,
        "cyclic spectral-window mapping",
        "non-finite spectral-window mapping",
    )?;
    if pending {
        return Err(system_error());
    }
    Ok(windows.into_iter().map(|window| window as i32).collect())
}
