// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public calibrater signatures whose implementations are assigned to open issues.

use std::path::Path;

use casa_calibration::gencal::CalibrationSpecification;
use numpy::PyUntypedArray;
use pyo3::{
    exceptions::PySystemError,
    prelude::*,
    types::{PyBytes, PyList, PyListMethods, PyString, PyTuple},
};

use super::arguments::{append_double_vector, strict_bool, supplied, text};
use super::{PyCalibrater, runtime_error};
use crate::convert::DoubleList;

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCalibrater {
    #[pyo3(signature = (modelimage), text_signature = "($self, modelimage)")]
    fn setmodel(&self, py: Python<'_>, modelimage: &str) -> PyResult<bool> {
        self.with_lifecycle(py, |state| {
            state
                .set_image_model(Path::new(modelimage))
                .map_err(runtime_error)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (caltable = None, time = None, spw = None, antenna = None, pol = None, caltype = None, parameter = None, infile = None, uniform = None), text_signature = "($self, caltable='', time='', spw='', antenna='', pol='', caltype='', parameter=[1.0], infile='', uniform=True)")]
    fn specifycal(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] caltable: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] time: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] spw: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] antenna: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] pol: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] caltype: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] parameter: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] infile: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] uniform: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let table = text(caltable, "caltable", "")?;
        // CASA accepts the time label without assigning it to table rows.
        text(time, "time", "")?;
        let spw = text(spw, "spw", "")?;
        let antenna = text(antenna, "antenna", "")?;
        let polarization = text(pol, "pol", "")?;
        let caltype = text(caltype, "caltype", "")?;
        let (parameters, tuple) = specify_parameters(parameter.as_ref())?;
        let infile = text(infile, "infile", "")?;
        let uniform = uniform
            .as_ref()
            .map(|value| strict_bool(value, "specifycal", 10))
            .transpose()?
            .unwrap_or(true);
        let wrote = self.attached(py, move |state| {
            state.specify_calibration(CalibrationSpecification {
                table: Path::new(&table),
                spw: &spw,
                antenna: &antenna,
                polarization: &polarization,
                caltype: &caltype,
                parameters,
                infile: (!infile.is_empty()).then_some(Path::new(&infile)),
                uniform,
            })
        })?;
        if tuple {
            // SWIG converts the tuple and runs the writer, then reports the
            // outstanding Python conversion exception on its return.
            return Err(PySystemError::new_err(
                "<built-in function calibrater_specifycal> returned a result with an exception set",
            ));
        }
        Ok(wrote)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn state(&self, py: Python<'_>) -> PyResult<bool> {
        use crate::tools::{LogLine, emit};

        let lines = self.with_lifecycle(py, |state| Ok(state.state_lines()))?;
        let attached = lines.is_some();
        let logs = match lines {
            Some(lines) => lines.into_iter().map(LogLine::info).collect(),
            None => vec![LogLine::error("Must first open a MeasurementSet.")],
        };
        emit(py, super::LOGGER, &logs)?;
        Ok(attached)
    }
}

fn specify_parameters(value: Option<&Bound<'_, PyAny>>) -> PyResult<(Vec<f64>, bool)> {
    let Some(value) = value else {
        return Ok((vec![1.], false));
    };
    if value.is_none()
        || value.is_instance(&value.py().import("numpy")?.getattr("complexfloating")?)?
        || value.is_instance_of::<pyo3::types::PyComplex>()
    {
        return Ok((Vec::new(), false));
    }
    if value.is_instance_of::<PyUntypedArray>()
        && value.getattr("dtype")?.getattr("kind")?.extract::<char>()? == 'O'
    {
        return Ok((Vec::new(), false));
    }
    // Scalar text takes the default value of the failed Python-to-double cast.
    // Text within a list or array goes through CASA's vector element conversion.
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Ok((vec![-1.], false));
    }
    if value.is_instance_of::<PyTuple>() {
        return Ok((value.extract::<DoubleList>()?.0, true));
    }
    let mut parameters = Vec::new();
    if let Ok(list) = value.cast::<PyList>()
        && list.iter().any(|item| item.is_instance_of::<PyList>())
    {
        let array = value
            .py()
            .import("numpy")?
            .call_method1("asarray", (value,))?;
        append_double_vector(&array, &mut parameters, 0)?;
    } else {
        append_double_vector(value, &mut parameters, 0)?;
    }
    Ok((parameters, false))
}
