// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public calibrater signatures whose implementations are assigned to open issues.

use std::path::Path;

use casa_calibration::CalibrationSpecification;
use pyo3::{exceptions::PySystemError, prelude::*, types::PyTuple};

use super::arguments::{strict_bool, supplied, text};
use super::{PyCalibrater, runtime_error};
use crate::convert::DoubleList;
use crate::tools::not_yet_implemented_in;

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCalibrater {
    #[pyo3(signature = (tablein, reference = None, tableout = None, transfer = None, listfile = None, append = None, refspwmap = None, gainthreshold = None, antenna = None, timerange = None, scan = None, incremental = None, fitorder = None, display = None), text_signature = "($self, tablein, reference=[], tableout='', transfer=[], listfile='', append=False, refspwmap=[-1], gainthreshold=-1.0, antenna='', timerange='', scan='', incremental=False, fitorder=1, display=False)")]
    fn fluxscale(
        &self,
        tablein: &Bound<'_, PyAny>,
        reference: Option<&Bound<'_, PyAny>>,
        tableout: Option<&Bound<'_, PyAny>>,
        transfer: Option<&Bound<'_, PyAny>>,
        listfile: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        refspwmap: Option<&Bound<'_, PyAny>>,
        gainthreshold: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        timerange: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        incremental: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        display: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "fluxscale", 1528)
    }

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
        let tuple = parameter
            .as_ref()
            .is_some_and(|value| value.is_instance_of::<PyTuple>());
        let parameters = match parameter {
            None => vec![1.],
            Some(value) if value.is_none() => Vec::new(),
            Some(value) => value.extract::<DoubleList>()?.0,
        };
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
    fn state(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "state", 1528)
    }
}
