// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public calibrater signatures whose implementations are assigned to open issues.

use std::path::Path;

use pyo3::prelude::*;

use super::{PyCalibrater, runtime_error};
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

    #[pyo3(signature = (table = None, append = None, t = None, combine = None, degamp = None, degphase = None, visnorm = None, solnorm = None, maskcenter = None, maskedge = None, refant = None), text_signature = "($self, table='', append=False, t=[], combine='', degamp=3, degphase=3, visnorm=False, solnorm=True, maskcenter=0, maskedge=5.0, refant=[])")]
    fn setsolvebandpoly(
        &self,
        table: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        degamp: Option<&Bound<'_, PyAny>>,
        degphase: Option<&Bound<'_, PyAny>>,
        visnorm: Option<&Bound<'_, PyAny>>,
        solnorm: Option<&Bound<'_, PyAny>>,
        maskcenter: Option<&Bound<'_, PyAny>>,
        maskedge: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setsolvebandpoly", 1528)
    }

    #[pyo3(signature = (table = None, append = None, mode = None, splinetime = None, preavg = None, npointaver = None, phasewrap = None, refant = None), text_signature = "($self, table='', append=False, mode='PHAS', splinetime=10800.0, preavg=0.0, npointaver=10, phasewrap=250.0, refant=[])")]
    fn setsolvegainspline(
        &self,
        table: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        splinetime: Option<&Bound<'_, PyAny>>,
        preavg: Option<&Bound<'_, PyAny>>,
        npointaver: Option<&Bound<'_, PyAny>>,
        phasewrap: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setsolvegainspline", 1528)
    }

    #[pyo3(signature = (caltable = None, time = None, spw = None, antenna = None, pol = None, caltype = None, parameter = None, infile = None, uniform = None), text_signature = "($self, caltable='', time='', spw='', antenna='', pol='', caltype='', parameter=[1.0], infile='', uniform=True)")]
    fn specifycal(
        &self,
        caltable: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        pol: Option<&Bound<'_, PyAny>>,
        caltype: Option<&Bound<'_, PyAny>>,
        parameter: Option<&Bound<'_, PyAny>>,
        infile: Option<&Bound<'_, PyAny>>,
        uniform: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "specifycal", 1528)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn state(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "state", 1528)
    }
}
