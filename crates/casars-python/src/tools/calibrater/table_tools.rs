// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public calibration-table maintenance and accumulation.

use std::path::Path;

use casa_calibration::{
    CalibraterLifecycleError, CalibrationAccumulationRequest, update_calibration_table,
};
use pyo3::prelude::*;

use crate::convert::Variant;

use super::arguments::{number, strict_bool, supplied, text, window_map};
use super::{PyCalibrater, runtime_error, selector};

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (tablein = None, incrtable = None, tableout = None, field = Variant::Empty, calfield = Variant::Empty, interp = None, t = None, spwmap = None), text_signature = "($self, tablein='', incrtable='', tableout='', field=[], calfield=[], interp='linear', t=-1.0, spwmap=[-1])")]
    #[allow(clippy::too_many_arguments)]
    fn accumulate(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] tablein: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] incrtable: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tableout: Option<Bound<'_, PyAny>>,
        field: Variant,
        calfield: Variant,
        #[pyo3(from_py_with = supplied)] interp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] t: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] spwmap: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let request = CalibrationAccumulationRequest {
            table_in: text(tablein, "tablein", "")?.into(),
            incremental: text(incrtable, "incrtable", "")?.into(),
            table_out: text(tableout, "tableout", "")?.into(),
            field: selector(&field).unwrap_or_default(),
            calibration_field: selector(&calfield).unwrap_or_default(),
            interpolation: text(interp, "interp", "linear")?,
            interval_seconds: number(t, "accumulate", 8, -1.)?,
            spectral_window_map: window_map(spwmap, "accumulate", &[-1])?,
        };
        self.attached(py, |state| state.accumulate_table(&request))
    }

    #[pyo3(signature = (caltable, partype, caltype, singlechan), text_signature = "($self, caltable, partype, caltype, singlechan)")]
    fn createcaltable(
        &self,
        py: Python<'_>,
        caltable: Bound<'_, PyAny>,
        partype: Bound<'_, PyAny>,
        caltype: Bound<'_, PyAny>,
        singlechan: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let caltable = text(Some(caltable), "caltable", "")?;
        let partype = text(Some(partype), "partype", "")?;
        let caltype = text(Some(caltype), "caltype", "")?;
        let singlechan = strict_bool(singlechan, "createcaltable", 5)?;
        self.attached(py, |state| {
            match state.create_table(Path::new(&caltable), &partype, &caltype, singlechan) {
                Err(CalibraterLifecycleError::CreateTable(message))
                    if message == "the MeasurementSet and output calibration table overlap" =>
                {
                    Err(CalibraterLifecycleError::CreateTable(format!(
                        "Invalid Table operation: Cannot copy/rename; target table {caltable} is still open (is in the table cache)"
                    )))
                }
                result => result,
            }
        })
    }

    #[pyo3(signature = (caltable), text_signature = "($self, caltable)")]
    fn updatecaltable(&self, py: Python<'_>, caltable: Bound<'_, PyAny>) -> PyResult<bool> {
        let caltable = text(Some(caltable), "caltable", "")?;
        py.detach(|| update_calibration_table(Path::new(&caltable)))
            .map_err(runtime_error)?;
        Ok(true)
    }

    #[pyo3(signature = (tablein, tableout, field = Variant::Empty, smoothtype = None, smoothtime = None, ratesmooth = None), text_signature = "($self, tablein, tableout, field=[], smoothtype='median', smoothtime=60.0, ratesmooth=False)")]
    #[allow(clippy::too_many_arguments)]
    fn smooth(
        &self,
        py: Python<'_>,
        tablein: Bound<'_, PyAny>,
        tableout: Bound<'_, PyAny>,
        field: Variant,
        #[pyo3(from_py_with = supplied)] smoothtype: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] smoothtime: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] ratesmooth: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let tablein = text(Some(tablein), "tablein", "")?;
        let tableout = text(Some(tableout), "tableout", "")?;
        let field = selector(&field).unwrap_or_default();
        let smoothtype = text(smoothtype, "smoothtype", "median")?;
        let seconds = number(smoothtime, "smooth", 6, 60.)?;
        let ratesmooth = ratesmooth
            .as_ref()
            .map(|value| strict_bool(value, "smooth", 7))
            .transpose()?
            .unwrap_or(false);
        self.attached(py, |state| {
            state.smooth_table(
                Path::new(&tablein),
                Path::new(&tableout),
                &field,
                &smoothtype,
                seconds,
                ratesmooth,
            )
        })
    }

    #[pyo3(signature = (tablein, tableout, refantmode = None, refant = Variant::Empty), text_signature = "($self, tablein, tableout, refantmode='flexible', refant=[])")]
    fn rerefant(
        &self,
        py: Python<'_>,
        tablein: Bound<'_, PyAny>,
        tableout: Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] refantmode: Option<Bound<'_, PyAny>>,
        refant: Variant,
    ) -> PyResult<bool> {
        let tablein = text(Some(tablein), "tablein", "")?;
        let tableout = text(Some(tableout), "tableout", "")?;
        let mode = text(refantmode, "refantmode", "flexible")?;
        let selector = selector(&refant).unwrap_or_default();
        self.attached(py, |state| {
            state.rereference_table(Path::new(&tablein), Path::new(&tableout), &mode, &selector)
        })
    }
}
