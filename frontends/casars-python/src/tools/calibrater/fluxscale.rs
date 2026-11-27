// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct flux-scale transfer from the attached calibrater.

use std::path::PathBuf;

use casa_calibration::{
    fluxscale::{FluxScaleOptions, FluxScaleRequest},
    solve::CalibraterLifecycleError,
};
use casa_tasks::calibration::fluxscale_record;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use super::arguments::{listing_selector, strict_bool, supplied, text, window_map};
use super::{PyCalibrater, runtime_error};
use crate::convert::reply_value::reply_value;

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (tablein, reference = None, tableout = None, transfer = None, listfile = None, append = None, refspwmap = None, gainthreshold = None, antenna = None, timerange = None, scan = None, incremental = None, fitorder = None, display = None), text_signature = "($self, tablein, reference=[], tableout='', transfer=[], listfile='', append=False, refspwmap=[-1], gainthreshold=-1.0, antenna='', timerange='', scan='', incremental=False, fitorder=1, display=False)")]
    #[allow(clippy::too_many_arguments)]
    fn fluxscale(
        &self,
        py: Python<'_>,
        tablein: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] reference: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tableout: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] transfer: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] listfile: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] append: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] refspwmap: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] gainthreshold: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] antenna: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] timerange: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] scan: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] incremental: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] fitorder: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] display: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let flag = |value: Option<Bound<'_, PyAny>>, position| {
            value.map_or(Ok(false), |value| {
                strict_bool(&value, "fluxscale", position)
            })
        };
        let fields = |value: Option<Bound<'_, PyAny>>| -> PyResult<Vec<String>> {
            let selector = listing_selector(value.as_ref())?;
            Ok((!selector.is_empty())
                .then_some(selector)
                .into_iter()
                .collect())
        };
        let listfile = text(listfile, "listfile", "")?;
        let whitespace = [' ', '\t', '\n', '\r'];
        let listfile = whitespace
            .into_iter()
            .fold(listfile.as_str(), str::trim_start_matches);
        let listfile = whitespace.into_iter().fold(listfile, str::trim_end_matches);
        let refspwmap = window_map(refspwmap, "fluxscale", &[-1])?;
        let gainthreshold = gainthreshold.map_or(Ok(-1.), |value| {
            value.extract::<f32>().map_err(|_| {
                PyTypeError::new_err("in method 'calibrater_fluxscale', argument 9 of type 'float'")
            })
        })?;
        let fitorder = fitorder.map_or(Ok(1), |value| {
            value.extract::<i64>().map_err(|_| {
                PyTypeError::new_err("in method 'calibrater_fluxscale', argument 14 of type 'long'")
            })
        })? as i32;
        let request = FluxScaleRequest {
            options: FluxScaleOptions {
                antenna: text(antenna, "antenna", "")?,
                timerange: text(timerange, "timerange", "")?,
                scan: text(scan, "scan", "")?,
                fitorder,
                display: flag(display, 15)?,
                listfile: (!listfile.is_empty()).then(|| PathBuf::from(listfile)),
                measurement_set: None,
            },
            input_table: PathBuf::from(text(Some(tablein.clone()), "tablein", "")?),
            output_table: PathBuf::from(text(tableout, "tableout", "")?),
            reference_fields: fields(reference)?,
            transfer_fields: fields(transfer)?,
            refspwmap: if refspwmap == [-1] {
                Vec::new()
            } else {
                refspwmap
            },
            gainthreshold: (gainthreshold >= 0.).then_some(f64::from(gainthreshold)),
            incremental: flag(incremental, 13)?,
            append: flag(append, 7)?,
        };
        let report = self.with_lifecycle(py, move |state| {
            state.fluxscale_tool(request).map_err(|error| match error {
                CalibraterLifecycleError::Unattached => {
                    PyRuntimeError::new_err("Must first open a MeasurementSet.")
                }
                error => runtime_error(error),
            })
        })?;
        reply_value(py, &fluxscale_record(&report))
    }
}
