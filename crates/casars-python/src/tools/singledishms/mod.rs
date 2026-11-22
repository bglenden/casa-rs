// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA single-dish spectral preparation tool.

mod atmosphere;
mod baseline;
mod import;
mod line_fit;
pub(crate) mod surface;

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use casa_calibration::{SingleDishSession, SingleDishSmoothing};
use casa_ms::MsSelection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::Variant;

use super::{LogLine, calibrater::selector, emit};

#[pyclass(name = "singledishms", module = "casars.tools", frozen)]
pub(crate) struct PySingleDishMs {
    session: Mutex<SingleDishSession>,
}

#[pymethods]
impl PySingleDishMs {
    #[new]
    fn new() -> Self {
        Self {
            session: Mutex::default(),
        }
    }

    #[pyo3(signature = (ms_name = ""))]
    fn open(&self, py: Python<'_>, ms_name: &str) -> bool {
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .open(ms_name)
        })
    }

    fn close(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .close()
        })
    }

    fn done(&self, py: Python<'_>) -> bool {
        self.close(py)
    }

    fn name(&self, py: Python<'_>) -> PyResult<String> {
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (spw = Variant::Empty, field = Variant::Empty, antenna = Variant::Empty, timerange = Variant::Empty, scan = Variant::Empty, observation = Variant::Empty, polarization = Variant::Empty, beam = Variant::Empty, intent = Variant::Empty, feed = Variant::Empty, taql = "", reindex = true), text_signature = "($self, spw=[], field=[], antenna=[], timerange=[], scan=[], observation=[], polarization=[], beam=[], intent=[], feed=[], taql='', reindex=True)")]
    #[allow(clippy::too_many_arguments)]
    fn set_selection(
        &self,
        py: Python<'_>,
        spw: Variant,
        field: Variant,
        antenna: Variant,
        timerange: Variant,
        scan: Variant,
        observation: Variant,
        polarization: Variant,
        beam: Variant,
        intent: Variant,
        feed: Variant,
        taql: &str,
        reindex: bool,
    ) -> PyResult<bool> {
        let selection = MsSelection {
            spw: selector(&spw),
            field: selector(&field),
            antenna: selector(&antenna),
            timerange: selector(&timerange),
            scan: selector(&scan),
            observation: selector(&observation),
            correlation: selector(&polarization),
            intent: selector(&intent),
            feed: selector(&feed),
            msselect: (!taql.is_empty()).then(|| taql.to_owned()),
            ..MsSelection::default()
        };
        let success = py
            .detach(|| {
                self.session
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .set_selection(selection, reindex)
            })
            .map_err(PyRuntimeError::new_err)?;
        if selector(&beam).is_some_and(|beam| !beam.is_empty()) {
            emit(
                py,
                "casars.tools.singledishms",
                &[LogLine::warning(
                    "Beam selection is not yet supported. Ignoring beam selection",
                )],
            )?;
        }
        Ok(success)
    }

    #[pyo3(signature = (r#type = "gaussian", width = 0.0, datacolumn = "data", outfile = ""), text_signature = "($self, type='gaussian', width=0.0, datacolumn='data', outfile='')")]
    fn smooth(
        &self,
        py: Python<'_>,
        r#type: &str,
        width: f32,
        datacolumn: &str,
        outfile: &str,
    ) -> PyResult<bool> {
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .smooth(SingleDishSmoothing {
                    kernel: r#type,
                    width,
                    datacolumn,
                    output: Path::new(outfile),
                    overwrite: true,
                    history: None,
                })
                .map(|_| true)
        })
        .map_err(PyRuntimeError::new_err)
    }
}
