// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.brightnessunit` / `ia.coordsys` / `ia.history` wrappers delegating to [`casa_imageanalysis::tool::metadata`].

use casa_imageanalysis::tool::metadata::{
    brightness_unit, coordsys_for_axes, image_history, set_brightness_unit, set_coordinate_record,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::coordsys::PyCoordsys;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    fn brightnessunit(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.state()
                .image
                .as_ref()
                .map_or_else(String::new, brightness_unit)
        })
    }

    fn setbrightnessunit(&self, py: Python<'_>, unit: String) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            set_brightness_unit(attachment, &unit, state.history).map_err(raised)
        })
    }

    #[pyo3(signature = (axes=None), text_signature = "($self, axes=[-1])")]
    fn coordsys(&self, py: Python<'_>, axes: Option<IntList>) -> PyResult<Py<PyAny>> {
        let tool = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(PyCoordsys::empty());
            };
            let axes = axes.as_ref().map_or(&[][..], |arg| arg.0.as_slice());
            let (coordinates, record) = coordsys_for_axes(attachment, axes).map_err(raised)?;
            PyCoordsys::from_image_coordinates(&coordinates, record)
        })?;
        Ok(Py::new(py, tool)?.into_any())
    }

    fn setcoordsys(&self, py: Python<'_>, csys: &Bound<'_, PyDict>) -> PyResult<bool> {
        let record = casa_record(py, Some(csys))?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            set_coordinate_record(attachment, record, state.history).map_err(raised)
        })
    }

    #[pyo3(signature = (list=true), text_signature = "($self, list=True)")]
    fn history(&self, py: Python<'_>, list: bool) -> PyResult<Vec<String>> {
        let entries = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            image_history(attachment).map_err(raised)
        })?;
        if list {
            crate::tools::emit(
                py,
                "casars.tools.image",
                &entries
                    .iter()
                    .cloned()
                    .map(crate::tools::LogLine::info)
                    .collect::<Vec<_>>(),
            )?;
        }
        Ok(entries)
    }
}
