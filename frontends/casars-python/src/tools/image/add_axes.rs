// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.adddegaxes` wrapper delegating to [`casa_imageanalysis::tool::add_axes`].

use casa_imageanalysis::tool::add_axes::{AddDegAxesRequest, add_degenerate_axes};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, tool_error};

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", direction=false, spectral=false, stokes="", linear=false, tabular=false, overwrite=false, silent=false), text_signature = "($self, outfile='', direction=False, spectral=False, stokes='', linear=False, tabular=False, overwrite=False, silent=False)")]
    fn adddegaxes(
        &self,
        py: Python<'_>,
        outfile: &str,
        direction: bool,
        spectral: bool,
        stokes: &str,
        linear: bool,
        tabular: bool,
        overwrite: bool,
        silent: bool,
    ) -> PyResult<Py<Self>> {
        let request = AddDegAxesRequest {
            outfile,
            direction,
            spectral,
            stokes,
            linear,
            tabular,
            overwrite,
            silent,
        };
        let (handle, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((None, Vec::new()));
            };
            let (handle, logs) =
                add_degenerate_axes(attachment, &request, state.history, external_open_check())
                    .map_err(tool_error)?;
            Ok::<_, PyErr>((Some(handle), logs))
        })?;
        emit_tool_logs(py, &logs)?;
        Py::new(py, handle.map_or_else(Self::new, Self::attached))
    }
}
