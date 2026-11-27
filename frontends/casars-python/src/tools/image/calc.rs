// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.calc` wrapper delegating to [`casa_imageanalysis::tool::calc`].

use casa_imageanalysis::tool::calc::calc_attached_pixels;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixels, verbose=true), text_signature = "($self, pixels, verbose=True)")]
    fn calc(&self, py: Python<'_>, pixels: &str, verbose: bool) -> PyResult<bool> {
        let (result, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((false, Vec::new()));
            };
            calc_attached_pixels(attachment, pixels, verbose, state.history).map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Ok(result)
    }
}
