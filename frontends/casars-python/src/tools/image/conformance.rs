// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.isconform` wrapper delegating to [`casa_imageanalysis::tool::conformance`].

use std::path::Path;

use casa_imageanalysis::tool::conformance::is_conform;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    fn isconform(&self, py: Python<'_>, other: &str) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            is_conform(attachment, other, Path::new(other)).map_err(raised)
        })
    }
}
