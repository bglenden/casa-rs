// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.maskhandler` wrapper delegating to [`casa_imageanalysis::tool::masks`].

use casa_imageanalysis::tool::masks::mask_handler_casa;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::raised;
use super::records::string_vector;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (op="default", name=None), text_signature = "($self, op='default', name=[])")]
    fn maskhandler(
        &self,
        py: Python<'_>,
        op: &str,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let names = string_vector(name, "name")?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            mask_handler_casa(attachment, op, &names, state.history).map_err(raised)
        })
    }
}
