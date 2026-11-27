// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.calcmask` wrapper delegating to [`casa_imageanalysis::tool::mask_expression`].

use casa_imageanalysis::tool::mask_expression::calc_mask;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (mask="", name="", asdefault=true), text_signature = "($self, mask='', name='', asdefault=True)")]
    fn calcmask(&self, py: Python<'_>, mask: &str, name: &str, asdefault: bool) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            calc_mask(attachment, mask, name, asdefault, state.history).map_err(raised)
        })
    }
}
