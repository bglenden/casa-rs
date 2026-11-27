// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.rotatebeam` wrapper delegating to [`casa_imageanalysis::tool::beam_ops`].

use casa_imageanalysis::tool::beam_ops::rotate_beam_quantity;
use pyo3::prelude::*;

use crate::convert::{Variant, quantity};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::beam_edit::input_history;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (angle=Variant::Empty), text_signature = "($self, angle=[])")]
    fn rotatebeam(&self, py: Python<'_>, angle: Variant) -> PyResult<bool> {
        let messages = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let rotation = quantity(&angle)?;
            let angle_history = input_history(&angle);
            rotate_beam_quantity(attachment, &rotation, state.history, &angle_history)
                .map(Some)
                .map_err(raised)
        })?;
        let Some(messages) = messages else {
            return Ok(false);
        };
        let logs = messages.into_iter().map(LogLine::info).collect::<Vec<_>>();
        emit(py, "casars.tools.image", &logs)?;
        Ok(true)
    }
}
