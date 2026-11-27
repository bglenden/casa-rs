// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.continuumsub` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::continuum_sub`].

use casa_imageanalysis::tool::ops_transform::continuum_sub;
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Fit a polynomial to the chosen channels and return the residual image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outline="", outcont="continuumsub.im", region=RegionArgument::default(), channels=IntList::of(&[-1]), pol="", fitorder=0, overwrite=false), text_signature = "($self, outline='', outcont='continuumsub.im', region={}, channels=[-1], pol='', fitorder=0, overwrite=False)")]
    fn continuumsub(
        &self,
        py: Python<'_>,
        outline: &str,
        outcont: &str,
        region: RegionArgument,
        channels: IntList,
        pol: &str,
        fitorder: i64,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let product = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok(None);
            };
            let history_command = state.history.then(|| {
                let chan_str = channels
                    .0
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                let region_hist = region.history();
                format!(
                    "ia.continuumsub(outline={outline:?}, outcont={outcont:?}, region={region_hist}, channels=[{chan_str}], pol={pol:?}, fitorder={fitorder}, overwrite={overwrite})"
                )
            });
            continuum_sub(
                attached,
                outline,
                outcont,
                region.selection(),
                &channels.0,
                fitorder,
                overwrite,
                history_command,
                external_open_check(),
            )
            .map(Some)
            .map_err(raised)
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
