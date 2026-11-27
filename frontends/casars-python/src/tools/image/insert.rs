// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.insert` wrapper delegating to [`casa_imageanalysis::tool::ops_edit::insert_image`].

use casa_imageanalysis::tool::ops_edit::insert_image;
use pyo3::prelude::*;

use crate::convert::DoubleList;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};
use super::subimage::Region;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (infile="", region=None, locate=None, verbose=false), text_signature = "($self, infile='', region={}, locate=[-1.0], verbose=False)")]
    fn insert(
        &self,
        py: Python<'_>,
        infile: &str,
        region: Option<&Bound<'_, PyAny>>,
        locate: Option<DoubleList>,
        verbose: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let region = Region::argument(py, region, "insert")?;
        let locate = locate.map_or_else(|| vec![-1.0], |values| values.0);
        let (outcome, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok((false, Vec::new()));
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                let location = locate
                    .iter()
                    .map(|value| format!("{value:.6}"))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("ia.insert(infile=\"{infile}\", region={region_hist}, locate=[{location}], verbose={verbose})")
            });
            insert_image(
                attachment,
                infile,
                region.as_ref().map(Region::as_region),
                &locate,
                verbose,
                history_command,
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Ok(outcome)
    }
}
