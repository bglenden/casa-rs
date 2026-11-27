// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.makecomplex` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial`].

use std::path::Path;

use casa_imageanalysis::tool::ops_spatial::make_complex_image;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::Region;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile, imag, region=None, overwrite=false), text_signature = "($self, outfile, imag, region={}, overwrite=False)")]
    fn makecomplex(
        &self,
        py: Python<'_>,
        outfile: &str,
        imag: &str,
        region: Option<&Bound<'_, PyAny>>,
        overwrite: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let region = Region::argument(py, region, "makecomplex")?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                format!(
                    "ia.makecomplex(outfile={outfile:?}, imag={imag:?}, region={region_hist}, overwrite={overwrite})"
                )
            });
            make_complex_image(
                attachment,
                outfile,
                Path::new(imag),
                region.as_ref().map(Region::as_region),
                overwrite,
                history_command,
                external_open_check(),
            )
            .map_err(raised)?;
            Ok(true)
        })
    }
}
