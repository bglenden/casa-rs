// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.pad` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial`].

use std::path::Path;

use casa_imageanalysis::analysis::ImagePadOptions;
use casa_imageanalysis::tool::ops_spatial::pad_image;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Select an image region and pad the two direction axes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", npixels=1, value=0.0, padmask=false, overwrite=false, region=RegionArgument::default(), r#box="", chans="", stokes="", mask="", stretch=false, wantreturn=true), text_signature = "($self, outfile='', npixels=1, value=0.0, padmask=False, overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn pad(
        &self,
        py: Python<'_>,
        outfile: &str,
        npixels: i64,
        value: f64,
        #[pyo3(from_py_with = require_bool)] padmask: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        region: RegionArgument,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] wantreturn: bool,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let pixels = usize::try_from(npixels)
                .ok()
                .filter(|&count| count > 0)
                .ok_or_else(|| raised("Value of npixels must be greater than zero"))?;
            region.validate_selectors(r#box, chans, stokes)?;
            let options = ImagePadOptions {
                pixels,
                value: value as f32,
                padmask,
                region: region.selection(),
                box_selection: (!r#box.is_empty()).then_some(r#box),
                channels: (!chans.is_empty()).then_some(chans),
                stokes: (!stokes.is_empty()).then_some(stokes),
                mask: (!mask.is_empty()).then_some(mask),
                stretch,
                destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
            };
            let history_command = state.history.then(|| {
                let selected = region.history();
                let formatted = if value.is_nan() {
                    "nan".into()
                } else {
                    format!("{value:.6}")
                };
                format!(
                    "ia.pad(outfile={outfile:?}, npixels={npixels}, value={formatted}, padmask={padmask}, overwrite={overwrite}, region={selected}, box={box:?}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, stretch={stretch}, wantreturn={wantreturn})",
                    box = r#box
                )
            });
            pad_image(
                attachment,
                outfile,
                overwrite,
                options,
                wantreturn,
                history_command,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
