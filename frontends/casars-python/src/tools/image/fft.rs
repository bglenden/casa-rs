// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fft` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::fft_image`].

use casa_imageanalysis::analysis::SubimageRegion;
use casa_imageanalysis::tool::ops_transform::fft_image;
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

fn history_string(value: &str) -> String {
    format!("\"{}\"", if value.len() > 300 { "..." } else { value })
}

#[pymethods]
impl PyImageTool {
    /// Write selected products of the forward Fourier transform of an image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (real="", imag="", amp="", phase="", axes=IntList::of(&[-1]), region=RegionArgument::default(), mask=None, stretch=false, complex=""), text_signature = "($self, real='', imag='', amp='', phase='', axes=[-1], region={}, mask='', stretch=False, complex='')")]
    fn fft(
        &self,
        py: Python<'_>,
        real: &str,
        imag: &str,
        amp: &str,
        phase: &str,
        axes: IntList,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        complex: &str,
    ) -> PyResult<bool> {
        let mask = mask_argument(mask)?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let outfiles = [real, imag, amp, phase, complex];
            let history_command = state.history.then(|| {
                let [r, i, a, p, c] = outfiles.map(history_string);
                let axes_str = axes
                    .0
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                let region_hist = match region.selection() {
                    Some(SubimageRegion::Text(text)) => history_string(text),
                    _ => region.history(),
                };
                let mask_hist = history_string(&mask);
                format!(
                    "ia.fft(real={r}, imag={i}, amp={a}, phase={p}, axes=[{axes_str}], region={region_hist}, mask={mask_hist}, stretch={stretch}, complex={c})"
                )
            });
            fft_image(
                attachment,
                outfiles,
                &axes.0,
                region.selection(),
                &mask,
                stretch,
                history_command,
                external_open_check(),
            )
            .map_err(raised)
        })
    }
}
