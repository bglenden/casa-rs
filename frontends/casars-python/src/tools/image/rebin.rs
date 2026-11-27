// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.rebin` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial`].

use casa_imageanalysis::analysis::ImageRebinOptions;
use casa_imageanalysis::tool::ops_spatial::rebin_image;
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{Region, mask_argument};

#[pymethods]
impl PyImageTool {
    /// Average selected pixels in bins and return the rebinned image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile, bin, region=None, mask=None, dropdeg=false, overwrite=false, stretch=false, crop=false), text_signature = "($self, outfile, bin, region='', mask='', dropdeg=False, overwrite=False, stretch=False, crop=False)")]
    fn rebin(
        &self,
        py: Python<'_>,
        outfile: &str,
        bin: IntList,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
        stretch: bool,
        crop: bool,
    ) -> PyResult<Py<Self>> {
        let region = Region::capture(py, region);
        let mask = mask_argument(mask);
        let product = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let region = region?;
            let mask = mask?;
            let factors = bin
                .0
                .iter()
                .map(|&n| usize::try_from(n).map_err(|_| raised("All factors must be positive")))
                .collect::<PyResult<Vec<_>>>()?;
            let options = ImageRebinOptions {
                factors: &factors,
                region: region.as_ref().map(Region::as_region),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                stretch,
                dropdeg,
                crop,
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "\"\"".into(), Region::history);
                let factor_str = factors
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "ia.rebin(outfile={outfile:?}, bin=[{factor_str}], region={region_hist}, mask={mask:?}, dropdeg={dropdeg}, overwrite={overwrite}, stretch={stretch}, crop={crop})"
                )
            });
            rebin_image(
                attachment,
                outfile,
                overwrite,
                options,
                history_command,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(product))
    }
}
