// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.decimate` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial`].

use casa_imageanalysis::analysis::DecimationMethod;
use casa_imageanalysis::tool::ops_spatial::decimate_image;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::Region;

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axis=0, factor=1, method="copy", region=None, mask="", overwrite=false, stretch=false), text_signature = "($self, outfile='', axis=0, factor=1, method='copy', region='', mask='', overwrite=False, stretch=False)")]
    fn decimate(
        &self,
        py: Python<'_>,
        outfile: &str,
        axis: i64,
        factor: i64,
        method: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: &str,
        overwrite: bool,
        stretch: bool,
    ) -> PyResult<Py<Self>> {
        let region = Region::capture(py, region);
        let product = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let region = region?;
            let axis = usize::try_from(axis)
                .map_err(|_| raised("The value of axis cannot be negative"))?;
            let factor = usize::try_from(factor)
                .map_err(|_| raised("The value of factor cannot be negative"))?;
            let function = match method.as_bytes().first().map(u8::to_ascii_lowercase) {
                Some(b'c') => DecimationMethod::Copy,
                Some(b'm') => DecimationMethod::Mean,
                _ => return Err(raised(format!("Unsupported decimation method {method}"))),
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "\"\"".into(), Region::history);
                format!(
                    "ia.decimate(outfile={outfile:?}, axis={axis}, factor={factor}, method={method:?}, region={region_hist}, mask={mask:?}, overwrite={overwrite}, stretch={stretch})"
                )
            });
            let result = decimate_image(
                attachment,
                outfile,
                overwrite,
                axis,
                factor,
                function,
                region.as_ref().map(Region::as_region),
                (!mask.is_empty()).then_some(mask),
                stretch,
                history_command,
                external_open_check(),
            )
            .map_err(raised)?;
            Ok(Some(result))
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
