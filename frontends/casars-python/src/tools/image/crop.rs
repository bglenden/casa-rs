// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.crop` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial::crop_image`].

use casa_imageanalysis::analysis::ImageCropOptions;
use casa_imageanalysis::tool::ops_spatial::crop_image;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::IntList;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, raised};
use super::subimage::Region;

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axes=None, overwrite=false, region=None, r#box="", chans="", stokes="", mask="", stretch=false, wantreturn=true), text_signature = "($self, outfile='', axes=[], overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn crop(
        &self,
        py: Python<'_>,
        outfile: &str,
        axes: Option<IntList>,
        overwrite: bool,
        region: Option<&Bound<'_, PyAny>>,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        stretch: bool,
        wantreturn: bool,
    ) -> PyResult<Py<Self>> {
        let record_argument = region.is_none_or(|value| value.is_instance_of::<PyDict>());
        let omitted_history = match region {
            _ if record_argument => "{...}",
            Some(value) if value.is_instance_of::<PyString>() => "\"\"",
            _ => "[]",
        };
        let region = Region::capture(py, region);
        let (result, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok((None, Vec::new()));
            };
            let region = region?;
            if record_argument || region.is_some() {
                if !r#box.is_empty() {
                    return Err(raised("box and regionPtr cannot be simultaneously specified"));
                }
                if !chans.is_empty() || !stokes.is_empty() {
                    return Err(raised(
                        "regionPtr and regionName, chans, and/or stokes cannot be simultaneously specified",
                    ));
                }
            }
            let axes = axes.as_ref().map_or(&[][..], |axes| axes.0.as_slice());
            let options = ImageCropOptions {
                axes,
                box_selection: (!r#box.is_empty()).then_some(r#box),
                region: region.as_ref().map(Region::as_region),
                channels: (!chans.is_empty()).then_some(chans),
                stokes: (!stokes.is_empty()).then_some(stokes),
                mask: (!mask.is_empty()).then_some(mask),
                stretch,
            };
            let history_command = state.history.then(|| {
                let axes_str = axes.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
                let region_hist = region
                    .as_ref()
                    .map_or_else(|| omitted_history.into(), Region::history);
                format!(
                    "ia.crop(outfile={outfile:?}, axes=[{axes_str}], overwrite={overwrite}, region={region_hist}, box={box:?}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, stretch={stretch}, wantreturn={wantreturn})",
                    box = r#box
                )
            });
            crop_image(
                attached,
                outfile,
                overwrite,
                options,
                wantreturn,
                history_command,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Py::new(py, result.map_or_else(Self::new, Self::attached))
    }
}
