// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.replacemaskedpixels` wrapper delegating to [`casa_imageanalysis::tool::ops_edit::replace_masked_pixels`].

use casa_imageanalysis::tool::ops_edit::replace_masked_pixels;
use pyo3::prelude::*;

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};
use super::subimage::{Region, mask_argument};

fn pixel_argument(pixels: Option<&Bound<'_, PyAny>>) -> PyResult<(Variant, String)> {
    let variant = pixels
        .map(|value| value.extract::<Variant>())
        .transpose()?
        .unwrap_or(Variant::Empty);
    let label = match &variant {
        Variant::Number(value) if pixels.is_some_and(|v| v.extract::<i64>().is_err()) => {
            format!("{value:.6}")
        }
        Variant::Text(text) => format!("\"{text}\""),
        other => other.describe(),
    };
    Ok((variant, label))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pixels=None, region=None, mask=None, update=false, list=false, stretch=false), text_signature = "($self, pixels=[], region={}, mask='', update=False, list=False, stretch=False)")]
    fn replacemaskedpixels(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        update: bool,
        list: bool,
        stretch: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let (variant, pixel_history) = pixel_argument(pixels)?;
        let expression = match &variant {
            Variant::Text(text) => text.as_str(),
            _ => &pixel_history,
        };
        let region = Region::argument(py, region, "replacemaskedpixels")?;
        let mask = mask_argument(mask)?;
        let (logs, outcome) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok((Vec::new(), false));
            };
            Region::reject_compound_record(region.as_ref())?;
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                format!(
                    "ia.replacemaskedpixels(pixels={pixel_history}, region={region_hist}, mask=\"{mask}\", update={update}, list={list}, stretch={stretch})"
                )
            });
            replace_masked_pixels(
                attachment,
                expression,
                region.as_ref().map(Region::as_region),
                &mask,
                update,
                list,
                stretch,
                history_command,
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Ok(outcome)
    }
}
