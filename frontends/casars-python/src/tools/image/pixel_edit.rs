// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.set` wrapper delegating to [`casa_imageanalysis::tool::ops_edit::set_pixels_and_mask`].

use casa_imageanalysis::tool::ops_edit::set_pixels_and_mask;
use pyo3::prelude::*;

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};
use super::subimage::Region;

fn pixel_expression(pixels: &Variant, formatted_pixels: &str) -> PyResult<Option<String>> {
    if pixels.is_empty_sequence() {
        return Ok(None);
    }
    match pixels {
        Variant::Number(value) if value.is_finite() => Ok(Some(formatted_pixels.to_owned())),
        Variant::Text(text) if text.is_empty() || text == "[]" => Ok(None),
        Variant::Texts(_)
        | Variant::Numbers(_)
        | Variant::Complexes(_)
        | Variant::Array { .. }
        | Variant::ComplexArray { .. } => Err(raised("The pixels expression must be scalar")),
        _ => Ok(Some(pixels.describe())),
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixels=None, pixelmask=-1, region=None, list=false), text_signature = "($self, pixels=[], pixelmask=-1, region={}, list=False)")]
    fn set(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        pixelmask: i64,
        region: Option<&Bound<'_, PyAny>>,
        list: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let variant = pixels
            .map(|value| value.extract::<Variant>())
            .transpose()?
            .unwrap_or(Variant::Empty);
        let pixel_history = match &variant {
            Variant::Number(value) if pixels.is_some_and(|v| v.extract::<i64>().is_err()) => {
                format!("{value:.6}")
            }
            Variant::Text(text) => format!("\"{text}\""),
            other => other.describe(),
        };
        let expr = pixel_expression(&variant, &pixel_history)?;
        let region = Region::argument(py, region, "set")?;
        let (outcome, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok((false, Vec::new()));
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                format!("ia.set(pixels={pixel_history}, pixelmask={pixelmask}, region={region_hist}, list={list})")
            });
            set_pixels_and_mask(
                attachment,
                expr.as_deref(),
                pixelmask,
                region.as_ref().map(Region::as_region),
                list,
                history_command,
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Ok(outcome)
    }
}
