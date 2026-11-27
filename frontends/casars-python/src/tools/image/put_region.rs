// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.putregion` wrapper delegating to [`casa_imageanalysis::tool::ops_edit::put_region`].

use casa_imageanalysis::tool::ops_edit::put_region;
use casa_imageanalysis::tool::pixels::PixelChunk;
use ndarray::{Array1, ArrayD};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyTuple};

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};
use super::pixels::{Pixels, snapshot_array};
use super::subimage::Region;

fn supplied<'a, 'py>(value: Option<&'a Bound<'py, PyAny>>) -> Option<&'a Bound<'py, PyAny>> {
    value.filter(|value| {
        !value.is_none()
            && !value.cast::<PyList>().is_ok_and(|list| list.is_empty())
            && !value.cast::<PyTuple>().is_ok_and(|tuple| tuple.is_empty())
            && !value
                .cast::<PyUntypedArray>()
                .is_ok_and(|array| array.is_empty())
    })
}

fn mask_array(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<(ArrayD<bool>, String)> {
    if let Ok(array) = value.cast::<PyUntypedArray>()
        && array.dtype().kind() == b'b'
    {
        let mask = snapshot_array(py, array)?;
        let history = mask_history(&mask);
        return Ok((mask, history));
    }
    if let Ok(values) = value.extract::<Vec<bool>>() {
        let mask = Array1::from_vec(values).into_dyn();
        let history = mask_history(&mask);
        return Ok((mask, history));
    }
    match Pixels::capture(py, Some(value), true) {
        Ok(pixels @ Pixels::Real(_)) => {
            let history = pixels_history(value, &pixels)?;
            let Pixels::Real(values) = pixels else {
                unreachable!()
            };
            Ok((values.mapv(|value| value != 0.0), history))
        }
        _ => Err(raised("mask is not understood, try using an array")),
    }
}

fn pixels_history(value: &Bound<'_, PyAny>, pixels: &Pixels) -> PyResult<String> {
    if let Pixels::Real(values) = pixels
        && values.len() <= 100
    {
        let integral = if let Ok(array) = value.cast::<PyUntypedArray>() {
            matches!(array.dtype().kind(), b'i' | b'u')
        } else {
            value
                .try_iter()?
                .all(|value| value.is_ok_and(|value| value.is_instance_of::<PyInt>()))
        };
        if integral {
            let values = values
                .view()
                .reversed_axes()
                .iter()
                .map(|&value| (value as i64).to_string())
                .collect::<Vec<_>>();
            return Ok(format!("[{}]", values.join(",")));
        }
    }
    Ok(pixels.history())
}

fn mask_history(mask: &ArrayD<bool>) -> String {
    if mask.len() > 100 {
        return "\"[...]\"".into();
    }
    let values: Vec<_> = mask
        .view()
        .reversed_axes()
        .iter()
        .map(|&valid| if valid { "true" } else { "false" })
        .collect();
    format!("[{}]", values.join(","))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pixels=None, pixelmask=None, region=None, list=false, usemask=true, locking=true, replicate=false), text_signature = "($self, pixels=[], pixelmask=[], region={}, list=False, usemask=True, locking=True, replicate=False)")]
    fn putregion(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        pixelmask: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        list: bool,
        usemask: bool,
        locking: bool,
        replicate: bool,
    ) -> PyResult<bool> {
        let _ = locking;
        if self.state().image.is_none() {
            return Ok(false);
        }
        let (chunk, pixel_history) = if let Some(value) = supplied(pixels) {
            let pixels = Pixels::capture(py, Some(value), true)?;
            let history = pixels_history(value, &pixels)?;
            let chunk = match pixels {
                Pixels::Real(arr) => PixelChunk::Double(arr),
                Pixels::Complex(arr) => PixelChunk::DComplex(arr),
            };
            (Some(chunk), history)
        } else {
            (None, "[]".into())
        };
        let (mask, mask_history) = if let Some(value) = supplied(pixelmask) {
            let (mask, history) = mask_array(py, value)?;
            (Some(mask), history)
        } else {
            (None, "[]".into())
        };
        let region = Region::capture(py, region)?;
        let (ok, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((false, Vec::new()));
            };
            let history_command = state.history.then(|| {
                let region_hist = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                format!(
                    "ia.putregion(pixels={pixel_history}, pixelmask={mask_history}, region={region_hist}, list={list}, usemask={usemask}, replicate={replicate})"
                )
            });
            let logs = put_region(
                attachment,
                chunk.as_ref(),
                mask.as_ref(),
                region.as_ref().map(Region::as_region),
                list,
                usemask,
                replicate,
                history_command,
            )
            .map_err(raised)?;
            Ok((true, logs))
        })?;
        emit_tool_logs(py, &logs)?;
        Ok(ok)
    }
}
