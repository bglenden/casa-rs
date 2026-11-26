// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::analysis::{put_image_region, validate_put_region_shapes};
use casa_tables::LockType;
use ndarray::{Array1, ArrayD};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyTuple};

use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::masks::has_region;
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
        // CASA takes a write lock for the edit through either value of locking.
        let _ = locking;
        if self.state().image.is_none() {
            return Ok(false);
        }
        let (pixels, pixel_history) = if let Some(value) = supplied(pixels) {
            let pixels = Pixels::capture(py, Some(value), true)?;
            let history = pixels_history(value, &pixels)?;
            (Some(pixels), history)
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
        let (ok, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((false, Vec::new()));
            };
            let mut handle = attachment.lock();
            if handle.read_only_file_type().is_some() {
                return Err(raised("This image is not writable"));
            }
            if let Some(pixels) = &pixels {
                let complex = matches!(*handle, ImageHandle::Complex(_) | ImageHandle::DComplex(_));
                if pixels.is_complex() != complex {
                    return Err(raised(if complex {
                        "Real values cannot be put in images with complex valued pixels"
                    } else {
                        "Complex values cannot be put in images with real valued pixels"
                    }));
                }
            }
            validate_put_region_shapes(pixels.as_ref().map(Pixels::shape), mask.as_ref().map(ArrayD::shape))
                .map_err(raised)?;
            attachment.require_lock(&mut handle, LockType::Write)?;
            let fresh_mask_name = mask.as_ref().map(|_| {
                (0..)
                    .map(|index| format!("mask{index}"))
                    .find(|name| !has_region(&handle, name))
                    .expect("an unused mask name")
            });
            let selected = region.as_ref().map(Region::as_region);
            let command = state.history.then(|| {
                let region = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                format!("ia.putregion(pixels={pixel_history}, pixelmask={mask_history}, region={region}, list={list}, usemask={usemask}, replicate={replicate})")
            });
            let name = fresh_mask_name.as_deref().unwrap_or("");
            match &mut *handle {
                ImageHandle::Float(storage) => with_storage!(storage, image =>
                    put_image_region(image.as_mut(), pixels.as_ref().map(Pixels::f32), mask, selected, name, usemask, replicate)
                ),
                ImageHandle::Double(storage) => with_storage!(storage, image =>
                    put_image_region(image.as_mut(), pixels.as_ref().map(Pixels::f64), mask, selected, name, usemask, replicate)
                ),
                ImageHandle::Complex(storage) => with_storage!(storage, image =>
                    put_image_region(image.as_mut(), pixels.as_ref().map(Pixels::c32), mask, selected, name, usemask, replicate)
                ),
                ImageHandle::DComplex(storage) => with_storage!(storage, image =>
                    put_image_region(image.as_mut(), pixels.as_ref().map(Pixels::c64), mask, selected, name, usemask, replicate)
                ),
            }
            .map_err(raised)?;
            if let Some(command) = command {
                handle.record_history("putregion", command)?;
            }
            handle.save().map_err(raised)?;
            attachment.release_requested_lock(&mut handle)?;
            let logs = list.then(|| LogLine::info("Wrote the selected image region"));
            Ok((true, logs.into_iter().collect()))
        })?;
        emit(py, "casars.tools.image", &logs)?;
        Ok(ok)
    }
}
