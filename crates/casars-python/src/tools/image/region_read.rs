// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::RecordValue;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant, value::CasaValue};
use crate::tools::{LogLine, emit, table::casa_record};

use super::copy_region::Selection;
use super::handle::raised;
use super::mask_expression::{MaskTarget, evaluate_mask};
use super::pixels::{Chunk, ChunkRead, collapse_axes};
use super::{PyImageTool, stub};

fn is_boolean_sequence(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        return Ok(array.dtype().kind() == b'b');
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return Ok(value
            .try_iter()?
            .all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>())));
    }
    Ok(false)
}

pub(super) fn region_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    method: &'static str,
) -> PyResult<RecordValue> {
    let Some(value) = value else {
        return Ok(RecordValue::default());
    };
    if let Ok(record) = value.cast::<PyDict>() {
        return casa_record(py, Some(record));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return if text.to_str()?.is_empty() {
            Ok(RecordValue::default())
        } else {
            Err(stub(method).unwrap_err())
        };
    }
    if is_boolean_sequence(value)? {
        return Ok(RecordValue::default());
    }
    let variant = value.extract::<Variant>()?;
    let kind = match variant {
        Variant::Bool(_) => "bool",
        Variant::Number(_) => "numeric",
        Variant::Texts(_) => "stringvec",
        _ => "variant",
    };
    Err(raised(format!("Unsupported type for region {kind}")))
}

fn mask_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_str()?.to_owned());
    }
    if is_boolean_sequence(value)? {
        return Ok(String::new());
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (region=None, axes=None, mask=None, list=false, dropdeg=false, getmask=false, stretch=false), text_signature = "($self, region={}, axes=[-1], mask='', list=False, dropdeg=False, getmask=False, stretch=False)")]
    fn getregion(
        &self,
        py: Python<'_>,
        region: Option<&Bound<'_, PyAny>>,
        axes: Option<IntList>,
        mask: Option<&Bound<'_, PyAny>>,
        list: bool,
        dropdeg: bool,
        getmask: bool,
        stretch: bool,
    ) -> PyResult<Py<PyAny>> {
        let region = region_argument(py, region, "getregion");
        let mask = mask_argument(mask);
        let (value, log) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((
                    CasaValue::Bools {
                        shape: vec![0],
                        values: Vec::new(),
                    },
                    Vec::new(),
                ));
            };
            let region = region?;
            let mask = mask?;
            let mut image = attachment.lock();
            attachment.require_pixel_read_lock(&mut image)?;
            let shape = image.shape();
            let chunk = Chunk::from(Selection::new(shape, &region, "getregion")?);
            let axes = collapse_axes(
                axes.as_ref().map_or(&[], |arg| arg.0.as_slice()),
                shape.len(),
            )?;
            let expression = if mask.is_empty() {
                None
            } else {
                Some(evaluate_mask(
                    &mask,
                    MaskTarget {
                        shape,
                        section: Some((&chunk, image.coordinates())),
                        stretch,
                        method: "getregion",
                    },
                )?)
            };
            let data = chunk.read_from(
                &image,
                &ChunkRead {
                    axes: &axes,
                    dropdeg,
                    getmask,
                },
                expression,
            )?;
            image.release_requested_lock()?;
            let lines = list.then(|| {
                let end = chunk
                    .start
                    .iter()
                    .zip(&chunk.size)
                    .zip(&chunk.stride)
                    .map(|((&start, &count), &step)| start + (count - 1) * step)
                    .collect::<Vec<_>>();
                LogLine::info(format!(
                    "Selected bounding box {:?} to {end:?}",
                    chunk.start
                ))
            });
            Ok::<_, PyErr>((data, lines.into_iter().collect()))
        })?;
        emit(py, "casars.tools.image", &log)?;
        value.into_python(py)
    }
}
