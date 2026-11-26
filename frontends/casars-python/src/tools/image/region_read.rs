// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayD, RecordValue};
use casa_images::analysis::{ImageRegionSection, SubimageRegion, evaluate_image_tool_mask_section};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant, value::CasaValue};
use crate::tools::{LogLine, emit, table::casa_record};

use super::attachment::read_locked;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::pixels::{Chunk, ChunkRead, and_mask, collapse_axes};
use super::subimage::Region;
use super::{PyImageTool, stub};

pub(super) fn is_boolean_sequence(value: &Bound<'_, PyAny>) -> PyResult<bool> {
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

pub(super) fn read_region_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    method: &'static str,
) -> PyResult<Option<Region>> {
    if value.is_some_and(|value| value.is_instance_of::<PyString>()) {
        return Region::capture(py, value);
    }
    let record = region_argument(py, value, method)?;
    Ok((!record.is_empty()).then_some(Region::Record(record)))
}

pub(super) fn resolve_region_section(
    image: &ImageHandle,
    region: Option<SubimageRegion<'_>>,
    include_membership: bool,
) -> PyResult<ImageRegionSection> {
    if let ImageHandle::Float(storage) = image {
        storage
            .with_float_view(|image| ImageRegionSection::resolve(image, region, include_membership))
    } else {
        with_pixels!(image, storage => with_storage!(storage, image =>
            ImageRegionSection::resolve(image.as_ref(), region, include_membership)))
    }
    .map_err(raised)
}

pub(super) fn read_region_section(
    image: &ImageHandle,
    region: Option<SubimageRegion<'_>>,
    include_membership: bool,
) -> PyResult<(Chunk, Option<ArrayD<bool>>)> {
    let section = resolve_region_section(image, region, include_membership)?;
    Ok((
        Chunk {
            start: section.start,
            size: section.count,
            stride: section.stride,
        },
        section.membership,
    ))
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
        let region = read_region_argument(py, region, "getregion");
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
            read_locked(attachment, |image| {
                let shape = image.shape();
                let axes = collapse_axes(
                    axes.as_ref().map_or(&[], |arg| arg.0.as_slice()),
                    shape.len(),
                )?;
                let region = region.as_ref().map(Region::as_region);
                let (chunk, membership) =
                    read_region_section(image, region, getmask || !axes.is_empty())?;
                let expression = if mask.is_empty() {
                    None
                } else {
                    Some(
                        evaluate_image_tool_mask_section(
                            &mask,
                            shape,
                            image.coordinates(),
                            (&chunk.start, &chunk.size, &chunk.stride),
                            stretch,
                        )
                        .map_err(raised)?,
                    )
                };
                let data = chunk.read_from(
                    image,
                    &ChunkRead {
                        axes: &axes,
                        dropdeg,
                        getmask,
                    },
                    and_mask(membership, expression),
                )?;
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
                Ok((data, lines.into_iter().collect()))
            })
        })?;
        emit(py, "casars.tools.image", &log)?;
        value.into_python(py)
    }
}
