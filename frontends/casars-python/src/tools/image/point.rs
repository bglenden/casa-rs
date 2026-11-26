// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{ImageInterface, ImagePixel};
use num_complex::Complex64;
use pyo3::exceptions::{PyMemoryError, PySystemError};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};

struct ArrayConstant(f64);

impl<'a, 'py> FromPyObject<'a, 'py> for ArrayConstant {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        value.extract::<f64>().map(Self).map_err(|_| {
            PySystemError::new_err(
                "<built-in function image_makearray> returned a result with an exception set",
            )
        })
    }
}
use crate::convert::{DoubleList, value::CasaValue};

pub(super) fn pixel_position(
    coordinates: &casa_coordinates::CoordinateSystem,
    shape: &[usize],
    requested: &[i64],
) -> Option<Vec<usize>> {
    let reference = coordinates.reference_pixel();
    let requested = if requested == [-1] {
        &[][..]
    } else {
        requested
    };
    shape
        .iter()
        .enumerate()
        .map(|(axis, &size)| {
            let pos = requested
                .get(axis)
                .copied()
                .unwrap_or_else(|| (reference.get(axis).copied().unwrap_or(0.0) + 0.5) as i64);
            usize::try_from(pos).ok().filter(|&pos| pos < size)
        })
        .collect()
}

fn pixel_record<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    requested: &[i64],
    value: impl Fn(T) -> CasaValue,
) -> PyResult<CasaValue> {
    let Some(pixel) = pixel_position(image.coordinates(), image.shape(), requested) else {
        return Ok(CasaValue::empty_record());
    };
    let (pixel_value, mask) = pixel_sample(image, &pixel)?;
    Ok(point_reply(pixel, mask, image.units(), value(pixel_value)))
}

pub(super) fn pixel_sample<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    pixel: &[usize],
) -> PyResult<(T, bool)> {
    let one = vec![1; pixel.len()];
    let mask = image
        .get_mask_slice(pixel, &one, &one)
        .map_err(raised)?
        .and_then(|mask| mask.first().copied())
        .unwrap_or(true);
    let pixel_value = image.get_at(pixel).map_err(raised)?;
    Ok((pixel_value, mask))
}

fn point_reply(pixel: Vec<usize>, mask: bool, units: &str, value: CasaValue) -> CasaValue {
    CasaValue::record([
        ("mask", CasaValue::Bool(mask)),
        (
            "pixel",
            CasaValue::Ints {
                shape: vec![pixel.len()],
                values: pixel.into_iter().map(|axis| axis as i64).collect(),
            },
        ),
        (
            "value",
            CasaValue::record([("unit", CasaValue::Text(units.into())), ("value", value)]),
        ),
    ])
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixel=None), text_signature = "($self, pixel=[-1])")]
    fn pixelvalue(&self, py: Python<'_>, pixel: Option<DoubleList>) -> PyResult<Py<PyAny>> {
        let pixel = pixel.unwrap_or_else(|| DoubleList::of(&[-1.0])).0;
        let record = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let mut image = attachment.lock();
            image.check_single_precision("pixelvalue")?;
            if image.coordinates().n_world_axes() == 0 {
                return Err(raised(
                    "WCBox: Failed AlwaysAssert itsCSys.nWorldAxes() > 0",
                ));
            }
            let pixel = pixel.into_iter().map(|p| p as i64).collect::<Vec<_>>();
            if pixel_position(image.coordinates(), image.shape(), &pixel).is_none() {
                return Ok(CasaValue::empty_record());
            }
            attachment.require_pixel_read_lock(&mut image)?;
            if let ImageHandle::Float(Storage::Temp {
                image,
                fits: Some(fits),
                ..
            }) = &*image
            {
                let Some(pixel) = pixel_position(image.coordinates(), &fits.shape, &pixel) else {
                    return Ok(CasaValue::empty_record());
                };
                let ones = vec![1; pixel.len()];
                let values = fits.read(&pixel, &ones, &ones)?;
                let value = *values.first().expect("one FITS pixel requested");
                return Ok(point_reply(
                    pixel,
                    !value.is_nan(),
                    image.units(),
                    CasaValue::Float(f64::from(value)),
                ));
            }
            let value = match &*image {
                ImageHandle::Float(storage) => with_storage!(storage, image =>
                    pixel_record(image.as_ref(), &pixel, |v| CasaValue::Float(f64::from(v)))
                ),
                ImageHandle::Complex(storage) => with_storage!(storage, image =>
                    pixel_record(image.as_ref(), &pixel, |v| CasaValue::Complex(
                        Complex64::new(f64::from(v.re), f64::from(v.im)),
                    ))
                ),
                _ => unreachable!("double precision was rejected"),
            }?;
            image.release_requested_lock()?;
            Ok(value)
        })?;
        record.into_python(py)
    }

    #[pyo3(signature = (v=ArrayConstant(0.0), shape=DoubleList::of(&[0.0])), text_signature = "($self, v=0.0, shape=[0])")]
    fn makearray(
        &self,
        py: Python<'_>,
        v: ArrayConstant,
        shape: DoubleList,
    ) -> PyResult<Py<PyAny>> {
        let array = py.detach(|| {
            let shape = if shape.0.is_empty() {
                vec![1]
            } else {
                shape
                    .0
                    .into_iter()
                    .map(|size| {
                        if size.is_finite() && size >= 0.0 {
                            Ok(size as usize)
                        } else {
                            Err(raised("image array dimensions must be non-negative"))
                        }
                    })
                    .collect::<PyResult<Vec<_>>>()?
            };
            let size = shape
                .iter()
                .try_fold(1_usize, |size, &axis| size.checked_mul(axis))
                .ok_or_else(|| PyMemoryError::new_err("image array is too large"))?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(size)
                .map_err(|_| PyMemoryError::new_err("image array is too large"))?;
            values.resize(size, v.0);
            Ok::<_, PyErr>(CasaValue::Floats { shape, values })
        })?;
        array.into_python(py)
    }
}
