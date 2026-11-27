// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.pixelvalue` / `ia.makearray` wrapper delegating to [`casa_imageanalysis::tool::point`].

pub(super) use casa_imageanalysis::tool::point::pixel_position;
use casa_imageanalysis::tool::point::{PixelScalar, pixel_value_casa};
use casa_images::{ImageInterface, ImagePixel};
use num_complex::Complex64;
use pyo3::exceptions::{PyMemoryError, PySystemError};
use pyo3::prelude::*;

use crate::convert::{DoubleList, value::CasaValue};

use super::PyImageTool;
use super::handle::raised;

pub(super) fn pixel_sample<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    pixel: &[usize],
) -> PyResult<(T, bool)> {
    casa_imageanalysis::tool::point::pixel_sample(image, pixel).map_err(raised)
}

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

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixel=None), text_signature = "($self, pixel=[-1])")]
    fn pixelvalue(&self, py: Python<'_>, pixel: Option<DoubleList>) -> PyResult<Py<PyAny>> {
        let pixel = pixel.unwrap_or_else(|| DoubleList::of(&[-1.0])).0;
        let record = py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let Some(result) = pixel_value_casa(attachment, &pixel).map_err(raised)? else {
                return Ok(CasaValue::empty_record());
            };
            let scalar = match result.value {
                PixelScalar::Float(v) => CasaValue::Float(f64::from(v)),
                PixelScalar::Double(v) => CasaValue::Float(v),
                PixelScalar::Complex(v) => {
                    CasaValue::Complex(Complex64::new(f64::from(v.re), f64::from(v.im)))
                }
                PixelScalar::DComplex(v) => CasaValue::Complex(v),
            };
            Ok(CasaValue::record([
                ("mask", CasaValue::Bool(result.mask)),
                (
                    "pixel",
                    CasaValue::Ints {
                        shape: vec![result.pixel.len()],
                        values: result.pixel,
                    },
                ),
                (
                    "value",
                    CasaValue::record([("unit", CasaValue::Text(result.unit)), ("value", scalar)]),
                ),
            ]))
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
