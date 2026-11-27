// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.pixelvalue` / `ia.makearray` wrapper delegating to [`casa_imageanalysis::tool::point`].

use casa_imageanalysis::tool::point::{
    MakeArrayError, PixelScalar, make_constant_array, pixel_value_casa,
};
use num_complex::Complex64;
use pyo3::exceptions::{PyMemoryError, PySystemError};
use pyo3::prelude::*;

use crate::convert::{DoubleList, value::CasaValue};

use super::PyImageTool;
use super::handle::raised;

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
            let (shape, values) = make_constant_array(v.0, &shape.0).map_err(|err| match err {
                MakeArrayError::InvalidShape(err) => raised(err),
                MakeArrayError::OutOfMemory => PyMemoryError::new_err("image array is too large"),
            })?;
            Ok::<_, PyErr>(CasaValue::Floats { shape, values })
        })?;
        array.into_python(py)
    }
}
