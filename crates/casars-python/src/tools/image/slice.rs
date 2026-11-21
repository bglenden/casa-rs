// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::RegridMethod;
use casa_images::pixel_slice::{PixelSlice, get_slice};
use num_complex::{Complex32, Complex64};
use numpy::PyUntypedArray;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use crate::convert::{double_vector_element, value::CasaValue};

struct SliceVector {
    values: Vec<f64>,
    origin: VectorOrigin,
}

enum VectorOrigin {
    Sequence,
    Scalar,
    Text,
    OtherScalar,
}

fn vector_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function image_getslice> returned a result with an exception set",
    )
}

impl SliceVector {
    fn default_values(values: &[f64]) -> Self {
        Self {
            values: values.to_vec(),
            origin: VectorOrigin::Sequence,
        }
    }

    fn indices(self, argument: &str) -> PyResult<Vec<i64>> {
        match self.origin {
            VectorOrigin::Text => {
                return Err(PyTypeError::new_err(format!(
                    "argument _{argument} must not be a string"
                )));
            }
            VectorOrigin::OtherScalar => return Err(vector_error()),
            _ => (),
        }
        let mut values: Vec<_> = self
            .values
            .into_iter()
            .map(|value| i64::from(value as i32))
            .collect();
        if argument == "axes" && matches!(self.origin, VectorOrigin::Scalar) {
            // Scalar assignment replaces the first default axis only.
            values.push(1);
        }
        Ok(values)
    }
}

impl FromPyObject<'_, '_> for SliceVector {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyTuple>() {
            return Err(vector_error());
        }
        if value.is_none() {
            return Ok(Self::default_values(&[]));
        }
        if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
            return Ok(Self {
                values: vec![-1.0],
                origin: VectorOrigin::Text,
            });
        }
        let flat = if value.is_instance_of::<PyUntypedArray>() {
            value.call_method1("ravel", ("F",))?
        } else if let Ok(list) = value.cast::<PyList>() {
            if list
                .iter()
                .any(|item| item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>())
            {
                let py = value.py();
                let args = PyDict::new(py);
                args.set_item("dtype", "object")?;
                py.import("numpy")?
                    .call_method("asarray", (list,), Some(&args))?
                    .call_method1("ravel", ("F",))?
            } else {
                value.to_owned()
            }
        } else {
            let origin = if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() {
                VectorOrigin::Scalar
            } else {
                VectorOrigin::OtherScalar
            };
            return Ok(Self {
                values: vec![value.extract::<f64>()?],
                origin,
            });
        };
        let values = flat
            .try_iter()?
            .map(|item| item.map(|item| double_vector_element(&item).unwrap_or(0.0)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            values,
            origin: VectorOrigin::Sequence,
        })
    }
}

struct SliceMethod(String);

impl SliceMethod {
    fn parse(&self) -> PyResult<RegridMethod> {
        match self.0.as_bytes().first().map(u8::to_ascii_uppercase) {
            Some(b'N') => Ok(RegridMethod::Nearest),
            Some(b'L') => Ok(RegridMethod::Linear),
            Some(b'C') => Ok(RegridMethod::Cubic),
            _ => Err(raised("Illegal interpolation method")),
        }
    }
}

impl FromPyObject<'_, '_> for SliceMethod {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(bytes) = value.cast::<PyBytes>() {
            return Ok(Self(String::from_utf8_lossy(bytes.as_bytes()).into_owned()));
        }
        value
            .extract::<String>()
            .map(Self)
            .map_err(|_| PyTypeError::new_err("argument _method must be a string"))
    }
}

fn record<T>(
    slice: PixelSlice<T>,
    axes: [i64; 2],
    pixels: impl FnOnce(Vec<T>) -> CasaValue,
) -> CasaValue {
    let count = slice.positions.len();
    CasaValue::record([
        (
            "axes",
            CasaValue::Ints {
                shape: vec![2],
                values: axes.to_vec(),
            },
        ),
        (
            "distance",
            CasaValue::floats(slice.distance.into_iter().map(f64::from).collect()),
        ),
        (
            "mask",
            CasaValue::Bools {
                shape: vec![count],
                values: slice.mask,
            },
        ),
        ("pixel", pixels(slice.pixels)),
        (
            "xpos",
            CasaValue::floats(
                slice
                    .positions
                    .iter()
                    .map(|point| f64::from(point[0]))
                    .collect(),
            ),
        ),
        (
            "ypos",
            CasaValue::floats(
                slice
                    .positions
                    .into_iter()
                    .map(|point| f64::from(point[1]))
                    .collect(),
            ),
        ),
    ])
}

fn complex(values: Vec<Complex64>) -> CasaValue {
    CasaValue::Complexes {
        shape: vec![values.len()],
        values,
    }
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (x, y, axes=SliceVector::default_values(&[0.0, 1.0]), coord=SliceVector::default_values(&[-1.0]), npts=0, method=SliceMethod("linear".into())), text_signature = "($self, x, y, axes=[0, 1], coord=[-1], npts=0, method='linear')")]
    fn getslice(
        &self,
        py: Python<'_>,
        x: SliceVector,
        y: SliceVector,
        axes: SliceVector,
        coord: SliceVector,
        npts: i32,
        method: SliceMethod,
    ) -> PyResult<Py<PyAny>> {
        let axes = axes.indices("axes")?;
        let axes = [
            axes.first().copied().unwrap_or(-1),
            axes.get(1).copied().unwrap_or(-1),
        ];
        let coord = coord.indices("coord")?;
        let output = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let npts = usize::try_from(npts).map_err(|_| raised("npts must be non-negative"))?;
            let method = method.parse()?;
            let mut image = attachment.lock();
            attachment.require_pixel_read_lock(&mut image)?;
            macro_rules! read {
                ($view:expr, $pixels:expr) => {
                    get_slice($view, &x.values, &y.values, axes, &coord, npts, method)
                        .map(|slice| record(slice, axes, $pixels))
                        .map_err(raised)
                };
            }
            let result = match &*image {
                ImageHandle::Float(Storage::Temp {
                    image,
                    fits: Some(fits),
                    ..
                }) => {
                    read!(&fits.image_view(image), |pixels: Vec<f32>| {
                        CasaValue::floats(pixels.into_iter().map(f64::from).collect())
                    })
                }
                ImageHandle::Float(storage) => with_storage!(storage, view => {
                    read!(view.as_ref(), |pixels: Vec<f32>| {
                        CasaValue::floats(pixels.into_iter().map(f64::from).collect())
                    })
                }),
                ImageHandle::Double(storage) => with_storage!(storage, view => {
                    read!(view.as_ref(), CasaValue::floats)
                }),
                ImageHandle::Complex(storage) => with_storage!(storage, view => {
                    read!(view.as_ref(), |pixels: Vec<Complex32>| {
                        complex(pixels.into_iter().map(|value| {
                            Complex64::new(f64::from(value.re), f64::from(value.im))
                        }).collect())
                    })
                }),
                ImageHandle::DComplex(storage) => with_storage!(storage, view => {
                    read!(view.as_ref(), complex)
                }),
            };
            let release = attachment.release_requested_lock(&mut image);
            result.and_then(|value| release.map(|()| value))
        })?;
        output.into_python(py)
    }
}
