// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native-width I/O for callers of the former standalone Python data binding.

use std::path::PathBuf;

use casa_coordinates::{CoordinateType, FitsValue, fits::try_to_fits_header};
use casa_images::{ImageInterface, ImagePixelType};
use casa_types::{ArrayD, ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use ndarray::{ArrayViewD, Axis};
use numpy::{IntoPyArray, PyReadonlyArrayDyn, PyUntypedArray};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyComplex, PyDict, PyList};

use super::attachment::{self, AttachedImage};
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::{PyImageTool, pixels::snapshot_array};

impl PyImageTool {
    fn data_call<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut ImageHandle) -> PyResult<R> + Send,
    ) -> PyResult<R> {
        py.detach(|| {
            let state = self.state();
            let image = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Image is detached"))?;
            work(&mut image.lock())
        })
    }
}

macro_rules! native_array {
    ($handle:expr, $image:ident => $read:expr) => {
        match $handle {
            ImageHandle::Float(storage) => with_storage!(storage, $image => $read.map(ArrayValue::Float32)),
            ImageHandle::Double(storage) => with_storage!(storage, $image => $read.map(ArrayValue::Float64)),
            ImageHandle::Complex(storage) => with_storage!(storage, $image => $read.map(ArrayValue::Complex32)),
            ImageHandle::DComplex(storage) => with_storage!(storage, $image => $read.map(ArrayValue::Complex64)),
        }
    };
}

#[pymethods]
impl PyImageTool {
    #[staticmethod]
    fn _data_open(py: Python<'_>, path: PathBuf, writable: bool) -> PyResult<Self> {
        py.detach(|| {
            if !path.is_dir() {
                return Err(raised(format!(
                    "Cannot open native image {}",
                    path.display()
                )));
            }
            let path = std::path::absolute(path).map_err(raised)?;
            let image = AttachedImage::open_data(&mut attachment::registry(), &path, writable)?;
            Ok(Self::attached(image))
        })
    }

    #[getter]
    fn _data_shape(&self, py: Python<'_>) -> PyResult<Vec<usize>> {
        self.data_call(py, |image| Ok(image.shape().to_vec()))
    }

    #[getter]
    fn _data_pixel_type(&self, py: Python<'_>) -> PyResult<&'static str> {
        self.data_call(py, |image| {
            Ok(match image.kind() {
                ImagePixelType::Float32 => "float32",
                ImagePixelType::Float64 => "float64",
                ImagePixelType::Complex32 => "complex64",
                ImagePixelType::Complex64 => "complex128",
            })
        })
    }

    #[getter]
    fn _data_units(&self, py: Python<'_>) -> PyResult<String> {
        self.data_call(py, |image| Ok(with_pixels!(image, storage => with_storage!(storage, storage => storage.units().into()))))
    }

    #[getter]
    fn _data_image_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.data_call(py, |image| {
            super::beams::image_info(image)
                .map(|info| info.to_record())
                .map_err(raised)
        })?;
        record_to_py(py, record)
    }

    #[getter]
    fn _data_misc_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.data_call(py, |image| {
            Ok(with_pixels!(image, storage => with_storage!(storage, image => image.misc_info())))
        })?;
        record_to_py(py, record)
    }

    #[getter]
    fn _data_coordinate_system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.data_call(py, |image| Ok(image.coordinates().to_record()))?;
        record_to_py(py, record)
    }

    fn _direction_wcs(&self, py: Python<'_>) -> PyResult<Option<(usize, usize, Py<PyAny>)>> {
        let wcs = self.data_call(py, |image| {
            let longitude = with_pixels!(image, storage => with_storage!(storage, image => image.find_axis(CoordinateType::Direction)));
            let coordinates = image.coordinates();
            let shape = image.shape();
            let Some(longitude) = longitude.filter(|_| coordinates.n_pixel_axes() == shape.len()) else {
                return Ok(None);
            };
            let header = try_to_fits_header(coordinates, shape)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            Ok(Some((longitude, header)))
        })?;
        let Some((longitude, header)) = wcs else {
            return Ok(None);
        };
        let keywords = PyDict::new(py);
        for keyword in header.iter() {
            if matches!(keyword.name.as_str(), "TELESCOP" | "OBSERVER" | "DATE-OBS") {
                continue;
            }
            let value = match &keyword.value {
                FitsValue::String(text) => text.into_py_any(py)?,
                FitsValue::Integer(value) => value.into_py_any(py)?,
                FitsValue::Float(value) => value.into_py_any(py)?,
                FitsValue::Logical(value) => value.into_py_any(py)?,
            };
            keywords.set_item(&keyword.name, value)?;
        }
        Ok(Some((
            longitude,
            longitude + 1,
            keywords.into_any().unbind(),
        )))
    }

    #[getter]
    fn _data_mask_names(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.data_call(py, |image| {
            Ok(with_pixels!(image, storage => with_storage!(storage, image => image.mask_names())))
        })
    }

    #[getter]
    fn _data_default_mask_name(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.data_call(py, |image| Ok(with_pixels!(image, storage => with_storage!(storage, image => image.default_mask_name()))))
    }

    #[pyo3(signature = (start, shape, stride=None))]
    fn _data_slice(
        &self,
        py: Python<'_>,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Option<Vec<usize>>,
    ) -> PyResult<Py<PyAny>> {
        let stride = normalize_image_stride(&shape, stride)?;
        let data = self.data_call(py, |image| {
            native_array!(image, image => image.get_slice_with_stride(&start, &shape, &stride))
                .map_err(raised)
        })?;
        array_value_to_py(py, data)
    }

    fn _data_plane(&self, py: Python<'_>, axis: usize, index: usize) -> PyResult<Py<PyAny>> {
        let data = self.data_call(py, |image| {
            native_array!(image, image => image.get_plane(axis, index)).map_err(raised)
        })?;
        array_value_to_py(py, data)
    }

    #[pyo3(signature = (start, shape, stride=None))]
    fn _data_mask_slice(
        &self,
        py: Python<'_>,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Option<Vec<usize>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let stride = normalize_image_stride(&shape, stride)?;
        let mask = self.data_call(py, |image| {
            with_pixels!(image, storage => with_storage!(storage, image => ImageInterface::get_mask_slice(image.as_ref(), &start, &shape, &stride))).map_err(raised)
        })?;
        mask.map(|mask| array_to_numpy(py, mask)).transpose()
    }

    fn _data_put_slice(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        start: Vec<usize>,
        writable: bool,
    ) -> PyResult<()> {
        if !writable {
            return Err(PyValueError::new_err(
                "image was opened read-only; reopen with writable=True to modify it",
            ));
        }
        let kind = self.data_call(py, |image| Ok(image.kind()))?;
        let data = match kind {
            ImagePixelType::Float32 => ArrayValue::Float32(typed_snapshot(py, data)?),
            ImagePixelType::Float64 => ArrayValue::Float64(typed_snapshot(py, data)?),
            ImagePixelType::Complex32 => {
                ArrayValue::Complex32(typed_snapshot::<Complex32>(py, data)?)
            }
            ImagePixelType::Complex64 => {
                ArrayValue::Complex64(typed_snapshot::<Complex64>(py, data)?)
            }
        };
        py.detach(|| {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Image is detached"))?;
            attachment.check_directory()?;
            let mut image = attachment.lock();
            match (&mut *image, data) {
                (ImageHandle::Float(storage), ArrayValue::Float32(values)) => {
                    with_storage!(storage, image => image.put_slice(&values, &start))
                }
                (ImageHandle::Double(storage), ArrayValue::Float64(values)) => {
                    with_storage!(storage, image => image.put_slice(&values, &start))
                }
                (ImageHandle::Complex(storage), ArrayValue::Complex32(values)) => {
                    with_storage!(storage, image => image.put_slice(&values, &start))
                }
                (ImageHandle::DComplex(storage), ArrayValue::Complex64(values)) => {
                    with_storage!(storage, image => image.put_slice(&values, &start))
                }
                _ => return Err(raised("Image pixel type changed before the write")),
            }
            .map_err(raised)?;
            image.save().map_err(raised)
        })
    }
}

fn typed_snapshot<T: Copy + Send + Sync + numpy::Element>(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
) -> PyResult<ArrayD<T>> {
    let array = py.import("numpy")?.call_method1("asarray", (data,))?;
    // Preserve the data API's exact dtype check before making an owned copy.
    drop(array.extract::<PyReadonlyArrayDyn<'_, T>>()?);
    snapshot_array(py, array.cast::<PyUntypedArray>()?)
}

fn normalize_image_stride(shape: &[usize], stride: Option<Vec<usize>>) -> PyResult<Vec<usize>> {
    let stride = stride.unwrap_or_else(|| vec![1; shape.len()]);
    if stride.len() != shape.len() {
        return Err(PyValueError::new_err(format!(
            "stride rank {} does not match shape rank {}",
            stride.len(),
            shape.len()
        )));
    }
    if stride.contains(&0) {
        return Err(PyValueError::new_err("stride values must be >= 1"));
    }
    Ok(stride)
}

pub(super) fn record_to_py(py: Python<'_>, record: RecordValue) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for field in record.into_fields() {
        dict.set_item(field.name.as_str(), value_to_py(py, field.value)?.bind(py))?;
    }
    Ok(dict.into_any().unbind())
}

fn value_to_py(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Scalar(value) => scalar_to_py(py, &value),
        Value::Array(value) => array_value_to_py(py, value),
        Value::Record(value) => record_to_py(py, value),
        Value::TableRef(path) => path.as_str().into_py_any(py),
    }
}

fn scalar_to_py(py: Python<'_>, value: &ScalarValue) -> PyResult<Py<PyAny>> {
    match value {
        ScalarValue::Bool(value) => value.into_py_any(py),
        ScalarValue::UInt8(value) => value.into_py_any(py),
        ScalarValue::UInt16(value) => value.into_py_any(py),
        ScalarValue::UInt32(value) => value.into_py_any(py),
        ScalarValue::Int16(value) => value.into_py_any(py),
        ScalarValue::Int32(value) => value.into_py_any(py),
        ScalarValue::Int64(value) => value.into_py_any(py),
        ScalarValue::Float32(value) => value.into_py_any(py),
        ScalarValue::Float64(value) => value.into_py_any(py),
        ScalarValue::Complex32(value) => {
            Ok(
                PyComplex::from_doubles(py, value.re as f64, value.im as f64)
                    .into_any()
                    .unbind(),
            )
        }
        ScalarValue::Complex64(value) => Ok(PyComplex::from_doubles(py, value.re, value.im)
            .into_any()
            .unbind()),
        ScalarValue::String(value) => value.as_str().into_py_any(py),
    }
}

fn array_value_to_py(py: Python<'_>, value: ArrayValue) -> PyResult<Py<PyAny>> {
    match value {
        ArrayValue::Bool(values) => array_to_numpy(py, values),
        ArrayValue::UInt8(values) => array_to_numpy(py, values),
        ArrayValue::UInt16(values) => array_to_numpy(py, values),
        ArrayValue::UInt32(values) => array_to_numpy(py, values),
        ArrayValue::Int16(values) => array_to_numpy(py, values),
        ArrayValue::Int32(values) => array_to_numpy(py, values),
        ArrayValue::Int64(values) => array_to_numpy(py, values),
        ArrayValue::Float32(values) => array_to_numpy(py, values),
        ArrayValue::Float64(values) => array_to_numpy(py, values),
        ArrayValue::Complex32(values) => array_to_numpy(py, values),
        ArrayValue::Complex64(values) => array_to_numpy(py, values),
        ArrayValue::String(values) => string_array_to_py(py, values.view()),
    }
}

fn string_array_to_py(py: Python<'_>, values: ArrayViewD<'_, String>) -> PyResult<Py<PyAny>> {
    if values.ndim() == 0 {
        return values[[]].as_str().into_py_any(py);
    }

    let list = PyList::empty(py);
    if values.ndim() == 1 {
        for value in values.iter() {
            list.append(value.as_str())?;
        }
    } else {
        for subview in values.axis_iter(Axis(0)) {
            list.append(string_array_to_py(py, subview.into_dyn())?.bind(py))?;
        }
    }
    Ok(list.into_any().unbind())
}

fn array_to_numpy<T>(py: Python<'_>, array: ArrayD<T>) -> PyResult<Py<PyAny>>
where
    T: numpy::Element,
{
    Ok(array.into_pyarray(py).into_any().unbind())
}
