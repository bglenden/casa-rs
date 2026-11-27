// SPDX-License-Identifier: LGPL-3.0-or-later
//! Thin PyO3 native-width I/O accessors delegating to [`casa_imageanalysis::tool::data_compat`].

use std::path::PathBuf;

use casa_coordinates::FitsValue;
use casa_core::{ArrayD, ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use casa_imageanalysis::tool::data_compat::{
    data_coordinate_system, data_default_mask_name, data_direction_wcs, data_image_info,
    data_mask_names, data_mask_slice, data_misc_info, data_pixel_type, data_plane, data_put_slice,
    data_shape, data_slice, data_units, normalize_stride, open_data_image,
};
use casa_images::ImagePixelType;
use ndarray::{ArrayViewD, Axis};
use numpy::{IntoPyArray, PyReadonlyArrayDyn, PyUntypedArray};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyComplex, PyDict, PyList};

use super::attachment::AttachedImage;
use super::handle::raised;
use super::{PyImageTool, pixels::snapshot_array};

impl PyImageTool {
    fn attached_data<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&AttachedImage) -> PyResult<R> + Send,
    ) -> PyResult<R> {
        py.detach(|| {
            let state = self.state();
            let image = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Image is detached"))?;
            work(image)
        })
    }
}

#[pymethods]
impl PyImageTool {
    #[staticmethod]
    fn _data_open(py: Python<'_>, path: PathBuf, writable: bool) -> PyResult<Self> {
        py.detach(|| {
            let image = open_data_image(&path, writable).map_err(raised)?;
            Ok(Self::attached(image))
        })
    }

    #[getter]
    fn _data_shape(&self, py: Python<'_>) -> PyResult<Vec<usize>> {
        self.attached_data(py, |image| Ok(data_shape(image)))
    }

    #[getter]
    fn _data_pixel_type(&self, py: Python<'_>) -> PyResult<&'static str> {
        self.attached_data(py, |image| {
            Ok(match data_pixel_type(image) {
                ImagePixelType::Float32 => "float32",
                ImagePixelType::Float64 => "float64",
                ImagePixelType::Complex32 => "complex64",
                ImagePixelType::Complex64 => "complex128",
            })
        })
    }

    #[getter]
    fn _data_units(&self, py: Python<'_>) -> PyResult<String> {
        self.attached_data(py, |image| Ok(data_units(image)))
    }

    #[getter]
    fn _data_image_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.attached_data(py, |image| data_image_info(image).map_err(raised))?;
        record_to_py(py, record)
    }

    #[getter]
    fn _data_misc_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.attached_data(py, |image| Ok(data_misc_info(image)))?;
        record_to_py(py, record)
    }

    #[getter]
    fn _data_coordinate_system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = self.attached_data(py, |image| Ok(data_coordinate_system(image)))?;
        record_to_py(py, record)
    }

    fn _direction_wcs(&self, py: Python<'_>) -> PyResult<Option<(usize, usize, Py<PyAny>)>> {
        let wcs = self.attached_data(py, |image| {
            data_direction_wcs(image).map_err(|error| PyValueError::new_err(error.to_string()))
        })?;
        let Some((lon, lat, header)) = wcs else {
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
        Ok(Some((lon, lat, keywords.into_any().unbind())))
    }

    #[getter]
    fn _data_mask_names(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.attached_data(py, |image| Ok(data_mask_names(image)))
    }

    #[getter]
    fn _data_default_mask_name(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.attached_data(py, |image| Ok(data_default_mask_name(image)))
    }

    #[pyo3(signature = (start, shape, stride=None))]
    fn _data_slice(
        &self,
        py: Python<'_>,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Option<Vec<usize>>,
    ) -> PyResult<Py<PyAny>> {
        let stride = normalize_stride(&shape, stride)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let data = self.attached_data(py, |image| {
            data_slice(image, &start, &shape, &stride).map_err(raised)
        })?;
        array_value_to_py(py, data)
    }

    fn _data_plane(&self, py: Python<'_>, axis: usize, index: usize) -> PyResult<Py<PyAny>> {
        let data =
            self.attached_data(py, |image| data_plane(image, axis, index).map_err(raised))?;
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
        let stride = normalize_stride(&shape, stride)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let mask = self.attached_data(py, |image| {
            data_mask_slice(image, &start, &shape, &stride).map_err(raised)
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
        let kind = self.attached_data(py, |image| Ok(data_pixel_type(image)))?;
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
        self.attached_data(py, |image| {
            data_put_slice(image, data, &start).map_err(raised)
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
