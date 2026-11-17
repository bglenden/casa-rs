// SPDX-License-Identifier: LGPL-3.0-or-later

mod convert;
mod object_contract;
mod parameters;
mod plot;
mod tasks;
mod tools;

use std::path::PathBuf;

use casa_coordinates::{CoordinateType, FitsValue, fits::try_to_fits_header};
use casa_images::{AnyPagedImage, ImageError, ImagePixelType};
use casa_types::{ArrayD, ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use ndarray::{ArrayViewD, Axis, IxDyn};
use numpy::{IntoPyArray, PyReadonlyArrayDyn, PyUntypedArrayMethods};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyComplex, PyDict, PyDictMethods, PyList, PyListMethods};

use crate::object_contract::{DataObjectProtocolInfo, DataObjectSchemaBundle};

#[pyclass(name = "Image", module = "casars._core", unsendable)]
struct PyImage {
    inner: AnyPagedImage,
    writable: bool,
}

#[pymethods]
impl PyImage {
    #[staticmethod]
    #[pyo3(signature = (path, writable = false))]
    fn open(path: PathBuf, writable: bool) -> PyResult<Self> {
        Ok(Self {
            inner: AnyPagedImage::open(&path).map_err(image_err)?,
            writable,
        })
    }

    #[getter]
    fn shape(&self) -> Vec<usize> {
        self.inner.shape().to_vec()
    }

    #[getter]
    fn pixel_type(&self) -> &'static str {
        match self.inner.pixel_type() {
            ImagePixelType::Float32 => "float32",
            ImagePixelType::Float64 => "float64",
            ImagePixelType::Complex32 => "complex64",
            ImagePixelType::Complex64 => "complex128",
        }
    }

    #[getter]
    fn units(&self) -> String {
        match &self.inner {
            AnyPagedImage::Float32(image) => image.units().to_string(),
            AnyPagedImage::Float64(image) => image.units().to_string(),
            AnyPagedImage::Complex32(image) => image.units().to_string(),
            AnyPagedImage::Complex64(image) => image.units().to_string(),
        }
    }

    #[getter]
    fn image_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let info = match &self.inner {
            AnyPagedImage::Float32(image) => image.image_info(),
            AnyPagedImage::Float64(image) => image.image_info(),
            AnyPagedImage::Complex32(image) => image.image_info(),
            AnyPagedImage::Complex64(image) => image.image_info(),
        }
        .map_err(image_err)?;
        record_to_py(py, &info.to_record())
    }

    #[getter]
    fn misc_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let info = match &self.inner {
            AnyPagedImage::Float32(image) => image.misc_info(),
            AnyPagedImage::Float64(image) => image.misc_info(),
            AnyPagedImage::Complex32(image) => image.misc_info(),
            AnyPagedImage::Complex64(image) => image.misc_info(),
        };
        record_to_py(py, &info)
    }

    #[getter]
    fn coordinate_system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let coordinates = match &self.inner {
            AnyPagedImage::Float32(image) => image.coordinates(),
            AnyPagedImage::Float64(image) => image.coordinates(),
            AnyPagedImage::Complex32(image) => image.coordinates(),
            AnyPagedImage::Complex64(image) => image.coordinates(),
        };
        record_to_py(py, &coordinates.to_record())
    }

    /// The pixel axes of the image's direction and its FITS WCS keywords, as
    /// `(longitude_axis, latitude_axis, keywords)`, for `casars.imexplore`'s
    /// WCSAxes plots; `None` without a direction coordinate, or when the
    /// coordinate system does not describe every pixel axis.
    ///
    /// The keywords are those `exportfits` writes
    /// (`casa_coordinates::fits::to_fits_header`) for every axis, without
    /// its observation metadata, so a plot's WCS is the one a FITS export
    /// carries. Not part of the data-object surface.
    #[pyo3(name = "_direction_wcs")]
    fn direction_wcs(&self, py: Python<'_>) -> PyResult<Option<(usize, usize, Py<PyAny>)>> {
        let (coordinates, direction) = match &self.inner {
            AnyPagedImage::Float32(image) => (
                image.coordinates(),
                image.find_axis(CoordinateType::Direction),
            ),
            AnyPagedImage::Float64(image) => (
                image.coordinates(),
                image.find_axis(CoordinateType::Direction),
            ),
            AnyPagedImage::Complex32(image) => (
                image.coordinates(),
                image.find_axis(CoordinateType::Direction),
            ),
            AnyPagedImage::Complex64(image) => (
                image.coordinates(),
                image.find_axis(CoordinateType::Direction),
            ),
        };
        let shape = self.inner.shape();
        let Some(longitude) = direction.filter(|_| coordinates.n_pixel_axes() == shape.len())
        else {
            return Ok(None);
        };
        let keywords = PyDict::new(py);
        let header = try_to_fits_header(coordinates, shape)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
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
    fn mask_names(&self) -> Vec<String> {
        self.inner.mask_names()
    }

    #[getter]
    fn default_mask_name(&self) -> Option<String> {
        self.inner.default_mask_name()
    }

    #[pyo3(signature = (start, shape, stride = None))]
    fn get_slice(
        &self,
        py: Python<'_>,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Option<Vec<usize>>,
    ) -> PyResult<Py<PyAny>> {
        let stride = normalize_image_stride(&shape, stride)?;
        match &self.inner {
            AnyPagedImage::Float32(image) => array_to_numpy(
                py,
                image
                    .get_slice_with_stride(&start, &shape, &stride)
                    .map_err(image_err)?,
            ),
            AnyPagedImage::Float64(image) => array_to_numpy(
                py,
                image
                    .get_slice_with_stride(&start, &shape, &stride)
                    .map_err(image_err)?,
            ),
            AnyPagedImage::Complex32(image) => array_to_numpy(
                py,
                image
                    .get_slice_with_stride(&start, &shape, &stride)
                    .map_err(image_err)?,
            ),
            AnyPagedImage::Complex64(image) => array_to_numpy(
                py,
                image
                    .get_slice_with_stride(&start, &shape, &stride)
                    .map_err(image_err)?,
            ),
        }
    }

    fn get_plane(&self, py: Python<'_>, axis: usize, index: usize) -> PyResult<Py<PyAny>> {
        match &self.inner {
            AnyPagedImage::Float32(image) => {
                array_to_numpy(py, image.get_plane(axis, index).map_err(image_err)?)
            }
            AnyPagedImage::Float64(image) => {
                array_to_numpy(py, image.get_plane(axis, index).map_err(image_err)?)
            }
            AnyPagedImage::Complex32(image) => {
                array_to_numpy(py, image.get_plane(axis, index).map_err(image_err)?)
            }
            AnyPagedImage::Complex64(image) => {
                array_to_numpy(py, image.get_plane(axis, index).map_err(image_err)?)
            }
        }
    }

    #[pyo3(signature = (start, shape, stride = None))]
    fn get_mask_slice(
        &self,
        py: Python<'_>,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Option<Vec<usize>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let stride = normalize_image_stride(&shape, stride)?;
        self.inner
            .get_mask_slice(&start, &shape, &stride)
            .map_err(image_err)?
            .map(|mask| array_to_numpy(py, mask))
            .transpose()
    }

    fn put_slice(
        &mut self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        start: Vec<usize>,
    ) -> PyResult<()> {
        require_writable(self.writable, "image")?;
        match &mut self.inner {
            AnyPagedImage::Float32(image) => {
                let values = py_any_to_array::<f32>(py, data)?;
                image.put_slice(&values, &start).map_err(image_err)?;
            }
            AnyPagedImage::Float64(image) => {
                let values = py_any_to_array::<f64>(py, data)?;
                image.put_slice(&values, &start).map_err(image_err)?;
            }
            AnyPagedImage::Complex32(image) => {
                let values = py_any_to_array::<Complex32>(py, data)?;
                image.put_slice(&values, &start).map_err(image_err)?;
            }
            AnyPagedImage::Complex64(image) => {
                let values = py_any_to_array::<Complex64>(py, data)?;
                image.put_slice(&values, &start).map_err(image_err)?;
            }
        }
        self.inner.save().map_err(image_err)
    }
}

fn require_writable(writable: bool, kind: &str) -> PyResult<()> {
    if writable {
        Ok(())
    } else {
        Err(PyValueError::new_err(format!(
            "{kind} was opened read-only; reopen with writable=True to modify it"
        )))
    }
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

fn image_err(error: ImageError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn record_to_py(py: Python<'_>, record: &RecordValue) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for field in record.fields() {
        dict.set_item(field.name.as_str(), value_to_py(py, &field.value)?.bind(py))?;
    }
    Ok(dict.into_any().unbind())
}

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Scalar(value) => scalar_to_py(py, value),
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

fn array_value_to_py(py: Python<'_>, value: &ArrayValue) -> PyResult<Py<PyAny>> {
    match value {
        ArrayValue::Bool(values) => array_to_numpy(py, values.clone()),
        ArrayValue::UInt8(values) => array_to_numpy(py, values.clone()),
        ArrayValue::UInt16(values) => array_to_numpy(py, values.clone()),
        ArrayValue::UInt32(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Int16(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Int32(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Int64(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Float32(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Float64(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Complex32(values) => array_to_numpy(py, values.clone()),
        ArrayValue::Complex64(values) => array_to_numpy(py, values.clone()),
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

fn pyreadonly_to_array<T>(array: PyReadonlyArrayDyn<'_, T>) -> PyResult<ArrayD<T>>
where
    T: Clone + numpy::Element,
{
    let shape = array.shape().to_vec();
    let values = array.as_array().iter().cloned().collect::<Vec<_>>();
    ArrayD::from_shape_vec(IxDyn(&shape), values)
        .map_err(|error| PyValueError::new_err(format!("failed to build array: {error}")))
}

fn py_any_to_array<T>(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<ArrayD<T>>
where
    T: Clone + numpy::Element,
{
    let numpy = py.import("numpy")?;
    let array = numpy.call_method1("asarray", (value,))?;
    let array = array.extract::<PyReadonlyArrayDyn<'_, T>>()?;
    pyreadonly_to_array(array)
}

fn array_to_numpy<T>(py: Python<'_>, array: ArrayD<T>) -> PyResult<Py<PyAny>>
where
    T: numpy::Element,
{
    Ok(array.into_pyarray(py).into_any().unbind())
}

#[pyfunction]
fn linearmosaic(
    py: Python<'_>,
    pointings: Vec<(PathBuf, PathBuf)>,
    template: PathBuf,
    output: PathBuf,
    output_weight: PathBuf,
) -> PyResult<()> {
    py.detach(move || casa_images::linearmosaic(&pointings, &template, &output, &output_weight))
        .map_err(image_err)
}

#[pyfunction]
fn data_protocol_info_json() -> PyResult<String> {
    serde_json::to_string_pretty(&DataObjectProtocolInfo::current())
        .map_err(|error| PyRuntimeError::new_err(format!("serialize data protocol info: {error}")))
}

#[pyfunction]
fn data_schema_bundle_json() -> PyResult<String> {
    DataObjectSchemaBundle::current()
        .to_json_string()
        .map_err(|error| PyRuntimeError::new_err(format!("serialize data schema bundle: {error}")))
}

/// The extension module: the `casars.data` image and table objects, the
/// CASA tools of `casars.tools` (ADR-0017), and the parameter, plot and task
/// functions that `casars.parameters`, `casars.data` and `casars.tasks` call.
#[pymodule]
fn _core(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyImage>()?;
    module.add_class::<tools::simulator::PySimulator>()?;
    module.add_class::<tools::measures::PyMeasures>()?;
    module.add_class::<tools::quanta::PyQuanta>()?;
    module.add_class::<tools::componentlist::PyComponentList>()?;
    module.add_class::<tools::image::PyImageTool>()?;
    module.add_class::<tools::coordsys::PyCoordsys>()?;
    module.add_class::<tools::table::PyTableTool>()?;
    module.add_class::<tools::table::PyTableColIter>()?;
    module.add_class::<tools::table::PyTableRowTool>()?;
    module.add_class::<tools::msmetadata::PyMsMetadata>()?;
    module.add_class::<tools::ms::PyMsTool>()?;
    module.add_class::<tools::simutil::PySimutil>()?;
    module.add_function(wrap_pyfunction!(linearmosaic, module)?)?;
    module.add_function(wrap_pyfunction!(data_protocol_info_json, module)?)?;
    module.add_function(wrap_pyfunction!(data_schema_bundle_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_catalog, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_surface_bundle,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_profile_surface,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_template_toml,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_last, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_resolve, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_render_toml, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_provider_invocation,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_save, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_write_managed,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(tasks::application_catalog, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_executable, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_outcome, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_plot_documents, module)?)?;
    module.add_function(wrap_pyfunction!(plot::build_measurement_set_plot, module)?)?;
    Ok(())
}
