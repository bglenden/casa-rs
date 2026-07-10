// SPDX-License-Identifier: LGPL-3.0-or-later

mod object_contract;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_images::{AnyPagedImage, ImageError, ImagePixelType};
use casa_provider_contracts::{
    ParameterValue, SurfaceContractBundle, SurfaceKind, builtin_surface_bundle,
    builtin_surface_catalog,
};
use casa_tables::{RowRange, Table as CasaTable, TableError, TableOptions};
use casa_task_runtime::{
    BaseSource, ManagedProfileKind, ManagedStateStore, ParameterSession, ResolutionPatch,
    parse_profile, render_documented_template, write_parameter_profile_atomic,
};
use casa_types::{
    ArrayD, ArrayValue, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayViewD, Axis, IxDyn};
use numpy::{IntoPyArray, PyReadonlyArrayDyn, PyUntypedArrayMethods};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyAnyMethods, PyBool, PyComplex, PyDict, PyDictMethods, PyIterator, PyList,
    PyListMethods, PySequence, PyString,
};
use serde::Serialize;

use crate::object_contract::{DataObjectProtocolInfo, DataObjectSchemaBundle};

#[pyclass(name = "Image", module = "casars._core", unsendable)]
struct PyImage {
    inner: AnyPagedImage,
    writable: bool,
}

#[pyclass(name = "Table", module = "casars._core", unsendable)]
struct PyTable {
    path: PathBuf,
    inner: CasaTable,
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

#[pymethods]
impl PyTable {
    #[staticmethod]
    #[pyo3(signature = (path, writable = false))]
    fn open(path: PathBuf, writable: bool) -> PyResult<Self> {
        Ok(Self {
            inner: CasaTable::open(TableOptions::new(&path)).map_err(table_err)?,
            path,
            writable,
        })
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        if let Some(schema) = self.inner.schema() {
            return schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect();
        }

        let mut names = BTreeSet::new();
        for row_index in 0..self.inner.row_count() {
            if let Ok(row) = self.inner.row_accessor().row(row_index) {
                for field in row.fields() {
                    names.insert(field.name.clone());
                }
            }
        }
        names.into_iter().collect()
    }

    #[getter]
    fn keywords(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        record_to_py(py, self.inner.keywords())
    }

    fn column_keywords(&self, py: Python<'_>, column: &str) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .column_keywords(column)
            .map(|keywords| record_to_py(py, keywords))
            .transpose()
    }

    fn get_cell(&self, py: Python<'_>, row: usize, column: &str) -> PyResult<Py<PyAny>> {
        match self
            .inner
            .cell_accessor(row, column)
            .and_then(|cell| cell.value())
            .map_err(table_err)?
        {
            Some(value) => value_to_py(py, value),
            None => Ok(py.None()),
        }
    }

    fn set_cell(
        &mut self,
        py: Python<'_>,
        row: usize,
        column: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        require_writable(self.writable, "table")?;
        let value = py_to_value(py, value)?;
        self.inner
            .cell_accessor_mut(row, column)
            .and_then(|mut cell| cell.set(value))
            .map_err(table_err)?;
        self.inner
            .save(TableOptions::new(&self.path))
            .map_err(table_err)
    }

    #[pyo3(signature = (column, start = 0, count = None, step = 1))]
    fn get_column(
        &self,
        py: Python<'_>,
        column: &str,
        start: usize,
        count: Option<usize>,
        step: usize,
    ) -> PyResult<Py<PyAny>> {
        let row_range = build_row_range(self.inner.row_count(), start, count, step)?;
        let cells = self
            .inner
            .column_accessor(column)
            .and_then(|column| column.iter_range(row_range))
            .map_err(table_err)?
            .map(|cell| cell.value.cloned())
            .collect::<Vec<_>>();
        column_values_to_py(py, &cells)
    }

    #[pyo3(signature = (column, values, start = 0, step = 1))]
    fn put_column(
        &mut self,
        py: Python<'_>,
        column: &str,
        values: &Bound<'_, PyAny>,
        start: usize,
        step: usize,
    ) -> PyResult<usize> {
        require_writable(self.writable, "table")?;
        let values = py_iterable_to_values(py, values)?;
        let row_range = build_row_range_for_values(start, values.len(), step)?;
        let written = self
            .inner
            .column_accessor_mut(column)
            .and_then(|mut column| column.put_range(row_range, values))
            .map_err(table_err)?;
        self.inner
            .save(TableOptions::new(&self.path))
            .map_err(table_err)?;
        Ok(written)
    }

    fn set_column_keywords(&mut self, column: &str, keywords: &Bound<'_, PyAny>) -> PyResult<()> {
        require_writable(self.writable, "table")?;
        let keywords = py_to_record_value(keywords)?;
        self.inner.set_column_keywords(column.to_string(), keywords);
        self.inner
            .save(TableOptions::new(&self.path))
            .map_err(table_err)
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

fn table_err(error: TableError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn build_row_range(
    row_count: usize,
    start: usize,
    count: Option<usize>,
    step: usize,
) -> PyResult<RowRange> {
    if step == 0 {
        return Err(PyValueError::new_err("step must be >= 1"));
    }
    if start > row_count {
        return Err(PyValueError::new_err(format!(
            "start row {start} exceeds row count {row_count}"
        )));
    }

    let end = match count {
        Some(0) => start,
        Some(count) => start
            .checked_add(step.saturating_mul(count.saturating_sub(1)))
            .and_then(|last| last.checked_add(1))
            .ok_or_else(|| PyValueError::new_err("row range overflow"))?,
        None => row_count,
    };
    Ok(RowRange::with_stride(start, end.min(row_count), step))
}

fn build_row_range_for_values(start: usize, len: usize, step: usize) -> PyResult<RowRange> {
    if step == 0 {
        return Err(PyValueError::new_err("step must be >= 1"));
    }
    let end = if len == 0 {
        start
    } else {
        start
            .checked_add(step.saturating_mul(len.saturating_sub(1)))
            .and_then(|last| last.checked_add(1))
            .ok_or_else(|| PyValueError::new_err("row range overflow"))?
    };
    Ok(RowRange::with_stride(start, end, step))
}

fn py_iterable_to_values(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
    let iterator = PyIterator::from_object(values)?;
    let mut converted = Vec::new();
    for item in iterator {
        converted.push(py_to_value(py, &item?)?);
    }
    Ok(converted)
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

fn column_values_to_py(py: Python<'_>, values: &[Option<Value>]) -> PyResult<Py<PyAny>> {
    if values.iter().any(Option::is_none) {
        return values_to_list(py, values);
    }

    let cells = values
        .iter()
        .map(|value| value.as_ref().expect("checked above"))
        .collect::<Vec<_>>();
    if cells.is_empty() {
        return Ok(PyList::empty(py).into_any().unbind());
    }

    match cells[0] {
        Value::Scalar(ScalarValue::Bool(_)) => {
            scalar_column_to_numpy(py, &cells, extract_bool_scalar)
        }
        Value::Scalar(ScalarValue::UInt8(_)) => {
            scalar_column_to_numpy(py, &cells, extract_u8_scalar)
        }
        Value::Scalar(ScalarValue::UInt16(_)) => {
            scalar_column_to_numpy(py, &cells, extract_u16_scalar)
        }
        Value::Scalar(ScalarValue::UInt32(_)) => {
            scalar_column_to_numpy(py, &cells, extract_u32_scalar)
        }
        Value::Scalar(ScalarValue::Int16(_)) => {
            scalar_column_to_numpy(py, &cells, extract_i16_scalar)
        }
        Value::Scalar(ScalarValue::Int32(_)) => {
            scalar_column_to_numpy(py, &cells, extract_i32_scalar)
        }
        Value::Scalar(ScalarValue::Int64(_)) => {
            scalar_column_to_numpy(py, &cells, extract_i64_scalar)
        }
        Value::Scalar(ScalarValue::Float32(_)) => {
            scalar_column_to_numpy(py, &cells, extract_f32_scalar)
        }
        Value::Scalar(ScalarValue::Float64(_)) => {
            scalar_column_to_numpy(py, &cells, extract_f64_scalar)
        }
        Value::Scalar(ScalarValue::Complex32(_)) => {
            scalar_column_to_numpy(py, &cells, extract_c32_scalar)
        }
        Value::Scalar(ScalarValue::Complex64(_)) => {
            scalar_column_to_numpy(py, &cells, extract_c64_scalar)
        }
        Value::Scalar(ScalarValue::String(_)) => {
            let list = PyList::empty(py);
            for cell in cells {
                match cell {
                    Value::Scalar(ScalarValue::String(value)) => list.append(value)?,
                    _ => return values_to_list(py, values),
                }
            }
            Ok(list.into_any().unbind())
        }
        Value::Array(ArrayValue::Bool(_)) => array_column_to_py(py, &cells, extract_bool_array),
        Value::Array(ArrayValue::UInt8(_)) => array_column_to_py(py, &cells, extract_u8_array),
        Value::Array(ArrayValue::UInt16(_)) => array_column_to_py(py, &cells, extract_u16_array),
        Value::Array(ArrayValue::UInt32(_)) => array_column_to_py(py, &cells, extract_u32_array),
        Value::Array(ArrayValue::Int16(_)) => array_column_to_py(py, &cells, extract_i16_array),
        Value::Array(ArrayValue::Int32(_)) => array_column_to_py(py, &cells, extract_i32_array),
        Value::Array(ArrayValue::Int64(_)) => array_column_to_py(py, &cells, extract_i64_array),
        Value::Array(ArrayValue::Float32(_)) => array_column_to_py(py, &cells, extract_f32_array),
        Value::Array(ArrayValue::Float64(_)) => array_column_to_py(py, &cells, extract_f64_array),
        Value::Array(ArrayValue::Complex32(_)) => array_column_to_py(py, &cells, extract_c32_array),
        Value::Array(ArrayValue::Complex64(_)) => array_column_to_py(py, &cells, extract_c64_array),
        Value::Array(ArrayValue::String(_)) => values_to_list(py, values),
        Value::Record(_) | Value::TableRef(_) => values_to_list(py, values),
    }
}

fn values_to_list(py: Python<'_>, values: &[Option<Value>]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for value in values {
        match value {
            Some(value) => list.append(value_to_py(py, value)?.bind(py))?,
            None => list.append(py.None().bind(py))?,
        }
    }
    Ok(list.into_any().unbind())
}

fn scalar_column_to_numpy<T, F>(
    py: Python<'_>,
    values: &[&Value],
    extract: F,
) -> PyResult<Py<PyAny>>
where
    T: numpy::Element,
    F: Fn(&Value) -> Option<T>,
{
    let mut collected = Vec::with_capacity(values.len());
    for value in values {
        let Some(item) = extract(value) else {
            return Err(PyTypeError::new_err("column contains mixed scalar types"));
        };
        collected.push(item);
    }
    Ok(collected.into_pyarray(py).into_any().unbind())
}

fn array_column_to_py<T, F>(py: Python<'_>, values: &[&Value], extract: F) -> PyResult<Py<PyAny>>
where
    T: numpy::Element + Clone,
    F: Fn(&Value) -> Option<&ArrayD<T>>,
{
    let mut arrays = Vec::with_capacity(values.len());
    for value in values {
        let Some(array) = extract(value) else {
            return Err(PyTypeError::new_err("column contains mixed array types"));
        };
        arrays.push(array);
    }

    let first_shape = arrays[0].shape().to_vec();
    if arrays
        .iter()
        .all(|array| array.shape() == first_shape.as_slice())
    {
        let mut stacked_values = Vec::new();
        for array in &arrays {
            stacked_values.extend(array.iter().cloned());
        }
        let mut shape = Vec::with_capacity(first_shape.len() + 1);
        shape.push(arrays.len());
        shape.extend(first_shape);
        let stacked = ArrayD::from_shape_vec(IxDyn(&shape), stacked_values).map_err(|error| {
            PyValueError::new_err(format!("failed to stack column arrays: {error}"))
        })?;
        array_to_numpy(py, stacked)
    } else {
        let list = PyList::empty(py);
        for array in arrays {
            list.append(array_to_numpy(py, array.clone())?.bind(py))?;
        }
        Ok(list.into_any().unbind())
    }
}

fn extract_bool_scalar(value: &Value) -> Option<bool> {
    match value {
        Value::Scalar(ScalarValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn extract_u8_scalar(value: &Value) -> Option<u8> {
    match value {
        Value::Scalar(ScalarValue::UInt8(value)) => Some(*value),
        _ => None,
    }
}

fn extract_u16_scalar(value: &Value) -> Option<u16> {
    match value {
        Value::Scalar(ScalarValue::UInt16(value)) => Some(*value),
        _ => None,
    }
}

fn extract_u32_scalar(value: &Value) -> Option<u32> {
    match value {
        Value::Scalar(ScalarValue::UInt32(value)) => Some(*value),
        _ => None,
    }
}

fn extract_i16_scalar(value: &Value) -> Option<i16> {
    match value {
        Value::Scalar(ScalarValue::Int16(value)) => Some(*value),
        _ => None,
    }
}

fn extract_i32_scalar(value: &Value) -> Option<i32> {
    match value {
        Value::Scalar(ScalarValue::Int32(value)) => Some(*value),
        _ => None,
    }
}

fn extract_i64_scalar(value: &Value) -> Option<i64> {
    match value {
        Value::Scalar(ScalarValue::Int64(value)) => Some(*value),
        _ => None,
    }
}

fn extract_f32_scalar(value: &Value) -> Option<f32> {
    match value {
        Value::Scalar(ScalarValue::Float32(value)) => Some(*value),
        _ => None,
    }
}

fn extract_f64_scalar(value: &Value) -> Option<f64> {
    match value {
        Value::Scalar(ScalarValue::Float64(value)) => Some(*value),
        _ => None,
    }
}

fn extract_c32_scalar(value: &Value) -> Option<Complex32> {
    match value {
        Value::Scalar(ScalarValue::Complex32(value)) => Some(*value),
        _ => None,
    }
}

fn extract_c64_scalar(value: &Value) -> Option<Complex64> {
    match value {
        Value::Scalar(ScalarValue::Complex64(value)) => Some(*value),
        _ => None,
    }
}

fn extract_bool_array(value: &Value) -> Option<&ArrayD<bool>> {
    match value {
        Value::Array(ArrayValue::Bool(value)) => Some(value),
        _ => None,
    }
}

fn extract_u8_array(value: &Value) -> Option<&ArrayD<u8>> {
    match value {
        Value::Array(ArrayValue::UInt8(value)) => Some(value),
        _ => None,
    }
}

fn extract_u16_array(value: &Value) -> Option<&ArrayD<u16>> {
    match value {
        Value::Array(ArrayValue::UInt16(value)) => Some(value),
        _ => None,
    }
}

fn extract_u32_array(value: &Value) -> Option<&ArrayD<u32>> {
    match value {
        Value::Array(ArrayValue::UInt32(value)) => Some(value),
        _ => None,
    }
}

fn extract_i16_array(value: &Value) -> Option<&ArrayD<i16>> {
    match value {
        Value::Array(ArrayValue::Int16(value)) => Some(value),
        _ => None,
    }
}

fn extract_i32_array(value: &Value) -> Option<&ArrayD<i32>> {
    match value {
        Value::Array(ArrayValue::Int32(value)) => Some(value),
        _ => None,
    }
}

fn extract_i64_array(value: &Value) -> Option<&ArrayD<i64>> {
    match value {
        Value::Array(ArrayValue::Int64(value)) => Some(value),
        _ => None,
    }
}

fn extract_f32_array(value: &Value) -> Option<&ArrayD<f32>> {
    match value {
        Value::Array(ArrayValue::Float32(value)) => Some(value),
        _ => None,
    }
}

fn extract_f64_array(value: &Value) -> Option<&ArrayD<f64>> {
    match value {
        Value::Array(ArrayValue::Float64(value)) => Some(value),
        _ => None,
    }
}

fn extract_c32_array(value: &Value) -> Option<&ArrayD<Complex32>> {
    match value {
        Value::Array(ArrayValue::Complex32(value)) => Some(value),
        _ => None,
    }
}

fn extract_c64_array(value: &Value) -> Option<&ArrayD<Complex64>> {
    match value {
        Value::Array(ArrayValue::Complex64(value)) => Some(value),
        _ => None,
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

fn py_to_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Err(PyTypeError::new_err(
            "None is not a supported CASA table value",
        ));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return Ok(Value::Record(py_dict_to_record(dict)?));
    }
    if value.hasattr("dtype")? && value.hasattr("shape")? {
        return Ok(Value::Array(py_any_to_array_value(py, value)?));
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PySequence>() {
        if let Ok(array) = py_any_to_array_value(py, value) {
            return Ok(Value::Array(array));
        }
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(Value::Scalar(ScalarValue::Bool(value.extract()?)));
    }
    if let Ok(complex) = value.cast::<PyComplex>() {
        return Ok(Value::Scalar(ScalarValue::Complex64(Complex64::new(
            complex.real(),
            complex.imag(),
        ))));
    }
    if let Ok(string) = value.cast::<PyString>() {
        return Ok(Value::Scalar(ScalarValue::String(
            string.to_str()?.to_string(),
        )));
    }
    if let Ok(integer) = value.extract::<i64>() {
        return Ok(Value::Scalar(ScalarValue::Int64(integer)));
    }
    if let Ok(float) = value.extract::<f64>() {
        return Ok(Value::Scalar(ScalarValue::Float64(float)));
    }
    Err(PyTypeError::new_err(format!(
        "unsupported Python value for CASA conversion: {}",
        value.get_type().name()?
    )))
}

fn py_to_record_value(value: &Bound<'_, PyAny>) -> PyResult<RecordValue> {
    if let Ok(dict) = value.cast::<PyDict>() {
        return py_dict_to_record(dict);
    }
    Err(PyTypeError::new_err(format!(
        "expected dict-like record value, got {}",
        value.get_type().name()?
    )))
}

fn py_dict_to_record(dict: &Bound<'_, PyDict>) -> PyResult<RecordValue> {
    let mut fields = Vec::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let key = key.extract::<String>()?;
        fields.push(RecordField::new(key, py_to_value(dict.py(), &value)?));
    }
    Ok(RecordValue::new(fields))
}

fn py_any_to_array_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<ArrayValue> {
    let numpy = py.import("numpy")?;
    let array = numpy.call_method1("asarray", (value,))?;

    macro_rules! try_array {
        ($ty:ty, $variant:ident) => {
            if let Ok(array) = array.extract::<PyReadonlyArrayDyn<'_, $ty>>() {
                return Ok(ArrayValue::$variant(pyreadonly_to_array(array)?));
            }
        };
    }

    try_array!(bool, Bool);
    try_array!(u8, UInt8);
    try_array!(u16, UInt16);
    try_array!(u32, UInt32);
    try_array!(i16, Int16);
    try_array!(i32, Int32);
    try_array!(i64, Int64);
    try_array!(f32, Float32);
    try_array!(f64, Float64);
    try_array!(Complex32, Complex32);
    try_array!(Complex64, Complex64);

    Err(PyTypeError::new_err(
        "unsupported NumPy dtype for CASA array conversion",
    ))
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

#[derive(Serialize)]
struct ParameterBridgeSnapshot<'a> {
    surface_id: &'a str,
    surface_kind: SurfaceKind,
    contract_version: u32,
    base_source: &'a BaseSource,
    dirty: bool,
    states: &'a BTreeMap<String, casa_task_runtime::ParameterState>,
    diagnostics: &'a [casa_task_runtime::Diagnostic],
}

fn parameter_runtime_error(context: &str, error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(format!("{context}: {error}"))
}

fn parameter_bundle(surface_id: &str) -> PyResult<SurfaceContractBundle> {
    builtin_surface_bundle(surface_id)
        .map_err(|error| parameter_runtime_error("load parameter surface", error))
}

fn parameter_snapshot_json(session: &ParameterSession) -> PyResult<String> {
    let snapshot = ParameterBridgeSnapshot {
        surface_id: session.bundle().surface.id(),
        surface_kind: session.bundle().surface.kind(),
        contract_version: session.bundle().surface.contract_version(),
        base_source: session.base_source(),
        dirty: session.is_dirty(),
        states: session.states(),
        diagnostics: session.diagnostics(),
    };
    serde_json::to_string(&snapshot)
        .map_err(|error| parameter_runtime_error("serialize parameter state", error))
}

fn parameter_session_from_source(
    surface_id: &str,
    source: &str,
    profile_toml: Option<&str>,
    profile_path: Option<PathBuf>,
) -> PyResult<ParameterSession> {
    let bundle = parameter_bundle(surface_id)?;
    if source == "defaults" {
        if profile_toml.is_some() {
            return Err(PyValueError::new_err(
                "the defaults source cannot carry profile TOML",
            ));
        }
        return ParameterSession::defaults(bundle)
            .map_err(|error| parameter_runtime_error("resolve parameter defaults", error));
    }

    let profile_toml = profile_toml.ok_or_else(|| {
        PyValueError::new_err(format!("parameter source {source:?} requires profile TOML"))
    })?;
    let profile = parse_profile(profile_toml)
        .map_err(|error| parameter_runtime_error("parse parameter profile", error))?;
    let source = match source {
        "file" => BaseSource::File(profile_path.unwrap_or_else(|| PathBuf::from("<memory>"))),
        "last" => BaseSource::Last,
        "last_successful" => BaseSource::LastSuccessful,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown parameter source {other:?}; expected defaults, file, last, or last_successful"
            )));
        }
    };
    ParameterSession::from_profile(bundle, source, &profile)
        .map_err(|error| parameter_runtime_error("resolve parameter profile", error))
}

fn parse_parameter_patch(source: &str) -> PyResult<ResolutionPatch> {
    serde_json::from_str(source)
        .map_err(|error| parameter_runtime_error("parse parameter mutation patch", error))
}

fn parse_parameter_values(source: &str) -> PyResult<BTreeMap<String, ParameterValue>> {
    serde_json::from_str(source)
        .map_err(|error| parameter_runtime_error("parse resolved parameter values", error))
}

fn render_parameter_values(surface_id: &str, values_json: &str) -> PyResult<String> {
    let values = parse_parameter_values(values_json)?;
    let mut session = ParameterSession::defaults(parameter_bundle(surface_id)?)
        .map_err(|error| parameter_runtime_error("resolve parameter defaults", error))?;
    if !values.is_empty() {
        session
            .apply_override_patch(ResolutionPatch {
                values,
                unset: BTreeSet::new(),
            })
            .map_err(|error| parameter_runtime_error("resolve parameter values", error))?;
    }
    session
        .render_sparse()
        .map_err(|error| parameter_runtime_error("render sparse parameter profile", error))
}

#[pyfunction]
fn parameter_profile_surface(profile_toml: &str) -> PyResult<String> {
    parse_profile(profile_toml)
        .map(|profile| profile.header.surface)
        .map_err(|error| parameter_runtime_error("parse parameter profile", error))
}

#[pyfunction]
fn parameter_catalog_json() -> PyResult<String> {
    let catalog = builtin_surface_catalog()
        .map_err(|error| parameter_runtime_error("load parameter catalog", error))?;
    serde_json::to_string(catalog)
        .map_err(|error| parameter_runtime_error("serialize parameter catalog", error))
}

#[pyfunction]
fn parameter_surface_definition_json(surface_id: &str) -> PyResult<String> {
    let catalog = builtin_surface_catalog()
        .map_err(|error| parameter_runtime_error("load parameter catalog", error))?;
    let surface = catalog.surface(surface_id).ok_or_else(|| {
        PyValueError::new_err(format!("unknown configurable surface {surface_id:?}"))
    })?;
    serde_json::to_string(surface)
        .map_err(|error| parameter_runtime_error("serialize parameter definition", error))
}

#[pyfunction]
fn parameter_surface_bundle_json(surface_id: &str) -> PyResult<String> {
    serde_json::to_string(&parameter_bundle(surface_id)?)
        .map_err(|error| parameter_runtime_error("serialize parameter contract", error))
}

#[pyfunction]
fn parameter_defaults_json(surface_id: &str) -> PyResult<String> {
    let session = parameter_session_from_source(surface_id, "defaults", None, None)?;
    parameter_snapshot_json(&session)
}

#[pyfunction]
fn parameter_load_json(
    surface_id: &str,
    profile_toml: &str,
    source_path: PathBuf,
) -> PyResult<String> {
    let session =
        parameter_session_from_source(surface_id, "file", Some(profile_toml), Some(source_path))?;
    parameter_snapshot_json(&session)
}

#[pyfunction]
fn parameter_last_json(
    surface_id: &str,
    workspace: PathBuf,
    successful: bool,
) -> PyResult<Option<String>> {
    let bundle = parameter_bundle(surface_id)?;
    if successful && bundle.surface.kind() == SurfaceKind::Session {
        return Err(PyValueError::new_err(format!(
            "session surface {surface_id:?} does not have Last Successful"
        )));
    }
    let kind = if successful {
        ManagedProfileKind::LastSuccessful
    } else {
        ManagedProfileKind::Last
    };
    let store = ManagedStateStore::for_workspace(workspace);
    let Some(profile_toml) = store
        .read(surface_id, kind)
        .map_err(|error| parameter_runtime_error("read managed parameter profile", error))?
    else {
        return Ok(None);
    };
    let source = if successful {
        "last_successful"
    } else {
        "last"
    };
    let session = parameter_session_from_source(surface_id, source, Some(&profile_toml), None)?;
    parameter_snapshot_json(&session).map(Some)
}

#[pyfunction]
fn parameter_managed_profile_toml(
    surface_id: &str,
    workspace: PathBuf,
    successful: bool,
) -> PyResult<Option<String>> {
    let bundle = parameter_bundle(surface_id)?;
    if successful && bundle.surface.kind() == SurfaceKind::Session {
        return Err(PyValueError::new_err(format!(
            "session surface {surface_id:?} does not have Last Successful"
        )));
    }
    let kind = if successful {
        ManagedProfileKind::LastSuccessful
    } else {
        ManagedProfileKind::Last
    };
    ManagedStateStore::for_workspace(workspace)
        .read(surface_id, kind)
        .map_err(|error| parameter_runtime_error("read managed parameter profile", error))
}

#[pyfunction]
#[pyo3(signature = (surface_id, base_source, profile_toml=None, profile_path=None, patch_json="{\"values\":{},\"unset\":[]}"))]
fn parameter_resolve_json(
    surface_id: &str,
    base_source: &str,
    profile_toml: Option<&str>,
    profile_path: Option<PathBuf>,
    patch_json: &str,
) -> PyResult<String> {
    let mut session =
        parameter_session_from_source(surface_id, base_source, profile_toml, profile_path)?;
    let patch = parse_parameter_patch(patch_json)?;
    if !patch.values.is_empty() || !patch.unset.is_empty() {
        session
            .apply_override_patch(patch)
            .map_err(|error| parameter_runtime_error("resolve parameter mutations", error))?;
    }
    parameter_snapshot_json(&session)
}

#[pyfunction]
fn parameter_render_toml(surface_id: &str, values_json: &str) -> PyResult<String> {
    render_parameter_values(surface_id, values_json)
}

#[pyfunction]
fn parameter_template_toml(surface_id: &str) -> PyResult<String> {
    render_documented_template(&parameter_bundle(surface_id)?)
        .map_err(|error| parameter_runtime_error("render parameter template", error))
}

#[pyfunction]
fn parameter_save_toml(surface_id: &str, values_json: &str, path: PathBuf) -> PyResult<usize> {
    let profile = render_parameter_values(surface_id, values_json)?;
    write_parameter_profile(&path, &profile)?;
    Ok(profile.len())
}

#[pyfunction]
fn parameter_write_managed(
    surface_id: &str,
    workspace: PathBuf,
    values_json: &str,
    successful: bool,
) -> PyResult<String> {
    let bundle = parameter_bundle(surface_id)?;
    if successful && bundle.surface.kind() == SurfaceKind::Session {
        return Err(PyValueError::new_err(format!(
            "session surface {surface_id:?} does not have Last Successful"
        )));
    }
    let profile = render_parameter_values(surface_id, values_json)?;
    let kind = if successful {
        ManagedProfileKind::LastSuccessful
    } else {
        ManagedProfileKind::Last
    };
    let outcome = ManagedStateStore::for_workspace(workspace)
        .write(surface_id, kind, &profile)
        .map_err(|error| parameter_runtime_error("write managed parameter profile", error))?;
    Ok(outcome.path.to_string_lossy().into_owned())
}

fn write_parameter_profile(path: &Path, contents: &str) -> PyResult<()> {
    write_parameter_profile_atomic(path, contents)
        .map(|_| ())
        .map_err(|error| parameter_runtime_error("save parameter profile", error))
}

#[pymodule]
fn _core(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyImage>()?;
    module.add_class::<PyTable>()?;
    module.add_function(wrap_pyfunction!(data_protocol_info_json, module)?)?;
    module.add_function(wrap_pyfunction!(data_schema_bundle_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_catalog_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_surface_definition_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_surface_bundle_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_defaults_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_load_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_last_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_managed_profile_toml, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_profile_surface, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_resolve_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_render_toml, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_template_toml, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_save_toml, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_write_managed, module)?)?;
    Ok(())
}

#[cfg(test)]
mod parameter_bridge_tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn catalog_and_defaults_bridge_use_builtin_contracts() {
        let catalog: serde_json::Value =
            serde_json::from_str(&parameter_catalog_json().unwrap()).unwrap();
        assert_eq!(catalog["surfaces"].as_array().unwrap().len(), 42);

        let defaults: serde_json::Value =
            serde_json::from_str(&parameter_defaults_json("flagmanager").unwrap()).unwrap();
        assert_eq!(defaults["surface_id"], "flagmanager");
        assert_eq!(defaults["surface_kind"], "task");
        assert_eq!(defaults["states"]["mode"]["origin"], "default");
    }

    #[test]
    fn profile_surface_bridge_uses_the_authoritative_toml_parser() {
        let source = r#"[casars]
format = 1
surface = 'flagmanager'
kind = 'task'
contract = 1

[parameters]
vis = ['target.ms']
"#;
        assert_eq!(parameter_profile_surface(source).unwrap(), "flagmanager");

        let missing_surface = r#"[casars]
format = 1
kind = 'task'
contract = 1

[parameters]
vis = ['target.ms']
"#;
        assert!(parameter_profile_surface(missing_surface).is_err());
    }

    #[test]
    fn values_render_and_round_trip_through_managed_last() {
        let values = serde_json::json!({
            "vis": {"kind": "string", "value": "target.ms"},
            "comment": {"kind": "string", "value": "bridge test"}
        })
        .to_string();
        let rendered = parameter_render_toml("flagmanager", &values).unwrap();
        assert!(rendered.contains("vis = [\"target.ms\"]"));
        assert!(rendered.contains("comment = \"bridge test\""));

        let root = std::env::temp_dir().join(format!(
            "casars-python-parameters-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        parameter_write_managed("flagmanager", root.clone(), &values, false).unwrap();
        let snapshot = parameter_last_json("flagmanager", root.clone(), false)
            .unwrap()
            .expect("managed Last snapshot");
        let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(snapshot["states"]["vis"]["origin"], "base_profile");
        let _ = fs::remove_dir_all(root);
    }
}
