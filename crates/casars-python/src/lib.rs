// SPDX-License-Identifier: LGPL-3.0-or-later

mod object_contract;

use std::collections::BTreeSet;
use std::path::PathBuf;

use casa_images::{AnyPagedImage, ImageError, ImagePixelType};
use casa_tables::{RowRange, Table as CasaTable, TableError, TableOptions};
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
    fn coordinate_system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let coordinates = match &self.inner {
            AnyPagedImage::Float32(image) => image.coordinates(),
            AnyPagedImage::Float64(image) => image.coordinates(),
            AnyPagedImage::Complex32(image) => image.coordinates(),
            AnyPagedImage::Complex64(image) => image.coordinates(),
        };
        record_to_py(py, &coordinates.to_record())
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

#[pymodule]
fn _core(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Keep the generated UniFFI frontend boundary in this extension module so
    // Python wheels need only one native library. The Python package loads
    // these exported symbols through its generated UniFFI module; PyO3 remains
    // reserved for the NumPy-oriented Image and Table object API below.
    let _ = casars_frontend_services::application_catalog();
    module.add_class::<PyImage>()?;
    module.add_class::<PyTable>()?;
    module.add_function(wrap_pyfunction!(data_protocol_info_json, module)?)?;
    module.add_function(wrap_pyfunction!(data_schema_bundle_json, module)?)?;
    Ok(())
}
