// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write visibility records through the current MS tool view.

use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;
use numpy::{PyArray3, PyArrayDescrMethods, PyArrayMethods, PyUntypedArrayMethods};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::legacy_selection::deprecation;
use super::{PyMsTool, runtime_error};
use crate::tools::table::casa_record;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = RecordValue::default()), text_signature = "($self, items={})")]
    fn putdata(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_record)] items: RecordValue,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut().filter(|ms| ms.writable) {
                Some(ms) => ms.putdata_rows(items),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (items = RecordValue::default()), text_signature = "($self, items={})")]
    fn putdataold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_legacy_record)] items: RecordValue,
    ) -> PyResult<bool> {
        deprecation(py, "putdataold", "putdata")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut().filter(|ms| ms.writable) {
                Some(ms) => ms.put_legacy_data_rows(items),
                None => Ok(false),
            }
        })
    }
}

fn write_legacy_record(value: &Bound<'_, PyAny>) -> PyResult<RecordValue> {
    if let Ok(record) = value.cast::<PyDict>()
        && record.len() == 1
        && let Some((key, item)) = record.iter().next()
        && let Ok(name) = key.extract::<String>()
        && ["data", "corrected_data", "model_data"]
            .iter()
            .any(|column| name.eq_ignore_ascii_case(column))
        && let Some(value) = packed_legacy_data(&item)
    {
        return Ok(RecordValue::new(vec![RecordField {
            name,
            value: Value::Array(value),
        }]));
    }
    write_record(value)
}

fn packed_legacy_data(value: &Bound<'_, PyAny>) -> Option<ArrayValue> {
    let py = value.py();
    if !value.get_type().is(py.get_type::<PyArray3<Complex32>>()) {
        return None;
    }
    if let Ok(array) = value.cast::<PyArray3<Complex32>>()
        && array.dtype().is_native_byteorder() == Some(true)
        && array.is_fortran_contiguous()
    {
        let reader = array.try_readonly().ok()?;
        let values = reader.as_slice().ok()?.to_vec();
        return ArrayD::from_shape_vec(IxDyn(array.shape()).f(), values)
            .ok()
            .map(ArrayValue::Complex32);
    }
    let array = value.cast::<PyArray3<Complex64>>().ok()?;
    if array.dtype().is_native_byteorder() != Some(true) || !array.is_fortran_contiguous() {
        return None;
    }
    let reader = array.try_readonly().ok()?;
    let values = reader
        .as_slice()
        .ok()?
        .iter()
        .map(|value| Complex32::new(value.re as f32, value.im as f32))
        .collect();
    ArrayD::from_shape_vec(IxDyn(array.shape()).f(), values)
        .ok()
        .map(ArrayValue::Complex32)
}

fn write_record(value: &Bound<'_, PyAny>) -> PyResult<RecordValue> {
    let record = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("_items is not a dictionary"))?;
    casa_record(value.py(), Some(record))
}
