// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered record selection for the public MeasurementSet tool.

use casa_coordinates::display::format_stream_value;
use casa_core::quanta::format::{YMD, format_time};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::MsSelection;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyComplex, PyDict, PyFloat, PyInt, PyList, PySlice, PyString, PyTuple};

use super::selection_indices::selection_dictionary;
use super::{AttachedMs, PyMsTool, runtime_error};
use crate::tools::table::casa_record;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None), text_signature = "($self, items={})")]
    fn select(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selection_dictionary)] items: Option<Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let record = selection_record(py, items.as_ref())?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.select_record(record),
                None => Ok(true),
            }
        })
    }
}

pub(super) fn selection_record(
    py: Python<'_>,
    items: Option<&Bound<'_, PyDict>>,
) -> PyResult<RecordValue> {
    let Some(items) = items.filter(|items| !items.is_empty()) else {
        return Ok(RecordValue::default());
    };
    let converted = PyDict::new(py);
    for (key, item) in items.iter() {
        let name = if let Ok(bytes) = key.cast::<PyBytes>() {
            String::from_utf8_lossy(bytes.as_bytes()).into_owned()
        } else if let Ok(name) = key.cast::<PyString>() {
            name.extract::<String>()?
        } else {
            continue;
        };
        if RecordSelector::from_label(&name.to_ascii_uppercase()).is_none() {
            continue;
        }
        let item = if let Ok(array) = item.cast::<PyUntypedArray>() {
            let dtype = array.dtype();
            if !matches!(
                (dtype.kind(), dtype.itemsize()),
                (b'b', 1)
                    | (b'i' | b'u', 1 | 2 | 4 | 8)
                    | (b'f', 4 | 8)
                    | (b'c', 8 | 16)
                    | (b'U', _)
            ) {
                continue;
            }
            let array = if array.ndim() == 0 {
                // The CASA variant for a zero-dimensional array has no elements.
                item.call_method0("ravel")?
                    .get_item(PySlice::new(py, 0, 0, 1))?
            } else {
                item
            };
            if dtype.kind() == b'u' && dtype.itemsize() == 8 {
                // The selectors consume Int32 IDs. Preserve the low bits of
                // UInt64 input even when its value exceeds the signed range.
                array.call_method1("astype", ("int64",))?
            } else {
                array
            }
        } else if let Ok(bytes) = item.cast::<PyBytes>() {
            PyString::new(py, &String::from_utf8_lossy(bytes.as_bytes())).into_any()
        } else if item.is_instance_of::<PyInt>()
            || item.is_instance_of::<PyFloat>()
            || item.is_instance_of::<PyComplex>()
            || item.is_instance_of::<PyString>()
            || item.is_instance_of::<PyDict>()
            || item.is_instance_of::<PyList>()
            || item.is_instance_of::<PyTuple>()
        {
            item
        } else {
            // Unrecognized variants, including standalone NumPy integer
            // scalars and None, are omitted by the CASA binding.
            continue;
        };
        converted.set_item(name, item)?;
    }
    casa_record(py, Some(&converted))
}

pub(super) enum RecordSelector {
    ScalarIds,
    Rows,
    IfrNumber,
    Time,
    Times,
    Uvw(usize),
    UvDistance,
}

impl RecordSelector {
    pub(super) fn from_label(label: &str) -> Option<Self> {
        Some(match label {
            "ANTENNA1" | "ANTENNA2" | "ARRAY_ID" | "FEED1" | "FEED2" | "FIELD_ID"
            | "SCAN_NUMBER" | "DATA_DESC_ID" => Self::ScalarIds,
            "ROWS" => Self::Rows,
            "IFR_NUMBER" => Self::IfrNumber,
            "TIME" => Self::Time,
            "TIMES" => Self::Times,
            "U" => Self::Uvw(1),
            "V" => Self::Uvw(2),
            "W" => Self::Uvw(3),
            "UVDIST" => Self::UvDistance,
            _ => return None,
        })
    }
}

impl AttachedMs {
    fn select_record(&mut self, record: RecordValue) -> PyResult<bool> {
        if !self.initialize_record_selection()? {
            return Ok(true);
        }
        let mut fields = record.into_fields();
        // The binding's record is ordered by the original key spelling.
        // ROWID() in each field refers to the result of the previous fields.
        fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let mut result = true;
        for field in fields {
            result &= self.apply_record_field(field)?;
        }
        Ok(result)
    }

    fn apply_record_field(&mut self, field: RecordField) -> PyResult<bool> {
        let label = field.name.to_ascii_uppercase();
        let selector = RecordSelector::from_label(&label).expect("converted selection field");
        match selector {
            RecordSelector::ScalarIds | RecordSelector::Rows => {
                let values = integer_items(&field)?;
                let name = if label == "ROWS" { "ROWID()" } else { &label };
                let ids = values
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                self.apply_direct_taql(format!("{name} IN [{ids}]"))
            }
            RecordSelector::IfrNumber => {
                let baselines = integer_items(&field)?
                    .into_iter()
                    .map(|number| format!("{}&{}", number / 1000, number % 1000))
                    .collect::<Vec<_>>()
                    .join(";");
                self.apply_structured_record_selector(MsSelection {
                    antenna: Some(baselines),
                    ..MsSelection::default()
                })
            }
            RecordSelector::Times => {
                let snapshot = self.selection_snapshot()?;
                let mut rows = snapshot
                    .ms
                    .select_ms_tool_times(real_items(&field)?)
                    .map_err(runtime_error)?;
                self.map_tool_rows(&mut rows);
                self.set_exact_time_rows(rows);
                Ok(true)
            }
            range => self.apply_record_range(range, &field),
        }
    }

    fn apply_record_range(&mut self, range: RecordSelector, field: &RecordField) -> PyResult<bool> {
        let values = real_items(field)?;
        let &[lower, upper] = values.as_slice() else {
            return Ok(false);
        };
        match range {
            RecordSelector::Time => {
                if !lower.is_finite() || !upper.is_finite() {
                    return Err(runtime_error("time range requires finite MJD seconds"));
                }
                let start = format_time(lower / 86_400.0, YMD, 0);
                let end = format_time(upper / 86_400.0, YMD, 0);
                self.apply_structured_record_selector(MsSelection {
                    timerange: Some(format!("{start}~{end}")),
                    ..MsSelection::default()
                })
            }
            RecordSelector::UvDistance => self.apply_structured_record_selector(MsSelection {
                uvrange: Some(format!(
                    "{}~{}",
                    format_stream_value(lower),
                    format_stream_value(upper)
                )),
                ..MsSelection::default()
            }),
            RecordSelector::Uvw(axis) => {
                let lower = format_stream_value(lower);
                let upper = format_stream_value(upper);
                self.apply_direct_taql(format!("UVW[{axis}] >= {lower} && UVW[{axis}] <= {upper}"))
            }
            _ => unreachable!("record range field checked before dispatch"),
        }
    }

    fn apply_structured_record_selector(&mut self, selection: MsSelection) -> PyResult<bool> {
        let snapshot = self.selection_snapshot()?;
        let parsed = self
            .selection_state
            .parse(&snapshot.ms, &selection)
            .map_err(runtime_error)?;
        if !parsed.has_row_expression() {
            self.synchronize_metadata_selection()?;
            return Ok(false);
        }
        let rows = parsed.select_rows(&snapshot.ms).map_err(runtime_error)?;
        if !self.apply_tool_rows(rows)? {
            return Err(runtime_error(
                "MSSelectionNullSelection : The selected table has zero rows.",
            ));
        }
        Ok(true)
    }
}

fn incorrect_type(field: &RecordField) -> PyErr {
    runtime_error(format!(
        "RecordRep::get_pointer - incorrect data type for field {}",
        field.name
    ))
}

fn ensure_vector(shape: &[usize], field: &RecordField) -> PyResult<()> {
    if shape.iter().filter(|&&size| size > 1).count() > 1 {
        return Err(runtime_error(format!(
            "ms.select field {} requires a vector, got array shape {shape:?}",
            field.name
        )));
    }
    Ok(())
}

pub(super) fn integer_items(field: &RecordField) -> PyResult<Vec<i32>> {
    integer_items_as(field, |value| value as i32)
}

pub(super) fn integer_items_as<T>(
    field: &RecordField,
    convert: impl Fn(i64) -> T,
) -> PyResult<Vec<T>> {
    macro_rules! integers {
        ($values:expr) => {{
            ensure_vector($values.shape(), field)?;
            $values
                .view()
                .reversed_axes()
                .iter()
                .map(|&value| convert(i64::from(value)))
                .collect()
        }};
    }
    Ok(match &field.value {
        Value::Scalar(ScalarValue::UInt8(value)) => vec![convert(i64::from(*value))],
        Value::Scalar(ScalarValue::UInt16(value)) => vec![convert(i64::from(*value))],
        Value::Scalar(ScalarValue::UInt32(value)) => vec![convert(i64::from(*value))],
        Value::Scalar(ScalarValue::Int16(value)) => vec![convert(i64::from(*value))],
        Value::Scalar(ScalarValue::Int32(value)) => vec![convert(i64::from(*value))],
        Value::Scalar(ScalarValue::Int64(value)) => vec![convert(*value)],
        Value::Array(ArrayValue::UInt8(values)) => integers!(values),
        Value::Array(ArrayValue::UInt16(values)) => integers!(values),
        Value::Array(ArrayValue::UInt32(values)) => integers!(values),
        Value::Array(ArrayValue::Int16(values)) => integers!(values),
        Value::Array(ArrayValue::Int32(values)) => integers!(values),
        Value::Array(ArrayValue::Int64(values)) => integers!(values),
        _ => return Err(incorrect_type(field)),
    })
}

pub(super) fn real_items(field: &RecordField) -> PyResult<Vec<f64>> {
    Ok(match &field.value {
        Value::Scalar(ScalarValue::Float32(value)) => vec![f64::from(*value)],
        Value::Scalar(ScalarValue::Float64(value)) => vec![*value],
        Value::Array(ArrayValue::Float32(values)) => {
            ensure_vector(values.shape(), field)?;
            values
                .view()
                .reversed_axes()
                .iter()
                .copied()
                .map(f64::from)
                .collect()
        }
        Value::Array(ArrayValue::Float64(values)) => {
            ensure_vector(values.shape(), field)?;
            values.view().reversed_axes().iter().copied().collect()
        }
        _ => return Err(incorrect_type(field)),
    })
}
