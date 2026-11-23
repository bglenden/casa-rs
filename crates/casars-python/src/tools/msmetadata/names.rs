// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;

use casa_ms::{MsError, MsResult};
use numpy::{IntoPyArray, PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyString, PyTuple};

use super::PyMsMetadata;
use crate::convert::{nested_sequence_items, value::CasaValue};

enum Indices {
    Empty,
    EmptyString,
    BooleanVector,
    Scalar(i64),
    Vector(Vec<i64>),
    Unsupported,
}

impl Indices {
    fn capture(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value.filter(|value| !value.is_none()) else {
            return Ok(Self::Empty);
        };
        if let Ok(text) = value.cast::<PyString>() {
            return Ok(if text.to_str()?.is_empty() {
                Self::EmptyString
            } else {
                Self::Unsupported
            });
        }
        if value.is_instance_of::<PyBool>() {
            return Ok(Self::Unsupported);
        }
        let items = if let Ok(array) = value.cast::<PyUntypedArray>() {
            match array.dtype().kind() {
                b'b' if array.len() != 0 => return Ok(Self::BooleanVector),
                b'b' => return Ok(Self::Empty),
                b'i' | b'u' => value
                    .call_method1("ravel", ("F",))?
                    .call_method0("tolist")?
                    .try_iter()?
                    .collect::<PyResult<Vec<_>>>()?,
                _ => return Ok(Self::Unsupported),
            }
        } else if value.is_instance_of::<PyTuple>() || value.is_instance_of::<PyList>() {
            if let Some((_, items)) = nested_sequence_items(value)? {
                items
            } else {
                value.try_iter()?.collect::<PyResult<Vec<_>>>()?
            }
        } else if let Ok(index) = value.extract::<i64>() {
            return Ok(Self::Scalar(index));
        } else {
            return Ok(Self::Unsupported);
        };
        if items.is_empty() {
            return Ok(Self::Empty);
        }
        if items.iter().all(|item| item.is_instance_of::<PyBool>()) {
            return Ok(Self::BooleanVector);
        }
        match items
            .iter()
            .map(|item| item.extract::<i64>())
            .collect::<PyResult<Vec<_>>>()
        {
            Ok(values) => Ok(Self::Vector(values)),
            Err(_) => Ok(Self::Unsupported),
        }
    }

    fn rows(self, count: usize, name: &str) -> MsResult<Vec<usize>> {
        match self {
            Self::Empty => Ok((0..count).collect()),
            Self::Scalar(index) => Ok(vec![checked_row(index, count, name)?]),
            Self::Vector(indices) => indices
                .into_iter()
                .map(|index| checked_row(index, count, name))
                .collect(),
            _ => Err(MsError::InvalidInput(format!(
                "Unsupported type for {name}. It must be an integer or an integer array"
            ))),
        }
    }
}

fn checked_row(index: i64, count: usize, name: &str) -> MsResult<usize> {
    usize::try_from(index)
        .ok()
        .filter(|&index| index < count)
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "{name} {index} out of range, must be nonnegative and less than {count}"
            ))
        })
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (antennaids = None), text_signature = "($self, antennaids=-1)")]
    fn antennanames(
        &self,
        py: Python<'_>,
        antennaids: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let ids = match Indices::capture(antennaids)? {
            Indices::BooleanVector => Indices::Empty,
            Indices::Scalar(id) if id < 0 => Indices::Empty,
            ids => ids,
        };
        self.with_ms(py, |ms| {
            let antenna = ms.antenna()?;
            ids.rows(antenna.row_count(), "Antenna ID")?
                .into_iter()
                .map(|row| antenna.name(row))
                .collect()
        })
    }

    #[pyo3(signature = (fieldids = None), text_signature = "($self, fieldids='')")]
    fn namesforfields(
        &self,
        py: Python<'_>,
        fieldids: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let ids = match Indices::capture(fieldids)? {
            Indices::EmptyString => Indices::Empty,
            ids => ids,
        };
        self.with_ms(py, |ms| {
            let fields = ms.field()?;
            ids.rows(fields.row_count(), "Field ID")?
                .into_iter()
                .map(|row| fields.name(row))
                .collect()
        })
    }

    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn namesforspws(
        &self,
        py: Python<'_>,
        spwids: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let ids = match Indices::capture(spwids)? {
            Indices::EmptyString | Indices::BooleanVector => Indices::Empty,
            ids => ids,
        };
        self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            ids.rows(windows.row_count(), "Spectral window ID")?
                .into_iter()
                .map(|row| windows.name(row))
                .collect()
        })
    }

    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn bandwidths(&self, py: Python<'_>, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let ids = match Indices::capture(spw)? {
            Indices::BooleanVector => Indices::Empty,
            Indices::Scalar(id) if id < 0 => Indices::Empty,
            ids => ids,
        };
        let bandwidth = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            if let Indices::Scalar(id) = ids {
                let row = checked_row(id, windows.row_count(), "Spectral window ID")?;
                return Ok(CasaValue::Float(windows.total_bandwidth(row)?));
            }
            let values = ids
                .rows(windows.row_count(), "Spectral window ID")?
                .into_iter()
                .map(|row| windows.total_bandwidth(row))
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::Floats {
                shape: vec![values.len()],
                values,
            })
        })?;
        bandwidth.into_python(py)
    }

    #[pyo3(signature = (field = -1), text_signature = "($self, field=-1)")]
    fn sourceidforfield(&self, py: Python<'_>, field: i64) -> PyResult<i32> {
        self.with_ms(py, |ms| {
            let fields = ms.field()?;
            fields.source_id(checked_row(field, fields.row_count(), "Field ID")?)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourceidsfromsourcetable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let ids = self.with_ms(py, |ms| {
            let sources = ms.source()?;
            (0..sources.row_count())
                .map(|row| sources.i32(row, "SOURCE_ID").map(i64::from))
                .collect::<MsResult<Vec<_>>>()
        })?;
        Ok(ids.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcenames(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.with_ms(py, |ms| {
            let sources = ms.source()?;
            (0..sources.row_count())
                .map(|row| sources.string(row, "NAME"))
                .collect()
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nsources(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_ms(py, |ms| {
            let sources = ms.source()?;
            let ids = (0..sources.row_count())
                .map(|row| sources.i32(row, "SOURCE_ID"))
                .collect::<MsResult<BTreeSet<_>>>()?;
            Ok(ids.len())
        })
    }
}
