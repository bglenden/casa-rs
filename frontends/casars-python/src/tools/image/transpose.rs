// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.transpose` wrapper delegating to [`casa_imageanalysis::tool::transpose`].

use casa_imageanalysis::tool::transpose::{TransposeOrder as PureOrder, transpose_attached};
use numpy::PyUntypedArray;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyList, PyString, PyTuple};

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{external_open_check, raised};

enum TransposeOrder {
    Integer(i64),
    String(String),
    Names(Vec<String>),
    Unsupported(&'static str),
}

fn text(value: &Bound<'_, PyAny>) -> Option<String> {
    if let Ok(value) = value.cast::<PyBytes>() {
        return Some(String::from_utf8_lossy(value.as_bytes()).into_owned());
    }
    value.cast::<PyString>().ok().map(|value| value.to_string())
}

impl FromPyObject<'_, '_> for TransposeOrder {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Ok(Self::Unsupported("bool"));
        }
        if let Some(value) = text(&value) {
            return Ok(Self::String(value));
        }
        if let Ok(value) = value.extract::<i64>() {
            return Ok(Self::Integer(value));
        }
        if (value.is_instance_of::<PyList>()
            || value.is_instance_of::<PyTuple>()
            || value.is_instance_of::<PyUntypedArray>())
            && let Ok(items) = value.try_iter()
        {
            let items = items.collect::<PyResult<Vec<_>>>()?;
            if !items.is_empty()
                && let Some(names) = items.iter().map(text).collect::<Option<Vec<_>>>()
            {
                return Ok(Self::Names(names));
            }
            if !items.is_empty() && items.iter().all(|item| item.extract::<i64>().is_ok()) {
                return Ok(Self::Unsupported("intvec"));
            }
        }
        Ok(Self::Unsupported(match value.extract::<Variant>()? {
            Variant::Empty => "boolvec",
            Variant::Number(_) => "double",
            Variant::Complex(_) => "complex",
            Variant::Complexes(_) | Variant::ComplexArray { .. } => "complexvec",
            Variant::Numbers(_) | Variant::Array { .. } => "doublevec",
            Variant::Record(_) => "record",
            _ => "variant",
        }))
    }
}

impl TransposeOrder {
    fn into_pure(self) -> PureOrder {
        match self {
            Self::Integer(v) => PureOrder::Integer(v),
            Self::String(s) => PureOrder::Digits(s),
            Self::Names(names) => PureOrder::Tokens(names),
            Self::Unsupported(kind) => PureOrder::Unsupported(kind),
        }
    }
}

struct OutputName(String);

impl FromPyObject<'_, '_> for OutputName {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        text(&value)
            .map(Self)
            .ok_or_else(|| PyTypeError::new_err("argument _outfile must be a string"))
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile=OutputName(String::new()), order=TransposeOrder::Unsupported("boolvec")), text_signature = "($self, outfile='', order=[])")]
    fn transpose(
        &self,
        py: Python<'_>,
        outfile: OutputName,
        order: TransposeOrder,
    ) -> PyResult<Py<Self>> {
        let order = order.into_pure();
        let output = py.detach(|| {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("No image specified to transpose"))?;
            transpose_attached(
                attachment,
                &outfile.0,
                &order,
                false,
                state.history,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(output))
    }
}
