// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA spectrum arguments and their safe conversion.

use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyComplex, PyString};

use super::{PyComponentList, setflux, setshape, supplied};
use crate::convert::Variant;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

pub(super) struct VectorArg {
    pub(super) values: Vec<f64>,
    pub(super) invalid: bool,
}

pub(super) struct Request {
    pub(super) name: String,
    pub(super) index: Variant,
    pub(super) frequencies: VectorArg,
    pub(super) flux: VectorArg,
    pub(super) frame: String,
}

/// Convert a binding double-vector in column-major order; retain failed
/// Python conversions so no component is changed while an error is pending.
pub(super) fn vector(value: Option<Bound<'_, PyAny>>, default: f64) -> PyResult<(VectorArg, bool)> {
    let Some(value) = value else {
        return Ok((
            VectorArg {
                values: vec![default],
                invalid: false,
            },
            false,
        ));
    };
    let pending = setshape::optional_pending(&value)?;
    if value.is_none() {
        return Ok((
            VectorArg {
                values: Vec::new(),
                invalid: false,
            },
            pending,
        ));
    }
    let numpy = value.py().import("numpy")?;
    let array = match numpy.getattr("asarray")?.call1((&value, "object")) {
        Ok(array) => array,
        Err(_) => {
            return Ok((
                VectorArg {
                    values: Vec::new(),
                    invalid: true,
                },
                pending,
            ));
        }
    };
    let flattened = array.call_method1("ravel", ("F",))?;
    let mut values = Vec::new();
    let mut invalid = false;
    for item in flattened.try_iter()? {
        let item = item?;
        let number = if item.is_none() {
            Some(0.0)
        } else if let Ok(flag) = item.cast::<PyBool>() {
            Some(if flag.is_true() { 1.0 } else { 0.0 })
        } else if let Ok(text) = item.cast::<PyString>() {
            text.to_str()?.trim().parse::<f64>().ok()
        } else if let Ok(complex) = item.cast::<PyComplex>() {
            (complex.imag() == 0.0).then_some(complex.real())
        } else {
            item.extract::<f64>().ok()
        };
        match number {
            Some(number) => values.push(number),
            None => {
                invalid = true;
                values.push(0.0);
            }
        }
    }
    Ok((VectorArg { values, invalid }, pending))
}

pub(super) fn text(value: Option<Bound<'_, PyAny>>, default: &str, name: &str) -> PyResult<String> {
    let string: String = match value {
        None => default.to_string(),
        Some(value) if value.is_instance_of::<PyString>() => value.extract()?,
        Some(value) if value.is_instance_of::<PyBytes>() => {
            let bytes = value.cast::<PyBytes>()?.as_bytes();
            let end = bytes
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(bytes.len());
            PyBytes::new(value.py(), &bytes[..end])
                .call_method1("decode", ("utf-8",))?
                .extract()?
        }
        Some(_) => {
            return Err(PyTypeError::new_err(format!(
                "argument _{name} must be a string"
            )));
        }
    };
    Ok(string.split('\0').next().unwrap_or_default().to_string())
}

#[pymethods]
impl PyComponentList {
    /// Replace a component's spectral model while retaining its frequency.
    #[pyo3(
        signature = (which, r#type = None, index = None, tabularfreqs = None, tabularflux = None, tabularframe = None),
        text_signature = "($self, which, type='spectral index', index=[], tabularfreqs=[100000000000.0], tabularflux=[1.0], tabularframe='LSRK')"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setspectrum(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] r#type: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] index: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularfreqs: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularflux: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularframe: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, index_pending) = match which.extract::<i64>() {
            Ok(index) => (i64::from(index as i32), false),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                (-1, true)
            }
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let name = text(r#type, "spectral index", "type")?;
        let (index, value_pending) = setflux::argument(index)?;
        let (frequencies, freq_pending) = vector(tabularfreqs, 1e11)?;
        let (flux, flux_pending) = vector(tabularflux, 1.0)?;
        let frame = text(tabularframe, "LSRK", "tabularframe")?;
        let pending = index_pending || value_pending || freq_pending || flux_pending;
        let request = Request {
            name,
            index,
            frequencies,
            flux,
            frame,
        };
        let result = self.call(py, move |state, lines| {
            state
                .setspectrum(which, &request, pending)
                .inspect_err(|err| {
                    lines.push(LogLine::error(err.to_string()));
                })
                .map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_setspectrum> returned a result with an exception set",
            ));
        }
        Ok(result)
    }
}
