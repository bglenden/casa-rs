// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversion of the attached imager's flux-calibration arguments.

use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use pyo3::{PyResult, exceptions::PyTypeError};

use crate::convert::{Variant, measures::frequency};

pub(super) fn coefficients(value: Variant, name: &str) -> PyResult<Vec<f64>> {
    match value {
        Variant::Empty => Ok(Vec::new()),
        Variant::Number(value) => Ok(vec![value]),
        Variant::Numbers(values) => Ok(values),
        _ => Err(PyTypeError::new_err(format!(
            "{name} requires a number or a numeric vector"
        ))),
    }
}

pub(super) fn flux(value: Variant) -> PyResult<[f64; 4]> {
    let values = coefficients(value, "fluxdensity")?;
    let mut stokes = [0.; 4];
    for (out, value) in stokes.iter_mut().zip(values) {
        *out = value;
    }
    Ok(stokes)
}

pub(super) fn reference(value: &Variant) -> PyResult<MFrequency> {
    let default = || MFrequency::new(1e9, FrequencyRef::LSRK);
    if matches!(value, Variant::Empty) {
        return Ok(default());
    }
    Ok(frequency(value)?.0.unwrap_or_else(default))
}
