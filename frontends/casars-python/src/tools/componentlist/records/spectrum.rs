// SPDX-License-Identifier: LGPL-3.0-or-later
//! Required spectral fields: table loading tolerates failures, cl imports do not.
use super::{numbers, scalar};
use casa_components::SpectralType;
use casa_core::{ArrayValue, RecordValue, Value};

pub(super) fn validate(record: &RecordValue, kind: SpectralType) -> Result<(), String> {
    match kind {
        SpectralType::Constant => Ok(()),
        SpectralType::SpectralIndex => index(record),
        SpectralType::PowerLogPolynomial => polynomial(record),
        SpectralType::Tabular => tabular(record),
    }
}
fn index(record: &RecordValue) -> Result<(), String> {
    let value = record
        .get("index")
        .ok_or("The 'spectrum' record must have an 'index' field\n")?;
    real_scalar(value, "index")?;
    if let Some(value) = record.get("stokesindex")
        && numbers(value).is_none_or(|v| !matches!(v.len(), 1 | 4))
    {
        return Err("Stokes indices is not of length 1 or 4\n".into());
    }
    if let Some(value) = record.get("error") {
        real_scalar(value, "error")?;
    }
    Ok(())
}
fn real_scalar(value: &Value, name: &str) -> Result<(), String> {
    if let Value::Array(array) = value
        && array.shape().iter().product::<usize>() != 1
    {
        return Err(format!("The '{name}' field must be a scalar\n"));
    }
    if scalar(value).is_none() {
        return Err(format!("The '{name}' field must be a real number\n"));
    }
    Ok(())
}
fn real_array(value: &Value) -> Option<usize> {
    match value {
        Value::Array(ArrayValue::Float64(v)) => Some(v.len()),
        Value::Array(ArrayValue::Float32(v)) => Some(v.len()),
        Value::Array(ArrayValue::Int32(v)) => Some(v.len()),
        _ => None,
    }
}
fn polynomial(record: &RecordValue) -> Result<(), String> {
    let coefficients = record
        .get("coeffs")
        .ok_or("The record must have an 'coeffs' field")?;
    let length = real_array(coefficients).ok_or("The 'coeff' field must be an array of numbers")?;
    if let Some(error) = record.get("error") {
        let n = real_array(error).ok_or("The 'error' field must be an array of numbers")?;
        if n != length {
            return Err("Coefficient and error array lengths are not equal".into());
        }
    }
    Ok(())
}
fn tabular(record: &RecordValue) -> Result<(), String> {
    for name in ["freqRef", "tabFreqVal", "ival", "referenceFreq"] {
        if record.get(name).is_none() {
            return Err(format!(
                "The 'TabularSpectrum' record must have an '{name}' field\n"
            ));
        }
    }
    if record.get("maxFreq").is_none() || record.get("minFreq").is_none() {
        return Err(
            "The 'TabularSpectrum' record must have a 'maxFreq' and a 'minFreq' field\n".into(),
        );
    }
    if let Some(value) = record.get("freqRef")
        && !matches!(value, Value::Record(_))
    {
        return Err(format!(
            "RecordRep::get_pointer - incorrect data type Record   used for field freqRef with type {}",
            super::type_name(value)
        ));
    }
    for name in ["referenceFreq", "maxFreq", "minFreq"] {
        if record.get(name).and_then(scalar).is_none() {
            return Err("RecordInterface::asDouble - invalid data type".into());
        }
    }
    for name in ["tabFreqVal", "ival", "qval", "uval", "vval"] {
        if let Some(value) = record.get(name)
            && real_array(value).is_none()
        {
            return Err(format!(
                "RecordRep::get_pointer - incorrect data type unknown  used for field {name} with type {}",
                super::type_name(value)
            ));
        }
    }
    Ok(())
}
