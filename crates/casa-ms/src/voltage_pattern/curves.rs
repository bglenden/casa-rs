// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sampled radial beam laws stored by CASA VPManager.

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_numerics::BeamPattern;

use super::{VoltagePatternError, radial_pattern, reference_radius};

/// Return the peak and retained bytes of a saved radial lookup. The input
/// record is covered by the caller's table reservation.
#[must_use]
pub fn radial_curve_storage(record: &RecordValue) -> (u64, u64) {
    let len = |field| match record.get(field) {
        Some(Value::Array(ArrayValue::Float64(value))) => value.len() as u64,
        _ => 0,
    };
    let lookup = BeamPattern::BYTES + 128;
    match name(record) {
        Some("IPOLY") if record.get("fitfreqs").is_some() => {
            let retained = 256 + (len("coeff") + len("fitfreqs")) * size_of::<f64>() as u64;
            (retained, retained)
        }
        Some("COSPOLY") => (lookup + len("scale") * size_of::<f64>() as u64, lookup),
        Some("NUMERIC") => (
            lookup + len("vect") * (size_of::<f32>() + size_of::<f64>()) as u64,
            lookup,
        ),
        _ => (lookup, lookup),
    }
}

/// Compile a Gaussian, Airy, polynomial, inverse polynomial, cosine
/// polynomial or sinc-interpolated radial pattern from its CASA record.
/// The caller reserves the memory reported by [`radial_curve_storage`].
///
/// # Errors
/// The record is malformed, has no valid radial lookup, or uses another law.
pub fn radial_lookup(record: &RecordValue) -> Result<BeamPattern, VoltagePatternError> {
    let name = name(record);
    if matches!(name, Some("GAUSS" | "AIRY")) {
        return Ok(radial_pattern(record)?.pattern());
    }
    let (radius, ghz) = reference_radius(record)?;
    let Some(Value::Scalar(ScalarValue::Bool(voltage))) = record.get("isthisvp") else {
        return Err(VoltagePatternError::read("missing boolean isthisvp"));
    };
    let voltage = *voltage;
    let lookup = match name {
        Some("POLY") => BeamPattern::polynomial(radius, numbers(record, "coeff")?, false, voltage),
        Some("IPOLY") if record.get("fitfreqs").is_some() => {
            frequency_curve(record, radius, voltage)?
        }
        Some("IPOLY") => BeamPattern::polynomial(radius, numbers(record, "coeff")?, true, voltage),
        Some("COSPOLY") => BeamPattern::cosine_polynomial(
            radius,
            numbers(record, "coeff")?,
            &numbers(record, "scale")?
                .iter()
                .map(|scale| scale / ghz)
                .collect::<Vec<_>>(),
            voltage,
        ),
        Some("NUMERIC") => BeamPattern::numeric(
            radius,
            &numbers(record, "vect")?
                .iter()
                .map(|value| *value as f32)
                .collect::<Vec<_>>(),
            voltage,
        ),
        _ => {
            return Err(VoltagePatternError::read(
                "expected a saved radial beam law",
            ));
        }
    };
    lookup
        .ok_or_else(|| VoltagePatternError::read("invalid radial coefficients or voltage samples"))
}

fn name(record: &RecordValue) -> Option<&str> {
    match record.get("name") {
        Some(Value::Scalar(ScalarValue::String(name))) => Some(name),
        _ => None,
    }
}

fn numbers<'a>(record: &'a RecordValue, name: &str) -> Result<&'a [f64], VoltagePatternError> {
    if let Some(Value::Array(ArrayValue::Float64(values))) = record.get(name)
        && values.ndim() == 1
        && let Some(values) = values.as_slice()
        && !values.is_empty()
        && values.iter().all(|value| value.is_finite())
    {
        return Ok(values);
    }
    Err(VoltagePatternError::read(format!(
        "invalid numeric vector {name}"
    )))
}

fn frequency_curve(
    record: &RecordValue,
    radius: f64,
    voltage: bool,
) -> Result<Option<BeamPattern>, VoltagePatternError> {
    let Some(Value::Array(ArrayValue::Float64(coefficients))) = record.get("coeff") else {
        return Err(VoltagePatternError::read("missing coefficient matrix"));
    };
    let frequencies = numbers(record, "fitfreqs")?;
    if coefficients.ndim() != 2 || coefficients.shape()[1] != frequencies.len() {
        return Err(VoltagePatternError::read(
            "coefficient columns and fit frequencies do not match",
        ));
    }
    let mut columns = Vec::with_capacity(coefficients.len());
    for column in coefficients.axis_iter(ndarray::Axis(1)) {
        columns.extend(column.iter().copied());
    }
    Ok(BeamPattern::frequency_inverse_polynomial(
        radius,
        columns,
        frequencies.to_vec(),
        voltage,
    ))
}
