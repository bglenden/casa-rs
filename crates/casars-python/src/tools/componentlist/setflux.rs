// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's component flux setter, preserving values it would silently discard.

use casa_components::{Flux, Polarisation};
use num_complex::Complex64;
use pyo3::prelude::*;

use crate::convert::{ConvertError, ConvertResult, Variant, casacore_unit, complex_values};

/// Preserve NumPy scalar and empty-array distinctions in the CASA binding.
pub(super) fn argument(value: Option<Bound<'_, PyAny>>) -> PyResult<(Variant, bool)> {
    if let Some(ref object) = value {
        let ndim = object.getattr("ndim").and_then(|v| v.extract::<usize>());
        if ndim.is_ok() && object.is_instance(&object.py().import("numpy")?.getattr("ndarray")?)? {
            if matches!(ndim, Ok(0)) {
                return super::setrefdir_args::argument(Some(object.call_method0("item")?));
            }
            let size = object.getattr("size")?.extract::<usize>()?;
            let kind = object
                .getattr("dtype")?
                .getattr("kind")?
                .extract::<String>()?;
            if size == 0 && matches!(kind.as_str(), "f" | "i" | "u" | "c") {
                return Ok((Variant::Numbers(Vec::new()), false));
            }
        }
    }
    super::setrefdir_args::argument(value)
}

/// CASA flattens numeric arrays in column-major order before setting flux.
fn numeric(value: &Variant) -> Option<Vec<Complex64>> {
    match value {
        Variant::Array { values, .. } => {
            Some(values.iter().map(|&v| Complex64::new(v, 0.0)).collect())
        }
        Variant::ComplexArray { values, .. } => Some(values.clone()),
        _ => complex_values(value),
    }
}

/// Prepare and validate the whole flux before changing any component.
pub(super) fn prepare(
    value: &Variant,
    unit: &str,
    polarization: &str,
    error: &Variant,
) -> ConvertResult<Flux> {
    let pol = Polarisation::parse_casa(polarization).map_err(|_| {
        ConvertError::runtime(
            "Unknown polarization. Possible values are:\n 'Stokes'  'Linear'  'Circular' ",
        )
    })?;
    let target = casacore_unit(unit).map_err(ConvertError::runtime)?;
    let jy = casacore_unit("Jy").map_err(ConvertError::runtime)?;
    if !target.conformant(&jy) {
        return Err(ConvertError::runtime(
            "The flux units must have the same dimensions as the Jansky\nFlux not changed on any components",
        ));
    }
    let values = if value.is_empty_sequence() {
        vec![Complex64::new(0.0, 0.0); 4]
    } else {
        let values = numeric(value).ok_or_else(|| {
            ConvertError::runtime("Could not understand the type of flux value variable")
        })?;
        if values.is_empty() {
            vec![Complex64::new(0.0, 0.0); 4]
        } else {
            values
        }
    };
    let errors = numeric(error);
    if errors
        .as_ref()
        .is_some_and(|errors| errors.len() != values.len())
    {
        return Err(ConvertError::runtime(
            "Error array must have same length as value array",
        ));
    }
    if !matches!((values.len(), pol), (1, Polarisation::Stokes) | (4, _)) {
        return Err(ConvertError::runtime(
            "The flux must have one or four elements,\none element can only be used if the polarization is 'Stokes'.\nFlux not changed on any components",
        ));
    }
    for item in values.iter().chain(errors.iter().flatten()) {
        if !item.re.is_finite() || !item.im.is_finite() {
            return Err(ConvertError::runtime(
                "non-finite flux or error refused (#1053)",
            ));
        }
        if item.im != 0.0 {
            return Err(ConvertError::runtime(
                "imaginary flux or error would be discarded by CASA (#1053)",
            ));
        }
    }
    let mut result_values = [Complex64::new(0.0, 0.0); 4];
    result_values[..values.len()].copy_from_slice(&values);
    let mut result_errors = [Complex64::new(0.0, 0.0); 4];
    if let Some(errors) = errors {
        result_errors[..errors.len()].copy_from_slice(&errors);
    }
    // CASA's setter ultimately stores the values as Stokes, regardless of pol.
    Flux::new(result_values, unit, Polarisation::Stokes)
        .map(|flux| flux.with_errors(result_errors))
        .map_err(|error| ConvertError::runtime(error.to_string()))
}
