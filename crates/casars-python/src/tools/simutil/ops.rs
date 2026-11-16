// SPDX-License-Identifier: LGPL-3.0-or-later
//! Arguments and results of `casars.simutil.simutil`'s methods, as CASA's
//! `simutil` takes and returns them, over [`casa_simulation::simutil`].

use casa_simulation::simutil::SimutilError;
use casa_simulation::simutil::direction::{CentredDirections, Directions};
use casa_simulation::simutil::skymodel::{ModelSummary, ModelValue, SkyModelError};
use casa_types::quanta::Quantity;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::value::CasaValue;
use crate::convert::{StringList, Variant, quantity as casa_quantity};

/// A `simutil` refusal as the `Exception` CASA's `simutil.msg(...,
/// priority='error')` raises: a `RuntimeError` with its message.
pub(super) fn raised(error: SimutilError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// A quantity as `qa.quantity` reads a string or a record.
pub(super) fn quantity(value: &Variant) -> PyResult<Quantity> {
    Ok(casa_quantity(value)?)
}

/// A direction string or a list of them (CASA's `self.direction`), kept
/// apart: CASA splits a list, even of one direction, at its mean.
pub(super) fn directions(value: &Bound<'_, PyAny>) -> PyResult<Directions> {
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(Directions::One(text.to_str()?.to_string()));
    }
    let directions = value.extract::<StringList>()?.0;
    if directions.is_empty() {
        return Err(PyRuntimeError::new_err(
            "can't interpret '[]' as a direction",
        ));
    }
    Ok(Directions::List(directions))
}

/// The directions of a string (one) or a list, for the methods that take
/// a list.
pub(super) fn listed(directions: Directions) -> Vec<String> {
    match directions {
        Directions::One(direction) => vec![direction],
        Directions::List(directions) => directions,
    }
}

/// Capture and validate `calc_pointings2` sizes in CASA's input order.
/// Keep the variants so later calculations can retry unit conversion without
/// extracting Python objects or repeating their side effects.
pub(super) fn size_values(
    value: &Bound<'_, PyAny>,
) -> PyResult<(
    [Variant; 2],
    [Quantity; 2],
    casa_types::quanta::registry::UnitState,
)> {
    let registry = casa_types::quanta::registry::global_registry();
    let before = registry.unit_state();
    let one = |item: &Bound<'_, PyAny>| {
        let value = item.extract::<Variant>()?;
        let quantity = quantity(&value)?;
        Ok::<_, PyErr>((value, quantity))
    };
    let (values, sizes) = if value.is_instance_of::<PyString>() || value.is_instance_of::<PyDict>()
    {
        let size = one(value)?;
        ([size.0.clone(), size.0], [size.1.clone(), size.1])
    } else {
        let items: Vec<Bound<'_, PyAny>> = value
            .try_iter()
            .map_err(|_| {
                PyTypeError::new_err(format!(
                    "can't interpret '{}' as a CASA quantity",
                    value.repr().map_or_else(|_| "?".into(), |r| r.to_string())
                ))
            })?
            .collect::<PyResult<_>>()?;
        match items.as_slice() {
            [] => {
                return Err(PyRuntimeError::new_err(
                    "can't interpret '[]' as a CASA quantity",
                ));
            }
            [only] => {
                let size = one(only)?;
                ([size.0.clone(), size.0], [size.1.clone(), size.1])
            }
            [x, y, ..] => {
                let x = one(x)?;
                let y = one(y)?;
                ([x.0, y.0], [x.1, y.1])
            }
        }
    };
    let after = registry.unit_state();
    if before == after {
        Ok((values, sizes, after))
    } else {
        let (sizes, state) = registry.with_stable_units(|| {
            size_quantities(&values).map(|sizes| (sizes, registry.unit_state()))
        })?;
        Ok((values, sizes, state))
    }
}

/// Convert both captured sizes against the current unit definitions.
pub(super) fn size_quantities(values: &[Variant; 2]) -> PyResult<[Quantity; 2]> {
    Ok([quantity(&values[0])?, quantity(&values[1])?])
}

/// `average_direction`'s and `median_direction`'s `(direction, offsets)`:
/// the offsets a NumPy array of shape (2, n), degrees.
pub(super) fn centred(centred: CentredDirections) -> CasaValue {
    let n = centred.offsets.len();
    CasaValue::Tuple(vec![
        CasaValue::Text(centred.centre),
        CasaValue::Floats {
            shape: vec![2, n],
            values: centred.offsets.into_iter().flatten().collect(),
        },
    ])
}

/// A sky-model refusal: a missing or unwritable file as `OSError`, the rest
/// (CASA's `simutil.msg(..., priority='error')`) as `RuntimeError`.
pub(super) fn sky_model_raised(error: SkyModelError) -> PyErr {
    match error {
        SkyModelError::Io { .. } => PyOSError::new_err(error.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// `modifymodel`'s tuple: the cells and sizes as quantity records, the
/// centre frequency and width as the strings given or as records.
pub(super) fn model_summary(summary: ModelSummary) -> CasaValue {
    let quantity = |value: &Quantity| CasaValue::quantity(value.value(), value.unit().name());
    let given_or_model = |value: &ModelValue| match value {
        ModelValue::Given(text) => CasaValue::Text(text.clone()),
        ModelValue::Model(value) => quantity(value),
    };
    CasaValue::Tuple(vec![
        CasaValue::Text(summary.refdir),
        CasaValue::List(summary.cell.iter().map(quantity).collect()),
        CasaValue::List(summary.size.iter().map(quantity).collect()),
        CasaValue::Int(summary.nchan as i64),
        given_or_model(&summary.specrefval),
        CasaValue::Float(summary.specrefpix),
        given_or_model(&summary.width),
        CasaValue::Text(summary.stokes),
    ])
}

/// A list of strings.
pub(super) fn texts(values: Vec<String>) -> CasaValue {
    CasaValue::List(values.into_iter().map(CasaValue::Text).collect())
}

/// Capture a non-empty string or record once for `simutil.isquantity`.
pub(super) fn quantity_candidate(value: &Bound<'_, PyAny>) -> Option<Variant> {
    if value.len().map_or(true, |len| len == 0) {
        return None;
    }
    let variant = value.extract::<Variant>().ok()?;
    matches!(variant, Variant::Text(_) | Variant::Record(_)).then_some(variant)
}

/// Python's `str()` of each time, as `write_pointings` writes them: a
/// sequence's items, or the one value.
pub(super) fn times(time: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let shown = |item: &Bound<'_, PyAny>| item.str().map(|text| text.to_string());
    if time.is_instance_of::<PyString>() {
        return Ok(vec![shown(time)?]);
    }
    match time.try_iter() {
        Ok(items) => items.map(|item| shown(&item?)).collect(),
        Err(_) => Ok(vec![shown(time)?]),
    }
}
