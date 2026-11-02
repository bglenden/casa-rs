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

/// `calc_pointings2`'s `size`: one quantity (a string or a record) for
/// both axes, or a sequence of them, the first two used (one repeated).
pub(super) fn sizes(value: &Bound<'_, PyAny>) -> PyResult<[Quantity; 2]> {
    let one = |item: &Bound<'_, PyAny>| quantity(&item.extract::<Variant>()?);
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyDict>() {
        let size = one(value)?;
        return Ok([size.clone(), size]);
    }
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
        [] => Err(PyRuntimeError::new_err(
            "can't interpret '[]' as a CASA quantity",
        )),
        [only] => {
            let size = one(only)?;
            Ok([size.clone(), size])
        }
        [x, y, ..] => Ok([one(x)?, one(y)?]),
    }
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

/// Whether a value is a quantity as `simutil.isquantity` tests one: a
/// non-empty string or record that casacore reads.
pub(super) fn is_quantity(value: &Bound<'_, PyAny>) -> bool {
    if value.len().map_or(true, |len| len == 0) {
        return false;
    }
    value.extract::<Variant>().is_ok_and(|variant| {
        matches!(variant, Variant::Text(_) | Variant::Record(_)) && quantity(&variant).is_ok()
    })
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
