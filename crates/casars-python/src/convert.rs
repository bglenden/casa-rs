// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's argument conventions, shared by every `casars.tools` class
//! (ADR-0019 item 5).
//!
//! CASA tools take three kinds of loosely typed arguments, and scripts rely on
//! how CASA's C++ layer (`CasacSupport.cc` in casatools) reads each:
//!
//! - **`any` parameters** arrive as a [`Variant`]: a string, a number, a
//!   record, `[]` (the default of every `any` parameter in the installed
//!   binding) and so on. [`quantity`] and [`quantity_value`] read quantities,
//!   and the [`measures`] module reads epochs, directions and positions from
//!   strings and from records as casacore's `MeasureHolder` writes them.
//! - **Vector parameters** ([`DoubleList`], [`StringList`]) take a sequence
//!   or a single value.
//! - Everything else is a plain Python scalar.
//!
//! Conversions report CASA's messages where CASA reports the same failure.
//! They deliberately diverge in two places, recorded in each function: they
//! refuse a value whose unit cannot be converted to the parameter's unit
//! (CASA silently scales it by the unit factors, so `integrationtime='10Jy'`
//! is 1e-25 s), and a string parameter given a `str` takes it as one element
//! (CASA's string-vector typemap replaces only the default's first element,
//! keeps the default for `[]` and ignores tuples).

pub(crate) mod measures;

use std::collections::BTreeMap;
use std::fmt;

use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};

/// The speed of light, m/s (casacore's `C::c`).
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
/// casacore's `C::circle / C::day`: radians of angle per second of time.
const RADIANS_PER_SECOND: f64 = std::f64::consts::TAU / 86_400.0;

/// A failed argument conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConvertError {
    /// CASA raises `RuntimeError` with this message.
    Runtime(String),
    /// The Python value has a type the parameter cannot take (`TypeError`).
    Type(String),
}

impl ConvertError {
    pub(crate) fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime(message.into())
    }
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) | Self::Type(message) => f.write_str(message),
        }
    }
}

impl From<ConvertError> for PyErr {
    fn from(error: ConvertError) -> Self {
        match error {
            ConvertError::Runtime(message) => PyRuntimeError::new_err(message),
            ConvertError::Type(message) => PyTypeError::new_err(message),
        }
    }
}

/// The result of a conversion.
pub(crate) type ConvertResult<T> = Result<T, ConvertError>;

/// A Python value passed to an `any` parameter (CASA's `casac::variant`),
/// captured before the call releases the GIL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Variant {
    /// `None` or an empty sequence: the installed binding's `[]` default.
    Empty,
    /// A `bool`.
    Bool(bool),
    /// An `int` or `float` (NumPy scalars included).
    Number(f64),
    /// A `str`.
    Text(String),
    /// A non-empty sequence of `str`.
    Texts(Vec<String>),
    /// A non-empty sequence of numbers.
    Numbers(Vec<f64>),
    /// A `dict` with `str` keys.
    Record(BTreeMap<String, Variant>),
    /// Anything else, kept as its `repr` for messages.
    Other(String),
}

impl Variant {
    /// The value as CASA's `variant::toString` would show it in a message.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Empty => "[]".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.to_string(),
            Self::Text(text) => text.clone(),
            Self::Texts(texts) => format!("[{}]", texts.join(", ")),
            Self::Numbers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(f64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Record(_) => "{...}".to_string(),
            Self::Other(repr) => repr.clone(),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for Variant {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        variant_from_python(&object)
    }
}

/// Captures `object` as a [`Variant`].
fn variant_from_python(object: &Bound<'_, PyAny>) -> PyResult<Variant> {
    if object.is_none() {
        return Ok(Variant::Empty);
    }
    if let Ok(flag) = object.cast::<PyBool>() {
        return Ok(Variant::Bool(flag.is_true()));
    }
    if let Ok(text) = object.cast::<PyString>() {
        return Ok(Variant::Text(text.to_str()?.to_string()));
    }
    if let Ok(dict) = object.cast::<PyDict>() {
        let mut record = BTreeMap::new();
        for (key, value) in dict.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err("a CASA record argument must have str keys"))?;
            record.insert(key, variant_from_python(&value)?);
        }
        return Ok(Variant::Record(record));
    }
    if object.is_instance_of::<PyInt>() || object.is_instance_of::<PyFloat>() {
        return Ok(Variant::Number(object.extract::<f64>()?));
    }
    if is_sequence(object)? {
        return sequence_variant(object);
    }
    if let Ok(number) = object.extract::<f64>() {
        return Ok(Variant::Number(number));
    }
    Ok(Variant::Other(object.repr()?.to_string()))
}

/// Whether `object` is a list, tuple, NumPy array or similar sized iterable
/// (but not a string, bytes or mapping).
fn is_sequence(object: &Bound<'_, PyAny>) -> PyResult<bool> {
    if object.is_instance_of::<PyString>() || object.is_instance_of::<PyDict>() {
        return Ok(false);
    }
    if object.hasattr("decode")? {
        // bytes and bytearray
        return Ok(false);
    }
    Ok(object.hasattr("__len__")? && object.hasattr("__iter__")?)
}

/// A sequence as [`Variant::Empty`], [`Variant::Texts`] or
/// [`Variant::Numbers`], or [`Variant::Other`] when it mixes kinds.
fn sequence_variant(object: &Bound<'_, PyAny>) -> PyResult<Variant> {
    let items = object.try_iter()?.collect::<PyResult<Vec<_>>>()?;
    if items.is_empty() {
        return Ok(Variant::Empty);
    }
    if let Ok(texts) = items
        .iter()
        .map(|item| item.cast::<PyString>().map(|text| text.to_string()))
        .collect::<Result<Vec<_>, _>>()
    {
        return Ok(Variant::Texts(texts));
    }
    let numbers = items
        .iter()
        .map(|item| {
            if item.is_instance_of::<PyString>() || item.is_instance_of::<PyBool>() {
                None
            } else {
                item.extract::<f64>().ok()
            }
        })
        .collect::<Option<Vec<_>>>();
    Ok(match numbers {
        Some(numbers) => Variant::Numbers(numbers),
        None => Variant::Other(object.repr()?.to_string()),
    })
}

/// A vector-of-doubles parameter: a sequence of numbers, or one number.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DoubleList(pub(crate) Vec<f64>);

impl DoubleList {
    /// The binding default `values`.
    pub(crate) fn of(values: &[f64]) -> Self {
        Self(values.to_vec())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for DoubleList {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let object = &*object;
        if object.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(format!(
                "expected a number or a sequence of numbers, got the str {}",
                object.repr()?
            )));
        }
        if is_sequence(object)? {
            return object
                .try_iter()?
                .map(|item| {
                    let item = item?;
                    if item.is_instance_of::<PyString>() {
                        return Err(PyTypeError::new_err(format!(
                            "expected a sequence of numbers, found the str {}",
                            item.repr()?
                        )));
                    }
                    item.extract::<f64>()
                })
                .collect::<PyResult<Vec<_>>>()
                .map(Self);
        }
        Ok(Self(vec![object.extract::<f64>()?]))
    }
}

/// A vector-of-strings parameter: a sequence of `str`, or one `str` as a
/// one-element list.
///
/// This deliberately does not reproduce CASA's string-vector typemap, which
/// keeps the binding default for `[]`, replaces only the default's first
/// element with a plain `str` or with a shorter list's elements, and ignores
/// tuples and NumPy arrays.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StringList(pub(crate) Vec<String>);

impl StringList {
    /// The binding default `values`.
    pub(crate) fn of(values: &[&str]) -> Self {
        Self(values.iter().map(|value| (*value).to_string()).collect())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for StringList {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let object = &*object;
        if let Ok(text) = object.cast::<PyString>() {
            return Ok(Self(vec![text.to_str()?.to_string()]));
        }
        if !is_sequence(object)? {
            return Err(PyTypeError::new_err(format!(
                "expected a str or a sequence of str, got {}",
                object.repr()?
            )));
        }
        object
            .try_iter()?
            .map(|item| {
                let item = item?;
                item.cast::<PyString>()
                    .map_err(|_| {
                        PyTypeError::new_err(format!(
                            "expected a sequence of str, found {}",
                            item.repr()
                                .map_or_else(|_| "?".to_string(), |repr| repr.to_string())
                        ))
                    })
                    .and_then(|text| Ok(text.to_str()?.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()
            .map(Self)
    }
}

/// CASA's `casaQuantity(variant)`: a string through casacore's reader, a
/// `{'value', 'unit'}` record, or a number (dimensionless). Every other
/// value, including `[]` and lists of numbers, is a dimensionless 0.
pub(crate) fn quantity(value: &Variant) -> ConvertResult<Quantity> {
    match value {
        Variant::Text(text) => read_quantity(text, " in converting quantity"),
        // CASA reads a string vector's `toString`, which never parses.
        Variant::Texts(_) => Err(quantity_read_error(
            &value.describe(),
            " in converting quantity",
        )),
        Variant::Record(record) => quantity_record(record).map_err(|error| {
            ConvertError::runtime(format!("Error {error} in converting quantity"))
        }),
        Variant::Number(number) => Ok(Quantity::dimensionless(*number)),
        Variant::Bool(flag) => Ok(Quantity::dimensionless(f64::from(u8::from(*flag)))),
        Variant::Empty | Variant::Numbers(_) | Variant::Other(_) => {
            Ok(Quantity::dimensionless(0.0))
        }
    }
}

/// CASA's `casaQuantity(variant).getValue(unit)`: the value in `unit`, where
/// a dimensionless value is in `unit`'s SI base (`integrationtime=10` is
/// 10 s, `freq=1.4e9` is Hz), angles and times convert at 360 degrees a day,
/// and lengths and frequencies convert as wavelengths.
///
/// Divergence: casacore's `getValue` does not check conformance, so CASA
/// takes `integrationtime='10Jy'` as 1e-25 s and `'10m'` (metres) as 10 s.
/// This refuses them with casacore's own conformance message.
pub(crate) fn quantity_value(value: &Variant, unit: &str) -> ConvertResult<f64> {
    value_in(&quantity(value)?, unit)
}

/// `quantity`'s value in `unit`; see [`quantity_value`].
pub(crate) fn value_in(quantity: &Quantity, unit: &str) -> ConvertResult<f64> {
    let target = Unit::new(unit)
        .map_err(|error| ConvertError::runtime(format!("invalid target unit {unit:?}: {error}")))?;
    let from = quantity.unit().val();
    let to = target.val();
    // casacore's `d1 = otherFac/myFac`, divided into the value.
    let ratio = to.factor / from.factor;
    let time = UnitDim::basis(Dimension::Time);
    let angle = UnitDim::basis(Dimension::Angle);
    let length = UnitDim::basis(Dimension::Length);
    let frequency = time.pow(-1);
    if from.dim == to.dim || from.dim.is_dimensionless() {
        Ok(quantity.value() / ratio)
    } else if from.dim == angle && to.dim == time {
        Ok(quantity.value() / (ratio * RADIANS_PER_SECOND))
    } else if from.dim == time && to.dim == angle {
        Ok(quantity.value() / (ratio / RADIANS_PER_SECOND))
    } else if (from.dim == frequency && to.dim == length)
        || (from.dim == length && to.dim == frequency)
    {
        Ok(SPEED_OF_LIGHT_M_S / quantity.value() / from.factor / to.factor)
    } else {
        Err(ConvertError::runtime(format!(
            "From/to units not consistent. Cannot convert {} to {unit}",
            quantity.unit().name()
        )))
    }
}

/// casacore's quantity reader on `text`, failing with CASA's message and
/// `context` appended.
pub(crate) fn read_quantity(text: &str, context: &str) -> ConvertResult<Quantity> {
    Quantity::read(text).map_err(|_| quantity_read_error(text, context))
}

fn quantity_read_error(text: &str, context: &str) -> ConvertError {
    ConvertError::runtime(format!(
        "Error in QuantumHolder::fromString with input string \"{text}\": \
         Illegal input units or format\n{context}"
    ))
}

/// `QuantumHolder::fromRecord` for a scalar quantity: `{'value': number,
/// 'unit': str}`. The error is casacore's wording without the context.
pub(crate) fn quantity_record(record: &BTreeMap<String, Variant>) -> Result<Quantity, String> {
    let (Some(value), Some(Variant::Text(unit))) = (record.get("value"), record.get("unit")) else {
        return Err("Illegal Quantity record in QuantumHolder::fromRecord\n".to_string());
    };
    let value = match value {
        Variant::Number(number) => *number,
        Variant::Numbers(numbers) if numbers.len() == 1 => numbers[0],
        _ => return Err("Wrong QuantumHolder to convert asQuantumDouble".to_string()),
    };
    Quantity::new(value, unit).map_err(|_| format!("Unit::check Illegal unit string '{unit}'"))
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
