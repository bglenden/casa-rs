// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's argument conventions, shared by every `casars.tools` class
//! (ADR-0017).
//!
//! CASA tools take three kinds of loosely typed arguments, and scripts rely on
//! how CASA's C++ layer (`CasacSupport.cc` in casatools) reads each:
//!
//! - **`any` parameters** arrive as a [`Variant`]: a string, a number (real
//!   or complex), a record, `[]` (the default of every `any` parameter in the
//!   installed binding) and so on. [`quantity`] and [`quantity_value`] read
//!   quantities, [`complex_values`] reads numbers as casatools' complex
//!   vectors, and the [`measures`] module reads epochs, directions,
//!   positions and frequencies from strings and from records as casacore's
//!   `MeasureHolder` writes them.
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

pub(crate) mod holder;
pub(crate) mod measures;
pub(crate) mod quantum;
pub(crate) mod reply_value;
pub(crate) mod value;

use std::collections::BTreeMap;
use std::fmt;

use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};
use num_complex::Complex64;
use numpy::{PyReadonlyArrayDyn, PyUntypedArrayMethods};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

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

/// The original scalar kind of a record's `value`, retained for CASA's
/// QuantumHolder conversion while leaving the record's ordinary values intact.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RecordScalarKind {
    NativeInteger(i64),
    IntegerVector(Option<i32>),
    EmptyFloatVector,
    NumpyUnsupported,
}

/// A record and optional information erased by the numeric variant conversion.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VariantRecord {
    fields: BTreeMap<String, Variant>,
    scalar_kind: Option<RecordScalarKind>,
}

impl From<BTreeMap<String, Variant>> for VariantRecord {
    fn from(fields: BTreeMap<String, Variant>) -> Self {
        Self {
            fields,
            scalar_kind: None,
        }
    }
}

impl std::ops::Deref for VariantRecord {
    type Target = BTreeMap<String, Variant>;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

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
    /// A `complex` (NumPy's `complex128` is one): casatools' `COMPLEX`.
    Complex(Complex64),
    /// A non-empty sequence of numbers with a `complex` among them, such as
    /// a one-dimensional `complex128` array: casatools' `COMPLEXVEC`.
    Complexes(Vec<Complex64>),
    /// A NumPy array of two or more dimensions, its values in Fortran
    /// (column-major) order, as casatools' `variant` holds them.
    Array { shape: Vec<usize>, values: Vec<f64> },
    /// A complex NumPy array, retaining its shape and Fortran-order values.
    ComplexArray {
        shape: Vec<usize>,
        values: Vec<Complex64>,
    },
    /// A `dict` with `str` keys.
    Record(VariantRecord),
    /// Anything else, kept as its `repr` for messages.
    Other(String),
}

impl Variant {
    /// Whether this has the empty-vector behavior of CASA's variant.
    pub(crate) fn is_empty_sequence(&self) -> bool {
        matches!(self, Self::Empty)
            || matches!(self, Self::ComplexArray { values, .. } if values.is_empty())
    }

    /// The value as CASA's `variant::toString` would show it in a message.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Empty => "[]".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.to_string(),
            Self::Text(text) => text.clone(),
            Self::Texts(texts) => format!(
                "[{}]",
                texts
                    .iter()
                    .map(|text| format!("\"{text}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Numbers(values) | Self::Array { values, .. } => format!(
                "[{}]",
                values
                    .iter()
                    .map(f64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            // C++'s `std::complex` output.
            Self::Complex(value) => format!("({},{})", value.re, value.im),
            Self::Complexes(values) | Self::ComplexArray { values, .. } => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| format!("({},{})", value.re, value.im))
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
        // CASA does not accept a NumPy complex64 scalar in a quantity
        // record. Keep that diagnostic separate from the preserved value.
        let complex64_scalar = match dict.get_item("value")? {
            Some(value) => is_numpy_complex64_scalar(&value)?,
            None => false,
        };
        let has_unit = dict.get_item("unit")?.is_some();
        let mut record = BTreeMap::new();
        for (key, value) in dict.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err("a CASA record argument must have str keys"))?;
            let converted = if key == "value" && has_unit {
                match zero_dim_record_float(&value)? {
                    Some(array) => array,
                    None => variant_from_python(&value)?,
                }
            } else {
                variant_from_python(&value)?
            };
            record.insert(key, converted);
        }
        let scalar_kind = if complex64_scalar {
            Some(RecordScalarKind::NumpyUnsupported)
        } else {
            match dict.get_item("value")? {
                Some(value)
                    if value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>() =>
                {
                    value
                        .extract::<i64>()
                        .ok()
                        .map(RecordScalarKind::NativeInteger)
                }
                Some(value)
                    if !value.is_instance_of::<PyFloat>()
                        && !value.is_instance_of::<PyComplex>()
                        && matches!(
                            value
                                .getattr("dtype")
                                .and_then(|d| d.getattr("kind"))
                                .and_then(|k| k.extract::<String>())
                                .as_deref(),
                            Ok("i" | "u" | "f" | "b")
                        ) =>
                {
                    let scalar = value.py().import("numpy")?.getattr("generic")?;
                    if value.is_instance(&scalar)? {
                        Some(RecordScalarKind::NumpyUnsupported)
                    } else {
                        record_vector_kind(&value)?
                    }
                }
                Some(value) => record_vector_kind(&value)?,
                None => None,
            }
        };
        return Ok(Variant::Record(VariantRecord {
            fields: record,
            scalar_kind,
        }));
    }
    if let Ok(value) = object.cast::<PyComplex>() {
        return Ok(Variant::Complex(complex_of(value)));
    }
    if object.is_instance_of::<PyInt>() || object.is_instance_of::<PyFloat>() {
        return Ok(Variant::Number(object.extract::<f64>()?));
    }
    if let Some(array) = numeric_array(object)? {
        return Ok(array);
    }
    if is_sequence(object)? {
        return sequence_variant(object);
    }
    if is_numpy_complex64_scalar(object)? {
        let re = object.getattr("real")?.extract::<f64>()?;
        let im = object.getattr("imag")?.extract::<f64>()?;
        return Ok(Variant::Complex(Complex64::new(re, im)));
    }
    if let Ok(number) = object.extract::<f64>() {
        return Ok(Variant::Number(number));
    }
    Ok(Variant::Other(object.repr()?.to_string()))
}

/// CASA rejects NumPy's complex64 scalar in quantity records, unlike Python
/// complex and NumPy complex128. Do not classify zero-dimensional arrays.
fn is_numpy_complex64_scalar(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let Ok(dtype) = value.getattr("dtype") else {
        return Ok(false);
    };
    if !matches!(
        dtype
            .getattr("kind")
            .and_then(|k| k.extract::<String>())
            .as_deref(),
        Ok("c")
    ) || dtype
        .getattr("itemsize")
        .and_then(|n| n.extract::<usize>())
        .ok()
        != Some(8)
    {
        return Ok(false);
    }
    value.is_instance(&value.py().import("numpy")?.getattr("generic")?)
}

/// Keep the integer kind and low 32 bits of one-dimensional record arrays.
fn record_vector_kind(value: &Bound<'_, PyAny>) -> PyResult<Option<RecordScalarKind>> {
    let array_kind = value
        .getattr("dtype")
        .and_then(|d| d.getattr("kind"))
        .and_then(|k| k.extract::<String>());
    if matches!(array_kind.as_deref(), Ok("f"))
        && matches!(
            value
                .getattr("dtype")
                .and_then(|d| d.getattr("itemsize"))
                .and_then(|n| n.extract::<usize>()),
            Ok(4 | 8)
        )
        && value
            .getattr("ndim")
            .and_then(|n| n.extract::<usize>())
            .ok()
            == Some(1)
        && value.is_instance(&value.py().import("numpy")?.getattr("ndarray")?)?
        && value.len()? == 0
    {
        return Ok(Some(RecordScalarKind::EmptyFloatVector));
    }
    let is_array = matches!(array_kind.as_deref(), Ok("i" | "u" | "b"))
        && value
            .getattr("ndim")
            .and_then(|n| n.extract::<usize>())
            .ok()
            == Some(1)
        && value.is_instance(&value.py().import("numpy")?.getattr("ndarray")?)?;
    let values = if is_array {
        if matches!(array_kind.as_deref(), Ok("b")) {
            return Ok(Some(RecordScalarKind::NumpyUnsupported));
        }
        value.call_method0("tolist")?
    } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        value.clone()
    } else {
        return Ok(None);
    };
    let values = values.try_iter()?.collect::<PyResult<Vec<_>>>()?;
    if values.is_empty() {
        return Ok(is_array.then_some(RecordScalarKind::IntegerVector(None)));
    }
    if values.iter().all(|v| v.is_instance_of::<PyBool>()) {
        return Ok(Some(RecordScalarKind::NumpyUnsupported));
    }
    let native_integer =
        |v: &Bound<'_, PyAny>| v.is_instance_of::<PyInt>() && !v.is_instance_of::<PyBool>();
    if !values.iter().all(native_integer) {
        let signed_integer = value.py().import("numpy")?.getattr("signedinteger")?;
        for item in &values {
            if !native_integer(item) && !item.is_instance(&signed_integer)? {
                return Ok(None);
            }
        }
    }
    if values.len() > 1 {
        return Ok(Some(RecordScalarKind::IntegerVector(None)));
    }
    let first = &values[0];
    let narrowed = first.extract::<i64>().map(|n| n as i32).ok().or_else(|| {
        is_array
            .then(|| first.extract::<u64>().ok().map(|n| n as i32))
            .flatten()
    });
    Ok(narrowed.map(|value| RecordScalarKind::IntegerVector(Some(value))))
}

/// Preserve a zero-dimensional floating array inside a quantity-like record.
/// Bare arguments keep their existing conversion until their callers can
/// handle arrays without discarding their values.
fn zero_dim_record_float(object: &Bound<'_, PyAny>) -> PyResult<Option<Variant>> {
    if object.is_instance_of::<PyFloat>()
        || object.is_instance_of::<PyInt>()
        || object.is_instance_of::<PyString>()
        || object.is_instance_of::<PyComplex>()
        || object
            .getattr("ndim")
            .and_then(|n| n.extract::<usize>())
            .ok()
            != Some(0)
    {
        return Ok(None);
    }
    let is_float = matches!(
        object
            .getattr("dtype")
            .and_then(|dtype| dtype.getattr("kind"))
            .and_then(|kind| kind.extract::<String>())
            .as_deref(),
        Ok("f")
    );
    if !is_float
        || !matches!(
            object
                .getattr("dtype")
                .and_then(|dtype| dtype.getattr("itemsize"))
                .and_then(|size| size.extract::<usize>()),
            Ok(4 | 8)
        )
        || !object.is_instance(&object.py().import("numpy")?.getattr("ndarray")?)?
    {
        return Ok(None);
    }
    let doubles = object.call_method1("astype", ("float64",))?;
    let array = doubles.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
    Ok(Some(Variant::Array {
        shape: array.shape().to_vec(),
        values: array.as_array().iter().copied().collect(),
    }))
}

/// A numeric NumPy array with a retained shape. Complex arrays also handle
/// zero and one dimensions without discarding their imaginary parts.
fn numeric_array(object: &Bound<'_, PyAny>) -> PyResult<Option<Variant>> {
    let Ok(ndim) = object
        .getattr("ndim")
        .and_then(|ndim| ndim.extract::<usize>())
    else {
        return Ok(None);
    };
    if !object.hasattr("astype")? {
        return Ok(None);
    }
    let kind = object
        .getattr("dtype")
        .and_then(|dtype| dtype.getattr("kind"))
        .and_then(|kind| kind.extract::<String>());
    if matches!(kind.as_deref(), Ok("c"))
        && object.is_instance(&object.py().import("numpy")?.getattr("ndarray")?)?
    {
        let complexes = object.call_method1("astype", ("complex128",))?;
        let array = complexes.extract::<PyReadonlyArrayDyn<'_, Complex64>>()?;
        let shape = array.shape().to_vec();
        let values = array.as_array().t().iter().copied().collect();
        return Ok(Some(Variant::ComplexArray { shape, values }));
    }
    if ndim < 2 {
        return Ok(None);
    }
    if !matches!(kind.as_deref(), Ok("f" | "i" | "u")) {
        return Ok(None);
    }
    let doubles = object.call_method1("astype", ("float64",))?;
    let array = doubles.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
    let shape = array.shape().to_vec();
    // Fortran order of the array is C order of its transpose.
    let values = array.as_array().t().iter().copied().collect();
    Ok(Some(Variant::Array { shape, values }))
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

/// A sequence as [`Variant::Empty`], [`Variant::Texts`],
/// [`Variant::Numbers`] or (with a `complex` among the numbers)
/// [`Variant::Complexes`], or [`Variant::Other`] when it mixes kinds.
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
    let number = |item: &Bound<'_, PyAny>| {
        if item.is_instance_of::<PyString>() || item.is_instance_of::<PyBool>() {
            None
        } else {
            item.extract::<f64>().ok()
        }
    };
    let variant = if items.iter().any(|item| item.is_instance_of::<PyComplex>()) {
        items
            .iter()
            .map(|item| match item.cast::<PyComplex>() {
                Ok(value) => Some(complex_of(value)),
                Err(_) => number(item).map(|re| Complex64::new(re, 0.0)),
            })
            .collect::<Option<Vec<_>>>()
            .map(Variant::Complexes)
    } else {
        items
            .iter()
            .map(number)
            .collect::<Option<Vec<_>>>()
            .map(Variant::Numbers)
    };
    match variant {
        Some(variant) => Ok(variant),
        None => Ok(Variant::Other(object.repr()?.to_string())),
    }
}

/// A Python `complex` as a [`Complex64`].
fn complex_of(value: &Bound<'_, PyComplex>) -> Complex64 {
    Complex64::new(value.real(), value.imag())
}

/// A record parameter (casatools' `record`): a `dict`. Anything else is the
/// `TypeError` CASA's binding raises.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RecordArg(pub(crate) BTreeMap<String, Variant>);

impl RecordArg {
    /// The empty record, the binding default `{}`.
    pub(crate) fn empty() -> Self {
        Self(BTreeMap::new())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for RecordArg {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if !object.is_instance_of::<PyDict>() {
            return Err(PyTypeError::new_err("is not a dictionary"));
        }
        match variant_from_python(&object)? {
            Variant::Record(record) => Ok(Self(record.fields)),
            _ => unreachable!("a dict reads as a record"),
        }
    }
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

/// A vector-of-ints parameter: a sequence of integers, or one integer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IntList(pub(crate) Vec<i64>);

impl IntList {
    /// The binding default `values`.
    pub(crate) fn of(values: &[i64]) -> Self {
        Self(values.to_vec())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for IntList {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let object = &*object;
        if object.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(format!(
                "expected an int or a sequence of ints, got the str {}",
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
                            "expected a sequence of ints, found the str {}",
                            item.repr()?
                        )));
                    }
                    item.extract::<i64>()
                })
                .collect::<PyResult<Vec<_>>>()
                .map(Self);
        }
        Ok(Self(vec![object.extract::<i64>()?]))
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
/// `{'value', 'unit'}` record, or a number (dimensionless). A complex number
/// raises, as casacore cannot take a complex quantity as one double. Every
/// other value, including `[]` and lists of numbers, is a dimensionless 0.
pub(crate) fn quantity(value: &Variant) -> ConvertResult<Quantity> {
    match value {
        Variant::Text(text) => read_quantity(text, " in converting quantity"),
        // CASA reads a string vector's `toString`, which never parses.
        Variant::Texts(_) => Err(quantity_read_error(
            &value.describe(),
            " in converting quantity",
        )),
        // A record of several values reads, then fails to be one quantity.
        Variant::Record(record) => quantity_variant_record(record).map_err(|error| {
            if error.starts_with("Wrong") || error.starts_with("Unit::check Illegal unit string ") {
                ConvertError::runtime(error)
            } else {
                ConvertError::runtime(format!("Error {error} in converting quantity"))
            }
        }),
        Variant::Number(number) => Ok(Quantity::dimensionless(*number)),
        Variant::Bool(flag) => Ok(Quantity::dimensionless(f64::from(u8::from(*flag)))),
        Variant::Complex(_) | Variant::ComplexArray { .. } => Err(ConvertError::runtime(
            "Wrong QuantumHolder to convert asQuantumDouble",
        )),
        Variant::Empty
        | Variant::Numbers(_)
        | Variant::Complexes(_)
        | Variant::Array { .. }
        | Variant::Other(_) => Ok(Quantity::dimensionless(0.0)),
    }
}

/// casatools' `variant::toComplexVec` for a numeric value: a number or a
/// complex number as one value, a sequence of either as its values, with
/// the numbers' imaginary parts zero; `None` for any other value.
pub(crate) fn complex_values(value: &Variant) -> Option<Vec<Complex64>> {
    let real = |value: f64| Complex64::new(value, 0.0);
    match value {
        Variant::Number(number) => Some(vec![real(*number)]),
        Variant::Numbers(numbers) => Some(numbers.iter().copied().map(real).collect()),
        Variant::Complex(value) => Some(vec![*value]),
        Variant::Complexes(values) => Some(values.clone()),
        Variant::ComplexArray { shape, values } if shape.len() <= 1 => Some(values.clone()),
        _ => None,
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

/// A Python quantity record, preserving CASA's scalar integer distinctions.
pub(crate) fn quantity_variant_record(record: &VariantRecord) -> Result<Quantity, String> {
    let Some(Variant::Text(unit)) = record.get("unit") else {
        return Err("Illegal Quantum record in QuantumHolder::fromRecord\n".to_string());
    };
    match record.scalar_kind {
        Some(RecordScalarKind::NativeInteger(value)) => Ok(Quantity::with_unit(
            f64::from(value as i32),
            casacore_unit(unit)?,
        )),
        Some(RecordScalarKind::IntegerVector(Some(value))) => {
            Ok(Quantity::with_unit(f64::from(value), casacore_unit(unit)?))
        }
        Some(RecordScalarKind::IntegerVector(None) | RecordScalarKind::EmptyFloatVector) => {
            casacore_unit(unit)?;
            Err("Wrong QuantumHolder to convert asQuantumDouble".to_string())
        }
        Some(RecordScalarKind::NumpyUnsupported) => {
            Err("Illegal Quantum record in QuantumHolder::fromRecord\n".to_string())
        }
        None => quantity_record(&record.fields),
    }
}

/// `QuantumHolder::fromRecord` for a scalar quantity: `{'value': number,
/// 'unit': str}`. The error is casacore's wording without the context.
pub(crate) fn quantity_record(record: &BTreeMap<String, Variant>) -> Result<Quantity, String> {
    let (Some(value), Some(Variant::Text(unit))) = (record.get("value"), record.get("unit")) else {
        return Err("Illegal Quantum record in QuantumHolder::fromRecord\n".to_string());
    };
    let value = match value {
        Variant::Number(number) => *number,
        Variant::Numbers(numbers) if numbers.len() == 1 => numbers[0],
        // Numeric arrays and complex values are valid QuantumHolders, so
        // CASA checks their unit before a scalar operation refuses them.
        Variant::Numbers(_)
        | Variant::Array { .. }
        | Variant::Complex(_)
        | Variant::Complexes(_)
        | Variant::ComplexArray { .. } => {
            casacore_unit(unit)?;
            return Err("Wrong QuantumHolder to convert asQuantumDouble".to_string());
        }
        Variant::Other(_) => {
            return Err("Wrong QuantumHolder to convert asQuantumDouble".to_string());
        }
        _ => return Err("Illegal Quantum record in QuantumHolder::fromRecord\n".to_string()),
    };
    Ok(Quantity::with_unit(value, casacore_unit(unit)?))
}

/// casacore's `Unit(name)`: the name normalised as `Unit::check` normalises
/// it, failing with casacore's message for a name it cannot read.
pub(crate) fn casacore_unit(name: &str) -> Result<Unit, String> {
    casa_types::quanta::casacore::casacore_unit(name).map_err(|error| error.0)
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
