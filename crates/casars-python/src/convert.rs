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

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::{CString, c_char};
use std::fmt;
use std::ptr;

use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};
use num_complex::Complex64;
use numpy::{PyReadonlyArrayDyn, PyUntypedArrayMethods};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

pub(crate) mod integer_vector;

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
    /// A native Python integer outside the signed 64-bit input range.
    IntegerOverflow,
}

impl ConvertError {
    pub(crate) fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime(message.into())
    }

    fn in_converting_quantity(self) -> Self {
        match self {
            Self::Runtime(message)
                if !message.starts_with("Wrong")
                    && !message.starts_with("Unit::check Illegal unit string ") =>
            {
                Self::Runtime(format!("Error {message} in converting quantity"))
            }
            error => error,
        }
    }
}

const INTEGER_OVERFLOW: &str = "Python int outside the signed 64-bit range in a quantity record";

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) | Self::Type(message) => f.write_str(message),
            Self::IntegerOverflow => f.write_str(INTEGER_OVERFLOW),
        }
    }
}

impl From<ConvertError> for PyErr {
    fn from(error: ConvertError) -> Self {
        match error {
            ConvertError::Runtime(message) => PyRuntimeError::new_err(message),
            ConvertError::Type(message) => PyTypeError::new_err(message),
            ConvertError::IntegerOverflow => PyOverflowError::new_err(INTEGER_OVERFLOW),
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
    IntegerOverflow,
    SingletonIntegerVector(i32),
    NonScalarNumeric,
    EmptyFloatVector,
    NumpyUnsupported,
    MalformedSequence,
    MixedBoolFloat { shape: Vec<usize>, values: Vec<f64> },
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

impl VariantRecord {
    /// Whether a captured quantity vector has CASA's floating numeric type.
    /// MeasureHolder accepts floating vectors, but not integer QuantumHolders.
    pub(crate) fn is_double_vector(&self) -> bool {
        matches!(
            self.scalar_kind,
            None | Some(
                RecordScalarKind::IntegerOverflow
                    | RecordScalarKind::MalformedSequence
                    | RecordScalarKind::MixedBoolFloat { .. }
            )
        )
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

thread_local! {
    static CAPTURE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Bound recursive Python capture, including cycles, before entering native work.
struct CaptureDepth;
impl CaptureDepth {
    fn enter() -> PyResult<Self> {
        CAPTURE_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= 128 {
                return Err(PyValueError::new_err(
                    "cyclic or too deeply nested CASA value (#1252)",
                ));
            }
            depth.set(current + 1);
            Ok(Self)
        })
    }
}
impl Drop for CaptureDepth {
    fn drop(&mut self) {
        CAPTURE_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// Captures `object` as a [`Variant`].
fn variant_from_python(object: &Bound<'_, PyAny>) -> PyResult<Variant> {
    let _capture = CaptureDepth::enter()?;
    if object.is_none() {
        return Ok(Variant::Empty);
    }
    if let Ok(flag) = object.cast::<PyBool>() {
        return Ok(Variant::Bool(flag.is_true()));
    }
    if let Ok(text) = object.cast::<PyString>() {
        return Ok(Variant::Text(text.to_str()?.to_string()));
    }
    if let Ok(bytes) = object.cast::<PyBytes>() {
        let text: String = bytes.call_method1("decode", ("utf-8",))?.extract()?;
        return Ok(Variant::Text(
            text.split('\0').next().unwrap_or_default().to_owned(),
        ));
    }
    if let Ok(dict) = object.cast::<PyDict>() {
        let has_unit = dict.get_item("unit")?.is_some();
        let mut record = BTreeMap::new();
        let mut scalar_kind = None;
        for (key, value) in dict.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err("a CASA record argument must have str keys"))?;
            if key == "value" {
                // Classify oversized integers before formatting a generic variant.
                scalar_kind = record_value_kind(&value, has_unit)?;
            }
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

fn record_value_kind(
    value: &Bound<'_, PyAny>,
    has_unit: bool,
) -> PyResult<Option<RecordScalarKind>> {
    if value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>() {
        return match value.extract::<i64>() {
            Ok(number) => Ok(Some(RecordScalarKind::NativeInteger(number))),
            Err(_) if has_unit => overflow_integer_kind(value).map(Some),
            Err(_) => Ok(None),
        };
    }
    if is_numpy_complex64_scalar(value)? {
        return Ok(Some(RecordScalarKind::NumpyUnsupported));
    }
    if !value.is_instance_of::<PyFloat>()
        && !value.is_instance_of::<PyComplex>()
        && matches!(
            value
                .getattr("dtype")
                .and_then(|d| d.getattr("kind"))
                .and_then(|k| k.extract::<String>())
                .as_deref(),
            Ok("i" | "u" | "f" | "b")
        )
        && value.is_instance(&value.py().import("numpy")?.getattr("generic")?)?
    {
        return Ok(Some(RecordScalarKind::NumpyUnsupported));
    }
    record_vector_kind(value, has_unit)
}

fn overflow_integer_kind(value: &Bound<'_, PyAny>) -> PyResult<RecordScalarKind> {
    // Keep the generic numeric value when representable. Otherwise report
    // overflow now, before the generic sequence reader falls back to repr().
    value
        .extract::<f64>()
        .map_err(|_| ConvertError::IntegerOverflow)?;
    Ok(RecordScalarKind::IntegerOverflow)
}

/// CASA rejects NumPy's complex64 scalar in quantity records, unlike Python
/// complex and NumPy complex128. Do not classify zero-dimensional arrays.
fn is_numpy_complex64_scalar(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyFloat>()
        || value.is_instance_of::<PyInt>()
        || value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyList>()
        || value.is_instance_of::<PyTuple>()
        || value.is_instance_of::<PyDict>()
        || value.is_instance_of::<PyComplex>()
        || value.is_none()
    {
        return Ok(false);
    }
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

/// Keep the integer kind and low 32 bits of record arrays and sequences.
fn record_vector_kind(
    value: &Bound<'_, PyAny>,
    has_unit: bool,
) -> PyResult<Option<RecordScalarKind>> {
    if value.is_instance_of::<PyFloat>()
        || value.is_instance_of::<PyInt>()
        || value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyDict>()
        || value.is_instance_of::<PyComplex>()
        || value.is_none()
    {
        return Ok(None);
    }
    if let Ok(array) = value.extract::<PyReadonlyArrayDyn<'_, f64>>() {
        if array.ndim() == 1 && array.is_empty() {
            return Ok(Some(RecordScalarKind::EmptyFloatVector));
        }
        return Ok(None);
    }
    let (is_array, array_kind) =
        if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            (false, Err(PyTypeError::new_err("not an array")))
        } else {
            let array_kind = value
                .getattr("dtype")
                .and_then(|d| d.getattr("kind"))
                .and_then(|k| k.extract::<String>());
            if matches!(array_kind.as_deref(), Ok("f"))
                && value.is_instance(&value.py().import("numpy")?.getattr("ndarray")?)?
            {
                let itemsize = value
                    .getattr("dtype")
                    .and_then(|d| d.getattr("itemsize"))
                    .and_then(|n| n.extract::<usize>())
                    .ok();
                if !matches!(itemsize, Some(4 | 8)) {
                    return Ok(Some(RecordScalarKind::NumpyUnsupported));
                }
                if value
                    .getattr("ndim")
                    .and_then(|n| n.extract::<usize>())
                    .ok()
                    == Some(1)
                    && value.len()? == 0
                {
                    return Ok(Some(RecordScalarKind::EmptyFloatVector));
                }
                return Ok(None);
            }
            let is_array = matches!(array_kind.as_deref(), Ok("i" | "u" | "b"))
                && value
                    .getattr("ndim")
                    .and_then(|n| n.extract::<usize>())
                    .ok()
                    .is_some_and(|n| n >= 1)
                && value.is_instance(&value.py().import("numpy")?.getattr("ndarray")?)?;
            (is_array, array_kind)
        };
    let (shape, values) = if is_array {
        if matches!(array_kind.as_deref(), Ok("b")) {
            return Ok(Some(RecordScalarKind::NumpyUnsupported));
        }
        let values = value
            .call_method0("ravel")?
            .call_method0("tolist")?
            .try_iter()?
            .collect::<PyResult<Vec<_>>>()?;
        (None, values)
    } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        let items = value.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if items
            .first()
            .is_some_and(|item| item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>())
        {
            match nested_sequence_items(value)? {
                Some((shape, flat)) => (Some(shape), flat),
                None => return Ok(None),
            }
        } else {
            (None, items)
        }
    } else {
        return Ok(None);
    };
    if values.is_empty() {
        return Ok(is_array.then_some(RecordScalarKind::NonScalarNumeric));
    }
    if values.iter().all(|v| v.is_instance_of::<PyBool>()) {
        return Ok(Some(RecordScalarKind::NumpyUnsupported));
    }
    let native_integer =
        |v: &Bound<'_, PyAny>| v.is_instance_of::<PyInt>() && !v.is_instance_of::<PyBool>();
    if !values.iter().all(native_integer) {
        let numpy = value.py().import("numpy")?;
        let signed_integer = numpy.getattr("signedinteger")?;
        if has_unit
            && !is_array
            && let Some(kind) = mixed_sequence_kind(shape, &values, &numpy)?
        {
            return Ok(Some(kind));
        }
        for item in &values {
            if !native_integer(item) && !item.is_instance(&signed_integer)? {
                return Ok(None);
            }
        }
    }
    if values.len() > 1 {
        return Ok(Some(RecordScalarKind::NonScalarNumeric));
    }
    let first = &values[0];
    let narrowed = first.extract::<i64>().map(|n| n as i32).ok().or_else(|| {
        is_array
            .then(|| first.extract::<u64>().ok().map(|n| n as i32))
            .flatten()
    });
    match narrowed {
        Some(value) => Ok(Some(RecordScalarKind::SingletonIntegerVector(value))),
        None if has_unit && native_integer(first) => overflow_integer_kind(first).map(Some),
        None => Ok(None),
    }
}

fn mixed_sequence_kind(
    shape: Option<Vec<usize>>,
    items: &[Bound<'_, PyAny>],
    numpy: &Bound<'_, PyModule>,
) -> PyResult<Option<RecordScalarKind>> {
    let needs_numpy_type_check = |item: &&Bound<'_, PyAny>| {
        !item.is_instance_of::<PyInt>()
            && !item.is_instance_of::<PyFloat>()
            && !item.is_instance_of::<PyComplex>()
    };
    let mut foreign = items.iter().filter(needs_numpy_type_check).peekable();
    if foreign.peek().is_some() {
        let unsigned_integer = numpy.getattr("unsignedinteger")?;
        let boolean = numpy.getattr("bool_")?;
        for item in foreign {
            // CASA's binding silently drops these scalars from mixed sequences.
            if item.is_instance(&unsigned_integer)? || item.is_instance(&boolean)? {
                return Ok(Some(RecordScalarKind::MalformedSequence));
            }
        }
    }
    if items.iter().any(|item| item.is_instance_of::<PyBool>()) {
        return mixed_bool_numeric_kind(shape, items, numpy).map(Some);
    }
    Ok(None)
}

fn mixed_bool_numeric_kind(
    shape: Option<Vec<usize>>,
    items: &[Bound<'_, PyAny>],
    numpy: &Bound<'_, PyModule>,
) -> PyResult<RecordScalarKind> {
    // CASA accepts the numpy.int* types here. NumPy's signedinteger class
    // also includes timedelta64 and longlong, which CASA silently drops.
    let signed_integers = PyTuple::new(
        numpy.py(),
        [
            numpy.getattr("int8")?,
            numpy.getattr("int16")?,
            numpy.getattr("int32")?,
            numpy.getattr("int64")?,
        ],
    )?;
    let floating = numpy.getattr("floating")?;
    let longdouble = numpy.getattr("longdouble")?;
    let mut has_float = false;
    let mut has_complex = false;
    for item in items {
        if item.is_instance_of::<PyInt>() || item.is_instance(&signed_integers)? {
            continue;
        }
        if item.is_instance_of::<PyFloat>()
            || (item.is_instance(&floating)? && !item.is_instance(&longdouble)?)
        {
            has_float = true;
        } else if item.is_instance_of::<PyComplex>() {
            has_complex = true;
        } else {
            return Ok(RecordScalarKind::NumpyUnsupported);
        }
    }
    if has_float && !has_complex {
        if let Some(integer) = items.iter().find(|item| {
            item.is_instance_of::<PyInt>()
                && !item.is_instance_of::<PyBool>()
                && item.extract::<i64>().is_err()
        }) {
            return overflow_integer_kind(integer);
        }
        let values = items
            .iter()
            .map(|item| item.extract::<f64>())
            .collect::<PyResult<Vec<_>>>()?;
        let shape = shape.unwrap_or_else(|| vec![values.len()]);
        Ok(RecordScalarKind::MixedBoolFloat { shape, values })
    } else {
        Ok(RecordScalarKind::NonScalarNumeric)
    }
}

/// Preserve a zero-dimensional floating array inside a quantity-like record.
/// Bare arguments keep their existing conversion until their callers can
/// handle arrays without discarding their values.
fn zero_dim_record_float(object: &Bound<'_, PyAny>) -> PyResult<Option<Variant>> {
    if object.is_instance_of::<PyFloat>()
        || object.is_instance_of::<PyInt>()
        || object.is_instance_of::<PyString>()
        || object.is_instance_of::<PyComplex>()
        || object.is_instance_of::<PyList>()
        || object.is_instance_of::<PyTuple>()
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
    if object.is_instance_of::<PyList>() || object.is_instance_of::<PyTuple>() {
        return Ok(None);
    }
    if let Ok(array) = object.extract::<PyReadonlyArrayDyn<'_, f64>>() {
        if array.ndim() == 1 && !array.is_empty() {
            let values = array.as_slice().map_or_else(
                |_| array.as_array().iter().copied().collect(),
                |s| s.to_vec(),
            );
            return Ok(Some(Variant::Numbers(values)));
        }
        if array.ndim() >= 2 {
            let shape = array.shape().to_vec();
            let values = array.as_array().t().iter().copied().collect();
            return Ok(Some(Variant::Array { shape, values }));
        }
    }
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

type NestedItems<'py> = (Vec<usize>, Vec<Bound<'py, PyAny>>);

/// Flatten a nested Python `list` or `tuple` of two or more dimensions into
/// `(shape, flat_items_in_fortran_order)`. Returns `None` if any list/tuple
/// is empty or nesting depth is inconsistent.
pub(crate) fn nested_sequence_items<'py>(
    object: &Bound<'py, PyAny>,
) -> PyResult<Option<NestedItems<'py>>> {
    fn collect<'py>(seq: &Bound<'py, PyAny>, depth: usize) -> PyResult<Option<NestedItems<'py>>> {
        if depth >= 128 {
            return Err(PyValueError::new_err(
                "cyclic or too deeply nested CASA sequence (#1252)",
            ));
        }
        if !seq.is_instance_of::<PyList>() && !seq.is_instance_of::<PyTuple>() {
            return Ok(None);
        }
        let items = seq.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if items.is_empty() {
            return Ok(None);
        }
        if !items
            .iter()
            .any(|item| item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>())
        {
            return Ok(Some((vec![items.len()], items)));
        }
        let mut sub_results = Vec::with_capacity(items.len());
        for item in &items {
            let Some(sub) = collect(item, depth + 1)? else {
                return Ok(None);
            };
            sub_results.push(sub);
        }
        let sub_shape = sub_results[0].0.clone();
        if sub_results.iter().any(|(shape, _)| *shape != sub_shape) {
            return Ok(None);
        }
        let sub_len = sub_results[0].1.len();
        let mut flat = Vec::with_capacity(items.len() * sub_len);
        for idx in 0..sub_len {
            for (_, sub_items) in &sub_results {
                flat.push(sub_items[idx].clone());
            }
        }
        let mut shape = Vec::with_capacity(1 + sub_shape.len());
        shape.push(items.len());
        shape.extend(sub_shape);
        Ok(Some((shape, flat)))
    }
    let Some((shape, items)) = collect(object, 0)? else {
        return Ok(None);
    };
    Ok((shape.len() >= 2).then_some((shape, items)))
}

/// A sequence as [`Variant::Empty`], [`Variant::Texts`],
/// [`Variant::Numbers`], [`Variant::Array`] or (with a `complex` among the
/// numbers) [`Variant::Complexes`] / [`Variant::ComplexArray`], or
/// [`Variant::Other`] when it mixes kinds.
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
    if items[0].is_instance_of::<PyList>() || items[0].is_instance_of::<PyTuple>() {
        if let Some((shape, flat)) = nested_sequence_items(object)? {
            let variant = if flat.iter().any(|item| item.is_instance_of::<PyComplex>()) {
                flat.iter()
                    .map(|item| match item.cast::<PyComplex>() {
                        Ok(value) => Some(complex_of(value)),
                        Err(_) => number(item).map(|re| Complex64::new(re, 0.0)),
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|values| Variant::ComplexArray { shape, values })
            } else {
                flat.iter()
                    .map(number)
                    .collect::<Option<Vec<_>>>()
                    .map(|values| Variant::Array { shape, values })
            };
            if let Some(variant) = variant {
                return Ok(variant);
            }
        }
        return Ok(Variant::Other(object.repr()?.to_string()));
    }
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

/// Read a C numeric string prefix in CASA vector typemaps that accept text.
pub(crate) fn double_vector_element(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    if let Some(text) = numeric_text(value)? {
        // SAFETY: text is NUL-terminated and no end pointer was requested.
        return Ok(unsafe { strtod(text.as_ptr(), ptr::null_mut()) });
    }
    value.extract()
}

pub(crate) fn numeric_text(value: &Bound<'_, PyAny>) -> PyResult<Option<CString>> {
    let bytes = if let Ok(text) = value.cast::<PyString>() {
        text.to_str()?.as_bytes()
    } else if let Ok(text) = value.cast::<PyBytes>() {
        text.as_bytes()
    } else {
        return Ok(None);
    };
    let prefix = bytes.split(|byte| *byte == 0).next().unwrap_or_default();
    Ok(Some(
        CString::new(prefix).expect("the prefix contains no NUL"),
    ))
}

unsafe extern "C" {
    fn strtod(text: *const c_char, end: *mut *mut c_char) -> f64;
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
        Variant::Record(record) => {
            quantity_variant_record(record).map_err(ConvertError::in_converting_quantity)
        }
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
pub(crate) fn quantity_variant_record(record: &VariantRecord) -> ConvertResult<Quantity> {
    let malformed =
        || ConvertError::runtime("Illegal Quantum record in QuantumHolder::fromRecord\n");
    let Some(Variant::Text(unit)) = record.get("unit") else {
        return Err(malformed());
    };
    let unit = || casacore_unit(unit).map_err(ConvertError::runtime);
    match &record.scalar_kind {
        Some(RecordScalarKind::NativeInteger(value)) => {
            Ok(Quantity::with_unit(f64::from(*value as i32), unit()?))
        }
        Some(RecordScalarKind::SingletonIntegerVector(value)) => {
            Ok(Quantity::with_unit(f64::from(*value), unit()?))
        }
        Some(
            RecordScalarKind::NonScalarNumeric
            | RecordScalarKind::EmptyFloatVector
            | RecordScalarKind::MixedBoolFloat { .. },
        ) => {
            unit()?;
            Err(ConvertError::runtime(
                "Wrong QuantumHolder to convert asQuantumDouble",
            ))
        }
        Some(RecordScalarKind::NumpyUnsupported | RecordScalarKind::MalformedSequence) => {
            Err(malformed())
        }
        Some(RecordScalarKind::IntegerOverflow) => Err(ConvertError::IntegerOverflow),
        None => quantity_record(&record.fields).map_err(ConvertError::runtime),
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
        Variant::Array { shape, values } if !shape.is_empty() && values.len() == 1 => values[0],
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
