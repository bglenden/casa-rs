// SPDX-License-Identifier: LGPL-3.0-or-later
//! The value a CASA task returns, as a task's typed reply carries it (#931).
//!
//! CASA's tasks return Python values: `None`, `True`, a dictionary such as
//! `imstat`'s statistics record, whose entries are NumPy arrays with their
//! own dtypes and shapes, or `flagdata`'s summary, keyed by integers. JSON
//! keeps none of those distinctions, so the reply carries a [`CasaValue`],
//! a tagged tree that does: Python rebuilds CASA's own types from it, and a
//! command line prints it as plain JSON ([`CasaValue::to_plain_json`]).
//!
//! An array's `data` is in column-major (Fortran) order, as casacore holds
//! arrays: the first axis varies fastest.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

/// A value as CASA's Python returns it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CasaValue {
    /// Python's `None`.
    #[default]
    None,
    /// A `bool`.
    Bool { value: bool },
    /// An `int`.
    Int { value: i64 },
    /// A `float`.
    Float { value: CasaFloat },
    /// A `str`.
    String { value: String },
    /// A `list`.
    List { items: Vec<CasaValue> },
    /// A `dict`, its entries in order.
    Dict { entries: Vec<CasaEntry> },
    /// A NumPy array of `bool`.
    BoolArray { shape: Vec<usize>, data: Vec<bool> },
    /// A NumPy array of integers.
    IntArray {
        dtype: IntDtype,
        shape: Vec<usize>,
        data: Vec<i64>,
    },
    /// A NumPy array of floats.
    FloatArray {
        dtype: FloatDtype,
        shape: Vec<usize>,
        data: Vec<CasaFloat>,
    },
    /// A NumPy array of complex numbers, each `[real, imaginary]`.
    ComplexArray {
        dtype: ComplexDtype,
        shape: Vec<usize>,
        data: Vec<[CasaFloat; 2]>,
    },
    /// A NumPy array of `str`.
    StrArray {
        shape: Vec<usize>,
        data: Vec<String>,
    },
}

/// One dictionary entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CasaEntry {
    pub key: CasaKey,
    pub value: CasaValue,
}

/// A dictionary key: CASA's records use strings, its summaries integers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum CasaKey {
    Int(i64),
    String(String),
}

/// The dtype of a [`CasaValue::IntArray`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntDtype {
    Int32,
    Int64,
}

/// The dtype of a [`CasaValue::FloatArray`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FloatDtype {
    Float32,
    Float64,
}

/// The dtype of a [`CasaValue::ComplexArray`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ComplexDtype {
    Complex64,
    Complex128,
}

/// A float that survives JSON: a number when finite, else `"nan"`, `"inf"`
/// or `"-inf"`, which CASA's statistics of an empty or masked selection
/// hold.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(from = "FloatRepr", into = "FloatRepr")]
pub struct CasaFloat(pub f64);

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum FloatRepr {
    Number(f64),
    NonFinite(NonFinite),
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
enum NonFinite {
    #[serde(rename = "nan")]
    Nan,
    #[serde(rename = "inf")]
    Inf,
    #[serde(rename = "-inf")]
    NegInf,
}

impl From<FloatRepr> for CasaFloat {
    fn from(repr: FloatRepr) -> Self {
        Self(match repr {
            FloatRepr::Number(value) => value,
            FloatRepr::NonFinite(NonFinite::Nan) => f64::NAN,
            FloatRepr::NonFinite(NonFinite::Inf) => f64::INFINITY,
            FloatRepr::NonFinite(NonFinite::NegInf) => f64::NEG_INFINITY,
        })
    }
}

impl From<CasaFloat> for FloatRepr {
    fn from(CasaFloat(value): CasaFloat) -> Self {
        if value.is_nan() {
            Self::NonFinite(NonFinite::Nan)
        } else if value == f64::INFINITY {
            Self::NonFinite(NonFinite::Inf)
        } else if value == f64::NEG_INFINITY {
            Self::NonFinite(NonFinite::NegInf)
        } else {
            Self::Number(value)
        }
    }
}

impl CasaValue {
    /// A `str`.
    pub fn string(value: impl Into<String>) -> Self {
        Self::String {
            value: value.into(),
        }
    }

    /// A `float`.
    pub fn float(value: f64) -> Self {
        Self::Float {
            value: CasaFloat(value),
        }
    }

    /// A `dict` of string keys, in the order given.
    pub fn dict<K: Into<String>>(entries: impl IntoIterator<Item = (K, CasaValue)>) -> Self {
        Self::Dict {
            entries: entries
                .into_iter()
                .map(|(key, value)| CasaEntry {
                    key: CasaKey::String(key.into()),
                    value,
                })
                .collect(),
        }
    }

    /// A one-dimensional `float64` array.
    pub fn float64_vector(values: impl IntoIterator<Item = f64>) -> Self {
        let data = values.into_iter().map(CasaFloat).collect::<Vec<_>>();
        Self::FloatArray {
            dtype: FloatDtype::Float64,
            shape: vec![data.len()],
            data,
        }
    }

    /// A one-dimensional `int64` array.
    pub fn int64_vector(values: impl IntoIterator<Item = i64>) -> Self {
        let data = values.into_iter().collect::<Vec<_>>();
        Self::IntArray {
            dtype: IntDtype::Int64,
            shape: vec![data.len()],
            data,
        }
    }

    /// A one-dimensional `str` array.
    pub fn str_vector<S: Into<String>>(values: impl IntoIterator<Item = S>) -> Self {
        let data = values.into_iter().map(Into::into).collect::<Vec<_>>();
        Self::StrArray {
            shape: vec![data.len()],
            data,
        }
    }

    /// Plain JSON read as Python reads it: objects become string-keyed
    /// dictionaries and arrays lists. For tasks whose return value is not
    /// yet CASA's own (#931).
    pub fn from_plain_json(value: &Value) -> Self {
        match value {
            Value::Null => Self::None,
            Value::Bool(value) => Self::Bool { value: *value },
            Value::Number(number) => match number.as_i64() {
                Some(value) => Self::Int { value },
                None => Self::float(number.as_f64().unwrap_or(f64::NAN)),
            },
            Value::String(value) => Self::string(value.clone()),
            Value::Array(items) => Self::List {
                items: items.iter().map(Self::from_plain_json).collect(),
            },
            Value::Object(entries) => Self::dict(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::from_plain_json(value))),
            ),
        }
    }

    /// The value as plain JSON, as a command line prints it: arrays as
    /// nested lists in their shape, integer keys as strings, and non-finite
    /// floats as `"nan"`, `"inf"` or `"-inf"`.
    pub fn to_plain_json(&self) -> Value {
        match self {
            Self::None => Value::Null,
            Self::Bool { value } => Value::Bool(*value),
            Self::Int { value } => Value::from(*value),
            Self::Float { value } => plain_float(*value),
            Self::String { value } => Value::String(value.clone()),
            Self::List { items } => Value::Array(items.iter().map(Self::to_plain_json).collect()),
            Self::Dict { entries } => Value::Object(
                entries
                    .iter()
                    .map(|entry| {
                        let key = match &entry.key {
                            CasaKey::Int(key) => key.to_string(),
                            CasaKey::String(key) => key.clone(),
                        };
                        (key, entry.value.to_plain_json())
                    })
                    .collect::<Map<_, _>>(),
            ),
            Self::BoolArray { shape, data } => nest(shape, data, |value| Value::Bool(*value)),
            Self::IntArray { shape, data, .. } => nest(shape, data, |value| Value::from(*value)),
            Self::FloatArray { shape, data, .. } => nest(shape, data, |value| plain_float(*value)),
            Self::ComplexArray { shape, data, .. } => nest(shape, data, |[re, im]| {
                Value::Array(vec![plain_float(*re), plain_float(*im)])
            }),
            Self::StrArray { shape, data } => {
                nest(shape, data, |value| Value::String(value.clone()))
            }
        }
    }

    /// Whether every array's data has as many elements as its shape holds.
    pub fn is_consistent(&self) -> bool {
        let fits = |shape: &[usize], len: usize| shape.iter().product::<usize>() == len;
        match self {
            Self::List { items } => items.iter().all(Self::is_consistent),
            Self::Dict { entries } => entries.iter().all(|entry| entry.value.is_consistent()),
            Self::BoolArray { shape, data } => fits(shape, data.len()),
            Self::IntArray { shape, data, .. } => fits(shape, data.len()),
            Self::FloatArray { shape, data, .. } => fits(shape, data.len()),
            Self::ComplexArray { shape, data, .. } => fits(shape, data.len()),
            Self::StrArray { shape, data } => fits(shape, data.len()),
            Self::None
            | Self::Bool { .. }
            | Self::Int { .. }
            | Self::Float { .. }
            | Self::String { .. } => true,
        }
    }
}

fn plain_float(CasaFloat(value): CasaFloat) -> Value {
    match Number::from_f64(value) {
        Some(number) => Value::Number(number),
        None => serde_json::to_value(CasaFloat(value)).unwrap_or(Value::Null),
    }
}

/// Column-major `data` as nested lists of `shape`, the first axis outermost
/// as NumPy's `tolist` nests them; a zero-dimensional array is its one
/// element.
fn nest<T>(shape: &[usize], data: &[T], element: impl Fn(&T) -> Value + Copy) -> Value {
    fn level<T>(
        shape: &[usize],
        offset: usize,
        stride: usize,
        data: &[T],
        element: impl Fn(&T) -> Value + Copy,
    ) -> Value {
        match shape.split_first() {
            None => data.get(offset).map_or(Value::Null, element),
            Some((&count, rest)) => Value::Array(
                (0..count)
                    .map(|index| {
                        level(rest, offset + index * stride, stride * count, data, element)
                    })
                    .collect(),
            ),
        }
    }
    level(shape, 0, 1, data, element)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn values_keep_their_python_types_on_the_wire() {
        let value = CasaValue::dict([
            ("npts", CasaValue::float64_vector([12.0])),
            ("blc", CasaValue::int64_vector([0, 0, 0, 0])),
            (
                "blcf",
                CasaValue::string("00:00:00.000, +00.00.00.000, I, 1.415e+09Hz"),
            ),
            ("min", CasaValue::float64_vector([f64::NAN])),
        ]);
        let text = serde_json::to_string(&value).unwrap();
        assert!(
            text.contains(r#"{"kind":"float_array","dtype":"float64","shape":[1],"data":[12.0]}"#)
        );
        assert!(text.contains(r#""data":["nan"]"#));
        let back: CasaValue = serde_json::from_str(&text).unwrap();
        // NaN is not equal to itself; compare the wire form.
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
    }

    #[test]
    fn integer_keys_and_non_finite_floats_round_trip() {
        let value = CasaValue::Dict {
            entries: vec![CasaEntry {
                key: CasaKey::Int(3),
                value: CasaValue::Float {
                    value: CasaFloat(f64::NEG_INFINITY),
                },
            }],
        };
        let wire = serde_json::to_value(&value).unwrap();
        assert_eq!(
            wire,
            json!({"kind": "dict", "entries": [{"key": 3, "value": {"kind": "float", "value": "-inf"}}]})
        );
        assert_eq!(serde_json::from_value::<CasaValue>(wire).unwrap(), value);
    }

    #[test]
    fn plain_json_nests_column_major_arrays_in_their_shape() {
        let value = CasaValue::FloatArray {
            dtype: FloatDtype::Float32,
            shape: vec![2, 3],
            data: [1.0, 2.0, 3.0, 4.0, 5.0, f64::INFINITY]
                .map(CasaFloat)
                .to_vec(),
        };
        assert!(value.is_consistent());
        assert_eq!(
            value.to_plain_json(),
            json!([[1.0, 3.0, 5.0], [2.0, 4.0, "inf"]])
        );
        let scalar = CasaValue::IntArray {
            dtype: IntDtype::Int32,
            shape: vec![],
            data: vec![7],
        };
        assert_eq!(scalar.to_plain_json(), json!(7));
        let ragged = CasaValue::StrArray {
            shape: vec![2],
            data: vec!["I".into()],
        };
        assert!(!ragged.is_consistent());
    }

    #[test]
    fn plain_json_reads_as_python_reads_it() {
        let value =
            CasaValue::from_plain_json(&json!({"n": 3, "names": ["a"], "none": null, "x": 0.5}));
        assert_eq!(
            value,
            CasaValue::dict([
                ("n", CasaValue::Int { value: 3 }),
                (
                    "names",
                    CasaValue::List {
                        items: vec![CasaValue::string("a")]
                    }
                ),
                ("none", CasaValue::None),
                ("x", CasaValue::float(0.5)),
            ])
        );
    }
}
