// SPDX-License-Identifier: LGPL-3.0-or-later
//! Values CASA's tools return: casatools' `record` and `variant` results,
//! built without the GIL and converted to Python once a call is done.
//!
//! casatools hands records to Python as `dict`s whose keys are sorted (its
//! records are `std::map`s), so [`CasaValue::record`] sorts them; arrays
//! become NumPy arrays, numeric ones of `float64` and string ones of
//! NumPy's unicode type, as casatools returns them.

use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use numpy::IntoPyArray;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::Variant;

/// A value a CASA tool returns.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CasaValue {
    /// A `bool`.
    Bool(bool),
    /// An `int`.
    Int(i64),
    /// A `float`.
    Float(f64),
    /// A `str`.
    Text(String),
    /// A NumPy `float64` array of `shape`, its values in Fortran (column-major)
    /// order, as casacore arrays hold them.
    Floats { shape: Vec<usize>, values: Vec<f64> },
    /// A NumPy unicode array.
    Texts(Vec<String>),
    /// A Python `list`.
    List(Vec<CasaValue>),
    /// A Python `tuple`.
    Tuple(Vec<CasaValue>),
    /// A `dict` with sorted keys.
    Record(Vec<(String, CasaValue)>),
}

impl CasaValue {
    /// A record of `fields`, sorted by key as casatools sorts them.
    pub(crate) fn record(fields: impl IntoIterator<Item = (impl Into<String>, CasaValue)>) -> Self {
        let mut fields = fields
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect::<Vec<_>>();
        fields.sort_by(|left, right| left.0.cmp(&right.0));
        Self::Record(fields)
    }

    /// The empty record `{}`: what CASA's tools return for a failure they
    /// log.
    pub(crate) fn empty_record() -> Self {
        Self::Record(Vec::new())
    }

    /// A one-dimensional `float64` array.
    pub(crate) fn floats(values: Vec<f64>) -> Self {
        Self::Floats {
            shape: vec![values.len()],
            values,
        }
    }

    /// An argument as CASA hands a record field back (`me.getvalue`).
    pub(crate) fn from_variant(value: &Variant) -> Self {
        match value {
            Variant::Empty => Self::List(Vec::new()),
            Variant::Bool(flag) => Self::Bool(*flag),
            Variant::Number(number) => Self::Float(*number),
            Variant::Text(text) | Variant::Other(text) => Self::Text(text.clone()),
            Variant::Texts(texts) => Self::Texts(texts.clone()),
            Variant::Numbers(numbers) => Self::floats(numbers.clone()),
            Variant::Array { shape, values } => Self::Floats {
                shape: shape.clone(),
                values: values.clone(),
            },
            Variant::Record(record) => Self::record(
                record
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::from_variant(value))),
            ),
        }
    }

    /// A quantity record `{'unit': unit, 'value': value}`.
    pub(crate) fn quantity(value: f64, unit: impl Into<String>) -> Self {
        Self::record([
            ("unit", Self::Text(unit.into())),
            ("value", Self::Float(value)),
        ])
    }

    /// The value as a Python object.
    pub(crate) fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Bool(flag) => flag.into_pyobject(py)?.to_owned().into_any().unbind(),
            Self::Int(value) => value.into_pyobject(py)?.into_any().unbind(),
            Self::Float(value) => value.into_pyobject(py)?.into_any().unbind(),
            Self::Text(text) => text.into_pyobject(py)?.into_any().unbind(),
            Self::Floats { shape, values } => {
                let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values).map_err(|error| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "array of shape {shape:?}: {error}"
                    ))
                })?;
                array.into_pyarray(py).into_any().unbind()
            }
            Self::Texts(texts) => {
                let keywords = PyDict::new(py);
                keywords.set_item("dtype", "str")?;
                py.import("numpy")?
                    .call_method("array", (PyList::new(py, texts)?,), Some(&keywords))?
                    .unbind()
            }
            Self::List(items) => {
                let items = items
                    .into_iter()
                    .map(|item| item.into_python(py))
                    .collect::<PyResult<Vec<_>>>()?;
                PyList::new(py, items)?.into_any().unbind()
            }
            Self::Tuple(items) => {
                let items = items
                    .into_iter()
                    .map(|item| item.into_python(py))
                    .collect::<PyResult<Vec<_>>>()?;
                PyTuple::new(py, items)?.into_any().unbind()
            }
            Self::Record(fields) => {
                let dict = PyDict::new(py);
                for (key, value) in fields {
                    dict.set_item(key, value.into_python(py)?)?;
                }
                dict.into_any().unbind()
            }
        })
    }
}

/// `value` as a C++ `ostream` writes a `double` by default: six significant
/// digits, in fixed or scientific notation as `%g` chooses, without
/// trailing zeros (`3.14159`, `6.37358e+06`, `58849`).
pub(crate) fn stream_double(value: f64) -> String {
    const PRECISION: usize = 6;
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".to_string()
        } else if value > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    let scientific = format!("{:.*e}", PRECISION - 1, value);
    let (mantissa, exponent) = scientific.split_once('e').expect("Rust writes an exponent");
    let exponent: i32 = exponent.parse().expect("Rust writes an integer exponent");
    let trim = |digits: &str| {
        if digits.contains('.') {
            digits
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            digits.to_string()
        }
    };
    if exponent < -4 || exponent >= PRECISION as i32 {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    } else {
        let decimals = usize::try_from(PRECISION as i32 - 1 - exponent).unwrap_or(0);
        trim(&format!("{value:.decimals$}"))
    }
}

#[cfg(test)]
mod tests {
    use super::{CasaValue, stream_double};

    #[test]
    fn doubles_stream_as_cpp_writes_them() {
        for (value, written) in [
            (std::f64::consts::PI, "3.14159"),
            (0.785_398_163_397_448_2, "0.785398"),
            (6_373_576.280_915_651, "6.37358e+06"),
            (58_849.0, "58849"),
            (58_849.25, "58849.2"),
            (1.4e9, "1.4e+09"),
            (-1.878_294_258_139_436_2, "-1.87829"),
            (1e-6, "1e-06"),
            (0.000_123_4, "0.0001234"),
            (999_999.5, "1e+06"),
            (30.0, "30"),
        ] {
            assert_eq!(stream_double(value), written, "{value}");
        }
    }

    #[test]
    fn records_sort_their_keys_as_casatools_does() {
        let record = CasaValue::record([
            ("type", CasaValue::Text("epoch".into())),
            ("m0", CasaValue::quantity(1.0, "d")),
            ("refer", CasaValue::Text("UTC".into())),
        ]);
        let CasaValue::Record(fields) = record else {
            panic!("a record");
        };
        let keys = fields
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["m0", "refer", "type"]);
        let CasaValue::Record(quantity) = &fields[0].1 else {
            panic!("a quantity record");
        };
        assert_eq!(quantity[0].0, "unit");
        assert_eq!(quantity[1].0, "value");
    }
}
