// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's toCasaString conversion for variant selection arguments.

use numpy::PyUntypedArray;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::Variant;

enum ListingSelectorElement {
    Text(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

pub(crate) fn listing_selector(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let mut items = Vec::new();
    listing_selector_elements(value, &mut items, 0)?;
    let floats = items
        .iter()
        .any(|item| matches!(item, ListingSelectorElement::Float(_)));
    let integers = items
        .iter()
        .any(|item| matches!(item, ListingSelectorElement::Integer(_)));
    Ok(items
        .into_iter()
        .map(|item| match item {
            ListingSelectorElement::Text(text) => text,
            ListingSelectorElement::Float(value) => format!("{value:.6}"),
            ListingSelectorElement::Integer(value) if floats => format!("{:.6}", value as f64),
            ListingSelectorElement::Integer(value) => value.to_string(),
            ListingSelectorElement::Bool(value) if floats => {
                format!("{:.6}", u8::from(value) as f64)
            }
            ListingSelectorElement::Bool(value) if integers => u8::from(value).to_string(),
            ListingSelectorElement::Bool(value) => value.to_string(),
        })
        .collect::<Vec<_>>()
        .join(","))
}

fn listing_selector_elements(
    value: &Bound<'_, PyAny>,
    items: &mut Vec<ListingSelectorElement>,
    depth: usize,
) -> PyResult<()> {
    use ListingSelectorElement::{Bool, Float, Integer, Text};

    if depth == 128 {
        return Err(PyValueError::new_err(
            "cyclic or too deeply nested listing selector",
        ));
    }
    if value.is_none() {
        return Ok(());
    }
    if value.is_instance_of::<PyUntypedArray>() {
        let flattened = value
            .call_method1("ravel", ("F",))?
            .call_method0("tolist")?;
        return listing_selector_elements(&flattened, items, depth + 1);
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        for item in value.try_iter()? {
            listing_selector_elements(&item?, items, depth + 1)?;
        }
        return Ok(());
    }
    let element = if let Ok(flag) = value.cast::<PyBool>() {
        Bool(flag.is_true())
    } else if value.is_instance_of::<PyInt>() {
        Integer(value.extract()?)
    } else if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        let text: String = if value.is_instance_of::<PyBytes>() {
            value.call_method1("decode", ("utf-8",))?.extract()?
        } else {
            value.extract()?
        };
        Text(text.split('\0').next().unwrap_or_default().into())
    } else if value.is_instance_of::<PyFloat>() {
        Float(value.extract()?)
    } else {
        let numpy = value.py().import("numpy")?;
        if value.is_instance(&numpy.getattr("signedinteger")?)? {
            Integer(value.extract()?)
        } else if value.is_instance(&numpy.getattr("floating")?)? {
            Float(value.extract()?)
        } else if value.is_instance(&numpy.getattr("unsignedinteger")?)?
            || value.is_instance(&numpy.getattr("bool_")?)?
        {
            return Ok(());
        } else {
            Text(value.extract::<Variant>()?.describe())
        }
    };
    items.push(element);
    Ok(())
}
