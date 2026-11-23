// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL mask construction, inspection, and Boolean reductions.

use super::TaqlError;
use super::eval::{ArrayValue, ExprValue};

pub(super) fn create(value: &ExprValue, flag_value: &ExprValue) -> Result<ExprValue, TaqlError> {
    let (array, old_mask) = require_array("marray", value)?;
    let (flags, _) = require_array("marray", flag_value)?;
    if array.shape != flags.shape {
        return Err(type_error("marray() data and mask shapes differ"));
    }
    let mask = flags
        .data
        .iter()
        .enumerate()
        .map(|(index, flag)| match flag {
            ExprValue::Bool(flag) => Ok(*flag || old_mask.is_some_and(|mask| mask[index])),
            _ => Err(type_error("marray() needs a Boolean mask")),
        })
        .collect::<Result<_, _>>()?;
    Ok(ExprValue::MaskedArray(array.clone(), mask))
}

pub(super) fn inspect(name: &str, value: &ExprValue) -> Result<ExprValue, TaqlError> {
    let (array, mask) = require_array(name, value)?;
    Ok(match name {
        "arraydata" | "getarrdata" => ExprValue::Array(array.clone()),
        "negatemask" => {
            let mask = (0..array.data.len())
                .map(|index| !mask.is_some_and(|flag| flag[index]))
                .collect();
            ExprValue::MaskedArray(array.clone(), mask)
        }
        _ => {
            // The pinned Boolean accessor asks for the mask of its Boolean mask.
            let mask = mask.filter(|_| !matches!(array.data.first(), Some(ExprValue::Bool(_))));
            let data = (0..array.data.len())
                .map(|index| ExprValue::Bool(mask.is_some_and(|flag| flag[index])))
                .collect();
            ExprValue::Array(ArrayValue {
                shape: array.shape.clone(),
                data,
            })
        }
    })
}

pub(super) fn replace(
    name: &str,
    value: &ExprValue,
    replacements: &ExprValue,
) -> Result<ExprValue, TaqlError> {
    let (array, mask) = require_array(name, value)?;
    let replacement_array = replacements.array_with_mask().map(|(array, _)| array);
    if replacement_array.is_some_and(|replacement| replacement.shape != array.shape) {
        return Err(type_error(format!("{name}() array shapes differ")));
    }
    let masked = name == "replacemasked";
    let data = array
        .data
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let is_masked = mask.is_some_and(|flag| flag[index]);
            if is_masked == masked {
                replacement_array
                    .map_or(replacements, |array| &array.data[index])
                    .clone()
            } else {
                value.clone()
            }
        })
        .collect();
    Ok(ExprValue::from_array_with_mask(
        ArrayValue {
            shape: array.shape.clone(),
            data,
        },
        mask.map(<[bool]>::to_vec),
    ))
}

pub(super) fn reduce_bool(name: &str, value: &ExprValue) -> Result<ExprValue, TaqlError> {
    let (array, mask) = require_array(name, value)?;
    let mut values = array
        .data
        .iter()
        .enumerate()
        .filter(|(index, _)| !mask.is_some_and(|flag| flag[*index]))
        .map(|(_, value)| value)
        .peekable();
    let is_true = |v: &ExprValue| matches!(v, ExprValue::Bool(true));
    Ok(match name {
        "any" => ExprValue::Bool(values.any(is_true)),
        "all" => {
            ExprValue::Bool((mask.is_none() || values.peek().is_some()) && values.all(is_true))
        }
        "ntrue" => ExprValue::Int(values.filter(|value| is_true(*value)).count() as i64),
        _ => ExprValue::Int(
            values
                .filter(|value| matches!(value, ExprValue::Bool(false)))
                .count() as i64,
        ),
    })
}

fn require_array<'a>(
    name: &str,
    val: &'a ExprValue,
) -> Result<(&'a ArrayValue, Option<&'a [bool]>), TaqlError> {
    val.array_with_mask()
        .ok_or_else(|| type_error(format!("{name}() requires Array, got {}", val.type_name())))
}

fn type_error(message: impl Into<String>) -> TaqlError {
    TaqlError::TypeError {
        message: message.into(),
    }
}
