// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayD, ArrayValue, Complex64, RecordField, RecordValue, ScalarValue, Value};
use serde_json::{Map, Value as Json, json};

use crate::ImageError;

pub(crate) fn decode(value: &Json) -> Result<RecordValue, ImageError> {
    let object = value.as_object().ok_or_else(|| {
        ImageError::CasaSelection("Json error: JsonValue::getValueMap - invalid data type".into())
    })?;
    let fields = object
        .iter()
        .map(|(name, value)| Ok(RecordField::new(name, decode_value(value)?)))
        .collect::<Result<Vec<_>, ImageError>>()?;
    Ok(RecordValue::new(fields))
}

fn decode_value(value: &Json) -> Result<Value, ImageError> {
    let scalar = match value {
        Json::Bool(value) => ScalarValue::Bool(*value),
        Json::Number(value) => match value.as_i64() {
            Some(integer) => ScalarValue::Int64(integer),
            None => ScalarValue::Float64(value.as_f64().expect("JSON number")),
        },
        Json::String(value) => ScalarValue::String(value.clone()),
        Json::Object(_) => match decode_complex(value) {
            Some(value) => ScalarValue::Complex64(value),
            None => return decode(value).map(Value::Record),
        },
        Json::Array(values) => return decode_vector(values).map(Value::Array),
        Json::Null => {
            return Err(ImageError::CasaSelection(
                "Json error: JsonValue::getValueHolder - invalid data type".into(),
            ));
        }
    };
    Ok(Value::Scalar(scalar))
}

fn decode_complex(value: &Json) -> Option<Complex64> {
    // JsonOut writes complex values with this exact member order. An
    // ordinary record may contain the same names in name order (i, r).
    let mut fields = value.as_object()?.iter();
    let (real_name, real) = fields.next()?;
    let (imaginary_name, imaginary) = fields.next()?;
    if real_name != "r" || imaginary_name != "i" || fields.next().is_some() {
        return None;
    }
    Some(Complex64::new(real.as_f64()?, imaginary.as_f64()?))
}

fn decode_vector(values: &[Json]) -> Result<ArrayValue, ImageError> {
    use ndarray::Array1;

    if values.is_empty() {
        return Err(ImageError::CasaSelection(
            "Json error: empty vector in MiscInfo".into(),
        ));
    }
    if values.iter().any(Json::is_array) {
        return Err(ImageError::CasaSelection(
            "Json error: JsonValue::getDouble - invalid data type".into(),
        ));
    }
    if let Some(items) = values.iter().map(Json::as_bool).collect::<Option<Vec<_>>>() {
        return Ok(ArrayValue::Bool(Array1::from_vec(items).into_dyn()));
    }
    if let Some(items) = values.iter().map(Json::as_i64).collect::<Option<Vec<_>>>() {
        return Ok(ArrayValue::Int64(Array1::from_vec(items).into_dyn()));
    }
    if let Some(items) = values.iter().map(Json::as_f64).collect::<Option<Vec<_>>>() {
        return Ok(ArrayValue::Float64(Array1::from_vec(items).into_dyn()));
    }
    if let Some(items) = values
        .iter()
        .map(|item| {
            decode_complex(item).or_else(|| item.as_f64().map(|real| Complex64::new(real, 0.)))
        })
        .collect::<Option<Vec<_>>>()
    {
        return Ok(ArrayValue::Complex64(Array1::from_vec(items).into_dyn()));
    }
    if let Some(items) = values
        .iter()
        .map(|item| item.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()
    {
        return Ok(ArrayValue::String(Array1::from_vec(items).into_dyn()));
    }
    Err(ImageError::CasaSelection(
        "Json error: JsonValue::getValueHolder - vector of mixed data types".into(),
    ))
}

pub(crate) fn encode(record: &RecordValue) -> Result<Json, ImageError> {
    let mut fields = Map::new();
    for field in record.fields() {
        fields.insert(field.name.clone(), encode_value(&field.value)?);
    }
    fields.sort_keys();
    Ok(Json::Object(fields))
}

fn encode_value(value: &Value) -> Result<Json, ImageError> {
    match value {
        Value::Scalar(value) => encode_scalar(value),
        Value::Record(record) => encode(record),
        Value::Array(array) => encode_vector(array),
        Value::TableRef(_) => Err(ImageError::CasaSelection(
            "Saved image expression MiscInfo cannot store table references".into(),
        )),
    }
}

fn encode_scalar(value: &ScalarValue) -> Result<Json, ImageError> {
    let encoded = match value {
        ScalarValue::Bool(value) => json!(value),
        ScalarValue::UInt8(value) => json!(value),
        ScalarValue::UInt16(value) => json!(value),
        ScalarValue::UInt32(value) => json!(value),
        ScalarValue::Int16(value) => json!(value),
        ScalarValue::Int32(value) => json!(value),
        ScalarValue::Int64(value) => json!(value),
        ScalarValue::Float32(value) => json!(value),
        ScalarValue::Float64(value) => json!(value),
        ScalarValue::Complex32(value) => {
            return encode_complex(f64::from(value.re), f64::from(value.im));
        }
        ScalarValue::Complex64(value) => return encode_complex(value.re, value.im),
        ScalarValue::String(value) => json!(value),
    };
    ensure_finite(encoded)
}

fn encode_complex(real: f64, imaginary: f64) -> Result<Json, ImageError> {
    Ok(json!({"r": ensure_finite(json!(real))?, "i": ensure_finite(json!(imaginary))?}))
}

fn encode_vector(value: &ArrayValue) -> Result<Json, ImageError> {
    if value.ndim() != 1 || value.is_empty() {
        return Err(ImageError::CasaSelection(
            "Saved image expression MiscInfo requires non-empty one-dimensional arrays".into(),
        ));
    }
    let encoded = match value {
        ArrayValue::Bool(value) => vector(value),
        ArrayValue::UInt8(value) => vector(value),
        ArrayValue::UInt16(value) => vector(value),
        ArrayValue::UInt32(value) => vector(value),
        ArrayValue::Int16(value) => vector(value),
        ArrayValue::Int32(value) => vector(value),
        ArrayValue::Int64(value) => vector(value),
        ArrayValue::Float32(value) => vector(value),
        ArrayValue::Float64(value) => vector(value),
        ArrayValue::Complex32(value) => value
            .iter()
            .map(|item| encode_complex(f64::from(item.re), f64::from(item.im)))
            .collect(),
        ArrayValue::Complex64(value) => value
            .iter()
            .map(|item| encode_complex(item.re, item.im))
            .collect(),
        ArrayValue::String(value) => vector(value),
    }?;
    Ok(Json::Array(encoded))
}

fn vector<T: serde::Serialize>(values: &ArrayD<T>) -> Result<Vec<Json>, ImageError> {
    values
        .iter()
        .map(|value| ensure_finite(json!(value)))
        .collect()
}

fn ensure_finite(value: Json) -> Result<Json, ImageError> {
    if value.is_null() {
        Err(ImageError::CasaSelection(
            "Saved image expression MiscInfo requires finite numeric values".into(),
        ))
    } else {
        Ok(value)
    }
}
