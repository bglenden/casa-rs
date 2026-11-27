// SPDX-License-Identifier: LGPL-3.0-or-later
//! Move typed casacore visibility records into the Python reply representation.

use casa_core::{ArrayValue, RecordValue, Value};
use ndarray::ArrayD;
use num_complex::Complex64;

use crate::convert::value::CasaValue;

pub(super) fn visibility_record(record: RecordValue) -> CasaValue {
    CasaValue::record(record.into_fields().into_iter().map(|field| {
        let value = match field.value {
            Value::Array(array) => visibility_array(array),
            Value::Record(record) => visibility_record(record),
            other => CasaValue::from_value(&other),
        };
        (field.name, value)
    }))
}

fn visibility_array(array: ArrayValue) -> CasaValue {
    macro_rules! move_array {
        ($array:expr, $variant:ident) => {{
            let (shape, values) = take_fortran($array);
            CasaValue::$variant { shape, values }
        }};
        ($array:expr, $variant:ident, $convert:expr) => {{
            let (shape, values) = take_fortran($array);
            CasaValue::$variant {
                shape,
                values: values.into_iter().map($convert).collect(),
            }
        }};
    }
    match array {
        ArrayValue::Bool(array) => move_array!(array, Bools),
        ArrayValue::UInt8(array) => move_array!(array, Ints, i64::from),
        ArrayValue::UInt16(array) => move_array!(array, Ints, i64::from),
        ArrayValue::UInt32(array) => move_array!(array, Ints, i64::from),
        ArrayValue::Int16(array) => move_array!(array, Ints, i64::from),
        ArrayValue::Int32(array) => move_array!(array, Ints, i64::from),
        ArrayValue::Int64(array) => move_array!(array, Ints),
        ArrayValue::Float32(array) => move_array!(array, Floats, f64::from),
        ArrayValue::Float64(array) => move_array!(array, Floats),
        ArrayValue::Complex32(array) => move_array!(array, Complexes, |value| Complex64::new(
            f64::from(value.re),
            f64::from(value.im)
        )),
        ArrayValue::Complex64(array) => move_array!(array, Complexes),
        ArrayValue::String(array) => move_array!(array, TextArray),
    }
}

fn take_fortran<T>(array: ArrayD<T>) -> (Vec<usize>, Vec<T>) {
    let shape = array.shape().to_vec();
    let array = array.reversed_axes();
    let length = array.len();
    if array.is_standard_layout() {
        let (mut values, offset) = array.into_raw_vec_and_offset();
        if let Some(offset) = offset {
            values.truncate(offset + length);
            values.drain(..offset);
        } else {
            values.clear();
        }
        (shape, values)
    } else {
        (shape, array.into_iter().collect())
    }
}
