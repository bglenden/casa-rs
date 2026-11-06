// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's metadata history rendering for typed custom values.
use casa_types::ArrayValue;

use super::projection_header::cpp_number_precision;

pub(super) fn format_array(array: &ArrayValue) -> String {
    let values: Vec<String> = match array {
        ArrayValue::Bool(values) => values
            .iter()
            .map(|v| if *v { "1" } else { "0" }.to_string())
            .collect(),
        ArrayValue::Int32(values) => values.iter().map(ToString::to_string).collect(),
        ArrayValue::Float64(values) => values
            .iter()
            .map(|value| cpp_number_precision(*value, 6))
            .collect(),
        ArrayValue::String(values) => values.iter().cloned().collect(),
        _ => unreachable!("only parameter-converted arrays are formatted"),
    };
    format!("[{}]", values.join(", "))
}
