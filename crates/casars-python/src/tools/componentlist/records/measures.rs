// SPDX-License-Identifier: LGPL-3.0-or-later
//! Component MeasureHolder records, preserving canonical offsets in memory.
use super::{replace, scalar};
use crate::convert::Variant;
use crate::convert::holder::{CasaMeasure, Measure, MeasureKind};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

pub(super) fn normalize(
    input: &RecordValue,
    kind: MeasureKind,
    lines: &mut Vec<crate::tools::LogLine>,
) -> Result<RecordValue, String> {
    let fields = input
        .fields()
        .iter()
        .filter(|f| f.name != "offset")
        .map(|f| (f.name.clone(), variant(&f.value)))
        .collect();
    let read = Measure::from_record(&fields).map_err(|e| e.message())?;
    if let Some(warning) = read.warning {
        lines.push(crate::tools::LogLine::warning(warning));
    }
    let measure = read.measure;
    if measure.kind() != kind {
        return Err("Illegal quantity in MeasureHolder::fromRecord\n".into());
    }
    let mut record = match measure.to_casa().map_err(|e| e.to_string())? {
        CasaMeasure::Direction(direction) => casa_types::measures::direction_to_record(&direction),
        CasaMeasure::Frequency(frequency) => casa_types::measures::frequency_to_record(&frequency),
        _ => unreachable!("only direction and frequency component fields"),
    };
    if let Some(offset) = input.get("offset") {
        let Value::Record(offset) = offset else {
            return Err("Illegal Measure record in MeasureHolder::fromRecord\n".into());
        };
        replace(
            &mut record,
            "offset",
            Value::Record(normalize(offset, kind, lines)?),
        );
    }
    if let Some(error) = input.get("error") {
        replace(&mut record, "error", error.clone());
    }
    Ok(record)
}

fn variant(value: &Value) -> Variant {
    if let Some(number) = scalar(value) {
        return Variant::Number(number);
    }
    match value {
        Value::Record(record) => Variant::Record(
            record
                .fields()
                .iter()
                .map(|f| (f.name.clone(), variant(&f.value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into(),
        ),
        Value::Scalar(ScalarValue::String(s)) => Variant::Text(s.clone()),
        Value::Scalar(ScalarValue::Bool(v)) => Variant::Bool(*v),
        Value::Array(ArrayValue::Float64(a)) => Variant::Numbers(a.iter().copied().collect()),
        Value::Array(ArrayValue::Int32(a)) => {
            Variant::Numbers(a.iter().map(|v| f64::from(*v)).collect())
        }
        _ => Variant::Other(format!("{value:?}")),
    }
}
