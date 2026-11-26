// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{RecordField, RecordValue, ScalarValue, Value};

use super::reduction::Moments;
use crate::{MsError, MsResult};

pub(super) fn modern_record(
    mut moments: Moments,
    masked: bool,
    weighted: bool,
    quantiles: bool,
    boolean_column: bool,
) -> RecordValue {
    let populated = moments.count > 0;
    let scalar = |value: f64| {
        if populated && boolean_column {
            ScalarValue::Int64(value as i64)
        } else {
            ScalarValue::Float64(value)
        }
    };
    let missing = if quantiles { f64::NAN } else { 0.0 };
    let variance = moments.variance();
    let mut fields = RecordValue::new(vec![
        field("isMasked", ScalarValue::Bool(masked && populated)),
        field("isWeighted", ScalarValue::Bool(weighted && populated)),
        field("npts", ScalarValue::Float64(moments.count as f64)),
    ]);
    for (name, value) in [
        ("mean", moments.mean),
        ("rms", moments.rms()),
        ("stddev", variance.sqrt()),
        ("sum", moments.sum),
        ("sumOfWeights", moments.sum_weights),
        ("sumsq", moments.sum_squares),
        ("variance", variance),
    ] {
        fields.push(field(name, scalar(if populated { value } else { missing })));
    }
    if let Some(min) = moments.min {
        fields.push(field("min", scalar(min.value)));
        fields.push(field(
            "minDatasetIndex",
            ScalarValue::Int64(min.dataset as i64),
        ));
        fields.push(field("minIndex", ScalarValue::Int64(min.index as i64)));
    }
    if let Some(max) = moments.max {
        fields.push(field("max", scalar(max.value)));
        fields.push(field(
            "maxDatasetIndex",
            ScalarValue::Int64(max.dataset as i64),
        ));
        fields.push(field("maxIndex", ScalarValue::Int64(max.index as i64)));
    }
    if quantiles {
        let observed = moments.quantiles();
        for (name, value) in [
            ("firstquartile", observed.as_ref().map(|q| q.first)),
            ("thirdquartile", observed.as_ref().map(|q| q.third)),
            ("median", observed.as_ref().map(|q| q.median)),
            (
                "medabsdevmed",
                observed.as_ref().map(|q| q.median_deviation),
            ),
        ] {
            fields.push(field(name, scalar(value.unwrap_or(f64::NAN))));
        }
    }
    fields
        .fields_mut()
        .sort_unstable_by(|a, b| a.name.cmp(&b.name));
    fields
}

pub(super) fn legacy_record(mut moments: Moments) -> MsResult<RecordValue> {
    let quantiles = moments
        .quantiles()
        .ok_or_else(|| MsError::InvalidInput("All selected rows are flagged.".into()))?;
    let variance = moments.variance();
    Ok(RecordValue::new(
        [
            ("max", moments.max.unwrap().value),
            ("mean", moments.mean),
            ("medabsdevmed", quantiles.median_deviation),
            ("median", quantiles.median),
            ("min", moments.min.unwrap().value),
            ("npts", moments.count as f64),
            ("quartile", quantiles.third - quantiles.first),
            ("rms", moments.rms()),
            ("stddev", variance.sqrt()),
            ("sum", moments.sum),
            ("sumsq", moments.sum_squares),
            ("var", variance),
        ]
        .into_iter()
        .map(|(name, value)| field(name, ScalarValue::Float64(value)))
        .collect(),
    ))
}

fn field(name: &str, value: ScalarValue) -> RecordField {
    RecordField::new(name, Value::Scalar(value))
}
