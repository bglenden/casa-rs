// SPDX-License-Identifier: LGPL-3.0-or-later
//! Preserve SOURCE row records while remapping their logical and spectral IDs.

use std::collections::BTreeMap;

use casa_types::{RecordValue, ScalarValue, Value};

use crate::subtables::{get_f64, get_i32};
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

const IDENTITY: &[&str] = &[
    "CALIBRATION_GROUP",
    "CODE",
    "NAME",
    "NUM_LINES",
    "DIRECTION",
    "PROPER_MOTION",
    "POSITION",
    "PULSAR_ID",
    "REST_FREQUENCY",
    "SYSVEL",
    "TRANSITION",
];

#[derive(Default)]
pub(super) struct SourceRows {
    pub(super) additions: Vec<RecordValue>,
    pub(super) updates: Vec<(usize, RecordValue)>,
    pub(super) ids: BTreeMap<i32, i32>,
}

pub(super) fn reconcile(
    target: &MeasurementSet,
    donor: &MeasurementSet,
    windows: &[usize],
) -> MsResult<SourceRows> {
    let Some(target) = target.subtable(SubtableId::Source) else {
        return Ok(SourceRows::default());
    };
    let Some(donor) = donor.subtable(SubtableId::Source) else {
        return Ok(SourceRows::default());
    };
    if donor.row_count() == 0 {
        return Ok(SourceRows::default());
    }
    let mut known = target.rows()?.to_vec();
    let offset = (0..target.row_count())
        .map(|row| get_i32(target, row, "SOURCE_ID"))
        .collect::<MsResult<Vec<_>>>()?
        .into_iter()
        .max()
        .unwrap_or(-1)
        .checked_add(1)
        .ok_or_else(|| MsError::InvalidInput("SOURCE_ID exceeds int32".into()))?;
    let mut ids = BTreeMap::new();
    let mut added = Vec::new();
    let mut modified = BTreeMap::new();

    for (index, original) in donor.rows()?.iter().enumerate() {
        let id = get_i32(donor, index, "SOURCE_ID")?;
        let window = get_i32(donor, index, "SPECTRAL_WINDOW_ID")?;
        let mapped_window = map_window(windows, window)?;
        let time = get_f64(donor, index, "TIME")?;
        let interval = get_f64(donor, index, "INTERVAL")?;
        let new_id = id
            .checked_add(offset)
            .ok_or_else(|| MsError::InvalidInput("SOURCE_ID exceeds int32".into()))?;
        let mut incoming = original.clone();
        incoming.upsert("SPECTRAL_WINDOW_ID", integer(mapped_window));
        let same_window = known.iter().position(|record| {
            record.get("SPECTRAL_WINDOW_ID") == Some(&integer(mapped_window))
                && equivalent(record, &incoming)
        });
        if let Some(found) = same_window {
            let record = &mut known[found];
            let mapped = read_id(record, "SOURCE_ID")?;
            ids.insert(id, mapped);
            widen_interval(record, time, interval)?;
            if found < target.row_count() {
                modified.insert(found, record.clone());
            }
        } else {
            let equivalent_id = known
                .iter()
                .find(|row| equivalent(row, &incoming))
                .map(|row| read_id(row, "SOURCE_ID"))
                .transpose()?;
            let mapped = *ids.entry(id).or_insert(equivalent_id.unwrap_or(new_id));
            incoming.upsert("SOURCE_ID", integer(mapped));
            added.push((id, known.len()));
            known.push(incoming);
        }
    }
    let additions = added
        .into_iter()
        .map(|(input_id, index)| {
            let mut row = known[index].clone();
            row.upsert("SOURCE_ID", integer(ids[&input_id]));
            row
        })
        .collect();
    Ok(SourceRows {
        additions,
        updates: modified.into_iter().collect(),
        ids,
    })
}

fn equivalent(a: &RecordValue, b: &RecordValue) -> bool {
    IDENTITY.iter().all(|field| a.get(field) == b.get(field))
}

fn widen_interval(row: &mut RecordValue, time: f64, width: f64) -> MsResult<()> {
    let known = read_number(row, "TIME")?;
    let half = read_number(row, "INTERVAL")? / 2.0;
    let low = (time - width / 2.0).min(known - half);
    let high = (time + width / 2.0).max(known + half);
    row.upsert(
        "TIME",
        Value::Scalar(ScalarValue::Float64((low + high) / 2.0)),
    );
    row.upsert("INTERVAL", Value::Scalar(ScalarValue::Float64(high - low)));
    Ok(())
}

fn read_number(row: &RecordValue, field: &str) -> MsResult<f64> {
    match row.get(field) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        _ => Err(MsError::InvalidInput(format!(
            "SOURCE {field} must be a double"
        ))),
    }
}

fn read_id(row: &RecordValue, field: &str) -> MsResult<i32> {
    match row.get(field) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        _ => Err(MsError::InvalidInput(format!(
            "SOURCE {field} must be an integer"
        ))),
    }
}

fn map_window(windows: &[usize], window: i32) -> MsResult<i32> {
    if window < 0 {
        return Ok(window);
    }
    let index = window as usize;
    let mapped = windows.get(index).ok_or_else(|| MsError::InvalidIndex {
        context: "SOURCE spectral window".into(),
        index,
        max: windows.len().saturating_sub(1),
    })?;
    i32::try_from(*mapped)
        .map_err(|_| MsError::InvalidInput("spectral window ID exceeds int32".into()))
}

fn integer(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}
