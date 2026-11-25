// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concatenate optional weather, calibration, and Earth-orientation rows.

use std::collections::BTreeMap;

use casa_tables::Table;
use casa_types::{RecordValue, ScalarValue, Value};

use crate::{MeasurementSet, MsError, MsResult, SubtableId};

pub(super) fn reconcile<'a>(
    target: &'a MeasurementSet,
    donor: &MeasurementSet,
    extra: [&'a BTreeMap<&str, Table>; 2],
    antenna_ids: &[usize],
    spectral_window_ids: &[usize],
    remap_spectral_windows: bool,
    observation_ids: &[usize],
) -> MsResult<Vec<(&'a Table, Vec<RecordValue>)>> {
    let mut changes = Vec::new();
    for (id, name) in [
        (Some(SubtableId::SysCal), "SYSCAL"),
        (Some(SubtableId::Weather), "WEATHER"),
        (None, "GAIN_CURVE"),
        (None, "PHASE_CAL"),
        (None, "EARTH_ORIENTATION"),
    ] {
        let Some(target_table) = linked_table(target, extra[0], id, name) else {
            continue;
        };
        if target_table.row_count() == 0 {
            continue;
        }
        let Some(donor_table) = linked_table(donor, extra[1], id, name) else {
            continue;
        };
        if donor_table.row_count() == 0 {
            continue;
        }
        let mut records = donor_table.rows()?.to_vec();
        for record in &mut records {
            if name == "EARTH_ORIENTATION" {
                remap(record, name, "OBSERVATION_ID", observation_ids, true)?;
                continue;
            }
            remap(record, name, "ANTENNA_ID", antenna_ids, name == "WEATHER")?;
            if remap_spectral_windows && name != "WEATHER" {
                remap(
                    record,
                    name,
                    "SPECTRAL_WINDOW_ID",
                    spectral_window_ids,
                    false,
                )?;
            }
        }
        changes.push((target_table, records));
    }
    Ok(changes)
}

fn linked_table<'a>(
    ms: &'a MeasurementSet,
    extra: &'a BTreeMap<&str, Table>,
    id: Option<SubtableId>,
    name: &str,
) -> Option<&'a Table> {
    match id {
        Some(id) => ms.subtable(id),
        None => extra.get(name),
    }
}

fn remap(
    row: &mut RecordValue,
    table: &str,
    column: &str,
    ids: &[usize],
    allow_undefined: bool,
) -> MsResult<()> {
    let Some(Value::Scalar(ScalarValue::Int32(index))) = row.get(column) else {
        return Err(MsError::InvalidInput(format!(
            "{table}.{column} requires an integer"
        )));
    };
    if *index == -1 && allow_undefined {
        return Ok(());
    }
    let replacement = usize::try_from(*index)
        .ok()
        .and_then(|index| ids.get(index))
        .and_then(|&id| i32::try_from(id).ok())
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "{table}.{column} contains invalid donor ID {index}"
            ))
        })?;
    row.upsert(column, Value::Scalar(ScalarValue::Int32(replacement)));
    Ok(())
}
