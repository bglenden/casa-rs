// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concatenate optional weather, calibration, and Earth-orientation rows.

use std::collections::BTreeMap;

use casa_core::{RecordValue, ScalarValue, Value};
use casa_tables::Table;

use super::{TableRowsChange, main_rows::sparse_id};
use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};

pub(super) fn reconcile<'a>(
    target: &'a MeasurementSet,
    donor: &MeasurementSet,
    extra: [&'a BTreeMap<&str, Table>; 2],
    antenna_ids: &[usize],
    spectral_window_ids: &[usize],
    remap_spectral_windows: bool,
    observation_ids: [&[usize]; 2],
) -> MsResult<Vec<(&'a Table, TableRowsChange)>> {
    let [attached_observation_ids, donor_observation_ids] = observation_ids;
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
        let mut records = linked_table(donor, extra[1], id, name)
            .filter(|table| table.row_count() > 0)
            .map(|table| table.rows().map(|rows| rows.to_vec()))
            .transpose()?
            .unwrap_or_default();
        for record in &mut records {
            if name == "EARTH_ORIENTATION" {
                remap(record, name, "OBSERVATION_ID", donor_observation_ids, true)?;
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
        let updated = if name == "EARTH_ORIENTATION" && !attached_observation_ids.is_empty() {
            remap_attached_observations(target_table, attached_observation_ids)?
        } else {
            Vec::new()
        };
        if !records.is_empty() || !updated.is_empty() {
            changes.push((
                target_table,
                TableRowsChange {
                    appended: records,
                    updated,
                    ..TableRowsChange::default()
                },
            ));
        }
    }
    Ok(changes)
}

fn remap_attached_observations(
    table: &Table,
    ids: &[usize],
) -> MsResult<Vec<(usize, RecordValue)>> {
    let name = "OBSERVATION_ID";
    table
        .column_accessor(name)?
        .scalar_cells_owned()?
        .into_iter()
        .enumerate()
        .filter_map(|(row, value)| {
            let Some(ScalarValue::Int32(old)) = value else {
                return Some(Err(MsError::InvalidInput(
                    "EARTH_ORIENTATION.OBSERVATION_ID requires an integer".into(),
                )));
            };
            match sparse_id(ids, old) {
                Ok(new) if new == old => None,
                Ok(new) => Some(Ok((
                    row,
                    RecordValue::new(vec![casa_core::RecordField::new(
                        name,
                        Value::Scalar(ScalarValue::Int32(new)),
                    )]),
                ))),
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
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
