// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concatenate optional weather, calibration, and Earth-orientation rows.

use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions, add_directory};
use casa_types::{RecordValue, ScalarValue, Value};

use crate::{MeasurementSet, MsError, MsResult, SubtableId};

pub(super) fn reconcile(
    target: &MeasurementSet,
    donor: &MeasurementSet,
    antenna_ids: &[usize],
    spectral_window_ids: &[usize],
    remap_spectral_windows: bool,
    observation_ids: &[usize],
) -> MsResult<Vec<(PathBuf, Vec<RecordValue>)>> {
    let mut changes = Vec::new();
    for (id, name) in [
        (Some(SubtableId::SysCal), "SYSCAL"),
        (Some(SubtableId::Weather), "WEATHER"),
        (None, "GAIN_CURVE"),
        (None, "PHASE_CAL"),
        (None, "EARTH_ORIENTATION"),
    ] {
        let Some(target_table) = linked_table(target, id, name)? else {
            continue;
        };
        if target_table.row_count() == 0 {
            continue;
        }
        let Some(donor_table) = linked_table(donor, id, name)? else {
            continue;
        };
        if donor_table.row_count() == 0 {
            continue;
        }
        let target_path = target_table.path().expect("existing subtable").to_owned();
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
        changes.push((target_path, records));
    }
    Ok(changes)
}

fn linked_table(
    ms: &MeasurementSet,
    id: Option<SubtableId>,
    name: &str,
) -> MsResult<Option<Table>> {
    let path = match id {
        Some(id) => ms.subtable(id).and_then(Table::path).map(Path::to_owned),
        None => match ms.main_table().keywords().get(name) {
            Some(Value::TableRef(value)) => {
                let main = ms.main_table().path().expect("existing MeasurementSet");
                Some(add_directory(value, main))
            }
            _ => None,
        },
    };
    path.map(|path| Table::open(TableOptions::new(path)))
        .transpose()
        .map_err(Into::into)
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
