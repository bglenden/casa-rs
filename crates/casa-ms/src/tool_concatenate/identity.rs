// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reconcile the MeasurementSet identities whose matching is independent of
//! spectral windows, feed mappings, and field source IDs.

use casa_tables::{Table, table_quantum::ScalarQuantColumn};
use casa_types::{RecordValue, ScalarValue, Value};

use crate::MsResult;

const OBSERVATION: &[&str] = &[
    "FLAG_ROW",
    "OBSERVER",
    "PROJECT",
    "RELEASE_DATE",
    "TELESCOPE_NAME",
    "TIME_RANGE",
];
const PROCESSOR: &[&str] = &[
    "FLAG_ROW", "MODE_ID", "TYPE", "TYPE_ID", "SUB_TYPE", "PASS_ID",
];
const STATE: &[&str] = &["OBS_MODE", "REF", "SIG", "SUB_SCAN"];

pub(super) struct IdentityRows {
    pub(super) additions: Vec<RecordValue>,
    pub(super) donor_ids: Vec<usize>,
}

/// CASA compares observations by the identity fields without comparing LOG
/// or SCHEDULE. Preserve the complete first record in each matching group.
pub(super) fn observations(target: &Table, donor: &Table) -> MsResult<IdentityRows> {
    match_rows(target, donor, |left, right| {
        equal_fields(left, right, OBSERVATION)
    })
}

/// PROCESSOR's optional PASS_ID participates when its target column exists.
pub(super) fn processors(target: &Table, donor: &Table) -> MsResult<IdentityRows> {
    let has_pass_id = target
        .schema()
        .is_some_and(|schema| schema.contains_column("PASS_ID"));
    let fields = if has_pass_id {
        PROCESSOR
    } else {
        &PROCESSOR[..PROCESSOR.len() - 1]
    };
    match_rows(target, donor, |left, right| {
        equal_fields(left, right, fields)
    })
}

pub(super) fn states(target: &Table, donor: &Table) -> MsResult<IdentityRows> {
    let target_rows = target.rows()?;
    let donor_rows = donor.rows()?;
    let mut temperatures = state_temperatures(target)?;
    let donor_temperatures = state_temperatures(donor)?;
    let mut additions = Vec::new();
    let mut donor_ids = Vec::with_capacity(donor_rows.len());
    for (incoming, incoming_k) in donor_rows.iter().zip(donor_temperatures) {
        let existing = target_rows.iter().chain(additions.iter());
        let last = temperatures.len().saturating_sub(1);
        let matched = existing
            .rev()
            .enumerate()
            .find(|(offset, row)| {
                let [cal, load] = temperatures[last - offset];
                row.get("FLAG_ROW") != Some(&Value::Scalar(ScalarValue::Bool(true)))
                    && (cal - incoming_k[0]).abs() < 1.0
                    && (load - incoming_k[1]).abs() < 1.0
                    && equal_fields(row, incoming, STATE)
            })
            .map(|(offset, _)| last - offset);
        let index = matched.unwrap_or_else(|| {
            let index = target_rows.len() + additions.len();
            additions.push(incoming.clone());
            temperatures.push(incoming_k);
            index
        });
        donor_ids.push(index);
    }
    Ok(IdentityRows {
        additions,
        donor_ids,
    })
}

fn match_rows(
    target: &Table,
    donor: &Table,
    mut equivalent: impl FnMut(&RecordValue, &RecordValue) -> bool,
) -> MsResult<IdentityRows> {
    let existing = target.rows()?;
    let incoming = donor.rows()?;
    let mut additions = Vec::new();
    let mut donor_ids = Vec::with_capacity(incoming.len());
    for row in incoming {
        let matched = existing
            .iter()
            .chain(additions.iter())
            .position(|other| equivalent(other, row));
        let index = matched.unwrap_or_else(|| {
            let index = existing.len() + additions.len();
            additions.push(row.clone());
            index
        });
        donor_ids.push(index);
    }
    Ok(IdentityRows {
        additions,
        donor_ids,
    })
}

fn state_temperatures(table: &Table) -> MsResult<Vec<[f64; 2]>> {
    if table.row_count() == 0 {
        return Ok(Vec::new());
    }
    let cal = ScalarQuantColumn::with_unit(table, "CAL", "K")?;
    let load = ScalarQuantColumn::with_unit(table, "LOAD", "K")?;
    Ok((0..table.row_count())
        .map(|row| Ok([cal.get(row)?.value(), load.get(row)?.value()]))
        .collect::<Result<_, casa_tables::TableError>>()?)
}

fn equal_fields(left: &RecordValue, right: &RecordValue, fields: &[&str]) -> bool {
    fields
        .iter()
        .all(|field| left.get(field) == right.get(field))
}
