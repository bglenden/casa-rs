// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA sorts time bins within the selected array, field, scan and state.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::{MsError, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::{RequiredScalarColumnValues, Table};

use super::{StatWtCombine, StatWtTimeBin};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Boundary {
    array: i32,
    scan: i32,
    state: i32,
    field: i32,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Baseline {
    data_description: i32,
    antenna1: i32,
    antenna2: i32,
}

pub(super) struct Group<'a> {
    pub(super) rows: Vec<&'a ResolvedMsSelectionRow>,
    timestamp_indexes: Vec<usize>,
}

impl Group<'_> {
    pub(super) fn window(&self, center: usize, time_bin: StatWtTimeBin) -> Range<usize> {
        match time_bin {
            StatWtTimeBin::Timestamps(count) => {
                let center = self.timestamp_indexes[center];
                let start = center.saturating_sub(count / 2);
                let end = center.saturating_add(count / 2);
                self.timestamp_indexes.partition_point(|&slot| slot < start)
                    ..self.timestamp_indexes.partition_point(|&slot| slot <= end)
            }
            StatWtTimeBin::Seconds(width) => {
                let center = self.rows[center].time_mjd_seconds;
                let half = width / 2.0;
                self.rows
                    .partition_point(|row| row.time_mjd_seconds < center - half)
                    ..self
                        .rows
                        .partition_point(|row| row.time_mjd_seconds <= center + half)
            }
        }
    }
}

pub(super) fn groups<'a>(
    table: &Table,
    selection: &'a ResolvedMsSelection,
    combine: StatWtCombine,
    time_bin: StatWtTimeBin,
    sliding: bool,
) -> MsResult<Vec<Group<'a>>> {
    let rows = selection.row_indices().collect::<Vec<_>>();
    let mut metadata = table
        .required_scalar_columns_owned_for_rows(&["ARRAY_ID", "SCAN_NUMBER", "STATE_ID"], &rows)?;
    let mut take_column = |name: &str| match metadata.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(MsError::InvalidInput(format!("{name} must be int32"))),
    };
    let arrays = take_column("ARRAY_ID")?;
    let scans = take_column("SCAN_NUMBER")?;
    let states = take_column("STATE_ID")?;
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for (index, row) in selection.selected_rows.iter().enumerate() {
        let key = Boundary {
            array: arrays[index],
            scan: if combine.scans { 0 } else { scans[index] },
            state: if combine.states { 0 } else { states[index] },
            field: if combine.fields { 0 } else { row.field_id },
        };
        groups.entry(key).or_default().push(row);
    }
    let mut ordered = Vec::new();
    for rows in groups.values_mut() {
        rows.sort_unstable_by(|left, right| {
            left.time_mjd_seconds
                .total_cmp(&right.time_mjd_seconds)
                .then_with(|| left.row_index.cmp(&right.row_index))
        });
        baseline_groups(rows, time_bin, sliding, &mut ordered);
    }
    ordered.sort_unstable_by_key(|group| group.rows[0].row_index);
    Ok(ordered)
}

fn baseline_groups<'a>(
    rows: &[&'a ResolvedMsSelectionRow],
    time_bin: StatWtTimeBin,
    sliding: bool,
    output: &mut Vec<Group<'a>>,
) {
    let mut groups = BTreeMap::<_, Group<'_>>::new();
    let mut timestamp = 0;
    let mut previous = rows[0].time_mjd_seconds;
    let mut start = previous;
    for &row in rows {
        let time = row.time_mjd_seconds;
        if time != previous {
            timestamp += 1;
            previous = time;
            let next_bin = match time_bin {
                StatWtTimeBin::Timestamps(count) => timestamp == count,
                StatWtTimeBin::Seconds(width) => time - start >= width,
            };
            if next_bin && !sliding {
                output.extend(std::mem::take(&mut groups).into_values());
                timestamp = 0;
                start = time;
            }
        }
        let key = Baseline {
            data_description: row.data_desc_id,
            antenna1: row.antenna1_id.min(row.antenna2_id),
            antenna2: row.antenna1_id.max(row.antenna2_id),
        };
        let group = groups.entry(key).or_insert_with(|| Group {
            rows: vec![],
            timestamp_indexes: vec![],
        });
        group.rows.push(row);
        if sliding {
            group.timestamp_indexes.push(timestamp);
        }
    }
    output.extend(groups.into_values());
}
