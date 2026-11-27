// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-iterator chunk boundaries and per-baseline time windows.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::{MsResult, ResolvedMsSelectionRow};

use super::{StatWtCombine, StatWtMs, StatWtTimeBin};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Boundary {
    scan: i32,
    state: i32,
    field: i32,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Baseline {
    antenna1: i32,
    antenna2: i32,
}

pub(super) struct Group<'a> {
    pub(super) rows: Vec<&'a ResolvedMsSelectionRow>,
    pub(super) exposures: Vec<f64>,
    timestamp_indexes: Vec<usize>,
    timestamp_count: usize,
}

impl Group<'_> {
    pub(super) fn window(
        &self,
        center: usize,
        time_bin: StatWtTimeBin,
        sliding: bool,
    ) -> Range<usize> {
        match time_bin {
            StatWtTimeBin::Timestamps(count) => {
                let center = self.timestamp_indexes[center];
                let start = if sliding {
                    center.saturating_sub((count - 1) / 2)
                } else {
                    (center / count) * count
                };
                let start = start.min(self.timestamp_count.saturating_sub(count));
                let end = start.saturating_add(count);
                self.timestamp_indexes.partition_point(|&slot| slot < start)
                    ..self.timestamp_indexes.partition_point(|&slot| slot < end)
            }
            StatWtTimeBin::Seconds(_) if !sliding => 0..self.rows.len(),
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
    ms: &StatWtMs,
    selection: &'a [ResolvedMsSelectionRow],
    combine: StatWtCombine,
    time_bin: StatWtTimeBin,
    sliding: bool,
) -> MsResult<Vec<Group<'a>>> {
    let metadata = ms.row_metadata(selection)?;
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for (row, metadata) in selection.iter().zip(metadata) {
        let key = Boundary {
            scan: if combine.scans { 0 } else { metadata.scan },
            state: if combine.states { 0 } else { metadata.state },
            field: if combine.fields { 0 } else { row.field_id },
        };
        groups
            .entry((metadata.array, row.data_desc_id))
            .or_default()
            .push((row, key, metadata.exposure));
    }
    let mut ordered = Vec::new();
    for rows in groups.values_mut() {
        rows.sort_unstable_by(|left, right| {
            left.0
                .time_mjd_seconds
                .total_cmp(&right.0.time_mjd_seconds)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.0.row_index.cmp(&right.0.row_index))
        });
        let mut start = 0;
        for index in 1..rows.len() {
            let boundary_changed = rows[index].1 != rows[start].1;
            let start_time = rows[start].0.time_mjd_seconds;
            let time = rows[index].0.time_mjd_seconds;
            // casacore's MSInterval anchors the comparison 10 ms before
            // the first timestamp and resets that anchor for each chunk.
            let anchor = start_time - 0.01;
            let duration_elapsed = matches!(time_bin, StatWtTimeBin::Seconds(width)
                if !sliding && ((time - anchor) / width).floor()
                    > ((start_time - anchor) / width).floor());
            let single_timestamp_bin_elapsed =
                matches!(time_bin, StatWtTimeBin::Timestamps(1)) && time != start_time;
            if boundary_changed || duration_elapsed || single_timestamp_bin_elapsed {
                baseline_groups(&rows[start..index], &mut ordered);
                start = index;
            }
        }
        baseline_groups(&rows[start..], &mut ordered);
    }
    ordered.sort_unstable_by_key(|group| group.rows[0].row_index);
    Ok(ordered)
}

fn baseline_groups<'a>(
    rows: &[(&'a ResolvedMsSelectionRow, Boundary, f64)],
    output: &mut Vec<Group<'a>>,
) {
    let mut groups = BTreeMap::<_, Group<'_>>::new();
    let mut timestamp = 0;
    let mut previous = rows[0].0.time_mjd_seconds;
    for &(row, _, exposure) in rows {
        let time = row.time_mjd_seconds;
        if time != previous {
            timestamp += 1;
            previous = time;
        }
        let key = Baseline {
            antenna1: row.antenna1_id.min(row.antenna2_id),
            antenna2: row.antenna1_id.max(row.antenna2_id),
        };
        let group = groups.entry(key).or_insert_with(|| Group {
            rows: vec![],
            exposures: vec![],
            timestamp_indexes: vec![],
            timestamp_count: 0,
        });
        group.rows.push(row);
        group.exposures.push(exposure);
        group.timestamp_indexes.push(timestamp);
    }
    output.extend(groups.into_values().map(|mut group| {
        group.timestamp_count = timestamp + 1;
        group
    }));
}
