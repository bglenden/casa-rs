// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration solution intervals use the same controls as visibility plots.

use std::collections::BTreeMap;
use std::ops::Range;

use crate::{TaskFailure, TaskInvocation};

use super::axis::SampleCoordinates;
use super::selection::Selection;
use super::table_data::CalibrationTable;
use casa_plot_data::MsAverageSpec;

pub(in crate::ms::msexplore) fn options(
    task: &TaskInvocation<'_>,
) -> Result<MsAverageSpec, TaskFailure> {
    if !task.flag("averagedata")? {
        return Ok(MsAverageSpec::default());
    }
    let number = |name: &str| {
        task.text(name)?
            .filter(|text| !text.is_empty())
            .map(|text| {
                text.parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .ok_or_else(|| {
                        TaskFailure::validation(format!("{name} requires a nonnegative number"))
                    })
            })
            .transpose()
    };
    Ok(MsAverageSpec {
        avgchannel: number("avgchannel")?
            .map(|value| value as usize)
            .filter(|&count| count > 0),
        avgtime: number("avgtime")?.filter(|&seconds| seconds > 0.0),
        avgscan: task.flag("avgscan")?,
        avgfield: task.flag("avgfield")?,
        avgbaseline: task.flag("avgbaseline")?,
        avgantenna: task.flag("avgantenna")?,
        avgspw: task.flag("avgspw")?,
        scalar: task.flag("scalar")?,
    })
}

pub(in crate::ms::msexplore) fn active(spec: &MsAverageSpec) -> bool {
    spec.avgchannel.is_some()
        || spec.avgtime.is_some()
        || spec.avgbaseline
        || spec.avgantenna
        || spec.avgspw
        || spec.scalar
}

fn sort_scope(row: &SampleCoordinates, spec: &MsAverageSpec) -> [i32; 3] {
    [
        if spec.avgscan { 0 } else { row.scan },
        if spec.avgfield { 0 } else { row.field },
        if spec.avgspw { 0 } else { row.window },
    ]
}

pub(super) fn sort(rows: &mut [SampleCoordinates], spec: &MsAverageSpec) {
    rows.sort_by(|a, b| {
        sort_scope(a, spec)
            .cmp(&sort_scope(b, spec))
            .then_with(|| a.time.total_cmp(&b.time))
            .then_with(|| a.field.cmp(&b.field))
            .then_with(|| a.window.cmp(&b.window))
            .then_with(|| a.row.cmp(&b.row))
    });
}

pub(super) fn intervals(rows: &[SampleCoordinates], spec: &MsAverageSpec) -> Vec<Range<usize>> {
    let mut intervals = Vec::new();
    let mut start = 0;
    for (end, end_row) in rows.iter().enumerate().skip(1) {
        let first = &rows[start];
        let elapsed = end_row.time - first.time;
        if sort_scope(first, spec) != sort_scope(end_row, spec)
            || first.observation != end_row.observation
            || elapsed < 0.0
            || elapsed > spec.avgtime.unwrap_or_default()
        {
            intervals.push(start..end);
            start = end;
        }
    }
    if start < rows.len() {
        intervals.push(start..rows.len());
    }
    intervals
}

pub(super) fn coordinates(rows: &[SampleCoordinates]) -> SampleCoordinates {
    let mut metadata = rows[0];
    metadata.time = rows[0].time.midpoint(rows[rows.len() - 1].time);
    metadata.interval = 0.0;
    for row in &rows[1..] {
        if metadata.scan != row.scan {
            metadata.scan = -1;
        }
        if metadata.field != row.field {
            metadata.field = -1;
        }
        if metadata.window != row.window {
            metadata.window = -1;
        }
    }
    metadata
}

pub(super) fn row_groups(
    rows: &[SampleCoordinates],
    spec: &MsAverageSpec,
    baseline_based: bool,
) -> BTreeMap<[i32; 2], Vec<SampleCoordinates>> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    let reference_antenna = rows[0].antennas[1];
    for &row in rows {
        for id in antenna_groups(row, spec, baseline_based, reference_antenna)
            .into_iter()
            .flatten()
        {
            groups.entry(id).or_default().push(row);
        }
    }
    groups
}

fn antenna_groups(
    row: SampleCoordinates,
    spec: &MsAverageSpec,
    baseline_based: bool,
    reference_antenna: i32,
) -> [Option<[i32; 2]>; 2] {
    let [a, b] = row.antennas;
    if spec.avgbaseline {
        [Some([-1, -1]), None]
    } else if spec.avgantenna {
        [(a >= 0).then_some([a, -1]), (b >= 0).then_some([b, -1])]
    } else if baseline_based {
        [Some([a.min(b), a.max(b)]), None]
    } else {
        [Some([a, reference_antenna]), None]
    }
}

pub(super) fn channel_frequencies(
    table: &CalibrationTable,
    rows: &[SampleCoordinates],
    selection: &Selection,
    spec: &MsAverageSpec,
) -> Result<Vec<f64>, String> {
    let Some(width) = spec.avgchannel.filter(|&width| width > 1) else {
        return Ok(Vec::new());
    };
    let first = table.solution(rows[0])?;
    let shape = first.values.shape();
    let first_channels = selection.channels(rows[0].window, shape[1]);
    let width = width.min(first_channels.len().max(1));
    let bins = first_channels.len() / width;
    let mut frequencies = vec![(0.0, 0usize); bins];
    let mut averaged_flags = BTreeMap::<_, Vec<bool>>::new();
    for &row in rows {
        let solution = table.solution(row)?;
        if solution.values.shape() != shape {
            return Err("calibration solutions being averaged must have the same shape".into());
        }
        let window = usize::try_from(row.window)
            .ok()
            .and_then(|id| table.windows.get(id))
            .ok_or_else(|| {
                format!(
                    "calibration row {} has unknown window {}",
                    row.row, row.window
                )
            })?;
        let channels = selection.channels(row.window, shape[1]);
        let groups = antenna_groups(
            row,
            spec,
            table.subtype.contains("Mueller"),
            rows[0].antennas[1],
        );
        for receptor in 0..shape[0] {
            for (bin, members) in channels.chunks_exact(width).take(bins).enumerate() {
                for &channel in members {
                    let flagged = solution.flagged(receptor, channel);
                    let mut include = false;
                    for group in groups.into_iter().flatten() {
                        let flags = averaged_flags
                            .entry(group)
                            .or_insert_with(|| vec![true; shape[0] * bins]);
                        let entry = &mut flags[receptor * bins + bin];
                        if !flagged && *entry {
                            *entry = false;
                            frequencies[bin] = (0.0, 0);
                        }
                        include |= !flagged || *entry;
                    }
                    if include {
                        let (total, count) = &mut frequencies[bin];
                        *total += window
                            .frequencies
                            .get(channel)
                            .copied()
                            .unwrap_or(window.reference);
                        *count += 1;
                    }
                }
            }
        }
    }
    Ok(frequencies
        .into_iter()
        .map(|(sum, count)| sum / (count as f64 * 1e9))
        .collect())
}
