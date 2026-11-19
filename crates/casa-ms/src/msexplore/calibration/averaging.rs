// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration solution intervals use the same controls as visibility plots.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::axis::SampleCoordinates;
use crate::MsAverageSpec;

pub(super) fn options(task: &TaskInvocation<'_>) -> Result<MsAverageSpec, TaskFailure> {
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

pub(super) fn active(spec: &MsAverageSpec) -> bool {
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
        let [a, b] = row.antennas;
        if spec.avgbaseline {
            groups.entry([-1, -1]).or_default().push(row);
        } else if spec.avgantenna {
            for antenna in [a, b].into_iter().filter(|&id| id >= 0) {
                groups.entry([antenna, -1]).or_default().push(row);
            }
        } else if baseline_based {
            groups.entry([a.min(b), a.max(b)]).or_default().push(row);
        } else {
            groups.entry([a, reference_antenna]).or_default().push(row);
        }
    }
    groups
}
