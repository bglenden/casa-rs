// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selected solution coordinates and page groups of the bandpass task.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::table::{SolutionRow, Solutions};

#[derive(Debug, Default)]
pub(crate) struct Selectors {
    pub(super) antenna: Vec<String>,
    pub(super) field: Vec<String>,
    pub(super) spw: Vec<String>,
    pub(super) time: Vec<String>,
    pub(super) scan: Vec<String>,
    pub(super) baseband: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Overlay {
    pub(super) antenna: bool,
    pub(super) time: bool,
    pub(super) window: bool,
    pub(super) baseband: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SelectedRow {
    pub(super) row: usize,
    pub(super) time_index: usize,
}

#[derive(Clone, Copy, Debug, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub(super) struct GroupKey {
    pub(super) antenna: Option<i32>,
    pub(super) window: Option<i32>,
    pub(super) baseband: Option<i32>,
    pub(super) time_index: Option<usize>,
}

struct Selection {
    antenna: BTreeSet<i32>,
    field: BTreeSet<i32>,
    window: BTreeSet<i32>,
    time: BTreeSet<i32>,
    scan: BTreeSet<i32>,
    baseband: BTreeSet<i32>,
}

pub(super) fn groups(
    source: &Solutions,
    selectors: &Selectors,
    overlay: Overlay,
    threshold_seconds: f64,
) -> Result<BTreeMap<GroupKey, Vec<SelectedRow>>, String> {
    let times = TimeGroups::for_source(source, threshold_seconds);
    let select = Selection::new(source, selectors, times.len())?;
    let mut groups: BTreeMap<GroupKey, Vec<SelectedRow>> = BTreeMap::new();
    let mut first_rows = HashSet::new();
    for (row_index, row) in source.rows.iter().enumerate() {
        let Some(time_index) = times.group(row) else {
            continue;
        };
        let baseband = usize::try_from(row.window)
            .ok()
            .and_then(|window| source.windows.get(window))
            .and_then(|window| window.baseband);
        if !select.includes(row, time_index, baseband) {
            continue;
        }
        let key = GroupKey {
            antenna: (!overlay.antenna).then_some(row.antenna),
            window: (!overlay.window && !overlay.baseband).then_some(row.window),
            baseband: baseband.filter(|_| overlay.baseband),
            time_index: (!overlay.time).then_some(time_index),
        };
        // Rows in one scan group can have duplicate solutions.
        if first_rows.insert((key, row.antenna, row.window, time_index)) {
            groups.entry(key).or_default().push(SelectedRow {
                row: row_index,
                time_index,
            });
        }
    }
    for group in groups.values_mut() {
        group.sort_by_key(|selected| {
            let row = source.rows[selected.row];
            (row.window, row.antenna, selected.time_index)
        });
    }
    Ok(groups)
}

impl Selection {
    fn new(source: &Solutions, selectors: &Selectors, times: usize) -> Result<Self, String> {
        Ok(Self {
            antenna: select(
                &selectors.antenna,
                source.rows.iter().map(|row| row.antenna),
                |id| name_at(&source.antennas, id),
            )?,
            field: select(
                &selectors.field,
                source.rows.iter().map(|row| row.field),
                |id| name_at(&source.fields, id),
            )?,
            window: select(
                &selectors.spw,
                source.rows.iter().map(|row| row.window),
                |id| {
                    usize::try_from(id)
                        .ok()
                        .and_then(|id| source.windows.get(id))
                        .map_or("", |window| &window.name)
                },
            )?,
            time: select(&selectors.time, (0..times).map(|time| time as i32), |_| "")?,
            scan: select(
                &selectors.scan,
                source.rows.iter().filter_map(|row| row.scan),
                |_| "",
            )?,
            baseband: select(
                &selectors.baseband,
                source.windows.iter().filter_map(|window| window.baseband),
                |_| "",
            )?,
        })
    }

    fn includes(&self, row: &SolutionRow, time: usize, baseband: Option<i32>) -> bool {
        included(&self.antenna, Some(row.antenna))
            && included(&self.field, Some(row.field))
            && included(&self.window, Some(row.window))
            && included(&self.time, i32::try_from(time).ok())
            && included(&self.scan, row.scan)
            && included(&self.baseband, baseband)
    }
}

fn included(selected: &BTreeSet<i32>, value: Option<i32>) -> bool {
    selected.is_empty() || value.is_some_and(|id| selected.contains(&id))
}

fn name_at(names: &[String], id: i32) -> &str {
    usize::try_from(id)
        .ok()
        .and_then(|id| names.get(id))
        .map_or("", String::as_str)
}

fn select<'a>(
    selectors: &[String],
    identifiers: impl Iterator<Item = i32>,
    name: impl Fn(i32) -> &'a str,
) -> Result<BTreeSet<i32>, String> {
    let words = selectors
        .iter()
        .flat_map(|part| part.split(',').map(str::trim))
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if words.is_empty() || words.contains(&"*") {
        return Ok(BTreeSet::new());
    }
    let identifiers = identifiers.collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();
    for word in words {
        let matches = identifiers.iter().copied().filter(|&id| {
            if let Ok(number) = word.parse::<i32>() {
                return id == number;
            }
            if let Some((start, stop)) = word.split_once('~')
                && let (Ok(start), Ok(stop)) =
                    (start.trim().parse::<i32>(), stop.trim().parse::<i32>())
            {
                return (start..=stop).contains(&id);
            }
            let stored = name(id);
            word.strip_suffix('*')
                .map_or(stored == word, |prefix| stored.starts_with(prefix))
        });
        let count = matches
            .inspect(|&id| {
                selected.insert(id);
            })
            .count();
        if count == 0 {
            return Err(format!("selection {word:?} matches no bandpass solution"));
        }
    }
    Ok(selected)
}

enum TimeGroups {
    Scans(BTreeMap<i32, usize>),
    Epochs { times: Vec<f64>, threshold: f64 },
}

impl TimeGroups {
    fn for_source(source: &Solutions, threshold: f64) -> Self {
        if source.subtype != "SDSKY_PS" && source.rows.iter().all(|row| row.scan.is_some()) {
            let scans = source
                .rows
                .iter()
                .filter_map(|row| row.scan)
                .collect::<BTreeSet<_>>();
            return Self::Scans(
                scans
                    .into_iter()
                    .enumerate()
                    .map(|(index, scan)| (scan, index))
                    .collect(),
            );
        }
        let mut times = source.rows.iter().map(|row| row.time).collect::<Vec<_>>();
        times.sort_unstable_by(f64::total_cmp);
        times.dedup();
        let mut groups = Vec::new();
        for time in times {
            if groups
                .last()
                .is_none_or(|last: &f64| (time - *last).abs() >= threshold)
            {
                groups.push(time);
            }
        }
        Self::Epochs {
            times: groups,
            threshold,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Scans(groups) => groups.len(),
            Self::Epochs { times, .. } => times.len(),
        }
    }

    fn group(&self, row: &SolutionRow) -> Option<usize> {
        match self {
            Self::Scans(groups) => row.scan.and_then(|scan| groups.get(&scan)).copied(),
            Self::Epochs { times, threshold } => times
                .iter()
                .position(|time| row.time == *time || (row.time - time).abs() < *threshold),
        }
    }
}
