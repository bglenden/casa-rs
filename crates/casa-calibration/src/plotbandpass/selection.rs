// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selected solution coordinates and page groups of the bandpass task.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::table::{SolutionRow, Solutions};

#[derive(Debug, Default)]
pub(crate) struct Selectors {
    pub(crate) antenna: Vec<String>,
    pub(crate) field: Vec<String>,
    pub(crate) spw: Vec<String>,
    pub(crate) time: Vec<String>,
    pub(crate) scan: Vec<String>,
    pub(crate) baseband: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Overlay {
    pub(crate) antenna: bool,
    pub(crate) time: bool,
    pub(crate) window: bool,
    pub(crate) baseband: bool,
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

pub(super) struct SelectionMismatch {
    pub(super) parameter: &'static str,
    pub(super) word: String,
    pub(super) invalid_integer: bool,
}

pub(super) struct SelectedGroups {
    pub(super) groups: BTreeMap<GroupKey, Vec<SelectedRow>>,
    pub(super) time_count: usize,
    pub(super) fields: Vec<i32>,
}

pub(super) fn groups(
    source: &Solutions,
    selectors: &Selectors,
    overlay: Overlay,
    threshold_seconds: f64,
) -> Result<SelectedGroups, SelectionMismatch> {
    let times = TimeGroups::for_source(source, threshold_seconds);
    let select = Selection::new(source, selectors, &times, overlay)?;
    let fields = source
        .rows
        .iter()
        .map(|row| row.field)
        .filter(|&id| included(&select.field, Some(id)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
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
            baseband: baseband.filter(|_| overlay.window && !overlay.baseband),
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
            let (major, minor) = if overlay.antenna && overlay.time {
                (selected.time_index, row.antenna as usize)
            } else {
                (row.antenna as usize, selected.time_index)
            };
            (row.window, major, minor)
        });
    }
    Ok(SelectedGroups {
        groups,
        time_count: times.len(),
        fields,
    })
}

impl Selection {
    fn new(
        source: &Solutions,
        selectors: &Selectors,
        times: &TimeGroups,
        overlay: Overlay,
    ) -> Result<Self, SelectionMismatch> {
        let scan = if overlay.time {
            BTreeSet::new()
        } else {
            select(
                "scans",
                &selectors.scan,
                source.rows.iter().filter_map(|row| row.scan),
                |_| "",
            )?
        };
        let offset = scan
            .iter()
            .next()
            .and_then(|id| times.scan_index(*id))
            .unwrap_or(0) as i32;
        let time = select(
            "timeranges",
            &selectors.time,
            0..times.len() as i32 - offset,
            |_| "",
        )?
        .into_iter()
        .map(|relative| relative + offset)
        .collect();
        Ok(Self {
            antenna: select(
                "antenna",
                &selectors.antenna,
                source.rows.iter().map(|row| row.antenna),
                |id| name_at(&source.antennas, id),
            )?,
            field: select(
                "field",
                &selectors.field,
                source.rows.iter().map(|row| row.field),
                |id| name_at(&source.fields, id),
            )?,
            window: select_windows(source, &selectors.spw)?,
            time,
            scan,
            baseband: select(
                "basebands",
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

fn select_windows(
    source: &Solutions,
    selectors: &[String],
) -> Result<BTreeSet<i32>, SelectionMismatch> {
    let words = selector_words(selectors).collect::<Vec<_>>();
    let inline = |word: &str, invalid_integer| SelectionMismatch {
        parameter: "spw",
        word: word.into(),
        invalid_integer,
    };
    if let Some(word) = words.iter().find(|word| word.contains('!')) {
        return Err(inline(word, false));
    }
    if words.is_empty() || words.iter().any(|word| word.contains('*')) {
        return Ok(BTreeSet::new());
    }
    let solved = source
        .rows
        .iter()
        .map(|row| row.window)
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();
    for word in words {
        let number = |value: &str| value.trim().parse::<i32>().map_err(|_| inline(value, true));
        let (start, end) = match word.split_once('~') {
            Some((start, end)) => (number(start)?, number(end)?),
            None => {
                let id = number(word)?;
                (id, id)
            }
        };
        if source.measurement_set.is_some()
            && (start < 0 || end >= source.windows.len() as i32)
            && !(start == end && solved.contains(&start))
        {
            return Err(inline(word, false));
        }
        selected.extend(solved.iter().filter(|&&id| id >= start && id <= end));
    }
    if selected.is_empty() {
        return Err(inline(&selectors.join(","), false));
    }
    Ok(selected)
}

fn selector_words(selectors: &[String]) -> impl Iterator<Item = &str> {
    selectors
        .iter()
        .flat_map(|part| part.split(',').map(str::trim))
        .filter(|part| !part.is_empty())
}

fn select<'a>(
    parameter: &'static str,
    selectors: &[String],
    identifiers: impl Iterator<Item = i32>,
    name: impl Fn(i32) -> &'a str,
) -> Result<BTreeSet<i32>, SelectionMismatch> {
    let words = selector_words(selectors).collect::<Vec<_>>();
    if parameter == "timeranges" {
        for raw in &words {
            let word = raw.strip_prefix('!').unwrap_or(raw);
            for endpoint in word.split('~') {
                if endpoint.trim().parse::<i32>().is_err() {
                    return Err(SelectionMismatch {
                        parameter,
                        word: endpoint.into(),
                        invalid_integer: true,
                    });
                }
            }
        }
    }
    if words.is_empty() || words.contains(&"*") {
        return Ok(BTreeSet::new());
    }
    let identifiers = identifiers.collect::<BTreeSet<_>>();
    let mut selected = if words.iter().any(|word| word.starts_with('!')) {
        identifiers.clone()
    } else {
        BTreeSet::new()
    };
    for raw in &words {
        let (word, exclude) = raw
            .strip_prefix('!')
            .map_or((*raw, false), |word| (word, true));
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
                if exclude {
                    selected.remove(&id);
                } else {
                    selected.insert(id);
                }
            })
            .count();
        if count == 0 {
            return Err(SelectionMismatch {
                parameter,
                word: word.into(),
                invalid_integer: false,
            });
        }
    }
    if selected.is_empty() {
        return Err(SelectionMismatch {
            parameter,
            word: words.join(","),
            invalid_integer: false,
        });
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

    fn scan_index(&self, scan: i32) -> Option<usize> {
        match self {
            Self::Scans(scans) => scans.get(&scan).copied(),
            Self::Epochs { .. } => None,
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
