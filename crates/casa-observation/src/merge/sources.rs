// SPDX-License-Identifier: LGPL-3.0-or-later
//! Merging logical sources as `MSConcat` merges `SOURCE` rows
//! (`copySource`, `updateSource`, `updateSource2`), each record a row.
//!
//! The incoming sources are added under new IDs, their records in the
//! merged windows. Then, over every record of the merged table:
//! 1. a record equivalent to an earlier one of the same window is removed,
//!    the fields of its source sent to the earlier one's source, and the
//!    earlier record's validity widened to span both;
//! 2. a record equivalent to an earlier one of another source (in another
//!    window, since step 1 removed those of the same) takes that source,
//!    and the fields of its own source follow;
//! 3. when an incoming field matched a merged one, a record whose source no
//!    field names takes the source of an equivalent record of another
//!    window and source, compared on direction but not lines (which differ
//!    by window), or is removed if there is none.
//!
//! As in CASA, steps 1 and 2 send fields by source while records keep
//! theirs: a source whose fields went elsewhere keeps its other records,
//! which step 3 then joins to an equivalent source or removes.
//!
//! Equivalence compares every member but the source, window, time and
//! validity, each stated member equal; CASA's `sourceRowsEquivalent`
//! instead lets its last optional column decide. A merged record is
//! centred on the union of the two validities, where CASA widens the
//! interval but leaves `TIME` unchanged. Sources are numbered in the order
//! they first appear, where CASA's renumbering can split a source of
//! several records, and every field, not only those appended, names its
//! merged source. Each is recorded in `docs/CASA (C++) bugs.md`.

use casa_types::measures::MEpoch;

use super::Merged;
use crate::{Catalogue, ObservationError, SourceEntry, SourceId, SourceRecord, SourceWindow};

/// One record of the merged table, as a `SOURCE` row: its source and name.
#[derive(Clone, Debug)]
struct Row {
    source: usize,
    name: String,
    record: SourceRecord,
}

/// What an equivalence compares beyond the members always compared.
#[derive(Clone, Copy)]
struct Compared {
    /// The direction and position.
    direction: bool,
    /// The transitions and rest frequencies.
    lines: bool,
}

/// Whether two optional members are equal by `same`, each stated or both
/// not.
fn equal_options<T>(a: &Option<T>, b: &Option<T>, same: impl Fn(&T, &T) -> bool) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => same(a, b),
        (None, None) => true,
        _ => false,
    }
}

/// Whether two required members are equal by `same`: both stated, since an
/// unstated value is equal to nothing.
fn equal_stated<T>(a: &Option<T>, b: &Option<T>, same: impl Fn(&T, &T) -> bool) -> bool {
    a.as_ref().zip(b.as_ref()).is_some_and(|(a, b)| same(a, b))
}

/// Whether two rows are one record of one source, but for their source,
/// window, time and validity.
fn equivalent(a: &Row, b: &Row, compared: Compared) -> bool {
    let (x, y) = (&a.record, &b.record);
    let directions = |p: &casa_types::measures::direction::MDirection,
                      q: &casa_types::measures::direction::MDirection| {
        p.refer() == q.refer() && p.cosines() == q.cosines()
    };
    a.name == b.name
        && equal_stated(&x.calibration_group, &y.calibration_group, PartialEq::eq)
        && equal_stated(&x.code, &y.code, PartialEq::eq)
        && x.num_lines == y.num_lines
        && equal_stated(
            &x.proper_motion_rad_s,
            &y.proper_motion_rad_s,
            PartialEq::eq,
        )
        && (!compared.direction
            || (equal_stated(&x.direction, &y.direction, directions)
                && equal_options(&x.position, &y.position, |p, q| {
                    p.refer() == q.refer() && p.values() == q.values()
                })))
        && equal_options(&x.pulsar_id, &y.pulsar_id, PartialEq::eq)
        && equal_options(
            &x.systemic_velocities,
            &y.systemic_velocities,
            PartialEq::eq,
        )
        && (!compared.lines
            || (equal_options(&x.transitions, &y.transitions, PartialEq::eq)
                && equal_options(&x.rest_frequencies, &y.rest_frequencies, |p, q| {
                    p.len() == q.len()
                        && p.iter()
                            .zip(q)
                            .all(|(p, q)| p.refer() == q.refer() && p.hz() == q.hz())
                })))
}

/// Widen `kept`'s validity to span `removed`'s too, centred on the union.
/// Records that do not state both are left as they are.
fn widen(kept: &mut SourceRecord, removed: &SourceRecord) {
    let span = |record: &SourceRecord| {
        let time = record.time.as_ref()?;
        let interval = record.interval_s?;
        let value = time.value();
        let centre = value.day() * 86_400.0 + value.frac() * 86_400.0;
        Some((
            centre - interval / 2.0,
            centre + interval / 2.0,
            time.refer(),
        ))
    };
    let (Some(a), Some(b)) = (span(kept), span(removed)) else {
        return;
    };
    if a.2 != b.2 {
        return;
    }
    let (low, high) = (a.0.min(b.0), a.1.max(b.1));
    kept.time = Some(MEpoch::from_mjd_seconds((low + high) / 2.0, a.2));
    kept.interval_s = Some(high - low);
}

/// Merge `catalogue`'s sources into `merged`, whose fields from `before` on
/// are the input's appended ones, still naming its sources; `matched` says
/// whether any of its fields matched a merged one. The input's map, the
/// last of `merged.inputs`, gains its sources and gives its windows.
pub(super) fn append(
    merged: &mut Merged,
    catalogue: &Catalogue,
    (before, matched): (usize, bool),
) -> Result<(), ObservationError> {
    let windows = merged
        .inputs
        .last()
        .map(|map| map.windows.clone())
        .unwrap_or_default();
    let existing = merged.sources.len();
    let mut rows = rows(&merged.sources, 0, Some);
    rows.extend(self::rows(&catalogue.sources, existing, |window| {
        windows.get(window.0 as usize).copied()
    }));
    let count = existing + catalogue.sources.len();
    for (index, field) in merged.fields.iter_mut().enumerate() {
        if index >= before {
            field.source = field
                .source
                .map(|id| SourceId((existing + id.0 as usize) as u32));
        }
    }
    // Where the fields of each source (existing, then incoming) go.
    let mut sent: Vec<usize> = (0..count).collect();
    redundant(&mut rows, &mut sent);
    unify(&mut rows, &mut sent);
    if matched {
        let mut named = vec![false; count];
        for source in merged.fields.iter().filter_map(|field| field.source) {
            named[sent[source.0 as usize]] = true;
        }
        stray(&mut rows, &named);
    }
    regroup(merged, rows, &sent, existing);
    Ok(())
}

/// The rows of `sources`, numbered from `first`, each record's window as
/// `window` maps it (`None` for one the merge has not written).
fn rows(
    sources: &[SourceEntry],
    first: usize,
    window: impl Fn(crate::WindowId) -> Option<crate::WindowId>,
) -> Vec<Row> {
    let mut rows = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        for record in &source.records {
            let mut record = record.clone();
            if let SourceWindow::Window(id) = record.window {
                match window(id) {
                    Some(id) => record.window = SourceWindow::Window(id),
                    None => continue,
                }
            }
            rows.push(Row {
                source: first + index,
                name: source.name.clone(),
                record,
            });
        }
    }
    rows
}

/// Step 1: a record equivalent to an earlier one of its window removed, the
/// fields of its source sent to the earlier one's (the last such record
/// deciding, as CASA's map is overwritten).
fn redundant(rows: &mut Vec<Row>, sent: &mut [usize]) {
    let every = Compared {
        direction: true,
        lines: true,
    };
    let mut kept: Vec<Row> = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let earlier = kept.iter().position(|known| {
            known.record.window == row.record.window && equivalent(known, &row, every)
        });
        match earlier {
            Some(index) => {
                widen(&mut kept[index].record, &row.record);
                sent[row.source] = kept[index].source;
            }
            None => kept.push(row),
        }
    }
    *rows = kept;
}

/// Step 2: a record equivalent to an earlier one of another source takes
/// that source, and the fields sent to its own source go to that one.
fn unify(rows: &mut [Row], sent: &mut [usize]) {
    let every = Compared {
        direction: true,
        lines: true,
    };
    for k in 1..rows.len() {
        let earlier = (0..k)
            .find(|j| rows[*j].source != rows[k].source && equivalent(&rows[*j], &rows[k], every));
        if let Some(j) = earlier {
            let (from, to) = (rows[k].source, rows[j].source);
            for target in sent.iter_mut() {
                if *target == from {
                    *target = to;
                }
            }
            rows[k].source = to;
        }
    }
}

/// Step 3: a record of a source no field names (`named`, by source) takes
/// the source of an equivalent record of another window and source,
/// compared on direction but not lines, or is removed.
fn stray(rows: &mut Vec<Row>, named: &[bool]) {
    let on_direction = Compared {
        direction: true,
        lines: false,
    };
    let mut index = 0;
    while index < rows.len() {
        if named[rows[index].source] {
            index += 1;
            continue;
        }
        let other = (0..rows.len()).find(|k| {
            rows[*k].record.window != rows[index].record.window
                && rows[*k].source != rows[index].source
                && equivalent(&rows[*k], &rows[index], on_direction)
        });
        match other {
            Some(k) => {
                rows[index].source = rows[k].source;
                index += 1;
            }
            None => {
                rows.remove(index);
            }
        }
    }
}

/// The merged sources from `rows`, numbered in the order they first appear,
/// each with its records in row order; every field and every input's map
/// names its source's new number, through `sent` for those of the input
/// (numbered from `existing`) and those before it.
fn regroup(merged: &mut Merged, rows: Vec<Row>, sent: &[usize], existing: usize) {
    let mut numbered: Vec<Option<usize>> = vec![None; sent.len()];
    let mut sources: Vec<SourceEntry> = Vec::new();
    for row in rows {
        let number = *numbered[row.source].get_or_insert_with(|| {
            sources.push(SourceEntry {
                name: row.name.clone(),
                records: Vec::new(),
            });
            sources.len() - 1
        });
        sources[number].records.push(row.record);
    }
    let to = |source: usize| numbered[sent[source]].map(|number| SourceId(number as u32));
    for field in &mut merged.fields {
        field.source = field.source.and_then(|id| to(id.0 as usize));
    }
    let incoming = sent.len() - existing;
    let (input, earlier) = merged.inputs.split_last_mut().expect("this input's map");
    for map in earlier {
        for id in &mut map.sources {
            *id = id.and_then(|id| to(id.0 as usize));
        }
    }
    input.sources = (0..incoming).map(|index| to(existing + index)).collect();
    merged.sources = sources;
}
