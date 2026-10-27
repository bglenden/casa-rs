// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific-group fragments of a time-ordered traversal, shared by the
//! MSv2 and MSv4 adapters: the group of every time of a partition's grid,
//! keyed as production `gaincal` keys solutions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;

use casa_observation::{
    FieldId, FragmentPosition, GroupFragment, GroupKey, Grouping, ObservationError, PartitionId,
    ScanId, TimeRuns,
};

use super::GroupClosure as Closure;

/// The group of every time and where each group starts and ends, computed
/// once at binding from the grid's times, fields and scans: the traversal's
/// group index. It holds a 32-byte key per time and an extent per group;
/// the trial accepts it in exchange for proven closure at delivery.
///
/// Keys are those of the whole grid, so traversals of different time and
/// field selections agree on every time's group; each group's extent is the
/// part of it the traversal's selection holds, so fragments are positioned
/// and closed within the selection and marked `clipped` when it cuts the
/// group (a time range, or a field selection that a group combining fields
/// spans).
#[derive(Debug)]
pub(super) struct GroupIndex {
    keys: Vec<GroupKey>,
    /// Each group's first time within the selection and the time past its
    /// last.
    extent: BTreeMap<GroupKey, Range<usize>>,
    /// The groups the selection cuts: empty for the whole grid.
    clipped: BTreeSet<GroupKey>,
    closure: Closure,
    /// Groups whose last time was delivered under deferred closure, still
    /// to be announced.
    pending_ends: VecDeque<GroupKey>,
}

/// The group of each of the grid times `times` (seconds, ascending) with
/// `fields` and `scans` (when the store names scans) at each, keyed as
/// production `gaincal` keys solutions over the whole grid; or `None` when
/// `grouping` asks for no groups.
pub(super) fn group_keys(
    partition: PartitionId,
    grouping: Grouping,
    times: &[f64],
    fields: &[FieldId],
    scans: Option<&[ScanId]>,
) -> Result<Option<Vec<GroupKey>>, ObservationError> {
    let keyed_by_scan = match grouping {
        Grouping::None => return Ok(None),
        Grouping::TimeBins { combine_scans, .. } => !combine_scans,
        Grouping::PerScan { .. } | Grouping::PerIntegration => true,
    };
    if fields.len() != times.len() || scans.is_some_and(|scans| scans.len() != times.len()) {
        return Err(ObservationError::InvalidStore {
            reason: "the grid has not one field and scan per time".to_string(),
        });
    }
    if keyed_by_scan && scans.is_none() {
        return Err(ObservationError::InvalidRequest {
            reason: "the grouping keys by scan, and the store names none".to_string(),
        });
    }
    if let Grouping::TimeBins { width_s, .. } = grouping
        && width_s.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater)
    {
        return Err(ObservationError::InvalidRequest {
            reason: format!("time-bin width {width_s} s is not positive"),
        });
    }
    if times.windows(2).any(|pair| pair[1] < pair[0]) {
        return Err(ObservationError::InvalidStore {
            reason: "the grid's times are not ascending".to_string(),
        });
    }
    // Production gaincal's rule: each (field, scan) key's bins count from
    // its first time; each key's integrations count from zero.
    let mut origins = BTreeMap::<(Option<FieldId>, Option<ScanId>), (f64, u64)>::new();
    let mut keys = Vec::with_capacity(times.len());
    for (unit, (&time, &field)) in times.iter().zip(fields).enumerate() {
        // Present whenever the grouping keys by it, checked above.
        let scan = scans.map(|scans| scans[unit]).filter(|_| keyed_by_scan);
        let field = match grouping {
            Grouping::None => unreachable!("handled above"),
            Grouping::TimeBins { combine_fields, .. } | Grouping::PerScan { combine_fields } => {
                (!combine_fields).then_some(field)
            }
            Grouping::PerIntegration => Some(field),
        };
        let (origin, count) = origins.entry((field, scan)).or_insert((time, 0));
        let bin = match grouping {
            Grouping::TimeBins { width_s, .. } => {
                ((time - *origin) / width_s).floor().max(0.0) as u64
            }
            Grouping::PerScan { .. } => 0,
            Grouping::PerIntegration => {
                let bin = *count;
                *count += 1;
                bin
            }
            Grouping::None => unreachable!("handled above"),
        };
        keys.push(GroupKey {
            partition,
            field,
            scan,
            bin,
        });
    }
    Ok(Some(keys))
}

impl GroupIndex {
    /// Index the grid times `times` (seconds, ascending) with `fields` and
    /// `scans` (when the store names scans) at each, for a traversal of the
    /// grid times `selection`; or `None` when the request asks for no
    /// groups.
    pub(super) fn new(
        partition: PartitionId,
        grouping: Grouping,
        closure: Closure,
        times: &[f64],
        fields: &[FieldId],
        scans: Option<&[ScanId]>,
        selection: &TimeRuns,
    ) -> Result<Option<Self>, ObservationError> {
        let Some(keys) = group_keys(partition, grouping, times, fields, scans)? else {
            return Ok(None);
        };
        let mut extent = BTreeMap::<GroupKey, Range<usize>>::new();
        for run in selection.runs() {
            for (unit, key) in keys.iter().enumerate().take(run.end).skip(run.start) {
                extent
                    .entry(*key)
                    .and_modify(|range| range.end = unit + 1)
                    .or_insert(unit..unit + 1);
            }
        }
        // The selection cuts a group it holds that has a time outside it.
        let mut clipped = BTreeSet::new();
        let mut held = selection.runs().iter().peekable();
        for (unit, key) in keys.iter().enumerate() {
            while held.next_if(|run| run.end <= unit).is_some() {}
            let selected = held.peek().is_some_and(|run| run.start <= unit);
            if !selected && extent.contains_key(key) {
                clipped.insert(*key);
            }
        }
        Ok(Some(Self {
            keys,
            extent,
            clipped,
            closure,
            pending_ends: VecDeque::new(),
        }))
    }

    /// The next group announcement owed before further data, if any.
    pub(super) fn pop_end(&mut self) -> Option<GroupKey> {
        self.pending_ends.pop_front()
    }

    /// Append the fragments of times `times` to `fragments`, with sample
    /// ranges of `samples_per_time` samples per time, counted from the
    /// batch's first time.
    pub(super) fn fragments(
        &mut self,
        times: Range<usize>,
        samples_per_time: usize,
        fragments: &mut Vec<GroupFragment>,
    ) {
        let mut start = times.start;
        while start < times.end {
            let key = self.keys[start];
            let mut end = start + 1;
            while end < times.end && self.keys[end] == key {
                end += 1;
            }
            let extent = &self.extent[&key];
            let first = start == extent.start;
            let last = end == extent.end;
            let position = match (first, last, self.closure) {
                (true, true, Closure::AtDelivery) => FragmentPosition::Whole,
                (true, _, _) => FragmentPosition::First,
                (false, true, Closure::AtDelivery) => FragmentPosition::Last,
                (false, _, _) => FragmentPosition::Middle,
            };
            if last && self.closure == Closure::Deferred {
                self.pending_ends.push_back(key);
            }
            fragments.push(GroupFragment {
                key,
                samples: (start - times.start) * samples_per_time
                    ..(end - times.start) * samples_per_time,
                position,
                clipped: self.clipped.contains(&key),
            });
            start = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(grouping: Grouping) -> GroupIndex {
        // Two fields alternating, one scan then another: field A, A, B, B,
        // A in scan 1, then B, B in scan 2, ten seconds apart.
        let times: Vec<f64> = (0..7).map(|t| f64::from(t) * 10.0).collect();
        let fields = [0, 0, 1, 1, 0, 1, 1].map(FieldId);
        let scans = [0, 0, 0, 0, 0, 1, 1].map(ScanId);
        GroupIndex::new(
            PartitionId(0),
            grouping,
            Closure::AtDelivery,
            &times,
            &fields,
            Some(&scans),
            &TimeRuns::whole(0..7),
        )
        .expect("index")
        .expect("groups")
    }

    #[test]
    fn bins_count_from_each_field_and_scan_runs_first_time() {
        let index = index(Grouping::TimeBins {
            width_s: 25.0,
            combine_fields: false,
            combine_scans: false,
        });
        let bins: Vec<(Option<u32>, Option<u32>, u64)> = index
            .keys
            .iter()
            .map(|key| (key.field.map(|f| f.0), key.scan.map(|s| s.0), key.bin))
            .collect();
        // Field A's clock starts at 0 s: 0, 10, 40 s -> bins 0, 0, 1.
        // Field B's starts at 20 s: 20, 30 s -> bin 0; in scan 2 it restarts
        // at 50 s: 50, 60 s -> bin 0.
        assert_eq!(
            bins,
            [
                (Some(0), Some(0), 0),
                (Some(0), Some(0), 0),
                (Some(1), Some(0), 0),
                (Some(1), Some(0), 0),
                (Some(0), Some(0), 1),
                (Some(1), Some(1), 0),
                (Some(1), Some(1), 0),
            ]
        );
    }

    #[test]
    fn per_scan_and_per_integration_match_production_solints() {
        let per_scan = index(Grouping::PerScan {
            combine_fields: true,
        });
        let scans: Vec<_> = per_scan
            .keys
            .iter()
            .map(|key| (key.field, key.scan.map(|s| s.0), key.bin))
            .collect();
        assert!(scans[..5].iter().all(|k| *k == (None, Some(0), 0)));
        assert!(scans[5..].iter().all(|k| *k == (None, Some(1), 0)));
        let mut per_integration = index(Grouping::PerIntegration);
        let bins: Vec<u64> = per_integration.keys.iter().map(|key| key.bin).collect();
        assert_eq!(bins, [0, 1, 0, 1, 2, 0, 1]);
        let mut fragments = Vec::new();
        per_integration.fragments(0..7, 10, &mut fragments);
        assert_eq!(fragments.len(), 7);
        assert!(
            fragments
                .iter()
                .all(|f| f.position == FragmentPosition::Whole)
        );
        assert_eq!(fragments[6].samples, 60..70);
    }

    /// A selection keeps the whole grid's keys, positions and closes each
    /// group within itself, and marks the groups it cuts.
    #[test]
    fn a_selection_closes_the_parts_it_holds_and_marks_cut_groups() {
        let times: Vec<f64> = (0..7).map(|t| f64::from(t) * 10.0).collect();
        let fields = [0, 0, 1, 1, 0, 1, 1].map(FieldId);
        let scans = [0, 0, 0, 0, 0, 1, 1].map(ScanId);
        let grouping = Grouping::TimeBins {
            width_s: f64::INFINITY,
            combine_fields: false,
            combine_scans: true,
        };
        let index = |selection| {
            GroupIndex::new(
                PartitionId(0),
                grouping,
                Closure::AtDelivery,
                &times,
                &fields,
                Some(&scans),
                &TimeRuns::whole(selection),
            )
            .expect("index")
            .expect("groups")
        };
        let mut whole = Vec::new();
        index(0..7).fragments(0..7, 1, &mut whole);
        let mut sliced = Vec::new();
        index(1..4).fragments(1..4, 1, &mut sliced);
        // Field A's group (times 0, 1, 4) and B's (2, 3, 5, 6) are both
        // cut by times 1-3: A's part is time 1, B's times 2-3, each whole
        // within the selection and closed there.
        let seen: Vec<_> = sliced
            .iter()
            .map(|f| (f.key.field, f.samples.clone(), f.position, f.clipped))
            .collect();
        assert_eq!(
            seen,
            [
                (Some(FieldId(0)), 0..1, FragmentPosition::Whole, true),
                (Some(FieldId(1)), 1..3, FragmentPosition::Whole, true),
            ]
        );
        assert!(whole.iter().all(|f| !f.clipped));
        assert_eq!(sliced[0].key, whole[0].key);
        assert_eq!(sliced[1].key, whole[1].key);
    }

    #[test]
    fn a_recurring_field_closes_at_its_last_run() {
        let mut index = index(Grouping::TimeBins {
            width_s: f64::INFINITY,
            combine_fields: false,
            combine_scans: true,
        });
        let mut fragments = Vec::new();
        index.fragments(0..7, 1, &mut fragments);
        let positions: Vec<_> = fragments.iter().map(|f| f.position).collect();
        // Field A: times 0-1 (First), 4 (Last); field B: 2-3 (First), 5-6 (Last).
        assert_eq!(
            positions,
            [
                FragmentPosition::First,
                FragmentPosition::First,
                FragmentPosition::Last,
                FragmentPosition::Last
            ]
        );
        assert_eq!(fragments[2].samples, 4..5);
    }

    /// A field selection holds whole times in runs: a group keyed by field
    /// is held whole or not at all, and one that combines fields is held in
    /// parts across the gaps, closed at its last selected time and marked
    /// clipped.
    #[test]
    fn a_field_selection_holds_field_groups_whole_and_clips_combined_ones() {
        let times: Vec<f64> = (0..7).map(|t| f64::from(t) * 10.0).collect();
        let fields = [0, 0, 1, 1, 0, 1, 1].map(FieldId);
        let selection = TimeRuns::selected(
            0..7,
            &fields,
            &casa_observation::FieldSelection::Only(vec![FieldId(0)]),
        );
        assert_eq!(selection.runs(), [0..2, 4..5]);
        let fragments = |combine_fields| {
            let mut index = GroupIndex::new(
                PartitionId(0),
                Grouping::TimeBins {
                    width_s: f64::INFINITY,
                    combine_fields,
                    combine_scans: true,
                },
                Closure::AtDelivery,
                &times,
                &fields,
                None,
                &selection,
            )
            .expect("index")
            .expect("groups");
            let mut fragments = Vec::new();
            for run in selection.runs() {
                index.fragments(run.clone(), 1, &mut fragments);
            }
            fragments
                .into_iter()
                .map(|f| (f.key.field, f.position, f.clipped))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            fragments(false),
            [
                (Some(FieldId(0)), FragmentPosition::First, false),
                (Some(FieldId(0)), FragmentPosition::Last, false),
            ]
        );
        assert_eq!(
            fragments(true),
            [
                (None, FragmentPosition::First, true),
                (None, FragmentPosition::Last, true),
            ]
        );
    }
}
