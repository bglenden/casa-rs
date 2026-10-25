// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific-group fragments of a time-ordered traversal, shared by the
//! MSv2 and MSv4 adapters: the group of every time of a partition's grid,
//! keyed as production `gaincal` keys solutions.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;

use casa_observation::{
    FieldId, FragmentPosition, GroupFragment, GroupKey, Grouping, ObservationError, PartitionId,
    ScanId,
};

use super::GroupClosure as Closure;

/// The group of every time and where each group starts and ends, computed
/// once at binding from the grid's times, fields and scans: the traversal's
/// group index. Its size is two words per time; the trial accepts it in
/// exchange for proven closure at delivery.
#[derive(Debug)]
pub(super) struct GroupIndex {
    keys: Vec<GroupKey>,
    extent: BTreeMap<GroupKey, Range<usize>>,
    closure: Closure,
    /// Groups whose last time was delivered under deferred closure, still
    /// to be announced.
    pending_ends: VecDeque<GroupKey>,
}

impl GroupIndex {
    /// Index the grid times `times` (seconds, ascending) with `fields` and
    /// `scans` at each, or `None` when the request asks for no groups.
    pub(super) fn new(
        partition: PartitionId,
        grouping: Grouping,
        closure: Closure,
        times: &[f64],
        fields: &[FieldId],
        scans: &[ScanId],
    ) -> Result<Option<Self>, ObservationError> {
        if matches!(grouping, Grouping::None) {
            return Ok(None);
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
        // Production gaincal's rule: each (field, scan) key's bins count
        // from its first time; each key's integrations count from zero.
        let mut origins = BTreeMap::<(Option<FieldId>, Option<ScanId>), (f64, u64)>::new();
        let mut keys = Vec::with_capacity(times.len());
        let mut extent = BTreeMap::<GroupKey, Range<usize>>::new();
        for (unit, ((&time, &field), &scan)) in times.iter().zip(fields).zip(scans).enumerate() {
            let (field, scan) = match grouping {
                Grouping::None => unreachable!("handled above"),
                Grouping::TimeBins {
                    combine_fields,
                    combine_scans,
                    ..
                } => (
                    (!combine_fields).then_some(field),
                    (!combine_scans).then_some(scan),
                ),
                Grouping::PerScan { combine_fields } => {
                    ((!combine_fields).then_some(field), Some(scan))
                }
                Grouping::PerIntegration => (Some(field), Some(scan)),
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
            let key = GroupKey {
                partition,
                field,
                scan,
                bin,
            };
            extent
                .entry(key)
                .and_modify(|range| range.end = unit + 1)
                .or_insert(unit..unit + 1);
            keys.push(key);
        }
        Ok(Some(Self {
            keys,
            extent,
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
            &scans,
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
}
