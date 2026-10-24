// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific-group fragments of a time-ordered traversal, shared by the
//! MSv2 and MSv4 adapters.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;

use casa_observation::{
    FieldId, FragmentPosition, GroupFragment, GroupKey, Grouping, ObservationError, PartitionId,
};

use super::GroupClosure as Closure;

/// The group of every traversal unit (an MSv2 row, an MSv4 time) and where
/// each group starts and ends, computed once at binding from the unit
/// times and fields: the traversal's group index. Its size is two words per
/// unit; the trial accepts it in exchange for proven closure at delivery.
#[derive(Debug)]
pub(super) struct GroupIndex {
    keys: Vec<GroupKey>,
    extent: BTreeMap<GroupKey, Range<usize>>,
    closure: Closure,
    /// Groups whose last unit was delivered under deferred closure, still to
    /// be announced.
    pending_ends: VecDeque<GroupKey>,
}

impl GroupIndex {
    /// Index units whose times are `times` (seconds, non-decreasing) and
    /// fields `fields`, or `None` when the request asks for no groups.
    pub(super) fn new(
        partition: PartitionId,
        grouping: Grouping,
        closure: Closure,
        times: &[f64],
        fields: &[u32],
    ) -> Result<Option<Self>, ObservationError> {
        let Grouping::TimeBins {
            width_s,
            combine_fields,
        } = grouping
        else {
            return Ok(None);
        };
        if !(width_s.is_finite() && width_s > 0.0) {
            return Err(ObservationError::InvalidRequest {
                reason: format!("time-bin width {width_s} s is not positive"),
            });
        }
        if times.windows(2).any(|pair| pair[1] < pair[0]) {
            return Err(ObservationError::Unsupported {
                what: "time-binned groups over a traversal that is not in time order",
            });
        }
        // casa-rs gaincal's rule: each field's bins count from its first time.
        let mut origins = BTreeMap::<Option<u32>, f64>::new();
        let mut keys = Vec::with_capacity(times.len());
        let mut extent = BTreeMap::<GroupKey, Range<usize>>::new();
        for (unit, (&time, &field)) in times.iter().zip(fields).enumerate() {
            let field = (!combine_fields).then_some(field);
            let origin = *origins.entry(field).or_insert(time);
            let key = GroupKey {
                partition,
                field: field.map(FieldId),
                bin: ((time - origin) / width_s).floor().max(0.0) as u64,
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

    /// Append the fragments of units `units` to `fragments`, with sample
    /// ranges scaled by `samples_per_unit` (one per MSv2 row, the baselines
    /// of an MSv4 time) and counted from the batch's first unit.
    pub(super) fn fragments(
        &mut self,
        units: Range<usize>,
        samples_per_unit: usize,
        fragments: &mut Vec<GroupFragment>,
    ) {
        let mut start = units.start;
        while start < units.end {
            let key = self.keys[start];
            let mut end = start + 1;
            while end < units.end && self.keys[end] == key {
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
                samples: (start - units.start) * samples_per_unit
                    ..(end - units.start) * samples_per_unit,
                position,
            });
            start = end;
        }
    }
}
