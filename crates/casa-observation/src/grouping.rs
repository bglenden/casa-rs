// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific groups that span batches, and the events a cursor delivers.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::catalogue::{FieldId, PartitionId, ScanId};

/// A scientific group: the samples a consumer treats together, such as one
/// gain solution interval. Batches and storage chunks do not bound it.
///
/// Keyed as production `gaincal` keys solutions: by partition (spectral
/// window, observation and intent), field unless the request combines
/// fields, scan unless it combines scans, and the time bin counted from the
/// first time of the (field, scan) run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey {
    /// The partition. Groups never span partitions; a consumer that
    /// combines spectral windows merges groups by (field, scan, bin) across
    /// cursors.
    pub partition: PartitionId,
    /// The field, unless the request combines fields.
    pub field: Option<FieldId>,
    /// The scan, unless the request combines scans.
    pub scan: Option<ScanId>,
    /// The time bin within the (field, scan) run.
    pub bin: u64,
}

/// Where a fragment lies in its group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentPosition {
    /// The whole group: no earlier or later fragments.
    Whole,
    /// The group's first fragment; more follow.
    First,
    /// Neither first nor last.
    Middle,
    /// The group's last fragment.
    Last,
}

impl FragmentPosition {
    /// Whether this fragment completes its group.
    #[must_use]
    pub fn closes(self) -> bool {
        matches!(self, Self::Whole | Self::Last)
    }
}

/// The samples of one group within one batch: whole times, so a contiguous
/// range of the batch's samples.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupFragment {
    /// The group.
    pub key: GroupKey,
    /// The batch's samples that belong to it: whole times, in order.
    pub samples: Range<usize>,
    /// Where the fragment lies in its group. `Last` or `Whole` is given only
    /// when the source has proven no later sample belongs to the group;
    /// otherwise a later [`ReadEvent::GroupEnd`] closes it.
    ///
    /// Fragments belong to the delivered batch, never to compute windows of
    /// it: [`VisibilityBatch::channel_window`] and
    /// [`VisibilityBatch::time_window`] carry none, so a closing position
    /// is seen once, however many work units share the batch.
    pub position: FragmentPosition,
}

/// Joins the two completions of a group that concurrent work consumes: its
/// input (a closing fragment or [`ReadEvent::GroupEnd`]) and the work units
/// begun on its fragments or windows. A group is ready to finalize only
/// when both have happened.
///
/// A consumer calls [`Self::begin`] for each unit of work it starts on a
/// group, which hands out a [`WorkToken`]; gives the token to
/// [`Self::finish`] when that unit is done; and calls [`Self::close`] when
/// the source completes the group's input. Whichever call makes the group
/// ready returns `true`, once. Tokens cannot be copied, so a unit finishes
/// once and only begun work can finish.
#[derive(Debug, Default)]
pub struct GroupCompletion {
    pending: BTreeMap<GroupKey, usize>,
    /// Groups whose input is complete and whose work is still pending.
    closed: BTreeSet<GroupKey>,
    /// Groups already reported ready.
    finalized: BTreeSet<GroupKey>,
}

/// One unit of work begun on a group ([`GroupCompletion::begin`]). It ends
/// when it is handed to [`GroupCompletion::finish`]; it is `Send`, so it can
/// travel with the work and come back with its result.
#[derive(Debug)]
#[must_use = "a unit of work ends only when its token is finished"]
pub struct WorkToken {
    key: GroupKey,
}

impl WorkToken {
    /// The group the work is on.
    #[must_use]
    pub fn key(&self) -> GroupKey {
        self.key
    }
}

impl GroupCompletion {
    /// A unit of work on `key` has started; finish it with the token.
    ///
    /// # Errors
    ///
    /// If the group's input has already completed: no work can begin on a
    /// group after its closure.
    pub fn begin(&mut self, key: GroupKey) -> Result<WorkToken, ObservationError> {
        if self.closed.contains(&key) || self.finalized.contains(&key) {
            return Err(ObservationError::InvalidBatch {
                reason: format!("work begun on {key:?} after its input completed"),
            });
        }
        *self.pending.entry(key).or_default() += 1;
        Ok(WorkToken { key })
    }

    /// The unit of work `token` names has finished; `true` if that makes
    /// its group ready.
    ///
    /// # Panics
    ///
    /// If `token` was issued by another `GroupCompletion`.
    pub fn finish(&mut self, token: WorkToken) -> bool {
        let key = token.key;
        let pending = self
            .pending
            .get_mut(&key)
            .expect("a token's work is pending in the completion that issued it");
        *pending -= 1;
        if *pending > 0 {
            return false;
        }
        self.pending.remove(&key);
        if self.closed.remove(&key) {
            self.finalized.insert(key);
            true
        } else {
            false
        }
    }

    /// The group's input is complete; `true` if no work on it is pending,
    /// so the group is ready now.
    ///
    /// # Errors
    ///
    /// If the group was already closed.
    pub fn close(&mut self, key: GroupKey) -> Result<bool, ObservationError> {
        if self.closed.contains(&key) || self.finalized.contains(&key) {
            return Err(ObservationError::InvalidBatch {
                reason: format!("{key:?} closed twice"),
            });
        }
        if self.pending.contains_key(&key) {
            self.closed.insert(key);
            Ok(false)
        } else {
            self.finalized.insert(key);
            Ok(true)
        }
    }

    /// Units of work pending on `key`.
    #[must_use]
    pub fn pending(&self, key: GroupKey) -> usize {
        self.pending.get(&key).copied().unwrap_or(0)
    }

    /// Whether no work is pending and no closed group awaits its work.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && self.closed.is_empty()
    }
}

/// What a cursor delivers.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a batch is a few hundred bytes of views, returned once per batch; boxing it would allocate per batch"
)]
pub enum ReadEvent<'a> {
    /// The next batch.
    Batch(VisibilityBatch<'a>),
    /// No further samples of this group will arrive. The group's earlier
    /// fragments may still be in a consumer's work units: input is complete,
    /// the work need not be.
    GroupEnd(GroupKey),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bin: u64) -> GroupKey {
        GroupKey {
            partition: PartitionId(0),
            field: None,
            scan: None,
            bin,
        }
    }

    #[test]
    fn a_group_is_ready_once_after_its_input_and_its_work() {
        let mut completion = GroupCompletion::default();
        let first = completion.begin(key(0)).expect("begin");
        let second = completion.begin(key(0)).expect("begin");
        assert_eq!(second.key(), key(0));
        assert!(!completion.close(key(0)).expect("close"), "work pending");
        assert!(completion.begin(key(0)).is_err(), "no work after closure");
        assert!(!completion.finish(first));
        assert!(completion.finish(second), "ready once");
        assert!(completion.close(key(0)).is_err(), "closed twice");
        assert!(
            completion.close(key(1)).expect("close"),
            "no work: ready now"
        );
        assert!(completion.is_idle());
    }
}
