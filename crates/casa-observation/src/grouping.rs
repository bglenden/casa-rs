// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific groups that span batches, and the events a cursor delivers.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::catalogue::{FieldId, PartitionId, ScanId};

/// A scientific group: the samples a consumer treats together, such as one
/// gain solution interval. Batches and storage chunks do not bound it.
///
/// Keyed as production `gaincal` keys solutions: by partition (spectral
/// window, observation and intent), field unless the request combines
/// fields, scan unless it combines scans, and the time bin counted from the
/// first time of the (field, scan) run. Keys are taken over the times of
/// the request's field selection ([`AccessRequest::fields`]), as `gaincal`
/// keys the rows it selects, and over every grid time whatever the time
/// selection, so the slices of a time split agree on them.
///
/// [`AccessRequest::fields`]: crate::AccessRequest::fields
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
    ///
    /// Under a time selection ([`AccessRequest::times`]) the position and
    /// the completion describe the part of the group the selection holds.
    /// A field selection ([`AccessRequest::fields`]) cuts no group: groups
    /// hold only its fields' times, and a group spans the gaps between its
    /// runs of times, closing at its last selected time.
    ///
    /// [`AccessRequest::times`]: crate::AccessRequest::times
    /// [`AccessRequest::fields`]: crate::AccessRequest::fields
    pub position: FragmentPosition,
    /// Whether the request's time selection holds only part of the group:
    /// the rest of its samples lie outside the selected times, and a
    /// traversal of those times delivers them, under the same key. The
    /// slices of a [`TimeSplit`](crate::TimeSplit) share a completion that
    /// joins such a group's parts; a group no time selection cuts is whole
    /// in its traversal, whatever the field selection.
    pub clipped: bool,
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
///
/// A token dropped unfinished (its unit panicked, failed or was cancelled)
/// abandons its group: the group never becomes ready, and the next
/// [`Self::finish`], [`Self::close`] or [`Self::is_idle`] reports
/// [`ObservationError::WorkAbandoned`] rather than waiting for work that
/// will not come.
///
/// One completion serves one traversal; the traversals of a time split
/// share a [`SplitCompletion`](crate::SplitCompletion) instead. A source
/// completes the input of a (partition, field, scan) series' groups in bin
/// order, since it delivers whole times in time order, so the completion
/// keeps only each series' latest closed bin to refuse a second closure or
/// work begun after one: its memory is the series and the groups in
/// flight, however long the traversal.
#[derive(Debug, Default)]
pub struct GroupCompletion {
    joins: Joins,
}

impl GroupCompletion {
    /// A unit of work on `key` has started; finish it with the token.
    ///
    /// # Errors
    ///
    /// If the group's input has already completed (no work can begin on a
    /// group after its closure), or its work was abandoned.
    pub fn begin(&mut self, key: GroupKey) -> Result<WorkToken, ObservationError> {
        self.joins.begin(0, key)
    }

    /// The unit of work `token` names has finished; `true` if that makes
    /// its group ready.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] if a unit of work was abandoned.
    ///
    /// # Panics
    ///
    /// If `token` was issued by another `GroupCompletion`.
    pub fn finish(&mut self, token: WorkToken) -> Result<bool, ObservationError> {
        self.joins.finish(token)
    }

    /// The group's input is complete; `true` if no work on it is pending,
    /// so the group is ready now.
    ///
    /// # Errors
    ///
    /// If the group was already closed, or a later group of its series;
    /// [`ObservationError::WorkAbandoned`] if a unit of work was abandoned.
    pub fn close(&mut self, key: GroupKey) -> Result<bool, ObservationError> {
        self.joins.close(0, key)
    }

    /// Units of work pending on `key`.
    #[must_use]
    pub fn pending(&self, key: GroupKey) -> usize {
        self.joins.pending(key)
    }

    /// Whether no work is pending and no closed group awaits its work.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] if a unit of work was abandoned:
    /// the completion then never becomes idle.
    pub fn is_idle(&mut self) -> Result<bool, ObservationError> {
        self.joins.is_idle()
    }
}

/// A group's series: its key without the bin.
type Series = (PartitionId, Option<FieldId>, Option<ScanId>);

fn series(key: GroupKey) -> Series {
    (key.partition, key.field, key.scan)
}

/// One unit of work begun on a group ([`GroupCompletion::begin`]). It ends
/// when it is handed to [`GroupCompletion::finish`]; it is `Send`, so it can
/// travel with the work and come back with its result. Dropping it
/// unfinished abandons the group.
#[derive(Debug)]
#[must_use = "a unit of work ends only when its token is finished"]
pub struct WorkToken {
    key: GroupKey,
    /// Where an unfinished drop reports the group; `None` once finished.
    abandoned: Option<Arc<Mutex<Vec<GroupKey>>>>,
}

impl WorkToken {
    /// The group the work is on.
    #[must_use]
    pub fn key(&self) -> GroupKey {
        self.key
    }
}

impl Drop for WorkToken {
    fn drop(&mut self) {
        if let Some(abandoned) = self.abandoned.take() {
            abandoned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self.key);
        }
    }
}

/// The joins of groups' input and work over the traversals that deliver
/// them: one traversal for a [`GroupCompletion`], a split's slices for a
/// [`SplitCompletion`](crate::SplitCompletion). Traversals are numbered;
/// each completes its series' groups in bin order.
///
/// A group a split divides arrives in several traversals, each completing
/// its own part: its input is complete only once every part's is, however
/// the traversals interleave, so work begun on an early part keeps the
/// group from being ready while a later part closes.
#[derive(Debug, Default)]
pub(crate) struct Joins {
    pending: BTreeMap<GroupKey, usize>,
    /// Groups whose input is complete and whose work is still pending.
    closed: BTreeSet<GroupKey>,
    /// Each traversal's series' latest bin whose input completed there.
    closed_through: BTreeMap<(usize, Series), u64>,
    /// Groups divided among traversals, with the traversals whose part's
    /// input has yet to complete; a group held whole by one traversal is
    /// not here.
    open_parts: BTreeMap<GroupKey, BTreeSet<usize>>,
    /// Groups whose tokens were dropped unfinished, until counted.
    abandoned: Arc<Mutex<Vec<GroupKey>>>,
    /// Groups with abandoned work.
    failed: BTreeSet<GroupKey>,
}

impl Joins {
    /// Joins over traversals that divide the groups `parts` names, each
    /// among the traversals named (two or more).
    pub(crate) fn divided(parts: BTreeMap<GroupKey, BTreeSet<usize>>) -> Self {
        Self {
            open_parts: parts,
            ..Self::default()
        }
    }

    /// As [`GroupCompletion::begin`], for traversal `traversal`.
    pub(crate) fn begin(
        &mut self,
        traversal: usize,
        key: GroupKey,
    ) -> Result<WorkToken, ObservationError> {
        self.settle()?;
        if self.is_closed(traversal, key) {
            return Err(ObservationError::InvalidBatch {
                reason: format!("work begun on {key:?} after its input completed"),
            });
        }
        *self.pending.entry(key).or_default() += 1;
        Ok(WorkToken {
            key,
            abandoned: Some(Arc::clone(&self.abandoned)),
        })
    }

    /// As [`GroupCompletion::finish`].
    pub(crate) fn finish(&mut self, mut token: WorkToken) -> Result<bool, ObservationError> {
        token.abandoned = None;
        let key = token.key;
        self.release(key);
        self.settle()?;
        Ok(!self.pending.contains_key(&key) && self.closed.remove(&key))
    }

    /// As [`GroupCompletion::close`], for traversal `traversal`'s part of
    /// the group: the group's input is complete when every part's is.
    pub(crate) fn close(
        &mut self,
        traversal: usize,
        key: GroupKey,
    ) -> Result<bool, ObservationError> {
        self.settle()?;
        if self.is_closed(traversal, key) {
            return Err(ObservationError::InvalidBatch {
                reason: format!("{key:?} closed twice, or after a later group of its series"),
            });
        }
        self.closed_through
            .insert((traversal, series(key)), key.bin);
        if let Some(open) = self.open_parts.get_mut(&key) {
            open.remove(&traversal);
            if !open.is_empty() {
                return Ok(false);
            }
            self.open_parts.remove(&key);
        }
        if self.pending.contains_key(&key) {
            self.closed.insert(key);
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Traversal `traversal` is over: each divided group whose part it
    /// left open is abandoned, as its input will not complete.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] when it left a part open, or
    /// work was abandoned before.
    pub(crate) fn end(&mut self, traversal: usize) -> Result<(), ObservationError> {
        let left: Vec<GroupKey> = self
            .open_parts
            .iter()
            .filter(|(_, open)| open.contains(&traversal))
            .map(|(key, _)| *key)
            .collect();
        for key in left {
            self.open_parts.remove(&key);
            self.closed.remove(&key);
            self.failed.insert(key);
        }
        self.settle()
    }

    /// As [`GroupCompletion::pending`].
    pub(crate) fn pending(&self, key: GroupKey) -> usize {
        self.pending.get(&key).copied().unwrap_or(0)
    }

    /// Whether no work is pending, no closed group awaits its work and no
    /// divided group awaits a part.
    pub(crate) fn is_idle(&mut self) -> Result<bool, ObservationError> {
        self.settle()?;
        Ok(self.pending.is_empty() && self.closed.is_empty() && self.open_parts.is_empty())
    }

    /// Whether traversal `traversal`'s part of `key` has completed its
    /// input: its bin is at or before the series' latest closed bin there.
    fn is_closed(&self, traversal: usize, key: GroupKey) -> bool {
        self.closed_through
            .get(&(traversal, series(key)))
            .is_some_and(|bin| key.bin <= *bin)
    }

    /// One unit of work on `key` is over.
    ///
    /// # Panics
    ///
    /// If none was pending.
    fn release(&mut self, key: GroupKey) {
        let pending = self
            .pending
            .get_mut(&key)
            .expect("a token's work is pending in the completion that issued it");
        *pending -= 1;
        if *pending == 0 {
            self.pending.remove(&key);
        }
    }

    /// Count the tokens dropped unfinished, and report the first group with
    /// abandoned work.
    fn settle(&mut self) -> Result<(), ObservationError> {
        let abandoned = std::mem::take(
            &mut *self
                .abandoned
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for key in abandoned {
            self.release(key);
            self.closed.remove(&key);
            self.failed.insert(key);
        }
        match self.failed.first() {
            Some(&group) => Err(ObservationError::WorkAbandoned { group }),
            None => Ok(()),
        }
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
        assert!(!completion.finish(first).expect("finish"));
        assert!(completion.finish(second).expect("finish"), "ready once");
        assert!(completion.close(key(0)).is_err(), "closed twice");
        assert!(
            completion.close(key(1)).expect("close"),
            "no work: ready now"
        );
        assert!(completion.close(key(0)).is_err(), "behind its series");
        assert!(completion.is_idle().expect("idle"));
        assert_eq!(
            completion.joins.closed_through.len(),
            1,
            "one series, however many groups"
        );
    }

    /// A unit that panics with its token, or drops it, abandons the group:
    /// the join reports it instead of waiting for work that will not come.
    #[test]
    fn abandoned_work_is_an_error_not_a_wait() {
        let mut completion = GroupCompletion::default();
        let kept = completion.begin(key(0)).expect("begin");
        let lost = completion.begin(key(0)).expect("begin");
        let unit = std::thread::spawn(move || {
            let _token = lost;
            panic!("the unit fails holding its token");
        });
        assert!(unit.join().is_err());
        assert!(matches!(
            completion.is_idle(),
            Err(ObservationError::WorkAbandoned { group }) if group == key(0)
        ));
        assert!(completion.close(key(0)).is_err());
        assert!(
            completion.finish(kept).is_err(),
            "its group never becomes ready"
        );
        assert!(completion.begin(key(1)).is_err());
    }
}
