// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation boundary: an opened observation estimates and binds
//! traversals, and a traversal's cursors fill batches.

use std::sync::Arc;

use crate::ObservationError;
use crate::buffers::BatchBuffers;
use crate::catalogue::{Catalogue, PartitionId};
use crate::creation::TimeBaselineGrid;
use crate::grouping::{GroupKey, ReadEvent};
use crate::pointing::{PointingRequest, PointingSeries};
use crate::pool::{BufferPool, FilledBatch};
use crate::request::{AccessRequest, Estimate, Grouping, IoCounters, PartitionEstimate};
use crate::selection::FieldSelection;

/// An opened observation, whatever stores it.
///
/// This is the one interface algorithm code sees: the catalogue to plan
/// from, [`Self::estimate`] to cost a batch shape before reading anything,
/// and [`Self::bind_read`] for each traversal it needs. Choosing a backend
/// and its storage options happens when the observation is opened, outside
/// the algorithm; afterwards the algorithm handles only the [`Catalogue`],
/// [`AccessRequest`]s, the bound [`Traversal`] and [`VisibilityBatch`]
/// views. Dispatch to the store happens once per bind and once per batch,
/// never per element.
///
/// [`VisibilityBatch`]: crate::VisibilityBatch
pub trait Observation: Sync {
    /// The observation's catalogue, shared with every batch it delivers.
    fn catalogue(&self) -> &Arc<Catalogue>;

    /// What `request` would cost, from the catalogue and the store's
    /// geometry alone: no payload is read. Binding the same request
    /// returns the same estimate.
    ///
    /// # Errors
    ///
    /// As [`Self::bind_read`].
    fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError>;

    /// Bind a traversal of `request`: one cursor per requested partition,
    /// each `Send`, so threads can share the observation by reference and
    /// run partitions in parallel.
    ///
    /// # Errors
    ///
    /// When the request names something the observation does not have,
    /// asks for a role the store does not hold, needs a profile the
    /// store's adapter does not implement, or budgets less than one time.
    /// Nothing has been read.
    fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError>;

    /// The sample grid of partition `id` as data group `data_group` places
    /// it (its fields), for a producer to declare when it writes the
    /// partition elsewhere: from the store's index, reading no payload.
    ///
    /// # Errors
    ///
    /// When the observation has no such partition or data group, or the
    /// partition is not one grid: no scan at each time, or acquisitions of
    /// more than one integration interval.
    fn grid(&self, id: PartitionId, data_group: &str)
    -> Result<TimeBaselineGrid, ObservationError>;

    /// The scientific group of each of partition `id`'s grid times that
    /// `fields` holds under `grouping`, as data group `data_group` places
    /// them (its fields), keyed as a traversal keys them (over the grid's
    /// field-selected times, whatever its time selection: bins count from
    /// the first such time of their key, [`GroupKey`]); `None` for a time
    /// `fields` excludes, and `None` overall when
    /// `grouping` asks for no groups. From the store's index, reading no
    /// payload. A [`TimeSplit`](crate::TimeSplit) counts the groups its
    /// cuts divide from these.
    ///
    /// # Errors
    ///
    /// When the observation has no such partition or data group, or
    /// `grouping` cannot key its times (by scan, when the store names none;
    /// by a time bin that is not positive).
    fn group_keys(
        &self,
        id: PartitionId,
        data_group: &str,
        grouping: Grouping,
        fields: &FieldSelection,
    ) -> Result<Option<Vec<Option<GroupKey>>>, ObservationError>;

    /// Partition `request.partition`'s pointing samples for
    /// `request.antennas` over `request.times`, reading no visibility
    /// payload ([`PointingSeries::bracket`] answers queries). A store
    /// without pointing gives no samples.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidRequest`] for an unknown partition or
    /// antenna, times that are not a finite span, or pointing beyond the
    /// request's budget; [`ObservationError::Unsupported`] for pointing the
    /// boundary does not carry (an MSv4 target, samples in several frames,
    /// an MSv4 frame or polynomial its reader does not take);
    /// [`ObservationError::InvalidStore`] for malformed pointing, an MSv2
    /// row of more terms than its reader's bound included.
    fn pointing(&self, request: &PointingRequest) -> Result<PointingSeries, ObservationError>;
}

/// A bound traversal of one partition of an [`Observation`].
pub type BoundCursor<'o> = Box<dyn VisibilityCursor + Send + 'o>;

/// A bound traversal: one cursor per requested partition, in request order,
/// and the estimate binding confirmed.
///
/// A sequential consumer reads the partitions one after another through
/// [`Self::next`] or [`Self::next_owned`]; a parallel consumer takes the
/// cursors and gives each a thread.
pub struct Traversal<'o> {
    /// The cursors, one per partition.
    pub cursors: Vec<BoundCursor<'o>>,
    /// What the traversal will cost.
    pub estimate: Estimate,
    current: usize,
}

/// Whose turn it is in a traversal.
enum Turn {
    /// Cursor `index` has a batch to fill.
    Batch(usize),
    /// A group's input is complete.
    GroupEnd(GroupKey),
    /// Every cursor is exhausted.
    End,
}

impl<'o> Traversal<'o> {
    /// A traversal of `cursors` with `estimate`.
    #[must_use]
    pub fn new(cursors: Vec<BoundCursor<'o>>, estimate: Estimate) -> Self {
        Self {
            cursors,
            estimate,
            current: 0,
        }
    }

    /// The next turn, after moving past exhausted cursors.
    fn turn(&mut self) -> Turn {
        while let Some(cursor) = self.cursors.get_mut(self.current) {
            match cursor.step() {
                Step::End => self.current += 1,
                Step::GroupEnd(key) => return Turn::GroupEnd(key),
                Step::Batch => return Turn::Batch(self.current),
            }
        }
        Turn::End
    }

    /// The next event of the partitions in order, borrowing `buffers`.
    ///
    /// # Errors
    ///
    /// As [`VisibilityCursor::fill`].
    pub fn next<'b>(
        &'b mut self,
        buffers: &'b mut BatchBuffers,
    ) -> Result<Option<ReadEvent<'b>>, ObservationError> {
        match self.turn() {
            Turn::End => Ok(None),
            Turn::GroupEnd(key) => Ok(Some(ReadEvent::GroupEnd(key))),
            Turn::Batch(index) => {
                self.cursors[index].fill(buffers)?;
                let buffers: &'b BatchBuffers = buffers;
                buffers
                    .batch()
                    .map(|batch| Some(ReadEvent::Batch(batch)))
                    .ok_or_else(unfinished)
            }
        }
    }

    /// The next event of the partitions in order, a batch filled into a
    /// buffer of `pool` ([`VisibilityCursor::next_owned`]).
    ///
    /// # Errors
    ///
    /// As [`VisibilityCursor::fill`]; the buffer goes back to the pool.
    pub fn next_owned(&mut self, pool: &BufferPool) -> Result<OwnedEvent, ObservationError> {
        match self.turn() {
            Turn::End => Ok(OwnedEvent::End),
            Turn::GroupEnd(key) => Ok(OwnedEvent::GroupEnd(key)),
            Turn::Batch(index) => {
                let cursor = &mut self.cursors[index];
                pool.fill(|buffers| cursor.fill(buffers))
                    .map(OwnedEvent::Batch)
            }
        }
    }

    /// What every cursor has read so far, summed.
    #[must_use]
    pub fn counters(&self) -> IoCounters {
        let mut total = IoCounters::default();
        for cursor in &self.cursors {
            total.add(cursor.counters());
        }
        total
    }
}

/// What a cursor will do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// A batch: [`VisibilityCursor::fill`] it.
    Batch,
    /// No further samples of this group will arrive. Announced once; the
    /// cursor has moved past it.
    GroupEnd(GroupKey),
    /// The traversal is over.
    End,
}

/// What [`VisibilityCursor::next_owned`] delivers.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a filled batch is moved once per batch; boxing it would allocate per batch"
)]
pub enum OwnedEvent {
    /// The next batch, in a buffer of the pool.
    Batch(FilledBatch),
    /// No further samples of this group will arrive.
    GroupEnd(GroupKey),
    /// The traversal is over.
    End,
}

/// A bound traversal of one partition of an [`AccessRequest`].
///
/// An adapter implements [`Self::step`] and [`Self::fill`]; consumers call
/// [`Self::next`], which lends the filled buffers as a [`VisibilityBatch`],
/// or [`Self::next_owned`], which fills a pooled buffer and hands it over
/// as a [`FilledBatch`].
///
/// [`VisibilityBatch`]: crate::VisibilityBatch
pub trait VisibilityCursor {
    /// The observation's catalogue: what the batches' views refer to.
    fn catalogue(&self) -> &Arc<Catalogue>;

    /// What this partition's traversal costs.
    fn estimate(&self) -> &PartitionEstimate;

    /// What comes next. A group end is consumed by this call; a batch is
    /// not, so asking twice before filling is harmless.
    fn step(&mut self) -> Step;

    /// Fill `buffers` with the next batch, through [`BatchBuffers::begin`]
    /// and [`crate::BatchFill::finish`]. Only valid after [`Self::step`]
    /// answered [`Step::Batch`].
    ///
    /// # Errors
    ///
    /// When the store cannot be read or holds data the bound profile does
    /// not admit.
    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<(), ObservationError>;

    /// What the traversal has read so far.
    fn counters(&self) -> IoCounters;

    /// The next event, borrowing `buffers`: they cannot be refilled or
    /// mutated until the event is dropped.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`].
    fn next<'b>(
        &'b mut self,
        buffers: &'b mut BatchBuffers,
    ) -> Result<Option<ReadEvent<'b>>, ObservationError> {
        match self.step() {
            Step::End => Ok(None),
            Step::GroupEnd(key) => Ok(Some(ReadEvent::GroupEnd(key))),
            Step::Batch => {
                self.fill(buffers)?;
                let buffers: &'b BatchBuffers = buffers;
                buffers
                    .batch()
                    .map(|batch| Some(ReadEvent::Batch(batch)))
                    .ok_or_else(unfinished)
            }
        }
    }

    /// The next event, a batch filled into a buffer of `pool`. A buffer is
    /// taken only for a batch, and the call waits while the pool has none
    /// free: the pool's bound is the traversal's.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`]; the buffer goes back to the pool.
    fn next_owned(&mut self, pool: &BufferPool) -> Result<OwnedEvent, ObservationError> {
        match self.step() {
            Step::End => Ok(OwnedEvent::End),
            Step::GroupEnd(key) => Ok(OwnedEvent::GroupEnd(key)),
            Step::Batch => pool
                .fill(|buffers| self.fill(buffers))
                .map(OwnedEvent::Batch),
        }
    }
}

fn unfinished() -> ObservationError {
    ObservationError::InvalidBatch {
        reason: "the cursor reported a batch it did not finish".to_string(),
    }
}

impl<C: VisibilityCursor + ?Sized> VisibilityCursor for Box<C> {
    fn catalogue(&self) -> &Arc<Catalogue> {
        (**self).catalogue()
    }

    fn estimate(&self) -> &PartitionEstimate {
        (**self).estimate()
    }

    fn step(&mut self) -> Step {
        (**self).step()
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<(), ObservationError> {
        (**self).fill(buffers)
    }

    fn counters(&self) -> IoCounters {
        (**self).counters()
    }
}
