// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation boundary: an opened observation estimates and binds
//! traversals, and a traversal's cursors fill batches.

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::buffers::BatchBuffers;
use crate::catalogue::Catalogue;
use crate::grouping::{GroupKey, ReadEvent};
use crate::request::{AccessRequest, Estimate, IoCounters, PartitionEstimate};

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
pub trait Observation: Sync {
    /// The observation's catalogue.
    fn catalogue(&self) -> &Catalogue;

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

    /// The cursor whose turn it is, after moving past exhausted ones.
    fn advance(&mut self) -> Result<Option<usize>, Step> {
        loop {
            let Some(cursor) = self.cursors.get_mut(self.current) else {
                return Ok(None);
            };
            match cursor.step() {
                Step::End => self.current += 1,
                Step::GroupEnd(key) => return Err(Step::GroupEnd(key)),
                Step::Batch => return Ok(Some(self.current)),
            }
        }
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
        let index = match self.advance() {
            Ok(None) => return Ok(None),
            Ok(Some(index)) => index,
            Err(Step::GroupEnd(key)) => return Ok(Some(ReadEvent::GroupEnd(key))),
            Err(_) => unreachable!("advance returns only group ends as errors"),
        };
        let cursor = &mut self.cursors[index];
        cursor.fill(buffers)?;
        let buffers: &'b BatchBuffers = buffers;
        buffers
            .batch(cursor.catalogue())
            .map(|batch| Some(ReadEvent::Batch(batch)))
            .ok_or_else(unfinished)
    }

    /// The next event of the partitions in order, taking `buffers` if given
    /// and handing them back with it ([`VisibilityCursor::next_owned`]).
    ///
    /// # Errors
    ///
    /// As [`VisibilityCursor::fill`], with the buffers.
    pub fn next_owned(
        &mut self,
        buffers: Option<BatchBuffers>,
    ) -> Result<OwnedEvent, Box<Reclaimed>> {
        match self.advance() {
            Ok(None) => Ok(OwnedEvent::End(buffers)),
            Err(Step::GroupEnd(key)) => Ok(OwnedEvent::GroupEnd(key, buffers)),
            Err(_) => unreachable!("advance returns only group ends as errors"),
            Ok(Some(index)) => self.cursors[index].next_owned(buffers),
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

/// A batch together with the buffers it was filled into: an owned value a
/// work unit can take, share read-only through an `Arc`, or hand back
/// whole for refilling.
///
/// [`VisibilityBatch`] borrows its buffers, so it cannot outlive or leave
/// the caller that filled them. A `FilledBatch` moves with its storage
/// instead, which is how a batch's lifetime is separated from the
/// traversal's: the cursor can fill the next buffers while earlier batches
/// are still in use elsewhere, and a slow user keeps exactly its own
/// batch's allocation alive. The batch's views are built on demand from
/// the observation's catalogue ([`Self::batch`]).
#[derive(Debug)]
pub struct FilledBatch {
    buffers: BatchBuffers,
}

impl FilledBatch {
    /// The batch: views of the owned buffers and of `catalogue`, the
    /// observation's.
    #[must_use]
    pub fn batch<'c>(&'c self, catalogue: &'c Catalogue) -> VisibilityBatch<'c> {
        self.buffers
            .batch(catalogue)
            .expect("a filled batch holds a batch")
    }

    /// The buffers, for refilling.
    #[must_use]
    pub fn into_buffers(self) -> BatchBuffers {
        self.buffers
    }

    /// Bytes the buffers hold, from their capacities.
    #[must_use]
    pub fn capacity_bytes(&self) -> usize {
        self.buffers.capacity_bytes()
    }

    /// Copy `batch` into `buffers` ([`BatchBuffers::pack`]): a window with
    /// storage of its own, and the payload elements copied.
    ///
    /// # Errors
    ///
    /// As [`BatchBuffers::pack`]; the buffers come back with the error.
    pub fn pack(
        batch: &VisibilityBatch<'_>,
        mut buffers: BatchBuffers,
        catalogue: &Catalogue,
    ) -> Result<(Self, usize), Box<Reclaimed>> {
        match buffers.pack(batch, catalogue) {
            Ok(copied) => Ok((Self { buffers }, copied)),
            Err(error) => Err(Box::new(Reclaimed { error, buffers })),
        }
    }
}

/// What [`VisibilityCursor::next_owned`] delivers. Every variant that is
/// not a batch hands back the buffers it was given, so a pool never
/// depends on knowing in advance how many batches a traversal has, and
/// never lends a buffer only to learn the traversal was over.
#[derive(Debug)]
pub enum OwnedEvent {
    /// The next batch, with its buffers.
    Batch(FilledBatch),
    /// No further samples of this group will arrive; the buffers, if any
    /// were given, come back unused.
    GroupEnd(GroupKey, Option<BatchBuffers>),
    /// The traversal is over; the buffers, if any were given, come back
    /// unused.
    End(Option<BatchBuffers>),
    /// The next event is a batch and no buffers were given: call again with
    /// some.
    NeedBuffers,
}

/// A failed owned read, with the buffers it was given (boxed: the buffers
/// are several hundred bytes of vector headers, and errors are rare).
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct Reclaimed {
    /// What failed.
    #[source]
    pub error: ObservationError,
    /// The buffers, emptied.
    pub buffers: BatchBuffers,
}

/// A bound traversal of one partition of an [`AccessRequest`].
///
/// An adapter implements [`Self::step`] and [`Self::fill`]; consumers call
/// [`Self::next`], which lends the filled buffers as a [`VisibilityBatch`],
/// or [`Self::next_owned`], which hands them over as a [`FilledBatch`].
pub trait VisibilityCursor {
    /// The observation's catalogue: what the batches' views refer to.
    fn catalogue(&self) -> &Catalogue;

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
                    .batch(self.catalogue())
                    .map(|batch| Some(ReadEvent::Batch(batch)))
                    .ok_or_else(unfinished)
            }
        }
    }

    /// The next event, taking `buffers` if given and handing them back with
    /// it, whatever happens. Without buffers the cursor answers what comes
    /// next ([`OwnedEvent::NeedBuffers`] for a batch) and fills nothing.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`], with the buffers.
    fn next_owned(&mut self, buffers: Option<BatchBuffers>) -> Result<OwnedEvent, Box<Reclaimed>> {
        match self.step() {
            Step::End => Ok(OwnedEvent::End(buffers)),
            Step::GroupEnd(key) => Ok(OwnedEvent::GroupEnd(key, buffers)),
            Step::Batch => {
                let Some(mut buffers) = buffers else {
                    return Ok(OwnedEvent::NeedBuffers);
                };
                match self.fill(&mut buffers) {
                    Ok(()) if buffers.header().is_some() => {
                        Ok(OwnedEvent::Batch(FilledBatch { buffers }))
                    }
                    Ok(()) => Err(Box::new(Reclaimed {
                        error: unfinished(),
                        buffers,
                    })),
                    Err(error) => {
                        buffers.clear();
                        Err(Box::new(Reclaimed { error, buffers }))
                    }
                }
            }
        }
    }
}

fn unfinished() -> ObservationError {
    ObservationError::InvalidBatch {
        reason: "the cursor reported a batch it did not finish".to_string(),
    }
}

impl<C: VisibilityCursor + ?Sized> VisibilityCursor for Box<C> {
    fn catalogue(&self) -> &Catalogue {
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
