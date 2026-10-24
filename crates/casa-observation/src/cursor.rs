// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation boundary: an opened observation binds traversals, and a
//! traversal's cursor fills batches.

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::buffers::BatchBuffers;
use crate::catalogue::Catalogue;
use crate::grouping::{GroupKey, ReadEvent};
use crate::request::{AccessRequest, IoCounters, Realization};

/// An opened observation, whatever stores it.
///
/// This is the one interface algorithm code sees: the catalogue to plan
/// from, and [`Self::bind_read`] for each traversal it needs. Choosing a
/// backend and its storage options happens when the observation is opened,
/// outside the algorithm; afterwards the algorithm handles only the
/// [`Catalogue`], [`AccessRequest`]s, the bound [`VisibilityCursor`] and
/// [`VisibilityBatch`] views. Dispatch to the store happens once per bind
/// and once per batch, never per element.
pub trait Observation: Sync {
    /// The observation's catalogue.
    fn catalogue(&self) -> &Catalogue;

    /// Bind a traversal of `request`.
    ///
    /// # Errors
    ///
    /// When the request names something the observation does not have,
    /// asks for a role the store does not hold, or needs a profile the
    /// store's adapter does not implement. Nothing has been read.
    fn bind_read(&self, request: &AccessRequest) -> Result<BoundCursor<'_>, ObservationError>;
}

/// A bound traversal of an [`Observation`].
pub type BoundCursor<'o> = Box<dyn VisibilityCursor + Send + 'o>;

/// What one [`VisibilityCursor::fill`] produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fill {
    /// The buffers hold a finished batch ([`crate::BatchFill::finish`]).
    Batch,
    /// No further samples of this group will arrive; the buffers are empty.
    GroupEnd(GroupKey),
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
/// batch's allocation alive.
#[derive(Debug)]
pub struct FilledBatch {
    buffers: BatchBuffers,
}

impl FilledBatch {
    /// The batch: views of the owned buffers.
    #[must_use]
    pub fn batch(&self) -> VisibilityBatch<'_> {
        self.buffers.batch().expect("a filled batch holds a batch")
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
    ) -> Result<(Self, usize), Box<Reclaimed>> {
        match buffers.pack(batch) {
            Ok(copied) => Ok((Self { buffers }, copied)),
            Err(error) => Err(Box::new(Reclaimed { error, buffers })),
        }
    }
}

/// What [`VisibilityCursor::next_owned`] delivers. Every variant that is
/// not a batch hands the buffers back, so a pool never depends on knowing
/// in advance how many batches a traversal has.
#[derive(Debug)]
pub enum OwnedEvent {
    /// The next batch, with its buffers.
    Batch(FilledBatch),
    /// No further samples of this group will arrive; the buffers come back
    /// unused.
    GroupEnd(GroupKey, BatchBuffers),
    /// The traversal is over; the buffers come back unused.
    End(BatchBuffers),
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

/// A bound traversal of one [`AccessRequest`].
///
/// An adapter implements [`Self::fill`]; consumers call [`Self::next`],
/// which lends the filled buffers as a [`VisibilityBatch`], or
/// [`Self::next_owned`], which hands them over as a [`FilledBatch`].
pub trait VisibilityCursor {
    /// How the source realized the request.
    fn realization(&self) -> &Realization;

    /// Fill `buffers` with the next batch, through
    /// [`BatchBuffers::begin`] and [`crate::BatchFill::finish`], or announce
    /// a group's end, or return `None` at the end of the traversal.
    ///
    /// # Errors
    ///
    /// When the store cannot be read or holds data the bound profile does
    /// not admit.
    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError>;

    /// What the traversal has read so far.
    fn counters(&self) -> IoCounters;

    /// The next event, borrowing `buffers`: they cannot be refilled or
    /// mutated until the event is dropped.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`].
    fn next<'b>(
        &mut self,
        buffers: &'b mut BatchBuffers,
    ) -> Result<Option<ReadEvent<'b>>, ObservationError> {
        match self.fill(buffers)? {
            None => Ok(None),
            Some(Fill::GroupEnd(key)) => Ok(Some(ReadEvent::GroupEnd(key))),
            Some(Fill::Batch) => {
                let buffers: &'b BatchBuffers = buffers;
                buffers
                    .batch()
                    .map(|batch| Some(ReadEvent::Batch(batch)))
                    .ok_or_else(unfinished)
            }
        }
    }

    /// The next event, taking `buffers` and handing them back with it,
    /// whatever happens.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`], with the buffers.
    fn next_owned(&mut self, mut buffers: BatchBuffers) -> Result<OwnedEvent, Box<Reclaimed>> {
        match self.fill(&mut buffers) {
            Ok(None) => Ok(OwnedEvent::End(buffers)),
            Ok(Some(Fill::GroupEnd(key))) => Ok(OwnedEvent::GroupEnd(key, buffers)),
            Ok(Some(Fill::Batch)) if buffers.header().is_some() => {
                Ok(OwnedEvent::Batch(FilledBatch { buffers }))
            }
            Ok(Some(Fill::Batch)) => Err(Box::new(Reclaimed {
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

fn unfinished() -> ObservationError {
    ObservationError::InvalidBatch {
        reason: "the cursor reported a batch it did not finish".to_string(),
    }
}

impl<C: VisibilityCursor + ?Sized> VisibilityCursor for Box<C> {
    fn realization(&self) -> &Realization {
        (**self).realization()
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError> {
        (**self).fill(buffers)
    }

    fn counters(&self) -> IoCounters {
        (**self).counters()
    }
}
