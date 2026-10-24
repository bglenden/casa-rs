// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Shared observation contracts for MeasurementSet v2 and MSv4: the
//! [`VisibilityBatch`] that storage adapters fill and numerical consumers
//! read, the [`AccessRequest`] consumers bind, and the [`VisibilityCursor`]
//! that delivers batches.
//!
//! **Trial status.** This crate is the executable sketch of
//! `docs/measurement-set/contracts.md` for the MSv2/MSv4 trial
//! (`docs/measurement-set/trial/README.md`). It implements only what the
//! trial's two examples need: one partition and one data group per
//! traversal, the sample-major layout, `Complex<f32>` visibilities,
//! time-ordered traversals and time-bin groups. Its interfaces are not
//! frozen.
//!
//! # The pieces
//!
//! - A [`Catalogue`] names an opened observation's antennas, fields and
//!   partitions with identities scoped to that opening.
//! - An [`AccessRequest`] says what a traversal must deliver: the
//!   partition, data group, roles, channel window, layout, precision,
//!   scientific grouping and the largest batch. The adapter binds it once and
//!   reports its [`Realization`]: what it will read and decode.
//! - A [`VisibilityCursor`] fills caller-owned [`BatchBuffers`] and lends
//!   them as a [`VisibilityBatch`], or announces a [`ReadEvent::GroupEnd`].
//! - A batch holds the actual numbers as [`ndarray`] views, with the
//!   samples' coordinates in a [`SampleTopology`] (listed, as MSv2 rows, or
//!   factored time × baseline, as MSv4) and its times, frequencies and UVW in
//!   native Measures views ([`casa_types::measures::EpochView`] and friends).
//!
//! # Example
//!
//! A consumer binds a request, then loops over events; the borrow of the
//! buffers ends before each refill.
//!
//! ```
//! use casa_observation::{BatchBuffers, ObservationError, ReadEvent, VisibilityCursor};
//!
//! fn total_weight(cursor: &mut impl VisibilityCursor) -> Result<f64, ObservationError> {
//!     let mut buffers = BatchBuffers::default();
//!     let mut total = 0.0;
//!     while let Some(event) = cursor.next(&mut buffers)? {
//!         let ReadEvent::Batch(batch) = event else { continue };
//!         let (Some(weights), Some(flags)) = (batch.group.weights, batch.group.flags) else {
//!             continue;
//!         };
//!         for ((sample, channel, correlation), flagged) in flags.indexed_iter() {
//!             if !flagged {
//!                 total += f64::from(weights.get(sample, channel, correlation));
//!             }
//!         }
//!     }
//!     Ok(total)
//! }
//! ```

mod batch;
mod buffers;
mod catalogue;
mod grouping;
mod request;

pub use batch::{
    DataGroupView, IrregularSamples, Occupancy, SampleCoordinate, SampleTopology,
    TimeBaselineSamples, VisibilityBatch, VisibilityValues, WeightView,
};
pub use buffers::{BatchBuffers, BatchShape, TopologyShape};
pub use casa_types::measures::{EpochView, FrequencyView, UvwView};
pub use catalogue::{
    AntennaEntry, AntennaId, Catalogue, Correlation, DataGroupEntry, FieldEntry, FieldId,
    OrderedBaseline, PartitionEntry, PartitionId, VisibilityUnits,
};
pub use grouping::{FragmentPosition, GroupFragment, GroupKey, ReadEvent};
pub use request::{
    AccessRequest, Closure, Grouping, IoCounters, Layout, Precision, Realization, Roles,
};

/// Why a request could not be bound or a traversal failed.
#[derive(Debug, thiserror::Error)]
pub enum ObservationError {
    /// The request asks for something this source or the trial does not
    /// implement. Nothing has been read.
    #[error("unsupported: {what}")]
    Unsupported {
        /// What is unsupported.
        what: &'static str,
    },
    /// The request is inconsistent with the observation.
    #[error("invalid request: {reason}")]
    InvalidRequest {
        /// Why.
        reason: String,
    },
    /// The store does not hold what its profile requires.
    #[error("invalid store: {reason}")]
    InvalidStore {
        /// Why.
        reason: String,
    },
    /// A filled array disagrees with the batch's shape.
    #[error("{array} holds {actual} elements; the batch shape needs {expected}")]
    Shape {
        /// The array.
        array: &'static str,
        /// Its length.
        actual: usize,
        /// The length the shape implies.
        expected: usize,
    },
    /// The storage layer failed.
    #[error("storage: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// What one [`VisibilityCursor::fill`] produced.
#[derive(Clone, Debug, PartialEq)]
pub enum Fill {
    /// The buffers hold a batch of this shape.
    Batch(BatchShape),
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
    shape: BatchShape,
}

impl FilledBatch {
    /// The batch: views of the owned buffers, checked when this value was
    /// made.
    #[must_use]
    pub fn batch(&self) -> VisibilityBatch<'_> {
        self.buffers
            .view(&self.shape)
            .expect("shape checked at fill")
    }

    /// The shape the buffers were filled to.
    #[must_use]
    pub fn shape(&self) -> &BatchShape {
        &self.shape
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
    /// As [`BatchBuffers::pack`].
    pub fn pack(
        batch: &VisibilityBatch<'_>,
        mut buffers: BatchBuffers,
    ) -> Result<(Self, usize), ObservationError> {
        let (shape, copied) = buffers.pack(batch)?;
        Ok((Self { buffers, shape }, copied))
    }
}

/// What [`VisibilityCursor::next_owned`] delivers.
#[derive(Debug)]
pub enum OwnedEvent {
    /// The next batch, with its buffers.
    Batch(FilledBatch),
    /// No further samples of this group will arrive; the buffers come back
    /// unused.
    GroupEnd(GroupKey, BatchBuffers),
}

/// A bound traversal of one [`AccessRequest`].
///
/// An adapter implements [`Self::fill`]; consumers call [`Self::next`],
/// which lends the filled buffers as a [`VisibilityBatch`], or
/// [`Self::next_owned`], which hands them over as a [`FilledBatch`].
/// Format dispatch happens once per call, never per element.
pub trait VisibilityCursor {
    /// How the source realized the request.
    fn realization(&self) -> &Realization;

    /// Fill `buffers` with the next batch, or announce a group's end, or
    /// return `None` at the end of the traversal.
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
            Some(Fill::Batch(shape)) => {
                let buffers: &'b BatchBuffers = buffers;
                buffers
                    .view(&shape)
                    .map(|batch| Some(ReadEvent::Batch(batch)))
            }
        }
    }

    /// The next event, taking `buffers` and handing them back with it.
    ///
    /// # Errors
    ///
    /// As [`Self::fill`].
    fn next_owned(
        &mut self,
        mut buffers: BatchBuffers,
    ) -> Result<Option<OwnedEvent>, ObservationError> {
        match self.fill(&mut buffers)? {
            None => Ok(None),
            Some(Fill::GroupEnd(key)) => Ok(Some(OwnedEvent::GroupEnd(key, buffers))),
            Some(Fill::Batch(shape)) => {
                buffers.view(&shape)?;
                Ok(Some(OwnedEvent::Batch(FilledBatch { buffers, shape })))
            }
        }
    }
}
