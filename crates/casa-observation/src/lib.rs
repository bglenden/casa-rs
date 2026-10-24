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

/// A bound traversal of one [`AccessRequest`].
///
/// [`Self::next`] fills `buffers` and lends them as the next event; the
/// returned event borrows the buffers, so they cannot be refilled or
/// mutated until it is dropped. Format dispatch happens once per call, never
/// per element.
pub trait VisibilityCursor {
    /// How the source realized the request.
    fn realization(&self) -> &Realization;

    /// The next event, or `None` at the end of the traversal.
    ///
    /// # Errors
    ///
    /// When the store cannot be read or holds data the bound profile does
    /// not admit.
    fn next<'b>(
        &mut self,
        buffers: &'b mut BatchBuffers,
    ) -> Result<Option<ReadEvent<'b>>, ObservationError>;

    /// What the traversal has read so far.
    fn counters(&self) -> IoCounters;
}
