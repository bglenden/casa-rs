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
//! - An [`Observation`] is the boundary algorithm code sees, whatever
//!   stores the data: its [`Catalogue`] names the antennas, fields, scans
//!   and partitions with identities scoped to that opening, and it binds
//!   traversals. Backend choice happens when the observation is opened.
//! - An [`AccessRequest`] says what a traversal must deliver: the
//!   partition, data group, roles (payload, time auxiliaries, scan),
//!   channel window, layout, precision, scientific grouping and the largest
//!   batch. The adapter binds it once, refusing a role the store lacks, and
//!   reports its [`Realization`]: what it will read.
//! - A [`VisibilityCursor`] fills caller-owned [`BatchBuffers`] through one
//!   checked path ([`BatchBuffers::begin`], [`BatchFill::finish`]) and lends
//!   them as a [`VisibilityBatch`], or announces a [`ReadEvent::GroupEnd`].
//! - A batch holds the actual numbers as [`ndarray`] views, with the
//!   samples' coordinates in a [`SampleTopology`] (listed, as MSv2 rows, or
//!   factored time × baseline, as MSv4), its times, frequencies and UVW in
//!   native Measures views ([`casa_types::measures::EpochView`] and friends),
//!   and one [`Validity`] rule over its stored flags.
//!
//! # Example
//!
//! A consumer binds a request against any observation, then loops over
//! events; the borrow of the buffers ends before each refill.
//!
//! ```
//! use casa_observation::{AccessRequest, BatchBuffers, Observation, ObservationError, ReadEvent};
//!
//! fn total_weight(
//!     observation: &dyn Observation,
//!     request: &AccessRequest,
//! ) -> Result<f64, ObservationError> {
//!     let mut cursor = observation.bind_read(request)?;
//!     let mut buffers = BatchBuffers::default();
//!     let mut total = 0.0;
//!     while let Some(event) = cursor.next(&mut buffers)? {
//!         let ReadEvent::Batch(batch) = event else { continue };
//!         let (Some(weights), validity) = (batch.group.weights, batch.validity()) else {
//!             continue;
//!         };
//!         for sample in 0..batch.samples() {
//!             for channel in 0..batch.channel_count() {
//!                 for correlation in 0..batch.correlations.len() {
//!                     if !validity.excluded(sample, channel, correlation) {
//!                         total += f64::from(weights.get(sample, channel, correlation));
//!                     }
//!                 }
//!             }
//!         }
//!     }
//!     Ok(total)
//! }
//! ```

mod batch;
mod buffers;
mod catalogue;
mod creation;
mod cursor;
mod grouping;
mod request;

pub use batch::{
    DataGroupView, IrregularSamples, Occupancy, SampleCoordinate, SampleTiming, SampleTopology,
    Shared, TimeBaselineSamples, Validity, VisibilityBatch, VisibilityValues, WeightView,
};
pub use buffers::{
    BatchArrays, BatchBuffers, BatchFill, BatchHeader, IdLimits, Sharing, TopologyShape,
};
pub use casa_types::measures::{EpochView, FrequencyView, UvwView};
pub use catalogue::{
    AntennaEntry, AntennaId, Catalogue, Correlation, DataGroupEntry, FieldEntry, FieldId,
    OrderedBaseline, PartitionEntry, PartitionId, ScanEntry, ScanId, UvwFrame, VisibilityUnits,
};
pub use creation::TimeBaselineGrid;
pub use cursor::{
    BoundCursor, Fill, FilledBatch, Observation, OwnedEvent, Reclaimed, VisibilityCursor,
};
pub use grouping::{FragmentPosition, GroupCompletion, GroupFragment, GroupKey, ReadEvent};
pub use request::{AccessRequest, Grouping, IoCounters, Layout, Precision, Realization, Roles};

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
    /// A filled array disagrees with the batch's header.
    #[error("{array} holds {actual} elements; the batch header needs {expected}")]
    Shape {
        /// The array.
        array: &'static str,
        /// Its length.
        actual: usize,
        /// The length the header implies.
        expected: usize,
    },
    /// A batch's identities, fragments or sizes are inconsistent, or a
    /// consumer's completion bookkeeping was misused.
    #[error("invalid batch: {reason}")]
    InvalidBatch {
        /// Why.
        reason: String,
    },
    /// The storage layer failed.
    #[error("storage: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
}
