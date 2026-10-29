// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Shared observation contracts for MeasurementSet v2 and MSv4: the
//! [`VisibilityBatch`] that storage adapters fill and numerical consumers
//! read, the [`AccessRequest`] consumers estimate and bind, and the
//! [`Traversal`] of [`VisibilityCursor`]s that delivers batches.
//!
//! **Trial status.** This crate is the executable sketch of
//! `docs/measurement-set/contracts.md` for the MSv2/MSv4 trial
//! (`docs/measurement-set/trial/README.md`), at its third revision
//! (`docs/measurement-set/trial/revision-2-design.md`, then the README's
//! "Revision 3"). Its interfaces are
//! not frozen.
//!
//! # The principle
//!
//! MSv4 is the future: the interface takes MSv4's native shape, and the
//! MSv2 adapter assembles MSv2 data into it once, at opening. A consumer
//! cannot tell the stores apart from a batch.
//!
//! # The pieces
//!
//! - An [`Observation`] is the boundary algorithm code sees, whatever
//!   stores the data: its [`Catalogue`] names the antennas, fields, scans,
//!   observing sessions and partitions with identities scoped to that
//!   opening, and holds every per-partition constant once (channel
//!   frequencies and frame, correlation products, the baseline table, the
//!   time system, the data groups). Backend choice happens when the
//!   observation is opened.
//! - A **partition** is a store unit described by its content: spectral
//!   window, correlation products, intents, observing session, feed pair.
//!   The MSv2 adapter partitions rows the way XRADIO's converter does, so
//!   the same observation opens as the same partition set from either
//!   store.
//! - An [`AccessRequest`] says what a traversal must deliver: the
//!   partition set, data group, roles (payload, time auxiliaries, scan),
//!   channel window, correlation products and flag policy, scientific
//!   grouping and the byte budget of one batch. [`Observation::estimate`]
//!   costs it from the catalogue and the store's geometry alone (the
//!   **batch shape**: times per batch, bytes, read amplification, natural
//!   blocks, padding); [`Observation::bind_read`] binds it, refusing a role
//!   the store lacks, and returns one cursor per partition with the same
//!   [`Estimate`].
//! - A [`VisibilityCursor`] fills caller-owned [`BatchBuffers`] through one
//!   checked path ([`BatchBuffers::begin`], [`BatchFill::finish`]) and lends
//!   them as a [`VisibilityBatch`], or announces a [`ReadEvent::GroupEnd`].
//!   Owned batches ([`FilledBatch`]) are filled into a bounded
//!   [`BufferPool`] and return their buffers when dropped; concurrent work
//!   on a group is joined by [`GroupCompletion`]'s [`WorkToken`]s.
//!   Within a cursor the **traversal order** is fixed: whole times of one
//!   channel window of one partition, in time order; across cursors it is
//!   the consumer's.
//! - A batch is whole times of the partition's time × baseline grid:
//!   sample `t × B + b`, with the field and scan per time, its times,
//!   frequencies and UVW in native Measures views
//!   ([`casa_types::measures::EpochView`] and friends), the payload as
//!   [`ndarray`] views in the **payload order** `[sample, channel,
//!   correlation]`, and one [`Validity`] rule over its flags, values and
//!   weights. A grid cell no acquisition filled is padding: flagged in
//!   every store (the MSv4 adapter folds XRADIO's NaN weight into the
//!   flag), its values and time auxiliaries NaN.
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
//!     let mut traversal = observation.bind_read(request)?;
//!     let mut buffers = BatchBuffers::default();
//!     let mut total = 0.0;
//!     while let Some(event) = traversal.next(&mut buffers)? {
//!         let ReadEvent::Batch(batch) = event else { continue };
//!         let (Some(weights), validity) = (batch.group.weights, batch.validity()) else {
//!             continue;
//!         };
//!         let [samples, channels, correlations] = batch.shape();
//!         for sample in 0..samples {
//!             for channel in 0..channels {
//!                 for correlation in 0..correlations {
//!                     if !validity.excluded(sample, channel, correlation) {
//!                         total += f64::from(weights[[sample, channel, correlation]]);
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
mod pointing;
mod pool;
mod request;
mod selection;
mod split;
mod update;

pub use batch::{DataGroupView, SampleCoordinate, SampleTiming, Shared, Validity, VisibilityBatch};
pub use buffers::{BatchArrays, BatchBuffers, BatchFill, BatchHeader, Sharing};
pub use casa_types::Correlation;
pub use casa_types::measures::{EpochView, FrequencyView, MUvw, UvwView};
pub use catalogue::{
    AntennaEntry, AntennaId, Binding, BindingTolerance, Catalogue, DataGroupEntry, FieldEntry,
    FieldId, ObservationEntry, ObservationId, OrderedBaseline, PartitionEntry, PartitionId,
    Receptors, ScanEntry, ScanId, UvwFrame, VisibilityUnits, alt_az_mount,
};
pub use creation::TimeBaselineGrid;
pub use cursor::{BoundCursor, Observation, OwnedEvent, Step, Traversal, VisibilityCursor};
pub use grouping::{
    FragmentPosition, GroupCompletion, GroupFragment, GroupKey, ReadEvent, WorkToken,
};
pub use pointing::{
    AntennaPointing, PointingBracket, PointingCandidate, PointingDirection, PointingRequest,
    PointingSeries,
};
pub use pool::{BufferPool, FilledBatch};
pub use request::{
    AccessRequest, CorrelationAxis, CorrelationRequest, Deliver, Estimate, FlagsFrom, Grouping,
    IoCounters, NaturalBlocks, PartitionEstimate, ResolvedRequest, Roles, SigmaFallback, TimeRange,
    TimeSelection, WeightRequest, WeightRule, WeightSource, inverse_sigma_squared, times_per_batch,
};
pub use selection::{FieldSelection, TimeRuns};
pub use split::{SliceCompletion, SplitCompletion, TimeSplit};
pub use update::{
    AliasScope, BatchAddress, PatchValues, ResolvedTarget, UpdateGuarantees, UpdateReport,
    UpdateTarget, Updater, WeightChannels, WritableRole, check_values, refuse_payload_checks,
    uniform_over_channels,
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
    /// Holding what was asked would take more memory than its budget, so
    /// it was refused before that memory was allocated.
    #[error("{what} needs {required} bytes, beyond the {budget} it may hold")]
    OverBudget {
        /// What was refused.
        what: &'static str,
        /// The bytes it needs, as far as it was found before the refusal.
        required: usize,
        /// The bytes it may hold.
        budget: usize,
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
    /// Every buffer of a [`BufferPool`] stayed in use for the wait limit
    /// of the handle taking one ([`BufferPool::waiting_at_most`]). Nothing
    /// was read: asking again continues the traversal where it was.
    #[error("all {capacity} buffers of the pool stayed in use")]
    PoolExhausted {
        /// Buffers in the pool.
        capacity: usize,
    },
    /// A unit of work on `group` ended without finishing its
    /// [`WorkToken`]: it panicked, failed or was cancelled, so the group
    /// cannot complete.
    #[error("work on {group:?} was abandoned")]
    WorkAbandoned {
        /// The group.
        group: GroupKey,
    },
    /// A batch's identities, fragments or sizes are inconsistent, or a
    /// consumer's completion bookkeeping was misused.
    #[error("invalid batch: {reason}")]
    InvalidBatch {
        /// Why.
        reason: String,
    },
    /// Another session in this process already holds what this one needs:
    /// write intent on the store, or times a live updater of the session
    /// claims.
    /// Nothing was opened or written.
    #[error("conflict: {reason}")]
    Conflict {
        /// What is held.
        reason: String,
    },
    /// The storage layer failed.
    #[error("storage: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
}
