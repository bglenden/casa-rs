// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Retained and bounded MeasurementSet observation access for native imaging.
//!
//! This package evaluates storage-owned selected content against the immutable
//! contracts in `casa-imaging-model`. It owns no imaging science, execution
//! scheduling, device selection, or publication behavior.

mod access;
mod bound_observation;
mod content_plan;
mod row_selection;

pub use access::{BoundObservationSamples, BoundObservationSource, BoundObservationSourceError};
pub use bound_observation::{
    BoundSelectedObservation, BoundSelectedObservationError, BoundSelectedObservationSamples,
    ObservationSourceReadPlan,
};
pub use content_plan::{
    SelectedObservationContentBudget, SelectedObservationContentPlan,
    SelectedObservationContentPlanError,
};
