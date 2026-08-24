// SPDX-License-Identifier: LGPL-3.0-or-later

//! Retained and bounded MeasurementSet observation access for native imaging.
//!
//! This module evaluates storage-owned selected content against immutable
//! casa-imaging-model contracts. It owns no reconstruction, scheduling,
//! device selection, product generation, or publication behavior.

mod access;
mod bound_observation;
mod content_plan;
mod measures;
mod row_access;
mod row_selection;
mod spectral_contributions;
#[cfg(test)]
mod tests;

pub use access::BoundObservationSourceError;
pub(crate) use access::{BoundObservationSamples, BoundObservationSource};
pub use bound_observation::{
    BoundSelectedObservation, BoundSelectedObservationError, ObservationSourceBinding,
    SelectedObservationCompletion, SelectedObservationTraversalError,
};
pub use content_plan::SelectedObservationContentBudget;
pub(crate) use content_plan::{
    SelectedObservationContentPlan, SelectedObservationContentPlanError,
};
pub use measures::{SelectedObservationMeasures, SelectedObservationMeasuresError};
pub use row_access::{SelectedObservationRow, SelectedObservationRowSelection};
pub use spectral_contributions::SelectedObservationTraversalSample;
