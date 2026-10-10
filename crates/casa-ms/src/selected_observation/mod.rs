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
mod spectral_evaluation;
#[cfg(test)]
mod tests;

pub use crate::ms::SelectedObservationRow;
pub(crate) use access::BoundObservationSource;
pub use access::{
    BoundObservationSourceError, SelectedObservationBlock, SelectedObservationNumericGeometry,
};
pub use bound_observation::{
    BoundSelectedObservation, BoundSelectedObservationError, DeferredSelectedObservationAccess,
    ObservationSourceBinding, SelectedObservationBlockSource,
};
pub use content_plan::{
    SelectedObservationContentBudget, SelectedObservationContentPlan,
    SelectedObservationContentPlanError, SelectedObservationContentRequirements,
    SelectedObservationReferenceDataBudget,
};
pub use measures::{SelectedObservationMeasures, SelectedObservationMeasuresError};
pub use row_access::SelectedObservationRowSelection;
pub use spectral_evaluation::{
    SelectedObservationSpectralEnvelope, SelectedObservationSpectralEnvelopeReducer,
    SelectedObservationSpectralWindow,
};
