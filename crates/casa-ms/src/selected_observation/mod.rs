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

pub(crate) use access::{
    BoundObservationSamples, BoundObservationSource, validate_selected_coordinates,
};
pub use access::{BoundObservationSourceError, SelectedObservationBlock};
pub use bound_observation::{
    BoundSelectedObservation, BoundSelectedObservationError, ObservationSourceBinding,
    SelectedObservationBlockConsumer, SelectedObservationBlockSource,
    SelectedObservationCompletion, SelectedObservationReplayAuthorization,
    SelectedObservationReplayProof, SelectedObservationResidencyCertificate,
    SelectedObservationTerminal, SelectedObservationTraversalError,
    SelectedObservationTraversalMeasurements,
};
pub use content_plan::SelectedObservationContentBudget;
pub(crate) use content_plan::{
    SelectedObservationContentPlan, SelectedObservationContentPlanError,
};
pub use measures::{SelectedObservationMeasures, SelectedObservationMeasuresError};
pub use row_access::{SelectedObservationRow, SelectedObservationRowSelection};
pub use spectral_evaluation::{
    SelectedObservationSpectralRange, SelectedObservationSpectralRangeMeasurements,
    SelectedObservationTraversalRun, SelectedObservationTraversalSample,
};

fn maximum_selected_correlations(problem: &casa_imaging_model::CompiledProblem) -> usize {
    problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .flat_map(|source| source.selection().correlations())
        .map(|selection| selection.products().len())
        .max()
        .unwrap_or(0)
}
