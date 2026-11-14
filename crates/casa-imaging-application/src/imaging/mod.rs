// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native imaging on the major-cycle pass: the measurement operator built
//! from the compiled problem, the MeasurementSet as a bounded source, and
//! the loop of major and minor cycles.

mod charts;
mod continuum;
mod cycle;
mod images;
mod measurement;
mod observation_source;
mod setup;
mod trace;
mod visibility_write;

pub(crate) use cycle::{Forms, ImagingInputs, MvcCycleSpec, form_separate_psf, plane_bounds, run};
pub(crate) use images::prepare_model;
pub(crate) use measurement::{
    KernelAdmission, domain_operator, image_extent, metal_kernel_set, multi_store_kernel_set,
    reference_frequency_hz, row_samples, selected_correlations, standard_kernel_set,
};
pub(crate) use observation_source::{
    BatchRoute, Batching, ObservationSource, PartitionSelection, RouteStore, SelectedPartition,
    StoreBatches, StoreKeys, StoreRoute, WriteBack, admit_batches, pointing_direction,
};
pub(crate) use visibility_write::VisibilityWriteTarget;

use casa_imaging_deconvolution::{
    MajorCycleError, MaskError, ModelLifecycleError, NormalStateError,
};
use casa_imaging_model::{CompiledProblem, FrequencyFrame};
use casa_imaging_operator::OperatorError;
use casa_imaging_runtime::pass::PassError;
use casa_imaging_runtime::{Admission, Cancelled, MinorCycleRunError};

/// CASA's REST AW channel map uses source-shifted frequencies, while
/// AWProjectFT::setupVBStore supplies the native wavelengths to the gridder.
fn rest_aw_uses_native_wavelengths(problem: &CompiledProblem) -> bool {
    problem
        .science()
        .measurement_equation()
        .aw_projection()
        .is_some()
        && problem.geometry().spectral().output_frame() == FrequencyFrame::Rest
}

/// A failure of a native imaging run.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ImagingError {
    /// The compiled problem needs a capability the pass does not provide.
    #[error("unsupported imaging problem: {reason}")]
    Unsupported {
        /// What is unsupported.
        reason: &'static str,
    },
    /// The measurement operator rejected its inputs.
    #[error(transparent)]
    Operator(#[from] OperatorError),
    /// The AW convolution-function catalog could not be opened or generated.
    #[error(transparent)]
    KernelSet(#[from] casa_imaging_operator::AwCatalogError),
    /// A major-cycle or density pass failed.
    #[error(transparent)]
    Pass(#[from] PassError),
    /// A minor cycle failed.
    #[error(transparent)]
    Minor(#[from] MinorCycleRunError),
    /// A normal state could not be assembled.
    #[error(transparent)]
    Normal(#[from] NormalStateError),
    /// An MVC conversion or model handoff failed.
    #[error("MVC handoff: {0}")]
    Mvc(#[source] crate::ApplicationError),
    /// The model lifecycle rejected a generation or update.
    #[error(transparent)]
    Model(#[from] ModelLifecycleError),
    /// A major-cycle reconciliation failed.
    #[error(transparent)]
    MajorCycle(#[from] MajorCycleError),
    /// The next cycle's masks could not be planned.
    #[error(transparent)]
    Mask(#[from] MaskError),
    /// A phase's memory did not fit the resource policy.
    #[error(transparent)]
    Admission(#[from] Admission),
    /// The run was cancelled between phases.
    #[error(transparent)]
    Cancelled(#[from] Cancelled),
    /// The selected observation could not be opened.
    #[error("selected observation: {0}")]
    Observation(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The paged cube state could not be created.
    #[error("cube state: {0}")]
    CubeState(#[from] std::io::Error),
    /// The start model could not be read or regridded.
    #[error("startmodel: {0}")]
    StartModel(#[source] casa_images::ImageError),
    /// The PSF, sums of weights or residual of an earlier run could not be
    /// read (`calcpsf=False`, `calcres=False`).
    #[error("an earlier run's products: {0}")]
    Existing(#[source] casa_images::ImageError),
    /// A start model in flat-noise units could not be made physical with
    /// the initial pass's weights.
    #[error("startmodel units: {0}")]
    StartModelUnits(#[source] casa_imaging_products::ProductsError),
}

impl From<crate::native::NativeError> for ImagingError {
    fn from(error: crate::native::NativeError) -> Self {
        match error {
            crate::native::NativeError::Admission(error) => Self::Admission(error),
            crate::native::NativeError::Cancelled(error) => Self::Cancelled(error),
            crate::native::NativeError::Other(error) => Self::Mvc(error),
        }
    }
}
