// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native imaging on the major-cycle pass: the measurement operator built
//! from the compiled problem, the MeasurementSet as a bounded source, and
//! the loop of major and minor cycles (plan section 5.4).

mod continuum;
mod cycle;
mod images;
mod measurement;
mod source;
mod visibility_write;

pub(crate) use cycle::{ImagingInputs, run};
pub(crate) use visibility_write::VisibilityWriteTarget;

use casa_imaging_operator::OperatorError;
use casa_imaging_reconstruction::{
    MajorCycleError, MaskError, ModelLifecycleError, SpectralOperatorError,
};
use casa_imaging_runtime::pass::PassError;
use casa_imaging_runtime::{MinorCycleRunError, ResourceError};

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
    Normal(#[from] SpectralOperatorError),
    /// The model lifecycle rejected a generation or update.
    #[error(transparent)]
    Model(#[from] ModelLifecycleError),
    /// A major-cycle reconciliation failed.
    #[error(transparent)]
    MajorCycle(#[from] MajorCycleError),
    /// The next cycle's masks could not be planned.
    #[error(transparent)]
    Mask(#[from] MaskError),
    /// The host resources could not be read.
    #[error(transparent)]
    Resources(#[from] ResourceError),
    /// The selected observation could not be opened.
    #[error("selected observation: {0}")]
    Observation(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The paged cube state could not be created.
    #[error("cube state: {0}")]
    CubeState(#[from] std::io::Error),
}
