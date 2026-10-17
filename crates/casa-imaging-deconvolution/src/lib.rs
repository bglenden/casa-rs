// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Image-domain reconstruction: the state a run reconstructs from and into,
//! and the minor cycle that updates it.
//!
//! **State.** A major-cycle pass appends its unnormalised images to a
//! [`MajorCycle`], which pairs them with the final model the pass predicted
//! and releases both as one [`MajorCycleCompletion`]: the
//! [`FinalNormalState`] (residual, PSF moments, sensitivity and `sumwt` per
//! plane, resident or paged) and the [`ModelGeneration`]. A
//! [`ModelLifecycle`] owns the run's model generations and applies each
//! minor cycle's sparse update. [`ReconstructionMask`]s, from boxes or
//! CASA's `auto-multithresh`, bound where the model may change.
//!
//! **Minor cycle.** [`run_plane`] runs a [`Solver`] (Högbom, Clark, multiscale, multi-term)
//! over one plane's [`MinorCycleView`] in steps, under the per-plane rules
//! of [`PlaneControl`], and returns the plane's model [`Delta`]. The
//! run-wide rules between major cycles — `niter`, the global threshold with
//! its 1% tolerance, `nmajor`, automatic `cycleniter`, the cycle threshold
//! and divergence — are [`Controller`]. A cube runs every plane, a mosaic of
//! image fields every field, under one [`CycleControls`].
//!
//! Every rule follows CASA's `tclean` (`synthesis/ImagerObjects` and
//! `synthesis/MeasurementEquations`); each item names the function it
//! follows. The design is section 5.5 of
//! `docs/imaging-architecture/imaging-foundation-plan-20261007.md`.

mod beam;
mod clark;
mod controller;
mod driver;
mod hogbom;
mod major_cycle;
mod mask;
mod model_lifecycle;
mod model_storage;
mod multiscale;
mod normal_state;
mod normal_values;
mod patch;
mod plane;
mod psf;
mod refresh;
mod scales;
mod solver;
mod taylor;

pub use major_cycle::{
    FinalNormalDomainState, FinalNormalState, FinalNormalStateCoefficientTerm,
    FinalNormalStateNormalMoment, FinalNormalStatePlane, FinalNormalStateWindow, MajorCycle,
    MajorCycleCompletion, MajorCycleError, PassAppender, normal_state_window_residency_bytes,
};
pub use mask::{
    AutoMaskBeam, AutoMultithreshControls, AutoMultithreshEvidence, ImageDomainMaskMaterialization,
    ImageDomainReconstructionMaskPlans, ImageDomainReconstructionMasks, MaskBox, MaskError,
    ReconstructionMask, ReconstructionMaskPlan, ReconstructionMaskSet, auto_multithresh,
    direction_world_to_pixel, reproject_mask_support,
};
pub(crate) use model_lifecycle::validate_model_value;
pub use model_lifecycle::{
    ModelGeneration, ModelLifecycle, ModelLifecycleError, PreparedFinalModel,
};
pub use model_storage::{
    ModelSampleStorage, ModelSampleUpdate, ModelStorageFactory, ModelStoragePlan,
};
pub(crate) use normal_state::PassNormalState;
pub use normal_state::storage::{
    FinalNormalPlaneReader, NormalArrayStorage, NormalStorageFactory, NormalStoragePlan,
};
pub use normal_state::{
    NormalBasis, NormalStateError, PassImages, SpectralChannelValidity, SpectralSlabPlan,
    resident_bytes_per_cell,
};
pub use normal_values::{NormalValues, SensitivityValues};

pub use beam::{
    DEFAULT_PSF_FIT_CUTOFF, FitAttemptFailure, PsfBeamFitError, RestoringBeam, fit_restoring_beam,
    psf_fit_workspace_bytes,
};
pub use clark::{Clark, ClarkState};
pub use controller::{
    CleanStop, Controller, CycleControls, PlaneControl, PlaneStatistics, PlaneStop,
    ResidualStatistics,
};
pub use driver::{Component, PlaneOutcome, run_plane, solve_bytes};
pub use hogbom::Hogbom;
pub use multiscale::{Multiscale, MultiscaleState};
pub use plane::{
    PlaneShape, RobustNoise, Support, casacore_max_abs, first_peak, peak_magnitude, psf_peak,
    robust_noise,
};
pub use psf::{ClarkPatch, PsfSummary};
pub use refresh::LinearRefresh;
/// The scale taper, public for the T0 laws' extended sources only.
#[doc(hidden)]
pub use scales::spheroidal;
pub use solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd};
pub use taylor::{Taylor, TaylorState};

/// Why a minor cycle could not run.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Error {
    /// The PSF has no positive finite peak.
    #[error("the PSF has no positive finite peak")]
    PsfPeak,
    /// The PSF main lobe could not be fitted.
    #[error(transparent)]
    BeamFit(#[from] PsfBeamFitError),
    /// The PSF convolved twice with a scale peaks negative; the scale is
    /// too large for the PSF (`MatrixCleaner::clean`).
    #[error("the PSF convolved with a scale peaks negative; use smaller scales")]
    NegativeScalePeak,
    /// No requested scale fits in half the image.
    #[error("no multi-term scale fits in half the image")]
    NoScale,
    /// The plane is too narrow for the multi-term cleaner's PSF patch
    /// (`findBeamPatch` leaves no pixel).
    #[error("the image is too small for the multi-term PSF patch")]
    PlaneTooSmall,
    /// The multi-term Hessian is singular or its rows are dependent; the
    /// data cannot support that many Taylor terms.
    #[error(
        "the multi-term Hessian is not invertible; check that the frequency coverage \
         supports the requested Taylor terms"
    )]
    SingularHessian,
    /// Solver arithmetic left the finite numbers.
    #[error("minor-cycle arithmetic produced a non-finite value")]
    NonFinite,
    /// A transform could not be planned or run.
    #[error(transparent)]
    Fft(#[from] casa_fft::FftError),
}
