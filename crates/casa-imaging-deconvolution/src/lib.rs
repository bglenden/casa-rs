// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Image-domain deconvolution: the minor-cycle driver, the CLEAN controller
//! and the solvers.
//!
//! [`run_plane`] runs a [`Solver`] (Högbom, Clark, multiscale, multi-term)
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
mod multiscale;
mod patch;
mod plane;
mod psf;
mod refresh;
mod scales;
mod solver;
mod taylor;

pub use beam::{
    DEFAULT_PSF_FIT_CUTOFF, PsfBeamFitError, RestoringBeam, fit_restoring_beam,
    psf_fit_workspace_bytes,
};
pub use clark::{Clark, ClarkState};
pub use controller::{
    CleanStop, Controller, CycleControls, PlaneControl, PlaneStatistics, PlaneStop,
    ResidualStatistics,
};
pub use driver::{Component, PlaneOutcome, run_plane};
pub use hogbom::Hogbom;
pub use multiscale::{Multiscale, MultiscaleState};
pub use plane::{
    PlaneShape, RobustNoise, Support, casacore_max_abs, first_peak, peak_magnitude, robust_noise,
};
pub use psf::{ClarkPatch, PsfSummary};
pub use refresh::LinearRefresh;
pub use scales::spheroidal;
pub use solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepStop};
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
    #[error("FFT: {0}")]
    Fft(String),
}

impl From<casa_fft::FftError> for Error {
    fn from(error: casa_fft::FftError) -> Self {
        Self::Fft(error.to_string())
    }
}
