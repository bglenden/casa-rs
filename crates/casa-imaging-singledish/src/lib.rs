// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Single-dish image-plane gridding, as CASA's `SDGrid` does it for
//! `tsdimaging` and for the simulator's total-power prediction.
//!
//! Single-dish (total-power) imaging places each spectrum at the image
//! pixel of its pointing and spreads it over the pixels around with a
//! radial kernel; there is no FFT, no uv coordinate and no gridding
//! correction. Prediction is the adjoint: the model sampled through the
//! same kernel at each pointing. ADR-0016 records this as its own operator
//! family, beside the four interferometric kernel sets of
//! `casa-imaging-operator`.
//!
//! | CASA | casa-rs |
//! |---|---|
//! | `SDGrid::init`, `SDGrid::findPBAsConvFunction` (`gridfunction`, `convsupport`, `truncate`, `gwidth`, `jwidth`) | [`GridFunction`], [`KernelTable`] |
//! | `sgridsd`, `ogridsd` (`fgridsd.f`): a pointing's pixels | [`KernelTable::taps`] |
//! | `dgridsd`: the model at a pointing | [`KernelTable::degrid`] |
//! | `ggridsd`, `SDGrid::getImage`, `getWeightImage` | [`TotalPowerGrid`] |
//! | `SDGrid::pickWeights` | [`weights_from_rows`], [`weights_from_spectra`] |
//! | `tsdimaging`'s `do_weight_mask` | [`weight_mask_threshold`] |
//! | `SingleDishBeamUtil::getPointingSamplingRaster` | [`raster_sampling`] |
//! | `tsdimaging`'s `set_beam_size` sampling choice | [`beam_sampling`] |
//! | `sdbeamutil.TheoreticalBeam`, `primaryBeamArcsec` | [`theoretical_beam_arcsec`], [`primary_beam_fwhm_arcsec`] |
//!
//! Reading the spectra and pointings from a MeasurementSet, placing a
//! pointing on the image (`SDGrid::getXYPos`), and writing the products are
//! the task's (#938).
//!
//! The gridder holds its two grids whole, as CASA does, under an admission
//! of [`TotalPowerGrid::bytes`], and grids the image's channels in parallel
//! on the imaging runtime's worker team.

mod beam;
mod grid;
mod kernel;
mod sampling;
mod weights;

pub use beam::{BeamSetup, primary_beam_fwhm_arcsec, theoretical_beam_arcsec};
pub use grid::{FlagMerge, GridShape, SpectrumBlock, TotalPowerGrid, TotalPowerImage};
pub use kernel::{GridFunction, KernelParameters, KernelTable, PowerPattern, SAMPLING, Tap};
pub use sampling::{Pointing, RasterSampling, beam_sampling, raster_sampling};
pub use weights::{weight_mask_threshold, weights_from_rows, weights_from_spectra};

/// Why single-dish gridding refused.
#[derive(Debug, thiserror::Error)]
pub enum SingleDishError {
    /// A kernel CASA does not have, or parameters that give no kernel.
    #[error("{0}")]
    Kernel(String),
    /// Weights of a number of correlations the gridder does not image.
    #[error("{0}")]
    Correlations(String),
    /// Arrays that disagree in shape, or an image too large.
    #[error("{0}")]
    Shape(String),
}
