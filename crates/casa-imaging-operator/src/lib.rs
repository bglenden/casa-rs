// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The shared radio-interferometric measurement operator.
//!
//! This crate owns the forward operator `A` (model image to predicted
//! visibilities) and its adjoint `A*` (weighted visibilities to unnormalised
//! image-domain normal images) as one implementation parameterised by a
//! convolution-function set ([`ConvolutionFunctionSet`]: the standard
//! [`Spheroidal`] set here, W-planes, AW catalog and mosaic primary beam in
//! later tickets) and a gridding backend ([`GridBackend`]: [`CpuBackend`]
//! here, Metal later). It also owns the global weighting generation
//! ([`WeightingGeneration`]) and the spectral resampler that turns native
//! rows into [`Placement`]s.
//!
//! The design is `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
//! section 5.3 and ADR-0016; ticket IF-1 (#650) filled this crate.
//!
//! # Conventions
//!
//! Grid and image planes are `[y][x]`, x fastest. Visibilities follow the
//! MeasurementSet convention `V = ∫ I e^{+2πi(ul + vm)}`; the adjoint uses
//! an unnormalised inverse FFT, so a source at direction cosines `(l, m)`
//! lands on image pixel `(l/Δx, m/Δy)` from the reference pixel with the
//! signed WCS increments. Values in a [`SampleBlock`] are pre-multiplied by
//! the imaging weight and the phase-centre phasor `e^{iφ}`; prediction
//! applies `e^{−iφ}`. The image correction is one real diagonal applied on
//! both sides of the operator (CASA divides the model and the image by the
//! same spheroidal response), so `A*` is the exact adjoint of `A` up to the
//! polarization basis, whose image-domain conversion follows CASA's
//! `StokesImageUtil` including its `1/2` factors and `sumwt` rule.

mod accumulator;
mod backend;
mod convolution;
mod cpu;
mod error;
mod fft;
mod geometry;
mod operator;
mod polarization;
mod resample;
mod sample;
mod spheroidal;
mod weighting;

pub use accumulator::{
    AccumulatorLayout, GridAccumulator, GridPrecision, GridScalar, GridStorage, Mode, ModeSet,
    PlaneRange, Tile,
};
pub use backend::{GridBackend, PreparedModelGrids, Work};
pub use convolution::{
    ConvolutionFunctionSet, ImageCorrection, MuellerRouting, RowContext, TapLayout,
};
pub use cpu::CpuBackend;
pub use error::OperatorError;
pub use geometry::{CellLocation, GridGeometry, GridPadding, ImageExtent};
pub use operator::{
    Basis, MeasurementOperator, ModelImages, ModelPlane, ModelPrescale, NormalImages, NormalPlane,
    NormalSection,
};
pub use polarization::{FeedBasis, GridPolarization, PolarizationRouting};
pub use resample::{NativeRow, SpectralKernel, SpectralResampler};
pub use sample::{CfKey, Placement, SampleBlock, SampleBuffer};
pub use spheroidal::{SPHEROIDAL_OVERSAMPLING, SPHEROIDAL_SUPPORT, Spheroidal, grdsf};
pub use weighting::{
    DensityCellRule, DensityGrid, DensityGridShape, RobustFactors, Taper, WeightingGeneration,
    build_density_grid,
};
