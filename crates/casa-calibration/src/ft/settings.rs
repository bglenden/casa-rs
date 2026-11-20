// SPDX-License-Identifier: LGPL-3.0-or-later
//! The effective Fourier transform options for legacy imager prediction.

use casa_imaging_operator::{GridPrecision, WPlaneCount};

use super::voltage_pattern::PreparedVoltagePattern;

/// Convolution selected for the legacy imager's Fourier transform.
#[derive(Clone, Copy, Debug, Default)]
pub enum ImagerGridder {
    /// The ordinary spheroidal GridFT convolution.
    #[default]
    Spheroidal,
    /// Nearest-cell Boxcar convolution.
    Boxcar,
    /// W projection and its fixed, automatic or restored plane policy.
    WProject(WPlaneCount),
}

/// Settings applied when preparing new model grids.
#[derive(Clone, Copy, Debug)]
pub struct ImagerTransformOptions {
    /// Convolution kernel.
    pub gridder: ImagerGridder,
    /// CASA grid padding factor.
    pub padding: f32,
    /// FFT and gridding precision.
    pub precision: GridPrecision,
    /// CASA interpolation code (nearest 0, linear 1, cubic 2).
    pub frequency_interpolation: i32,
    /// Requested cache size in complex pixels, or the CASA default.
    pub cache_size: i64,
    /// Scratch grid tile side.
    pub tile_size: i32,
    /// Worker limit stored for CASA readers. Native FT currently uses
    /// one worker per prediction.
    pub threads: i32,
    /// Override of the observatory position in ITRF spherical coordinates.
    pub observatory: Option<[f64; 3]>,
}

impl Default for ImagerTransformOptions {
    fn default() -> Self {
        Self {
            gridder: ImagerGridder::Spheroidal,
            padding: 1.0,
            precision: GridPrecision::F64,
            frequency_interpolation: 0,
            cache_size: 1_000_000_000,
            tile_size: 16,
            threads: -1,
            observatory: None,
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct PredictionGeometry {
    pub(super) settings: ImagerTransformOptions,
    pub(super) phase_centre_time: Option<f64>,
    pub(super) voltage_pattern: Option<PreparedVoltagePattern>,
}
