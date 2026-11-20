// SPDX-License-Identifier: LGPL-3.0-or-later
//! The effective Fourier transform options for legacy imager prediction.

use std::path::PathBuf;

use casa_imaging_operator::{GridPrecision, WPlaneCount};
use casa_types::measures::direction::MDirection;
use casa_types::measures::position::MPosition;

use super::voltage_pattern::PreparedVoltagePattern;

/// Legacy `SDGrid` convolution selected by `imager.setoptions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagerSingleDishKernel {
    /// Nearest-cell box convolution.
    Box,
    /// Prolate spheroidal convolution.
    Spheroidal,
    /// Gaussian convolution.
    Gaussian,
    /// Gaussian-times-jinc convolution.
    GaussianJinc,
    /// Telescope primary beam.
    PrimaryBeam,
}

impl ImagerSingleDishKernel {
    /// Parse a CASA single-dish grid function, case independently.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "box" => Some(Self::Box),
            "sf" => Some(Self::Spheroidal),
            "gauss" => Some(Self::Gaussian),
            "gjinc" => Some(Self::GaussianJinc),
            "pb" => Some(Self::PrimaryBeam),
            _ => None,
        }
    }

    /// Grid function name used by CASA records.
    pub fn name(self) -> &'static str {
        match self {
            Self::Box => "BOX",
            Self::Spheroidal => "SF",
            Self::Gaussian => "GAUSS",
            Self::GaussianJinc => "GJINC",
            Self::PrimaryBeam => "PB",
        }
    }
}

/// Single-dish image-plane sampling, optionally combined with synthesis.
#[derive(Clone, Copy, Debug)]
pub struct ImagerSingleDish {
    /// The kernel applied to each antenna's pointing.
    pub kernel: ImagerSingleDishKernel,
    /// Predict interferometric visibilities using GridFT as well.
    pub combined: bool,
}

/// Convolution selected for the legacy imager's Fourier transform.
#[derive(Clone, Copy, Debug, Default)]
pub enum ImagerGridder {
    /// The ordinary spheroidal GridFT convolution.
    #[default]
    Spheroidal,
    /// Nearest-cell Boxcar convolution.
    Boxcar,
    /// The legacy heterogeneous-array MosaicFT convolution.
    Mosaic,
    /// W projection and its fixed, automatic or restored plane policy.
    WProject(WPlaneCount),
    /// Measure W over the active visibility selection before choosing planes.
    AutoWProject,
}

impl ImagerGridder {
    /// W projection with an explicit plane count or CASA's automatic choice.
    pub fn w_project(planes: i32) -> Self {
        if planes > 0 {
            Self::WProject(WPlaneCount::Fixed(planes as u32))
        } else {
            Self::AutoWProject
        }
    }
}

/// Settings applied when preparing new model grids.
#[derive(Clone, Copy, Debug)]
pub struct ImagerTransformOptions {
    /// Convolution kernel.
    pub gridder: ImagerGridder,
    /// The optional image-plane transform for total-power measurements.
    pub single_dish: Option<ImagerSingleDish>,
    /// CASA grid padding factor.
    pub padding: f32,
    /// FFT and gridding precision.
    pub precision: GridPrecision,
    /// CASA interpolation code (nearest 0, linear 1, cubic 2, spline 3).
    pub frequency_interpolation: i32,
    /// Requested cache size in complex pixels, or the CASA default.
    pub cache_size: i64,
    /// Scratch grid tile side.
    pub tile_size: i32,
    /// Worker limit for independent model-plane transforms, also stored for
    /// CASA readers. Nonpositive values use the available worker count.
    pub threads: i32,
    /// Override of the observatory position in ITRF spherical coordinates.
    pub observatory: Option<[f64; 3]>,
}

impl Default for ImagerTransformOptions {
    fn default() -> Self {
        Self {
            gridder: ImagerGridder::Spheroidal,
            single_dish: None,
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

impl ImagerTransformOptions {
    pub(super) fn observatory_override(&self) -> Option<MPosition> {
        self.observatory.map(|[longitude, latitude, radius]| {
            let horizontal = radius * latitude.cos();
            MPosition::new_itrf(
                horizontal * longitude.cos(),
                horizontal * longitude.sin(),
                radius * latitude.sin(),
            )
        })
    }

    /// Use the same gridding precision CASA selects through setoptions.
    pub fn use_single_precision(&mut self, single: bool) {
        self.precision = if single {
            GridPrecision::F32
        } else {
            GridPrecision::F64
        };
    }
}

#[derive(Clone, Default)]
pub(super) struct PredictionGeometry {
    pub(super) settings: ImagerTransformOptions,
    pub(super) native_threads: i32,
    pub(super) facets: usize,
    pub(super) tangent: Option<MDirection>,
    pub(super) phase_centre_time: Option<f64>,
    pub(super) frequency_frame_valid: bool,
    pub(super) voltage_pattern: Option<PreparedVoltagePattern>,
    pub(super) mosaic_voltage_table: Option<PathBuf>,
}

impl PredictionGeometry {
    pub(super) fn use_mosaic_outlier_grid(&mut self) {
        self.settings = ImagerTransformOptions {
            precision: GridPrecision::F32,
            cache_size: 1_000_000,
            observatory: self.settings.observatory,
            ..ImagerTransformOptions::default()
        };
        self.frequency_frame_valid = false;
    }
}
