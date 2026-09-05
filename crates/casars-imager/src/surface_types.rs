// SPDX-License-Identifier: LGPL-3.0-or-later

//! Task-surface value types.
//!
//! These values express caller intent only. They do not perform imaging
//! calculations or expose types from a displaced implementation package.

use std::path::PathBuf;

/// Visibility-weighting request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightingMode {
    /// Natural weighting.
    Natural,
    /// Uniform density weighting.
    Uniform,
    /// Briggs robust weighting.
    Briggs {
        /// Robustness in the CASA interval `[-2, 2]`.
        robust: f32,
    },
    /// Briggs bandwidth-taper weighting.
    BriggsBwTaper {
        /// Robustness in the CASA interval `[-2, 2]`.
        robust: f32,
    },
}

/// Restoring-beam request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RestoringBeamMode {
    /// Fit each plane independently.
    #[default]
    PerPlane,
    /// Fit one common beam.
    Common,
}

/// One Gaussian UV-taper axis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UvTaperSize {
    /// Image-domain FWHM in radians.
    ImageFwhmRad(f64),
    /// Baseline-domain HWHM in wavelengths.
    BaselineHwhmLambda(f64),
}

/// Gaussian UV-taper request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GaussianUvTaper {
    /// Major-axis size.
    pub major: UvTaperSize,
    /// Minor-axis size.
    pub minor: UvTaperSize,
    /// Position angle in radians.
    pub position_angle_rad: f64,
}

/// Högbom iteration-accounting request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HogbomIterationMode {
    /// Strict iteration cap.
    Strict,
    /// CASA-inclusive historical accounting.
    CasaInclusive,
}

/// Minor-cycle solver request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deconvolver {
    /// Högbom point components.
    Hogbom,
    /// Multi-term MFS.
    Mtmfs,
    /// Clark point components.
    Clark,
    /// Multiscale components.
    Multiscale,
}

/// W-term correction request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WTermMode {
    /// No explicit correction.
    None,
    /// Direct correction.
    Direct,
    /// W-projection.
    WProject,
}

/// A/W-projection normalization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwProjectNormalization {
    /// Flat-noise normalization.
    FlatNoise,
    /// Flat-sky normalization.
    FlatSky,
    /// Primary-beam-squared normalization.
    PbSquare,
}

/// A/W-projection task controls retained until their native owner lands.
#[derive(Debug, Clone, PartialEq)]
pub struct AwProjectControls {
    /// Convolution-function cache path.
    pub cf_cache: PathBuf,
    /// Requested resident cache ceiling.
    pub cf_resident_bytes: usize,
    /// Explicit W-plane count.
    pub w_plane_count: Option<usize>,
    /// Optional PSF phase centre in radians.
    pub psf_phase_center_direction_rad: Option<[f64; 2]>,
    /// Optional voltage-pattern table.
    pub vp_table: Option<PathBuf>,
    /// Enable the aperture term.
    pub a_term: bool,
    /// Enable the prolate-spheroidal term.
    pub ps_term: bool,
    /// Enable wideband A-projection.
    pub wb_awp: bool,
    /// Select conjugate-frequency beams.
    pub conjugate_beams: bool,
    /// Parallactic-angle computation step in degrees.
    pub compute_pa_step_deg: f64,
    /// Parallactic-angle rotation step in degrees.
    pub rotate_pa_step_deg: f64,
    /// Pointing-offset standard deviations in arcseconds.
    pub pointing_offset_sigdev: Vec<f64>,
    /// Use POINTING-table directions.
    pub use_pointing: bool,
    /// Enable mosaic weight-density behavior.
    pub mosaic_weighting: bool,
    /// Requested image normalization.
    pub normalization: AwProjectNormalization,
}

impl AwProjectControls {
    /// CASA task defaults, transported without executing A/W projection.
    pub fn casa_defaults(cf_cache: PathBuf) -> Self {
        Self {
            cf_cache,
            cf_resident_bytes: 256 * 1024 * 1024,
            w_plane_count: None,
            psf_phase_center_direction_rad: None,
            vp_table: None,
            a_term: true,
            ps_term: false,
            wb_awp: true,
            conjugate_beams: true,
            compute_pa_step_deg: 360.0,
            rotate_pa_step_deg: 360.0,
            pointing_offset_sigdev: vec![0.0],
            use_pointing: false,
            mosaic_weighting: false,
            normalization: AwProjectNormalization::FlatNoise,
        }
    }
}

/// Final native continuum stop reason projected to the task surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanStopReason {
    /// Global threshold satisfied.
    GlobalThresholdReached,
    /// Noise-derived threshold satisfied.
    NsigmaThresholdReached,
    /// Current cycle threshold satisfied.
    CycleThresholdReached,
    /// Iteration budget exhausted.
    IterationLimitReached,
    /// Major-cycle budget exhausted.
    MajorCycleLimitReached,
    /// No eligible pixel remained.
    NoCleanablePixels,
    /// Residual behavior indicated divergence.
    DivergenceDetected,
}
