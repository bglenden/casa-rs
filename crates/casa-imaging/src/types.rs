// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public request/result types for the pure imaging core.

use std::time::Duration;

use ndarray::{Array2, Array4};
use num_complex::Complex32;

use crate::ImagingError;

/// Fixed CASA-style axis ordering for persisted products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisKind {
    /// The first direction axis, persisted as right ascension / longitude.
    RightAscension,
    /// The second direction axis, persisted as declination / latitude.
    Declination,
    /// The degenerate scalar Stokes axis retained for CASA-compatible output.
    Stokes,
    /// The degenerate scalar spectral axis retained for CASA-compatible output.
    Frequency,
}

/// Supported scalar imaging planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneStokes {
    /// Strict Stokes I derived from paired unflagged parallel hands.
    I,
    /// Stokes Q derived from the appropriate paired correlations.
    Q,
    /// Stokes U derived from the appropriate paired correlations.
    U,
    /// Stokes V derived from the appropriate paired correlations.
    V,
    /// Explicit `XX` imaging without Stokes collapse.
    XX,
    /// Explicit `YY` imaging without Stokes collapse.
    YY,
    /// Explicit `RR` imaging without Stokes collapse.
    RR,
    /// Explicit `LL` imaging without Stokes collapse.
    LL,
}

impl PlaneStokes {
    /// Returns the canonical CASA/FITS label for this plane.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::I => "I",
            Self::Q => "Q",
            Self::U => "U",
            Self::V => "V",
            Self::XX => "XX",
            Self::YY => "YY",
            Self::RR => "RR",
            Self::LL => "LL",
        }
    }
}

/// Weighting modes supported by the imaging core.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightingMode {
    /// Natural weighting using the supplied per-sample weights.
    Natural,
    /// Uniform weighting using a CASA-style local density grid.
    Uniform,
    /// Briggs robust weighting using CASA-style `rmode='norm'` scaling.
    Briggs {
        /// Robustness parameter in the usual CASA range `[-2, 2]`.
        robust: f32,
    },
    /// Briggs robust weighting using CASA `rmode='bwtaper'` bandwidth tapering.
    BriggsBwTaper {
        /// Robustness parameter in the usual CASA range `[-2, 2]`.
        robust: f32,
    },
}

impl WeightingMode {
    pub(crate) fn validate(self) -> Result<(), ImagingError> {
        match self {
            Self::Natural | Self::Uniform => Ok(()),
            Self::Briggs { robust } | Self::BriggsBwTaper { robust }
                if robust.is_finite() && (-2.0..=2.0).contains(&robust) =>
            {
                Ok(())
            }
            Self::Briggs { .. } | Self::BriggsBwTaper { .. } => Err(ImagingError::InvalidRequest(
                "Briggs robust must be finite and in the interval [-2, 2]".to_string(),
            )),
        }
    }
}

/// Imaging gridder family used for one MFS plane.
#[derive(Debug, Clone, PartialEq)]
pub enum GridderMode {
    /// CASA `gridder='standard'`.
    Standard,
    /// CASA `gridder='mosaic'` for homogeneous primary-beam aware imaging.
    Mosaic(MosaicGridderConfig),
}

/// One homogeneous primary-beam model usable by the mosaic dirty gridder.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PrimaryBeamModel {
    /// Circular Airy voltage pattern with optional central blockage.
    Airy {
        /// Dish diameter in meters.
        dish_diameter_m: f64,
        /// Central blockage diameter in meters.
        blockage_diameter_m: f64,
    },
    /// CASA `PBMath1DEVLA` common primary-beam voltage model.
    EvlaLBandCommon,
}

impl PrimaryBeamModel {
    fn validate(self) -> Result<(), ImagingError> {
        match self {
            Self::Airy {
                dish_diameter_m,
                blockage_diameter_m,
            } => {
                if !(dish_diameter_m.is_finite() && dish_diameter_m > 0.0) {
                    return Err(ImagingError::InvalidRequest(
                        "primary-beam dish diameter must be finite and > 0 m".to_string(),
                    ));
                }
                if !(blockage_diameter_m.is_finite() && blockage_diameter_m >= 0.0) {
                    return Err(ImagingError::InvalidRequest(
                        "primary-beam blockage diameter must be finite and >= 0 m".to_string(),
                    ));
                }
                if blockage_diameter_m >= dish_diameter_m {
                    return Err(ImagingError::InvalidRequest(
                        "primary-beam blockage diameter must be smaller than the dish diameter"
                            .to_string(),
                    ));
                }
                Ok(())
            }
            Self::EvlaLBandCommon => Ok(()),
        }
    }
}

/// Per-sample metadata aligned with one scalar visibility batch.
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilityMetadataBatch {
    /// World frequency in Hz associated with each scalar sample.
    pub sample_frequency_hz: Vec<f64>,
    /// CASA-style PB/conv-function frequency bucket in Hz for each scalar
    /// sample.
    pub beam_frequency_hz: Vec<f64>,
    /// Primary-beam model for this homogeneous metadata batch.
    pub primary_beam_model: PrimaryBeamModel,
    /// Beam-center direction `[ra, dec]` in radians for each sample.
    ///
    /// For the current `gridder='mosaic'` parity path this follows the row's
    /// FIELD phase center because the source-of-truth runs use
    /// `usepointing=False`.
    pub pointing_direction_rad: Vec<[f64; 2]>,
}

impl VisibilityMetadataBatch {
    pub(crate) fn validate_len(&self, expected: usize) -> Result<(), ImagingError> {
        for (label, len) in [
            ("sample_frequency_hz", self.sample_frequency_hz.len()),
            ("beam_frequency_hz", self.beam_frequency_hz.len()),
            ("pointing_direction_rad", self.pointing_direction_rad.len()),
        ] {
            if len != expected {
                return Err(ImagingError::InvalidRequest(format!(
                    "visibility metadata batch length mismatch: visibility={expected}, {label}={len}"
                )));
            }
        }
        for frequency_hz in &self.sample_frequency_hz {
            if !(frequency_hz.is_finite() && *frequency_hz > 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "visibility metadata frequencies must be finite positive Hz".to_string(),
                ));
            }
        }
        for frequency_hz in &self.beam_frequency_hz {
            if !(frequency_hz.is_finite() && *frequency_hz > 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "visibility metadata beam frequencies must be finite positive Hz".to_string(),
                ));
            }
        }
        self.primary_beam_model.validate()?;
        for direction in &self.pointing_direction_rad {
            if !(direction[0].is_finite() && direction[1].is_finite()) {
                return Err(ImagingError::InvalidRequest(
                    "visibility metadata pointing directions must be finite radians".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Additional request state needed by the mosaic dirty gridder.
#[derive(Debug, Clone, PartialEq)]
pub struct MosaicGridderConfig {
    /// Image phase-center direction `[ra, dec]` in radians.
    pub phase_center_direction_rad: [f64; 2],
    /// Homogeneous primary-beam model shared by the selected data.
    pub primary_beam_model: PrimaryBeamModel,
    /// Minimum normalized primary-beam response allowed during flat-noise
    /// normalization.
    pub pb_limit: f32,
    /// Per-batch metadata aligned with [`ImagingRequest::visibility_batches`].
    pub metadata_batches: Vec<VisibilityMetadataBatch>,
}

impl MosaicGridderConfig {
    fn validate(&self, visibility_batches: &[VisibilityBatch]) -> Result<(), ImagingError> {
        if !(self.phase_center_direction_rad[0].is_finite()
            && self.phase_center_direction_rad[1].is_finite())
        {
            return Err(ImagingError::InvalidRequest(
                "mosaic phase-center direction must be finite radians".to_string(),
            ));
        }
        self.primary_beam_model.validate()?;
        if !(self.pb_limit.is_finite() && self.pb_limit != 0.0) {
            return Err(ImagingError::InvalidRequest(
                "mosaic pb_limit must be finite and non-zero".to_string(),
            ));
        }
        if self.metadata_batches.len() != visibility_batches.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "mosaic metadata batch count {} does not match visibility batch count {}",
                self.metadata_batches.len(),
                visibility_batches.len()
            )));
        }
        for (batch, metadata) in visibility_batches.iter().zip(self.metadata_batches.iter()) {
            metadata.validate_len(batch.len())?;
        }
        Ok(())
    }
}

/// How density-based imaging weights are accumulated for spectral cubes.
///
/// CASA exposes this as `perchanweightdensity`. When enabled, each output
/// spectral plane derives uniform/Briggs density weights from only the samples
/// that contribute to that plane. When disabled, all selected cube samples
/// contribute to a shared density estimate reused by every plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WeightDensityMode {
    /// Use one shared density estimate across the whole selected cube.
    #[default]
    Combined,
    /// Recompute density weights independently for each output plane.
    PerPlane,
}

/// Restoring-beam policy for restored image products.
///
/// CASA exposes this via `restoringbeam=''` for per-plane fitted beams and
/// `restoringbeam='common'` for a single minimum-area enclosing beam shared by
/// every spectral plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestoringBeamMode {
    /// Fit and use an independent restoring beam for each plane.
    #[default]
    PerPlane,
    /// Use one common enclosing restoring beam across the full cube.
    Common,
}

/// One axis length for a CASA-style Gaussian UV taper.
///
/// CASA accepts taper sizes either as angular image-domain FWHM values
/// (for example `50arcsec`) or as UV-domain half-width-at-half-maximum values
/// in wavelengths (for example `910lambda`). [`GaussianUvTaper`] keeps that
/// distinction explicit so the pure imaging core can apply the same
/// `VisImagingWeight::setFilter` formulas that CASA uses internally.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UvTaperSize {
    /// Image-domain Gaussian FWHM in radians.
    ImageFwhmRad(f64),
    /// UV-domain Gaussian half-width at half maximum in wavelengths.
    BaselineHwhmLambda(f64),
}

impl UvTaperSize {
    fn validate(self, axis: &str) -> Result<(), ImagingError> {
        let value = match self {
            Self::ImageFwhmRad(value) | Self::BaselineHwhmLambda(value) => value,
        };
        if !(value.is_finite() && value > 0.0) {
            return Err(ImagingError::InvalidRequest(format!(
                "{axis} UV taper size must be finite and > 0"
            )));
        }
        Ok(())
    }
}

/// CASA-style Gaussian UV taper applied after imaging-weight calculation.
///
/// This follows `casa::VisImagingWeight::setFilter()` / `filter()`: the taper
/// multiplies already-computed imaging weights by a rotated Gaussian in the UV
/// plane. Position angle uses the same convention as CASA image beams: zero
/// along +y, increasing toward -x.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GaussianUvTaper {
    /// Major-axis taper size.
    pub major: UvTaperSize,
    /// Minor-axis taper size.
    pub minor: UvTaperSize,
    /// Position angle in radians, zero along +y and increasing toward -x.
    pub position_angle_rad: f64,
}

impl GaussianUvTaper {
    pub(crate) fn validate(self) -> Result<(), ImagingError> {
        self.major.validate("major")?;
        self.minor.validate("minor")?;
        if !self.position_angle_rad.is_finite() {
            return Err(ImagingError::InvalidRequest(
                "UV taper position angle must be finite".to_string(),
            ));
        }
        Ok(())
    }
}

/// Compatibility target for the first imaging wave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityMode {
    /// Single-plane `tclean`-style MFS imaging with CASA-compatible products.
    CasaStandardMfs,
}

/// Hogbom minor-cycle iteration accounting policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HogbomIterationMode {
    /// Treat `niter` and `cycleniter` as strict caps on committed components.
    Strict,
    /// Mirror CASA's historical `SDAlgorithmHogbomClean` / `hclean` behavior.
    ///
    /// CASA passes `siter = 0` and `cycleNiter` to an inclusive Fortran
    /// `do iter = siter, niter` loop, so one minor-cycle call can commit one
    /// extra component while reporting `iterdone == cycleNiter`.
    CasaInclusive,
}

/// One output-model channel contribution used during cube degridding.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeModelChannelContribution {
    /// Output-model channel used while degridding this sample.
    pub model_channel_index: usize,
    /// Linear interpolation factor applied to that model channel.
    pub factor: f32,
}

/// Per-sample cube-model interpolation state aligned with one visibility batch.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeModelInterpolationBatch {
    /// Model-channel contributions for each scalar sample in the paired
    /// visibility batch.
    pub sample_contributions: Vec<Vec<CubeModelChannelContribution>>,
}

/// One spectral plane of a cube-imaging request.
///
/// Each entry carries the already-selected scalar visibility batches for one
/// output spectral plane, along with the cube-model interpolation state needed
/// by the CASA-style major cycle when predicting visibilities for that plane.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeChannelRequest {
    /// World frequency in Hz for this output spectral plane.
    pub channel_frequency_hz: f64,
    /// Chunked scalar visibility samples for this spectral plane.
    pub visibility_batches: Vec<VisibilityBatch>,
    /// Optional source-channel samples used only to build per-plane cube
    /// weighting density.
    pub density_batches: Vec<VisibilityBatch>,
    /// Per-sample model-channel interpolation state used during cube
    /// prediction and residual refresh.
    pub model_interpolation_batches: Vec<CubeModelInterpolationBatch>,
}

impl CubeChannelRequest {
    pub(crate) fn validate(&self, require_model_interpolation: bool) -> Result<(), ImagingError> {
        if !(self.channel_frequency_hz.is_finite() && self.channel_frequency_hz > 0.0) {
            return Err(ImagingError::InvalidRequest(
                "cube channel frequencies must be finite positive Hz".to_string(),
            ));
        }
        if self.visibility_batches.is_empty() {
            return Err(ImagingError::InvalidRequest(
                "each cube channel requires at least one visibility batch".to_string(),
            ));
        }
        for batch in &self.density_batches {
            batch.validate()?;
        }
        for batch in &self.visibility_batches {
            batch.validate()?;
        }
        if self.model_interpolation_batches.is_empty() && !require_model_interpolation {
            return Ok(());
        }
        if self.model_interpolation_batches.len() != self.visibility_batches.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "cube model interpolation batch count {} does not match visibility batch count {}",
                self.model_interpolation_batches.len(),
                self.visibility_batches.len()
            )));
        }
        for (batch_index, (batch, interpolation)) in self
            .visibility_batches
            .iter()
            .zip(self.model_interpolation_batches.iter())
            .enumerate()
        {
            if interpolation.sample_contributions.len() != batch.len() {
                return Err(ImagingError::InvalidRequest(format!(
                    "cube model interpolation batch {batch_index} length {} does not match visibility batch length {}",
                    interpolation.sample_contributions.len(),
                    batch.len()
                )));
            }
            for (sample_index, sample_contributions) in
                interpolation.sample_contributions.iter().enumerate()
            {
                for contribution in sample_contributions {
                    if !(contribution.factor.is_finite() && contribution.factor >= 0.0) {
                        return Err(ImagingError::InvalidRequest(format!(
                            "cube model interpolation factor at batch {batch_index} sample {sample_index} must be finite and >= 0"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Minor-cycle deconvolver requested for the Cotton-Schwab controller.
///
/// The first implementation wave only executes [`Self::Hogbom`], but the
/// request contract carries the solver choice now so the major/minor-cycle
/// controller can grow without changing the top-level imaging API again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deconvolver {
    /// Point-component Hogbom minor cycle.
    Hogbom,
    /// Multi-term multi-frequency synthesis minor cycle.
    ///
    /// This solver is only valid for `specmode='mfs'` requests that also
    /// provide an explicit Taylor-term count via [`MtmfsRequest`].
    Mtmfs,
    /// Clark point-source minor cycle.
    ///
    /// This variant is reserved for the next wave and currently returns
    /// [`ImagingError::Unsupported`].
    Clark,
    /// Multiscale minor cycle for extended emission.
    ///
    /// This variant is reserved for the next wave and currently returns
    /// [`ImagingError::Unsupported`].
    Multiscale,
}

/// `w`-term handling mode for the pure imaging engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WTermMode {
    /// Standard 2-D imaging with no explicit non-coplanar correction.
    None,
    /// Slow but direct per-sample `w`-term correction in the Fourier sum.
    Direct,
    /// `wproject`-style non-coplanar correction request.
    ///
    /// The first Rust implementation routes this through the same exact
    /// per-sample backend as [`Self::Direct`] so higher-level parity work can
    /// land before the faster approximate kernel.
    WProject,
}

/// Two-dimensional image geometry for the MFS image plane.
///
/// The public contract is explicit about the image-plane sampling even though
/// the persisted CASA products retain degenerate Stokes and Frequency axes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImageGeometry {
    /// `[nx, ny]` image shape in pixels.
    pub image_shape: [usize; 2],
    /// `[dx, dy]` pixel size in radians. Both values are positive in the pure core.
    pub cell_size_rad: [f64; 2],
}

impl ImageGeometry {
    /// Returns the image width in pixels.
    pub fn nx(self) -> usize {
        self.image_shape[0]
    }

    /// Returns the image height in pixels.
    pub fn ny(self) -> usize {
        self.image_shape[1]
    }

    /// Returns the largest angular extent of the requested image in radians.
    pub fn field_of_view_rad(self) -> f64 {
        let x = self.nx() as f64 * self.cell_size_rad[0];
        let y = self.ny() as f64 * self.cell_size_rad[1];
        x.max(y)
    }

    pub(crate) fn validate(self) -> Result<(), ImagingError> {
        if self.nx() < 8 || self.ny() < 8 {
            return Err(ImagingError::InvalidRequest(
                "image shape must be at least 8x8".to_string(),
            ));
        }
        if !(self.cell_size_rad[0].is_finite()
            && self.cell_size_rad[0] > 0.0
            && self.cell_size_rad[1].is_finite()
            && self.cell_size_rad[1] > 0.0)
        {
            return Err(ImagingError::InvalidRequest(
                "cell sizes must be finite positive radians".to_string(),
            ));
        }
        Ok(())
    }
}

/// Chunkable scalar visibility samples consumed by the imaging core.
///
/// Each row is one already-selected scalar visibility sample in wavelength
/// coordinates. Samples can represent strict Stokes-I visibilities or explicit
/// single-correlation imaging planes such as `XX` or `RR`.
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilityBatch {
    /// Baseline `u` coordinate in wavelengths for each scalar sample.
    pub u_lambda: Vec<f64>,
    /// Baseline `v` coordinate in wavelengths for each scalar sample.
    pub v_lambda: Vec<f64>,
    /// Baseline `w` coordinate in wavelengths for each scalar sample.
    pub w_lambda: Vec<f64>,
    /// Natural-imaging sample weight for each scalar sample.
    pub weight: Vec<f32>,
    /// Logical multiplicity factor for the CASA-style reported `sumwt` product.
    ///
    /// The pure core grids conjugate-mirrored samples explicitly for FFT-based
    /// normalization, but the persisted `.sumwt` contract is not always the
    /// same as that mirrored normalization weight. For example, explicit raw
    /// `XX` imaging reports one logical sample weight, while strict Stokes-I
    /// collapse reports the paired-hand logical contribution.
    pub sumwt_factor: Vec<f32>,
    /// Whether each sample participates in image/PSF gridding.
    ///
    /// CASA's standard weighting path can derive imaging weights from a wider
    /// logical sample set than the final gridder actually accepts. For example,
    /// with `useautocorr=false`, autocorrelations contribute to the old
    /// `VisImagingWeight` density calculation but are later rejected by
    /// `GridFT` during gridding and `sumwt` accumulation.
    pub gridable: Vec<bool>,
    /// Complex scalar visibility for each sample.
    pub visibility: Vec<Complex32>,
}

impl VisibilityBatch {
    /// Returns the number of scalar samples in the batch.
    pub fn len(&self) -> usize {
        self.visibility.len()
    }

    /// Returns `true` when the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.visibility.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<(), ImagingError> {
        let expected = self.visibility.len();
        for (label, len) in [
            ("u_lambda", self.u_lambda.len()),
            ("v_lambda", self.v_lambda.len()),
            ("w_lambda", self.w_lambda.len()),
            ("weight", self.weight.len()),
            ("sumwt_factor", self.sumwt_factor.len()),
            ("gridable", self.gridable.len()),
        ] {
            if len != expected {
                return Err(ImagingError::InvalidRequest(format!(
                    "visibility batch length mismatch: visibility={expected}, {label}={len}"
                )));
            }
        }
        Ok(())
    }
}

/// Columnar paired parallel-hand samples used to derive strict Stokes-I input.
///
/// Adapters such as the MeasurementSet frontend gather the relevant `XX`+`YY`
/// or `RR`+`LL` samples into this columnar form, then let the imaging core own
/// the strict flag and averaging semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct ParallelHandBatch {
    /// Baseline `u` coordinate in wavelengths for each logical paired sample.
    pub u_lambda: Vec<f64>,
    /// Baseline `v` coordinate in wavelengths for each logical paired sample.
    pub v_lambda: Vec<f64>,
    /// Baseline `w` coordinate in wavelengths for each logical paired sample.
    pub w_lambda: Vec<f64>,
    /// First parallel hand, for example `XX` or `RR`.
    pub first_visibility: Vec<Complex32>,
    /// Second parallel hand, for example `YY` or `LL`.
    pub second_visibility: Vec<Complex32>,
    /// Weight associated with the first hand.
    pub first_weight: Vec<f32>,
    /// Weight associated with the second hand.
    pub second_weight: Vec<f32>,
    /// Flag state for the first hand.
    pub first_flagged: Vec<bool>,
    /// Flag state for the second hand.
    pub second_flagged: Vec<bool>,
    /// Whether each paired logical sample participates in final gridding.
    pub gridable: Vec<bool>,
}

impl ParallelHandBatch {
    /// Returns the number of logical paired samples in the batch.
    pub fn len(&self) -> usize {
        self.first_visibility.len()
    }

    /// Returns whether the batch contains no logical paired samples.
    pub fn is_empty(&self) -> bool {
        self.first_visibility.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<(), ImagingError> {
        let expected = self.len();
        for (label, len) in [
            ("u_lambda", self.u_lambda.len()),
            ("v_lambda", self.v_lambda.len()),
            ("w_lambda", self.w_lambda.len()),
            ("second_visibility", self.second_visibility.len()),
            ("first_weight", self.first_weight.len()),
            ("second_weight", self.second_weight.len()),
            ("first_flagged", self.first_flagged.len()),
            ("second_flagged", self.second_flagged.len()),
            ("gridable", self.gridable.len()),
        ] {
            if len != expected {
                return Err(ImagingError::InvalidRequest(format!(
                    "parallel-hand batch length mismatch: first_visibility={expected}, {label}={len}"
                )));
            }
        }
        Ok(())
    }

    /// Collapses unflagged paired parallel hands into strict Stokes-I samples.
    ///
    /// The current contract is intentionally strict: if either hand is flagged,
    /// that logical sample is dropped instead of silently falling back to a
    /// pseudo-I estimate.
    pub fn collapse_to_stokes_i(&self) -> Result<VisibilityBatch, ImagingError> {
        self.validate()?;

        let mut u_lambda = Vec::with_capacity(self.len());
        let mut v_lambda = Vec::with_capacity(self.len());
        let mut w_lambda = Vec::with_capacity(self.len());
        let mut weight = Vec::with_capacity(self.len());
        let mut sumwt_factor = Vec::with_capacity(self.len());
        let mut gridable = Vec::with_capacity(self.len());
        let mut visibility = Vec::with_capacity(self.len());

        for index in 0..self.len() {
            if self.first_flagged[index] || self.second_flagged[index] {
                continue;
            }
            let first_weight = self.first_weight[index];
            let second_weight = self.second_weight[index];
            if !(first_weight.is_finite()
                && first_weight > 0.0
                && second_weight.is_finite()
                && second_weight > 0.0)
            {
                continue;
            }
            let vis = (self.first_visibility[index] + self.second_visibility[index]) * 0.5;
            if !(vis.re.is_finite() && vis.im.is_finite()) {
                continue;
            }
            // Keep the visibility collapse as the strict Stokes-I half-sum, but
            // match CASA's natural-weight normalization by averaging the paired
            // parallel-hand weights into a single logical sample weight.
            let combined_weight = 0.5 * (first_weight + second_weight);
            if !(combined_weight.is_finite() && combined_weight > 0.0) {
                continue;
            }

            u_lambda.push(self.u_lambda[index]);
            v_lambda.push(self.v_lambda[index]);
            w_lambda.push(self.w_lambda[index]);
            weight.push(combined_weight);
            sumwt_factor.push(2.0);
            gridable.push(self.gridable[index]);
            visibility.push(vis);
        }

        let collapsed = VisibilityBatch {
            u_lambda,
            v_lambda,
            w_lambda,
            weight,
            sumwt_factor,
            gridable,
            visibility,
        };
        collapsed.validate()?;
        Ok(collapsed)
    }
}

/// CLEAN controls for the Cotton-Schwab major/minor-cycle controller.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CleanConfig {
    /// Maximum number of reported minor-cycle iterations.
    pub niter: usize,
    /// Maximum number of major-cycle residual refreshes after the initial
    /// residual calculation. `None` follows CASA's default unlimited policy.
    pub major_cycle_limit: Option<usize>,
    /// Loop gain applied to each selected component.
    pub gain: f32,
    /// Absolute stopping threshold in `Jy/beam`.
    pub threshold_jy_per_beam: f32,
    /// CASA-style robust-RMS stopping multiplier.
    ///
    /// When positive, the controller derives an additional stopping threshold
    /// as `nsigma * robust_rms` and uses the larger of that value and the
    /// standard `cyclethreshold` during minor cycles.
    pub nsigma: f32,
    /// Main-lobe cutoff used by the restoring-beam fit.
    pub psf_cutoff: f32,
    /// Maximum number of minor-cycle updates between residual refreshes.
    pub minor_cycle_length: usize,
    /// CASA-style cycle threshold scale relative to the PSF sidelobe level.
    pub cyclefactor: f32,
    /// Lower clamp for the PSF fraction used to derive `cyclethreshold`.
    pub min_psf_fraction: f32,
    /// Upper clamp for the PSF fraction used to derive `cyclethreshold`.
    pub max_psf_fraction: f32,
    /// Hogbom iteration accounting policy.
    pub hogbom_iteration_mode: HogbomIterationMode,
}

impl Default for CleanConfig {
    fn default() -> Self {
        Self {
            niter: 0,
            major_cycle_limit: None,
            gain: 0.1,
            threshold_jy_per_beam: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::Strict,
        }
    }
}

/// Final reason why the Hogbom minor/major-cycle loop stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanStopReason {
    /// The requested global CLEAN threshold was already satisfied.
    GlobalThresholdReached,
    /// The requested robust-RMS-derived `nsigma` threshold was satisfied.
    NsigmaThresholdReached,
    /// The current minor cycle hit its CASA-style `cyclethreshold`.
    CycleThresholdReached,
    /// The requested total iteration budget was exhausted.
    IterationLimitReached,
    /// The requested major-cycle budget was exhausted.
    MajorCycleLimitReached,
    /// No cleanable masked pixel was available for the current request.
    NoCleanablePixels,
    /// The residual peak increased materially after prior progress.
    DivergenceDetected,
}

impl CleanConfig {
    pub(crate) fn validate(self) -> Result<(), ImagingError> {
        if !(self.gain.is_finite() && self.gain > 0.0 && self.gain <= 1.0) {
            return Err(ImagingError::InvalidRequest(
                "gain must be finite and in the interval (0, 1]".to_string(),
            ));
        }
        if !(self.threshold_jy_per_beam.is_finite() && self.threshold_jy_per_beam >= 0.0) {
            return Err(ImagingError::InvalidRequest(
                "threshold must be finite and >= 0 Jy/beam".to_string(),
            ));
        }
        if !(self.nsigma.is_finite() && self.nsigma >= 0.0) {
            return Err(ImagingError::InvalidRequest(
                "nsigma must be finite and >= 0".to_string(),
            ));
        }
        if !(self.psf_cutoff.is_finite() && self.psf_cutoff > 0.0 && self.psf_cutoff < 1.0) {
            return Err(ImagingError::InvalidRequest(
                "psf_cutoff must be finite and in the interval (0, 1)".to_string(),
            ));
        }
        if self.minor_cycle_length == 0 {
            return Err(ImagingError::InvalidRequest(
                "minor_cycle_length must be at least 1".to_string(),
            ));
        }
        if !(self.cyclefactor.is_finite() && self.cyclefactor > 0.0) {
            return Err(ImagingError::InvalidRequest(
                "cyclefactor must be finite and > 0".to_string(),
            ));
        }
        if !(self.min_psf_fraction.is_finite()
            && self.min_psf_fraction >= 0.0
            && self.min_psf_fraction <= 1.0)
        {
            return Err(ImagingError::InvalidRequest(
                "min_psf_fraction must be finite and in the interval [0, 1]".to_string(),
            ));
        }
        if !(self.max_psf_fraction.is_finite()
            && self.max_psf_fraction >= 0.0
            && self.max_psf_fraction <= 1.0)
        {
            return Err(ImagingError::InvalidRequest(
                "max_psf_fraction must be finite and in the interval [0, 1]".to_string(),
            ));
        }
        if self.min_psf_fraction > self.max_psf_fraction {
            return Err(ImagingError::InvalidRequest(
                "min_psf_fraction must be <= max_psf_fraction".to_string(),
            ));
        }
        Ok(())
    }
}

/// Top-level request consumed by the pure imaging engine.
#[derive(Debug, Clone, PartialEq)]
pub struct ImagingRequest {
    /// Requested image geometry for the MFS image plane.
    pub geometry: ImageGeometry,
    /// Chunked scalar visibility samples to grid and deconvolve.
    pub visibility_batches: Vec<VisibilityBatch>,
    /// Requested gridder family and any additional gridder-specific metadata.
    pub gridder_mode: GridderMode,
    /// Scalar imaging plane to produce.
    pub plane_stokes: PlaneStokes,
    /// Weighting policy used by the run.
    pub weighting: WeightingMode,
    /// Reference frequency in Hz for metadata and diagnostics.
    pub reffreq_hz: f64,
    /// Inclusive selected frequency range in Hz.
    pub selected_frequency_range_hz: [f64; 2],
    /// Requested minor-cycle deconvolver.
    pub deconvolver: Deconvolver,
    /// Requested multiscale kernel sizes in pixels.
    ///
    /// This is ignored by point-source deconvolvers. Under
    /// [`Deconvolver::Multiscale`], an empty list defaults internally to the
    /// CASA-style single point scale `[0]`.
    pub multiscale_scales: Vec<f32>,
    /// CASA-style multiscale selection bias.
    ///
    /// CASA multiplies each scale's smoothed residual peak by
    /// `1 - smallscalebias * scale/maxscale` before selecting the best scale.
    /// `0.0` gives no bias, positive values prefer smaller scales, and
    /// negative values prefer larger scales.
    pub small_scale_bias: f32,
    /// Deconvolver-independent CLEAN and major/minor-cycle controls.
    pub clean: CleanConfig,
    /// Optional image-plane clean mask. `true` pixels are eligible for component picks.
    pub clean_mask: Option<Array2<bool>>,
    /// Optional starting model image used to seed the CLEAN model plane.
    pub initial_model: Option<Array2<f32>>,
    /// Requested `w`-term handling mode.
    pub w_term_mode: WTermMode,
    /// Optional explicit `wproject` plane budget.
    ///
    /// When set, [`WTermMode::WProject`] uses exactly this many planes instead
    /// of the internal auto estimate. This matches CASA's `wprojplanes`
    /// control. Ignored for other `w`-term modes.
    pub w_project_planes: Option<usize>,
    /// Declared compatibility target for the run.
    pub compatibility: CompatibilityMode,
}

impl ImagingRequest {
    pub(crate) fn validate(&self) -> Result<(), ImagingError> {
        self.geometry.validate()?;
        self.weighting.validate()?;
        if let GridderMode::Mosaic(config) = &self.gridder_mode {
            config.validate(&self.visibility_batches)?;
        }
        if !(self.reffreq_hz.is_finite() && self.reffreq_hz > 0.0) {
            return Err(ImagingError::InvalidRequest(
                "reffreq_hz must be a finite positive frequency".to_string(),
            ));
        }
        if !(self.selected_frequency_range_hz[0].is_finite()
            && self.selected_frequency_range_hz[1].is_finite()
            && self.selected_frequency_range_hz[0] > 0.0
            && self.selected_frequency_range_hz[1] >= self.selected_frequency_range_hz[0])
        {
            return Err(ImagingError::InvalidRequest(
                "selected_frequency_range_hz must be a finite ordered positive range".to_string(),
            ));
        }
        self.clean.validate()?;
        if let Some(initial_model) = self.initial_model.as_ref() {
            let expected = (self.geometry.image_shape[0], self.geometry.image_shape[1]);
            if initial_model.dim() != expected {
                return Err(ImagingError::InvalidRequest(format!(
                    "initial_model shape {:?} does not match requested image shape {:?}",
                    initial_model.dim(),
                    expected
                )));
            }
            if initial_model.iter().any(|value| !value.is_finite()) {
                return Err(ImagingError::InvalidRequest(
                    "initial_model contains non-finite pixels".to_string(),
                ));
            }
        }
        for scale in &self.multiscale_scales {
            if !(scale.is_finite() && *scale >= 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "multiscale scales must be finite and >= 0 pixels".to_string(),
                ));
            }
        }
        if !(self.small_scale_bias.is_finite() && (-1.0..=1.0).contains(&self.small_scale_bias)) {
            return Err(ImagingError::InvalidRequest(
                "small_scale_bias must be finite and in the interval [-1, 1]".to_string(),
            ));
        }
        if let Some(mask) = &self.clean_mask {
            if mask.dim() != (self.geometry.nx(), self.geometry.ny()) {
                return Err(ImagingError::InvalidRequest(format!(
                    "clean mask shape {:?} does not match image shape {:?}",
                    mask.dim(),
                    (self.geometry.nx(), self.geometry.ny())
                )));
            }
        }
        if matches!(self.w_project_planes, Some(0)) {
            return Err(ImagingError::InvalidRequest(
                "w_project_planes must be >= 1 when provided".to_string(),
            ));
        }
        if self.visibility_batches.is_empty() {
            return Err(ImagingError::InvalidRequest(
                "at least one visibility batch is required".to_string(),
            ));
        }
        for batch in &self.visibility_batches {
            batch.validate()?;
        }
        Ok(())
    }
}

/// CASA `auto-multithresh` controls used by the cube CLEAN controller.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CubeAutoMultiThresholdConfig {
    /// Sidelobe threshold factor multiplied by the PSF sidelobe level.
    pub sidelobe_threshold: f32,
    /// Noise threshold factor multiplied by the robust residual RMS.
    pub noise_threshold: f32,
    /// Lower noise threshold factor used when growing an existing mask.
    pub low_noise_threshold: f32,
    /// Negative-feature threshold factor; zero disables negative masks.
    pub negative_threshold: f32,
    /// Smoothing factor, in restoring-beam units, for CASA's mask smoothing
    /// stage.
    pub smooth_factor: f32,
    /// Minimum connected-region size as a fraction of the fitted beam area.
    pub min_beam_frac: f32,
    /// Fraction of the smoothed-mask peak used to cut mask edges.
    pub cut_threshold: f32,
    /// Maximum constrained binary-dilation iterations for later mask updates.
    pub grow_iterations: usize,
    /// Whether grown masks are pruned after dilation.
    pub do_grow_prune: bool,
    /// CASA percent-change stop control for later automask updates.
    pub min_percent_change: f32,
    /// Use CASA's fast-noise statistics path.
    pub fast_noise: bool,
}

impl Default for CubeAutoMultiThresholdConfig {
    fn default() -> Self {
        Self {
            sidelobe_threshold: 3.0,
            noise_threshold: 5.0,
            low_noise_threshold: 1.5,
            negative_threshold: 0.0,
            smooth_factor: 1.0,
            min_beam_frac: 0.3,
            cut_threshold: 0.01,
            grow_iterations: 75,
            do_grow_prune: true,
            min_percent_change: -1.0,
            fast_noise: true,
        }
    }
}

/// Top-level request consumed by the pure imaging engine for spectral cubes.
///
/// Each output spectral plane is imaged independently through the same core
/// controller used for MFS imaging and then stacked on a real spectral axis in
/// CASA ordering. This cleaned-cube wave intentionally stays narrow: runtime
/// Doppler/frame correction is still handled in the frontend adapter, and
/// deconvolution support is currently limited to the point-source
/// deconvolvers Hogbom and Clark.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeImagingRequest {
    /// Requested two-dimensional geometry shared by every spectral plane.
    pub geometry: ImageGeometry,
    /// Ordered spectral planes to image. Their order is preserved in the
    /// output cube's spectral axis.
    pub channels: Vec<CubeChannelRequest>,
    /// Scalar Stokes plane to produce.
    pub plane_stokes: PlaneStokes,
    /// Weighting policy applied independently to each channel plane.
    pub weighting: WeightingMode,
    /// Whether uniform/Briggs density estimates are shared or per plane.
    pub weight_density_mode: WeightDensityMode,
    /// Optional CASA-style Gaussian UV taper applied after weighting.
    pub uv_taper: Option<GaussianUvTaper>,
    /// Restoring-beam policy for the restored cube products.
    pub restoring_beam_mode: RestoringBeamMode,
    /// Requested minor-cycle deconvolver.
    pub deconvolver: Deconvolver,
    /// Requested multiscale kernel sizes in pixels.
    ///
    /// This is ignored by point-source deconvolvers. Under
    /// [`Deconvolver::Multiscale`], an empty list defaults internally to the
    /// CASA-style single point scale `[0]`.
    pub multiscale_scales: Vec<f32>,
    /// CASA-style multiscale selection bias shared by every plane.
    ///
    /// This follows the same semantics as [`ImagingRequest::small_scale_bias`].
    pub small_scale_bias: f32,
    /// Deconvolver-independent CLEAN and major/minor-cycle controls applied to
    /// each spectral plane.
    pub clean: CleanConfig,
    /// Optional image-plane clean mask shared by every spectral plane. `true`
    /// pixels are eligible for component picks.
    pub clean_mask: Option<Array2<bool>>,
    /// Optional CASA-style cube clean mask with shape `(nx, ny, 1, nchan)`.
    ///
    /// This represents masks that differ by output spectral channel, matching
    /// CASA image-mask semantics. When both [`Self::clean_mask`] and this
    /// field are present, a pixel must be true in both masks to be eligible.
    pub channel_clean_mask: Option<Array4<bool>>,
    /// Optional CASA `auto-multithresh` mask updates run inside the cube CLEAN
    /// controller.
    ///
    /// The initial mask is generated from the first residual before any minor
    /// iterations. Later major-cycle residual refreshes update the positive
    /// mask and may grow existing mask regions, matching CASA's
    /// `iterdone > 0` growth gate.
    pub auto_mask: Option<CubeAutoMultiThresholdConfig>,
    /// Restoring-beam fit cutoff used for each channel PSF.
    pub psf_cutoff: f32,
    /// Requested `w`-term handling mode.
    pub w_term_mode: WTermMode,
    /// Optional explicit `wproject` plane budget shared by every plane.
    ///
    /// Ignored unless [`Self::w_term_mode`] is [`WTermMode::WProject`].
    pub w_project_planes: Option<usize>,
    /// Declared compatibility target for the run.
    pub compatibility: CompatibilityMode,
}

impl CubeImagingRequest {
    pub(crate) fn validate(&self) -> Result<(), ImagingError> {
        self.geometry.validate()?;
        self.weighting.validate()?;
        if let Some(taper) = self.uv_taper {
            taper.validate()?;
        }
        self.clean.validate()?;
        if !(self.psf_cutoff.is_finite() && (0.0..1.0).contains(&self.psf_cutoff)) {
            return Err(ImagingError::InvalidRequest(
                "psf_cutoff must be finite and in the interval [0, 1)".to_string(),
            ));
        }
        for scale in &self.multiscale_scales {
            if !(scale.is_finite() && *scale >= 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "multiscale scales must be finite and >= 0 pixels".to_string(),
                ));
            }
        }
        if !(self.small_scale_bias.is_finite() && (-1.0..=1.0).contains(&self.small_scale_bias)) {
            return Err(ImagingError::InvalidRequest(
                "small_scale_bias must be finite and in the interval [-1, 1]".to_string(),
            ));
        }
        if let Some(mask) = &self.clean_mask {
            if mask.dim() != (self.geometry.nx(), self.geometry.ny()) {
                return Err(ImagingError::InvalidRequest(format!(
                    "clean mask shape {:?} does not match image shape {:?}",
                    mask.dim(),
                    (self.geometry.nx(), self.geometry.ny())
                )));
            }
        }
        if let Some(mask) = &self.channel_clean_mask {
            let expected = (
                self.geometry.nx(),
                self.geometry.ny(),
                1,
                self.channels.len(),
            );
            if mask.dim() != expected {
                return Err(ImagingError::InvalidRequest(format!(
                    "channel clean mask shape {:?} does not match cube image shape {:?}",
                    mask.dim(),
                    expected
                )));
            }
        }
        if matches!(self.w_project_planes, Some(0)) {
            return Err(ImagingError::InvalidRequest(
                "w_project_planes must be >= 1 when provided".to_string(),
            ));
        }
        if self.channels.is_empty() {
            return Err(ImagingError::InvalidRequest(
                "cube imaging requires at least one spectral plane".to_string(),
            ));
        }
        let require_model_interpolation = self.clean.niter > 0;
        for channel in &self.channels {
            channel.validate(require_model_interpolation)?;
        }
        Ok(())
    }
}

/// Restoring-beam parameters derived from the PSF main lobe.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BeamFit {
    /// Fitted restoring-beam major-axis FWHM in radians.
    pub major_fwhm_rad: f64,
    /// Fitted restoring-beam minor-axis FWHM in radians.
    pub minor_fwhm_rad: f64,
    /// Fitted position angle in radians, following the casacore GaussianBeam
    /// convention: zero along +y, increasing toward -x.
    pub position_angle_rad: f64,
}

/// One minor-cycle block executed by a CLEAN controller.
///
/// This records the same high-signal state that CASA exposes through
/// `summaryminor`: the controller iteration window, the residual peak before
/// and after the block, the cycle threshold, and the stop code. Multiscale
/// runs additionally record the initial chosen scale/candidate so parity tests
/// can localize the first numerical divergence without adding app-specific
/// debug paths.
#[derive(Debug, Clone, PartialEq)]
pub struct MinorCycleTrace {
    /// Zero-based minor-cycle block index within the run.
    pub cycle_index: usize,
    /// CASA-style reported iteration count at the start of this block.
    pub start_reported_iteration: usize,
    /// CASA-style reported iterations consumed by this block.
    pub reported_updates: usize,
    /// Actual component updates committed in this block.
    pub actual_updates: usize,
    /// Peak absolute residual at the start of the block.
    pub start_peak_residual_jy_per_beam: f32,
    /// Peak absolute residual immediately after the block, before any exact
    /// major-cycle residual refresh.
    pub end_peak_residual_jy_per_beam: f32,
    /// CASA-style `cyclethreshold` supplied to this block.
    pub cycle_threshold_jy_per_beam: f32,
    /// Sum of model pixel values after this block, matching CASA's
    /// `summaryminor.modelFlux` scalar for single-plane deconvolution.
    pub model_flux_jy: f32,
    /// CASA-style per-plane `nsigma` threshold supplied to this block.
    pub nsigma_threshold_jy_per_beam: f32,
    /// Final reason why this block stopped, when one is available.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Initial multiscale scale size in pixels for the chosen candidate.
    pub initial_scale_pixels: Option<f32>,
    /// Initial candidate strength before applying loop gain.
    pub initial_candidate_strength_jy_per_beam: Option<f32>,
    /// Initial candidate pixel position in image coordinates.
    pub initial_candidate_position: Option<[usize; 2]>,
    /// Model center-pixel value immediately after the block.
    pub center_model_value_jy_per_pixel: f32,
    /// Residual center-pixel value immediately after the block.
    pub center_residual_value_jy_per_beam: f32,
}

/// Run-time diagnostics returned with every imaging run.
#[derive(Debug, Clone, PartialEq)]
pub struct ImagingDiagnostics {
    /// Human-readable warnings about approximation limits or rejected modes.
    pub warnings: Vec<String>,
    /// Number of scalar samples that contributed to the gridded products.
    pub gridded_samples: usize,
    /// Number of scalar samples dropped during gridding setup.
    pub skipped_samples: usize,
    /// CASA-style major-cycle count for this plane.
    ///
    /// When CLEAN is requested, this follows CASA's external `nmajordone`
    /// convention and therefore includes the initial residual calculation plus
    /// each subsequent exact residual refresh.
    pub major_cycles: usize,
    /// Number of Hogbom component updates executed.
    ///
    /// With [`HogbomIterationMode::CasaInclusive`], this can exceed the
    /// externally reported [`CleanConfig::niter`] by one per Hogbom minor-cycle
    /// call.
    pub minor_iterations: usize,
    /// Final reason why the Hogbom controller stopped, if CLEAN was requested.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Per-block minor-cycle trace for the controller.
    pub minor_cycle_traces: Vec<MinorCycleTrace>,
    /// Peak absolute residual before CLEAN iterations.
    pub initial_residual_peak_jy_per_beam: f32,
    /// Peak absolute residual after the final refresh.
    pub final_residual_peak_jy_per_beam: f32,
    /// Maximum absolute `w` coordinate seen in the input batches.
    pub max_abs_w_lambda: f64,
    /// Fractional bandwidth `(f_max - f_min) / reffreq`.
    pub fractional_bandwidth: f64,
    /// Estimated maximum absolute PSF sidelobe level outside the main lobe.
    pub max_psf_sidelobe_level: f32,
    /// Final CASA-style `cyclethreshold` used by the minor cycle.
    pub final_cycle_threshold_jy_per_beam: f32,
    /// Number of `true` pixels in the clean mask, or the full image area when unmasked.
    pub clean_mask_pixels: usize,
    /// Number of PSF beam-fit attempts executed.
    pub beam_fit_attempts: usize,
    /// The final `psfcutoff` used by the beam fitter, when the fit succeeded.
    pub beam_fit_cutoff_used: Option<f32>,
    /// Internal beam-fit search summary for the fitted PSF main lobe.
    pub beam_fit_debug: Option<BeamFitDebugSummary>,
    /// Pre-normalization mosaic weight/sensitivity image when the mosaic dirty
    /// path is active. `None` for non-mosaic runs.
    pub mosaic_weight_image: Option<Array2<f32>>,
    /// Stage timings collected while building the products.
    pub stage_timings: ImagingStageTimings,
}

/// Debug summary of the CASA-style PSF beam-fit search and resampling steps.
#[derive(Debug, Clone, PartialEq)]
pub struct BeamFitDebugSummary {
    /// Peak pixel used as the PSF fit center in the input plane.
    pub peak_index: (usize, usize),
    /// Peak PSF value before normalization.
    pub peak_value: f32,
    /// Number of above-threshold samples retained by the first FindNpoints-style pass.
    pub first_pass_points: usize,
    /// Bottom-left corner of the first pass bounding box.
    pub first_pass_blc: (usize, usize),
    /// Top-right corner of the first pass bounding box.
    pub first_pass_trc: (usize, usize),
    /// Expanded square fit window shape before interpolation.
    pub expanded_window_shape: (usize, usize),
    /// Integer oversampling factor applied to the fit window.
    pub oversampling: usize,
    /// Interpolated PSF window shape.
    pub resampled_shape: (usize, usize),
    /// Number of above-threshold samples retained by the second FindNpoints-style pass.
    pub second_pass_points: usize,
    /// Bottom-left corner of the second pass bounding box in interpolated pixels.
    pub second_pass_blc: (usize, usize),
    /// Top-right corner of the second pass bounding box in interpolated pixels.
    pub second_pass_trc: (usize, usize),
}

/// Result of fitting a CASA-style restoring beam to a PSF image plane.
///
/// This mirrors the internal `StokesImageUtil::FitGaussianPSF`-style workflow
/// used by the imaging pipeline and is useful for parity checks against
/// external PSF products.
#[derive(Debug, Clone, PartialEq)]
pub struct PsfBeamFitResult {
    /// Fitted restoring beam, when the nonlinear fit converged.
    pub beam: Option<BeamFit>,
    /// Human-readable warnings gathered while retrying the fit.
    pub warnings: Vec<String>,
    /// Number of fit attempts executed before convergence or failure.
    pub attempts: usize,
    /// Final `psfcutoff` used by the fitter, when available.
    pub cutoff_used: Option<f32>,
    /// Search and interpolation summary for the fitted PSF plane.
    pub debug: Option<BeamFitDebugSummary>,
}

/// Stage-level timing summary for one imaging run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImagingStageTimings {
    /// Controller bookkeeping time outside explicit minor-cycle solves and
    /// major-cycle residual refreshes.
    pub controller_overhead: Duration,
    /// Time spent applying geometry-dependent imaging weights and tapers.
    pub weighting: Duration,
    /// Time spent gridding PSF/sample weights.
    pub psf_grid: Duration,
    /// Time spent FFTing the PSF grid.
    pub psf_fft: Duration,
    /// Time spent applying PSF correction and normalization.
    pub psf_normalize: Duration,
    /// Time spent FFTing model images before degridding.
    pub model_fft: Duration,
    /// Time spent degridding/gridding residual visibilities.
    pub residual_degrid_grid: Duration,
    /// Time spent FFTing residual grids back to image space.
    pub residual_fft: Duration,
    /// Time spent applying residual correction and normalization.
    pub residual_normalize: Duration,
    /// Time spent in minor-cycle PSF subtraction/component updates.
    pub minor_cycle: Duration,
    /// Time spent inside the solver-specific minor-cycle loop.
    pub minor_cycle_solve: Duration,
    /// Time spent recomputing the image residual during major-cycle refreshes.
    ///
    /// This is the aggregate wall time for each residual refresh and therefore
    /// includes the lower-level `model_fft`, `residual_degrid_grid`,
    /// `residual_fft`, and `residual_normalize` subtotals.
    pub major_cycle_refresh: Duration,
    /// Time spent fitting the restoring beam from the PSF.
    pub beam_fit: Duration,
    /// Time spent restoring the component model with the fitted beam.
    pub restore: Duration,
    /// Total end-to-end time spent inside the imaging core.
    pub total: Duration,
}

impl Default for ImagingStageTimings {
    fn default() -> Self {
        Self {
            controller_overhead: Duration::ZERO,
            weighting: Duration::ZERO,
            psf_grid: Duration::ZERO,
            psf_fft: Duration::ZERO,
            psf_normalize: Duration::ZERO,
            model_fft: Duration::ZERO,
            residual_degrid_grid: Duration::ZERO,
            residual_fft: Duration::ZERO,
            residual_normalize: Duration::ZERO,
            minor_cycle: Duration::ZERO,
            minor_cycle_solve: Duration::ZERO,
            major_cycle_refresh: Duration::ZERO,
            beam_fit: Duration::ZERO,
            restore: Duration::ZERO,
            total: Duration::ZERO,
        }
    }
}

/// Declared metadata contract for persisted compatibility products.
#[derive(Debug, Clone, PartialEq)]
pub struct CompatibilityMetadata {
    /// Persisted axis ordering for output products.
    pub axis_order: [AxisKind; 4],
    /// Scalar Stokes plane represented in the products.
    pub plane_stokes: PlaneStokes,
    /// Reference frequency in Hz used for metadata.
    pub reffreq_hz: f64,
    /// Ordered world frequencies in Hz for the persisted spectral axis.
    pub channel_frequencies_hz: Vec<f64>,
    /// Brightness units for the PSF product.
    pub psf_units: String,
    /// Brightness units for the residual product.
    pub residual_units: String,
    /// Brightness units for the model product.
    pub model_units: String,
    /// Brightness units for the restored image product.
    pub image_units: String,
}

/// Result of a complete dirty-imaging / Hogbom run.
#[derive(Debug, Clone, PartialEq)]
pub struct ImagingResult {
    /// Normalized PSF cube with degenerate Stokes/Frequency axes.
    pub psf: Array4<f32>,
    /// Final residual cube with degenerate Stokes/Frequency axes.
    pub residual: Array4<f32>,
    /// Component model cube with degenerate Stokes/Frequency axes.
    pub model: Array4<f32>,
    /// Restored image cube with degenerate Stokes/Frequency axes.
    pub image: Array4<f32>,
    /// CASA-style `sumwt` product stored as a single degenerate pixel.
    pub sumwt: Array4<f32>,
    /// Restoring beam fitted from the PSF, when the fit succeeds.
    pub beam: Option<BeamFit>,
    /// Diagnostics collected while building the products.
    pub diagnostics: ImagingDiagnostics,
    /// Declared metadata contract for downstream persistence.
    pub compatibility: CompatibilityMetadata,
}

/// Top-level request for CASA-style MTMFS imaging.
#[derive(Debug, Clone, PartialEq)]
pub struct MtmfsRequest {
    /// Requested image geometry for the MFS image plane.
    pub geometry: ImageGeometry,
    /// Chunked scalar visibility samples to grid and deconvolve.
    pub visibility_batches: Vec<VisibilityBatch>,
    /// Per-sample world frequency in Hz aligned with each visibility batch.
    pub sample_frequency_batches_hz: Vec<Vec<f64>>,
    /// Requested gridder family and any additional gridder-specific metadata.
    pub gridder_mode: GridderMode,
    /// Scalar imaging plane to produce.
    pub plane_stokes: PlaneStokes,
    /// Weighting policy used by the run.
    pub weighting: WeightingMode,
    /// Reference frequency in Hz for Taylor-basis evaluation and metadata.
    pub reffreq_hz: f64,
    /// Inclusive selected frequency range in Hz.
    pub selected_frequency_range_hz: [f64; 2],
    /// Number of Taylor terms to solve for.
    pub nterms: usize,
    /// Requested multiscale kernel sizes in pixels. An empty list selects
    /// point-source MT-MFS minor-cycle updates.
    pub multiscale_scales: Vec<f32>,
    /// CASA-style multiscale selection bias used when `multiscale_scales` is not empty.
    pub small_scale_bias: f32,
    /// W-term correction mode used for MT-MFS gridding.
    pub w_term_mode: WTermMode,
    /// Optional W-projection plane count. `None` selects the automatic heuristic.
    pub w_project_planes: Option<usize>,
    /// Deconvolver-independent CLEAN and major/minor-cycle controls.
    pub clean: CleanConfig,
    /// Optional image-plane clean mask. `true` pixels are eligible for component picks.
    pub clean_mask: Option<Array2<bool>>,
    /// Declared compatibility target for the run.
    pub compatibility: CompatibilityMode,
}

impl MtmfsRequest {
    pub(crate) fn validate(&self) -> Result<(), ImagingError> {
        self.geometry.validate()?;
        self.weighting.validate()?;
        if !(self.reffreq_hz.is_finite() && self.reffreq_hz > 0.0) {
            return Err(ImagingError::InvalidRequest(
                "reffreq_hz must be a finite positive frequency".to_string(),
            ));
        }
        if !(self.selected_frequency_range_hz[0].is_finite()
            && self.selected_frequency_range_hz[1].is_finite()
            && self.selected_frequency_range_hz[0] > 0.0
            && self.selected_frequency_range_hz[1] >= self.selected_frequency_range_hz[0])
        {
            return Err(ImagingError::InvalidRequest(
                "selected_frequency_range_hz must be a finite ordered positive range".to_string(),
            ));
        }
        if self.nterms == 0 {
            return Err(ImagingError::InvalidRequest(
                "MTMFS requires nterms >= 1".to_string(),
            ));
        }
        for scale in &self.multiscale_scales {
            if !(scale.is_finite() && *scale >= 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "MTMFS multiscale scales must be finite and >= 0".to_string(),
                ));
            }
        }
        if !(self.small_scale_bias.is_finite() && self.small_scale_bias >= 0.0) {
            return Err(ImagingError::InvalidRequest(
                "MTMFS small_scale_bias must be finite and >= 0".to_string(),
            ));
        }
        if self.w_term_mode == WTermMode::Direct {
            return Err(ImagingError::Unsupported(
                "MTMFS currently supports WTermMode::None and WTermMode::WProject".to_string(),
            ));
        }
        self.clean.validate()?;
        if let Some(mask) = &self.clean_mask {
            if mask.dim() != (self.geometry.nx(), self.geometry.ny()) {
                return Err(ImagingError::InvalidRequest(format!(
                    "clean mask shape {:?} does not match image shape {:?}",
                    mask.dim(),
                    (self.geometry.nx(), self.geometry.ny())
                )));
            }
        }
        if self.visibility_batches.is_empty() {
            return Err(ImagingError::InvalidRequest(
                "MTMFS requires at least one visibility batch".to_string(),
            ));
        }
        if self.sample_frequency_batches_hz.len() != self.visibility_batches.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "sample_frequency_batches_hz count {} does not match visibility batch count {}",
                self.sample_frequency_batches_hz.len(),
                self.visibility_batches.len()
            )));
        }
        for (batch_index, (batch, frequencies_hz)) in self
            .visibility_batches
            .iter()
            .zip(self.sample_frequency_batches_hz.iter())
            .enumerate()
        {
            batch.validate()?;
            if frequencies_hz.len() != batch.len() {
                return Err(ImagingError::InvalidRequest(format!(
                    "sample_frequency_batches_hz[{batch_index}] length {} does not match visibility batch length {}",
                    frequencies_hz.len(),
                    batch.len()
                )));
            }
            for frequency_hz in frequencies_hz {
                if !(frequency_hz.is_finite() && *frequency_hz > 0.0) {
                    return Err(ImagingError::InvalidRequest(format!(
                        "MTMFS sample frequency at batch {batch_index} must be finite and > 0 Hz"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Result of a CASA-style MTMFS run.
#[derive(Debug, Clone, PartialEq)]
pub struct MtmfsResult {
    /// Normalized Taylor-term PSF images, with `2*nterms - 1` entries.
    pub psf_terms: Vec<Array4<f32>>,
    /// Final Taylor-term residual images, with `nterms` entries.
    pub residual_terms: Vec<Array4<f32>>,
    /// Taylor-term component model images, with `nterms` entries.
    pub model_terms: Vec<Array4<f32>>,
    /// Restored Taylor-term images, with `nterms` entries.
    pub image_terms: Vec<Array4<f32>>,
    /// CASA-style `sumwt` products, with `2*nterms - 1` entries.
    pub sumwt_terms: Vec<Array4<f32>>,
    /// Derived spectral-index image, when `nterms > 1`.
    pub alpha: Option<Array4<f32>>,
    /// Derived spectral-index error image, when `nterms > 1`.
    pub alpha_error: Option<Array4<f32>>,
    /// Restoring beam fitted from the zeroth-order PSF, when the fit succeeds.
    pub beam: Option<BeamFit>,
    /// Diagnostics collected while building the products.
    pub diagnostics: ImagingDiagnostics,
    /// Declared metadata contract for downstream persistence.
    pub compatibility: CompatibilityMetadata,
}

/// One scalar sample after the weighting seam has produced a final imaging weight.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingSampleDiagnostics {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Input weight before weighting/taper transforms.
    pub input_weight: f32,
    /// Density value sampled for uniform/Briggs weighting, when applicable.
    pub density_weight: Option<f32>,
    /// Final imaging weight after weighting and taper transforms.
    pub output_weight: f32,
    /// CASA-style logical multiplicity factor used for reported `sumwt`.
    pub sumwt_factor: f32,
    /// Whether this sample remains eligible for final gridding.
    pub gridable: bool,
    /// Contribution of this sample to FFT normalization.
    pub normalization_contribution: f32,
    /// Contribution of this sample to CASA's persisted `.sumwt`.
    pub reported_contribution: f32,
}

/// Explicit weighting-seam diagnostics for one dirty-imaging plane.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingDiagnostics {
    /// Weighting policy used by the run.
    pub weighting: WeightingMode,
    /// Density-sharing mode used for uniform/Briggs weighting.
    pub weight_density_mode: WeightDensityMode,
    /// Optional Gaussian UV taper applied after weighting.
    pub uv_taper: Option<GaussianUvTaper>,
    /// Per-sample weighting results in stable input order.
    pub samples: Vec<WeightingSampleDiagnostics>,
    /// Number of samples that contribute to gridding and normalization.
    pub gridded_samples: usize,
    /// Number of samples rejected before gridding or normalization.
    pub skipped_samples: usize,
    /// Sum of contributions used for FFT/image normalization.
    pub normalization_sumwt: f32,
    /// CASA-style sum of logical sample weights persisted in `.sumwt`.
    pub reported_sumwt: f32,
}

/// One scalar sample after a major-cycle model prediction / residual refresh.
#[derive(Debug, Clone, PartialEq)]
pub struct ResidualSampleDiagnostics {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Observed visibility sample after preparation and weighting.
    pub observed_visibility: Complex32,
    /// Predicted visibility from the supplied model image.
    pub predicted_visibility: Complex32,
    /// Residual visibility `observed - predicted` before imaging-weight scaling.
    pub residual_visibility: Complex32,
    /// Final imaging weight applied to this sample.
    pub weight: f32,
    /// Whether this sample remains eligible for final gridding.
    pub gridable: bool,
}

/// Explicit major-cycle residual-refresh diagnostics for one imaging plane.
#[derive(Debug, Clone, PartialEq)]
pub struct ResidualRefreshDiagnostics {
    /// Per-sample prediction / residual results in stable input order.
    pub samples: Vec<ResidualSampleDiagnostics>,
    /// Refreshed residual image in `(x, y)` order.
    pub residual_image: Array2<f32>,
    /// Sum of weighted-sample contributions used for normalization.
    pub normalization_sumwt: f32,
    /// CASA-style reported `sumwt` used for the persisted `.sumwt` product.
    pub reported_sumwt: f32,
    /// PSF peak used when normalizing the refreshed residual image.
    pub psf_peak: f32,
    /// Number of samples that contributed to the refreshed residual image.
    pub gridded_samples: usize,
    /// Number of samples rejected before gridding.
    pub skipped_samples: usize,
}

/// One convolution-function plane in the current `wproject` CF plan.
#[derive(Debug, Clone, PartialEq)]
pub struct WProjectKernelDiagnostics {
    /// Zero-based plane index.
    pub plane_index: usize,
    /// Effective `w` value in wavelengths represented by this kernel plane.
    pub w_lambda: f64,
    /// Kernel support radius in grid cells.
    pub support: usize,
    /// Integral of the normalized kernel over its sampled support.
    pub kernel_integral: f32,
}

/// One weighted sample after `wproject` sample planning.
#[derive(Debug, Clone, PartialEq)]
pub struct WProjectSamplePlanDiagnostics {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight attached to the sample.
    pub weight: f32,
    /// CASA-style logical multiplicity factor used for reported `sumwt`.
    pub sumwt_factor: f32,
    /// Selected convolution-function plane index.
    pub plane_index: usize,
    /// Grid-center x location for the sample plan.
    pub loc_x: isize,
    /// Grid-center y location for the sample plan.
    pub loc_y: isize,
    /// Sub-grid x offset in oversampled kernel coordinates.
    pub off_x: isize,
    /// Sub-grid y offset in oversampled kernel coordinates.
    pub off_y: isize,
    /// Whether the kernel is conjugated for positive-`w` samples.
    pub conjugate_kernel: bool,
    /// Sample-local normalization gathered from the chosen kernel support.
    pub normalization: f32,
    /// Kernel support radius in grid cells for the selected plane.
    pub support: usize,
}

/// Reason a weighted sample did not survive `wproject` planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WProjectSkipReason {
    /// Sample was marked non-gridable before `wproject` planning.
    NotGridable,
    /// Sample coordinates, visibility, weight, or `sumwt_factor` were invalid.
    InvalidInput,
    /// The planned support footprint would run outside the padded grid.
    OutsideGrid,
}

/// One sample rejected before contributing to a `wproject` grid plan.
#[derive(Debug, Clone, PartialEq)]
pub struct WProjectSkippedSampleDiagnostics {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight attached to the sample before rejection.
    pub weight: f32,
    /// CASA-style logical multiplicity factor attached before rejection.
    pub sumwt_factor: f32,
    /// Canonical rejection reason.
    pub reason: WProjectSkipReason,
}

/// Explicit `wproject` planning diagnostics for one imaging plane.
#[derive(Debug, Clone, PartialEq)]
pub struct WProjectDiagnostics {
    /// Optional explicit `wprojplanes` request from the caller.
    pub requested_plane_count: Option<usize>,
    /// Actual number of kernel planes in the current CF plan.
    pub plane_count: usize,
    /// Kernel oversampling factor.
    pub sampling: usize,
    /// CASA-style `w`-axis scale used to map samples to planes.
    pub w_scale: f64,
    /// Maximum absolute input `w` value seen during planning.
    pub max_abs_w_lambda: f64,
    /// One summary per kernel plane.
    pub kernels: Vec<WProjectKernelDiagnostics>,
    /// One planned sample in stable input order.
    pub samples: Vec<WProjectSamplePlanDiagnostics>,
    /// Samples rejected before gridding, with explicit reasons.
    pub skipped_samples: Vec<WProjectSkippedSampleDiagnostics>,
    /// Sum of weighted-sample contributions used for normalization.
    pub normalization_sumwt: f32,
    /// CASA-style reported `sumwt` used for the persisted `.sumwt` product.
    pub reported_sumwt: f32,
    /// Number of samples that contributed to the final `wproject` grid plan.
    pub gridded_samples: usize,
}

/// Aggregate diagnostics for spectral-cube imaging.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeImagingDiagnostics {
    /// Human-readable warnings gathered across all spectral planes.
    pub warnings: Vec<String>,
    /// Total number of scalar samples that contributed to the cube.
    pub gridded_samples: usize,
    /// Total number of scalar samples dropped during validation or gridding.
    pub skipped_samples: usize,
    /// Aggregate CASA-style major-cycle count for the cube controller.
    ///
    /// This follows CASA's external `nmajordone` contract for cubes rather
    /// than the per-plane count reported in [`Self::channel_diagnostics`].
    pub major_cycles: usize,
    /// Aggregate CASA-style reported minor-iteration count for the cube.
    ///
    /// This tracks the controller's global reported iteration budget across
    /// planes and therefore matches CASA's cube `iterdone` contract more
    /// closely than summing per-plane actual component updates.
    pub minor_iterations: usize,
    /// Final reason why the cube controller stopped, if CLEAN was requested.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Per-channel diagnostics in output spectral order.
    pub channel_diagnostics: Vec<ImagingDiagnostics>,
    /// Aggregate stage timings summed across channels, with `total`
    /// representing end-to-end cube wall time.
    pub stage_timings: ImagingStageTimings,
}

/// Result of a complete spectral-cube run.
#[derive(Debug, Clone, PartialEq)]
pub struct CubeImagingResult {
    /// Normalized PSF cube with a real spectral axis.
    pub psf: Array4<f32>,
    /// Final residual cube with a real spectral axis.
    pub residual: Array4<f32>,
    /// Component model cube.
    pub model: Array4<f32>,
    /// Restored image cube. For dirty-only cubes this is the residual plus any
    /// restoring-beam processing applied per channel.
    pub image: Array4<f32>,
    /// CASA-style `sumwt` product with one scalar per spectral plane.
    pub sumwt: Array4<f32>,
    /// Final clean mask cube used by the controller, if CLEAN was run with an
    /// explicit or generated mask.
    pub clean_mask: Option<Array4<bool>>,
    /// Beam fitted from each per-plane PSF, in spectral order.
    pub beams: Vec<Option<BeamFit>>,
    /// Restoring beam actually applied to each restored image plane.
    ///
    /// This matches [`Self::beams`] for per-plane restoring, and carries the
    /// repeated common enclosing beam for `restoringbeam='common'`.
    pub restored_beams: Vec<Option<BeamFit>>,
    /// Diagnostics collected while building the cube.
    pub diagnostics: CubeImagingDiagnostics,
    /// Declared metadata contract for downstream persistence.
    pub compatibility: CompatibilityMetadata,
}
