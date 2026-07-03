// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public request/result types for the pure imaging core.

use std::{collections::VecDeque, ops::Range, sync::Arc, time::Duration};

use ndarray::{Array2, Array4, Zip};
use num_complex::{Complex32, Complex64};

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

    /// CASA-style logical `sumwt` multiplicity for a paired-hand collapse into
    /// this plane.
    pub fn paired_sumwt_factor(self) -> f32 {
        match self {
            Self::I => 2.0,
            Self::Q | Self::U | Self::V => 1.0,
            Self::XX | Self::YY | Self::RR | Self::LL => 1.0,
        }
    }

    /// Selects the paired correlations and collapse transform for this
    /// derived Stokes plane.
    ///
    /// Correlation codes use CASA/casacore numbering: `RR=5`, `RL=6`,
    /// `LR=7`, `LL=8`, `XX=9`, `XY=10`, `YX=11`, and `YY=12`.
    pub fn derive_pair_selection(
        self,
        corr_types: &[i32],
    ) -> Result<((usize, usize), StandardMfsPairCollapseTransform), ImagingError> {
        let correlation_index = |corr_code| corr_types.iter().position(|code| *code == corr_code);
        let xx_yy = correlation_index(9).zip(correlation_index(12));
        let xy_yx = correlation_index(10).zip(correlation_index(11));
        let rr_ll = correlation_index(5).zip(correlation_index(8));
        let rl_lr = correlation_index(6).zip(correlation_index(7));

        match self {
            Self::I => xx_yy
                .map(|pair| (pair, StandardMfsPairCollapseTransform::HalfSum))
                .or_else(|| rr_ll.map(|pair| (pair, StandardMfsPairCollapseTransform::HalfSum)))
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "Stokes I imaging requires XX+YY or RR+LL unless an explicit raw correlation plane is selected"
                            .to_string(),
                    )
                }),
            Self::Q => xx_yy
                .map(|pair| (pair, StandardMfsPairCollapseTransform::HalfDifference))
                .or_else(|| rl_lr.map(|pair| (pair, StandardMfsPairCollapseTransform::HalfSum)))
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "Stokes Q imaging requires XX+YY (linear basis) or RL+LR (circular basis)"
                            .to_string(),
                    )
                }),
            Self::U => xy_yx
                .map(|pair| (pair, StandardMfsPairCollapseTransform::HalfSum))
                .or_else(|| {
                    rl_lr.map(|pair| {
                        (
                            pair,
                            StandardMfsPairCollapseTransform::PositiveHalfImagDifference,
                        )
                    })
                })
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "Stokes U imaging requires XY+YX (linear basis) or RL+LR (circular basis)"
                            .to_string(),
                    )
                }),
            Self::V => xy_yx
                .map(|pair| {
                    (
                        pair,
                        StandardMfsPairCollapseTransform::NegativeHalfImagDifference,
                    )
                })
                .or_else(|| {
                    rr_ll.map(|pair| (pair, StandardMfsPairCollapseTransform::HalfDifference))
                })
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "Stokes V imaging requires XY+YX (linear basis) or RR+LL (circular basis)"
                            .to_string(),
                    )
                }),
            Self::XX | Self::YY | Self::RR | Self::LL => Err(ImagingError::InvalidRequest(
                format!("{self:?} is a raw correlation plane, not a derived Stokes plane"),
            )),
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

/// Request for planning the standard-MFS density-source channel domain.
///
/// This keeps CASA cube `perchanweightdensity=false` behavior in the reusable
/// imaging layer while frontends remain responsible for adapting the resulting
/// channel domain to their MeasurementSet readers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StandardMfsDensitySourcePlanRequest {
    /// True when the target image behaves like a CASA spectral cube or cubedata
    /// plane rather than continuum MFS.
    pub cube_like: bool,
    /// True when this target is dirty-only, either by explicit dirty mode or
    /// because CLEAN has zero minor-cycle iterations.
    pub dirty_only: bool,
    /// Optional selected-channel count for the target image.
    pub target_channel_count: Option<usize>,
    /// Total source-channel count available from the selected SPW/DDID.
    pub full_source_channel_count: usize,
    /// CASA `perchanweightdensity` state.
    pub per_channel_weight_density: bool,
    /// Weighting mode requested for the standard-MFS plane.
    pub weighting: WeightingMode,
}

/// Planned channel domain for standard-MFS density weighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardMfsDensitySourcePlan {
    /// Estimated target image channel count after frontend selection.
    pub target_channel_count: usize,
    /// Channel start to use for density accumulation. `None` means the full
    /// selected SPW/DDID domain.
    pub density_channel_start: Option<usize>,
    /// Channel count to use for density accumulation. `None` means the full
    /// selected SPW/DDID domain.
    pub density_channel_count: Option<usize>,
    /// Estimated density-source channel count.
    pub density_channel_count_estimate: usize,
    /// True when density weighting intentionally uses a channel domain different
    /// from the target image.
    pub uses_distinct_density_source: bool,
}

/// Plan the source-channel domain used by standard-MFS density weighting.
pub fn plan_standard_mfs_density_source(
    request: StandardMfsDensitySourcePlanRequest,
) -> StandardMfsDensitySourcePlan {
    let full_source_channel_count = request.full_source_channel_count.max(1);
    let target_channel_count = request
        .target_channel_count
        .unwrap_or(full_source_channel_count)
        .max(1);
    let density_weighting = matches!(
        request.weighting,
        WeightingMode::Uniform | WeightingMode::Briggs { .. } | WeightingMode::BriggsBwTaper { .. }
    );
    let uses_distinct_density_source = request.dirty_only
        && request.cube_like
        && request.target_channel_count == Some(1)
        && !request.per_channel_weight_density
        && density_weighting;
    let (density_channel_start, density_channel_count, density_channel_count_estimate) =
        if uses_distinct_density_source {
            (None, None, full_source_channel_count)
        } else {
            (
                None,
                request.target_channel_count,
                request
                    .target_channel_count
                    .unwrap_or(full_source_channel_count)
                    .max(1),
            )
        };
    StandardMfsDensitySourcePlan {
        target_channel_count,
        density_channel_start,
        density_channel_count,
        density_channel_count_estimate,
        uses_distinct_density_source,
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

/// Half-open visibility sample range `[start, end)` inside one scalar
/// visibility batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilitySampleRange {
    /// First sample index included in the range.
    pub start: usize,
    /// First sample index after the range.
    pub end: usize,
}

/// Compact mosaic metadata for one pointing/PB-frequency group.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupedVisibilityMetadata {
    /// CASA-style PB/conv-function frequency bucket in Hz for this group.
    pub beam_frequency_hz: f64,
    /// Primary-beam model for this homogeneous group.
    pub primary_beam_model: PrimaryBeamModel,
    /// Beam-center direction `[ra, dec]` in radians for this group.
    pub pointing_direction_rad: [f64; 2],
    /// Contiguous sample ranges in the aligned visibility batch.
    pub sample_ranges: Vec<VisibilitySampleRange>,
}

/// Compact per-batch mosaic metadata aligned with one scalar visibility batch.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupedVisibilityMetadataBatch {
    /// Number of scalar visibility samples represented by the batch.
    pub sample_count: usize,
    /// Groups of samples that share pointing direction, beam frequency, and PB
    /// model.
    pub groups: Vec<GroupedVisibilityMetadata>,
}

impl GroupedVisibilityMetadataBatch {
    pub(crate) fn validate_len(&self, expected: usize) -> Result<(), ImagingError> {
        if self.sample_count != expected {
            return Err(ImagingError::InvalidRequest(format!(
                "grouped visibility metadata sample count {} does not match visibility batch sample count {expected}",
                self.sample_count
            )));
        }
        for group in &self.groups {
            if !(group.beam_frequency_hz.is_finite() && group.beam_frequency_hz > 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "grouped visibility metadata beam frequencies must be finite positive Hz"
                        .to_string(),
                ));
            }
            group.primary_beam_model.validate()?;
            if !(group.pointing_direction_rad[0].is_finite()
                && group.pointing_direction_rad[1].is_finite())
            {
                return Err(ImagingError::InvalidRequest(
                    "grouped visibility metadata pointing directions must be finite radians"
                        .to_string(),
                ));
            }
            for range in &group.sample_ranges {
                if range.start >= range.end || range.end > expected {
                    return Err(ImagingError::InvalidRequest(format!(
                        "grouped visibility metadata range [{}, {}) is invalid for sample count {expected}",
                        range.start, range.end
                    )));
                }
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
    /// Compact grouped metadata aligned with
    /// [`ImagingRequest::visibility_batches`]. This is mutually exclusive with
    /// [`Self::metadata_batches`] and avoids expanding pointing/PB metadata for
    /// large homogeneous mosaic selections.
    pub grouped_metadata_batches: Vec<GroupedVisibilityMetadataBatch>,
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
        let has_expanded_metadata = !self.metadata_batches.is_empty();
        let has_grouped_metadata = !self.grouped_metadata_batches.is_empty();
        if has_expanded_metadata == has_grouped_metadata {
            return Err(ImagingError::InvalidRequest(
                "mosaic gridder requires exactly one expanded or grouped metadata representation"
                    .to_string(),
            ));
        }
        if has_expanded_metadata && self.metadata_batches.len() != visibility_batches.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "mosaic metadata batch count {} does not match visibility batch count {}",
                self.metadata_batches.len(),
                visibility_batches.len()
            )));
        }
        if has_expanded_metadata {
            for (batch, metadata) in visibility_batches.iter().zip(self.metadata_batches.iter()) {
                metadata.validate_len(batch.len())?;
            }
        } else {
            if self.grouped_metadata_batches.len() != visibility_batches.len() {
                return Err(ImagingError::InvalidRequest(format!(
                    "grouped mosaic metadata batch count {} does not match visibility batch count {}",
                    self.grouped_metadata_batches.len(),
                    visibility_batches.len()
                )));
            }
            for (batch, metadata) in visibility_batches
                .iter()
                .zip(self.grouped_metadata_batches.iter())
            {
                metadata.validate_len(batch.len())?;
            }
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

/// Stable identity for one homogeneous visibility source partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VisibilitySourcePartitionId(pub usize);

/// Shape invariant for one homogeneous visibility source partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilitySourceShape {
    /// Number of source channels in the partition.
    pub channel_count: usize,
    /// Number of correlations in each source row/channel sample.
    pub correlation_count: usize,
}

/// Source identity and shape visible to the pure imaging core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilitySourcePartition {
    /// Frontend-owned source partition id.
    pub id: VisibilitySourcePartitionId,
    /// Frontend-owned MeasurementSet/source id.
    pub ms_id: usize,
    /// Main-table data-description id.
    pub data_desc_id: i32,
    /// Spectral-window id.
    pub spectral_window_id: i32,
    /// Polarization id.
    pub polarization_id: i32,
    /// Homogeneous source shape.
    pub shape: VisibilitySourceShape,
}

/// Replay behavior guaranteed by a visibility source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCapability {
    /// The stream can be consumed once.
    SinglePass,
    /// The stream can be replayed by reopening the source.
    Reopenable,
    /// The stream is backed by an in-memory bounded cache.
    Cached,
}

/// Conservative resident-size limits for one visibility stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityStreamBounds {
    /// Maximum rows exposed by one block.
    pub max_rows_per_block: usize,
    /// Maximum source channels exposed by one block.
    pub max_channels_per_block: usize,
    /// Maximum correlations exposed by one row/channel sample.
    pub max_correlations: usize,
    /// Maximum blocks retained concurrently by the stream contract.
    pub max_concurrent_blocks: usize,
    /// Maximum resident visibility bytes promised by the source.
    pub max_resident_visibility_bytes: usize,
}

impl VisibilityStreamBounds {
    /// Validate that the stream declares finite bounded residency.
    pub fn validate(self) -> Result<(), ImagingError> {
        if self.max_rows_per_block == 0 {
            return Err(ImagingError::InvalidRequest(
                "visibility stream max_rows_per_block must be greater than zero".to_string(),
            ));
        }
        if self.max_channels_per_block == 0 {
            return Err(ImagingError::InvalidRequest(
                "visibility stream max_channels_per_block must be greater than zero".to_string(),
            ));
        }
        if self.max_correlations == 0 {
            return Err(ImagingError::InvalidRequest(
                "visibility stream max_correlations must be greater than zero".to_string(),
            ));
        }
        if self.max_concurrent_blocks == 0 {
            return Err(ImagingError::InvalidRequest(
                "visibility stream max_concurrent_blocks must be greater than zero".to_string(),
            ));
        }
        if self.max_resident_visibility_bytes == 0 {
            return Err(ImagingError::InvalidRequest(
                "visibility stream max_resident_visibility_bytes must be greater than zero"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Runtime residency telemetry reported by a visibility stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VisibilityStreamTelemetry {
    /// Number of blocks emitted by the stream.
    pub blocks_emitted: usize,
    /// Maximum rows observed in one emitted block.
    pub max_rows_per_block_seen: usize,
    /// Maximum channels observed in one emitted block.
    pub max_channels_per_block_seen: usize,
    /// Maximum concurrently resident blocks observed by the source.
    pub max_concurrent_blocks_seen: usize,
    /// Maximum resident visibility bytes observed by the source.
    pub max_resident_visibility_bytes_seen: usize,
}

/// Bounded block stream over selected visibility data.
pub trait VisibilityBlockStream {
    /// Return the static bounds promised by this stream.
    fn bounds(&self) -> VisibilityStreamBounds;

    /// Return the replay behavior promised by this stream.
    fn replay_capability(&self) -> ReplayCapability;

    /// Stream borrowed visibility blocks in stable source order.
    fn stream_blocks(
        &mut self,
        consumer: &mut dyn for<'a> FnMut(VisibilityBlockView<'a>) -> Result<(), ImagingError>,
    ) -> Result<VisibilityStreamTelemetry, ImagingError>;
}

/// Semantic selected-visibility source consumed by imaging plans.
pub trait VisibilitySource {
    /// Homogeneous partitions contained by this source.
    fn partitions(&self) -> &[VisibilitySourcePartition];

    /// Return the static bounds promised by this source.
    fn bounds(&self) -> VisibilityStreamBounds;

    /// Return the replay behavior promised by this source.
    fn replay_capability(&self) -> ReplayCapability;

    /// Stream borrowed visibility blocks in stable source order.
    fn stream_blocks(
        &mut self,
        consumer: &mut dyn for<'a> FnMut(VisibilityBlockView<'a>) -> Result<(), ImagingError>,
    ) -> Result<VisibilityStreamTelemetry, ImagingError>;
}

/// Borrowed complex sample storage for columnar source visibility data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VisibilityComplexSamplesRef<'a> {
    /// Native Complex32 samples.
    Complex32(&'a [Complex32]),
    /// Native Complex64 samples.
    Complex64(&'a [Complex64]),
}

impl VisibilityComplexSamplesRef<'_> {
    /// Number of complex values in the borrowed storage.
    pub fn len(self) -> usize {
        match self {
            Self::Complex32(values) => values.len(),
            Self::Complex64(values) => values.len(),
        }
    }

    /// Returns true when the borrowed storage is empty.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

/// Borrowed real sample storage for columnar source weights.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VisibilityFloatSamplesRef<'a> {
    /// Native Float32 samples.
    Float32(&'a [f32]),
    /// Native Float64 samples.
    Float64(&'a [f64]),
}

impl VisibilityFloatSamplesRef<'_> {
    /// Number of real values in the borrowed storage.
    pub fn len(self) -> usize {
        match self {
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
        }
    }

    /// Returns true when the borrowed storage is empty.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

/// Borrowed homogeneous columnar source block.
///
/// Channelized arrays use `[channel][row][correlation]` layout. Row sidecars use
/// `[row]`, and UVW uses `[row][axis]` with three axes per row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VisibilityBlockView<'a> {
    /// Homogeneous partition identity and shape.
    pub partition: VisibilitySourcePartition,
    /// Original source row indices in stable processing order.
    pub row_indices: &'a [usize],
    /// First source channel represented in channelized arrays.
    pub channel_start: usize,
    /// Number of source channels represented in channelized arrays.
    pub channel_count: usize,
    /// Complex visibility samples.
    pub data: Option<VisibilityComplexSamplesRef<'a>>,
    /// Channel flags.
    pub flags: Option<&'a [bool]>,
    /// Per-row weights.
    pub weights: Option<VisibilityFloatSamplesRef<'a>>,
    /// Per-channel weights.
    pub weight_spectrum: Option<VisibilityFloatSamplesRef<'a>>,
    /// UVW coordinates.
    pub uvw_m: Option<&'a [f64]>,
    /// Row flags.
    pub flag_row: Option<&'a [bool]>,
    /// Antenna1 ids.
    pub antenna1: Option<&'a [i32]>,
    /// Antenna2 ids.
    pub antenna2: Option<&'a [i32]>,
    /// Field ids.
    pub field_ids: Option<&'a [i32]>,
    /// Time values.
    pub time: Option<&'a [f64]>,
}

impl VisibilityBlockView<'_> {
    /// Number of rows represented by this source block.
    pub fn row_count(self) -> usize {
        self.row_indices.len()
    }

    /// Source-channel range represented by this source block.
    pub fn channel_range(self) -> Range<usize> {
        self.channel_start..self.channel_start.saturating_add(self.channel_count)
    }

    /// Index into `[channel][row][correlation]` sample arrays.
    pub fn channel_row_corr_index(
        self,
        channel_slot: usize,
        row_slot: usize,
        corr_slot: usize,
    ) -> usize {
        (channel_slot * self.row_count() + row_slot) * self.partition.shape.correlation_count
            + corr_slot
    }

    /// Validate shape consistency for the borrowed block.
    pub fn validate(self) -> Result<(), ImagingError> {
        if self.channel_count == 0 {
            return Err(ImagingError::InvalidRequest(
                "source block channel_count must be greater than zero".to_string(),
            ));
        }
        let end = self
            .channel_start
            .checked_add(self.channel_count)
            .ok_or_else(|| {
                ImagingError::InvalidRequest("source block channel range overflow".to_string())
            })?;
        if end > self.partition.shape.channel_count {
            return Err(ImagingError::InvalidRequest(format!(
                "source block channel range {}..{} exceeds partition channel count {}",
                self.channel_start, end, self.partition.shape.channel_count
            )));
        }
        let sample_count = self
            .row_count()
            .saturating_mul(self.channel_count)
            .saturating_mul(self.partition.shape.correlation_count);
        if let Some(data) = self.data {
            validate_len("source data", data.len(), sample_count)?;
        }
        if let Some(flags) = self.flags {
            validate_len("source flags", flags.len(), sample_count)?;
        }
        if let Some(weights) = self.weights {
            validate_len(
                "source weights",
                weights.len(),
                self.row_count()
                    .saturating_mul(self.partition.shape.correlation_count),
            )?;
        }
        if let Some(weight_spectrum) = self.weight_spectrum {
            validate_len(
                "source weight_spectrum",
                weight_spectrum.len(),
                sample_count,
            )?;
        }
        if let Some(uvw_m) = self.uvw_m {
            validate_len(
                "source uvw",
                uvw_m.len(),
                self.row_count().saturating_mul(3),
            )?;
        }
        validate_optional_row_len("source flag_row", self.flag_row, self.row_count())?;
        validate_optional_row_len("source antenna1", self.antenna1, self.row_count())?;
        validate_optional_row_len("source antenna2", self.antenna2, self.row_count())?;
        validate_optional_row_len("source field_ids", self.field_ids, self.row_count())?;
        validate_optional_row_len("source time", self.time, self.row_count())?;
        Ok(())
    }
}

fn validate_len(name: &str, actual: usize, expected: usize) -> Result<(), ImagingError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ImagingError::InvalidRequest(format!(
            "{name} length {actual} does not match expected {expected}"
        )))
    }
}

fn validate_optional_row_len<T>(
    name: &str,
    values: Option<&[T]>,
    expected: usize,
) -> Result<(), ImagingError> {
    match values {
        Some(values) => validate_len(name, values.len(), expected),
        None => Ok(()),
    }
}

/// Compact spectral routing state for a borrowed source block.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralRoutePlan {
    /// Source-channel to output-plane mapping.
    channel_routes: Vec<SourceChannelRoute>,
}

impl SpectralRoutePlan {
    /// Route each source channel in a block to the matching output plane.
    pub fn identity_for_block(source: VisibilityBlockView<'_>) -> Self {
        let channel_routes = source
            .channel_range()
            .enumerate()
            .map(|(plane_index, source_channel)| SourceChannelRoute {
                source_channel,
                output_planes: vec![OutputPlaneContribution {
                    plane_index,
                    factor: 1.0,
                }],
            })
            .collect();
        Self { channel_routes }
    }

    /// Number of source-channel routes represented by this plan.
    pub fn channel_route_count(&self) -> usize {
        self.channel_routes.len()
    }
}

/// Mapping for one represented source channel.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceChannelRoute {
    /// Source channel index.
    source_channel: usize,
    /// Output planes receiving this source channel.
    output_planes: Vec<OutputPlaneContribution>,
}

/// Contribution of one source channel to one output plane.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutputPlaneContribution {
    /// Output plane index.
    plane_index: usize,
    /// Linear contribution factor.
    factor: f32,
}

/// Compact Stokes/correlation routing state.
#[derive(Debug, Clone, PartialEq)]
pub struct PolarizationRoutePlan {
    /// Output Stokes plane represented by this route.
    pub output_stokes: PlaneStokes,
}

/// Compact geometric routing state.
#[derive(Debug, Clone, PartialEq)]
pub struct GeometryRoutePlan {
    /// Image-plane geometry for routed samples.
    pub geometry: ImageGeometry,
}

/// Compact weighting route state.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingRoutePlan {
    /// Weighting mode for this route.
    pub weighting: WeightingMode,
}

/// Compact gridder route state.
#[derive(Debug, Clone, PartialEq)]
pub struct GridderRoutePlan {
    /// Gridder mode for this route.
    pub gridder_mode: GridderMode,
}

/// Compact model routing state.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelRoutePlan {
    /// Output-model planes available for prediction.
    pub model_plane_count: usize,
}

/// Borrowed source block plus compact route state consumed by imaging stages.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImagingSourceBlockView<'a> {
    /// Borrowed source samples and row sidecars.
    pub source: VisibilityBlockView<'a>,
    /// Spectral routing plan.
    pub spectral: &'a SpectralRoutePlan,
    /// Polarization routing plan.
    pub polarization: &'a PolarizationRoutePlan,
    /// Geometry routing plan.
    pub geometry: &'a GeometryRoutePlan,
    /// Weighting routing plan.
    pub weighting: &'a WeightingRoutePlan,
    /// Gridder routing plan.
    pub gridder: &'a GridderRoutePlan,
    /// Optional model routing plan.
    pub model: Option<&'a ModelRoutePlan>,
}

impl ImagingSourceBlockView<'_> {
    /// Validate borrowed source and route-level shape consistency.
    pub fn validate(self) -> Result<(), ImagingError> {
        self.source.validate()?;
        if self.spectral.channel_routes.len() != self.source.channel_count {
            return Err(ImagingError::InvalidRequest(format!(
                "spectral route count {} does not match source block channel count {}",
                self.spectral.channel_routes.len(),
                self.source.channel_count
            )));
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

/// One already-selected scalar visibility sample in wavelength coordinates.
///
/// This is the single-sample counterpart to [`VisibilityBatch`]. Streaming
/// frontends use it when retaining an owned columnar batch for every replay
/// would exceed the intended residency budget.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalarVisibilitySample {
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight after natural/uniform/Briggs weighting.
    pub weight: f32,
    /// Logical multiplicity factor for CASA-style reported `sumwt`.
    pub sumwt_factor: f32,
    /// Whether this sample participates in image/PSF gridding.
    pub gridable: bool,
    /// Complex scalar visibility.
    pub visibility: Complex32,
}

/// Already weighted scalar standard-MFS sample streamed directly to gridding.
///
/// This is the single-sample equivalent of one row in [`VisibilityBatch`]. It
/// lets frontends feed bounded row blocks to the standard-MFS replay path
/// without first materializing a full owned batch for every major-cycle replay.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StandardMfsWeightedSample {
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight after natural/uniform/Briggs weighting.
    pub weight: f32,
    /// Logical multiplicity factor for CASA-style reported `sumwt`.
    pub sumwt_factor: f32,
    /// Whether this sample participates in image/PSF gridding.
    pub gridable: bool,
    /// Complex scalar visibility.
    pub visibility: Complex32,
}

impl From<ScalarVisibilitySample> for StandardMfsWeightedSample {
    fn from(sample: ScalarVisibilitySample) -> Self {
        Self {
            u_lambda: sample.u_lambda,
            v_lambda: sample.v_lambda,
            w_lambda: sample.w_lambda,
            weight: sample.weight,
            sumwt_factor: sample.sumwt_factor,
            gridable: sample.gridable,
            visibility: sample.visibility,
        }
    }
}

/// Tile-routed standard-MFS sample before final density-dependent weighting.
///
/// Fixed-tile workers use this payload to compute final weights and taps close
/// to the resident tile buffer. The producer computes only the deterministic
/// owner-tile center needed for routing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StandardMfsRoutedSample {
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Positive-tap center x cell in the padded standard grid.
    pub center_x: u32,
    /// Positive-tap center y cell in the padded standard grid.
    pub center_y: u32,
    /// Routed-sample flags; use [`Self::finite_visibility`].
    pub flags: u16,
    /// Number of 2-D tap visits expected for work attribution.
    pub tap_count: u8,
    /// Natural input weight before Uniform/Briggs reweighting.
    pub natural_weight: f32,
    /// Logical multiplicity factor for CASA-style reported `sumwt`.
    pub sumwt_factor: f32,
    /// Baseline `w` coordinate in wavelengths, retained for diagnostics.
    pub w_lambda: f64,
    /// Complex scalar visibility.
    pub visibility: Complex32,
}

impl StandardMfsRoutedSample {
    /// Visibility is finite and contributes to dirty/residual grids.
    pub const FINITE_VISIBILITY: u16 = 1 << 0;
    /// Returns true when the sample visibility can contribute to dirty/residual grids.
    pub fn finite_visibility(self) -> bool {
        self.flags & Self::FINITE_VISIBILITY != 0
    }
}

/// Pair-collapse operation for row-shaped standard-MFS visibility runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardMfsPairCollapseTransform {
    /// Strict half-sum collapse, used for Stokes I.
    HalfSum,
    /// Strict half-difference collapse.
    HalfDifference,
    /// Positive imaginary half-difference collapse.
    PositiveHalfImagDifference,
    /// Negative imaginary half-difference collapse.
    NegativeHalfImagDifference,
}

impl StandardMfsPairCollapseTransform {
    /// Collapse paired parallel-hand visibilities into the requested Stokes-like
    /// scalar visibility.
    pub fn collapse(self, first_visibility: Complex32, second_visibility: Complex32) -> Complex32 {
        match self {
            Self::HalfSum => (first_visibility + second_visibility) * 0.5,
            Self::HalfDifference => (first_visibility - second_visibility) * 0.5,
            Self::PositiveHalfImagDifference => {
                (first_visibility - second_visibility) * Complex32::new(0.0, 0.5)
            }
            Self::NegativeHalfImagDifference => {
                (first_visibility - second_visibility) * Complex32::new(0.0, -0.5)
            }
        }
    }
}

/// Polarization selection represented by a row-shaped standard-MFS visibility run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StandardMfsVisibilityPolarization {
    /// Use one explicit correlation from the row.
    Explicit {
        /// Correlation index in the row DATA/FLAG/WEIGHT arrays.
        corr_index: usize,
        /// Logical multiplicity factor for CASA-style reported `sumwt`.
        sumwt_factor: f32,
    },
    /// Collapse two correlations into one logical visibility in the worker.
    CollapsedPair {
        /// First correlation index in the row DATA/FLAG/WEIGHT arrays.
        first_corr_index: usize,
        /// Second correlation index in the row DATA/FLAG/WEIGHT arrays.
        second_corr_index: usize,
        /// Collapse operation for the paired correlations.
        transform: StandardMfsPairCollapseTransform,
        /// Logical multiplicity factor for CASA-style reported `sumwt`.
        sumwt_factor: f32,
    },
}

/// Row-shaped standard-MFS visibility input before backend-specific routing.
///
/// Frontends build this from selected MeasurementSet rows or equivalent
/// visibility stores. The imaging core owns tap routing, run slicing, and
/// backend cache layout from this point forward.
#[derive(Debug, Clone, PartialEq)]
pub struct StandardMfsVisibilityRow {
    /// Baseline `uvw` coordinate in meters.
    pub uvw_m: [f64; 3],
    /// MeasurementSet spectral-window id for diagnostics.
    pub spw_id: usize,
    /// First source-channel index loaded into the row matrices.
    pub channel_origin: usize,
    /// Selected source-channel indices addressed by this row.
    pub source_channel_indices: Arc<[usize]>,
    /// Per-selected-channel conversion factor from meters to wavelengths.
    pub channel_lambda_scales: Arc<[f64]>,
    /// Complex visibility matrix in `[correlation, local_channel]` order.
    pub data: Array2<Complex32>,
    /// Flag matrix in `[correlation, local_channel]` order.
    pub flag: Array2<bool>,
    /// Per-correlation natural weights.
    pub weight: Arc<[f32]>,
    /// Optional per-correlation, per-channel natural weights.
    pub weight_spectrum: Option<Array2<f32>>,
    /// Whether this row is gridable by the standard interferometric path.
    pub gridable: bool,
    /// Polarization/correlation interpretation for this imaging plane.
    pub polarization: StandardMfsVisibilityPolarization,
}

impl StandardMfsVisibilityRow {
    pub(crate) fn into_routed_row(self) -> StandardMfsRoutedVisibilityRow {
        StandardMfsRoutedVisibilityRow {
            uvw_m: self.uvw_m,
            spw_id: self.spw_id,
            channel_origin: self.channel_origin,
            source_channel_indices: self.source_channel_indices,
            channel_lambda_scales: self.channel_lambda_scales,
            data: self.data,
            flag: self.flag,
            weight: self.weight,
            weight_spectrum: self.weight_spectrum,
            gridable: self.gridable,
            polarization: self.polarization,
        }
    }
}

/// Owned row-shaped visibility payload for standard-MFS fixed-tile routing.
///
/// A row may be shared by several tile-local runs through `Arc`; this avoids
/// copying `u/v/w`, flags, weights, weight spectra, and visibility matrices
/// into one scalar queue record per channel lane.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StandardMfsRoutedVisibilityRow {
    /// Baseline `uvw` coordinate in meters.
    pub uvw_m: [f64; 3],
    /// MeasurementSet spectral-window id for diagnostics.
    pub spw_id: usize,
    /// First source-channel index loaded into the row matrices.
    pub channel_origin: usize,
    /// Selected source-channel indices addressed by routed runs.
    pub source_channel_indices: Arc<[usize]>,
    /// Per-selected-channel conversion factor from meters to wavelengths.
    pub channel_lambda_scales: Arc<[f64]>,
    /// Complex visibility matrix in `[correlation, local_channel]` order.
    pub data: Array2<Complex32>,
    /// Flag matrix in `[correlation, local_channel]` order.
    pub flag: Array2<bool>,
    /// Per-correlation natural weights.
    pub weight: Arc<[f32]>,
    /// Optional per-correlation, per-channel natural weights.
    pub weight_spectrum: Option<Array2<f32>>,
    /// Whether this row is gridable by the standard interferometric path.
    pub gridable: bool,
    /// Polarization/correlation interpretation for this imaging plane.
    pub polarization: StandardMfsVisibilityPolarization,
}

/// Tile-routed row/channel span for standard-MFS fixed-tile workers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StandardMfsRoutedVisibilityRun {
    /// Shared row payload.
    pub row: Arc<StandardMfsRoutedVisibilityRow>,
    /// Range into `row.source_channel_indices` and `row.channel_lambda_scales`.
    pub source_slot_range: Range<usize>,
    /// Positive-tap centers for each source slot in `source_slot_range`.
    pub tap_centers: Arc<[[u32; 2]]>,
    /// Stable first input sequence for deterministic scheduling tie-breaks.
    pub first_input_seq: u64,
}

impl StandardMfsRoutedVisibilityRun {
    /// Returns the number of logical channel lanes in this run.
    pub fn len(&self) -> usize {
        self.source_slot_range
            .end
            .saturating_sub(self.source_slot_range.start)
    }

    /// Returns `true` when the run contains no logical channel lanes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Counts returned while routing one standard-MFS visibility row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StandardMfsRoutedVisibilityAppendCounts {
    /// Candidate source-channel lanes considered for routing.
    pub candidate_samples: usize,
    /// Candidate lanes accepted by the standard-gridder tap planner.
    pub planned_samples: usize,
}

/// Opaque replay block of standard-MFS routed visibility runs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StandardMfsRoutedVisibilityBlock {
    pub(crate) runs: Vec<StandardMfsRoutedVisibilityRun>,
}

impl StandardMfsRoutedVisibilityBlock {
    /// Remove all routed runs while retaining allocation.
    pub fn clear(&mut self) {
        self.runs.clear();
    }

    /// Number of routed row/channel runs represented by this block.
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Number of logical channel lanes represented by this block.
    pub fn logical_lanes(&self) -> usize {
        self.runs
            .iter()
            .map(StandardMfsRoutedVisibilityRun::len)
            .sum()
    }

    /// Return true when this block contains no routed runs.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub(crate) fn push_run(&mut self, run: StandardMfsRoutedVisibilityRun) {
        self.runs.push(run);
    }

    pub(crate) fn runs(&self) -> &[StandardMfsRoutedVisibilityRun] {
        &self.runs
    }

    pub(crate) fn drain_runs(
        &mut self,
    ) -> impl Iterator<Item = StandardMfsRoutedVisibilityRun> + '_ {
        self.runs.drain(..)
    }
}

/// Already weighted and grid-center-located scalar standard-MFS sample.
///
/// This is the bounded row-block handoff used by streaming frontends that want
/// to route samples to fixed tiles without retaining full visibility batches.
/// It stores the deterministic positive-tap center for tile ownership, while
/// core workers consume the precomputed compact tap identity directly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StandardMfsPlannedWeightedSample {
    /// Baseline `u` coordinate in wavelengths.
    pub(crate) u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub(crate) v_lambda: f64,
    /// Positive-tap center x cell in the padded standard grid.
    pub(crate) center_x: u32,
    /// Positive-tap center y cell in the padded standard grid.
    pub(crate) center_y: u32,
    /// First x cell touched by the compact positive-tap stencil.
    pub(crate) x_start: u32,
    /// First y cell touched by the compact positive-tap stencil.
    pub(crate) y_start: u32,
    /// Compact x-axis prolate-spheroidal weight table index.
    pub(crate) x_weight_index: u32,
    /// Compact y-axis prolate-spheroidal weight table index.
    pub(crate) y_weight_index: u32,
    /// Planned-sample flags; use [`Self::finite_visibility`] and [`Self::psf_only`].
    pub(crate) flags: u16,
    /// Number of 2-D tap visits expected for work attribution.
    pub(crate) tap_count: u8,
    /// Product of final imaging weight and CASA-style `sumwt` factor.
    pub(crate) grid_weight: f32,
    /// Baseline `w` coordinate in wavelengths, retained for diagnostics.
    pub(crate) w_lambda: f64,
    /// Complex scalar visibility.
    pub(crate) visibility: Complex32,
}

impl StandardMfsPlannedWeightedSample {
    /// Visibility is finite and contributes to dirty/residual grids.
    pub(crate) const FINITE_VISIBILITY: u16 = 1 << 0;
    /// Visibility is nonfinite and contributes only to the PSF.
    pub(crate) const PSF_ONLY: u16 = 1 << 1;

    /// Returns true when the sample visibility can contribute to dirty/residual grids.
    pub(crate) fn finite_visibility(self) -> bool {
        self.flags & Self::FINITE_VISIBILITY != 0
    }

    /// Number of 2-D tap visits expected for work attribution.
    pub fn tap_count(self) -> u8 {
        self.tap_count
    }
}

/// Bounded planned standard-MFS samples with explicit scalar-run ranges.
///
/// Frontends use this when they can preserve row/channel run boundaries while
/// still streaming compact scalar planned samples. The samples remain the
/// canonical gridding payload; `runs` only describes contiguous slices that
/// should be considered together for tile routing.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct StandardMfsPlannedWeightedSampleRunBlock {
    samples: Vec<StandardMfsPlannedWeightedSample>,
    runs: Vec<Range<usize>>,
}

impl StandardMfsPlannedWeightedSampleRunBlock {
    /// Remove all samples and run ranges while retaining allocated capacity.
    pub fn clear(&mut self) {
        self.samples.clear();
        self.runs.clear();
    }

    /// Return the planned scalar samples in stable input order.
    pub fn samples(&self) -> &[StandardMfsPlannedWeightedSample] {
        &self.samples
    }

    /// Return contiguous run ranges into [`Self::samples`].
    pub(crate) fn runs(&self) -> &[Range<usize>] {
        &self.runs
    }

    /// Start a new run, returning the current sample offset.
    pub fn begin_run(&self) -> usize {
        self.samples.len()
    }

    /// Append one planned scalar sample to the current run under construction.
    pub fn push_sample(&mut self, sample: StandardMfsPlannedWeightedSample) {
        self.samples.push(sample);
    }

    /// Finish a run that began at `start`, recording it if it is non-empty.
    pub fn finish_run(&mut self, start: usize) {
        let end = self.samples.len();
        if start < end {
            self.runs.push(start..end);
        }
    }

    /// Append one run by copying the provided planned samples.
    pub(crate) fn push_run_from_slice(&mut self, samples: &[StandardMfsPlannedWeightedSample]) {
        let start = self.begin_run();
        self.samples.extend_from_slice(samples);
        self.finish_run(start);
    }

    /// Return the total number of planned scalar samples.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Return true when there are no planned scalar samples.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

/// Opaque bounded block of planned standard-MFS scalar samples.
///
/// The block preserves row/run boundaries for fixed-tile scheduling without
/// exposing compact tap-table identities or worker payload fields to frontends.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StandardMfsPlannedSampleBlock {
    pub(crate) inner: StandardMfsPlannedWeightedSampleRunBlock,
}

impl StandardMfsPlannedSampleBlock {
    /// Remove all planned samples and run ranges while retaining capacity.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Start a new run, returning the current sample offset.
    pub fn begin_run(&self) -> usize {
        self.inner.begin_run()
    }

    /// Finish a run that began at `start`, recording it if it is non-empty.
    pub fn finish_run(&mut self, start: usize) {
        self.inner.finish_run(start);
    }

    /// Return the total number of planned scalar samples.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true when there are no planned scalar samples.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the total planned 2-D tap visits represented by this block.
    pub fn tap_visits(&self) -> usize {
        self.inner
            .samples()
            .iter()
            .map(|sample| usize::from(sample.tap_count()))
            .sum()
    }

    pub(crate) fn samples(&self) -> &[StandardMfsPlannedWeightedSample] {
        self.inner.samples()
    }

    pub(crate) fn push_sample(&mut self, sample: StandardMfsPlannedWeightedSample) {
        self.inner.push_sample(sample);
    }

    pub(crate) fn inner(&self) -> &StandardMfsPlannedWeightedSampleRunBlock {
        &self.inner
    }
}

/// Bounded routed standard-MFS samples with explicit scalar-run ranges.
///
/// This is the fixed-tile worker handoff for final-weight-deferred execution:
/// samples are already routed to standard-gridder centers, but their
/// Uniform/Briggs weights are applied by the tile workers.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct StandardMfsRoutedSampleRunBlock {
    samples: Vec<StandardMfsRoutedSample>,
    runs: Vec<Range<usize>>,
}

impl StandardMfsRoutedSampleRunBlock {
    /// Return the routed scalar samples in stable input order.
    pub(crate) fn samples(&self) -> &[StandardMfsRoutedSample] {
        &self.samples
    }

    /// Return contiguous run ranges into [`Self::samples`].
    pub(crate) fn runs(&self) -> &[Range<usize>] {
        &self.runs
    }
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
        self.collapse_with_transform(StandardMfsPairCollapseTransform::HalfSum, PlaneStokes::I)
    }

    /// Collapses unflagged paired parallel hands with an explicit
    /// Stokes-like transform.
    ///
    /// If either hand is flagged, if either weight is non-positive, or if the
    /// transformed visibility is non-finite, that logical sample is dropped.
    pub fn collapse_with_transform(
        &self,
        transform: StandardMfsPairCollapseTransform,
        plane_stokes: PlaneStokes,
    ) -> Result<VisibilityBatch, ImagingError> {
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
            let vis =
                transform.collapse(self.first_visibility[index], self.second_visibility[index]);
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
            sumwt_factor.push(plane_stokes.paired_sumwt_factor());
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
            minor_cycle_length: 1000,
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

impl CubeAutoMultiThresholdConfig {
    /// Builds a CASA-style `auto-multithresh` clean-mask cube from residual
    /// products and per-plane diagnostics.
    ///
    /// The returned mask has shape `[nx, ny, 1, nchan]`; multiple residual
    /// Stokes planes are OR-combined into the single persisted clean-mask
    /// plane for each channel. `mask_beams` should contain the restoring beam
    /// selected for each output channel after any common-beam policy has been
    /// applied.
    pub fn build_cube_clean_mask(
        &self,
        geometry: ImageGeometry,
        dirty: &CubeImagingResult,
        mask_beams: &[Option<BeamFit>],
        user_mask: Option<&Array2<bool>>,
    ) -> Result<Array4<bool>, ImagingError> {
        let residual = &dirty.residual;
        let shape = residual.shape();
        if shape.len() != 4 {
            return Err(ImagingError::InvalidRequest(format!(
                "auto-multithresh residual product must be rank-4, found shape {shape:?}"
            )));
        }
        if shape[0] != geometry.nx() || shape[1] != geometry.ny() {
            return Err(ImagingError::InvalidRequest(format!(
                "auto-multithresh residual shape {:?} does not match image geometry {:?}",
                &shape[0..2],
                geometry.image_shape
            )));
        }
        if let Some(mask) = user_mask
            && mask.dim() != (geometry.nx(), geometry.ny())
        {
            return Err(ImagingError::InvalidRequest(format!(
                "auto-multithresh user mask shape {:?} does not match image geometry {:?}",
                mask.dim(),
                geometry.image_shape
            )));
        }
        let nstokes = shape[2];
        let nchan = shape[3];
        let result_channel_count = dirty.compatibility.channel_frequencies_hz.len();
        if result_channel_count != nchan {
            return Err(ImagingError::InvalidRequest(format!(
                "auto-multithresh residual channel count {nchan} does not match result channel count {result_channel_count}",
            )));
        }
        let mut mask = Array4::<bool>::from_elem((geometry.nx(), geometry.ny(), 1, nchan), false);
        for channel_index in 0..nchan {
            let mut channel_mask = user_mask.cloned().unwrap_or_else(|| {
                Array2::<bool>::from_elem((geometry.nx(), geometry.ny()), false)
            });
            let max_psf_sidelobe_level = dirty
                .diagnostics
                .channel_diagnostics
                .get(channel_index)
                .map(|diagnostics| diagnostics.max_psf_sidelobe_level)
                .unwrap_or(0.0);
            let beam = mask_beams.get(channel_index).copied().flatten();
            let min_region_pixels = auto_mask_min_region_pixels(geometry, beam, self);
            let beam_shape = auto_mask_beam_shape(geometry, beam, self);
            for stokes_index in 0..nstokes {
                let plane = residual
                    .slice(ndarray::s![.., .., stokes_index, channel_index])
                    .to_owned();
                let plane_mask = auto_multithresh_plane_mask(
                    &plane,
                    max_psf_sidelobe_level,
                    min_region_pixels,
                    beam_shape,
                    self,
                );
                Zip::from(&mut channel_mask)
                    .and(&plane_mask)
                    .for_each(|out, generated| *out = *out || *generated);
            }
            mask.slice_mut(ndarray::s![.., .., 0, channel_index])
                .assign(&channel_mask);
        }
        Ok(mask)
    }
}

fn auto_mask_min_region_pixels(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: &CubeAutoMultiThresholdConfig,
) -> usize {
    if config.min_beam_frac <= 0.0 {
        return 1;
    }
    let Some(beam) = beam else {
        return 1;
    };
    let cell_area = geometry.cell_size_rad[0].abs() * geometry.cell_size_rad[1].abs();
    if !(cell_area.is_finite() && cell_area > 0.0) {
        return 1;
    }
    let beam_area = std::f64::consts::PI * beam.major_fwhm_rad.abs() * beam.minor_fwhm_rad.abs()
        / (4.0 * std::f64::consts::LN_2);
    if !(beam_area.is_finite() && beam_area > 0.0) {
        return 1;
    }
    ((config.min_beam_frac as f64 * beam_area / cell_area).ceil() as usize).max(1)
}

#[derive(Debug, Clone, Copy)]
struct AutoMaskBeamShape {
    sigma_x_pixels: f64,
    sigma_y_pixels: f64,
    position_angle_rad: f64,
}

fn auto_mask_beam_shape(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: &CubeAutoMultiThresholdConfig,
) -> Option<AutoMaskBeamShape> {
    let beam = beam?;
    if config.smooth_factor <= 0.0 {
        return None;
    }
    let cell_x = geometry.cell_size_rad[0].abs();
    let cell_y = geometry.cell_size_rad[1].abs();
    if !(cell_x.is_finite() && cell_x > 0.0 && cell_y.is_finite() && cell_y > 0.0) {
        return None;
    }
    let sigma_from_fwhm = |fwhm_rad: f64, cell_rad: f64| {
        config.smooth_factor as f64 * fwhm_rad.abs()
            / (2.0 * (2.0 * std::f64::consts::LN_2).sqrt() * cell_rad)
    };
    let sigma_x_pixels = sigma_from_fwhm(beam.minor_fwhm_rad, cell_x);
    let sigma_y_pixels = sigma_from_fwhm(beam.major_fwhm_rad, cell_y);
    (sigma_x_pixels.is_finite()
        && sigma_x_pixels > 0.0
        && sigma_y_pixels.is_finite()
        && sigma_y_pixels > 0.0)
        .then_some(AutoMaskBeamShape {
            sigma_x_pixels,
            sigma_y_pixels,
            position_angle_rad: beam.position_angle_rad,
        })
}

fn auto_multithresh_plane_mask(
    residual: &Array2<f32>,
    max_psf_sidelobe_level: f32,
    min_region_pixels: usize,
    beam_shape: Option<AutoMaskBeamShape>,
    config: &CubeAutoMultiThresholdConfig,
) -> Array2<bool> {
    let Some(stats) = robust_plane_stats(residual) else {
        return Array2::<bool>::from_elem(residual.dim(), false);
    };
    let sidelobe_threshold =
        stats.median + max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax;
    let noise_threshold = stats.median + config.noise_threshold * stats.robust_rms;
    let low_noise_threshold = stats.median
        + (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
            .max(config.low_noise_threshold * stats.robust_rms);
    let main_threshold = sidelobe_threshold.max(noise_threshold);
    let mut initial = threshold_positive_mask(residual, main_threshold);
    prune_small_regions(&mut initial, min_region_pixels);
    let mut grown = smooth_and_cut_mask(&initial, beam_shape, config.cut_threshold);
    if config.grow_iterations > 0 {
        let constraint = threshold_positive_mask(residual, low_noise_threshold);
        grow_mask_constrained(&mut grown, &constraint, config.grow_iterations);
        if config.do_grow_prune {
            prune_small_regions(&mut grown, min_region_pixels);
        }
    }
    if config.negative_threshold > 0.0 {
        let negative_threshold = stats.median
            - (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
                .max(config.negative_threshold * stats.robust_rms);
        let mut negative = threshold_negative_mask(residual, negative_threshold);
        prune_small_regions(&mut negative, min_region_pixels);
        negative = smooth_and_cut_mask(&negative, beam_shape, config.cut_threshold);
        Zip::from(&mut grown)
            .and(&negative)
            .for_each(|out, generated| *out = *out || *generated);
    }
    grown
}

#[derive(Debug, Clone, Copy)]
struct RobustPlaneStats {
    median: f32,
    robust_rms: f32,
    absmax: f32,
}

fn robust_plane_stats(residual: &Array2<f32>) -> Option<RobustPlaneStats> {
    let mut values = residual
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let median = sorted_median(&values);
    let absmax = values
        .iter()
        .fold(0.0f32, |acc, value| acc.max(value.abs()));
    let mut deviations = values
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(|a, b| a.total_cmp(b));
    let mad_rms = sorted_median(&deviations) * 1.4826;
    let rms = (values.iter().map(|value| value * value).sum::<f32>() / values.len() as f32).sqrt();
    let robust_rms = if mad_rms > 0.0 { mad_rms } else { rms };
    Some(RobustPlaneStats {
        median,
        robust_rms,
        absmax,
    })
}

fn sorted_median(values: &[f32]) -> f32 {
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        0.5 * (values[mid - 1] + values[mid])
    } else {
        values[mid]
    }
}

fn threshold_positive_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value > threshold)
}

fn threshold_negative_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value < threshold)
}

fn smooth_and_cut_mask(
    mask: &Array2<bool>,
    beam_shape: Option<AutoMaskBeamShape>,
    cut_threshold: f32,
) -> Array2<bool> {
    let Some(beam_shape) = beam_shape else {
        return mask.clone();
    };
    let (nx, ny) = mask.dim();
    let radius_x = (beam_shape.sigma_x_pixels * 4.0).ceil().max(1.0) as isize;
    let radius_y = (beam_shape.sigma_y_pixels * 4.0).ceil().max(1.0) as isize;
    let cos_pa = beam_shape.position_angle_rad.cos();
    let sin_pa = beam_shape.position_angle_rad.sin();
    let mut smoothed = Array2::<f32>::zeros((nx, ny));
    for ((x, y), value) in mask.indexed_iter() {
        if !*value {
            continue;
        }
        let x = x as isize;
        let y = y as isize;
        for dx in -radius_x..=radius_x {
            let xx = x + dx;
            if !(0..nx as isize).contains(&xx) {
                continue;
            }
            for dy in -radius_y..=radius_y {
                let yy = y + dy;
                if !(0..ny as isize).contains(&yy) {
                    continue;
                }
                let rotated_x = dx as f64 * cos_pa + dy as f64 * sin_pa;
                let rotated_y = -dx as f64 * sin_pa + dy as f64 * cos_pa;
                let exponent = -0.5
                    * ((rotated_x / beam_shape.sigma_x_pixels).powi(2)
                        + (rotated_y / beam_shape.sigma_y_pixels).powi(2));
                smoothed[(xx as usize, yy as usize)] += exponent.exp() as f32;
            }
        }
    }
    let peak = smoothed.iter().copied().fold(0.0f32, f32::max);
    if !(peak.is_finite() && peak > 0.0) {
        return Array2::<bool>::from_elem((nx, ny), false);
    }
    let threshold = cut_threshold.max(0.0) * peak;
    smoothed.mapv(|value| value.is_finite() && value > threshold)
}

fn grow_mask_constrained(
    mask: &mut Array2<bool>,
    constraint: &Array2<bool>,
    max_iterations: usize,
) {
    let (nx, ny) = mask.dim();
    for _ in 0..max_iterations {
        let mut next = mask.clone();
        let mut changed = false;
        for x in 0..nx {
            for y in 0..ny {
                if mask[(x, y)] || !constraint[(x, y)] {
                    continue;
                }
                if neighboring_masked(mask, x, y) {
                    next[(x, y)] = true;
                    changed = true;
                }
            }
        }
        *mask = next;
        if !changed {
            break;
        }
    }
}

fn neighboring_masked(mask: &Array2<bool>, x: usize, y: usize) -> bool {
    let (nx, ny) = mask.dim();
    (x > 0 && mask[(x - 1, y)])
        || (x + 1 < nx && mask[(x + 1, y)])
        || (y > 0 && mask[(x, y - 1)])
        || (y + 1 < ny && mask[(x, y + 1)])
}

fn prune_small_regions(mask: &mut Array2<bool>, min_pixels: usize) {
    if min_pixels <= 1 {
        return;
    }
    let (nx, ny) = mask.dim();
    let mut visited = Array2::<bool>::from_elem((nx, ny), false);
    for x0 in 0..nx {
        for y0 in 0..ny {
            if visited[(x0, y0)] || !mask[(x0, y0)] {
                continue;
            }
            let mut region = Vec::new();
            let mut queue = VecDeque::from([(x0, y0)]);
            visited[(x0, y0)] = true;
            while let Some((x, y)) = queue.pop_front() {
                region.push((x, y));
                for (nx0, ny0) in neighbors4(mask.dim(), x, y) {
                    if !visited[(nx0, ny0)] && mask[(nx0, ny0)] {
                        visited[(nx0, ny0)] = true;
                        queue.push_back((nx0, ny0));
                    }
                }
            }
            if region.len() < min_pixels {
                for (x, y) in region {
                    mask[(x, y)] = false;
                }
            }
        }
    }
}

fn neighbors4(
    (nx, ny): (usize, usize),
    x: usize,
    y: usize,
) -> impl Iterator<Item = (usize, usize)> {
    let mut neighbors = [(usize::MAX, usize::MAX); 4];
    let mut count = 0;
    if x > 0 {
        neighbors[count] = (x - 1, y);
        count += 1;
    }
    if x + 1 < nx {
        neighbors[count] = (x + 1, y);
        count += 1;
    }
    if y > 0 {
        neighbors[count] = (x, y - 1);
        count += 1;
    }
    if y + 1 < ny {
        neighbors[count] = (x, y + 1);
        count += 1;
    }
    neighbors.into_iter().take(count)
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
    /// Peak absolute residual immediately after the block. Deconvolvers that
    /// maintain an approximate residual report that value before the exact
    /// major-cycle refresh; CASA-style Clark reports after the boundary refresh
    /// because its minor cycle only updates active pixels and the delta model.
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
    /// Sum of weighted-sample contributions used for FFT/image normalization.
    pub normalization_sumwt: f32,
    /// CASA-style reported `sumwt` persisted for the primary output plane.
    pub reported_sumwt: f32,
    /// PSF peak measured after `normalization_sumwt` scaling and before unit-peak normalization.
    pub psf_peak_normalization: f32,
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
    /// Time spent building backend executor state, including reusable sample
    /// plans and grid workspaces.
    pub executor_build: Duration,
    /// Time spent allocating and zero-initializing PSF grids.
    pub psf_grid_alloc: Duration,
    /// Time spent replaying/building planned scalar samples before grid updates.
    pub planned_sample_replay: Duration,
    /// Time spent in scalar grid-update loops after planned samples exist.
    pub grid_update: Duration,
    /// Time spent gridding PSF/sample weights.
    pub psf_grid: Duration,
    /// Time spent FFTing the PSF grid.
    pub psf_fft: Duration,
    /// Time spent copying/correcting the PSF image after FFT and before scalar
    /// normalization.
    pub psf_image_correction: Duration,
    /// Time spent applying PSF correction and normalization.
    pub psf_normalize: Duration,
    /// Time spent FFTing model images before degridding.
    pub model_fft: Duration,
    /// Time spent allocating and zero-initializing residual grids.
    pub residual_grid_alloc: Duration,
    /// Time spent degridding/gridding residual visibilities.
    pub residual_degrid_grid: Duration,
    /// Time spent FFTing residual grids back to image space.
    pub residual_fft: Duration,
    /// Time spent copying/correcting the residual image after FFT and before
    /// scalar normalization.
    pub residual_image_correction: Duration,
    /// Time spent applying residual correction and normalization.
    pub residual_normalize: Duration,
    /// Time spent preparing CLEAN cycle thresholds, peaks, and candidates.
    pub clean_cycle_setup: Duration,
    /// Time spent preparing deconvolver-specific reusable state.
    pub deconvolver_setup: Duration,
    /// Time spent in minor-cycle PSF subtraction/component updates.
    pub minor_cycle: Duration,
    /// Time spent inside the solver-specific minor-cycle loop.
    pub minor_cycle_solve: Duration,
    /// Time spent searching for CLEAN components inside solver-specific minor
    /// cycles.
    pub deconvolver_peak_search: Duration,
    /// Time spent collecting or rebuilding deconvolver-specific active sets.
    pub deconvolver_active_set_build: Duration,
    /// Time spent adding chosen components to model images.
    pub deconvolver_model_update: Duration,
    /// Time spent subtracting chosen components from residual-like solver
    /// state.
    pub deconvolver_psf_subtract: Duration,
    /// Time spent replaying accumulated components into refreshed residual
    /// state.
    pub deconvolver_residual_replay: Duration,
    /// Time spent in deconvolver-local convolution work.
    pub deconvolver_fft_convolve: Duration,
    /// Number of component-search operations performed inside deconvolvers.
    pub deconvolver_peak_searches: u64,
    /// Number of component/model updates performed inside deconvolvers.
    pub deconvolver_model_updates: u64,
    /// Number of residual-like subtraction updates performed inside
    /// deconvolvers.
    pub deconvolver_subtract_updates: u64,
    /// Estimated scalar image pixels examined by deconvolver peak searches.
    pub deconvolver_pixels_searched: u64,
    /// Estimated scalar image pixels touched by deconvolver component updates.
    pub deconvolver_pixels_touched: u64,
    /// Number of deconvolver peak-search operations that covered the full
    /// image extent.
    pub deconvolver_full_window_peak_searches: u64,
    /// Number of deconvolver subtraction operations that covered the full image
    /// extent.
    pub deconvolver_full_window_subtract_updates: u64,
    /// Largest scalar image area examined by a single deconvolver peak-search
    /// operation.
    pub deconvolver_peak_search_window_pixels_max: u64,
    /// Largest scalar image area touched by a single deconvolver subtraction
    /// operation.
    pub deconvolver_subtract_window_pixels_max: u64,
    /// Time spent recomputing the image residual during major-cycle refreshes.
    ///
    /// This is the aggregate wall time for each residual refresh and therefore
    /// includes the lower-level `model_fft`, `residual_degrid_grid`,
    /// `residual_fft`, and `residual_normalize` subtotals.
    pub major_cycle_refresh: Duration,
    /// Major-cycle refresh time not accounted for by model FFT, residual
    /// degrid/grid, residual FFT, or residual normalization.
    pub residual_refresh_overhead: Duration,
    /// Time spent rebuilding multiscale residual convolution state after an
    /// exact residual refresh.
    pub multiscale_scale_refresh: Duration,
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
            executor_build: Duration::ZERO,
            psf_grid_alloc: Duration::ZERO,
            planned_sample_replay: Duration::ZERO,
            grid_update: Duration::ZERO,
            psf_grid: Duration::ZERO,
            psf_fft: Duration::ZERO,
            psf_image_correction: Duration::ZERO,
            psf_normalize: Duration::ZERO,
            model_fft: Duration::ZERO,
            residual_grid_alloc: Duration::ZERO,
            residual_degrid_grid: Duration::ZERO,
            residual_fft: Duration::ZERO,
            residual_image_correction: Duration::ZERO,
            residual_normalize: Duration::ZERO,
            clean_cycle_setup: Duration::ZERO,
            deconvolver_setup: Duration::ZERO,
            minor_cycle: Duration::ZERO,
            minor_cycle_solve: Duration::ZERO,
            deconvolver_peak_search: Duration::ZERO,
            deconvolver_active_set_build: Duration::ZERO,
            deconvolver_model_update: Duration::ZERO,
            deconvolver_psf_subtract: Duration::ZERO,
            deconvolver_residual_replay: Duration::ZERO,
            deconvolver_fft_convolve: Duration::ZERO,
            deconvolver_peak_searches: 0,
            deconvolver_model_updates: 0,
            deconvolver_subtract_updates: 0,
            deconvolver_pixels_searched: 0,
            deconvolver_pixels_touched: 0,
            deconvolver_full_window_peak_searches: 0,
            deconvolver_full_window_subtract_updates: 0,
            deconvolver_peak_search_window_pixels_max: 0,
            deconvolver_subtract_window_pixels_max: 0,
            major_cycle_refresh: Duration::ZERO,
            residual_refresh_overhead: Duration::ZERO,
            multiscale_scale_refresh: Duration::ZERO,
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

/// Result of a dirty-only imaging run that does not materialize clean products.
#[derive(Debug, Clone, PartialEq)]
pub struct DirtyImagingResult {
    /// Normalized PSF cube with degenerate Stokes/Frequency axes.
    pub psf: Array4<f32>,
    /// Dirty residual cube with degenerate Stokes/Frequency axes.
    pub residual: Array4<f32>,
    /// CASA-style `sumwt` product stored as a single degenerate pixel.
    pub sumwt: Array4<f32>,
    /// Restoring beam fitted from the PSF, when the fit succeeds.
    pub beam: Option<BeamFit>,
    /// Diagnostics collected while building the dirty products.
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

#[cfg(test)]
mod auto_multithreshold_tests {
    use super::*;

    #[test]
    fn auto_multithresh_mask_thresholds_grows_and_prunes_regions() {
        let mut residual = Array2::<f32>::zeros((9, 9));
        residual[(4, 4)] = 10.0;
        residual[(4, 5)] = 4.0;
        residual[(5, 4)] = 4.0;
        residual[(0, 0)] = 9.0;
        let config = CubeAutoMultiThresholdConfig {
            sidelobe_threshold: 0.0,
            noise_threshold: 2.0,
            low_noise_threshold: 0.4,
            min_beam_frac: 0.0,
            grow_iterations: 2,
            ..CubeAutoMultiThresholdConfig::default()
        };

        let mask = auto_multithresh_plane_mask(&residual, 0.0, 2, None, &config);
        assert!(mask[(4, 4)]);
        assert!(mask[(4, 5)]);
        assert!(mask[(5, 4)]);
        assert!(!mask[(0, 0)], "single-pixel island should be pruned");
    }

    #[test]
    fn auto_multithresh_cube_mask_keeps_channels_separate() {
        let geometry = ImageGeometry {
            image_shape: [9, 9],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut residual = Array4::<f32>::zeros((9, 9, 1, 2));
        residual[(4, 4, 0, 0)] = 10.0;
        residual[(1, 7, 0, 1)] = 10.0;
        let diagnostics = |max_psf_sidelobe_level| ImagingDiagnostics {
            warnings: Vec::new(),
            gridded_samples: 0,
            skipped_samples: 0,
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            psf_peak_normalization: 0.0,
            major_cycles: 0,
            minor_iterations: 0,
            clean_stop_reason: None,
            minor_cycle_traces: Vec::new(),
            initial_residual_peak_jy_per_beam: 0.0,
            final_residual_peak_jy_per_beam: 0.0,
            max_abs_w_lambda: 0.0,
            fractional_bandwidth: 0.0,
            max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam: 0.0,
            clean_mask_pixels: 0,
            beam_fit_attempts: 0,
            beam_fit_cutoff_used: None,
            beam_fit_debug: None,
            mosaic_weight_image: None,
            stage_timings: ImagingStageTimings::default(),
        };
        let dirty = CubeImagingResult {
            psf: Array4::<f32>::zeros((9, 9, 1, 2)),
            residual,
            model: Array4::<f32>::zeros((9, 9, 1, 2)),
            image: Array4::<f32>::zeros((9, 9, 1, 2)),
            sumwt: Array4::<f32>::zeros((1, 1, 1, 2)),
            clean_mask: None,
            beams: vec![None, None],
            restored_beams: vec![None, None],
            diagnostics: CubeImagingDiagnostics {
                warnings: Vec::new(),
                gridded_samples: 0,
                skipped_samples: 0,
                major_cycles: 0,
                minor_iterations: 0,
                clean_stop_reason: None,
                channel_diagnostics: vec![diagnostics(0.1), diagnostics(0.1)],
                stage_timings: ImagingStageTimings::default(),
            },
            compatibility: CompatibilityMetadata {
                axis_order: [
                    AxisKind::RightAscension,
                    AxisKind::Declination,
                    AxisKind::Stokes,
                    AxisKind::Frequency,
                ],
                plane_stokes: PlaneStokes::I,
                reffreq_hz: 1.5,
                channel_frequencies_hz: vec![1.0, 2.0],
                psf_units: String::new(),
                residual_units: "Jy/beam".to_string(),
                model_units: "Jy/pixel".to_string(),
                image_units: "Jy/beam".to_string(),
            },
        };
        let config = CubeAutoMultiThresholdConfig {
            sidelobe_threshold: 0.5,
            noise_threshold: 0.0,
            low_noise_threshold: 0.0,
            min_beam_frac: 0.0,
            grow_iterations: 0,
            ..CubeAutoMultiThresholdConfig::default()
        };

        let mask = config
            .build_cube_clean_mask(geometry, &dirty, &dirty.beams, None)
            .unwrap();

        assert!(mask[(4, 4, 0, 0)]);
        assert!(!mask[(4, 4, 0, 1)]);
        assert!(mask[(1, 7, 0, 1)]);
        assert!(!mask[(1, 7, 0, 0)]);
    }
}

#[cfg(test)]
mod source_view_tests {
    use super::*;

    #[test]
    fn visibility_block_view_validates_shape_and_routes() {
        let partition = VisibilitySourcePartition {
            id: VisibilitySourcePartitionId(0),
            ms_id: 0,
            data_desc_id: 3,
            spectral_window_id: 4,
            polarization_id: 5,
            shape: VisibilitySourceShape {
                channel_count: 4,
                correlation_count: 2,
            },
        };
        let row_indices = [10usize, 11];
        let data = vec![Complex32::new(1.0, 0.0); 8];
        let flags = vec![false; 8];
        let weights = vec![1.0f32; 4];
        let uvw = vec![0.0f64; 6];
        let flag_row = [false, true];
        let source = VisibilityBlockView {
            partition,
            row_indices: &row_indices,
            channel_start: 1,
            channel_count: 2,
            data: Some(VisibilityComplexSamplesRef::Complex32(&data)),
            flags: Some(&flags),
            weights: Some(VisibilityFloatSamplesRef::Float32(&weights)),
            weight_spectrum: None,
            uvw_m: Some(&uvw),
            flag_row: Some(&flag_row),
            antenna1: None,
            antenna2: None,
            field_ids: None,
            time: None,
        };
        let spectral = SpectralRoutePlan {
            channel_routes: vec![
                SourceChannelRoute {
                    source_channel: 1,
                    output_planes: vec![OutputPlaneContribution {
                        plane_index: 0,
                        factor: 1.0,
                    }],
                },
                SourceChannelRoute {
                    source_channel: 2,
                    output_planes: vec![OutputPlaneContribution {
                        plane_index: 1,
                        factor: 1.0,
                    }],
                },
            ],
        };
        assert_eq!(SpectralRoutePlan::identity_for_block(source), spectral);
        let polarization = PolarizationRoutePlan {
            output_stokes: PlaneStokes::I,
        };
        let geometry = GeometryRoutePlan {
            geometry: ImageGeometry {
                image_shape: [64, 64],
                cell_size_rad: [1.0e-6, 1.0e-6],
            },
        };
        let weighting = WeightingRoutePlan {
            weighting: WeightingMode::Natural,
        };
        let gridder = GridderRoutePlan {
            gridder_mode: GridderMode::Standard,
        };
        let view = ImagingSourceBlockView {
            source,
            spectral: &spectral,
            polarization: &polarization,
            geometry: &geometry,
            weighting: &weighting,
            gridder: &gridder,
            model: None,
        };

        view.validate().unwrap();
        assert_eq!(view.source.channel_range(), 1..3);
        assert_eq!(view.source.channel_row_corr_index(1, 1, 1), 7);
    }

    #[test]
    fn visibility_block_view_rejects_route_channel_mismatch() {
        let partition = VisibilitySourcePartition {
            id: VisibilitySourcePartitionId(0),
            ms_id: 0,
            data_desc_id: 3,
            spectral_window_id: 4,
            polarization_id: 5,
            shape: VisibilitySourceShape {
                channel_count: 4,
                correlation_count: 1,
            },
        };
        let row_indices = [10usize];
        let data = vec![Complex32::new(1.0, 0.0); 2];
        let source = VisibilityBlockView {
            partition,
            row_indices: &row_indices,
            channel_start: 0,
            channel_count: 2,
            data: Some(VisibilityComplexSamplesRef::Complex32(&data)),
            flags: None,
            weights: None,
            weight_spectrum: None,
            uvw_m: None,
            flag_row: None,
            antenna1: None,
            antenna2: None,
            field_ids: None,
            time: None,
        };
        let spectral = SpectralRoutePlan {
            channel_routes: vec![SourceChannelRoute {
                source_channel: 0,
                output_planes: Vec::new(),
            }],
        };
        let polarization = PolarizationRoutePlan {
            output_stokes: PlaneStokes::I,
        };
        let geometry = GeometryRoutePlan {
            geometry: ImageGeometry {
                image_shape: [64, 64],
                cell_size_rad: [1.0e-6, 1.0e-6],
            },
        };
        let weighting = WeightingRoutePlan {
            weighting: WeightingMode::Natural,
        };
        let gridder = GridderRoutePlan {
            gridder_mode: GridderMode::Standard,
        };
        let view = ImagingSourceBlockView {
            source,
            spectral: &spectral,
            polarization: &polarization,
            geometry: &geometry,
            weighting: &weighting,
            gridder: &gridder,
            model: None,
        };

        let error = view.validate().unwrap_err();
        assert!(
            error.to_string().contains("spectral route count"),
            "{error}"
        );
    }

    #[test]
    fn visibility_stream_bounds_require_bounded_residency() {
        let valid = VisibilityStreamBounds {
            max_rows_per_block: 2,
            max_channels_per_block: 4,
            max_correlations: 2,
            max_concurrent_blocks: 1,
            max_resident_visibility_bytes: 256,
        };
        valid.validate().unwrap();

        let invalid = VisibilityStreamBounds {
            max_resident_visibility_bytes: 0,
            ..valid
        };
        let error = invalid.validate().unwrap_err();
        assert!(
            error.to_string().contains("max_resident_visibility_bytes"),
            "{error}"
        );
    }

    #[test]
    fn standard_mfs_density_source_plan_uses_full_cube_domain_for_combined_dirty_briggs() {
        let plan = plan_standard_mfs_density_source(StandardMfsDensitySourcePlanRequest {
            cube_like: true,
            dirty_only: true,
            target_channel_count: Some(1),
            full_source_channel_count: 16,
            per_channel_weight_density: false,
            weighting: WeightingMode::Briggs { robust: 0.5 },
        });

        assert_eq!(plan.target_channel_count, 1);
        assert_eq!(plan.density_channel_start, None);
        assert_eq!(plan.density_channel_count, None);
        assert_eq!(plan.density_channel_count_estimate, 16);
        assert!(plan.uses_distinct_density_source);
    }

    #[test]
    fn standard_mfs_density_source_plan_keeps_target_domain_without_density_weighting_switch() {
        for (weighting, per_channel_weight_density) in [
            (WeightingMode::Natural, false),
            (WeightingMode::Briggs { robust: 0.5 }, true),
        ] {
            let plan = plan_standard_mfs_density_source(StandardMfsDensitySourcePlanRequest {
                cube_like: true,
                dirty_only: true,
                target_channel_count: Some(1),
                full_source_channel_count: 16,
                per_channel_weight_density,
                weighting,
            });

            assert_eq!(plan.target_channel_count, 1);
            assert_eq!(plan.density_channel_start, None);
            assert_eq!(plan.density_channel_count, Some(1));
            assert_eq!(plan.density_channel_count_estimate, 1);
            assert!(!plan.uses_distinct_density_source);
        }
    }
}
