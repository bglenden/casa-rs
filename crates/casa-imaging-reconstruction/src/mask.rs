// SPDX-License-Identifier: LGPL-3.0-or-later

//! Generation-bound reconstruction masks.
//!
//! Reconstruction masks constrain where model updates may be placed. They are
//! intentionally distinct from product-validity masks and consume immutable
//! model/Normal-State lineage. Static user masks and auto-multithreshold masks
//! mint the same closed generation type, so solvers need no mask-mode branch.

use std::{collections::VecDeque, fmt};

use casa_imaging_model::{CompiledProblemId, DirectionCoordinateSpec, LogicalIdentity};
use thiserror::Error;

use crate::{Encoder, FinalNormalState, FinalNormalStateCompletionId, ModelGenerationId};

/// Reproject decoded source support onto the target direction grid.
///
/// Storage/application code supplies only decoded samples and coordinates;
/// reconstruction owns traversal, mapping, sampling, and uncovered support.
pub fn reproject_mask_support(
    source_coordinate: DirectionCoordinateSpec,
    source_shape: [usize; 2],
    source_support: &[bool],
    target_coordinate: DirectionCoordinateSpec,
    target_shape: [usize; 2],
) -> Result<Box<[bool]>, MaskError> {
    validate_shape(source_shape)?;
    validate_shape(target_shape)?;
    if source_support.len() != source_shape[0] * source_shape[1] {
        return Err(MaskError::ShapeMismatch);
    }
    let mut target_support = vec![false; target_shape[0] * target_shape[1]];
    for x in 0..target_shape[0] {
        for y in 0..target_shape[1] {
            let world = direction_pixel_to_world(target_coordinate, [x as f64, y as f64])?;
            let source = direction_world_to_pixel(source_coordinate, world)?;
            let sx = source[0].round();
            let sy = source[1].round();
            if sx >= 0.0
                && sy >= 0.0
                && sx < source_shape[0] as f64
                && sy < source_shape[1] as f64
                && source_support[sx as usize * source_shape[1] + sy as usize]
            {
                target_support[x * target_shape[1] + y] = true;
            }
        }
    }
    Ok(target_support.into_boxed_slice())
}

fn direction_pixel_to_world(
    coordinate: DirectionCoordinateSpec,
    pixel: [f64; 2],
) -> Result<[f64; 2], MaskError> {
    if coordinate.projection() != casa_imaging_model::Projection::Sin {
        return Err(MaskError::UnsupportedReprojection);
    }
    let offset = [
        pixel[0] - coordinate.reference_pixel()[0],
        pixel[1] - coordinate.reference_pixel()[1],
    ];
    let pc = coordinate.pc();
    let increment = coordinate.increment_rad();
    let x = increment[0] * (pc[0][0] * offset[0] + pc[0][1] * offset[1]);
    let y = increment[1] * (pc[1][0] * offset[0] + pc[1][1] * offset[1]);
    let radius_squared = x * x + y * y;
    if radius_squared > 1.0 + 1.0e-12 {
        return Err(MaskError::UnsupportedReprojection);
    }
    let phi = if radius_squared == 0.0 {
        0.0
    } else {
        x.atan2(-y)
    };
    let theta = (1.0 - radius_squared.min(1.0)).max(0.0).sqrt().asin();
    let reference = coordinate.reference_direction();
    let alpha_p = reference.longitude_rad();
    let delta_p = reference.latitude_rad();
    let phi_p = coordinate.pole_deg()[0].to_radians();
    let dphi = phi - phi_p;
    let sin_lat = theta.sin() * delta_p.sin() + theta.cos() * delta_p.cos() * dphi.cos();
    let latitude = sin_lat.clamp(-1.0, 1.0).asin();
    let longitude = alpha_p
        + (-theta.cos() * dphi.sin())
            .atan2(theta.sin() * delta_p.cos() - theta.cos() * delta_p.sin() * dphi.cos());
    Ok([longitude, latitude])
}

fn direction_world_to_pixel(
    coordinate: DirectionCoordinateSpec,
    world: [f64; 2],
) -> Result<[f64; 2], MaskError> {
    if coordinate.projection() != casa_imaging_model::Projection::Sin {
        return Err(MaskError::UnsupportedReprojection);
    }
    let reference = coordinate.reference_direction();
    let alpha_p = reference.longitude_rad();
    let delta_p = reference.latitude_rad();
    let phi_p = coordinate.pole_deg()[0].to_radians();
    let delta_alpha = world[0] - alpha_p;
    let sin_theta =
        world[1].sin() * delta_p.sin() + world[1].cos() * delta_p.cos() * delta_alpha.cos();
    let theta = sin_theta.clamp(-1.0, 1.0).asin();
    let phi = phi_p
        + (-world[1].cos() * delta_alpha.sin()).atan2(
            world[1].sin() * delta_p.cos() - world[1].cos() * delta_p.sin() * delta_alpha.cos(),
        );
    let x = theta.cos() * phi.sin();
    let y = -theta.cos() * phi.cos();
    let increment = coordinate.increment_rad();
    let intermediate = [x / increment[0], y / increment[1]];
    let pc = coordinate.pc();
    let determinant = pc[0][0] * pc[1][1] - pc[0][1] * pc[1][0];
    if !determinant.is_finite() || determinant.abs() < 1.0e-15 {
        return Err(MaskError::UnsupportedReprojection);
    }
    let offset = [
        (pc[1][1] * intermediate[0] - pc[0][1] * intermediate[1]) / determinant,
        (-pc[1][0] * intermediate[0] + pc[0][0] * intermediate[1]) / determinant,
    ];
    let reference_pixel = coordinate.reference_pixel();
    let pixel = [
        reference_pixel[0] + offset[0],
        reference_pixel[1] + offset[1],
    ];
    if pixel.iter().all(|value| value.is_finite()) {
        Ok(pixel)
    } else {
        Err(MaskError::UnsupportedReprojection)
    }
}

const MASK_DOMAIN: &[u8] = b"casa-rs-reconstruction-mask";
const MASK_VERSION: u32 = 1;

/// Stable identity of one immutable reconstruction-mask generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReconstructionMaskGenerationId(LogicalIdentity);

impl ReconstructionMaskGenerationId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ReconstructionMaskGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReconstructionMaskGenerationId(")?;
        crate::write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

/// Inclusive pixel box in the named mask coordinate grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaskBox {
    blc: [usize; 2],
    trc: [usize; 2],
}

/// Deferred static mask construction evaluated at an exact major-cycle boundary.
#[derive(Debug, Clone)]
pub enum ReconstructionMaskPlan {
    /// Admit every model-support pixel.
    FullPlane {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
    },
    /// Admit the union of explicit target-grid boxes.
    Boxes {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// Inclusive target-grid regions.
        boxes: Vec<MaskBox>,
    },
    /// Admit exact target-grid support reprojected from a named source WCS.
    Reprojected {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// Source mask direction coordinate retained as lineage evidence.
        source_coordinate: DirectionCoordinateSpec,
        /// Source mask direction-plane shape.
        source_shape: [usize; 2],
        /// Canonical target-grid support produced by exact WCS reprojection.
        support: Box<[bool]>,
    },
    /// Generate CASA auto-multithreshold support from the current Normal State.
    AutoMultithresh {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// Thresholding, pruning, smoothing, and growth controls.
        controls: AutoMultithreshControls,
        /// Number of completed major cycles before this boundary.
        completed_major_cycles: usize,
        /// Whether the previous cycle reached its stopping threshold.
        cycle_threshold_reached: bool,
        /// Prior immutable mask generation, when another cycle already ran.
        previous: Option<Box<ReconstructionMask>>,
        /// Whether CASA's channel-stop rule froze further mask evolution.
        evolution_stopped: bool,
    },
}

impl ReconstructionMaskPlan {
    /// Advance live automasking to the next major-cycle boundary.
    #[must_use]
    pub fn next_cycle(
        &self,
        current: &ReconstructionMask,
        completed_major_cycles: usize,
        cycle_threshold_reached: bool,
        evolution_stopped: bool,
    ) -> Self {
        match self {
            Self::AutoMultithresh {
                coordinate,
                controls,
                ..
            } => Self::AutoMultithresh {
                coordinate: *coordinate,
                controls: *controls,
                completed_major_cycles,
                cycle_threshold_reached,
                previous: Some(Box::new(current.clone())),
                evolution_stopped,
            },
            other => other.clone(),
        }
    }

    /// Materialize one immutable generation for the exact current model.
    pub fn materialize(
        &self,
        base: &crate::ModelGeneration,
        normal: &FinalNormalState,
    ) -> Result<(ReconstructionMask, Option<AutoMultithreshEvidence>), MaskError> {
        let problem = normal.problem_id();
        let model_generation = base.generation_id();
        let shape = normal.shape();
        match self {
            Self::FullPlane { coordinate } => Ok((
                ReconstructionMask::full_plane(problem, model_generation, *coordinate, shape)?,
                None,
            )),
            Self::Boxes { coordinate, boxes } => Ok((
                ReconstructionMask::from_boxes(
                    problem,
                    model_generation,
                    *coordinate,
                    shape,
                    boxes.iter().copied(),
                )?,
                None,
            )),
            Self::Reprojected {
                coordinate,
                source_coordinate,
                source_shape,
                support,
            } => Ok((
                ReconstructionMask::from_reprojected_support(
                    problem,
                    model_generation,
                    *coordinate,
                    shape,
                    support,
                    *source_coordinate,
                    *source_shape,
                )?,
                None,
            )),
            Self::AutoMultithresh {
                coordinate,
                controls,
                completed_major_cycles,
                cycle_threshold_reached,
                previous,
                evolution_stopped,
            } => {
                let valid_support = base
                    .samples()
                    .iter()
                    .map(|sample| sample.support() == casa_imaging_model::ModelSupport::Valid)
                    .collect::<Vec<_>>();
                let (mask, evidence) = auto_multithresh(
                    problem,
                    model_generation,
                    *coordinate,
                    normal,
                    previous.as_deref(),
                    &valid_support,
                    *completed_major_cycles,
                    *cycle_threshold_reached,
                    *evolution_stopped,
                    *controls,
                )?;
                Ok((mask, Some(evidence)))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AutoMaskBeam {
    major_fwhm_pixels: f64,
    minor_fwhm_pixels: f64,
    position_angle_rad: f64,
    area_pixels: f64,
    sidelobe_fraction: f64,
}

fn fit_auto_mask_beam(normal: &FinalNormalState) -> Result<AutoMaskBeam, MaskError> {
    let shape = normal.shape();
    let psf = normal
        .normal_approximation()
        .iter()
        .map(|sample| sample.re as f32)
        .collect::<Vec<_>>();
    let fitted = crate::fit_restoring_beam(&psf, shape, [1.0, 1.0], crate::DEFAULT_PSF_FIT_CUTOFF)
        .map_err(|_| MaskError::InvalidBeamArea)?;
    let major_fwhm_pixels = fitted.major_fwhm_rad();
    let minor_fwhm_pixels = fitted.minor_fwhm_rad();
    let area_pixels = std::f64::consts::PI * major_fwhm_pixels * minor_fwhm_pixels
        / (4.0 * std::f64::consts::LN_2);
    let sidelobe_fraction =
        crate::psf_beam::fitted_psf_sidelobe_fraction_with_beam(&psf, shape, fitted)
            .map_err(|_| MaskError::InvalidBeamArea)?;
    if [
        major_fwhm_pixels,
        minor_fwhm_pixels,
        area_pixels,
        sidelobe_fraction,
    ]
    .into_iter()
    .all(|value| value.is_finite() && value >= 0.0)
        && major_fwhm_pixels > 0.0
        && minor_fwhm_pixels > 0.0
        && area_pixels > 0.0
        && fitted.position_angle_rad().is_finite()
    {
        Ok(AutoMaskBeam {
            major_fwhm_pixels,
            minor_fwhm_pixels,
            position_angle_rad: fitted.position_angle_rad(),
            area_pixels,
            sidelobe_fraction,
        })
    } else {
        Err(MaskError::InvalidBeamArea)
    }
}

impl MaskBox {
    /// Construct an inclusive non-inverted box.
    pub fn new(blc: [usize; 2], trc: [usize; 2]) -> Result<Self, MaskError> {
        if blc[0] > trc[0] || blc[1] > trc[1] {
            return Err(MaskError::InvertedBox);
        }
        Ok(Self { blc, trc })
    }

    /// Return the inclusive lower corner.
    #[must_use]
    pub const fn blc(self) -> [usize; 2] {
        self.blc
    }

    /// Return the inclusive upper corner.
    #[must_use]
    pub const fn trc(self) -> [usize; 2] {
        self.trc
    }
}

/// Immutable reconstruction-mask generation.
#[derive(Debug, Clone)]
pub struct ReconstructionMask {
    generation: ReconstructionMaskGenerationId,
    problem: CompiledProblemId,
    model_generation: ModelGenerationId,
    normal_state: Option<FinalNormalStateCompletionId>,
    coordinate: DirectionCoordinateSpec,
    shape: [usize; 2],
    support: Box<[bool]>,
}

impl ReconstructionMask {
    /// Mint the all-valid default support for one exact model grid.
    pub fn full_plane(
        problem: CompiledProblemId,
        model_generation: ModelGenerationId,
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
    ) -> Result<Self, MaskError> {
        validate_shape(shape)?;
        Self::mint(
            problem,
            model_generation,
            None,
            coordinate,
            shape,
            vec![true; shape[0] * shape[1]],
            2,
            &[],
        )
    }

    /// Compile target-grid pixel boxes into one static mask generation.
    pub fn from_boxes(
        problem: CompiledProblemId,
        model_generation: ModelGenerationId,
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
        boxes: impl IntoIterator<Item = MaskBox>,
    ) -> Result<Self, MaskError> {
        validate_shape(shape)?;
        let mut support = vec![false; shape[0] * shape[1]];
        for region in boxes {
            if region.trc[0] >= shape[0] || region.trc[1] >= shape[1] {
                return Err(MaskError::OutsideTarget);
            }
            for x in region.blc[0]..=region.trc[0] {
                for y in region.blc[1]..=region.trc[1] {
                    support[x * shape[1] + y] = true;
                }
            }
        }
        if !support.iter().any(|value| *value) {
            return Err(MaskError::EmptyMask);
        }
        Self::mint(
            problem,
            model_generation,
            None,
            coordinate,
            shape,
            support,
            0,
            &[],
        )
    }

    /// Mint exact target-grid support already reprojected by the geometry owner.
    #[allow(clippy::too_many_arguments)]
    pub fn from_reprojected_support(
        problem: CompiledProblemId,
        model_generation: ModelGenerationId,
        target_coordinate: DirectionCoordinateSpec,
        target_shape: [usize; 2],
        support: &[bool],
        source_coordinate: DirectionCoordinateSpec,
        source_shape: [usize; 2],
    ) -> Result<Self, MaskError> {
        validate_shape(target_shape)?;
        validate_shape(source_shape)?;
        if support.len() != target_shape[0] * target_shape[1] {
            return Err(MaskError::ShapeMismatch);
        }
        if !support.iter().any(|value| *value) {
            return Err(MaskError::EmptyMask);
        }
        let mut source_identity = Encoder::new(b"casa-rs-reprojected-mask-source", 1);
        source_identity.usize(source_shape[0]);
        source_identity.usize(source_shape[1]);
        encode_coordinate(&mut source_identity, source_coordinate);
        Self::mint(
            problem,
            model_generation,
            None,
            target_coordinate,
            target_shape,
            support.to_vec(),
            1,
            &source_identity.finish(),
        )
    }

    /// Return the stable generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> ReconstructionMaskGenerationId {
        self.generation
    }

    /// Return the compiled problem whose model grid this mask constrains.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the exact model generation constrained by this mask.
    #[must_use]
    pub const fn model_generation(&self) -> ModelGenerationId {
        self.model_generation
    }

    /// Return the Normal State generation consumed by an auto mask.
    #[must_use]
    pub const fn normal_state_completion(&self) -> Option<FinalNormalStateCompletionId> {
        self.normal_state
    }

    /// Return the exact target direction coordinate.
    #[must_use]
    pub const fn coordinate(&self) -> DirectionCoordinateSpec {
        self.coordinate
    }

    /// Return the `[x, y]` mask shape.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// Return canonical x-major support bits.
    #[must_use]
    pub fn support(&self) -> &[bool] {
        &self.support
    }

    /// Test one target-grid pixel.
    #[must_use]
    pub fn contains(&self, pixel: [usize; 2]) -> bool {
        pixel[0] < self.shape[0]
            && pixel[1] < self.shape[1]
            && self.support[pixel[0] * self.shape[1] + pixel[1]]
    }

    #[allow(clippy::too_many_arguments)]
    fn mint(
        problem: CompiledProblemId,
        model_generation: ModelGenerationId,
        normal_state: Option<FinalNormalStateCompletionId>,
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
        support: Vec<bool>,
        source_kind: u8,
        source_identity: &[u8],
    ) -> Result<Self, MaskError> {
        let generation = mask_identity(
            problem,
            model_generation,
            normal_state,
            coordinate,
            shape,
            &support,
            source_kind,
            source_identity,
        );
        Ok(Self {
            generation,
            problem,
            model_generation,
            normal_state,
            coordinate,
            shape,
            support: support.into_boxed_slice(),
        })
    }
}

/// CASA auto-multithreshold controls.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultithreshControls {
    /// Sidelobe threshold multiplier.
    pub sidelobe_factor: f64,
    /// Robust-noise threshold multiplier.
    pub noise_factor: f64,
    /// Low-noise growth multiplier.
    pub low_noise_factor: f64,
    /// Negative-feature threshold multiplier; zero disables negative masks.
    pub negative_factor: f64,
    /// Minimum connected-region area as a fraction of the beam area.
    pub minimum_beam_fraction: f64,
    /// Gaussian smoothing FWHM as a fraction of the restoring beam.
    pub smooth_factor: f64,
    /// Fraction of the smoothed-mask peak retained.
    pub cut_threshold: f64,
    /// Four-connected growth iteration bound.
    pub grow_iterations: usize,
    /// Percent-change channel-stop threshold.
    pub minimum_percent_change: f64,
}

/// Inspectable evidence for one auto-multithreshold generation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultithreshEvidence {
    /// Prior immutable mask generation consumed by this update, when present.
    pub previous_mask_generation: Option<ReconstructionMaskGenerationId>,
    /// Robust residual median.
    pub median: f64,
    /// MAD-derived robust RMS.
    pub robust_rms: f64,
    /// Positive detection threshold.
    pub positive_threshold: f64,
    /// Low-noise growth threshold.
    pub low_noise_threshold: f64,
    /// Optional negative detection threshold.
    pub negative_threshold: Option<f64>,
    /// Number of support pixels that changed.
    pub changed_pixels: usize,
    /// Whether later cycles should stop evolving this plane.
    pub channel_stopped: bool,
}

/// Generate a new auto-multithreshold mask from immutable Normal State.
#[allow(clippy::too_many_arguments)]
pub fn auto_multithresh(
    problem: CompiledProblemId,
    model_generation: ModelGenerationId,
    coordinate: DirectionCoordinateSpec,
    normal: &FinalNormalState,
    previous: Option<&ReconstructionMask>,
    valid_support: &[bool],
    completed_major_cycles: usize,
    cycle_threshold_reached: bool,
    evolution_stopped: bool,
    controls: AutoMultithreshControls,
) -> Result<(ReconstructionMask, AutoMultithreshEvidence), MaskError> {
    let beam = fit_auto_mask_beam(normal)?;
    validate_auto_controls(controls, beam.area_pixels)?;
    let shape = normal.shape();
    if valid_support.len() != shape[0] * shape[1]
        || previous.is_some_and(|mask| {
            mask.problem != problem || mask.shape != shape || mask.coordinate != coordinate
        })
    {
        return Err(MaskError::ShapeMismatch);
    }
    // `FinalNormalState` retains the unnormalised normal-equation planes. CASA
    // derives automask thresholds from the displayed residual in model units,
    // so remove the accumulated normalisation exactly as the minor-cycle
    // solver does before applying Jy-valued controls.
    let psf_peak = normal
        .normal_approximation()
        .iter()
        .map(|value| value.re)
        .fold(f64::NEG_INFINITY, f64::max);
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MaskError::InvalidBeamArea);
    }
    let residual = normal
        .residual()
        .iter()
        .map(|value| value.re / psf_peak)
        .collect::<Vec<_>>();
    if residual.iter().any(|value| !value.is_finite()) {
        return Err(MaskError::NonfiniteResidual);
    }
    let residual_median = median(&residual);
    let deviations = residual
        .iter()
        .map(|value| (value - residual_median).abs())
        .collect::<Vec<_>>();
    let robust_rms = 1.482_602_218_505_602 * median(&deviations);
    let absolute_peak = residual
        .iter()
        .map(|value| (value - residual_median).abs())
        .fold(0.0_f64, f64::max);
    let sidelobe_level = beam.sidelobe_fraction;
    let positive_offset = (controls.sidelobe_factor * sidelobe_level * absolute_peak)
        .max(controls.noise_factor * robust_rms);
    let low_offset = (controls.sidelobe_factor * sidelobe_level * absolute_peak)
        .max(controls.low_noise_factor * robust_rms);
    let positive_threshold = residual_median + positive_offset;
    let low_noise_threshold = residual_median + low_offset;
    let negative_threshold = (controls.negative_factor > 0.0).then(|| {
        residual_median
            - (controls.sidelobe_factor * sidelobe_level * absolute_peak)
                .max(controls.negative_factor * robust_rms)
    });
    let mut detected = residual
        .iter()
        .zip(valid_support)
        .map(|(value, valid)| {
            *valid
                && (*value >= positive_threshold
                    || negative_threshold.is_some_and(|threshold| *value <= threshold))
        })
        .collect::<Vec<_>>();
    if evolution_stopped {
        detected.fill(false);
    }
    if !evolution_stopped {
        prune_regions(
            &mut detected,
            shape,
            (controls.minimum_beam_fraction * beam.area_pixels).ceil() as usize,
        );
        detected = smooth_and_cut(
            &detected,
            shape,
            controls.smooth_factor,
            beam,
            controls.cut_threshold,
        );
        if completed_major_cycles > 0 {
            grow_cross(
                &mut detected,
                shape,
                &residual,
                low_noise_threshold,
                controls.grow_iterations,
            );
        }
    }
    let mut support =
        previous.map_or_else(|| vec![false; detected.len()], |mask| mask.support.to_vec());
    let before = support.clone();
    for index in 0..support.len() {
        support[index] = valid_support[index] && (support[index] || detected[index]);
    }
    let changed_pixels = support
        .iter()
        .zip(&before)
        .filter(|(left, right)| left != right)
        .count();
    let previous_pixels = before.iter().filter(|value| **value).count();
    let percent_change = if previous_pixels == 0 {
        if changed_pixels == 0 { 0.0 } else { 100.0 }
    } else {
        100.0 * changed_pixels as f64 / previous_pixels as f64
    };
    let channel_stopped = evolution_stopped
        || support.iter().all(|value| !*value)
        || (controls.minimum_percent_change >= 0.0
            && cycle_threshold_reached
            && percent_change <= controls.minimum_percent_change);
    let mut source = Encoder::new(b"casa-rs-auto-multithresh-evidence", 1);
    source.identity(normal.completion_id().as_bytes());
    source.u64(crate::canonical_f64_bits(positive_threshold));
    source.u64(crate::canonical_f64_bits(low_noise_threshold));
    source.usize(completed_major_cycles);
    source.u8(u8::from(cycle_threshold_reached));
    source.u8(u8::from(evolution_stopped));
    match previous {
        Some(previous) => {
            source.u8(1);
            source.identity(previous.generation_id().as_bytes());
        }
        None => source.u8(0),
    }
    for value in [
        controls.sidelobe_factor,
        controls.noise_factor,
        controls.low_noise_factor,
        controls.negative_factor,
        controls.minimum_beam_fraction,
        controls.smooth_factor,
        controls.cut_threshold,
        controls.minimum_percent_change,
        beam.major_fwhm_pixels,
        beam.minor_fwhm_pixels,
        beam.position_angle_rad,
        beam.area_pixels,
        beam.sidelobe_fraction,
    ] {
        source.u64(crate::canonical_f64_bits(value));
    }
    source.usize(controls.grow_iterations);
    let mask = ReconstructionMask::mint(
        problem,
        model_generation,
        Some(normal.completion_id()),
        coordinate,
        shape,
        support,
        2,
        &source.finish(),
    )?;
    Ok((
        mask,
        AutoMultithreshEvidence {
            previous_mask_generation: previous.map(ReconstructionMask::generation_id),
            median: residual_median,
            robust_rms,
            positive_threshold,
            low_noise_threshold,
            negative_threshold,
            changed_pixels,
            channel_stopped,
        },
    ))
}

/// Mask construction or lineage failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MaskError {
    /// Source and target direction grids cannot be mapped by the accepted law.
    #[error("mask direction grids require a supported exact reprojection")]
    UnsupportedReprojection,
    /// The current PSF cannot define a finite positive beam area.
    #[error("normal-state PSF cannot define a finite positive beam area")]
    InvalidBeamArea,
    /// A pixel box is inverted.
    #[error("mask box requires blc <= trc")]
    InvertedBox,
    /// A static box lies outside its named grid.
    #[error("mask box lies outside the target grid")]
    OutsideTarget,
    /// Shape or lineage does not match the target grid.
    #[error("mask shape or lineage mismatch")]
    ShapeMismatch,
    /// Shape is empty or overflows addressable storage.
    #[error("mask shape must be finite and non-empty")]
    InvalidShape,
    /// Empty reconstruction masks fail visibly; no full-image fallback exists.
    #[error("reconstruction mask is empty")]
    EmptyMask,
    /// Auto-mask residual state contained a non-finite sample.
    #[error("auto-mask residual contains a non-finite sample")]
    NonfiniteResidual,
    /// Auto-mask controls or beam area are invalid.
    #[error("auto-multithreshold controls are invalid")]
    InvalidAutoControls,
}

fn validate_shape(shape: [usize; 2]) -> Result<(), MaskError> {
    if shape[0] == 0 || shape[1] == 0 || shape[0].checked_mul(shape[1]).is_none() {
        Err(MaskError::InvalidShape)
    } else {
        Ok(())
    }
}

fn validate_auto_controls(
    controls: AutoMultithreshControls,
    beam_area_pixels: f64,
) -> Result<(), MaskError> {
    let values = [
        controls.sidelobe_factor,
        controls.noise_factor,
        controls.low_noise_factor,
        controls.negative_factor,
        controls.minimum_beam_fraction,
        controls.smooth_factor,
        controls.cut_threshold,
        controls.minimum_percent_change,
        beam_area_pixels,
    ];
    if values.iter().enumerate().any(|(index, value)| {
        !value.is_finite() || (*value < 0.0 && index != 7) || (index == 7 && *value < -1.0)
    }) || controls.cut_threshold > 1.0
        || beam_area_pixels == 0.0
    {
        Err(MaskError::InvalidAutoControls)
    } else {
        Ok(())
    }
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) * 0.5
    } else {
        sorted[middle]
    }
}

fn prune_regions(mask: &mut [bool], shape: [usize; 2], minimum: usize) {
    if minimum <= 1 {
        return;
    }
    let mut seen = vec![false; mask.len()];
    for seed in 0..mask.len() {
        if !mask[seed] || seen[seed] {
            continue;
        }
        let mut queue = VecDeque::from([seed]);
        let mut component = Vec::new();
        seen[seed] = true;
        while let Some(index) = queue.pop_front() {
            component.push(index);
            let pixel = [index / shape[1], index % shape[1]];
            for offset in [[-1, 0], [1, 0], [0, -1], [0, 1]] {
                if let Some(neighbor) = offset_mask_pixel(pixel, offset, shape) {
                    let flat = neighbor[0] * shape[1] + neighbor[1];
                    if mask[flat] && !seen[flat] {
                        seen[flat] = true;
                        queue.push_back(flat);
                    }
                }
            }
        }
        if component.len() < minimum {
            for index in component {
                mask[index] = false;
            }
        }
    }
}

fn smooth_and_cut(
    mask: &[bool],
    shape: [usize; 2],
    smooth_factor: f64,
    beam: AutoMaskBeam,
    cut: f64,
) -> Vec<bool> {
    if smooth_factor == 0.0 || mask.iter().all(|value| !*value) {
        return mask.to_vec();
    }
    let sigma_major = smooth_factor * beam.major_fwhm_pixels / 2.354_820_045_030_949_3;
    let sigma_minor = smooth_factor * beam.minor_fwhm_pixels / 2.354_820_045_030_949_3;
    // Image2DConvolver::_shapeOfKernel uses a square +/-5-sigma Gaussian
    // kernel (`_sizeOfGaussian(width, 5.0)`) and then forces an odd shape.
    let radius = (5.0 * sigma_major + 0.5) as isize + 1;
    let cos_pa = beam.position_angle_rad.cos();
    let sin_pa = beam.position_angle_rad.sin();
    let mut smoothed = vec![0.0; mask.len()];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let mut value = 0.0;
            for dx in -radius..=radius {
                for dy in -radius..=radius {
                    if let Some(pixel) = offset_mask_pixel([x, y], [dx, dy], shape)
                        && mask[pixel[0] * shape[1] + pixel[1]]
                    {
                        let dx = dx as f64;
                        let dy = dy as f64;
                        let u = dx * cos_pa + dy * sin_pa;
                        let v = -dx * sin_pa + dy * cos_pa;
                        value +=
                            (-0.5 * ((u / sigma_minor).powi(2) + (v / sigma_major).powi(2))).exp();
                    }
                }
            }
            smoothed[x * shape[1] + y] = value;
        }
    }
    let peak = smoothed.iter().copied().fold(0.0_f64, f64::max);
    smoothed
        .into_iter()
        .map(|value| value >= cut * peak)
        .collect()
}

fn grow_cross(
    mask: &mut [bool],
    shape: [usize; 2],
    residual: &[f64],
    low_threshold: f64,
    iterations: usize,
) {
    for _ in 0..iterations {
        let previous = mask.to_owned();
        for index in 0..previous.len() {
            if previous[index] {
                continue;
            }
            let pixel = [index / shape[1], index % shape[1]];
            if residual[index] < low_threshold {
                continue;
            }
            mask[index] = [[-1, 0], [1, 0], [0, -1], [0, 1]]
                .into_iter()
                .any(|offset| {
                    offset_mask_pixel(pixel, offset, shape)
                        .is_some_and(|neighbor| previous[neighbor[0] * shape[1] + neighbor[1]])
                });
        }
    }
}

fn offset_mask_pixel(
    pixel: [usize; 2],
    offset: [isize; 2],
    shape: [usize; 2],
) -> Option<[usize; 2]> {
    let x = pixel[0].checked_add_signed(offset[0])?;
    let y = pixel[1].checked_add_signed(offset[1])?;
    (x < shape[0] && y < shape[1]).then_some([x, y])
}

#[allow(clippy::too_many_arguments)]
fn mask_identity(
    problem: CompiledProblemId,
    model_generation: ModelGenerationId,
    normal: Option<FinalNormalStateCompletionId>,
    coordinate: DirectionCoordinateSpec,
    shape: [usize; 2],
    support: &[bool],
    source_kind: u8,
    source_identity: &[u8],
) -> ReconstructionMaskGenerationId {
    let mut encoder = Encoder::new(MASK_DOMAIN, MASK_VERSION);
    encoder.identity(problem.as_bytes());
    encoder.identity(model_generation.as_bytes());
    match normal {
        None => encoder.u8(0),
        Some(normal) => {
            encoder.u8(1);
            encoder.identity(normal.as_bytes());
        }
    }
    encode_coordinate(&mut encoder, coordinate);
    encoder.usize(shape[0]);
    encoder.usize(shape[1]);
    encoder.u8(source_kind);
    encoder.bytes(source_identity);
    for value in support {
        encoder.u8(u8::from(*value));
    }
    ReconstructionMaskGenerationId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_coordinate(encoder: &mut Encoder, coordinate: DirectionCoordinateSpec) {
    encoder.u8(coordinate.projection() as u8);
    encoder.u8(coordinate.reference_direction().frame() as u8);
    encoder.u64(crate::canonical_f64_bits(
        coordinate.reference_direction().longitude_rad(),
    ));
    encoder.u64(crate::canonical_f64_bits(
        coordinate.reference_direction().latitude_rad(),
    ));
    for value in coordinate.reference_pixel() {
        encoder.u64(crate::canonical_f64_bits(value));
    }
    for value in coordinate.increment_rad() {
        encoder.u64(crate::canonical_f64_bits(value));
    }
    for value in coordinate.pc().into_iter().flatten() {
        encoder.u64(crate::canonical_f64_bits(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};

    fn coordinate(reference_pixel: [f64; 2]) -> DirectionCoordinateSpec {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            reference_pixel,
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        )
    }

    #[test]
    fn aligned_mask_reprojection_preserves_exact_support() {
        let support = [false, true, false, true, true, false, false, false, true];
        let projected = reproject_mask_support(
            coordinate([1.0, 1.0]),
            [3, 3],
            &support,
            coordinate([1.0, 1.0]),
            [3, 3],
        )
        .expect("aligned reprojection");
        assert_eq!(projected.as_ref(), support);
    }

    #[test]
    fn reprojection_uses_the_target_and_source_reference_pixels() {
        let mut support = [false; 9];
        support[4] = true;
        let projected = reproject_mask_support(
            coordinate([1.0, 1.0]),
            [3, 3],
            &support,
            coordinate([2.0, 1.0]),
            [4, 3],
        )
        .expect("shifted reference-pixel reprojection");
        assert!(projected[2 * 3 + 1]);
        assert_eq!(projected.iter().filter(|value| **value).count(), 1);
    }
}
