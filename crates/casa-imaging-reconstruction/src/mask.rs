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

    /// Reproject a same-frame direction mask with nearest-neighbour mask
    /// semantics. Geometry is explicit on both sides; a frame change fails
    /// closed instead of silently treating pixels as aligned.
    #[allow(clippy::too_many_arguments)]
    pub fn reproject_nearest(
        problem: CompiledProblemId,
        model_generation: ModelGenerationId,
        source_coordinate: DirectionCoordinateSpec,
        source_shape: [usize; 2],
        source_support: &[bool],
        target_coordinate: DirectionCoordinateSpec,
        target_shape: [usize; 2],
    ) -> Result<Self, MaskError> {
        validate_shape(source_shape)?;
        validate_shape(target_shape)?;
        if source_support.len() != source_shape[0] * source_shape[1] {
            return Err(MaskError::ShapeMismatch);
        }
        if source_coordinate.reference_direction().frame()
            != target_coordinate.reference_direction().frame()
        {
            return Err(MaskError::FrameConversionUnavailable);
        }
        let mut support = vec![false; target_shape[0] * target_shape[1]];
        for x in 0..target_shape[0] {
            for y in 0..target_shape[1] {
                let world = linear_world(target_coordinate, [x as f64, y as f64]);
                let source = linear_pixel(source_coordinate, world)?;
                let sx = source[0].round();
                let sy = source[1].round();
                if sx >= 0.0
                    && sy >= 0.0
                    && sx < source_shape[0] as f64
                    && sy < source_shape[1] as f64
                    && source_support[sx as usize * source_shape[1] + sy as usize]
                {
                    support[x * target_shape[1] + y] = true;
                }
            }
        }
        let mut source_identity = Encoder::new(b"casa-rs-mask-source", 1);
        source_identity.usize(source_shape[0]);
        source_identity.usize(source_shape[1]);
        encode_coordinate(&mut source_identity, source_coordinate);
        for value in source_support {
            source_identity.u8(u8::from(*value));
        }
        Self::mint(
            problem,
            model_generation,
            None,
            target_coordinate,
            target_shape,
            support,
            1,
            &source_identity.finish(),
        )
    }

    /// Return the stable generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> ReconstructionMaskGenerationId {
        self.generation
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
        if !support.iter().any(|value| *value) {
            return Err(MaskError::EmptyMask);
        }
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
    beam_area_pixels: f64,
    controls: AutoMultithreshControls,
) -> Result<(ReconstructionMask, AutoMultithreshEvidence), MaskError> {
    validate_auto_controls(controls, beam_area_pixels)?;
    let shape = normal.shape();
    if valid_support.len() != shape[0] * shape[1]
        || previous.is_some_and(|mask| {
            mask.problem != problem
                || mask.model_generation != model_generation
                || mask.shape != shape
                || mask.coordinate != coordinate
        })
    {
        return Err(MaskError::ShapeMismatch);
    }
    let residual = normal
        .residual()
        .iter()
        .map(|value| value.re)
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
    let psf_peak = normal
        .normal_approximation()
        .iter()
        .map(|value| value.re.abs())
        .fold(0.0_f64, f64::max);
    let sidelobe = normal
        .normal_approximation()
        .iter()
        .map(|value| value.re.abs())
        .filter(|value| value.to_bits() != psf_peak.to_bits())
        .fold(0.0_f64, f64::max);
    let sidelobe_level = if psf_peak > 0.0 {
        sidelobe / psf_peak
    } else {
        0.0
    };
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
    prune_regions(
        &mut detected,
        shape,
        (controls.minimum_beam_fraction * beam_area_pixels).ceil() as usize,
    );
    detected = smooth_and_cut(
        &detected,
        shape,
        controls.smooth_factor,
        beam_area_pixels,
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
    let channel_stopped = support.iter().all(|value| !*value)
        || (cycle_threshold_reached && percent_change <= controls.minimum_percent_change);
    let mut source = Encoder::new(b"casa-rs-auto-multithresh-evidence", 1);
    source.identity(normal.completion_id().as_bytes());
    source.u64(crate::canonical_f64_bits(positive_threshold));
    source.u64(crate::canonical_f64_bits(low_noise_threshold));
    source.usize(completed_major_cycles);
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
    /// No celestial-frame conversion was authorized.
    #[error("mask reprojection requires an explicit celestial-frame conversion")]
    FrameConversionUnavailable,
    /// Direction WCS is singular.
    #[error("mask direction coordinate is singular")]
    SingularCoordinate,
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
    if values
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
        || controls.cut_threshold > 1.0
        || beam_area_pixels == 0.0
    {
        Err(MaskError::InvalidAutoControls)
    } else {
        Ok(())
    }
}

fn linear_world(coordinate: DirectionCoordinateSpec, pixel: [f64; 2]) -> [f64; 2] {
    let delta = [
        (pixel[0] - coordinate.reference_pixel()[0]) * coordinate.increment_rad()[0],
        (pixel[1] - coordinate.reference_pixel()[1]) * coordinate.increment_rad()[1],
    ];
    let pc = coordinate.pc();
    let reference = coordinate.reference_direction();
    [
        reference.longitude_rad() + pc[0][0] * delta[0] + pc[0][1] * delta[1],
        reference.latitude_rad() + pc[1][0] * delta[0] + pc[1][1] * delta[1],
    ]
}

fn linear_pixel(
    coordinate: DirectionCoordinateSpec,
    world: [f64; 2],
) -> Result<[f64; 2], MaskError> {
    let reference = coordinate.reference_direction();
    let delta = [
        world[0] - reference.longitude_rad(),
        world[1] - reference.latitude_rad(),
    ];
    let pc = coordinate.pc();
    let determinant = pc[0][0] * pc[1][1] - pc[0][1] * pc[1][0];
    if determinant == 0.0 || !determinant.is_finite() {
        return Err(MaskError::SingularCoordinate);
    }
    let plane = [
        (pc[1][1] * delta[0] - pc[0][1] * delta[1]) / determinant,
        (-pc[1][0] * delta[0] + pc[0][0] * delta[1]) / determinant,
    ];
    Ok([
        coordinate.reference_pixel()[0] + plane[0] / coordinate.increment_rad()[0],
        coordinate.reference_pixel()[1] + plane[1] / coordinate.increment_rad()[1],
    ])
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
    beam_area_pixels: f64,
    cut: f64,
) -> Vec<bool> {
    if smooth_factor == 0.0 || mask.iter().all(|value| !*value) {
        return mask.to_vec();
    }
    let beam_fwhm = (4.0 * std::f64::consts::LN_2 * beam_area_pixels / std::f64::consts::PI).sqrt();
    let sigma = smooth_factor * beam_fwhm / (2.0 * (2.0 * std::f64::consts::LN_2).sqrt());
    let radius = (3.0 * sigma).ceil() as isize;
    let mut smoothed = vec![0.0; mask.len()];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let mut value = 0.0;
            for dx in -radius..=radius {
                for dy in -radius..=radius {
                    if let Some(pixel) = offset_mask_pixel([x, y], [dx, dy], shape)
                        && mask[pixel[0] * shape[1] + pixel[1]]
                    {
                        value += (-((dx * dx + dy * dy) as f64) / (2.0 * sigma * sigma)).exp();
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
