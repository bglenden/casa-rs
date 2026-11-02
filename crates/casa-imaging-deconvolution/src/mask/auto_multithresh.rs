// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's automatic multi-threshold mask
//! (`SDMaskHandler::autoMask` and `autoMaskByMultiThreshold`, CASA 6.7).
//!
//! At each major-cycle boundary the plane's thresholds come from its
//! residual: the larger of the PSF sidelobe level times `sidelobethreshold`
//! times the residual's absolute peak and the noise times
//! `noisethreshold`, both above the residual's median. The mask the plane
//! inherits then gains
//!
//! * the regions above that threshold, less those smaller than
//!   `minbeamfrac` beams, smoothed by `smoothfactor` beams and cut at
//!   `cutthreshold` of the smoothed peak;
//! * after the first cycle, the inherited mask grown by binary dilation
//!   through the pixels above the low-noise threshold, pruned when
//!   `dogrowprune`, then smoothed and cut the same way;
//! * with `negativethreshold`, the regions below the negative threshold,
//!   smoothed and cut but never pruned.
//!
//! A plane whose threshold is the noise's stops evolving
//! (`SDMaskHandler::skipChannels`) once its mask is empty, or once the
//! cycle threshold has reached the global threshold and the mask changed
//! by less than `minpercentchange` percent.

use std::collections::VecDeque;

use casa_imaging_model::DirectionCoordinateSpec;

use super::{MaskError, ReconstructionMask, offset_mask_pixel};
use crate::noise::{NoiseEstimate, plane_noise};
use crate::{FinalNormalState, NormalBasis, RobustNoise};

/// The primary PSF's fitted main lobe (pixels) and sidelobe level, measured
/// once per PSF by the minor cycle, as the automatic mask needs them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMaskBeam {
    major_fwhm_pixels: f64,
    minor_fwhm_pixels: f64,
    position_angle_rad: f64,
    area_pixels: f64,
    sidelobe_fraction: f64,
}

impl AutoMaskBeam {
    /// A beam of the given full widths at half maximum (pixels), position
    /// angle and fitted sidelobe level.
    #[must_use]
    pub const fn new(
        major_fwhm_pixels: f64,
        minor_fwhm_pixels: f64,
        position_angle_rad: f64,
        sidelobe_fraction: f64,
    ) -> Self {
        Self {
            major_fwhm_pixels,
            minor_fwhm_pixels,
            position_angle_rad,
            area_pixels: std::f64::consts::PI * major_fwhm_pixels * minor_fwhm_pixels
                / (4.0 * std::f64::consts::LN_2),
            sidelobe_fraction,
        }
    }
}

/// CASA auto-multithreshold controls: tclean's parameters of the same
/// names.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultithreshControls {
    /// Sidelobe threshold multiplier (`sidelobethreshold`).
    pub sidelobe_factor: f64,
    /// Robust-noise threshold multiplier (`noisethreshold`).
    pub noise_factor: f64,
    /// Low-noise growth multiplier (`lownoisethreshold`).
    pub low_noise_factor: f64,
    /// Negative-feature threshold multiplier; zero disables negative masks
    /// (`negativethreshold`).
    pub negative_factor: f64,
    /// Minimum connected-region area as a fraction of the beam area; zero
    /// or less prunes nothing (`minbeamfrac`).
    pub minimum_beam_fraction: f64,
    /// Gaussian smoothing FWHM as a fraction of the restoring beam
    /// (`smoothfactor`).
    pub smooth_factor: f64,
    /// Fraction of the smoothed-mask peak retained (`cutthreshold`).
    pub cut_threshold: f64,
    /// Bound on the binary dilations that grow the mask
    /// (`growiterations`).
    pub grow_iterations: usize,
    /// Whether the grown mask is pruned as the threshold mask is
    /// (`dogrowprune`).
    pub grow_prune: bool,
    /// Percent change below which a plane's mask stops evolving once the
    /// global threshold is reached; negative never stops on change
    /// (`minpercentchange`).
    pub minimum_percent_change: f64,
    /// How the plane's noise is estimated (`fastnoise`).
    pub noise: NoiseEstimate,
    /// Whether each update logs CASA's automask summary (`verbose`).
    pub verbose: bool,
}

/// Which threshold a plane's mask used (CASA's `thresh_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoMaskThreshold {
    /// The PSF sidelobe level times the residual's absolute peak.
    Sidelobe,
    /// The noise times `noisethreshold`.
    Noise,
}

impl AutoMaskThreshold {
    /// CASA's name of the threshold.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sidelobe => "sidelobe",
            Self::Noise => "noise",
        }
    }
}

/// Inspectable evidence for one auto-multithreshold generation: the
/// quantities of a row of CASA's automask summary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultithreshEvidence {
    /// Robust residual median; zero for a stopped plane under the robust
    /// noise estimate, as CASA records it.
    pub median: f64,
    /// MAD-derived robust RMS, zero likewise.
    pub robust_rms: f64,
    /// The residual's largest magnitude, signed.
    pub peak: f64,
    /// Positive detection threshold.
    pub positive_threshold: f64,
    /// Low-noise growth threshold.
    pub low_noise_threshold: f64,
    /// Optional negative detection threshold.
    pub negative_threshold: Option<f64>,
    /// Which threshold the positive one is.
    pub threshold: AutoMaskThreshold,
    /// Whether the plane was masked at this boundary; a stopped plane
    /// keeps its mask.
    pub masked: bool,
    /// Regions above the threshold before pruning.
    pub regions: usize,
    /// Of those, the regions pruned.
    pub pruned_regions: usize,
    /// Regions of the grown mask before its pruning.
    pub grow_regions: usize,
    /// Of those, the regions pruned.
    pub grow_pruned_regions: usize,
    /// Pixels of the negative mask.
    pub negative_pixels: usize,
    /// Number of support pixels that changed.
    pub changed_pixels: usize,
    /// Whether later cycles should stop evolving this plane.
    pub channel_stopped: bool,
}

/// Where a plane's automatic mask stands at one major-cycle boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoMaskCycle {
    /// Major cycles completed before this boundary; the mask grows only
    /// after the first.
    pub completed_major_cycles: usize,
    /// Whether the previous minor cycle's threshold was the global one.
    pub threshold_reached: bool,
    /// Whether an earlier boundary stopped the plane's evolution.
    pub stopped: bool,
}

/// Heap bytes per pixel [`auto_multithresh`] holds at most: the normalised
/// residual and the PSF it is divided by, a copy for its statistics and a
/// smoothed plane (`f64` each); region pruning's component list and
/// queue, grown by pushes (`usize` each, twice); and its twelve `bool`
/// planes.
pub const AUTO_MULTITHRESH_BYTES_PER_CELL: u64 =
    4 * size_of::<f64>() as u64 + 4 * size_of::<usize>() as u64 + 12;

/// The thresholds of one plane (`autoMaskByMultiThreshold`).
#[derive(Debug, Clone, Copy)]
struct Thresholds {
    positive: f64,
    low: f64,
    negative: f64,
    kind: AutoMaskThreshold,
}

impl Thresholds {
    /// Every threshold is relative to the median; the positive one is the
    /// larger of the sidelobe and noise thresholds, and is the sidelobe's
    /// when they tie.
    fn new(
        noise: RobustNoise,
        peak: f64,
        sidelobe_level: f64,
        controls: &AutoMultithreshControls,
    ) -> Self {
        let sidelobe = sidelobe_level * controls.sidelobe_factor * peak.abs();
        let noise_threshold = controls.noise_factor * noise.rms;
        let low_noise = controls.low_noise_factor * noise.rms;
        let negative = controls.negative_factor * noise.rms;
        let positive = (sidelobe + noise.median).max(noise_threshold + noise.median);
        Self {
            positive,
            low: (sidelobe + noise.median).max(low_noise + noise.median),
            negative: noise.median - sidelobe.max(negative),
            kind: if positive == sidelobe + noise.median {
                AutoMaskThreshold::Sidelobe
            } else {
                AutoMaskThreshold::Noise
            },
        }
    }
}

/// Generate the next auto-multithreshold mask of the primary plane of
/// `normal` from the mask it inherits, `previous`; `valid_support` is the
/// plane's valid pixels (the residual's pixel mask) in canonical x-major
/// order, and `beam` the PSF's fitted main lobe and sidelobe level.
///
/// # Errors
///
/// The state is not one channel of one plane, shapes disagree, the
/// residual is not finite, or the controls or beam are invalid.
pub fn auto_multithresh(
    coordinate: DirectionCoordinateSpec,
    normal: &FinalNormalState,
    previous: Option<&ReconstructionMask>,
    valid_support: &[bool],
    cycle: AutoMaskCycle,
    controls: AutoMultithreshControls,
    beam: AutoMaskBeam,
) -> Result<(ReconstructionMask, AutoMultithreshEvidence), MaskError> {
    validate_auto_controls(controls, beam.area_pixels)?;
    let residual = primary_residual(normal)?;
    let shape = normal.shape();
    if valid_support.len() != shape[0] * shape[1]
        || previous.is_some_and(|mask| mask.shape != shape || mask.coordinate != coordinate)
    {
        return Err(MaskError::ShapeMismatch);
    }
    let inherited =
        previous.map_or_else(|| vec![false; residual.len()], |mask| mask.support.to_vec());
    let (noise, peak) = statistics(&residual, valid_support, &inherited, cycle, controls);
    let thresholds = Thresholds::new(noise, peak, beam.sidelobe_fraction, &controls);
    let mut evidence = AutoMultithreshEvidence {
        median: noise.median,
        robust_rms: noise.rms,
        peak,
        positive_threshold: thresholds.positive,
        low_noise_threshold: thresholds.low,
        negative_threshold: (controls.negative_factor > 0.0).then_some(thresholds.negative),
        threshold: thresholds.kind,
        masked: !cycle.stopped,
        regions: 0,
        pruned_regions: 0,
        grow_regions: 0,
        grow_pruned_regions: 0,
        negative_pixels: 0,
        changed_pixels: 0,
        channel_stopped: cycle.stopped,
    };
    if cycle.completed_major_cycles == 0 && controls.cut_threshold >= 0.2 {
        tracing::warn!(
            "Faint regions may not be included in the final mask. Consider decreasing \
             cutthreshold."
        );
    }
    let added = if cycle.stopped {
        vec![false; residual.len()]
    } else {
        let plane = Plane {
            residual: &residual,
            valid: valid_support,
            shape,
            beam,
            controls: &controls,
        };
        plane.additions(&inherited, &thresholds, cycle, &mut evidence)
    };
    let support = inherited
        .iter()
        .zip(&added)
        .zip(valid_support)
        .map(|((kept, added), valid)| *valid && (*kept || *added))
        .collect::<Vec<_>>();
    evidence.changed_pixels = support
        .iter()
        .zip(&inherited)
        .filter(|(now, before)| now != before)
        .count();
    evidence.channel_stopped = skip_channel(&inherited, &support, &thresholds, cycle, &controls);
    if controls.verbose {
        log_summary(normal.slab().core_range().start, &evidence);
    }
    Ok((
        ReconstructionMask::new(coordinate, shape, support),
        evidence,
    ))
}

/// The primary plane's residual, divided by its PSF peak as the minor
/// cycle divides it: the first polarization of a one-channel state, or
/// the zeroth Taylor term.
fn primary_residual(normal: &FinalNormalState) -> Result<Vec<f64>, MaskError> {
    if normal.channel_count() != 1 {
        return Err(MaskError::ShapeMismatch);
    }
    let window = normal.read_window(normal.slab().core_range())?;
    let cells = normal.shape()[0] * normal.shape()[1];
    let (residual, psf) = if window.basis() == NormalBasis::Taylor {
        (
            window
                .coefficient_term(0)
                .ok_or(MaskError::ShapeMismatch)?
                .residual()
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>(),
            window
                .normal_moment(0)
                .ok_or(MaskError::ShapeMismatch)?
                .normal_approximation()
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>(),
        )
    } else {
        let plane = |values: crate::NormalValues<'_>| {
            values
                .slice(0..cells)
                .map(|values| values.iter().map(|value| value.re).collect::<Vec<_>>())
                .ok_or(MaskError::ShapeMismatch)
        };
        (
            plane(window.residual())?,
            plane(window.normal_approximation())?,
        )
    };
    // `FinalNormalState` retains the unnormalised normal-equation planes.
    // CASA derives automask thresholds from the displayed residual in model
    // units, so remove the accumulated normalisation before applying
    // Jy-valued controls.
    let psf_peak = psf.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MaskError::InvalidBeamArea);
    }
    let residual = residual
        .into_iter()
        .map(|value| value / psf_peak)
        .collect::<Vec<_>>();
    if residual.len() != cells || residual.iter().any(|value| !value.is_finite()) {
        return Err(MaskError::NonfiniteResidual);
    }
    Ok(residual)
}

/// The plane's noise and signed absolute peak over its valid pixels
/// (`SDMaskHandler::autoMask`). The fast estimate is the classical
/// statistics of every valid pixel; the robust one excludes the inherited
/// mask, and is zero for a stopped plane, as
/// `calcRobustImageStatistics` records a flagged channel.
fn statistics(
    residual: &[f64],
    valid: &[bool],
    inherited: &[bool],
    cycle: AutoMaskCycle,
    controls: AutoMultithreshControls,
) -> (RobustNoise, f64) {
    let peak = residual
        .iter()
        .zip(valid)
        .filter(|(_, valid)| **valid)
        .map(|(value, _)| *value)
        .fold(0.0_f64, |peak, value| {
            if value.abs() > peak.abs() {
                value
            } else {
                peak
            }
        });
    let zero = RobustNoise {
        median: 0.0,
        rms: 0.0,
    };
    let noise = if controls.noise == NoiseEstimate::Robust && cycle.stopped {
        zero
    } else {
        let full = inherited.iter().all(|set| *set);
        plane_noise(residual, valid, controls.noise, Some(inherited), full).unwrap_or(zero)
    };
    (noise, peak)
}

/// One plane's residual and what its mask is made with.
struct Plane<'a> {
    residual: &'a [f64],
    valid: &'a [bool],
    shape: [usize; 2],
    beam: AutoMaskBeam,
    controls: &'a AutoMultithreshControls,
}

impl Plane<'_> {
    /// What one boundary adds to the inherited mask: the threshold mask,
    /// the grown mask and the negative mask, each smoothed and cut.
    fn additions(
        &self,
        inherited: &[bool],
        thresholds: &Thresholds,
        cycle: AutoMaskCycle,
        evidence: &mut AutoMultithreshEvidence,
    ) -> Vec<bool> {
        let controls = self.controls;
        let prunes = controls.minimum_beam_fraction > 0.0;
        let prune_size = controls.minimum_beam_fraction * self.beam.area_pixels;
        let mut threshold = threshold_mask(self.residual, thresholds.positive);
        if prunes {
            // Only the pruning branch drops the residual's masked pixels.
            for (set, valid) in threshold.iter_mut().zip(self.valid) {
                *set &= *valid;
            }
            (evidence.regions, evidence.pruned_regions) =
                prune_regions(&mut threshold, self.shape, prune_size);
        }
        let mut added = self.smooth(&threshold);
        if cycle.completed_major_cycles > 0 && controls.grow_iterations > 0 {
            let constraint = threshold_mask(self.residual, thresholds.low);
            let mut grown = dilate(inherited, &constraint, self.shape, controls.grow_iterations);
            for (set, allowed) in grown.iter_mut().zip(&constraint) {
                *set &= *allowed;
            }
            if prunes && controls.grow_prune {
                (evidence.grow_regions, evidence.grow_pruned_regions) =
                    prune_regions(&mut grown, self.shape, prune_size);
            }
            for (set, grown) in added.iter_mut().zip(self.smooth(&grown)) {
                *set |= grown;
            }
        }
        if cycle.completed_major_cycles == 0 && !added.iter().any(|set| *set) {
            tracing::warn!(
                "No regions found by automasking: used the {} threshold to mask, but no \
                 emission was found.",
                thresholds.kind.name()
            );
        }
        if controls.negative_factor > 0.0 {
            let negative = self.smooth(&threshold_mask(self.residual, thresholds.negative));
            evidence.negative_pixels = negative.iter().filter(|set| **set).count();
            if evidence.negative_pixels == 0 {
                tracing::info!("No negative region was found by automask.");
            }
            for (set, negative) in added.iter_mut().zip(negative) {
                *set |= negative;
            }
        }
        added
    }

    /// `convolveMask` by `smoothfactor` beams, then CASA's cut of the
    /// smoothed mask at `cutthreshold` of its peak.
    fn smooth(&self, mask: &[bool]) -> Vec<bool> {
        smooth_and_cut(
            mask,
            self.shape,
            self.controls.smooth_factor,
            self.beam,
            self.controls.cut_threshold,
        )
    }
}

/// `SDMaskHandler::makeMaskByPerChanThreshold`: the pixels above a
/// non-negative threshold, or below a negative one.
fn threshold_mask(residual: &[f64], threshold: f64) -> Vec<bool> {
    if threshold < 0.0 {
        residual.iter().map(|value| *value < threshold).collect()
    } else {
        residual.iter().map(|value| *value > threshold).collect()
    }
}

/// `SDMaskHandler::skipChannels`: a noise-thresholded plane stops evolving
/// once its mask is empty, or once the global threshold is reached and its
/// mask changed by nothing or by less than `minpercentchange` percent.
fn skip_channel(
    inherited: &[bool],
    current: &[bool],
    thresholds: &Thresholds,
    cycle: AutoMaskCycle,
    controls: &AutoMultithreshControls,
) -> bool {
    if cycle.stopped || thresholds.kind != AutoMaskThreshold::Noise {
        return cycle.stopped;
    }
    let count = |mask: &[bool]| mask.iter().filter(|set| **set).count() as f64;
    let (before, now) = (count(inherited), count(current));
    let fraction = controls.minimum_percent_change / 100.0;
    let change = (now - before).abs();
    now == 0.0
        || (fraction >= 0.0
            && cycle.threshold_reached
            && (change == 0.0 || change < fraction * before))
}

/// CASA's `printAutomaskSummary` row of one plane, through `tracing`.
fn log_summary(channel: usize, evidence: &AutoMultithreshEvidence) {
    let masked = !evidence.channel_stopped;
    let (median, rms, threshold) = if masked {
        (
            evidence.median.to_string(),
            evidence.robust_rms.to_string(),
            evidence.positive_threshold.to_string(),
        )
    } else {
        ("--".to_string(), "--".to_string(), "--".to_string())
    };
    tracing::info!("========== automask summary ==========");
    tracing::info!(
        "chan masking? median   RMS         peak   thresh_type   thresh_value   N_reg N_pruned \
         N_grow N_grow_pruned N_neg_pix"
    );
    tracing::info!(
        "[C{channel}] {}        {median}  {rms}  {}  {}  {threshold}        {}  {}  {}  {}  {}",
        if masked { "T" } else { "F" },
        evidence.peak,
        evidence.threshold.name(),
        evidence.regions,
        evidence.pruned_regions,
        evidence.grow_regions,
        evidence.grow_pruned_regions,
        evidence.negative_pixels,
    );
    tracing::info!("========== END of automask summary ==========");
}

fn validate_auto_controls(
    controls: AutoMultithreshControls,
    beam_area_pixels: f64,
) -> Result<(), MaskError> {
    let nonnegative = [
        controls.sidelobe_factor,
        controls.noise_factor,
        controls.low_noise_factor,
        controls.negative_factor,
        beam_area_pixels,
    ];
    if nonnegative
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
        || [
            controls.minimum_beam_fraction,
            controls.cut_threshold,
            controls.minimum_percent_change,
        ]
        .iter()
        .any(|value| !value.is_finite())
        || !(controls.smooth_factor.is_finite() && controls.smooth_factor > 0.0)
        || beam_area_pixels == 0.0
    {
        Err(MaskError::InvalidAutoControls)
    } else {
        Ok(())
    }
}

/// `SDMaskHandler::YAPruneRegions`: remove the four-connected regions
/// smaller than `size` pixels; the region and pruned-region counts.
fn prune_regions(mask: &mut [bool], shape: [usize; 2], size: f64) -> (usize, usize) {
    if size == 0.0 {
        return (0, 0);
    }
    let mut seen = vec![false; mask.len()];
    let (mut regions, mut pruned) = (0, 0);
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
        regions += 1;
        if (component.len() as f64) < size {
            pruned += 1;
            for index in component {
                mask[index] = false;
            }
        }
    }
    (regions, pruned)
}

/// `SDMaskHandler::binaryDilation` with CASA's cross structuring element:
/// up to `iterations` dilations of `mask`, each seeded only by the mask's
/// pixels inside `constraint`, stopping early once one changes nothing. A
/// constraint with no pixel leaves the mask as it is
/// (`binaryDilationCore`).
fn dilate(mask: &[bool], constraint: &[bool], shape: [usize; 2], iterations: usize) -> Vec<bool> {
    let mut current = mask.to_vec();
    if !mask.iter().any(|set| *set) || !constraint.iter().any(|set| *set) {
        return current;
    }
    for _ in 0..iterations {
        let mut next = current.clone();
        for index in 0..current.len() {
            if !(current[index] && constraint[index]) {
                continue;
            }
            let pixel = [index / shape[1], index % shape[1]];
            for offset in [[-1, 0], [1, 0], [0, -1], [0, 1]] {
                if let Some(neighbor) = offset_mask_pixel(pixel, offset, shape) {
                    next[neighbor[0] * shape[1] + neighbor[1]] = true;
                }
            }
        }
        let changed = next != current;
        current = next;
        if !changed {
            break;
        }
    }
    current
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
    // CASA cuts with `makeMaskByPerChanThreshold`, so a negative
    // `cutthreshold` keeps the pixels below it: none.
    let peak = smoothed.iter().copied().fold(0.0_f64, f64::max);
    threshold_mask(&smoothed, cut * peak)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn beam() -> AutoMaskBeam {
        AutoMaskBeam {
            major_fwhm_pixels: 1.0,
            minor_fwhm_pixels: 1.0,
            position_angle_rad: 0.0,
            area_pixels: 1.0,
            sidelobe_fraction: 0.0,
        }
    }

    #[test]
    fn automask_cut_threshold_is_strict_like_casa() {
        assert_eq!(smooth_and_cut(&[true], [1, 1], 1.0, beam(), 1.0), [false]);
        // CASA cuts below a negative level, which no smoothed pixel is
        // (tclean 6.7.6 with cutthreshold=-0.1 stops with an empty mask).
        assert_eq!(
            smooth_and_cut(&[true, false], [1, 2], 1.0, beam(), -0.1),
            [false, false]
        );
    }

    #[test]
    fn the_peak_is_the_signed_extreme_of_the_valid_pixels() {
        let controls = AutoMultithreshControls {
            sidelobe_factor: 3.0,
            noise_factor: 5.0,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction: 0.3,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 75,
            grow_prune: true,
            minimum_percent_change: -1.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let cycle = AutoMaskCycle {
            completed_major_cycles: 0,
            threshold_reached: false,
            stopped: false,
        };
        let residual = [-41.1, -1.3, 128.9, -500.0];
        let valid = [true, true, true, false];
        let (_, peak) = statistics(&residual, &valid, &[false; 4], cycle, controls);
        // Neither recentred by the median nor read from an invalid pixel.
        assert_eq!(peak, 128.9);
        let mut robust = controls;
        robust.noise = NoiseEstimate::Robust;
        let stopped = AutoMaskCycle {
            stopped: true,
            ..cycle
        };
        let (noise, _) = statistics(&residual, &valid, &[false; 4], stopped, robust);
        assert_eq!((noise.median, noise.rms), (0.0, 0.0));
        let (noise, _) = statistics(&residual, &valid, &[false; 4], stopped, controls);
        assert_eq!(noise.median, -1.3);
    }

    #[test]
    fn a_negative_threshold_selects_the_pixels_below_it() {
        let residual = [-2.0, -0.5, 0.5, 2.0];
        assert_eq!(threshold_mask(&residual, 1.0), [false, false, false, true]);
        assert_eq!(threshold_mask(&residual, -1.0), [true, false, false, false]);
    }

    #[test]
    fn pruning_counts_regions_and_removes_those_under_the_size() {
        // A 2-pixel and a 1-pixel region on a 1 by 5 plane.
        let mut mask = [true, true, false, true, false];
        assert_eq!(prune_regions(&mut mask, [1, 5], 1.5), (2, 1));
        assert_eq!(mask, [true, true, false, false, false]);
        let mut kept = [true, false, true];
        assert_eq!(prune_regions(&mut kept, [1, 3], 0.0), (0, 0));
        assert_eq!(kept, [true, false, true]);
    }

    #[test]
    fn dilation_grows_only_from_seeds_inside_the_constraint() {
        // 1 by 7: the seed at 1 is constrained, the seed at 5 is not.
        let mask = [false, true, false, false, false, true, false];
        let constraint = [true, true, true, true, false, false, false];
        let grown = dilate(&mask, &constraint, [1, 7], 2);
        assert_eq!(grown, [true, true, true, true, false, true, false]);
        // One iteration grows one pixel.
        let once = dilate(&mask, &constraint, [1, 7], 1);
        assert_eq!(once, [true, true, true, false, false, true, false]);
        // An empty constraint leaves the mask unchanged.
        assert_eq!(dilate(&mask, &[false; 7], [1, 7], 3), mask);
    }

    #[test]
    fn thresholds_tie_to_the_sidelobe_and_offset_by_the_median() {
        let controls = AutoMultithreshControls {
            sidelobe_factor: 2.0,
            noise_factor: 4.0,
            low_noise_factor: 1.0,
            negative_factor: 3.0,
            minimum_beam_fraction: 0.3,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 75,
            grow_prune: true,
            minimum_percent_change: -1.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let noise = RobustNoise {
            median: 0.5,
            rms: 1.0,
        };
        // Sidelobe 0.1 · 2 · 20 = 4 ties the noise threshold 4 · 1.
        let tie = Thresholds::new(noise, -20.0, 0.1, &controls);
        assert_eq!(tie.kind, AutoMaskThreshold::Sidelobe);
        assert_eq!(tie.positive, 4.5);
        assert_eq!(tie.low, 4.5);
        assert_eq!(tie.negative, 0.5 - 4.0);
        let noisy = Thresholds::new(noise, 10.0, 0.1, &controls);
        assert_eq!(noisy.kind, AutoMaskThreshold::Noise);
        assert_eq!(noisy.positive, 4.5);
        assert_eq!(noisy.low, 2.5);
        assert_eq!(noisy.negative, 0.5 - 3.0);
        assert_eq!(crate::noise::CASA_MAD_TO_RMS, 1.4826);
    }

    #[test]
    fn skip_channels_stops_only_noise_thresholded_planes() {
        let controls = AutoMultithreshControls {
            sidelobe_factor: 3.0,
            noise_factor: 5.0,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction: 0.3,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 75,
            grow_prune: true,
            minimum_percent_change: 20.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let thresholds = |kind| Thresholds {
            positive: 1.0,
            low: 0.5,
            negative: -1.0,
            kind,
        };
        let cycle = |threshold_reached| AutoMaskCycle {
            completed_major_cycles: 2,
            threshold_reached,
            stopped: false,
        };
        let empty = [false; 10];
        let mut five = [false; 10];
        five[..5].fill(true);
        let mut six = five;
        six[5] = true;
        let noise = thresholds(AutoMaskThreshold::Noise);
        let sidelobe = thresholds(AutoMaskThreshold::Sidelobe);
        // An empty mask stops a noise-thresholded plane only.
        assert!(skip_channel(
            &empty,
            &empty,
            &noise,
            cycle(false),
            &controls
        ));
        assert!(!skip_channel(
            &empty,
            &empty,
            &sidelobe,
            cycle(false),
            &controls
        ));
        // A one-pixel change of five is 20%, not less than 20%: only an
        // unchanged mask stops it, and only once the threshold is reached.
        assert!(!skip_channel(&five, &six, &noise, cycle(true), &controls));
        assert!(skip_channel(&five, &five, &noise, cycle(true), &controls));
        assert!(!skip_channel(&five, &five, &noise, cycle(false), &controls));
        let mut never = controls;
        never.minimum_percent_change = -1.0;
        assert!(!skip_channel(&five, &five, &noise, cycle(true), &never));
    }
}
