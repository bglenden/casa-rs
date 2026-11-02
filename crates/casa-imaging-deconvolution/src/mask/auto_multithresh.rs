// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's automatic multi-threshold mask of one plane
//! (`SDMaskHandler::autoMask` and the per-plane loop of
//! `autoMaskByMultiThreshold`, CASA 6.7).
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
//! A channel whose first polarization's threshold is the noise's stops
//! evolving (`SDMaskHandler::skipChannels`) once its mask is empty, or once
//! the cycle threshold has reached the global threshold and the mask
//! changed by less than `minpercentchange` percent ([`skip_channel`]).
//! Which planes are masked together, and with which beam, is
//! [`super::auto_planes`]'s.

use std::collections::VecDeque;

use super::auto_planes::AutoMaskPlane;
use super::{MaskError, offset_mask_pixel};
use crate::RobustNoise;
use crate::noise::{NoiseEstimate, plane_noise};

/// The beam a plane's mask is smoothed and pruned by, in pixels, and the
/// image's PSF sidelobe level.
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
    /// Percent change below which a channel's mask stops evolving once the
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

/// Inspectable evidence for one plane's auto-multithreshold generation:
/// the quantities of its row of CASA's automask summary.
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
    /// Whether later cycles should stop evolving the plane's channel.
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
    /// Whether an earlier boundary stopped the plane's channel.
    pub stopped: bool,
}

/// Heap bytes per pixel one plane's automatic mask holds at most while it
/// is made ([`plane_mask`]): the residual it reads and a copy for its
/// statistics and a smoothed plane (`f64` each); region pruning's
/// component list and queue, grown by pushes (`usize` each, twice); and
/// its twelve `bool` planes, the valid pixels and new support among them.
pub const AUTO_MULTITHRESH_BYTES_PER_CELL: u64 =
    3 * size_of::<f64>() as u64 + 4 * size_of::<usize>() as u64 + 12;

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

/// Everything one plane's automatic mask is made from at a boundary.
pub(super) struct PlaneInputs<'a> {
    /// The plane's residual and valid pixels; `None` for a plane with no
    /// data, whose CASA residual is zero.
    pub(super) plane: Option<&'a AutoMaskPlane>,
    /// The mask the plane inherits, x-major.
    pub(super) inherited: &'a [bool],
    pub(super) shape: [usize; 2],
    /// Whether the inherited mask covers the whole first plane of the
    /// plane's channel block, so that its robust noise ignores the mask
    /// (`calcRobustImageStatistics`).
    pub(super) first_plane_full: bool,
    pub(super) cycle: AutoMaskCycle,
    pub(super) controls: &'a AutoMultithreshControls,
    /// The block's beam; `None` only where no plane of the block has data.
    pub(super) beam: Option<AutoMaskBeam>,
}

/// One plane's mask at a boundary, before its channel's stop rule.
pub(super) struct PlaneMask {
    /// The new support, x-major.
    pub(super) support: Vec<bool>,
    pub(super) evidence: AutoMultithreshEvidence,
    /// Whether the boundary added a positive (emission) region: CASA's
    /// positive mask of the plane at the first boundary.
    pub(super) positive: bool,
}

/// The next auto-multithreshold mask of one plane from the mask it
/// inherits.
///
/// # Errors
///
/// Shapes disagree, the residual is not finite, or the plane has data and
/// no beam.
pub(super) fn plane_mask(inputs: &PlaneInputs<'_>) -> Result<PlaneMask, MaskError> {
    let PlaneInputs {
        plane,
        inherited,
        shape,
        first_plane_full,
        cycle,
        controls,
        beam,
    } = *inputs;
    let cells = shape[0] * shape[1];
    if inherited.len() != cells {
        return Err(MaskError::ShapeMismatch);
    }
    let Some(plane) = plane else {
        return Ok(empty_plane(inherited, cycle, controls));
    };
    if plane.residual.len() != cells || plane.valid.len() != cells {
        return Err(MaskError::ShapeMismatch);
    }
    if plane.residual.iter().any(|value| !value.is_finite()) {
        return Err(MaskError::NonfiniteResidual);
    }
    let beam = beam.ok_or(MaskError::InvalidBeamArea)?;
    let (noise, peak) = statistics(
        &plane.residual,
        &plane.valid,
        inherited,
        first_plane_full,
        cycle,
        controls,
    );
    let thresholds = Thresholds::new(noise, peak, beam.sidelobe_fraction, controls);
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
    let (added, positive) = if cycle.stopped {
        (vec![false; cells], false)
    } else {
        let masking = Plane {
            residual: &plane.residual,
            valid: &plane.valid,
            shape,
            beam,
            controls,
        };
        masking.additions(inherited, &thresholds, cycle, &mut evidence)
    };
    let support = inherited
        .iter()
        .zip(&added)
        .zip(&plane.valid)
        .map(|((kept, added), valid)| *valid && (*kept || *added))
        .collect::<Vec<_>>();
    evidence.changed_pixels = changed(inherited, &support);
    Ok(PlaneMask {
        support,
        evidence,
        positive,
    })
}

/// A plane with no data: CASA's residual is zero there, so its statistics
/// and thresholds are zero, the zero thresholds tie to the sidelobe's and
/// nothing is added.
fn empty_plane(
    inherited: &[bool],
    cycle: AutoMaskCycle,
    controls: &AutoMultithreshControls,
) -> PlaneMask {
    PlaneMask {
        support: inherited.to_vec(),
        evidence: AutoMultithreshEvidence {
            median: 0.0,
            robust_rms: 0.0,
            peak: 0.0,
            positive_threshold: 0.0,
            low_noise_threshold: 0.0,
            negative_threshold: (controls.negative_factor > 0.0).then_some(0.0),
            threshold: AutoMaskThreshold::Sidelobe,
            masked: !cycle.stopped,
            regions: 0,
            pruned_regions: 0,
            grow_regions: 0,
            grow_pruned_regions: 0,
            negative_pixels: 0,
            changed_pixels: 0,
            channel_stopped: cycle.stopped,
        },
        positive: false,
    }
}

fn changed(before: &[bool], after: &[bool]) -> usize {
    before
        .iter()
        .zip(after)
        .filter(|(before, after)| before != after)
        .count()
}

/// The plane's noise and signed absolute peak over its valid pixels
/// (`SDMaskHandler::autoMask`). The fast estimate is the classical
/// statistics of every valid pixel; the robust one excludes the inherited
/// mask, unless the mask covers the block's whole first plane, and is zero
/// for a stopped plane, as `calcRobustImageStatistics` records a flagged
/// channel.
fn statistics(
    residual: &[f64],
    valid: &[bool],
    inherited: &[bool],
    first_plane_full: bool,
    cycle: AutoMaskCycle,
    controls: &AutoMultithreshControls,
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
        plane_noise(
            residual,
            valid,
            controls.noise,
            Some(inherited),
            first_plane_full,
        )
        .unwrap_or(zero)
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
    /// the grown mask and the negative mask, each smoothed and cut; and
    /// whether it added a positive (emission) region.
    fn additions(
        &self,
        inherited: &[bool],
        thresholds: &Thresholds,
        cycle: AutoMaskCycle,
        evidence: &mut AutoMultithreshEvidence,
    ) -> (Vec<bool>, bool) {
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
        let positive = added.iter().any(|set| *set);
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
        (added, positive)
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

/// `SDMaskHandler::skipChannels`: a channel whose first polarization's
/// threshold is the noise's stops evolving once its mask is empty, or once
/// the global threshold is reached and its mask changed by nothing or by
/// less than `minpercentchange` percent. `before` and `now` count the
/// channel's mask over every polarization.
pub(super) fn skip_channel(
    before: usize,
    now: usize,
    first: AutoMaskThreshold,
    cycle: AutoMaskCycle,
    controls: &AutoMultithreshControls,
) -> bool {
    if cycle.stopped || first != AutoMaskThreshold::Noise {
        return cycle.stopped;
    }
    let (before, now) = (before as f64, now as f64);
    let fraction = controls.minimum_percent_change / 100.0;
    let change = (now - before).abs();
    now == 0.0
        || (fraction >= 0.0
            && cycle.threshold_reached
            && (change == 0.0 || change < fraction * before))
}

/// Whether tclean's automask controls are usable: finite, the thresholds
/// non-negative and the smoothing positive.
pub(super) fn validate_auto_controls(controls: &AutoMultithreshControls) -> Result<(), MaskError> {
    let nonnegative = [
        controls.sidelobe_factor,
        controls.noise_factor,
        controls.low_noise_factor,
        controls.negative_factor,
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
    {
        Err(MaskError::InvalidAutoControls)
    } else {
        Ok(())
    }
}

/// Whether a block's beam is usable: a finite positive area.
pub(super) fn validate_beam(beam: &AutoMaskBeam) -> Result<(), MaskError> {
    if beam.area_pixels.is_finite() && beam.area_pixels > 0.0 {
        Ok(())
    } else {
        Err(MaskError::InvalidBeamArea)
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

/// `SDMaskHandler::convolveMask` by the beam scaled by `smooth_factor`,
/// then the cut at `cut` times the smoothed peak.
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
    // CASA cuts with `makeMaskByPerChanThreshold`, so a negative
    // `cutthreshold` keeps the pixels below it: none.
    let smoothed = smooth(mask, shape, smooth_factor, beam);
    let peak = smoothed.iter().copied().fold(0.0_f64, f64::max);
    threshold_mask(&smoothed, cut * peak)
}

/// The mask convolved with the unnormalized elliptical Gaussian of the
/// beam scaled by `smooth_factor`.
fn smooth(mask: &[bool], shape: [usize; 2], smooth_factor: f64, beam: AutoMaskBeam) -> Vec<f64> {
    let sigma_major = smooth_factor * beam.major_fwhm_pixels / 2.354_820_045_030_949_3;
    let sigma_minor = smooth_factor * beam.minor_fwhm_pixels / 2.354_820_045_030_949_3;
    // Image2DConvolver::_shapeOfKernel uses a square +/-5-sigma Gaussian
    // kernel (`_sizeOfGaussian(width, 5.0)`) and then forces an odd shape.
    // An offset beyond the image's extent reaches none of its pixels, so
    // the kernel stops there: a beam wider than the image (a PSF fitted
    // to few short baselines) would otherwise ask for a kernel of its own
    // size, gigabytes for a small image, to the same sums.
    let extent = isize::try_from(shape[0].max(shape[1])).unwrap_or(isize::MAX);
    let radius = ((5.0 * sigma_major + 0.5) as isize + 1).min(extent - 1);
    let side = 2 * radius.unsigned_abs() + 1;
    let cos_pa = beam.position_angle_rad.cos();
    let sin_pa = beam.position_angle_rad.sin();
    let kernel = (-radius..=radius)
        .flat_map(|dx| {
            (-radius..=radius).map(move |dy| {
                let (dx, dy) = (dx as f64, dy as f64);
                let u = dx * cos_pa + dy * sin_pa;
                let v = -dx * sin_pa + dy * cos_pa;
                (-0.5 * ((u / sigma_minor).powi(2) + (v / sigma_major).powi(2))).exp()
            })
        })
        .collect::<Vec<_>>();
    // Each output pixel sums the kernel at its offsets to the mask's set
    // pixels; scattering from the set pixels visits only those.
    let mut smoothed = vec![0.0; mask.len()];
    for (index, _) in mask.iter().enumerate().filter(|(_, set)| **set) {
        let pixel = [index / shape[1], index % shape[1]];
        for (row, dx) in (-radius..=radius).enumerate() {
            for (column, dy) in (-radius..=radius).enumerate() {
                if let Some(target) = offset_mask_pixel(pixel, [-dx, -dy], shape) {
                    smoothed[target[0] * shape[1] + target[1]] += kernel[row * side + column];
                }
            }
        }
    }
    smoothed
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CASA prunes and grows only the emission mask: an absorption feature
    /// smaller than the minimum beam fraction is masked where an emission
    /// feature of the same size is pruned, and the absorption mask does not
    /// grow into the low-noise residual around it
    /// (`SDMaskHandler::autoMaskByMultiThreshold`).
    #[test]
    fn the_absorption_mask_is_neither_pruned_nor_grown() {
        let shape = [16, 16];
        let mut residual = vec![0.0; 256];
        // One-pixel emission and absorption, each in a collar above the
        // low-noise threshold.
        let [emission, absorption] = [4 * 16 + 4, 11 * 16 + 11];
        residual[emission] = 1.0;
        residual[absorption] = -1.0;
        for index in [emission, absorption] {
            for neighbour in [index - 16, index + 16, index - 1, index + 1] {
                residual[neighbour] = 0.3;
            }
        }
        let controls = AutoMultithreshControls {
            sidelobe_factor: 0.0,
            noise_factor: 0.0,
            low_noise_factor: 0.0,
            negative_factor: 1.0,
            minimum_beam_fraction: 0.3,
            // No smoothing isolates the pruning and growth.
            smooth_factor: 0.0,
            cut_threshold: 0.0,
            grow_iterations: 4,
            grow_prune: true,
            minimum_percent_change: -1.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let plane = Plane {
            residual: &residual,
            valid: &[true; 256],
            shape,
            beam: AutoMaskBeam::new(3.0, 3.0, 0.0, 0.1),
            controls: &controls,
        };
        let thresholds = Thresholds {
            positive: 0.5,
            low: 0.2,
            negative: -0.5,
            kind: AutoMaskThreshold::Noise,
        };
        let cycle = AutoMaskCycle {
            completed_major_cycles: 1,
            threshold_reached: false,
            stopped: false,
        };
        let mut evidence = AutoMultithreshEvidence {
            median: 0.0,
            robust_rms: 0.0,
            peak: 1.0,
            positive_threshold: 0.5,
            low_noise_threshold: 0.2,
            negative_threshold: Some(-0.5),
            threshold: AutoMaskThreshold::Noise,
            masked: true,
            regions: 0,
            pruned_regions: 0,
            grow_regions: 0,
            grow_pruned_regions: 0,
            negative_pixels: 0,
            changed_pixels: 0,
            channel_stopped: false,
        };
        let (added, positive) = plane.additions(&[false; 256], &thresholds, cycle, &mut evidence);
        // The emission was pruned: only the absorption was added.
        assert!(!positive);
        let masked = added
            .iter()
            .enumerate()
            .filter(|(_, masked)| **masked)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(masked, [absorption]);
        assert_eq!((evidence.regions, evidence.pruned_regions), (1, 1));
        assert_eq!(evidence.negative_pixels, 1);
    }

    #[test]
    fn smoothing_scatters_the_gaussian_each_pixel_gathers() {
        let shape = [13, 9];
        let mask = (0..shape[0] * shape[1])
            .map(|index| index % 7 == 0 || index % 11 == 3)
            .collect::<Vec<_>>();
        let beam = AutoMaskBeam::new(3.0, 1.5, 0.6, 0.1);
        let smoothed = smooth(&mask, shape, 1.2, beam);
        let (sigma_major, sigma_minor) = (
            1.2 * 3.0 / 2.354_820_045_030_949_3,
            1.2 * 1.5 / 2.354_820_045_030_949_3,
        );
        let radius = (5.0 * sigma_major + 0.5) as isize + 1;
        for x in 0..shape[0] {
            for y in 0..shape[1] {
                let mut gathered = 0.0;
                for dx in -radius..=radius {
                    for dy in -radius..=radius {
                        if let Some(pixel) = offset_mask_pixel([x, y], [dx, dy], shape)
                            && mask[pixel[0] * shape[1] + pixel[1]]
                        {
                            let (dx, dy) = (dx as f64, dy as f64);
                            let u = dx * 0.6_f64.cos() + dy * 0.6_f64.sin();
                            let v = -dx * 0.6_f64.sin() + dy * 0.6_f64.cos();
                            gathered += (-0.5
                                * ((u / sigma_minor).powi(2) + (v / sigma_major).powi(2)))
                            .exp();
                        }
                    }
                }
                let value = smoothed[x * shape[1] + y];
                assert!(
                    (value - gathered).abs() <= 1e-12 * gathered.max(1.0),
                    "[{x}, {y}]"
                );
            }
        }
    }

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
        let (_, peak) = statistics(&residual, &valid, &[false; 4], false, cycle, &controls);
        // Neither recentred by the median nor read from an invalid pixel.
        assert_eq!(peak, 128.9);
        let mut robust = controls;
        robust.noise = NoiseEstimate::Robust;
        let stopped = AutoMaskCycle {
            stopped: true,
            ..cycle
        };
        let (noise, _) = statistics(&residual, &valid, &[false; 4], false, stopped, &robust);
        assert_eq!((noise.median, noise.rms), (0.0, 0.0));
        let (noise, _) = statistics(&residual, &valid, &[false; 4], false, stopped, &controls);
        assert_eq!(noise.median, -1.3);
    }

    /// The robust noise leaves the inherited mask out of a plane's
    /// statistics, unless the mask covers the whole first plane of the
    /// channel block: then every plane of the block takes Chauvenet
    /// statistics over all its valid pixels (`calcRobustImageStatistics`).
    #[test]
    fn a_full_first_plane_turns_every_plane_to_chauvenet() {
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
            noise: NoiseEstimate::Robust,
            verbose: false,
        };
        let cycle = AutoMaskCycle {
            completed_major_cycles: 1,
            threshold_reached: false,
            stopped: false,
        };
        let residual = [1.0, 2.0, 3.0, 40.0, 50.0];
        let valid = [true; 5];
        let inherited = [false, false, false, true, true];
        let (outside, _) = statistics(&residual, &valid, &inherited, false, cycle, &controls);
        assert_eq!(outside.median, 2.0);
        let (whole, _) = statistics(&residual, &valid, &inherited, true, cycle, &controls);
        assert_eq!(whole, crate::chauvenet_noise(residual.to_vec()).unwrap());
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
        let cycle = |threshold_reached| AutoMaskCycle {
            completed_major_cycles: 2,
            threshold_reached,
            stopped: false,
        };
        let noise = AutoMaskThreshold::Noise;
        let sidelobe = AutoMaskThreshold::Sidelobe;
        // An empty mask stops a noise-thresholded channel only.
        assert!(skip_channel(0, 0, noise, cycle(false), &controls));
        assert!(!skip_channel(0, 0, sidelobe, cycle(false), &controls));
        // A one-pixel change of five is 20%, not less than 20%: only an
        // unchanged mask stops it, and only once the threshold is reached.
        assert!(!skip_channel(5, 6, noise, cycle(true), &controls));
        assert!(skip_channel(5, 5, noise, cycle(true), &controls));
        assert!(!skip_channel(5, 5, noise, cycle(false), &controls));
        let mut never = controls;
        never.minimum_percent_change = -1.0;
        assert!(!skip_channel(5, 5, noise, cycle(true), &never));
        // A stopped channel stays stopped.
        let stopped = AutoMaskCycle {
            stopped: true,
            ..cycle(false)
        };
        assert!(skip_channel(5, 9, sidelobe, stopped, &controls));
    }

    /// A beam far wider than the image smooths by the same sums as the
    /// whole kernel would give: at each pixel, the unnormalized Gaussian
    /// at its offset from every set pixel. The kernel stops at the image's
    /// extent, so the smoothing holds kilobytes, not the gigabytes a
    /// ±5σ kernel of such a beam needs.
    #[test]
    fn a_beam_wider_than_the_image_smooths_by_the_same_sums() {
        let shape = [16, 12];
        let mut mask = vec![false; 16 * 12];
        for index in [0, 5 * 12 + 7, 15 * 12 + 11] {
            mask[index] = true;
        }
        let (major, minor, angle) = (4000.0, 2500.0, 0.3_f64);
        let smoothed = smooth(
            &mask,
            shape,
            1.0,
            AutoMaskBeam::new(major, minor, angle, 0.1),
        );
        let sigma = |fwhm: f64| fwhm / 2.354_820_045_030_949_3;
        for (target, value) in smoothed.iter().enumerate() {
            let [x, y] = [target / shape[1], target % shape[1]];
            let expected = mask
                .iter()
                .enumerate()
                .filter(|(_, set)| **set)
                .map(|(source, _)| {
                    let dx = x as f64 - (source / shape[1]) as f64;
                    let dy = y as f64 - (source % shape[1]) as f64;
                    let u = dx * angle.cos() + dy * angle.sin();
                    let v = -dx * angle.sin() + dy * angle.cos();
                    (-0.5 * ((u / sigma(minor)).powi(2) + (v / sigma(major)).powi(2))).exp()
                })
                .sum::<f64>();
            assert!(
                (value - expected).abs() <= 1.0e-12,
                "{target}: {value} against {expected}"
            );
        }
    }
}
