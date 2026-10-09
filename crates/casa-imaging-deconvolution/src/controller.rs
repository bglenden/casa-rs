// SPDX-License-Identifier: LGPL-3.0-or-later
//! The clean controller: the run-wide stopping rules between major cycles
//! and the per-plane rules inside a minor cycle.
//!
//! [`Controller`] is CASA's `grpcInteractiveCleanManager`
//! (`synthesis/ImagerObjects/grpcInteractiveClean.cc`): it owns the global
//! threshold with its 1% tolerance, `niter`, `nmajor`, the automatic
//! `cycleniter`, the cycle threshold and the divergence rules across major
//! cycles. [`PlaneControl`] is `SIMinorCycleController::majorCycleRequired`,
//! applied to every plane after each solver step.

use casa_imaging_model::ReconstructionControls;

use crate::plane::{RobustNoise, Support, peak_magnitude, robust_noise};

/// CASA hands each solver step at most this many iterations when the cycle
/// allows 5000 or more (`SDAlgorithmBase::deconvolve`).
const STEP_CHUNK_THRESHOLD: usize = 5000;
const STEP_CHUNK: usize = 2000;

/// The relative tolerance of the global threshold test (CAS-11278).
const THRESHOLD_TOLERANCE: f64 = 0.01;

/// Why the run stops cleaning, with CASA's `stopcode` numbering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanStop {
    /// 1: `niter` iterations are done.
    Iterations,
    /// 2: the peak residual is at or within 1% of the threshold.
    Threshold,
    /// 4: the peak residual did not change across the last major cycle.
    NoChange,
    /// 5: the unmasked peak residual grew to more than four times its
    /// value at the previous major cycle (a relative growth above 3).
    DivergedFromPrevious,
    /// 6: the unmasked peak residual grew to more than four times its
    /// minimum.
    DivergedFromMinimum,
    /// 7: the mask is empty.
    ZeroMask,
    /// 8: the peak residual is at or within 1% of the n-sigma threshold.
    NSigma,
    /// 9: `nmajor` major cycles are done.
    MajorCycles,
}

impl CleanStop {
    /// CASA's `stopcode`.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Iterations => 1,
            Self::Threshold => 2,
            Self::NoChange => 4,
            Self::DivergedFromPrevious => 5,
            Self::DivergedFromMinimum => 6,
            Self::ZeroMask => 7,
            Self::NSigma => 8,
            Self::MajorCycles => 9,
        }
    }
}

/// What one look at the residual of every plane and field reports to the
/// controller between major cycles (CASA's minor-cycle initialisation
/// record, merged over fields).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResidualStatistics {
    /// Largest peak residual of any image store: within the store's mask
    /// when its mask is not empty, else over its valid pixels
    /// (`SynthesisDeconvolver::initMinorCycle`).
    pub peak: f64,
    /// Largest residual magnitude on the valid pixels of any plane.
    pub peak_no_mask: f64,
    /// Largest fitted PSF sidelobe of any plane.
    pub psf_sidelobe: f64,
    /// The n-sigma threshold of the plane with the largest robust noise;
    /// zero when `nsigma` is off.
    pub nsigma_threshold: f64,
    /// Number of cleanable pixels over every plane (CASA's mask sum).
    pub mask_sum: usize,
}

impl ResidualStatistics {
    /// Fold one image store (a field with all its channels and
    /// polarizations) into the run's: each plane's statistics with its PSF
    /// sidelobe. The store's peak is its masked peak when its mask sum over
    /// every plane is positive, so a channel with an empty mask contributes
    /// nothing then.
    pub fn include_store<'a>(
        &mut self,
        planes: impl IntoIterator<Item = (&'a PlaneStatistics, f64)>,
        nsigma: f64,
    ) {
        let (mut in_mask, mut no_mask, mut mask_sum) = (0.0_f64, 0.0_f64, 0);
        for (plane, psf_sidelobe) in planes {
            in_mask = in_mask.max(plane.peak_in_mask);
            no_mask = no_mask.max(plane.peak_no_mask);
            mask_sum += plane.mask_sum;
            self.psf_sidelobe = self.psf_sidelobe.max(psf_sidelobe);
            if let Some(threshold) = plane.nsigma_threshold(nsigma) {
                self.nsigma_threshold = self.nsigma_threshold.max(threshold);
            }
        }
        self.peak = self.peak.max(if mask_sum > 0 { in_mask } else { no_mask });
        self.peak_no_mask = self.peak_no_mask.max(no_mask);
        self.mask_sum += mask_sum;
    }

    /// No plane yet.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            peak: 0.0,
            peak_no_mask: 0.0,
            psf_sidelobe: 0.0,
            nsigma_threshold: 0.0,
            mask_sum: 0,
        }
    }
}

/// One plane's residual before a minor cycle.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlaneStatistics {
    /// Largest residual magnitude on the cleanable support; zero when the
    /// support is empty.
    pub peak_in_mask: f64,
    /// Largest residual magnitude on the valid pixels.
    pub peak_no_mask: f64,
    /// Number of cleanable pixels.
    pub mask_sum: usize,
    /// Robust noise of the valid pixels, measured when `nsigma` is on.
    pub noise: Option<RobustNoise>,
    /// Whether the n-sigma threshold adds the median (automatic masking).
    pub automask: bool,
}

impl PlaneStatistics {
    /// Measure term 0 of a residual: the peaks on `support` and on `valid`,
    /// and the robust noise of `valid` when `nsigma` is on
    /// (`SIImageStore::calcRobustRMS` with the primary-beam mask).
    #[must_use]
    pub fn measure(
        residual: &[f64],
        support: &Support,
        valid: &Support,
        nsigma: f64,
        automask: bool,
    ) -> Self {
        Self {
            peak_in_mask: peak_magnitude(residual, support),
            peak_no_mask: peak_magnitude(residual, valid),
            mask_sum: support.count(),
            noise: (nsigma > 0.0)
                .then(|| robust_noise(residual, valid))
                .flatten(),
            automask,
        }
    }

    /// The plane's peak residual as its minor cycle starts from it: within
    /// the mask when the plane has one, else over the valid pixels
    /// (`SDAlgorithmBase::deconvolve`).
    #[must_use]
    pub fn peak(&self) -> f64 {
        if self.mask_sum > 0 {
            self.peak_in_mask
        } else {
            self.peak_no_mask
        }
    }

    /// `nsigma × rms`, plus the median under automatic masking; `None` when
    /// `nsigma` is off or the plane has no valid pixel.
    #[must_use]
    pub fn nsigma_threshold(&self, nsigma: f64) -> Option<f64> {
        if nsigma <= 0.0 {
            return None;
        }
        self.noise.map(|noise| {
            let threshold = nsigma * noise.rms;
            if self.automask {
                noise.median + threshold
            } else {
                threshold
            }
        })
    }
}

/// The controls of one minor cycle, the same for every plane and field
/// (`getMinorCycleControls`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CycleControls {
    /// Iterations each plane may do (`cycleniter`, at most what remains of
    /// `niter`).
    pub iterations: usize,
    /// The cycle threshold: the PSF-sidelobe rule, never below the global
    /// threshold.
    pub threshold: f64,
    /// Whether the cycle threshold is the global threshold.
    pub threshold_reached: bool,
    /// The loop gain.
    pub gain: f64,
    /// The n-sigma multiplier; zero when off.
    pub nsigma: f64,
}

impl CycleControls {
    /// The iterations one solver step may take: the cycle's limit, or 2000
    /// when that is 5000 or more (`SDAlgorithmBase.cc`).
    #[must_use]
    pub const fn step_iterations(&self) -> usize {
        if self.iterations < STEP_CHUNK_THRESHOLD {
            self.iterations
        } else {
            STEP_CHUNK
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct CycleThresholdRule {
    factor: f64,
    minimum_psf_fraction: f64,
    maximum_psf_fraction: f64,
}

/// The run-wide clean state between major cycles.
#[derive(Clone, Debug, PartialEq)]
pub struct Controller {
    niter: usize,
    cycle_niter: usize,
    gain: f64,
    threshold: f64,
    nsigma: f64,
    cycle_rule: Option<CycleThresholdRule>,
    nmajor: Option<usize>,
    iter_done: usize,
    major_done: usize,
    previous_major_count: usize,
    previous_peak: Option<f64>,
    previous_peak_no_mask: Option<f64>,
    minimum_peak_no_mask: f64,
    minor_cycle_peak: f64,
}

impl Controller {
    /// The controller of one run.
    ///
    /// `cycleniter` is CASA's automatic value, `niter`, unless the controls
    /// fix a positive smaller one (`setControlsFromRecord`). Without a cycle
    /// factor the cycle threshold is the global threshold.
    #[must_use]
    pub fn new(controls: &ReconstructionControls) -> Self {
        let niter = controls.max_minor_iterations();
        let cycle_niter = controls
            .cycle_iteration_limit()
            .filter(|limit| *limit > 0)
            .map_or(niter, |limit| limit.min(niter));
        // `with_cycle_threshold` sets the three together.
        let cycle_rule = match (
            controls.cycle_factor(),
            controls.minimum_psf_fraction(),
            controls.maximum_psf_fraction(),
        ) {
            (Some(factor), Some(minimum_psf_fraction), Some(maximum_psf_fraction)) => {
                Some(CycleThresholdRule {
                    factor,
                    minimum_psf_fraction,
                    maximum_psf_fraction,
                })
            }
            _ => None,
        };
        Self {
            niter,
            cycle_niter,
            gain: controls.gain(),
            threshold: controls.threshold_jy_per_beam(),
            nsigma: controls.noise_sigma().unwrap_or(0.0),
            cycle_rule,
            nmajor: controls.maximum_major_cycles(),
            iter_done: 0,
            major_done: 0,
            previous_major_count: 0,
            previous_peak: None,
            previous_peak_no_mask: None,
            minimum_peak_no_mask: 1.0e9,
            minor_cycle_peak: 0.0,
        }
    }

    /// The n-sigma multiplier; zero when off.
    #[must_use]
    pub const fn nsigma(&self) -> f64 {
        self.nsigma
    }

    /// Iterations charged so far (CASA's `iterdone`).
    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.iter_done
    }

    /// Major cycles after the initial one (CASA's `nmajordone` less one).
    #[must_use]
    pub const fn major_cycles(&self) -> usize {
        self.major_done
    }

    /// Whether the next major cycle certainly ends the run: the iterations
    /// or the major cycles are used up. A threshold stop cannot be known
    /// before the residual it tests.
    #[must_use]
    pub fn budget_spent(&self) -> bool {
        self.iter_done >= self.niter
            || self
                .nmajor
                .is_some_and(|nmajor| self.major_done + 1 >= nmajor)
    }

    /// CASA's `cleanComplete` after a major cycle: why the run stops, or
    /// `None` to run another minor cycle. Records the peaks for the
    /// divergence and no-change rules.
    pub fn clean_complete(&mut self, statistics: &ResidualStatistics) -> Option<CleanStop> {
        let previous_peak = *self.previous_peak.get_or_insert(statistics.peak);
        let previous_no_mask = *self
            .previous_peak_no_mask
            .get_or_insert(statistics.peak_no_mask);
        let stop = self.evaluate(statistics, statistics.peak, previous_peak, previous_no_mask);
        self.minimum_peak_no_mask = self.minimum_peak_no_mask.min(statistics.peak_no_mask.abs());
        self.previous_peak = Some(statistics.peak);
        self.previous_peak_no_mask = Some(statistics.peak_no_mask);
        self.previous_major_count = self.major_done;
        stop
    }

    /// CASA's `cleanComplete(lastcyclecheck=True)` before a major cycle: whether
    /// the peak the minor cycle reached already ends the run, so the coming
    /// major cycle is the last. Records nothing.
    #[must_use]
    pub fn last_cycle(&self, statistics: &ResidualStatistics) -> bool {
        let previous_peak = self.previous_peak.unwrap_or(statistics.peak);
        let previous_no_mask = self
            .previous_peak_no_mask
            .unwrap_or(statistics.peak_no_mask);
        self.evaluate(
            statistics,
            self.minor_cycle_peak,
            previous_peak,
            previous_no_mask,
        )
        .is_some()
    }

    fn evaluate(
        &self,
        statistics: &ResidualStatistics,
        use_peak: f64,
        previous_peak: f64,
        previous_no_mask: f64,
    ) -> Option<CleanStop> {
        let threshold = self.threshold;
        let nsigma = statistics.nsigma_threshold;
        let peak = statistics.peak;
        // CASA's first branch (an empty mask before the first major cycle)
        // never fires under tclean, whose initial major cycle is counted
        // before the first test.
        let mut stop = None;
        if self.iter_done >= self.niter
            || use_peak <= threshold
            || peak <= nsigma
            || (use_peak - threshold).abs() / threshold < THRESHOLD_TOLERANCE
            || (peak - nsigma).abs() / nsigma < THRESHOLD_TOLERANCE
        {
            if self.iter_done >= self.niter {
                stop = Some(CleanStop::Iterations);
            }
            if use_peak <= threshold || (use_peak - threshold) / threshold < THRESHOLD_TOLERANCE {
                stop = Some(CleanStop::Threshold);
            } else if (use_peak <= nsigma || (peak - nsigma) / nsigma < THRESHOLD_TOLERANCE)
                && nsigma != 0.0
            {
                stop = Some(CleanStop::NSigma);
            }
        } else if statistics.mask_sum == 0 {
            stop = Some(CleanStop::ZeroMask);
        } else if self.iter_done > 0
            && self.major_done > self.previous_major_count
            && (previous_peak - peak).abs() < 1.0e-10
        {
            stop = Some(CleanStop::NoChange);
        } else if self.iter_done > 0
            && (statistics.peak_no_mask - previous_no_mask).abs() / previous_no_mask.abs() > 3.0
        {
            stop = Some(CleanStop::DivergedFromPrevious);
        } else if self.iter_done > 0
            && (statistics.peak_no_mask.abs() - self.minimum_peak_no_mask)
                / self.minimum_peak_no_mask
                > 3.0
        {
            stop = Some(CleanStop::DivergedFromMinimum);
        }
        if stop.is_none() && self.nmajor.is_some_and(|nmajor| self.major_done >= nmajor) {
            stop = Some(CleanStop::MajorCycles);
        }
        stop
    }

    /// The controls of the next minor cycle (`getMinorCycleControls`):
    /// `cycleniter` limited to what remains of `niter`, and the cycle
    /// threshold `peak × clamp(sidelobe × cyclefactor, minpsffraction,
    /// maxpsffraction)`, never below the global threshold.
    #[must_use]
    pub fn cycle_controls(&self, statistics: &ResidualStatistics) -> CycleControls {
        let automatic = self.cycle_rule.map_or(0.0, |rule| {
            statistics.peak
                * (statistics.psf_sidelobe * rule.factor)
                    .max(rule.minimum_psf_fraction)
                    .min(rule.maximum_psf_fraction)
        });
        let threshold = automatic.max(self.threshold);
        CycleControls {
            iterations: self
                .cycle_niter
                .min(self.niter.saturating_sub(self.iter_done)),
            threshold,
            threshold_reached: threshold == self.threshold,
            gain: self.gain,
            nsigma: self.nsigma,
        }
    }

    /// Charge one minor cycle: its iterations, and the largest peak residual
    /// of the planes that stopped on a rule (`mergeCycleExecutionRecord`).
    pub fn record_minor_cycle(&mut self, iterations: usize, peak: f64) {
        self.iter_done += iterations;
        self.minor_cycle_peak = peak;
    }

    /// Count a major cycle after a minor cycle (`incrementMajorCycleCount`).
    pub fn end_major_cycle(&mut self) {
        self.major_done += 1;
    }
}

/// Why one plane's minor cycle stopped, with CASA's minor-cycle stop codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaneStop {
    /// 0: the plane has no cleanable pixel.
    ZeroMask,
    /// 1: the plane did `cycleniter` iterations.
    Iterations,
    /// 2: the peak residual is at or below the cycle threshold.
    CycleThreshold,
    /// 3: a solver step did no iteration.
    ZeroIterations,
    /// 4: the peak residual rose 10% above its minimum in this cycle.
    Diverged,
    /// 5: a solver step ended early without meeting a rule.
    Exited,
    /// 6: the peak residual is at or below the plane's n-sigma threshold.
    NSigma,
}

impl PlaneStop {
    /// CASA's minor-cycle stop code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::ZeroMask => 0,
            Self::Iterations => 1,
            Self::CycleThreshold => 2,
            Self::ZeroIterations => 3,
            Self::Diverged => 4,
            Self::Exited => 5,
            Self::NSigma => 6,
        }
    }
}

/// The per-plane controller of one minor cycle
/// (`SIMinorCycleController`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlaneControl {
    cycle_iterations: usize,
    cycle_threshold: f64,
    nsigma: f64,
    nsigma_threshold: f64,
    iterations: usize,
    last_step: Option<usize>,
    minimum: f64,
}

impl PlaneControl {
    /// The controller of one plane entering a cycle with peak residual
    /// `peak` (`setPeakResidual` then `resetMinResidual`).
    #[must_use]
    pub fn new(cycle: &CycleControls, plane: &PlaneStatistics, peak: f64) -> Self {
        Self {
            cycle_iterations: cycle.iterations,
            cycle_threshold: cycle.threshold,
            nsigma: cycle.nsigma,
            nsigma_threshold: plane.nsigma_threshold(cycle.nsigma).unwrap_or(0.0),
            iterations: 0,
            last_step: None,
            minimum: peak,
        }
    }

    /// The threshold the solver steps clean to: the larger of the cycle
    /// threshold and the plane's n-sigma threshold.
    #[must_use]
    pub fn step_threshold(&self) -> f64 {
        if self.nsigma > 0.0 {
            self.nsigma_threshold.max(self.cycle_threshold)
        } else {
            self.cycle_threshold
        }
    }

    /// Iterations charged to this plane in this cycle.
    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.iterations
    }

    /// Note a step's peak residual, signed as its solver reports it, before
    /// testing it (`setPeakResidual`): the minimum is signed, as CASA's is.
    pub fn observe(&mut self, peak: f64) {
        self.minimum = self.minimum.min(peak);
    }

    /// Charge one step (`incrementMinorCycleCount`).
    pub fn charge(&mut self, iterations: usize) {
        self.iterations += iterations;
        self.last_step = Some(iterations);
    }

    /// `majorCycleRequired`: why the plane stops at peak residual `peak`,
    /// or `None` to take another step. Later rules override earlier ones.
    #[must_use]
    pub fn stop(&self, peak: f64) -> Option<PlaneStop> {
        let magnitude = peak.abs();
        let mut stop = None;
        if self.iterations >= self.cycle_iterations {
            stop = Some(PlaneStop::Iterations);
        }
        if self.cycle_threshold >= self.nsigma_threshold {
            if magnitude <= self.cycle_threshold {
                stop = Some(PlaneStop::CycleThreshold);
            }
        } else if magnitude <= self.nsigma_threshold
            && self.last_step.is_some_and(|step| step > 0)
            && self.nsigma != 0.0
        {
            stop = Some(PlaneStop::NSigma);
        }
        if self.last_step == Some(0) {
            stop = Some(PlaneStop::ZeroIterations);
        }
        if self.last_step.is_some_and(|step| step > 0)
            && self.minimum.abs() > 0.0
            && (magnitude - self.minimum.abs()) / self.minimum.abs() > 0.1
        {
            stop = Some(PlaneStop::Diverged);
        }
        stop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controls(niter: usize) -> ReconstructionControls {
        ReconstructionControls::new(niter, 0.1, 0.01).with_cycle_threshold(1.0, 0.05, 0.8)
    }

    fn statistics(peak: f64) -> ResidualStatistics {
        ResidualStatistics {
            peak,
            peak_no_mask: peak,
            psf_sidelobe: 0.2,
            nsigma_threshold: 0.0,
            mask_sum: 100,
        }
    }

    #[test]
    fn automatic_cycleniter_is_what_remains_of_niter() {
        let mut controller = Controller::new(&controls(500));
        assert_eq!(controller.cycle_controls(&statistics(1.0)).iterations, 500);
        controller.record_minor_cycle(320, 0.5);
        assert_eq!(controller.cycle_controls(&statistics(1.0)).iterations, 180);
        let fixed = Controller::new(&controls(500).with_cycle_limits(100, None));
        assert_eq!(fixed.cycle_controls(&statistics(1.0)).iterations, 100);
        let zero = Controller::new(&controls(500).with_cycle_limits(0, None));
        assert_eq!(zero.cycle_controls(&statistics(1.0)).iterations, 500);
    }

    #[test]
    fn the_cycle_threshold_clamps_the_sidelobe_fraction_and_keeps_the_threshold_floor() {
        let controller = Controller::new(&controls(100));
        let cycle = controller.cycle_controls(&statistics(2.0));
        assert!((cycle.threshold - 0.4).abs() < 1e-15);
        assert!(!cycle.threshold_reached);
        // The 0.05 floor of a 0.01 sidelobe still exceeds the threshold.
        let mut low = statistics(2.0);
        low.psf_sidelobe = 0.01;
        assert!((controller.cycle_controls(&low).threshold - 0.1).abs() < 1e-15);
        // Below the threshold the cycle cleans to the threshold.
        let cycle = controller.cycle_controls(&statistics(0.04));
        assert_eq!(cycle.threshold, 0.01);
        assert!(cycle.threshold_reached);
    }

    /// The controller after tclean's first check, which follows the initial
    /// major cycle at zero iterations, then one minor and one major cycle.
    fn after_one_cycle(controls: &ReconstructionControls, first: f64) -> Controller {
        let mut controller = Controller::new(controls);
        assert_eq!(controller.clean_complete(&statistics(first)), None);
        controller.record_minor_cycle(5, 0.0);
        controller.end_major_cycle();
        controller
    }

    #[test]
    fn the_global_threshold_stops_within_one_percent() {
        for (peak, stop) in [
            (0.0102, None),
            (0.01009, Some(CleanStop::Threshold)),
            (0.005, Some(CleanStop::Threshold)),
        ] {
            let mut controller = after_one_cycle(&controls(100), 1.0);
            assert_eq!(controller.clean_complete(&statistics(peak)), stop, "{peak}");
        }
    }

    #[test]
    fn global_stop_codes_follow_casa_precedence() {
        // 7: an empty mask before any cleaning.
        let mut empty = statistics(1.0);
        empty.mask_sum = 0;
        assert_eq!(
            Controller::new(&controls(100)).clean_complete(&empty),
            Some(CleanStop::ZeroMask)
        );
        // 1, and 2 overrides 1.
        let mut controller = Controller::new(&controls(100));
        assert_eq!(controller.clean_complete(&statistics(1.0)), None);
        controller.record_minor_cycle(100, 0.0);
        controller.end_major_cycle();
        assert_eq!(
            controller.clone().clean_complete(&statistics(0.9)),
            Some(CleanStop::Iterations)
        );
        assert_eq!(
            controller.clean_complete(&statistics(0.001)),
            Some(CleanStop::Threshold)
        );
        // 8: n-sigma.
        let mut controller = after_one_cycle(&controls(100).with_noise_sigma(3.0), 1.0);
        let mut noisy = statistics(0.2);
        noisy.nsigma_threshold = 0.3;
        assert_eq!(controller.clean_complete(&noisy), Some(CleanStop::NSigma));
        // 4: no change across a major cycle.
        let mut controller = after_one_cycle(&controls(100), 0.5);
        assert_eq!(
            controller.clean_complete(&statistics(0.5)),
            Some(CleanStop::NoChange)
        );
        // 5: more than fourfold over the previous major cycle.
        let mut controller = after_one_cycle(&controls(100), 0.5);
        assert_eq!(controller.clean_complete(&statistics(1.9)), None);
        controller.end_major_cycle();
        assert_eq!(
            controller.clean_complete(&statistics(0.5)),
            None,
            "a falling peak is not divergence"
        );
        controller.end_major_cycle();
        assert_eq!(
            controller.clean_complete(&statistics(2.6)),
            Some(CleanStop::DivergedFromPrevious)
        );
        // 6: more than fourfold over the minimum, not over the previous.
        let mut controller = after_one_cycle(&controls(100), 0.5);
        assert_eq!(controller.clean_complete(&statistics(1.0)), None);
        controller.end_major_cycle();
        assert_eq!(
            controller.clean_complete(&statistics(2.1)),
            Some(CleanStop::DivergedFromMinimum)
        );
        // 9: nmajor.
        let mut controller = after_one_cycle(&controls(100).with_cycle_limits(10, Some(2)), 1.0);
        assert_eq!(controller.clean_complete(&statistics(0.6)), None);
        controller.end_major_cycle();
        assert!(controller.budget_spent());
        assert_eq!(
            controller.clean_complete(&statistics(0.4)),
            Some(CleanStop::MajorCycles)
        );
    }

    fn cycle(iterations: usize, threshold: f64, nsigma: f64) -> CycleControls {
        CycleControls {
            iterations,
            threshold,
            threshold_reached: false,
            gain: 0.1,
            nsigma,
        }
    }

    fn plane(noise: Option<RobustNoise>) -> PlaneStatistics {
        PlaneStatistics {
            peak_in_mask: 1.0,
            peak_no_mask: 1.0,
            mask_sum: 10,
            noise,
            automask: false,
        }
    }

    /// Stop codes 1–4 and 6 of `majorCycleRequired`, each on its own rule.
    #[test]
    fn plane_stop_codes() {
        // 2 before any step; n-sigma is not tested before a step.
        let control = PlaneControl::new(&cycle(50, 0.3, 0.0), &plane(None), 1.0);
        assert_eq!(control.stop(1.0), None);
        assert_eq!(control.stop(0.3), Some(PlaneStop::CycleThreshold));
        // 1: the cycle's iterations are spent.
        let mut control = PlaneControl::new(&cycle(50, 0.3, 0.0), &plane(None), 1.0);
        control.observe(1.0);
        control.charge(50);
        assert_eq!(control.stop(0.5), Some(PlaneStop::Iterations));
        // 3: a step did nothing.
        let mut control = PlaneControl::new(&cycle(50, 0.3, 0.0), &plane(None), 1.0);
        control.charge(0);
        assert_eq!(control.stop(0.9), Some(PlaneStop::ZeroIterations));
        // 4: more than 10% above the minimum after a step.
        let mut control = PlaneControl::new(&cycle(5000, 0.3, 0.0), &plane(None), 1.0);
        control.observe(0.6);
        control.charge(2000);
        assert_eq!(control.stop(0.65), None);
        assert_eq!(control.stop(0.67), Some(PlaneStop::Diverged));
        // 6: the n-sigma threshold above the cycle threshold.
        let noise = RobustNoise {
            median: 0.0,
            rms: 0.2,
        };
        let mut control = PlaneControl::new(&cycle(50, 0.3, 3.0), &plane(Some(noise)), 1.0);
        assert!((control.step_threshold() - 0.6).abs() < 1e-15);
        assert_eq!(control.stop(0.5), None, "not before a step");
        control.charge(10);
        assert_eq!(control.stop(0.5), Some(PlaneStop::NSigma));
        assert_eq!(control.stop(0.7), None);
    }

    /// A store with a mask reports its masked peak even where one channel
    /// has no mask; a store without one reports its unmasked peak.
    #[test]
    fn each_store_chooses_its_masked_or_unmasked_peak() {
        let plane = |peak_in_mask, peak_no_mask, mask_sum| PlaneStatistics {
            peak_in_mask,
            peak_no_mask,
            mask_sum,
            noise: None,
            automask: false,
        };
        let cube = [plane(0.5, 0.9, 10), plane(0.0, 2.0, 0)];
        assert_eq!((cube[0].peak(), cube[1].peak()), (0.5, 2.0));
        let mut statistics = ResidualStatistics::empty();
        statistics.include_store(cube.iter().map(|plane| (plane, 0.1)), 0.0);
        assert_eq!((statistics.peak, statistics.peak_no_mask), (0.5, 2.0));
        let unmasked = [plane(0.0, 0.7, 0)];
        statistics.include_store(unmasked.iter().map(|plane| (plane, 0.2)), 0.0);
        assert_eq!(statistics.peak, 0.7);
        assert_eq!(statistics.mask_sum, 10);
        assert_eq!(statistics.psf_sidelobe, 0.2);
    }

    #[test]
    fn steps_are_chunked_from_five_thousand_iterations() {
        assert_eq!(cycle(4999, 0.0, 0.0).step_iterations(), 4999);
        assert_eq!(cycle(5000, 0.0, 0.0).step_iterations(), 2000);
    }

    #[test]
    fn automask_adds_the_median_to_the_nsigma_threshold() {
        let noise = RobustNoise {
            median: 0.05,
            rms: 0.1,
        };
        let mut statistics = plane(Some(noise));
        assert!((statistics.nsigma_threshold(3.0).unwrap() - 0.3).abs() < 1e-15);
        statistics.automask = true;
        assert!((statistics.nsigma_threshold(3.0).unwrap() - 0.35).abs() < 1e-15);
        assert_eq!(statistics.nsigma_threshold(0.0), None);
    }
}
