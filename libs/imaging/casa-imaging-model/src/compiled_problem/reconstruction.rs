// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Frequency-domain model coefficient basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconstructionBasis {
    /// One coefficient shared across the selected frequency domain.
    Constant,
    /// Taylor polynomial coefficients across frequency.
    Taylor {
        /// Number of Taylor coefficients.
        terms: usize,
    },
    /// Independent coefficient state for each output channel.
    ChannelLocal {
        /// Number of output channels.
        channels: usize,
    },
}

/// Logical reconstruction algorithm, independent of its implementation backend.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconstructionAlgorithm {
    /// Produce normal-state and dirty products without a minor cycle.
    Dirty,
    /// Högbom point-component minor cycle.
    Hogbom,
    /// Clark point-component minor cycle, jointly over polarizations.
    Clark,
    /// Clark point-component minor cycle on each polarization separately.
    ClarkStokes,
    /// Image-plane maximum-entropy minor cycle.
    Mem,
    /// Multiscale minor cycle with explicit scale sizes in pixels.
    Multiscale {
        /// Canonical requested scale sizes.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
    /// Multi-term multi-frequency synthesis minor cycle.
    Mtmfs {
        /// Canonical requested scale sizes shared with multiscale cleaning.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
}

/// Accounting policy for Högbom's historical inclusive iteration loop.
///
/// Iteration limits remain task/controller budgets under both policies. CASA's
/// Högbom implementation may apply one additional component when a cycle runs
/// all the way to that budget; early scientific stops report every component
/// actually applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HogbomIterationAccounting {
    /// Apply and report no more components than the requested budget.
    Strict,
    /// Admit CASA's inclusive terminal component while preserving the reported budget.
    #[default]
    CasaInclusive,
}

/// CASA's PSF-sidelobe per-cycle stopping threshold law
/// (`SIMinorCycleController::getMinorCycleControls`): the cycle threshold is
/// the peak residual times `clamp(sidelobe × factor, minimum, maximum)` PSF
/// fraction, never below the global threshold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CycleThresholdRule {
    /// `cyclefactor`.
    pub factor: f64,
    /// `minpsffraction`.
    pub minimum_psf_fraction: f64,
    /// `maxpsffraction`.
    pub maximum_psf_fraction: f64,
}

/// How tclean estimates a residual plane's robust noise, which its
/// `nsigma` threshold and automatic mask read (`fastnoise`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NoiseEstimate {
    /// `fastnoise=True`: casacore's classical median and median absolute
    /// deviation of every valid pixel (`SDMaskHandler::calcImageStatistics`).
    #[default]
    Fast,
    /// `fastnoise=False`: Chauvenet-criterion statistics without a clean
    /// mask, else the classical statistics outside it
    /// (`SDMaskHandler::calcRobustImageStatistics`).
    Robust,
}

/// Scientific stopping and update controls for reconstruction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReconstructionControls {
    pub(super) max_minor_iterations: usize,
    pub(super) gain: f64,
    pub(super) threshold_jy_per_beam: f64,
    pub(super) cycle_iteration_limit: Option<usize>,
    pub(super) maximum_major_cycles: Option<usize>,
    pub(super) noise_sigma: Option<f64>,
    pub(super) noise_estimate: NoiseEstimate,
    pub(super) primary_beam_mask: f64,
    pub(super) cycle_threshold: Option<CycleThresholdRule>,
    pub(super) hogbom_iteration_accounting: HogbomIterationAccounting,
}

impl ReconstructionControls {
    /// Construct reconstruction controls.
    #[must_use]
    pub const fn new(max_minor_iterations: usize, gain: f64, threshold_jy_per_beam: f64) -> Self {
        Self {
            max_minor_iterations,
            gain,
            threshold_jy_per_beam,
            cycle_iteration_limit: None,
            maximum_major_cycles: None,
            noise_sigma: None,
            noise_estimate: NoiseEstimate::Fast,
            primary_beam_mask: 0.0,
            cycle_threshold: None,
            hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        }
    }

    /// Bind the iteration budget and post-initial major-cycle bound (zero is initial-only).
    #[must_use]
    pub const fn with_cycle_limits(
        mut self,
        cycle_iteration_limit: usize,
        maximum_major_cycles: Option<usize>,
    ) -> Self {
        self.cycle_iteration_limit = Some(cycle_iteration_limit);
        self.maximum_major_cycles = maximum_major_cycles;
        self
    }

    /// Bind an RMS-multiple stopping threshold in addition to the absolute threshold.
    #[must_use]
    pub const fn with_noise_sigma(mut self, noise_sigma: f64) -> Self {
        self.noise_sigma = Some(noise_sigma);
        self
    }

    /// Bind how the robust noise of the `nsigma` threshold is estimated.
    #[must_use]
    pub const fn with_noise_estimate(mut self, noise_estimate: NoiseEstimate) -> Self {
        self.noise_estimate = noise_estimate;
        self
    }

    /// Bind tclean's `pbmask`, the `.pb` level in `[0, 1)` above which the
    /// `nsigma` noise and an automatic mask read the residual
    /// (`SIImageStore::calcRobustRMS`, `SDMaskHandler::autoMaskWithinPB`).
    #[must_use]
    pub const fn with_primary_beam_mask(mut self, level: f64) -> Self {
        self.primary_beam_mask = level;
        self
    }

    /// Bind CASA's PSF-sidelobe-based per-cycle stopping threshold law.
    #[must_use]
    pub const fn with_cycle_threshold(
        mut self,
        cycle_factor: f64,
        minimum_psf_fraction: f64,
        maximum_psf_fraction: f64,
    ) -> Self {
        self.cycle_threshold = Some(CycleThresholdRule {
            factor: cycle_factor,
            minimum_psf_fraction,
            maximum_psf_fraction,
        });
        self
    }

    /// Select Högbom's strict or CASA-inclusive iteration accounting.
    #[must_use]
    pub const fn with_hogbom_iteration_accounting(
        mut self,
        accounting: HogbomIterationAccounting,
    ) -> Self {
        self.hogbom_iteration_accounting = accounting;
        self
    }

    /// Return the reported total minor-iteration budget.
    #[must_use]
    pub const fn max_minor_iterations(self) -> usize {
        self.max_minor_iterations
    }

    /// Return the loop gain.
    #[must_use]
    pub const fn gain(self) -> f64 {
        self.gain
    }

    /// Return the absolute stopping threshold in Jy/beam.
    #[must_use]
    pub const fn threshold_jy_per_beam(self) -> f64 {
        self.threshold_jy_per_beam
    }

    /// Return the explicit reported per-cycle iteration budget.
    #[must_use]
    pub const fn cycle_iteration_limit(self) -> Option<usize> {
        self.cycle_iteration_limit
    }

    /// Return the post-initial major-cycle bound; zero stops after the initial pass.
    #[must_use]
    pub const fn maximum_major_cycles(self) -> Option<usize> {
        self.maximum_major_cycles
    }

    /// Return the optional robust-RMS threshold multiplier.
    #[must_use]
    pub const fn noise_sigma(self) -> Option<f64> {
        self.noise_sigma
    }

    /// Return how the robust noise of the `nsigma` threshold is estimated.
    #[must_use]
    pub const fn noise_estimate(self) -> NoiseEstimate {
        self.noise_estimate
    }

    /// Return tclean's `pbmask`: zero reads the `.pb`'s whole support.
    #[must_use]
    pub const fn primary_beam_mask(self) -> f64 {
        self.primary_beam_mask
    }

    /// Return CASA's per-cycle threshold rule, when it is bound.
    #[must_use]
    pub const fn cycle_threshold_rule(self) -> Option<CycleThresholdRule> {
        self.cycle_threshold
    }

    /// Return Högbom's iteration-accounting policy.
    #[must_use]
    pub const fn hogbom_iteration_accounting(self) -> HogbomIterationAccounting {
        self.hogbom_iteration_accounting
    }
}

/// Logical reconstruction requirements for one imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconstructionContract {
    pub(super) basis: ReconstructionBasis,
    pub(super) algorithm: ReconstructionAlgorithm,
    pub(super) controls: ReconstructionControls,
    pub(super) polarization: PolarizationContract,
}

impl ReconstructionContract {
    /// Construct reconstruction requirements.
    #[must_use]
    pub const fn new(
        basis: ReconstructionBasis,
        algorithm: ReconstructionAlgorithm,
        controls: ReconstructionControls,
        polarization: PolarizationContract,
    ) -> Self {
        Self {
            basis,
            algorithm,
            controls,
            polarization,
        }
    }

    /// Return the reconstruction basis.
    #[must_use]
    pub const fn basis(&self) -> ReconstructionBasis {
        self.basis
    }

    /// Return the requested algorithm.
    #[must_use]
    pub const fn algorithm(&self) -> &ReconstructionAlgorithm {
        &self.algorithm
    }

    /// Return reconstruction controls.
    #[must_use]
    pub const fn controls(&self) -> ReconstructionControls {
        self.controls
    }

    /// Return reconstruction-owned polarization coordinates.
    #[must_use]
    pub const fn polarization(&self) -> &PolarizationContract {
        &self.polarization
    }

    pub(super) fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.polarization = self.polarization.canonicalize()?;
        if let ReconstructionAlgorithm::Multiscale { scales_px, .. }
        | ReconstructionAlgorithm::Mtmfs { scales_px, .. } = &mut self.algorithm
        {
            for scale in scales_px.iter_mut() {
                if *scale == 0.0 {
                    *scale = 0.0;
                }
            }
            scales_px.sort_unstable_by(|left, right| left.total_cmp(right));
            scales_px.dedup();
        }
        Ok(self)
    }
}
