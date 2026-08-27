// SPDX-License-Identifier: LGPL-3.0-or-later

//! Frozen global weighting generations and bounded weighted replay.

use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, fmt, mem::size_of};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, FiniteValuePolicy, ImageDomainRole, LogicalIdentity,
    SelectedObservationSample, SelectedSpectralContribution, SelectedSpectralContributions,
    UvTaper, WeightDensityScope, WeightingCommitmentId, WeightingScheme,
};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const GENERATION_DOMAIN: &[u8] = b"casa-rs-frozen-weighting-generation";
const GENERATION_VERSION: u32 = 1;
const REPLAY_DOMAIN: &[u8] = b"casa-rs-weighting-replay";
const REPLAY_VERSION: u32 = 1;
const COVERAGE_DOMAIN: &[u8] = b"casa-rs-weighting-replay-coverage";
const COVERAGE_VERSION: u32 = 2;
const CONSERVATIVE_TREE_ENTRY_BYTES: usize = 64;
const F32_EXPONENT_BINS: usize = 254;
const F64_EXPONENT_BINS: usize = 2_046;

macro_rules! weighting_identity {
    ($name:ident, $version:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(LogicalIdentity);

        impl $name {
            /// Identity schema version.
            pub const SCHEMA_VERSION: u32 = $version;

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }

            /// Return this typed value as a logical identity.
            #[must_use]
            pub const fn identity(self) -> LogicalIdentity {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.as_bytes())?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.as_bytes())
            }
        }
    };
}

weighting_identity!(
    WeightingGenerationId,
    GENERATION_VERSION,
    "Reconstruction-owned identity of one frozen global weighting generation."
);
weighting_identity!(
    WeightingReplayId,
    REPLAY_VERSION,
    "Identity of one complete bounded replay of a frozen weighting generation."
);
weighting_identity!(
    WeightingReplayCoverageId,
    COVERAGE_VERSION,
    "Identity of the exact weighted sample coverage emitted by one replay."
);

/// Physical choices for bounded density generation and weighted replay.
///
/// These values affect residency and block scheduling but never the frozen
/// generation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingExecutionLimits {
    max_block_samples: usize,
    density_partitions: usize,
}

impl WeightingExecutionLimits {
    /// Construct non-zero physical limits.
    pub fn new(
        max_block_samples: usize,
        density_partitions: usize,
    ) -> Result<Self, WeightingError> {
        if max_block_samples == 0 || density_partitions == 0 {
            return Err(WeightingError::ZeroExecutionLimit);
        }
        Ok(Self {
            max_block_samples,
            density_partitions,
        })
    }

    /// Maximum simultaneously retained selected samples.
    #[must_use]
    pub const fn max_block_samples(self) -> usize {
        self.max_block_samples
    }

    /// Number of deterministic density partials.
    #[must_use]
    pub const fn density_partitions(self) -> usize {
        self.density_partitions
    }
}

/// Complete byte projection for weighting-owned resident state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingResidency {
    density_grid_bytes: usize,
    robust_factor_bytes: usize,
    sum_weight_bytes: usize,
    deterministic_partial_bytes: usize,
    reduction_scratch_bytes: usize,
    replay_read_bytes: usize,
    weighted_block_bytes: usize,
    simultaneous_selected_weighted_bytes: usize,
    peak_bytes: usize,
}

impl WeightingResidency {
    /// Frozen density-grid bytes.
    #[must_use]
    pub const fn density_grid_bytes(self) -> usize {
        self.density_grid_bytes
    }

    /// Frozen robust-factor bytes.
    #[must_use]
    pub const fn robust_factor_bytes(self) -> usize {
        self.robust_factor_bytes
    }

    /// Frozen sum-weight bytes.
    #[must_use]
    pub const fn sum_weight_bytes(self) -> usize {
        self.sum_weight_bytes
    }

    /// Deterministic worker-partial bytes.
    #[must_use]
    pub const fn deterministic_partial_bytes(self) -> usize {
        self.deterministic_partial_bytes
    }

    /// Final-reduction scratch bytes.
    #[must_use]
    pub const fn reduction_scratch_bytes(self) -> usize {
        self.reduction_scratch_bytes
    }

    /// One bounded replay-read envelope block.
    #[must_use]
    pub const fn replay_read_bytes(self) -> usize {
        self.replay_read_bytes
    }

    /// One bounded weighted output block.
    #[must_use]
    pub const fn weighted_block_bytes(self) -> usize {
        self.weighted_block_bytes
    }

    /// Replay input and output blocks simultaneously retained at handoff.
    #[must_use]
    pub const fn simultaneous_selected_weighted_bytes(self) -> usize {
        self.simultaneous_selected_weighted_bytes
    }

    /// Conservative total weighting-owner residency.
    #[must_use]
    pub const fn peak_bytes(self) -> usize {
        self.peak_bytes
    }
}

/// Planned weighting work, bound to one compiled problem and commitment.
#[derive(Clone, Debug)]
pub struct WeightingPlan {
    problem: CompiledProblemId,
    commitment: WeightingCommitmentId,
    limits: WeightingExecutionLimits,
    grid: DensityGridShape,
    planned: WeightingResidency,
}

impl WeightingPlan {
    /// Return physical execution limits.
    #[must_use]
    pub const fn limits(&self) -> WeightingExecutionLimits {
        self.limits
    }

    /// Return the conservative planned byte projection.
    #[must_use]
    pub const fn planned_residency(&self) -> WeightingResidency {
        self.planned
    }

    /// Return the compiler-owned logical weighting commitment.
    #[must_use]
    pub const fn commitment_id(&self) -> WeightingCommitmentId {
        self.commitment
    }
}

/// Plan bounded weighting work for one compiler commitment.
pub fn plan_weighting(
    problem: &CompiledProblem,
    limits: WeightingExecutionLimits,
) -> Result<WeightingPlan, WeightingError> {
    let grid = density_grid_shape(problem)?;
    let cells = if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
        0
    } else {
        grid.cell_count()?
    };
    let density_grid_bytes = cells
        .checked_mul(size_of::<f64>())
        .ok_or(WeightingError::ResidencyOverflow)?;
    let robust_factor_bytes = grid
        .planes
        .checked_mul(size_of::<f64>())
        .ok_or(WeightingError::ResidencyOverflow)?;
    let sum_weight_bytes = robust_factor_bytes;
    let exact_cell_bytes = F32_EXPONENT_BINS
        .checked_add(1)
        .and_then(|entries| entries.checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES))
        .ok_or(WeightingError::ResidencyOverflow)?;
    let deterministic_partial_bytes = cells
        .checked_mul(exact_cell_bytes)
        .and_then(|bytes| bytes.checked_mul(limits.density_partitions))
        .and_then(|bytes| {
            limits
                .density_partitions
                .checked_mul(size_of::<DensityPartial>())
                .and_then(|containers| bytes.checked_add(containers))
        })
        .ok_or(WeightingError::ResidencyOverflow)?;
    let density_reduction_bytes = cells
        .checked_mul(exact_cell_bytes)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let sum_weight_reduction_bytes = F64_EXPONENT_BINS
        .checked_add(1)
        .and_then(|entries| entries.checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES))
        .and_then(|bytes| bytes.checked_mul(grid.planes))
        .ok_or(WeightingError::ResidencyOverflow)?;
    let reduction_scratch_bytes = density_reduction_bytes
        .checked_add(sum_weight_reduction_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<BTreeMap<usize, ExactF32Sum>>()))
        .ok_or(WeightingError::ResidencyOverflow)?;
    let replay_sample_bytes = size_of::<WeightingReplayInputSample>();
    let replay_read_bytes = limits
        .max_block_samples
        .checked_mul(replay_sample_bytes)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let weighted_block_bytes = limits
        .max_block_samples
        .checked_mul(size_of::<WeightingSampleValue>())
        .ok_or(WeightingError::ResidencyOverflow)?;
    let simultaneous_selected_weighted_bytes = replay_read_bytes
        .checked_add(weighted_block_bytes)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let peak_bytes = density_grid_bytes
        .checked_add(robust_factor_bytes)
        .and_then(|bytes| bytes.checked_add(sum_weight_bytes))
        .and_then(|bytes| bytes.checked_add(deterministic_partial_bytes))
        .and_then(|bytes| bytes.checked_add(reduction_scratch_bytes))
        .and_then(|bytes| bytes.checked_add(simultaneous_selected_weighted_bytes))
        .ok_or(WeightingError::ResidencyOverflow)?;
    Ok(WeightingPlan {
        problem: problem.problem_id(),
        commitment: problem.weighting().commitment_id(),
        limits,
        grid,
        planned: WeightingResidency {
            density_grid_bytes,
            robust_factor_bytes,
            sum_weight_bytes,
            deterministic_partial_bytes,
            reduction_scratch_bytes,
            replay_read_bytes,
            weighted_block_bytes,
            simultaneous_selected_weighted_bytes,
            peak_bytes,
        },
    })
}

/// Reconstruction-owned algorithm state prepared by bounded callback phases.
///
/// This value is deliberately neither traversal-completion evidence nor an
/// externally consumable frozen generation. Runtime must combine it with two
/// attempt-bound T17 completions before branding downstream replay blocks.
#[derive(Debug)]
pub struct WeightingAlgorithmState {
    generation_id: WeightingGenerationId,
    problem: CompiledProblemId,
    commitment: WeightingCommitmentId,
    sample_count: u64,
    grid: DensityGridShape,
    density: Box<[f64]>,
    robust_f2: Box<[f64]>,
    sum_weights: Box<[f64]>,
    frequency_range_hz: Option<[f64; 2]>,
    planned_residency: WeightingResidency,
    generation_residency: WeightingResidency,
    next_replay: AtomicU64,
}

impl WeightingAlgorithmState {
    pub(crate) fn matches_problem(&self, problem: &CompiledProblem) -> bool {
        self.problem == problem.problem_id()
            && self.commitment == problem.weighting().commitment_id()
    }

    /// Return the reconstruction algorithm identity for runtime authorization.
    #[must_use]
    pub const fn generation_id(&self) -> WeightingGenerationId {
        self.generation_id
    }

    /// Return the compiler commitment completed by this generation.
    #[must_use]
    pub const fn commitment_id(&self) -> WeightingCommitmentId {
        self.commitment
    }

    /// Return the exhaustive selected-sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return frozen sum weights in global or output-channel order.
    #[must_use]
    pub const fn sum_weights(&self) -> &[f64] {
        &self.sum_weights
    }

    /// Return the conservative planned byte projection.
    #[must_use]
    pub const fn planned_residency(&self) -> WeightingResidency {
        self.planned_residency
    }

    /// Return the generation pass's actual sparse-state receipt.
    #[must_use]
    pub const fn generation_residency(&self) -> WeightingResidency {
        self.generation_residency
    }

    /// Return the maximum selected samples in one planned replay block.
    #[must_use]
    pub const fn max_replay_block_samples(&self) -> usize {
        self.planned_residency.weighted_block_bytes / std::mem::size_of::<WeightingSampleValue>()
    }

    /// Begin one bounded weighted replay callback phase.
    pub fn begin_replay<'a>(
        &'a self,
        problem: &'a CompiledProblem,
        plan: &WeightingPlan,
    ) -> Result<WeightingReplayPhase<'a>, WeightingError> {
        self.validate_binding(problem, plan)?;
        let replay_sequence = self.next_replay.fetch_add(1, Ordering::Relaxed);
        if replay_sequence == u64::MAX {
            return Err(WeightingError::ReplayIdentityExhausted);
        }
        let input = Vec::with_capacity(plan.limits.max_block_samples);
        let block = Vec::with_capacity(plan.limits.max_block_samples);
        let peak_weighted_capacity = block.capacity();
        Ok(WeightingReplayPhase {
            generation: self,
            problem,
            max_block_samples: plan.limits.max_block_samples,
            input,
            block,
            peak_weighted_capacity,
            block_sequence: 0,
            coverage: CoverageEncoder::new(),
            sample_count: 0,
            replay_sequence,
        })
    }

    fn validate_binding(
        &self,
        problem: &CompiledProblem,
        plan: &WeightingPlan,
    ) -> Result<(), WeightingError> {
        if self.problem != problem.problem_id()
            || self.problem != plan.problem
            || self.commitment != problem.weighting().commitment_id()
            || self.commitment != plan.commitment
            || self.grid != plan.grid
        {
            return Err(WeightingError::ProblemMismatch);
        }
        Ok(())
    }

    fn weight(
        &self,
        problem: &CompiledProblem,
        sample: &SelectedObservationSample,
        contribution: Option<SelectedSpectralContribution>,
    ) -> Result<f64, WeightingError> {
        weight_from_state(
            problem,
            self.grid,
            &self.density,
            &self.robust_f2,
            self.frequency_range_hz,
            sample,
            contribution,
        )
    }
}

fn weight_from_state(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    density: &[f64],
    robust_f2: &[f64],
    frequency_range_hz: Option<[f64; 2]>,
    sample: &SelectedObservationSample,
    contribution: Option<SelectedSpectralContribution>,
) -> Result<f64, WeightingError> {
    let input = input_weight(problem, sample)?;
    if input == 0.0 {
        return Ok(0.0);
    }
    let (plane, uv) = weighting_coordinate(problem, grid, sample, contribution)?;
    let weighted = match problem.weighting().scheme() {
        WeightingScheme::Natural => input,
        WeightingScheme::Uniform => {
            let Some(cell) = density_lookup_cell(problem, grid, plane, uv) else {
                return Ok(0.0);
            };
            let cell_density = density[cell];
            if cell_density > 0.0 {
                input / cell_density
            } else {
                0.0
            }
        }
        WeightingScheme::Briggs { .. } => {
            let Some(cell) = density_lookup_cell(problem, grid, plane, uv) else {
                return Ok(0.0);
            };
            let cell_density = density[cell];
            input / (cell_density * robust_f2[plane] + 1.0)
        }
        WeightingScheme::BriggsBandwidthTaper { .. } => {
            let Some(cell) = density_lookup_cell(problem, grid, plane, uv) else {
                return Ok(0.0);
            };
            let cell_density = density[cell];
            let factor = bandwidth_taper_factor(grid, frequency_range_hz, uv);
            input / (cell_density * robust_f2[plane] / factor + 1.0)
        }
    };
    let weighted = weighted * gaussian_taper(problem.weighting().uv_taper(), uv);
    if weighted.is_finite() && weighted >= 0.0 {
        Ok(weighted)
    } else {
        Err(WeightingError::GeneratedNonFiniteWeight)
    }
}

/// Begin reconstruction-owned accumulation for a global density pass.
pub fn begin_weighting_generation(
    problem: &CompiledProblem,
    plan: &WeightingPlan,
) -> Result<WeightingDensityPhase, WeightingError> {
    if plan.problem != problem.problem_id()
        || plan.commitment != problem.weighting().commitment_id()
        || plan.grid != density_grid_shape(problem)?
    {
        return Err(WeightingError::ProblemMismatch);
    }
    Ok(WeightingDensityPhase {
        problem: problem.problem_id(),
        commitment: plan.commitment,
        grid: plan.grid,
        planned_residency: plan.planned,
        partials: (0..plan.limits.density_partitions)
            .map(|_| DensityPartial::default())
            .collect(),
        ordinal: 0,
        frequency_range_hz: None,
    })
}

/// Begin the sole weighted payload pass for natural weighting.
///
/// Natural weighting has no density dependency, so this phase computes final
/// weights, exact sum weights, weighted coverage, and bounded replay blocks in
/// the same traversal.
pub fn begin_natural_weighting_stream(
    problem: &CompiledProblem,
    plan: &WeightingPlan,
) -> Result<FusedWeightingPhase, WeightingError> {
    if !matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
        return Err(WeightingError::ProblemMismatch);
    }
    let density = begin_weighting_generation(problem, plan)?;
    Ok(density.finish(problem)?.into_fused(false, plan))
}

/// Mutable reconstruction state for the bounded density callback pass.
pub struct WeightingDensityPhase {
    problem: CompiledProblemId,
    commitment: WeightingCommitmentId,
    grid: DensityGridShape,
    planned_residency: WeightingResidency,
    partials: Vec<DensityPartial>,
    ordinal: u64,
    frequency_range_hz: Option<[f64; 2]>,
}

impl WeightingDensityPhase {
    /// Consume one sample delivered by the storage owner's T17 traversal.
    pub fn consume(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<(), WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        for contribution in contributions.iter() {
            extend_frequency_range(
                &mut self.frequency_range_hz,
                contribution.evaluation_frequency_hz(),
            );
        }
        let input = input_weight(problem, &sample)?;
        if input > 0.0 && !matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
            let partial_index = usize::try_from(
                self.ordinal
                    % u64::try_from(self.partials.len())
                        .map_err(|_| WeightingError::ResidencyOverflow)?,
            )
            .map_err(|_| WeightingError::ResidencyOverflow)?;
            match problem.weighting().density_scope() {
                WeightDensityScope::NotApplicable => {}
                WeightDensityScope::GlobalSelection => {
                    if let Some(contribution) = contributions.iter().next() {
                        let (_, uv) =
                            weighting_coordinate(problem, self.grid, &sample, Some(contribution))?;
                        add_density_sample(
                            problem,
                            self.grid,
                            &mut self.partials[partial_index],
                            0,
                            uv,
                            input,
                        )?;
                    }
                }
                WeightDensityScope::PerOutputChannel => {
                    for contribution in contributions.iter() {
                        let (plane, uv) =
                            weighting_coordinate(problem, self.grid, &sample, Some(contribution))?;
                        add_density_sample(
                            problem,
                            self.grid,
                            &mut self.partials[partial_index],
                            plane,
                            uv,
                            input * contribution.factor(),
                        )?;
                    }
                }
            }
        }
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or(WeightingError::SampleCountOverflow)?;
        Ok(())
    }

    /// Finish local density reduction and begin the sum-weight callback phase.
    pub fn finish(
        self,
        problem: &CompiledProblem,
    ) -> Result<WeightingSumWeightPhase, WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let deterministic_partial_bytes = self
            .partials
            .iter()
            .try_fold(0_usize, |total, partial| {
                total.checked_add(partial.resident_bytes()?)
            })
            .ok_or(WeightingError::ResidencyOverflow)?;
        let mut density_state = BTreeMap::<usize, ExactF32Sum>::new();
        for partial in self.partials {
            for (cell, sum) in partial.cells {
                density_state.entry(cell).or_default().merge(sum)?;
            }
        }
        let density_cells = if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
            0
        } else {
            self.grid.cell_count()?
        };
        let mut density = vec![0.0; density_cells];
        for (cell, sum) in &density_state {
            density[*cell] = sum.value();
        }
        let density = density.into_boxed_slice();
        let robust_f2 = robust_factors(problem, self.grid, &density);
        Ok(WeightingSumWeightPhase {
            problem: self.problem,
            commitment: self.commitment,
            grid: self.grid,
            density,
            density_state,
            robust_f2,
            frequency_range_hz: self.frequency_range_hz,
            planned_residency: self.planned_residency,
            deterministic_partial_bytes,
            density_sample_count: self.ordinal,
            sum_weights: (0..self.grid.planes)
                .map(|_| ExactF64Sum::default())
                .collect(),
            sum_sample_count: 0,
        })
    }

    /// Finish the density prepass and begin the terminal weighted payload pass.
    pub fn finish_into_stream(
        self,
        problem: &CompiledProblem,
        plan: &WeightingPlan,
    ) -> Result<FusedWeightingPhase, WeightingError> {
        if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
            return Err(WeightingError::ProblemMismatch);
        }
        Ok(self.finish(problem)?.into_fused(true, plan))
    }
}

/// Mutable reconstruction state for the bounded global sum-weight callback pass.
pub struct WeightingSumWeightPhase {
    problem: CompiledProblemId,
    commitment: WeightingCommitmentId,
    grid: DensityGridShape,
    density: Box<[f64]>,
    density_state: BTreeMap<usize, ExactF32Sum>,
    robust_f2: Box<[f64]>,
    frequency_range_hz: Option<[f64; 2]>,
    planned_residency: WeightingResidency,
    deterministic_partial_bytes: usize,
    density_sample_count: u64,
    sum_weights: Vec<ExactF64Sum>,
    sum_sample_count: u64,
}

impl WeightingSumWeightPhase {
    /// Consume one sample delivered by the storage owner's second T17 traversal.
    pub fn consume(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<(), WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let _ = self.weighted_sample(problem, sample, contributions)?;
        Ok(())
    }

    fn weighted_sample(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<WeightingSampleValue, WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let spectral_values = contributions
            .iter()
            .map(|contribution| {
                Ok(WeightingSpectralValue {
                    contribution,
                    imaging_weight: weight_from_state(
                        problem,
                        self.grid,
                        &self.density,
                        &self.robust_f2,
                        self.frequency_range_hz,
                        &sample,
                        Some(contribution),
                    )?,
                })
            })
            .collect::<Result<SmallVec<[_; 4]>, WeightingError>>()?;
        match problem.weighting().density_scope() {
            WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
                if let Some(value) = spectral_values.first() {
                    self.sum_weights[0].add(value.imaging_weight)?;
                }
            }
            WeightDensityScope::PerOutputChannel => {
                for value in &spectral_values {
                    let plane = contribution_plane(self.grid, value.contribution)?;
                    self.sum_weights[plane]
                        .add(value.imaging_weight * value.contribution.factor())?;
                }
            }
        }
        self.sum_sample_count = self
            .sum_sample_count
            .checked_add(1)
            .ok_or(WeightingError::SampleCountOverflow)?;
        Ok(WeightingSampleValue {
            sample,
            spectral_values,
        })
    }

    fn into_fused(self, density_prepass: bool, plan: &WeightingPlan) -> FusedWeightingPhase {
        FusedWeightingPhase {
            sum: self,
            density_prepass,
            block: Vec::with_capacity(plan.limits.max_block_samples),
            max_block_samples: plan.limits.max_block_samples,
            peak_weighted_capacity: plan.limits.max_block_samples,
            block_sequence: 0,
            coverage: CoverageEncoder::new(),
        }
    }

    /// Finalize algorithmic state; this is not traversal-completion evidence.
    pub fn finish(self) -> Result<WeightingAlgorithmState, WeightingError> {
        if self.density_sample_count != self.sum_sample_count {
            return Err(WeightingError::SelectedGenerationMismatch);
        }
        let sample_count = self.density_sample_count;
        let exact_sum_weight_bytes =
            exact_f64_state_resident_bytes(&self.sum_weights, self.sum_weights.capacity())
                .ok_or(WeightingError::ResidencyOverflow)?;
        let sum_weights = self
            .sum_weights
            .iter()
            .map(ExactF64Sum::value)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let generation_id = generation_identity(
            self.commitment,
            sample_count,
            self.grid,
            &self.density_state,
            &self.robust_f2,
            &sum_weights,
        );
        let density_grid_bytes = self
            .density
            .len()
            .checked_mul(size_of::<f64>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let reduction_scratch_bytes = exact_state_resident_bytes(&self.density_state)
            .and_then(|bytes| bytes.checked_add(exact_sum_weight_bytes))
            .ok_or(WeightingError::ResidencyOverflow)?;
        let robust_factor_bytes = self
            .robust_f2
            .len()
            .checked_mul(size_of::<f64>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let sum_weight_bytes = sum_weights
            .len()
            .checked_mul(size_of::<f64>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let peak_bytes = density_grid_bytes
            .checked_add(robust_factor_bytes)
            .and_then(|bytes| bytes.checked_add(sum_weight_bytes))
            .and_then(|bytes| bytes.checked_add(self.deterministic_partial_bytes))
            .and_then(|bytes| bytes.checked_add(reduction_scratch_bytes))
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok(WeightingAlgorithmState {
            generation_id,
            problem: self.problem,
            commitment: self.commitment,
            sample_count,
            grid: self.grid,
            density: self.density,
            robust_f2: self.robust_f2,
            sum_weights,
            frequency_range_hz: self.frequency_range_hz,
            planned_residency: self.planned_residency,
            generation_residency: WeightingResidency {
                density_grid_bytes,
                robust_factor_bytes,
                sum_weight_bytes,
                deterministic_partial_bytes: self.deterministic_partial_bytes,
                reduction_scratch_bytes,
                replay_read_bytes: 0,
                weighted_block_bytes: 0,
                simultaneous_selected_weighted_bytes: 0,
                peak_bytes,
            },
            next_replay: AtomicU64::new(0),
        })
    }
}

/// Terminal streaming phase that fuses exact sum-weight generation with the
/// complete weighted replay consumed by the major-cycle operator.
pub struct FusedWeightingPhase {
    sum: WeightingSumWeightPhase,
    density_prepass: bool,
    block: Vec<WeightingSampleValue>,
    max_block_samples: usize,
    peak_weighted_capacity: usize,
    block_sequence: u64,
    coverage: CoverageEncoder,
}

impl FusedWeightingPhase {
    /// Consume one selected payload sample and return a full bounded block.
    pub fn consume(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<WeightingReplayChunk>, WeightingError> {
        let weighted = self.sum.weighted_sample(problem, sample, contributions)?;
        self.coverage.push(&weighted);
        self.block.push(weighted);
        if self.block.len() == self.max_block_samples {
            self.take_block().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Finish the fused stream and late-bind generation, coverage, and replay.
    pub fn finish(
        mut self,
    ) -> Result<
        (
            Option<WeightingReplayChunk>,
            WeightingAlgorithmState,
            WeightingReplaySummary,
        ),
        WeightingError,
    > {
        let final_block = if self.block.is_empty() {
            None
        } else {
            Some(self.take_block()?)
        };
        if !self.density_prepass {
            self.sum.density_sample_count = self.sum.sum_sample_count;
        }
        let sample_count = self.sum.sum_sample_count;
        let block_count = self.block_sequence;
        let state = self.sum.finish()?;
        state.next_replay.store(1, Ordering::Relaxed);
        let coverage = self.coverage.finish(state.generation_id, sample_count);
        let replay_id =
            replay_identity(state.generation_id, coverage, sample_count, block_count, 0);
        let weighted_block_bytes = self
            .peak_weighted_capacity
            .checked_mul(size_of::<WeightingSampleValue>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let peak_bytes = state
            .generation_residency
            .peak_bytes
            .checked_add(weighted_block_bytes)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let summary = WeightingReplaySummary {
            replay_id,
            generation: state.generation_id,
            coverage,
            sample_count,
            block_count,
            replay_sequence: 0,
            residency: WeightingResidency {
                density_grid_bytes: state.generation_residency.density_grid_bytes,
                robust_factor_bytes: state.generation_residency.robust_factor_bytes,
                sum_weight_bytes: state.generation_residency.sum_weight_bytes,
                deterministic_partial_bytes: state.generation_residency.deterministic_partial_bytes,
                reduction_scratch_bytes: state.generation_residency.reduction_scratch_bytes,
                replay_read_bytes: 0,
                weighted_block_bytes,
                simultaneous_selected_weighted_bytes: weighted_block_bytes,
                peak_bytes,
            },
        };
        Ok((final_block, state, summary))
    }

    fn take_block(&mut self) -> Result<WeightingReplayChunk, WeightingError> {
        let sequence = self.block_sequence;
        self.block_sequence = self
            .block_sequence
            .checked_add(1)
            .ok_or(WeightingError::BlockCountOverflow)?;
        self.peak_weighted_capacity = self.peak_weighted_capacity.max(self.block.capacity());
        Ok(WeightingReplayChunk {
            sequence,
            samples: std::mem::take(&mut self.block),
        })
    }
}

/// One output-channel contribution carrying its reconstruction-owned W value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightingSpectralValue {
    contribution: SelectedSpectralContribution,
    imaging_weight: f64,
}

impl WeightingSpectralValue {
    /// Return the owner-reported output-channel contribution.
    #[must_use]
    pub const fn contribution(self) -> SelectedSpectralContribution {
        self.contribution
    }

    /// Return the final non-negative diagonal metric value.
    #[must_use]
    pub const fn imaging_weight(self) -> f64 {
        self.imaging_weight
    }

    /// Apply W to a complex scalar represented as `[real, imaginary]`.
    #[must_use]
    pub fn apply_metric(self, value: [f64; 2]) -> [f64; 2] {
        [
            value[0] * self.imaging_weight,
            value[1] * self.imaging_weight,
        ]
    }
}

/// One unbranded weighted selected sample produced by reconstruction.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingSampleValue {
    sample: SelectedObservationSample,
    spectral_values: SmallVec<[WeightingSpectralValue; 4]>,
}

impl WeightingSampleValue {
    /// Return the validated selected sample.
    #[must_use]
    pub const fn selected(&self) -> &SelectedObservationSample {
        &self.sample
    }

    /// Iterate over output-channel contributions and their W values.
    pub fn spectral_values(&self) -> impl Iterator<Item = WeightingSpectralValue> + '_ {
        self.spectral_values.iter().copied()
    }
}

/// One bounded, ordered, unbranded algorithm replay chunk.
///
/// Only runtime can turn this value into an externally consumable weighted
/// block by combining it with attempt-bound T17 completion evidence.
#[derive(Debug)]
pub struct WeightingReplayChunk {
    sequence: u64,
    samples: Vec<WeightingSampleValue>,
}

#[derive(Debug, Clone)]
struct WeightingReplayInputSample {
    sample: SelectedObservationSample,
    contributions: SelectedSpectralContributions,
}

impl WeightingReplayChunk {
    /// Return the zero-based replay block sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Return weighted samples in canonical selected-observation order.
    #[must_use]
    pub fn samples(&self) -> &[WeightingSampleValue] {
        &self.samples
    }

    /// Transfer the bounded sample buffer to the runtime authorization layer.
    #[must_use]
    pub fn into_samples(self) -> Vec<WeightingSampleValue> {
        self.samples
    }
}

/// Mutable reconstruction state for one bounded replay callback pass.
pub struct WeightingReplayPhase<'a> {
    generation: &'a WeightingAlgorithmState,
    problem: &'a CompiledProblem,
    max_block_samples: usize,
    input: Vec<WeightingReplayInputSample>,
    block: Vec<WeightingSampleValue>,
    peak_weighted_capacity: usize,
    block_sequence: u64,
    coverage: CoverageEncoder,
    sample_count: u64,
    replay_sequence: u64,
}

impl WeightingReplayPhase<'_> {
    /// Consume one T17 sample and return a full bounded block when ready.
    pub fn consume(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<WeightingReplayChunk>, WeightingError> {
        if self.generation.problem != problem.problem_id()
            || self.problem.problem_id() != problem.problem_id()
            || self.generation.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        self.input.push(WeightingReplayInputSample {
            sample,
            contributions,
        });
        if self.input.len() == self.max_block_samples {
            self.take_input_block().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Finish local replay state; this is not traversal-completion evidence.
    pub fn finish(
        mut self,
    ) -> Result<(Option<WeightingReplayChunk>, WeightingReplaySummary), WeightingError> {
        let final_block = if self.input.is_empty() {
            None
        } else {
            Some(self.take_input_block()?)
        };
        if self.sample_count != self.generation.sample_count {
            return Err(WeightingError::SelectedGenerationMismatch);
        }
        let coverage = self
            .coverage
            .finish(self.generation.generation_id, self.sample_count);
        let replay_id = replay_identity(
            self.generation.generation_id,
            coverage,
            self.sample_count,
            self.block_sequence,
            self.replay_sequence,
        );
        let weighted_block_bytes = self
            .peak_weighted_capacity
            .checked_mul(size_of::<WeightingSampleValue>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let replay_read_bytes = self
            .input
            .capacity()
            .checked_mul(size_of::<WeightingReplayInputSample>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let simultaneous_selected_weighted_bytes = replay_read_bytes
            .checked_add(weighted_block_bytes)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let peak_bytes = self
            .generation
            .generation_residency
            .density_grid_bytes
            .checked_add(self.generation.generation_residency.robust_factor_bytes)
            .and_then(|bytes| {
                bytes.checked_add(self.generation.generation_residency.sum_weight_bytes)
            })
            .and_then(|bytes| bytes.checked_add(simultaneous_selected_weighted_bytes))
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok((
            final_block,
            WeightingReplaySummary {
                replay_id,
                generation: self.generation.generation_id,
                coverage,
                sample_count: self.sample_count,
                block_count: self.block_sequence,
                replay_sequence: self.replay_sequence,
                residency: WeightingResidency {
                    density_grid_bytes: self.generation.generation_residency.density_grid_bytes,
                    robust_factor_bytes: self.generation.generation_residency.robust_factor_bytes,
                    sum_weight_bytes: self.generation.generation_residency.sum_weight_bytes,
                    deterministic_partial_bytes: 0,
                    reduction_scratch_bytes: 0,
                    replay_read_bytes,
                    weighted_block_bytes,
                    simultaneous_selected_weighted_bytes,
                    peak_bytes,
                },
            },
        ))
    }

    fn take_input_block(&mut self) -> Result<WeightingReplayChunk, WeightingError> {
        if self.block.capacity() == 0 {
            self.block = Vec::with_capacity(self.max_block_samples);
        }
        self.peak_weighted_capacity = self.peak_weighted_capacity.max(self.block.capacity());
        for input in &self.input {
            let spectral_values = input
                .contributions
                .iter()
                .map(|contribution| {
                    Ok(WeightingSpectralValue {
                        contribution,
                        imaging_weight: self.generation.weight(
                            self.problem,
                            &input.sample,
                            Some(contribution),
                        )?,
                    })
                })
                .collect::<Result<SmallVec<[_; 4]>, WeightingError>>()?;
            let weighted = WeightingSampleValue {
                sample: input.sample,
                spectral_values,
            };
            self.coverage.push(&weighted);
            self.sample_count = self
                .sample_count
                .checked_add(1)
                .ok_or(WeightingError::SampleCountOverflow)?;
            self.block.push(weighted);
        }
        self.input.clear();
        let sequence = self.block_sequence;
        self.block_sequence = self
            .block_sequence
            .checked_add(1)
            .ok_or(WeightingError::BlockCountOverflow)?;
        // The runtime consumes each returned block synchronously. Transfer the
        // original full-capacity allocation with its logical length intact;
        // shrinking a partial terminal block could transiently allocate a copy.
        let samples = std::mem::take(&mut self.block);
        Ok(WeightingReplayChunk { sequence, samples })
    }
}

/// Reconstruction result for a replay callback phase, not T17 completion evidence.
#[derive(Debug)]
pub struct WeightingReplaySummary {
    replay_id: WeightingReplayId,
    generation: WeightingGenerationId,
    coverage: WeightingReplayCoverageId,
    sample_count: u64,
    block_count: u64,
    replay_sequence: u64,
    residency: WeightingResidency,
}

impl WeightingReplaySummary {
    /// Return the replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.replay_id
    }

    /// Return the frozen W replayed by every block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.generation
    }

    /// Return exact emitted weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.coverage
    }

    /// Return emitted sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return emitted block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Return this generation's unique replay sequence.
    #[must_use]
    pub const fn replay_sequence(&self) -> u64 {
        self.replay_sequence
    }

    /// Return actual bounded replay residency.
    #[must_use]
    pub const fn residency(&self) -> WeightingResidency {
        self.residency
    }
}

/// Weighting failure independent of source or consumer I/O.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WeightingError {
    /// Reconstruction could not compile the paired sparse spectral stencil.
    #[error(transparent)]
    SpectralStencil(#[from] crate::SpectralStencilError),
    /// A physical execution limit was zero.
    #[error("weighting execution limits must be non-zero")]
    ZeroExecutionLimit,
    /// A byte or sample-count projection overflowed.
    #[error("weighting residency cannot be represented")]
    ResidencyOverflow,
    /// The compiled geometry has no canonical main domain.
    #[error("weighting requires one compiled main image domain")]
    MissingMainDomain,
    /// A plan or generation belongs to another compiled problem.
    #[error("weighting plan or generation belongs to another compiled problem")]
    ProblemMismatch,
    /// An evaluated frequency cannot be assigned to the compiled density scope.
    #[error("selected sample is outside the compiled output-channel density scope")]
    OutputChannelMismatch,
    /// The two global passes or a replay visited different selected content.
    #[error("weighting passes do not bind the same selected-observation generation")]
    SelectedGenerationMismatch,
    /// A generated weight was negative or non-finite.
    #[error("weighting generated a negative or non-finite metric value")]
    GeneratedNonFiniteWeight,
    /// Exact reduction state exceeded its bounded integer domain.
    #[error("exact deterministic weighting reduction overflowed")]
    ExactReductionOverflow,
    /// The selected sample count exceeded u64.
    #[error("weighting sample count overflowed")]
    SampleCountOverflow,
    /// The emitted block count exceeded u64.
    #[error("weighting replay block count overflowed")]
    BlockCountOverflow,
    /// The affine replay identity domain was exhausted.
    #[error("weighting replay identity exhausted")]
    ReplayIdentityExhausted,
    /// Emitted coverage differed from the inspected pass.
    #[error("weighting replay emitted incomplete coverage")]
    CoverageMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DensityGridShape {
    width: usize,
    height: usize,
    planes: usize,
    increment_rad: [u64; 2],
}

impl DensityGridShape {
    fn cell_count(self) -> Result<usize, WeightingError> {
        self.width
            .checked_mul(self.height)
            .and_then(|cells| cells.checked_mul(self.planes))
            .ok_or(WeightingError::ResidencyOverflow)
    }

    fn increments(self) -> [f64; 2] {
        [
            f64::from_bits(self.increment_rad[0]),
            f64::from_bits(self.increment_rad[1]),
        ]
    }
}

fn density_grid_shape(problem: &CompiledProblem) -> Result<DensityGridShape, WeightingError> {
    let domain = problem
        .geometry()
        .domains()
        .iter()
        .find(|domain| matches!(domain.role(), ImageDomainRole::Main))
        .ok_or(WeightingError::MissingMainDomain)?;
    let [width, height] = domain.shape().pixels();
    let increments = domain.direction().increment_rad();
    let planes = match problem.weighting().density_scope() {
        WeightDensityScope::PerOutputChannel => problem.geometry().spectral().output_channels(),
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => 1,
    };
    Ok(DensityGridShape {
        width,
        height,
        planes,
        increment_rad: [increments[0].to_bits(), increments[1].to_bits()],
    })
}

fn density_build_cell(
    problem: &CompiledProblem,
    shape: DensityGridShape,
    plane: usize,
    uv: [f64; 2],
) -> Option<usize> {
    match problem.weighting().density_scope() {
        WeightDensityScope::PerOutputChannel => cube_density_build_cell(shape, plane, uv),
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
            standard_density_cell(shape, plane, uv)
        }
    }
}

fn density_lookup_cell(
    problem: &CompiledProblem,
    shape: DensityGridShape,
    plane: usize,
    uv: [f64; 2],
) -> Option<usize> {
    match problem.weighting().density_scope() {
        WeightDensityScope::PerOutputChannel => cube_density_lookup_cell(shape, plane, uv),
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
            standard_density_cell(shape, plane, uv)
        }
    }
}

fn standard_density_cell(shape: DensityGridShape, plane: usize, uv: [f64; 2]) -> Option<usize> {
    let increments = shape.increments();
    // CASA VisImagingWeight stores UV coordinates and scale in Float before
    // truncating the density-cell coordinate toward zero.
    let width = shape.width as f32;
    let height = shape.height as f32;
    let x = ((uv[0] as f32) * width * (increments[0] as f32) + width / 2.0) as isize;
    let y = ((uv[1] as f32) * height * (increments[1] as f32) + height / 2.0) as isize;
    density_cell_index(shape, plane, x, y)
}

fn cube_density_build_cell(shape: DensityGridShape, plane: usize, uv: [f64; 2]) -> Option<usize> {
    let increments = shape.increments();
    let width = shape.width as f64;
    let height = shape.height as f64;
    let x = (uv[0] * width * increments[0] + width / 2.0 + 1.0).round() as isize - 1;
    let y = (-uv[1] * height * increments[1] + height / 2.0 + 1.0).round() as isize - 1;
    density_cell_index(shape, plane, x, y)
}

fn cube_density_lookup_cell(shape: DensityGridShape, plane: usize, uv: [f64; 2]) -> Option<usize> {
    let increments = shape.increments();
    let width = shape.width as f32;
    let height = shape.height as f32;
    let x = ((uv[0] as f32) * width * (increments[0] as f32) + width / 2.0).round() as isize;
    let y = (-(uv[1] as f32) * height * (increments[1] as f32) + height / 2.0).round() as isize;
    density_cell_index(shape, plane, x, y)
}

fn density_cell_index(shape: DensityGridShape, plane: usize, x: isize, y: isize) -> Option<usize> {
    if x <= 0 || y <= 0 || x >= shape.width as isize || y >= shape.height as isize {
        return None;
    }
    plane
        .checked_mul(shape.width.checked_mul(shape.height)?)?
        .checked_add((y as usize).checked_mul(shape.width)?)?
        .checked_add(x as usize)
}

fn add_density_sample(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    partial: &mut DensityPartial,
    plane: usize,
    uv: [f64; 2],
    input: f64,
) -> Result<(), WeightingError> {
    if let Some(cell) = density_build_cell(problem, grid, plane, uv) {
        partial.add(cell, input as f32)?;
        if let Some(conjugate) = density_build_cell(problem, grid, plane, [-uv[0], -uv[1]]) {
            partial.add(conjugate, input as f32)?;
        }
    }
    Ok(())
}

fn weighting_coordinate(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    sample: &SelectedObservationSample,
    contribution: Option<SelectedSpectralContribution>,
) -> Result<(usize, [f64; 2]), WeightingError> {
    match problem.weighting().density_scope() {
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
            let contribution = contribution.ok_or(WeightingError::OutputChannelMismatch)?;
            Ok((0, uv_lambda(sample, contribution.evaluation_frequency_hz())))
        }
        WeightDensityScope::PerOutputChannel => {
            let contribution = contribution.ok_or(WeightingError::OutputChannelMismatch)?;
            let plane = contribution_plane(grid, contribution)?;
            Ok((
                plane,
                uv_lambda(sample, contribution.evaluation_frequency_hz()),
            ))
        }
    }
}

fn contribution_plane(
    grid: DensityGridShape,
    contribution: SelectedSpectralContribution,
) -> Result<usize, WeightingError> {
    let plane = usize::try_from(contribution.output_channel())
        .map_err(|_| WeightingError::OutputChannelMismatch)?;
    (plane < grid.planes)
        .then_some(plane)
        .ok_or(WeightingError::OutputChannelMismatch)
}

fn uv_lambda(sample: &SelectedObservationSample, frequency_hz: f64) -> [f64; 2] {
    let scale = f64::from((frequency_hz / SPEED_OF_LIGHT_M_PER_S) as f32);
    [
        f64::from((sample.coordinates.density_uvw_m[0] * scale) as f32),
        f64::from((sample.coordinates.density_uvw_m[1] * scale) as f32),
    ]
}

fn input_weight(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
) -> Result<f64, WeightingError> {
    if sample.parallel_hand_group_flag || sample.row_flag {
        return Ok(0.0);
    }
    let weight = f64::from(sample.input_weight);
    if weight.is_finite() {
        return Ok(weight.max(0.0));
    }
    match problem.numerics().finite_values() {
        FiniteValuePolicy::FlagInputRejectGenerated => Ok(0.0),
        FiniteValuePolicy::RejectAll => Err(WeightingError::GeneratedNonFiniteWeight),
    }
}

fn gaussian_taper(taper: Option<UvTaper>, uv: [f64; 2]) -> f64 {
    let Some(taper) = taper else {
        return 1.0;
    };
    let sine = taper.position_angle_rad().sin();
    let cosine = taper.position_angle_rad().cos();
    let rotated_u = sine * uv[0] + cosine * uv[1];
    let rotated_v = -cosine * uv[0] + sine * uv[1];
    let major = std::f64::consts::LN_2 / taper.major_lambda().powi(2);
    let minor = std::f64::consts::LN_2 / taper.minor_lambda().powi(2);
    (-major * rotated_u.powi(2) - minor * rotated_v.powi(2)).exp()
}

fn bandwidth_taper_factor(grid: DensityGridShape, range: Option<[f64; 2]>, uv: [f64; 2]) -> f64 {
    let Some([minimum, maximum]) = range else {
        return 1.0;
    };
    let midpoint = 0.5 * (minimum + maximum);
    if midpoint <= 0.0 {
        return 1.0;
    }
    let fractional_bandwidth = (maximum - minimum) / midpoint;
    let increment = grid.increments();
    let u_cells = uv[0] * grid.width as f64 * increment[0];
    let v_cells = uv[1] * grid.height as f64 * increment[1];
    let n_cells = fractional_bandwidth * u_cells.hypot(v_cells);
    let mut factor = n_cells + 0.5;
    if factor < 1.5 {
        factor = (4.0 - n_cells) / (4.0 - 2.0 * n_cells);
    }
    factor.max(f64::MIN_POSITIVE)
}

fn extend_frequency_range(range: &mut Option<[f64; 2]>, frequency: f64) {
    *range = Some(match *range {
        None => [frequency, frequency],
        Some([minimum, maximum]) => [minimum.min(frequency), maximum.max(frequency)],
    });
}

fn robust_factors(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    density: &[f64],
) -> Box<[f64]> {
    let robust = match problem.weighting().scheme() {
        WeightingScheme::Briggs { robust } | WeightingScheme::BriggsBandwidthTaper { robust } => {
            Some(robust)
        }
        WeightingScheme::Natural | WeightingScheme::Uniform => None,
    };
    let mut factors = vec![0.0; grid.planes];
    let Some(robust) = robust else {
        return factors.into_boxed_slice();
    };
    let cells_per_plane = grid.width * grid.height;
    for (plane, factor) in factors.iter_mut().enumerate() {
        let start = plane * cells_per_plane;
        let end = start + cells_per_plane;
        let mut density_sum = 0.0;
        let mut density_square_sum = 0.0;
        for value in &density[start..end] {
            density_sum += *value;
            density_square_sum += value * value;
        }
        *factor = if density_sum > 0.0 && density_square_sum > 0.0 {
            (5.0 * 10_f64.powf(-robust)).powi(2) / (density_square_sum / density_sum)
        } else {
            0.0
        };
    }
    factors.into_boxed_slice()
}

#[derive(Default)]
struct DensityPartial {
    cells: BTreeMap<usize, ExactF32Sum>,
}

impl DensityPartial {
    fn add(&mut self, cell: usize, value: f32) -> Result<(), WeightingError> {
        self.cells.entry(cell).or_default().add(value)
    }

    fn resident_bytes(&self) -> Option<usize> {
        exact_state_resident_bytes(&self.cells)
    }
}

fn exact_state_resident_bytes(state: &BTreeMap<usize, ExactF32Sum>) -> Option<usize> {
    state
        .values()
        .try_fold(size_of::<BTreeMap<usize, ExactF32Sum>>(), |total, sum| {
            let entries = sum.bins.len().checked_add(1)?;
            total.checked_add(entries.checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES)?)
        })
}

fn exact_f64_state_resident_bytes(state: &[ExactF64Sum], capacity: usize) -> Option<usize> {
    state.iter().try_fold(
        capacity.checked_mul(size_of::<ExactF64Sum>())?,
        |total, sum| total.checked_add(sum.bins.len().checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES)?),
    )
}

#[derive(Debug, Default)]
struct ExactF32Sum {
    bins: BTreeMap<i16, u128>,
}

impl ExactF32Sum {
    fn add(&mut self, value: f32) -> Result<(), WeightingError> {
        if value == 0.0 {
            return Ok(());
        }
        let bits = value.to_bits();
        let exponent = ((bits >> 23) & 0xff) as i16;
        let fraction = bits & 0x7f_ffff;
        let (mantissa, power) = if exponent == 0 {
            (u128::from(fraction), -149)
        } else {
            (u128::from((1 << 23) | fraction), exponent - 127 - 23)
        };
        let bin = self.bins.entry(power).or_default();
        *bin = bin
            .checked_add(mantissa)
            .ok_or(WeightingError::ExactReductionOverflow)?;
        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<(), WeightingError> {
        for (power, mantissa) in other.bins {
            let bin = self.bins.entry(power).or_default();
            *bin = bin
                .checked_add(mantissa)
                .ok_or(WeightingError::ExactReductionOverflow)?;
        }
        Ok(())
    }

    fn value(&self) -> f64 {
        self.bins
            .iter()
            .map(|(power, mantissa)| (*mantissa as f64) * 2_f64.powi(i32::from(*power)))
            .sum()
    }
}

#[derive(Default)]
struct ExactF64Sum {
    bins: BTreeMap<i16, u128>,
}

impl ExactF64Sum {
    fn add(&mut self, value: f64) -> Result<(), WeightingError> {
        if value == 0.0 {
            return Ok(());
        }
        if !value.is_finite() || value < 0.0 {
            return Err(WeightingError::GeneratedNonFiniteWeight);
        }
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as i16;
        let fraction = bits & 0x000f_ffff_ffff_ffff;
        let (mantissa, power) = if exponent == 0 {
            (u128::from(fraction), -1074)
        } else {
            (u128::from((1_u64 << 52) | fraction), exponent - 1023 - 52)
        };
        let bin = self.bins.entry(power).or_default();
        *bin = bin
            .checked_add(mantissa)
            .ok_or(WeightingError::ExactReductionOverflow)?;
        Ok(())
    }

    fn value(&self) -> f64 {
        self.bins
            .iter()
            .map(|(power, mantissa)| (*mantissa as f64) * 2_f64.powi(i32::from(*power)))
            .sum()
    }
}

fn generation_identity(
    commitment: WeightingCommitmentId,
    sample_count: u64,
    grid: DensityGridShape,
    density: &BTreeMap<usize, ExactF32Sum>,
    robust_f2: &[f64],
    sum_weights: &[f64],
) -> WeightingGenerationId {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update(GENERATION_VERSION.to_be_bytes());
    hasher.update(commitment.as_bytes());
    hasher.update(sample_count.to_be_bytes());
    hash_usize(&mut hasher, grid.width);
    hash_usize(&mut hasher, grid.height);
    hash_usize(&mut hasher, grid.planes);
    hash_usize(&mut hasher, density.len());
    for (cell, sum) in density {
        hash_usize(&mut hasher, *cell);
        hash_usize(&mut hasher, sum.bins.len());
        for (power, mantissa) in &sum.bins {
            hasher.update(power.to_be_bytes());
            hasher.update(mantissa.to_be_bytes());
        }
    }
    for factor in robust_f2 {
        hasher.update(factor.to_bits().to_be_bytes());
    }
    for weight in sum_weights {
        hasher.update(weight.to_bits().to_be_bytes());
    }
    WeightingGenerationId(LogicalIdentity::from_sha256(hasher.finalize().into()))
}

fn hash_usize(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u128).to_be_bytes());
}

#[derive(Debug)]
pub(super) struct CoverageEncoder(Sha256);

impl CoverageEncoder {
    pub(super) fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(COVERAGE_DOMAIN);
        hasher.update((COVERAGE_VERSION + 1).to_be_bytes());
        Self(hasher)
    }

    pub(super) fn push(&mut self, weighted: &WeightingSampleValue) {
        let sample = weighted.selected();
        self.0
            .update(sample.address.measurement_set.identity().as_bytes());
        self.0.update(sample.address.physical_row.to_be_bytes());
        self.0
            .update(sample.address.data_description_id.to_be_bytes());
        self.0
            .update(sample.address.spectral_window_id.to_be_bytes());
        self.0.update(sample.address.channel_index.to_be_bytes());
        self.0
            .update(sample.address.correlation_index.to_be_bytes());
        let mut count = 0_u8;
        for value in weighted.spectral_values() {
            count += 1;
            self.0
                .update(value.contribution.output_channel().to_be_bytes());
            self.0
                .update(value.contribution.factor().to_bits().to_be_bytes());
            self.0.update(
                value
                    .contribution
                    .evaluation_frequency_hz()
                    .to_bits()
                    .to_be_bytes(),
            );
            self.0.update(value.imaging_weight.to_bits().to_be_bytes());
        }
        self.0.update([count]);
    }

    pub(super) fn finish(
        mut self,
        generation: WeightingGenerationId,
        sample_count: u64,
    ) -> WeightingReplayCoverageId {
        self.0.update(sample_count.to_be_bytes());
        let content = self.0.finalize();
        let mut hasher = Sha256::new();
        hasher.update(COVERAGE_DOMAIN);
        hasher.update((COVERAGE_VERSION + 1).to_be_bytes());
        hasher.update(generation.as_bytes());
        hasher.update(content);
        WeightingReplayCoverageId(LogicalIdentity::from_sha256(hasher.finalize().into()))
    }
}

fn replay_identity(
    generation: WeightingGenerationId,
    coverage: WeightingReplayCoverageId,
    sample_count: u64,
    block_count: u64,
    replay_sequence: u64,
) -> WeightingReplayId {
    let mut hasher = Sha256::new();
    hasher.update(REPLAY_DOMAIN);
    hasher.update(REPLAY_VERSION.to_be_bytes());
    hasher.update(generation.as_bytes());
    hasher.update(coverage.as_bytes());
    hasher.update(sample_count.to_be_bytes());
    hasher.update(block_count.to_be_bytes());
    hasher.update(replay_sequence.to_be_bytes());
    WeightingReplayId(LogicalIdentity::from_sha256(hasher.finalize().into()))
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
