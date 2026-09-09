// SPDX-License-Identifier: LGPL-3.0-or-later

//! Frozen global weighting generations and bounded weighted replay.

mod spectral_cache;
pub use spectral_cache::WeightingSpectralCache;

use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, fmt, mem::size_of};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, ContinuumTransformGenerationId, FiniteValuePolicy,
    ImageDomainRole, LogicalIdentity, SelectedAntennaResponses, SelectedImageDomainProjections,
    SelectedInputWeightGroup, SelectedObservationGenerationId, SelectedObservationSampleView,
    SelectedPointingDirections, SelectedSampleAddress, SelectedSpectralContribution,
    SelectedSpectralContributions, SelectedVisibilitySample, UvTaper, WeightDensityScope,
    WeightingCommitmentId, WeightingScheme,
};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

use crate::spectral_sampling::{
    CasaLinearOutputGrid, CasaLinearRowCursor, CasaLinearSample, NativeRowSpectralGeometry,
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const GENERATION_DOMAIN: &[u8] = b"casa-rs-frozen-weighting-generation";
const GENERATION_VERSION: u32 = 2;
const REPLAY_DOMAIN: &[u8] = b"casa-rs-weighting-replay";
const REPLAY_VERSION: u32 = 1;
const COVERAGE_DOMAIN: &[u8] = b"casa-rs-weighting-replay-coverage";
const COVERAGE_HASH_CHUNK_BYTES: usize = 256;
const COVERAGE_VERSION: u32 = 3;
const F32_MINIMUM_POWER: i16 = -149;
const F32_SUPERACCUMULATOR_LIMBS: usize = 6;
const CONSERVATIVE_TREE_ENTRY_BYTES: usize = 64;
const F64_EXPONENT_BINS: usize = 2_046;
const DENSITY_ACCUMULATOR_DOMAIN: &[u8] = b"casa-rs-exact-density-accumulator";
const DENSITY_ACCUMULATOR_VERSION: u32 = 1;

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

#[cfg(test)]
pub(crate) fn native_normal_fixture_weighting_ids() -> (
    WeightingGenerationId,
    WeightingReplayId,
    WeightingReplayCoverageId,
) {
    (
        WeightingGenerationId(LogicalIdentity::from_sha256([1; 32])),
        WeightingReplayId(LogicalIdentity::from_sha256([2; 32])),
        WeightingReplayCoverageId(LogicalIdentity::from_sha256([3; 32])),
    )
}

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
    density_layout_bytes: usize,
    density_grid_bytes: usize,
    robust_factor_bytes: usize,
    sum_weight_bytes: usize,
    shared_density_accumulator_bytes: usize,
    sum_weight_accumulator_bytes: usize,
    replay_read_bytes: usize,
    weighted_block_bytes: usize,
    weighted_sample_bytes: usize,
    simultaneous_selected_weighted_bytes: usize,
    spectral_cache_bytes: usize,
    peak_bytes: usize,
}

pub(crate) fn maximum_spectral_terms(problem: &CompiledProblem) -> usize {
    use casa_imaging_model::{ReconstructionBasis, SpectralKernel};
    match problem.reconstruction().basis() {
        ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => 1,
        ReconstructionBasis::TaylorViaChannelMajor { .. }
        | ReconstructionBasis::ChannelLocal { .. }
        | ReconstructionBasis::JointContinuumLine { .. } => {
            match problem.science().spectral().sampling().kernel() {
                SpectralKernel::Identity | SpectralKernel::Nearest => 1,
                SpectralKernel::Linear => 2,
                SpectralKernel::Cubic => 4,
                SpectralKernel::ChannelIntegration { maximum_terms } => maximum_terms,
            }
        }
    }
}

// SmallVec's collect, clone and push paths round spilled capacity to a power of two.
pub(crate) fn smallvec_heap_bytes<T>(maximum_len: usize) -> Result<usize, WeightingError> {
    if maximum_len <= 4 {
        return Ok(0);
    }
    maximum_len
        .checked_next_power_of_two()
        .and_then(|capacity| capacity.checked_mul(size_of::<T>()))
        .ok_or(WeightingError::ResidencyOverflow)
}

pub(crate) fn native_row_heap_bytes(
    maximum_correlations: usize,
    maximum_terms: usize,
) -> Result<usize, WeightingError> {
    let spectral = smallvec_heap_bytes::<WeightingSpectralValue>(maximum_terms)?
        .checked_mul(maximum_correlations)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let native_samples = smallvec_heap_bytes::<WeightingSampleValue>(maximum_correlations)?;
    // A growth step may hold the new backing and its preceding power-of-two capacity.
    let sample_growth_overlap =
        smallvec_heap_bytes::<WeightingSampleValue>(maximum_correlations.div_ceil(2))?;
    let observed = smallvec_heap_bytes::<num_complex::Complex64>(maximum_correlations)?;
    let correlations =
        smallvec_heap_bytes::<casa_imaging_model::CorrelationType>(maximum_correlations)?;
    let weights = smallvec_heap_bytes::<f64>(maximum_correlations)?;
    let flags = smallvec_heap_bytes::<bool>(maximum_correlations)?;
    native_samples
        .checked_add(sample_growth_overlap)
        .and_then(|bytes| bytes.checked_add(observed.checked_mul(3)?))
        .and_then(|bytes| bytes.checked_add(spectral))
        .and_then(|bytes| bytes.checked_add(correlations))
        .and_then(|bytes| bytes.checked_add(weights))
        .and_then(|bytes| bytes.checked_add(flags))
        .ok_or(WeightingError::ResidencyOverflow)
}

fn validate_spectral_contribution_capacity(
    problem: &CompiledProblem,
    contributions: &SelectedSpectralContributions,
) -> Result<(), WeightingError> {
    let maximum = maximum_spectral_terms(problem);
    if contributions.len() > maximum {
        return Err(WeightingError::SpectralContributionCapacity {
            actual: contributions.len(),
            maximum,
        });
    }
    Ok(())
}

impl WeightingResidency {
    /// Frozen cube-density geometry and transfer-law metadata.
    #[must_use]
    pub const fn density_layout_bytes(self) -> usize {
        self.density_layout_bytes
    }

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

    /// Shared exact density-accumulator bytes.
    #[must_use]
    pub const fn shared_density_accumulator_bytes(self) -> usize {
        self.shared_density_accumulator_bytes
    }

    /// Exact sum-weight accumulator bytes.
    #[must_use]
    pub const fn sum_weight_accumulator_bytes(self) -> usize {
        self.sum_weight_accumulator_bytes
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

    /// Return the bounded source-channel stencil cache and miss workspace.
    #[must_use]
    pub const fn spectral_cache_bytes(self) -> usize {
        self.spectral_cache_bytes
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
    let density_layout_bytes = if grid.cube_output.is_some() {
        size_of::<DensityGridShape>()
    } else {
        0
    };
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
    let sum_weight_bytes = grid
        .output_planes
        .checked_mul(size_of::<f64>())
        .ok_or(WeightingError::ResidencyOverflow)?;
    let shared_density_accumulator_bytes = cells
        .checked_mul(F32_SUPERACCUMULATOR_LIMBS)
        .and_then(|limbs| limbs.checked_mul(size_of::<u64>()))
        .and_then(|bytes| bytes.checked_add(size_of::<ExactF32Grid>()))
        .and_then(|bytes| {
            bytes.checked_add(if grid.cube_output.is_some() {
                exact_sum_capacity_bytes(grid.planes)?.checked_add(size_of::<CubeWeightRows>())?
            } else {
                0
            })
        })
        .ok_or(WeightingError::ResidencyOverflow)?;
    let sum_weight_accumulator_bytes = exact_sum_capacity_bytes(grid.output_planes)
        .and_then(|bytes| {
            bytes.checked_add(if grid.cube_output.is_some() {
                size_of::<CubeWeightRows>()
            } else {
                0
            })
        })
        .ok_or(WeightingError::ResidencyOverflow)?;
    let replay_read_bytes = 0;
    let weighted_sample_bytes = size_of::<WeightingSampleValue>()
        .checked_add(smallvec_heap_bytes::<WeightingSpectralValue>(
            maximum_spectral_terms(problem),
        )?)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let weighted_block_bytes = limits
        .max_block_samples
        .checked_mul(weighted_sample_bytes)
        .ok_or(WeightingError::ResidencyOverflow)?;
    let simultaneous_selected_weighted_bytes = weighted_block_bytes;
    let spectral_cache_bytes = WeightingSpectralCache::planned_bytes(problem)?;
    let peak_bytes = density_grid_bytes
        .checked_add(density_layout_bytes)
        .and_then(|bytes| bytes.checked_add(robust_factor_bytes))
        .and_then(|bytes| bytes.checked_add(sum_weight_bytes))
        .and_then(|bytes| bytes.checked_add(shared_density_accumulator_bytes))
        .and_then(|bytes| bytes.checked_add(sum_weight_accumulator_bytes))
        .and_then(|bytes| bytes.checked_add(simultaneous_selected_weighted_bytes))
        .and_then(|bytes| bytes.checked_add(spectral_cache_bytes))
        .ok_or(WeightingError::ResidencyOverflow)?;
    Ok(WeightingPlan {
        problem: problem.problem_id(),
        commitment: problem.weighting().commitment_id(),
        limits,
        grid,
        planned: WeightingResidency {
            density_layout_bytes,
            density_grid_bytes,
            robust_factor_bytes,
            sum_weight_bytes,
            shared_density_accumulator_bytes,
            sum_weight_accumulator_bytes,
            replay_read_bytes,
            weighted_block_bytes,
            weighted_sample_bytes,
            simultaneous_selected_weighted_bytes,
            spectral_cache_bytes,
            peak_bytes,
        },
    })
}

fn exact_sum_capacity_bytes(planes: usize) -> Option<usize> {
    F64_EXPONENT_BINS
        .checked_add(1)?
        .checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES)?
        .checked_mul(planes)
}

#[derive(Debug, Clone, Copy)]
struct NativeCubeWeight {
    value: f64,
    flag: bool,
    uvw_m: [f64; 3],
    correlations: usize,
}

#[derive(Debug)]
struct CubeWeightRows {
    cursor: CasaLinearRowCursor,
    previous: Option<NativeCubeWeight>,
}

impl CubeWeightRows {
    fn new() -> Self {
        Self {
            cursor: CasaLinearRowCursor::new(),
            previous: None,
        }
    }

    fn push(
        &mut self,
        sample: &WeightingSelectedSample,
        value: f64,
        output: CasaLinearOutputGrid,
        mut emit: impl FnMut(
            CasaLinearSample,
            NativeCubeWeight,
            NativeCubeWeight,
        ) -> Result<(), WeightingError>,
    ) -> Result<(), WeightingError> {
        let geometry = sample
            .row_spectral_geometry
            .ok_or(WeightingError::RowSpectralGeometryMismatch)?;
        let current = NativeCubeWeight {
            value,
            flag: sample.input_weight_group_flag
                || sample.parallel_hand_group_flag
                || sample.row_flag
                || !sample.input_weight.is_finite()
                || sample.input_weight < 0.0,
            uvw_m: sample.density_uvw_m,
            correlations: sample.correlation_group_size,
        };
        if let Some(points) = self.cursor.push(
            sample.address,
            geometry,
            sample.output_frame_frequency_hz,
            output,
        )? {
            let previous = self
                .previous
                .ok_or(WeightingError::RowSpectralGeometryMismatch)?;
            for point in points {
                emit(point, previous, current)?;
            }
        }
        self.previous = Some(current);
        Ok(())
    }
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

/// Sealed invariant from the first exhaustive encoded weighting replay.
///
/// Later replay may reuse the exact coverage identity only after reconstruction
/// validates the frozen weighting, selected-observation authorization,
/// transform generation, and deterministic terminal counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrozenWeightingCoverageProof {
    problem: CompiledProblemId,
    commitment: WeightingCommitmentId,
    generation: WeightingGenerationId,
    coverage: WeightingReplayCoverageId,
    selected_generation: SelectedObservationGenerationId,
    selected_sample_count: u64,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    weighted_sample_count: u64,
}

impl FrozenWeightingCoverageProof {
    /// Seal the encoded coverage produced by the first exhaustive replay.
    pub fn seal(
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
        replay: &WeightingReplaySummary,
        selected_generation: SelectedObservationGenerationId,
        selected_sample_count: u64,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<Self, WeightingError> {
        if !weighting.matches_problem(problem)
            || replay.weighting_generation() != weighting.generation_id()
            || replay.sample_count() != weighting.sample_count()
            || selected_sample_count != weighting.sample_count()
            || replay.coverage_proof_bytes() == 0
            || replay.coverage_proof_hash_calls() == 0
        {
            return Err(WeightingError::CoverageMismatch);
        }
        Ok(Self {
            problem: problem.problem_id(),
            commitment: problem.weighting().commitment_id(),
            generation: weighting.generation_id(),
            coverage: replay.coverage(),
            selected_generation,
            selected_sample_count,
            continuum_transform_generation,
            weighted_sample_count: replay.sample_count(),
        })
    }

    pub(crate) const fn coverage(self) -> WeightingReplayCoverageId {
        self.coverage
    }

    pub(crate) const fn generation(self) -> WeightingGenerationId {
        self.generation
    }

    pub(crate) fn matches_streaming_operator(
        self,
        problem: CompiledProblemId,
        commitment: WeightingCommitmentId,
    ) -> bool {
        self.problem == problem && self.commitment == commitment
    }

    pub(crate) fn matches_operator(
        self,
        problem: CompiledProblemId,
        commitment: WeightingCommitmentId,
        generation: WeightingGenerationId,
    ) -> bool {
        self.problem == problem && self.commitment == commitment && self.generation == generation
    }

    fn validates_static(
        self,
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> bool {
        self.problem == problem.problem_id()
            && self.commitment == problem.weighting().commitment_id()
            && self.generation == weighting.generation_id()
            && self.continuum_transform_generation == continuum_transform_generation
            && self.weighted_sample_count == weighting.sample_count()
    }

    /// Validate the current rebound terminal authority and the exact derived
    /// replay summary before any downstream result may commit.
    pub fn validate_derived_replay(
        self,
        selected_generation: SelectedObservationGenerationId,
        selected_sample_count: u64,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
        replay: &WeightingReplaySummary,
    ) -> Result<(), WeightingError> {
        if self.selected_generation != selected_generation
            || self.selected_sample_count != selected_sample_count
            || self.continuum_transform_generation != continuum_transform_generation
            || replay.weighting_generation() != self.generation
            || replay.coverage() != self.coverage
            || replay.sample_count() != self.weighted_sample_count
            || replay.coverage_proof_bytes() != 0
            || replay.coverage_proof_hash_calls() != 0
        {
            return Err(WeightingError::CoverageMismatch);
        }
        Ok(())
    }
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
        self.planned_residency.weighted_block_bytes / self.planned_residency.weighted_sample_bytes
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
        let block = Vec::with_capacity(plan.limits.max_block_samples);
        let peak_weighted_capacity = block.capacity();
        let coverage = CoverageEncoder::new();
        Ok(WeightingReplayPhase {
            generation: self,
            problem,
            max_block_samples: plan.limits.max_block_samples,
            block,
            pending: None,
            peak_weighted_capacity,
            block_sequence: 0,
            previous_checkpoint: coverage.checkpoint_token(),
            coverage,
            sample_count: 0,
            replay_sequence,
        })
    }

    /// Begin a later replay whose exact coverage was sealed by the first
    /// exhaustive encoded pass and reauthorized against fresh selected state.
    pub fn begin_derived_replay<'a>(
        &'a self,
        problem: &'a CompiledProblem,
        plan: &WeightingPlan,
        proof: FrozenWeightingCoverageProof,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<WeightingReplayPhase<'a>, WeightingError> {
        self.validate_binding(problem, plan)?;
        if !proof.validates_static(problem, self, continuum_transform_generation) {
            return Err(WeightingError::CoverageMismatch);
        }
        let replay_sequence = self.next_replay.fetch_add(1, Ordering::Relaxed);
        if replay_sequence == u64::MAX {
            return Err(WeightingError::ReplayIdentityExhausted);
        }
        let block = Vec::with_capacity(plan.limits.max_block_samples);
        let peak_weighted_capacity = block.capacity();
        let coverage = CoverageEncoder::derived(proof.coverage());
        Ok(WeightingReplayPhase {
            generation: self,
            problem,
            max_block_samples: plan.limits.max_block_samples,
            block,
            pending: None,
            peak_weighted_capacity,
            block_sequence: 0,
            previous_checkpoint: coverage.checkpoint_token(),
            coverage,
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
}

fn weight_from_state(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    density: &[f64],
    robust_f2: &[f64],
    frequency_range_hz: Option<[f64; 2]>,
    sample: &WeightingSelectedSample,
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
            if grid.cube_output.is_some() && cell_density <= 0.0 {
                return Ok(0.0);
            }
            input / (cell_density * robust_f2[plane] + 1.0)
        }
        WeightingScheme::BriggsBandwidthTaper { .. } => {
            let Some(cell) = density_lookup_cell(problem, grid, plane, uv) else {
                return Ok(0.0);
            };
            let cell_density = density[cell];
            if grid.cube_output.is_some() && cell_density <= 0.0 {
                return Ok(0.0);
            }
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

fn weighted_sample_from_state(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    density: &[f64],
    robust_f2: &[f64],
    frequency_range_hz: Option<[f64; 2]>,
    sample: WeightingSelectedSample,
    contributions: SelectedSpectralContributions,
) -> Result<WeightingSampleValue, WeightingError> {
    let weight = |contribution| {
        weight_from_state(
            problem,
            grid,
            density,
            robust_f2,
            frequency_range_hz,
            &sample,
            contribution,
        )
    };
    let source_imaging_weight = if let Some(output) = grid.cube_output {
        let density_output = output
            .with_padding(grid.padding)
            .ok_or(WeightingError::ResidencyOverflow)?;
        Some(
            match density_output.nearest_channel(sample.output_frame_frequency_hz) {
                None => 0.0,
                Some(plane) => weight(Some(
                    SelectedSpectralContribution::new(
                        u32::try_from(plane).map_err(|_| WeightingError::OutputChannelMismatch)?,
                        1.0,
                        sample.address.frequency_centre_hz,
                    )
                    .ok_or(WeightingError::OutputChannelMismatch)?,
                ))?,
            },
        )
    } else if problem.weighting().density_scope() != WeightDensityScope::PerOutputChannel {
        Some(weight(Some(
            SelectedSpectralContribution::new(0, 1.0, sample.output_frame_frequency_hz)
                .ok_or(WeightingError::OutputChannelMismatch)?,
        ))?)
    } else {
        None
    };
    let spectral_values = contributions
        .iter()
        .map(|contribution| {
            Ok(WeightingSpectralValue {
                contribution,
                imaging_weight: if grid.cube_output.is_some() {
                    source_imaging_weight.ok_or(WeightingError::OutputChannelMismatch)?
                } else {
                    weight(Some(contribution))?
                },
            })
        })
        .collect::<Result<SmallVec<[_; 4]>, WeightingError>>()?;
    Ok(WeightingSampleValue {
        sample,
        source_imaging_weight,
        spectral_values,
    })
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
        density: ExactF32Grid::new(
            if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
                0
            } else {
                plan.grid.cell_count()?
            },
        )?,
        ordinal: 0,
        frequency_range_hz: plan.grid.cube_output.map(|_| {
            let spectral = problem.geometry().spectral();
            let first = spectral.channel_centre_hz(0).expect("compiled output axis");
            let last = spectral
                .channel_centre_hz(spectral.output_channels() - 1)
                .expect("compiled output axis");
            [first.min(last), first.max(last)]
        }),
        cube_rows: plan.grid.cube_output.map(|_| CubeWeightRows::new()),
        raw_sum_weights: if plan.grid.cube_output.is_some() {
            (0..plan.grid.planes)
                .map(|_| ExactF64Sum::default())
                .collect()
        } else {
            Vec::new()
        },
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
    density: ExactF32Grid,
    ordinal: u64,
    frequency_range_hz: Option<[f64; 2]>,
    cube_rows: Option<CubeWeightRows>,
    raw_sum_weights: Vec<ExactF64Sum>,
}

impl WeightingDensityPhase {
    /// Consume one sample delivered by the storage owner's T17 traversal.
    pub fn consume<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: impl Into<SelectedObservationSampleView<'a>>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> Result<(), WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let sample = sample.into();
        let density_owner = sample.input_weight_group().is_density_owner();
        let sample =
            WeightingSelectedSample::from_selected(problem, sample, output_frame_frequency_hz)?;
        for contribution in contributions
            .iter()
            .filter(|_| self.grid.cube_output.is_none())
        {
            extend_frequency_range(
                &mut self.frequency_range_hz,
                contribution.evaluation_frequency_hz(),
            );
        }
        let input = input_weight(problem, &sample)?;
        if density_owner && let Some(rows) = &mut self.cube_rows {
            let output = self
                .grid
                .cube_output
                .expect("cube rows bind output geometry");
            let density_output = output
                .with_padding(self.grid.padding)
                .ok_or(WeightingError::ResidencyOverflow)?;
            rows.push(&sample, input, density_output, |point, left, right| {
                if point.linear_flag(left.flag, right.flag) {
                    return Ok(());
                }
                let [lf, rf] = point.factors();
                let value = f64::from((left.value * lf + right.value * rf) as f32);
                let uv = uv_lambda_for_coordinates(left.uvw_m, point.frequency_hz());
                if density_build_cell(problem, self.grid, point.output_channel(), uv).is_some() {
                    add_density_sample(
                        problem,
                        self.grid,
                        &mut self.density,
                        point.output_channel(),
                        uv,
                        value,
                    )?;
                    self.raw_sum_weights[point.output_channel()].add(value)?;
                }
                Ok(())
            })?;
        } else if self.cube_rows.is_none()
            && density_owner
            && input > 0.0
            && !matches!(problem.weighting().scheme(), WeightingScheme::Natural)
        {
            match problem.weighting().density_scope() {
                WeightDensityScope::NotApplicable => {}
                WeightDensityScope::GlobalSelection => {
                    if let Some(contribution) = contributions.iter().next() {
                        let (_, uv) =
                            weighting_coordinate(problem, self.grid, &sample, Some(contribution))?;
                        add_density_sample(problem, self.grid, &mut self.density, 0, uv, input)?;
                    }
                }
                WeightDensityScope::PerOutputChannel => {
                    for contribution in contributions.iter() {
                        let (plane, uv) =
                            weighting_coordinate(problem, self.grid, &sample, Some(contribution))?;
                        add_density_sample(
                            problem,
                            self.grid,
                            &mut self.density,
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
        mut self,
        problem: &CompiledProblem,
    ) -> Result<WeightingSumWeightPhase, WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        if let Some(rows) = &mut self.cube_rows {
            rows.cursor.finish()?;
            rows.previous = None;
        }
        let shared_density_accumulator_bytes = self
            .density
            .resident_bytes()?
            .checked_add(
                exact_f64_state_resident_bytes(
                    &self.raw_sum_weights,
                    self.raw_sum_weights.capacity(),
                )
                .ok_or(WeightingError::ResidencyOverflow)?,
            )
            .and_then(|bytes| {
                bytes.checked_add(if self.cube_rows.is_some() {
                    size_of::<CubeWeightRows>()
                } else {
                    0
                })
            })
            .ok_or(WeightingError::ResidencyOverflow)?;
        let density_digest = self.density.digest();
        let density = self.density.values()?;
        let robust_f2 = robust_factors(problem, self.grid, &density, &self.raw_sum_weights);
        Ok(WeightingSumWeightPhase {
            problem: self.problem,
            commitment: self.commitment,
            grid: self.grid,
            density,
            density_digest,
            robust_f2,
            frequency_range_hz: self.frequency_range_hz,
            planned_residency: self.planned_residency,
            shared_density_accumulator_bytes,
            density_sample_count: self.ordinal,
            sum_weights: (0..self.grid.output_planes)
                .map(|_| ExactF64Sum::default())
                .collect(),
            sum_sample_count: 0,
            cube_rows: self.cube_rows,
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
    density_digest: [u8; 32],
    robust_f2: Box<[f64]>,
    frequency_range_hz: Option<[f64; 2]>,
    planned_residency: WeightingResidency,
    shared_density_accumulator_bytes: usize,
    density_sample_count: u64,
    sum_weights: Vec<ExactF64Sum>,
    sum_sample_count: u64,
    cube_rows: Option<CubeWeightRows>,
}

impl WeightingSumWeightPhase {
    /// Consume one sample delivered by the storage owner's second T17 traversal.
    pub fn consume<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: impl Into<SelectedObservationSampleView<'a>>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> Result<(), WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        let _ = self.weighted_sample(
            problem,
            sample.into(),
            output_frame_frequency_hz,
            contributions,
        )?;
        Ok(())
    }

    fn weighted_sample(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSampleView<'_>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> Result<WeightingSampleValue, WeightingError> {
        if self.problem != problem.problem_id()
            || self.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        validate_spectral_contribution_capacity(problem, &contributions)?;
        let sample =
            WeightingSelectedSample::from_selected(problem, sample, output_frame_frequency_hz)?;
        let weighted = weighted_sample_from_state(
            problem,
            self.grid,
            &self.density,
            &self.robust_f2,
            self.frequency_range_hz,
            sample,
            contributions,
        )?;
        let sample = &weighted.sample;
        let spectral_values = &weighted.spectral_values;
        if let Some(rows) = &mut self.cube_rows {
            if sample.starts_correlation_group {
                rows.push(
                    sample,
                    weighted
                        .source_imaging_weight
                        .ok_or(WeightingError::OutputChannelMismatch)?,
                    self.grid.cube_output.expect("bound cube rows"),
                    |point, left, right| {
                        if point.linear_flag(left.flag, right.flag) {
                            return Ok(());
                        }
                        let chosen = if point.nearest_is_right() {
                            right
                        } else {
                            left
                        };
                        if !chosen.flag {
                            self.sum_weights[point.output_channel()]
                                .add(chosen.value * chosen.correlations as f64)?;
                        }
                        Ok(())
                    },
                )?;
            }
        } else {
            match problem.weighting().density_scope() {
                WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
                    if let Some(value) = spectral_values.first() {
                        self.sum_weights[0].add(value.imaging_weight)?;
                    }
                }
                WeightDensityScope::PerOutputChannel => {
                    for value in spectral_values {
                        let plane = contribution_plane(self.grid, value.contribution)?;
                        self.sum_weights[plane]
                            .add(value.imaging_weight * value.contribution.factor())?;
                    }
                }
            }
        }
        self.sum_sample_count = self
            .sum_sample_count
            .checked_add(1)
            .ok_or(WeightingError::SampleCountOverflow)?;
        Ok(weighted)
    }

    fn into_fused(self, density_prepass: bool, plan: &WeightingPlan) -> FusedWeightingPhase {
        let coverage = CoverageEncoder::new();
        FusedWeightingPhase {
            sum: self,
            density_prepass,
            block: Vec::with_capacity(plan.limits.max_block_samples),
            pending: None,
            max_block_samples: plan.limits.max_block_samples,
            peak_weighted_capacity: plan.limits.max_block_samples,
            block_sequence: 0,
            previous_checkpoint: coverage.checkpoint_token(),
            coverage,
        }
    }

    /// Finalize algorithmic state; this is not traversal-completion evidence.
    pub fn finish(mut self) -> Result<WeightingAlgorithmState, WeightingError> {
        if let Some(rows) = &mut self.cube_rows {
            rows.cursor.finish()?;
        }
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
            self.density_digest,
            &self.robust_f2,
            &sum_weights,
        );
        let density_grid_bytes = self
            .density
            .len()
            .checked_mul(size_of::<f64>())
            .ok_or(WeightingError::ResidencyOverflow)?;
        let sum_weight_accumulator_bytes = exact_sum_weight_bytes
            .checked_add(if self.cube_rows.is_some() {
                size_of::<CubeWeightRows>()
            } else {
                0
            })
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
            .checked_add(self.planned_residency.density_layout_bytes)
            .and_then(|bytes| bytes.checked_add(robust_factor_bytes))
            .and_then(|bytes| bytes.checked_add(sum_weight_bytes))
            .and_then(|bytes| bytes.checked_add(self.shared_density_accumulator_bytes))
            .and_then(|bytes| bytes.checked_add(sum_weight_accumulator_bytes))
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
                density_layout_bytes: self.planned_residency.density_layout_bytes,
                density_grid_bytes,
                robust_factor_bytes,
                sum_weight_bytes,
                shared_density_accumulator_bytes: self.shared_density_accumulator_bytes,
                sum_weight_accumulator_bytes,
                replay_read_bytes: 0,
                weighted_block_bytes: 0,
                weighted_sample_bytes: self.planned_residency.weighted_sample_bytes,
                simultaneous_selected_weighted_bytes: 0,
                spectral_cache_bytes: 0,
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
    pending: Option<WeightingSampleValue>,
    max_block_samples: usize,
    peak_weighted_capacity: usize,
    block_sequence: u64,
    coverage: CoverageEncoder,
    previous_checkpoint: [u8; 32],
}

impl FusedWeightingPhase {
    /// Consume one selected payload sample and return a full bounded block.
    pub fn consume<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: impl Into<SelectedObservationSampleView<'a>>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<WeightingReplayChunk>, WeightingError> {
        let weighted = self.sum.weighted_sample(
            problem,
            sample.into(),
            output_frame_frequency_hz,
            contributions,
        )?;
        let emitted = self
            .flush_before_group(&weighted)?
            .then(|| self.take_block())
            .transpose()?;
        self.coverage.push(&weighted);
        if let Some(emitted) = emitted {
            self.pending = Some(weighted);
            Ok(Some(emitted))
        } else {
            if self.block.capacity() == 0 {
                self.block = Vec::with_capacity(self.max_block_samples);
            }
            self.block.push(weighted);
            if self.block.len() == self.max_block_samples
                && self
                    .block
                    .last()
                    .is_some_and(|value| value.sample.ends_correlation_group())
            {
                self.take_block().map(Some)
            } else {
                Ok(None)
            }
        }
    }

    /// Return one synchronously consumed full chunk to this phase for refill.
    ///
    /// The runtime adapter calls this immediately after its scientific
    /// consumer releases the borrowed chunk. Keeping the allocation in the
    /// phase prevents one full weighted-buffer allocation per emitted chunk.
    pub fn reuse_emitted_block(
        &mut self,
        mut block: WeightingReplayChunk,
    ) -> Result<(), WeightingError> {
        if !self.block.is_empty()
            || block.samples.is_empty()
            || block.samples.len() > self.max_block_samples
            || block.sequence.checked_add(1) != Some(self.block_sequence)
        {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        block.samples.clear();
        if let Some(pending) = self.pending.take() {
            block.samples.push(pending);
        }
        self.block = block.samples;
        Ok(())
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
        if self.pending.is_some() {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
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
        let (coverage, coverage_proof_work) =
            self.coverage.finish(state.generation_id, sample_count);
        let replay_id =
            replay_identity(state.generation_id, coverage, sample_count, block_count, 0);
        let weighted_block_bytes = self
            .peak_weighted_capacity
            .checked_mul(state.planned_residency.weighted_sample_bytes)
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
            coverage_proof_bytes: coverage_proof_work.bytes,
            coverage_proof_hash_calls: coverage_proof_work.hash_calls,
            residency: WeightingResidency {
                density_layout_bytes: state.generation_residency.density_layout_bytes,
                density_grid_bytes: state.generation_residency.density_grid_bytes,
                robust_factor_bytes: state.generation_residency.robust_factor_bytes,
                sum_weight_bytes: state.generation_residency.sum_weight_bytes,
                shared_density_accumulator_bytes: state
                    .generation_residency
                    .shared_density_accumulator_bytes,
                sum_weight_accumulator_bytes: state
                    .generation_residency
                    .sum_weight_accumulator_bytes,
                replay_read_bytes: 0,
                weighted_block_bytes,
                weighted_sample_bytes: state.planned_residency.weighted_sample_bytes,
                simultaneous_selected_weighted_bytes: weighted_block_bytes,
                spectral_cache_bytes: 0,
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
        Ok(WeightingReplayChunk::new(
            sequence,
            std::mem::take(&mut self.block),
            &self.coverage,
            &mut self.previous_checkpoint,
        ))
    }

    fn flush_before_group(&self, weighted: &WeightingSampleValue) -> Result<bool, WeightingError> {
        if weighted.sample.correlation_group_size() > self.max_block_samples {
            return Err(WeightingError::ResidencyOverflow);
        }
        if weighted.sample.starts_correlation_group()
            && !self.block.is_empty()
            && self
                .block
                .len()
                .checked_add(weighted.sample.correlation_group_size())
                .is_none_or(|size| size > self.max_block_samples)
        {
            Ok(true)
        } else {
            Ok(false)
        }
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

/// Compact kernel projection of one validated selected sample.
///
/// Row-level provenance used only for source validation is absent. The bounded
/// block retains the field, pointing, selected spectral geometry, coordinates,
/// flags, visibility, and address consumed by the scientific kernels.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingSelectedSample {
    pub(crate) address: SelectedSampleAddress,
    pub(crate) visibility: SelectedVisibilitySample,
    pub(crate) channel_flag: bool,
    pub(crate) parallel_hand_group_flag: bool,
    pub(crate) input_weight_group_flag: bool,
    pub(crate) row_flag: bool,
    // Reconstruction-derived CASA imaging weight, not raw per-correlation storage weight.
    pub(crate) input_weight: f32,
    pub(crate) raw_input_weight: f32,
    pub(crate) starts_correlation_group: bool,
    pub(crate) ends_correlation_group: bool,
    pub(crate) correlation_group_size: usize,
    pub(crate) parallactic_angles_rad: [f64; 2],
    pub(crate) density_uvw_m: [f64; 3],
    output_frame_frequency_hz: f64,
    row_spectral_geometry: Option<NativeRowSpectralGeometry>,
    field_id: i32,
    pointing_directions: SelectedPointingDirections,
    aw_pointing_pixel: Option<[f64; 2]>,
    antenna_responses: Option<SelectedAntennaResponses>,
    domain_projections: SelectedImageDomainProjections,
}

impl WeightingSelectedSample {
    fn from_selected(
        problem: &CompiledProblem,
        sample: SelectedObservationSampleView<'_>,
        output_frame_frequency_hz: f64,
    ) -> Result<Self, WeightingError> {
        let row_spectral_geometry = sample.row_spectral_geometry();
        if row_spectral_geometry.is_some_and(|geometry| {
            !geometry.matches_sample(sample, problem.geometry().spectral().output_frame())
                || [Some(geometry.first()), geometry.second()]
                    .into_iter()
                    .flatten()
                    .any(|(channel, frequency)| {
                        channel == sample.address().channel_index
                            && frequency.to_bits() != output_frame_frequency_hz.to_bits()
                    })
        }) {
            return Err(WeightingError::RowSpectralGeometryMismatch);
        }
        let coordinates = sample.coordinates();
        let input_weight_group = sample.input_weight_group();
        let domain_projections = sample.domain_projections().clone();
        Ok(Self {
            address: sample.address(),
            visibility: sample.visibility(),
            channel_flag: sample.channel_flag(),
            parallel_hand_group_flag: sample.parallel_hand_group_flag(),
            input_weight_group_flag: input_weight_group.imaging_flag(),
            row_flag: sample.row_flag(),
            input_weight: casa_unpolarized_input_weight(input_weight_group),
            raw_input_weight: sample.input_weight(),
            starts_correlation_group: input_weight_group.is_density_owner(),
            ends_correlation_group: input_weight_group.is_terminal_member(),
            correlation_group_size: input_weight_group.member_count(),
            parallactic_angles_rad: coordinates.parallactic_angles_rad,
            density_uvw_m: coordinates.density_uvw_m,
            output_frame_frequency_hz,
            row_spectral_geometry: row_spectral_geometry.map(|geometry| {
                NativeRowSpectralGeometry {
                    channels: geometry.selected_channels(),
                    first: geometry.first(),
                    second: geometry.second(),
                }
            }),
            field_id: sample.metadata().field_id,
            pointing_directions: coordinates.pointing_directions,
            aw_pointing_pixel: primary_aw_pointing_pixel(&domain_projections),
            antenna_responses: sample.metadata().antenna_responses,
            domain_projections,
        })
    }

    /// Return the exact selected-sample address.
    #[must_use]
    pub const fn address(&self) -> SelectedSampleAddress {
        self.address
    }

    /// Return the selected visibility in its MeasurementSet storage precision.
    #[must_use]
    pub const fn visibility(&self) -> SelectedVisibilitySample {
        self.visibility
    }

    /// Return the exact selected cell flag.
    #[must_use]
    pub const fn channel_flag(&self) -> bool {
        self.channel_flag
    }

    /// Return whether either selected parallel hand flags this Stokes-I group.
    #[must_use]
    pub const fn parallel_hand_group_flag(&self) -> bool {
        self.parallel_hand_group_flag
    }

    /// Return the exact raw per-correlation MS input weight.
    #[must_use]
    pub const fn raw_input_weight(&self) -> f32 {
        self.raw_input_weight
    }

    /// Return row-local parallactic angles for the two receptors.
    #[must_use]
    pub const fn parallactic_angles_rad(&self) -> [f64; 2] {
        self.parallactic_angles_rad
    }

    /// Return the selected MeasurementSet field identifier used for mosaic routing.
    #[must_use]
    pub const fn field_id(&self) -> i32 {
        self.field_id
    }

    /// Return the source-channel centre transformed into the compiled output frame.
    #[must_use]
    pub const fn output_frame_frequency_hz(&self) -> f64 {
        self.output_frame_frequency_hz
    }

    /// Source-issued geometry of the complete selected native-channel vector.
    #[must_use]
    pub(crate) const fn row_spectral_geometry(&self) -> Option<NativeRowSpectralGeometry> {
        self.row_spectral_geometry
    }

    /// Return the two antenna pointing directions used by the mosaic response.
    #[must_use]
    pub const fn pointing_directions(&self) -> SelectedPointingDirections {
        self.pointing_directions
    }

    /// Return the exact CASA chart-local baseline pointing pixel used by AW projection.
    #[must_use]
    pub const fn aw_pointing_pixel(&self) -> Option<[f64; 2]> {
        self.aw_pointing_pixel
    }

    /// Return the owner-derived paired aperture classes, when required.
    #[must_use]
    pub const fn antenna_responses(&self) -> Option<SelectedAntennaResponses> {
        self.antenna_responses
    }

    fn starts_correlation_group(&self) -> bool {
        self.starts_correlation_group
    }

    fn correlation_group_size(&self) -> usize {
        self.correlation_group_size
    }

    fn ends_correlation_group(&self) -> bool {
        self.ends_correlation_group
    }

    /// Return the MAIN row flag.
    #[must_use]
    pub const fn row_flag(&self) -> bool {
        self.row_flag
    }

    /// Return the transformed UVW coordinate consumed by the paired operator.
    #[must_use]
    pub fn transformed_uvw_m(&self) -> [f64; 3] {
        self.primary_model_projection().transformed_uvw_m()
    }

    /// Return the phase-shift path length consumed by prediction and gridding.
    #[must_use]
    pub fn phase_shift_m(&self) -> f64 {
        self.primary_model_projection().phase_shift_m()
    }

    /// Return the projections for every compiled image domain.
    #[must_use]
    pub const fn domain_projections(&self) -> &SelectedImageDomainProjections {
        &self.domain_projections
    }

    fn primary_model_projection(&self) -> casa_imaging_model::SelectedPhaseCentreProjection {
        self.domain_projections
            .get(0)
            .expect("validated selected samples always contain the primary domain")
            .model()
    }
}

fn primary_aw_pointing_pixel(projections: &SelectedImageDomainProjections) -> Option<[f64; 2]> {
    projections
        .get(0)
        .and_then(|projection| projection.aw_pointing_pixel())
}

#[cfg(test)]
mod selected_sample_tests {
    use casa_imaging_model::{
        SelectedImageDomainProjection, SelectedImageDomainProjections,
        SelectedPhaseCentreProjection,
    };

    use super::{NativeRowSpectralGeometry, WeightingSelectedSample, primary_aw_pointing_pixel};

    #[test]
    fn chunk_checkpoint_chain_binds_each_emitted_prefix() {
        use super::{CoverageEncoder, WeightingReplayChunk};

        let mut coverage = CoverageEncoder::new();
        let empty = coverage.checkpoint_token();
        let mut previous = empty;
        coverage.update(b"first selected prefix");
        let first = WeightingReplayChunk::new(0, Vec::new(), &coverage, &mut previous);
        assert_eq!(first.previous_checkpoint(), empty);
        assert_eq!(first.checkpoint(), coverage.checkpoint_token());
        coverage.update(b"second selected prefix");
        let second = WeightingReplayChunk::new(1, Vec::new(), &coverage, &mut previous);
        assert_eq!(second.previous_checkpoint(), first.checkpoint());
        assert_eq!(previous, second.checkpoint());

        let mut other_coverage = CoverageEncoder::new();
        let mut other_previous = other_coverage.checkpoint_token();
        other_coverage.update(b"another selected prefix");
        let other_first =
            WeightingReplayChunk::new(0, Vec::new(), &other_coverage, &mut other_previous);
        other_coverage.update(b"second selected prefix");
        let other_second =
            WeightingReplayChunk::new(1, Vec::new(), &other_coverage, &mut other_previous);
        assert_eq!(other_second.sequence(), second.sequence());
        assert_eq!(other_second.previous_checkpoint(), other_first.checkpoint());
        assert_ne!(other_second.previous_checkpoint(), first.checkpoint());
        assert_ne!(other_second.checkpoint(), second.checkpoint());
    }

    #[test]
    fn checkpoint_tokens_preserve_science_hash_state_and_separate_derived_proofs() {
        use super::{CoverageEncoder, WeightingReplayCoverageId};
        use sha2::Digest;

        let mut coverage = CoverageEncoder::new();
        coverage.update(b"selected prefix");
        let token = coverage.checkpoint_token();
        assert_eq!(coverage.checkpoint_token(), token);
        let expected: [u8; 32] = coverage.hasher.clone().unwrap().finalize().into();
        assert_eq!(token, expected);

        let proof = WeightingReplayCoverageId(super::LogicalIdentity::from_sha256(token));
        let derived = CoverageEncoder::derived(proof);
        assert_ne!(derived.checkpoint_token(), token);
        assert_ne!(
            derived.checkpoint_token(),
            CoverageEncoder::new().checkpoint_token()
        );
        let other = CoverageEncoder::derived(WeightingReplayCoverageId(
            super::LogicalIdentity::from_sha256([7; 32]),
        ));
        assert_ne!(derived.checkpoint_token(), other.checkpoint_token());
        assert_eq!(derived.work.bytes, 0);
        assert_eq!(derived.work.hash_calls, 0);
    }

    #[test]
    fn spectral_heap_bound_matches_smallvec_collection_and_clone() {
        use super::{WeightingSpectralValue, smallvec_heap_bytes};
        use smallvec::SmallVec;

        for terms in [0, 1, 4, 5, 8, 9, 17] {
            let values = (0..terms)
                .map(|channel| {
                    Ok::<_, super::WeightingError>(WeightingSpectralValue {
                        contribution: casa_imaging_model::SelectedSpectralContribution::new(
                            channel as u32,
                            1.0,
                            1.0e9,
                        )
                        .unwrap(),
                        imaging_weight: 1.0,
                    })
                })
                .collect::<Result<SmallVec<[_; 4]>, _>>()
                .unwrap();
            for values in [&values, &values.clone()] {
                let heap = if values.spilled() {
                    values.capacity() * size_of::<WeightingSpectralValue>()
                } else {
                    0
                };
                assert_eq!(
                    heap,
                    smallvec_heap_bytes::<WeightingSpectralValue>(terms).unwrap()
                );
            }
        }
        assert!(smallvec_heap_bytes::<WeightingSpectralValue>(usize::MAX).is_err());
    }

    #[test]
    fn native_row_heap_counts_one_sample_bank_and_all_observed_vectors() {
        use super::{WeightingSampleValue, WeightingSpectralValue, native_row_heap_bytes};
        assert_eq!(native_row_heap_bytes(4, 4).unwrap(), 0);
        assert_eq!(
            native_row_heap_bytes(4, 5).unwrap(),
            4 * 8 * size_of::<WeightingSpectralValue>()
        );
        let q_capacity = (0..5)
            .collect::<smallvec::SmallVec<[usize; 4]>>()
            .capacity();
        let vectors = q_capacity
            * (size_of::<WeightingSampleValue>()
                + 3 * size_of::<num_complex::Complex64>()
                + size_of::<casa_imaging_model::CorrelationType>()
                + size_of::<f64>()
                + size_of::<bool>());
        assert_eq!(native_row_heap_bytes(5, 4).unwrap(), vectors);
        assert_eq!(
            native_row_heap_bytes(9, 4).unwrap(),
            24 * size_of::<WeightingSampleValue>()
                + 16 * (3 * size_of::<num_complex::Complex64>()
                    + size_of::<casa_imaging_model::CorrelationType>()
                    + size_of::<f64>()
                    + size_of::<bool>())
        );
        assert!(native_row_heap_bytes(usize::MAX, 5).is_err());
    }

    fn native_row_sample(channel: u32, row: u64) -> super::WeightingSampleValue {
        use casa_imaging_model::{
            CorrelationType, DirectionFrame, FrequencyFrame, LogicalIdentity,
            MeasurementSetIdentity, SkyDirection,
        };
        let direction = SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5);
        let frequency = 100.0 + f64::from(channel) * 100.0;
        super::WeightingSampleValue {
            sample: WeightingSelectedSample {
                address: super::SelectedSampleAddress {
                    measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256(
                        [1; 32],
                    )),
                    physical_row: row,
                    data_description_id: 0,
                    spectral_window_id: 0,
                    channel_index: channel,
                    frequency_centre_hz: frequency,
                    frequency_lower_hz: frequency - 50.0,
                    frequency_upper_hz: frequency + 50.0,
                    channel_width_hz: 100.0,
                    frequency_frame: FrequencyFrame::Topocentric,
                    polarization_id: 0,
                    correlation_index: 0,
                    correlation_type: CorrelationType::StokesI,
                },
                visibility: super::SelectedVisibilitySample::Complex32([1.0, 0.0]),
                channel_flag: false,
                parallel_hand_group_flag: false,
                input_weight_group_flag: false,
                row_flag: false,
                input_weight: 1.0,
                raw_input_weight: 1.0,
                starts_correlation_group: true,
                ends_correlation_group: true,
                correlation_group_size: 1,
                parallactic_angles_rad: [0.0; 2],
                density_uvw_m: [0.0; 3],
                output_frame_frequency_hz: frequency,
                row_spectral_geometry: Some(NativeRowSpectralGeometry {
                    channels: 3,
                    first: (0, 100.0),
                    second: Some((1, 200.0)),
                }),
                field_id: 0,
                pointing_directions: super::SelectedPointingDirections {
                    antenna1: direction,
                    antenna2: direction,
                },
                aw_pointing_pixel: None,
                antenna_responses: None,
                domain_projections: SelectedImageDomainProjections::one_domain_with_shared_psf(
                    SelectedPhaseCentreProjection::new([0.0; 3], 0.0).unwrap(),
                ),
            },
            source_imaging_weight: Some(4.0 + f64::from(channel) * 6.0),
            spectral_values: smallvec::SmallVec::new(),
        }
    }

    #[test]
    fn native_row_retains_previous_input_and_current_input_after_callback_errors() {
        use super::{CasaLinearOutputGrid, FiniteValuePolicy};
        use crate::spectral_operator::{
            CasaLinearRowResampler, NativeSpectralGroup, SpectralOperatorError,
        };
        use num_complex::Complex64;
        let output = CasaLinearOutputGrid::compile(&[100.0, 150.0, 200.0, 250.0, 300.0]).unwrap();
        for failure in 0..3 {
            let mut rows = CasaLinearRowResampler::<usize>::new();
            let mut samples = [native_row_sample(0, 0)];
            let mut observed = [Complex64::new(10.0, 0.0)];
            rows.push(
                NativeSpectralGroup {
                    frequency_hz: 100.0,
                    samples: &samples,
                    observed: &observed,
                    predicted: 1,
                },
                output,
                FiniteValuePolicy::RejectAll,
                true,
                |_, _, _| -> Result<(), SpectralOperatorError> {
                    unreachable!("first input cannot interpolate")
                },
                |_| unreachable!("first input cannot emit"),
            )
            .unwrap();
            assert!(matches!(
                rows.finish(),
                Err(SpectralOperatorError::IncompleteCoverage)
            ));
            samples[0] = native_row_sample(1, 0);
            samples[0].sample.channel_flag = true;
            observed[0] = Complex64::new(22.0, 0.0);
            let mut emitted = Vec::new();
            let result = rows.push(
                NativeSpectralGroup {
                    frequency_hz: 200.0,
                    samples: &samples,
                    observed: &observed,
                    predicted: 2,
                },
                output,
                FiniteValuePolicy::RejectAll,
                true,
                |left, right, _| {
                    assert_eq!((*left, *right), (1, 2));
                    if failure == 1 {
                        Err(SpectralOperatorError::InvalidSample)
                    } else {
                        Ok(())
                    }
                },
                |group| {
                    if failure == 2 {
                        return Err(SpectralOperatorError::InvalidSample);
                    }
                    emitted.push((
                        group.frequency_hz,
                        group.observed[0].re,
                        group.weights[0],
                        group.flags[0],
                    ));
                    Ok(())
                },
            );
            if failure == 0 {
                result.unwrap();
                assert!(emitted.contains(&(100.0, 10.0, 4.0, false)));
                assert!(emitted.contains(&(150.0, 16.0, 4.0, true)));
            } else {
                assert!(matches!(result, Err(SpectralOperatorError::InvalidSample)));
            }
            samples[0] = native_row_sample(2, 0);
            observed[0] = Complex64::new(34.0, 0.0);
            let mut pairs = 0;
            rows.push(
                NativeSpectralGroup {
                    frequency_hz: 300.0,
                    samples: &samples,
                    observed: &observed,
                    predicted: 3,
                },
                output,
                FiniteValuePolicy::RejectAll,
                true,
                |left, right, _| {
                    assert_eq!((*left, *right), (2, 3));
                    pairs += 1;
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();
            assert!(pairs > 0);
            rows.finish().unwrap();
            samples[0] = native_row_sample(0, 1);
            rows.push(
                NativeSpectralGroup {
                    frequency_hz: 100.0,
                    samples: &samples,
                    observed: &observed,
                    predicted: 4,
                },
                output,
                FiniteValuePolicy::RejectAll,
                true,
                |_, _, _| -> Result<(), SpectralOperatorError> {
                    unreachable!("new row cannot interpolate")
                },
                |_| Ok(()),
            )
            .unwrap();
            assert!(matches!(
                rows.finish(),
                Err(SpectralOperatorError::IncompleteCoverage)
            ));
        }
    }

    #[test]
    fn bounded_replay_retains_a_compact_kernel_projection_and_only_required_spectral_values() {
        let weighted_bytes = size_of::<WeightingSelectedSample>();
        let spectral_bytes = size_of::<Option<NativeRowSpectralGeometry>>();
        let source_bytes = size_of::<casa_imaging_model::SelectedObservationSample>();
        assert!(weighted_bytes < source_bytes);
        assert!(
            (weighted_bytes - spectral_bytes) * 3 < source_bytes * 2,
            "non-spectral kernel payload must retain the original compactness bound"
        );
        assert!(
            spectral_bytes
                < size_of::<Option<casa_imaging_model::SelectedRowSpectralGeometry>>() / 2,
            "row/frame provenance must not be duplicated in every weighted sample"
        );
    }

    #[test]
    fn weighting_selects_the_main_chart_exact_aw_pointing_pixel() {
        let phase = SelectedPhaseCentreProjection::new([0.0; 3], 0.0)
            .expect("finite phase-centre projection");
        let main = SelectedImageDomainProjection::with_shared_psf(0, phase)
            .with_aw_pointing_pixel([256.25, 255.75])
            .expect("finite main pointing pixel");
        let outlier = SelectedImageDomainProjection::with_shared_psf(1, phase)
            .with_aw_pointing_pixel([17.0, 19.0])
            .expect("finite outlier pointing pixel");
        let projections = SelectedImageDomainProjections::new([main, outlier])
            .expect("canonical domain projections");

        assert_eq!(
            primary_aw_pointing_pixel(&projections),
            Some([256.25, 255.75])
        );
    }
}

fn casa_unpolarized_input_weight(group: SelectedInputWeightGroup) -> f32 {
    let (first, last) = group.endpoints();
    match last {
        Some(last) => (first + last) / 2.0_f32,
        None => first,
    }
}

/// One unbranded weighted selected sample produced by reconstruction.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingSampleValue {
    sample: WeightingSelectedSample,
    source_imaging_weight: Option<f64>,
    spectral_values: SmallVec<[WeightingSpectralValue; 4]>,
}

impl WeightingSampleValue {
    /// Return the validated selected sample.
    #[must_use]
    pub const fn selected(&self) -> &WeightingSelectedSample {
        &self.sample
    }

    /// Iterate over output-channel contributions and their W values.
    pub fn spectral_values(&self) -> impl Iterator<Item = WeightingSpectralValue> + '_ {
        self.spectral_values.iter().copied()
    }

    /// Return the native imaging-weight scalar. For bound CASA cube weighting,
    /// this uses the native channel's density plane, independently of prediction support.
    #[must_use]
    pub const fn source_imaging_weight(&self) -> Option<f64> {
        self.source_imaging_weight
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
    coverage: CoverageEncoder,
    previous_checkpoint: [u8; 32],
    checkpoint: [u8; 32],
}

impl WeightingReplayChunk {
    fn new(
        sequence: u64,
        samples: Vec<WeightingSampleValue>,
        coverage: &CoverageEncoder,
        previous_checkpoint: &mut [u8; 32],
    ) -> Self {
        let checkpoint = coverage.checkpoint_token();
        Self {
            sequence,
            samples,
            coverage: coverage.clone(),
            previous_checkpoint: std::mem::replace(previous_checkpoint, checkpoint),
            checkpoint,
        }
    }

    pub(super) const fn previous_checkpoint(&self) -> [u8; 32] {
        self.previous_checkpoint
    }

    pub(super) const fn checkpoint(&self) -> [u8; 32] {
        self.checkpoint
    }

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

    /// Iterate complete row/channel correlation groups in canonical source order.
    pub fn correlation_groups(&self) -> impl Iterator<Item = &[WeightingSampleValue]> {
        let mut start = 0_usize;
        std::iter::from_fn(move || {
            if start == self.samples.len() {
                return None;
            }
            let end = self.samples[start + 1..]
                .iter()
                .position(|sample| sample.sample.starts_correlation_group())
                .map_or(self.samples.len(), |offset| start + 1 + offset);
            let group = &self.samples[start..end];
            start = end;
            Some(group)
        })
    }

    pub(super) const fn coverage_checkpoint(&self) -> &CoverageEncoder {
        &self.coverage
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
    block: Vec<WeightingSampleValue>,
    pending: Option<WeightingSampleValue>,
    peak_weighted_capacity: usize,
    block_sequence: u64,
    coverage: CoverageEncoder,
    previous_checkpoint: [u8; 32],
    sample_count: u64,
    replay_sequence: u64,
}

impl WeightingReplayPhase<'_> {
    /// Consume one T17 sample and return a full bounded block when ready.
    pub fn consume<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: impl Into<SelectedObservationSampleView<'a>>,
        output_frame_frequency_hz: f64,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<WeightingReplayChunk>, WeightingError> {
        if self.generation.problem != problem.problem_id()
            || self.problem.problem_id() != problem.problem_id()
            || self.generation.commitment != problem.weighting().commitment_id()
        {
            return Err(WeightingError::ProblemMismatch);
        }
        validate_spectral_contribution_capacity(problem, &contributions)?;
        let sample = WeightingSelectedSample::from_selected(
            problem,
            sample.into(),
            output_frame_frequency_hz,
        )?;
        let weighted = weighted_sample_from_state(
            problem,
            self.generation.grid,
            &self.generation.density,
            &self.generation.robust_f2,
            self.generation.frequency_range_hz,
            sample,
            contributions,
        )?;
        let emitted = self
            .flush_before_group(&weighted)?
            .then(|| self.take_block())
            .transpose()?;
        self.coverage.push(&weighted);
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .ok_or(WeightingError::SampleCountOverflow)?;
        if let Some(emitted) = emitted {
            self.pending = Some(weighted);
            Ok(Some(emitted))
        } else {
            if self.block.capacity() == 0 {
                self.block = Vec::with_capacity(self.max_block_samples);
            }
            self.block.push(weighted);
            if self.block.len() == self.max_block_samples
                && self
                    .block
                    .last()
                    .is_some_and(|value| value.sample.ends_correlation_group())
            {
                self.take_block().map(Some)
            } else {
                Ok(None)
            }
        }
    }

    /// Return one synchronously consumed full chunk to this phase for refill.
    pub fn reuse_emitted_block(
        &mut self,
        mut block: WeightingReplayChunk,
    ) -> Result<(), WeightingError> {
        if !self.block.is_empty()
            || block.samples.is_empty()
            || block.samples.len() > self.max_block_samples
            || block.sequence.checked_add(1) != Some(self.block_sequence)
        {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        block.samples.clear();
        if let Some(pending) = self.pending.take() {
            block.samples.push(pending);
        }
        self.block = block.samples;
        Ok(())
    }

    /// Finish local replay state; this is not traversal-completion evidence.
    pub fn finish(
        mut self,
    ) -> Result<(Option<WeightingReplayChunk>, WeightingReplaySummary), WeightingError> {
        if self.pending.is_some() {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        let final_block = if self.block.is_empty() {
            None
        } else {
            Some(self.take_block()?)
        };
        if self.sample_count != self.generation.sample_count {
            return Err(WeightingError::SelectedGenerationMismatch);
        }
        let (coverage, coverage_proof_work) = self
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
            .checked_mul(self.generation.planned_residency.weighted_sample_bytes)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let replay_read_bytes = 0;
        let simultaneous_selected_weighted_bytes = weighted_block_bytes;
        let peak_bytes = self
            .generation
            .generation_residency
            .density_grid_bytes
            .checked_add(self.generation.generation_residency.density_layout_bytes)
            .and_then(|bytes| {
                bytes.checked_add(self.generation.generation_residency.robust_factor_bytes)
            })
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
                coverage_proof_bytes: coverage_proof_work.bytes,
                coverage_proof_hash_calls: coverage_proof_work.hash_calls,
                residency: WeightingResidency {
                    density_layout_bytes: self.generation.generation_residency.density_layout_bytes,
                    density_grid_bytes: self.generation.generation_residency.density_grid_bytes,
                    robust_factor_bytes: self.generation.generation_residency.robust_factor_bytes,
                    sum_weight_bytes: self.generation.generation_residency.sum_weight_bytes,
                    shared_density_accumulator_bytes: 0,
                    sum_weight_accumulator_bytes: 0,
                    replay_read_bytes,
                    weighted_block_bytes,
                    weighted_sample_bytes: self.generation.planned_residency.weighted_sample_bytes,
                    simultaneous_selected_weighted_bytes,
                    spectral_cache_bytes: 0,
                    peak_bytes,
                },
            },
        ))
    }

    fn take_block(&mut self) -> Result<WeightingReplayChunk, WeightingError> {
        self.peak_weighted_capacity = self.peak_weighted_capacity.max(self.block.capacity());
        let sequence = self.block_sequence;
        self.block_sequence = self
            .block_sequence
            .checked_add(1)
            .ok_or(WeightingError::BlockCountOverflow)?;
        // The runtime consumes each returned block synchronously. Transfer the
        // original full-capacity allocation with its logical length intact;
        // shrinking a partial terminal block could transiently allocate a copy.
        let samples = std::mem::take(&mut self.block);
        Ok(WeightingReplayChunk::new(
            sequence,
            samples,
            &self.coverage,
            &mut self.previous_checkpoint,
        ))
    }

    fn flush_before_group(&self, weighted: &WeightingSampleValue) -> Result<bool, WeightingError> {
        if weighted.sample.correlation_group_size() > self.max_block_samples {
            return Err(WeightingError::ResidencyOverflow);
        }
        if weighted.sample.starts_correlation_group()
            && !self.block.is_empty()
            && self
                .block
                .len()
                .checked_add(weighted.sample.correlation_group_size())
                .is_none_or(|size| size > self.max_block_samples)
        {
            Ok(true)
        } else {
            Ok(false)
        }
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
    coverage_proof_bytes: u64,
    coverage_proof_hash_calls: u64,
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

    /// Return bytes handed to coverage identity hashers during this replay.
    #[must_use]
    pub const fn coverage_proof_bytes(&self) -> u64 {
        self.coverage_proof_bytes
    }

    /// Return coverage identity hasher update calls during this replay.
    #[must_use]
    pub const fn coverage_proof_hash_calls(&self) -> u64 {
        self.coverage_proof_hash_calls
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
    /// A supplied stencil exceeds the canonical compiled producer's bound.
    #[error("spectral contribution count {actual} exceeds compiled capacity {maximum}")]
    SpectralContributionCapacity {
        /// Number of contributions supplied by the caller.
        actual: usize,
        /// Maximum produced by the compiled basis and spectral kernel.
        maximum: usize,
    },
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
    /// The source-issued frequency geometry belongs to another row, frame or first pair.
    #[error("selected row spectral geometry does not match the weighted sample")]
    RowSpectralGeometryMismatch,
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
    /// A consumer returned a chunk that is not the latest full emitted block.
    #[error("returned weighting replay block does not match the active stream")]
    ReturnedBlockMismatch,
    /// The affine replay identity domain was exhausted.
    #[error("weighting replay identity exhausted")]
    ReplayIdentityExhausted,
    /// Emitted coverage differed from the inspected pass.
    #[error("weighting replay emitted incomplete coverage")]
    CoverageMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct DensityGridShape {
    width: usize,
    height: usize,
    planes: usize,
    output_planes: usize,
    padding: usize,
    cube_output: Option<CasaLinearOutputGrid>,
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
    let output_planes = match problem.weighting().density_scope() {
        WeightDensityScope::PerOutputChannel => problem.geometry().spectral().output_channels(),
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => 1,
    };
    let padding = problem.weighting().casa_cube_density_padding().unwrap_or(0);
    let planes = output_planes
        .checked_add(
            padding
                .checked_mul(2)
                .ok_or(WeightingError::ResidencyOverflow)?,
        )
        .ok_or(WeightingError::ResidencyOverflow)?;
    let cube_output = problem
        .weighting()
        .casa_cube_density_padding()
        .map(|_| {
            CasaLinearOutputGrid::from_spectral(problem.geometry().spectral())
                .ok_or(WeightingError::OutputChannelMismatch)
        })
        .transpose()?;
    Ok(DensityGridShape {
        width,
        height,
        planes,
        output_planes,
        padding,
        cube_output,
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
    density: &mut ExactF32Grid,
    plane: usize,
    uv: [f64; 2],
    input: f64,
) -> Result<(), WeightingError> {
    if let Some(cell) = density_build_cell(problem, grid, plane, uv) {
        density.add(cell, input as f32)?;
        if let Some(conjugate) = density_build_cell(problem, grid, plane, [-uv[0], -uv[1]]) {
            density.add(conjugate, input as f32)?;
        }
    }
    Ok(())
}

fn weighting_coordinate(
    problem: &CompiledProblem,
    grid: DensityGridShape,
    sample: &WeightingSelectedSample,
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

fn uv_lambda(sample: &WeightingSelectedSample, frequency_hz: f64) -> [f64; 2] {
    uv_lambda_for_coordinates(sample.density_uvw_m, frequency_hz)
}

fn uv_lambda_for_coordinates(uvw_m: [f64; 3], frequency_hz: f64) -> [f64; 2] {
    let scale = f64::from((frequency_hz / SPEED_OF_LIGHT_M_PER_S) as f32);
    [
        f64::from((uvw_m[0] * scale) as f32),
        f64::from((uvw_m[1] * scale) as f32),
    ]
}

fn input_weight(
    problem: &CompiledProblem,
    sample: &WeightingSelectedSample,
) -> Result<f64, WeightingError> {
    if sample.input_weight_group_flag || sample.parallel_hand_group_flag || sample.row_flag {
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
    raw_sum_weights: &[ExactF64Sum],
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
        if let Some(raw) = raw_sum_weights.get(plane) {
            density_sum = 2.0 * raw.value();
        }
        *factor = if density_sum > 0.0 && density_square_sum > 0.0 {
            (5.0 * 10_f64.powf(-robust)).powi(2) / (density_square_sum / density_sum)
        } else {
            0.0
        };
        if crate::imaging_science_trace_enabled() {
            let density_nonzero = density[start..end]
                .iter()
                .filter(|value| **value > 0.0)
                .count();
            let density_max = density[start..end].iter().copied().fold(0.0_f64, f64::max);
            eprintln!(
                "imaging_science_probe_v1 boundary=weighting_density plane={plane} width={} height={} increment_u_rad={:.17e} increment_v_rad={:.17e} density_sum={density_sum:.17e} density_sum_sq={density_square_sum:.17e} density_max={density_max:.17e} density_nonzero={density_nonzero} robust_f2={:.17e}",
                grid.width,
                grid.height,
                grid.increments()[0],
                grid.increments()[1],
                *factor,
            );
        }
    }
    factors.into_boxed_slice()
}

fn exact_f64_state_resident_bytes(state: &[ExactF64Sum], capacity: usize) -> Option<usize> {
    state.iter().try_fold(
        capacity.checked_mul(size_of::<ExactF64Sum>())?,
        |total, sum| total.checked_add(sum.bins.len().checked_mul(CONSERVATIVE_TREE_ENTRY_BYTES)?),
    )
}

#[derive(Debug)]
struct ExactF32Grid {
    cells: usize,
    limbs: Box<[u64]>,
}

impl ExactF32Grid {
    fn new(cells: usize) -> Result<Self, WeightingError> {
        let limb_count = cells
            .checked_mul(F32_SUPERACCUMULATOR_LIMBS)
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok(Self {
            cells,
            limbs: vec![0; limb_count].into_boxed_slice(),
        })
    }

    fn add(&mut self, cell: usize, value: f32) -> Result<(), WeightingError> {
        if value == 0.0 {
            return Ok(());
        }
        if !value.is_finite() || value < 0.0 {
            return Err(WeightingError::GeneratedNonFiniteWeight);
        }
        let bits = value.to_bits();
        let exponent = ((bits >> 23) & 0xff) as i16;
        let fraction = bits & 0x7f_ffff;
        let (mantissa, power) = if exponent == 0 {
            (u64::from(fraction), F32_MINIMUM_POWER)
        } else {
            (u64::from((1 << 23) | fraction), exponent - 127 - 23)
        };
        let shift = usize::try_from(power - F32_MINIMUM_POWER)
            .map_err(|_| WeightingError::GeneratedNonFiniteWeight)?;
        let start = cell
            .checked_mul(F32_SUPERACCUMULATOR_LIMBS)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let end = start
            .checked_add(F32_SUPERACCUMULATOR_LIMBS)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let accumulator = self
            .limbs
            .get_mut(start..end)
            .ok_or(WeightingError::OutputChannelMismatch)?;
        add_shifted_mantissa(accumulator, mantissa, shift)
    }

    fn resident_bytes(&self) -> Result<usize, WeightingError> {
        self.limbs
            .len()
            .checked_mul(size_of::<u64>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or(WeightingError::ResidencyOverflow)
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(DENSITY_ACCUMULATOR_DOMAIN);
        hasher.update(DENSITY_ACCUMULATOR_VERSION.to_be_bytes());
        hash_usize(&mut hasher, self.cells);
        hash_usize(&mut hasher, F32_SUPERACCUMULATOR_LIMBS);
        for limb in &self.limbs {
            hasher.update(limb.to_be_bytes());
        }
        hasher.finalize().into()
    }

    fn values(self) -> Result<Box<[f64]>, WeightingError> {
        if self.limbs.len() != self.cells * F32_SUPERACCUMULATOR_LIMBS {
            return Err(WeightingError::ResidencyOverflow);
        }
        Ok(self
            .limbs
            .chunks_exact(F32_SUPERACCUMULATOR_LIMBS)
            .map(exact_f32_accumulator_value)
            .collect::<Vec<_>>()
            .into_boxed_slice())
    }
}

fn add_shifted_mantissa(
    accumulator: &mut [u64],
    mantissa: u64,
    shift: usize,
) -> Result<(), WeightingError> {
    let word = shift / u64::BITS as usize;
    let bit = shift % u64::BITS as usize;
    let shifted = u128::from(mantissa) << bit;
    let low = shifted as u64;
    let high = (shifted >> u64::BITS) as u64;
    let mut carry = false;
    for (offset, addend) in [low, high].into_iter().enumerate() {
        let target = accumulator
            .get_mut(word + offset)
            .ok_or(WeightingError::ExactReductionOverflow)?;
        let (sum, addend_carry) = target.overflowing_add(addend);
        let (sum, carry_carry) = sum.overflowing_add(u64::from(carry));
        *target = sum;
        carry = addend_carry || carry_carry;
    }
    for target in accumulator.iter_mut().skip(word + 2) {
        if !carry {
            return Ok(());
        }
        let (sum, next) = target.overflowing_add(1);
        *target = sum;
        carry = next;
    }
    if carry {
        Err(WeightingError::ExactReductionOverflow)
    } else {
        Ok(())
    }
}

fn exact_f32_accumulator_value(accumulator: &[u64]) -> f64 {
    let Some(highest_bit) = accumulator.iter().rposition(|limb| *limb != 0).map(|word| {
        word * u64::BITS as usize + (u64::BITS - 1 - accumulator[word].leading_zeros()) as usize
    }) else {
        return 0.0;
    };
    let shift = highest_bit.saturating_sub(f64::MANTISSA_DIGITS as usize - 1);
    let word = shift / u64::BITS as usize;
    let bit = shift % u64::BITS as usize;
    let mut significand = accumulator[word] >> bit;
    if bit != 0 && word + 1 < accumulator.len() {
        significand |= accumulator[word + 1] << (u64::BITS as usize - bit);
    }
    if shift != 0 {
        let round_bit = shift - 1;
        let round_word = round_bit / u64::BITS as usize;
        let round_offset = round_bit % u64::BITS as usize;
        let halfway = accumulator[round_word] & (1_u64 << round_offset) != 0;
        let lower_word_bits = if round_offset == 0 {
            0
        } else {
            accumulator[round_word] & ((1_u64 << round_offset) - 1)
        };
        let sticky =
            lower_word_bits != 0 || accumulator[..round_word].iter().any(|limb| *limb != 0);
        if halfway && (sticky || significand & 1 != 0) {
            significand += 1;
        }
    }
    let mut scale = i32::try_from(shift).expect("six limbs fit i32") + i32::from(F32_MINIMUM_POWER);
    if significand == 1_u64 << f64::MANTISSA_DIGITS {
        significand >>= 1;
        scale += 1;
    }
    (significand as f64) * 2_f64.powi(scale)
}

#[cfg(test)]
mod exact_f32_accumulator_tests {
    use super::*;

    #[test]
    fn every_finite_exponent_and_subnormal_extremes_round_trip() {
        let values = [f32::from_bits(1), f32::from_bits(0x007f_ffff)]
            .into_iter()
            .chain((1_u32..=254).map(|exponent| f32::from_bits(exponent << 23)))
            .chain([f32::MAX]);
        for value in values {
            let mut grid = ExactF32Grid::new(1).expect("one-cell grid");
            grid.add(0, value).expect("finite positive f32");
            assert_eq!(grid.values().expect("density")[0], f64::from(value));
        }
    }

    #[test]
    fn carry_rounding_and_large_multiplicity_are_exact_and_order_invariant() {
        let mut carry = [u64::MAX, 0, 0, 0, 0, 0];
        add_shifted_mantissa(&mut carry, 1, 0).expect("carry into next limb");
        assert_eq!(carry, [0, 1, 0, 0, 0, 0]);

        let mut first = ExactF32Grid::new(1).expect("first grid");
        let mut second = ExactF32Grid::new(1).expect("second grid");
        for _ in 0..100_000 {
            first.add(0, 1.0).expect("unit contribution");
            first.add(0, 0.5).expect("half contribution");
        }
        for _ in 0..100_000 {
            second.add(0, 0.5).expect("half contribution");
        }
        for _ in 0..100_000 {
            second.add(0, 1.0).expect("unit contribution");
        }
        assert_eq!(first.digest(), second.digest());
        assert_eq!(first.values().expect("first density")[0], 150_000.0);
        assert_eq!(second.values().expect("second density")[0], 150_000.0);
    }

    #[test]
    fn casa_anchor_shape_touches_every_cell_within_the_planned_fixed_residency() {
        let cells = 1_024 * 1_024;
        let mut grid = ExactF32Grid::new(cells).expect("anchor density grid");
        for cell in 0..cells {
            let exponent = 1 + u32::try_from(cell % 254).expect("finite exponent");
            grid.add(cell, f32::from_bits(exponent << 23))
                .expect("one exact contribution per cell");
        }
        assert_eq!(
            grid.resident_bytes().expect("resident bytes"),
            cells * F32_SUPERACCUMULATOR_LIMBS * size_of::<u64>() + size_of::<ExactF32Grid>()
        );
        let values = grid.values().expect("frozen density");
        assert_eq!(values.len(), cells);
        assert!(values.iter().all(|value| value.is_finite() && *value > 0.0));
    }

    #[test]
    fn fixed_width_proves_the_u64_sample_domain_and_overflow_fails_closed() {
        assert!(
            F32_SUPERACCUMULATOR_LIMBS * u64::BITS as usize > 277 + u64::BITS as usize,
            "one f32 needs 277 magnitude bits and at most two density adds occur per sample"
        );
        let mut full = [u64::MAX; F32_SUPERACCUMULATOR_LIMBS];
        assert_eq!(
            add_shifted_mantissa(&mut full, 1, 0),
            Err(WeightingError::ExactReductionOverflow)
        );
    }
}

#[derive(Default)]
pub(crate) struct ExactF64Sum {
    pub(crate) bins: BTreeMap<i16, u128>,
}

impl ExactF64Sum {
    pub(crate) fn add(&mut self, value: f64) -> Result<(), WeightingError> {
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

    pub(crate) fn value(&self) -> f64 {
        self.bins
            .iter()
            .map(|(power, mantissa)| (*mantissa as f64) * 2_f64.powi(i32::from(*power)))
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn merge(&mut self, other: Self) -> Result<(), WeightingError> {
        for (power, mantissa) in other.bins {
            let bin = self.bins.entry(power).or_default();
            *bin = bin
                .checked_add(mantissa)
                .ok_or(WeightingError::ExactReductionOverflow)?;
        }
        Ok(())
    }
}

fn generation_identity(
    commitment: WeightingCommitmentId,
    sample_count: u64,
    grid: DensityGridShape,
    density_digest: [u8; 32],
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
    hasher.update(density_digest);
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

#[derive(Debug, Clone, Copy)]
pub(super) struct CoverageProofWork {
    pub(super) bytes: u64,
    pub(super) hash_calls: u64,
}

impl CoverageProofWork {
    fn checked_add(self, other: Self) -> Self {
        Self {
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .expect("coverage proof byte count fits u64"),
            hash_calls: self
                .hash_calls
                .checked_add(other.hash_calls)
                .expect("coverage proof hash-call count fits u64"),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct CoverageEncoder {
    hasher: Option<Sha256>,
    derived: Option<WeightingReplayCoverageId>,
    work: CoverageProofWork,
}

impl CoverageEncoder {
    pub(super) fn checkpoint_token(&self) -> [u8; 32] {
        if let Some(hasher) = &self.hasher {
            hasher.clone().finalize().into()
        } else {
            let mut hasher = Sha256::new();
            hasher.update(b"casa-rs-derived-weighting-checkpoint");
            hasher.update(
                self.derived
                    .expect("derived coverage has a proof")
                    .as_bytes(),
            );
            hasher.finalize().into()
        }
    }

    pub(super) fn new() -> Self {
        let mut encoder = Self {
            hasher: Some(Sha256::new()),
            derived: None,
            work: CoverageProofWork {
                bytes: 0,
                hash_calls: 0,
            },
        };
        encoder.update(COVERAGE_DOMAIN);
        encoder.update(&(COVERAGE_VERSION + 1).to_be_bytes());
        encoder
    }

    pub(super) fn derived(coverage: WeightingReplayCoverageId) -> Self {
        Self {
            hasher: None,
            derived: Some(coverage),
            work: CoverageProofWork {
                bytes: 0,
                hash_calls: 0,
            },
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.work.bytes = self
            .work
            .bytes
            .checked_add(u64::try_from(bytes.len()).expect("coverage proof chunk fits u64"))
            .expect("coverage proof byte count fits u64");
        self.work.hash_calls = self
            .work
            .hash_calls
            .checked_add(1)
            .expect("coverage proof hash-call count fits u64");
        self.hasher
            .as_mut()
            .expect("encoded coverage owns a hasher")
            .update(bytes);
    }

    pub(super) fn push(&mut self, weighted: &WeightingSampleValue) {
        if self.derived.is_some() {
            return;
        }
        let sample = weighted.selected();
        let mut chunk = [0_u8; COVERAGE_HASH_CHUNK_BYTES];
        let mut used = 0;
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.measurement_set.identity().as_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.physical_row.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.data_description_id.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.spectral_window_id.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.channel_index.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.address.correlation_index.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &sample.output_frame_frequency_hz.to_bits().to_be_bytes(),
        );
        match sample.row_spectral_geometry {
            Some(geometry) => {
                append_coverage_bytes(self, &mut chunk, &mut used, &[1]);
                append_coverage_bytes(
                    self,
                    &mut chunk,
                    &mut used,
                    &(geometry.selected_channels() as u128).to_be_bytes(),
                );
                let first = geometry.first();
                append_coverage_bytes(self, &mut chunk, &mut used, &first.0.to_be_bytes());
                append_coverage_bytes(
                    self,
                    &mut chunk,
                    &mut used,
                    &first.1.to_bits().to_be_bytes(),
                );
                if let Some(second) = geometry.second() {
                    append_coverage_bytes(self, &mut chunk, &mut used, &second.0.to_be_bytes());
                    append_coverage_bytes(
                        self,
                        &mut chunk,
                        &mut used,
                        &second.1.to_bits().to_be_bytes(),
                    );
                }
            }
            None => append_coverage_bytes(self, &mut chunk, &mut used, &[0]),
        }
        let mut count = 0_u8;
        for value in weighted.spectral_values() {
            count += 1;
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.contribution.output_channel().to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.contribution.factor().to_bits().to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value
                    .contribution
                    .evaluation_frequency_hz()
                    .to_bits()
                    .to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.imaging_weight.to_bits().to_be_bytes(),
            );
        }
        append_coverage_bytes(self, &mut chunk, &mut used, &[count]);
        self.update(&chunk[..used]);
    }

    pub(super) fn adopt(&mut self, checkpoint: &Self) {
        self.clone_from(checkpoint);
    }

    pub(super) fn finish(
        mut self,
        generation: WeightingGenerationId,
        sample_count: u64,
    ) -> (WeightingReplayCoverageId, CoverageProofWork) {
        if let Some(coverage) = self.derived {
            return (coverage, self.work);
        }
        self.update(&sample_count.to_be_bytes());
        let content_work = self.work;
        let content = self
            .hasher
            .take()
            .expect("encoded coverage owns a hasher")
            .finalize();
        let mut identity = Self::new();
        identity.update(&generation.as_bytes());
        identity.update(&content);
        let work = content_work.checked_add(identity.work);
        (
            WeightingReplayCoverageId(LogicalIdentity::from_sha256(
                identity
                    .hasher
                    .take()
                    .expect("coverage identity owns a hasher")
                    .finalize()
                    .into(),
            )),
            work,
        )
    }
}

#[inline]
fn append_coverage_bytes(
    encoder: &mut CoverageEncoder,
    chunk: &mut [u8; COVERAGE_HASH_CHUNK_BYTES],
    used: &mut usize,
    bytes: &[u8],
) {
    debug_assert!(bytes.len() <= chunk.len());
    if chunk.len() - *used < bytes.len() {
        encoder.update(&chunk[..*used]);
        *used = 0;
    }
    let end = *used + bytes.len();
    chunk[*used..end].copy_from_slice(bytes);
    *used = end;
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
