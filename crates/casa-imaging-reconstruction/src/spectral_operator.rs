// SPDX-License-Identifier: LGPL-3.0-or-later

//! Serial CPU basis-neutral spectral measurement operator and normal-state primitives.

use std::{fmt, sync::Arc};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, ContinuumTransformGenerationId,
    FiniteValuePolicy, InstrumentResponse, LogicalIdentity, NumericPrecision, NumericsContractId,
    PolarizationCoordinate, Projection, ReconstructionBasis, ReductionPolicy,
    SelectedObservationGenerationId, SelectedSampleAddress, SelectedVisibilitySample,
    SpectralKernel, WeightingCommitmentId,
};
use ndarray::{Array2, Axis};
use num_complex::Complex64;
use rustfft::{Fft, FftPlanner};
use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    ModelGeneration, ModelGenerationId, ModelGenerationOrigin, ModelSupport, canonical_f64_bits,
    weighting::{
        CoverageEncoder, FrozenWeightingCoverageProof, WeightingAlgorithmState,
        WeightingGenerationId, WeightingReplayChunk, WeightingReplayCoverageId, WeightingReplayId,
        WeightingReplaySummary,
    },
};

pub(crate) const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const NORMAL_STATE_CONTENT_DOMAIN: &[u8] = b"casa-rs-normal-state-content";
pub(crate) const SUPPORT: usize = 3;
const TAP_COUNT: usize = SUPPORT * 2 + 1;
#[cfg(test)]
const TAP_VISITS_PER_SAMPLE: u64 = (TAP_COUNT * TAP_COUNT) as u64;
pub(crate) const OVERSAMPLING: usize = 100;
// The pinned RustFFT mixed-radix planner stores at most one length-sized table
// per decomposition level. There are fewer than usize::BITS levels, two
// directions, and small node headers. Charging four complex values per level
// gives a hard, architecture-independent upper bound while the library keeps
// its plan internals opaque.
const FFT_PLAN_COMPLEX_BOUND_PER_AXIS: usize = 4 * usize::BITS as usize;
// One recipe plus both direction-specific cache entries per decomposition
// point, including hash-table control storage and Arc metadata.
const FFT_PLANNING_WORD_BOUND_PER_POINT: usize = 16;

/// One already-weighted spectral contribution accepted by the T19 algorithm.
///
/// This value contains no weighting policy, density, taper, generation, or
/// numerics fields. Runtime supplies it only after unwrapping a T18-branded
/// weighted block; the terminal runtime completion remains the authority that
/// proves exhaustive coverage.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SpectralOperatorSample {
    output_channel: usize,
    uvw_m: [f64; 3],
    frequency_hz: f64,
    phase_shift_m: f64,
    visibility: Complex64,
    imaging_weight: f64,
    spectral_factor: f64,
}

impl SpectralOperatorSample {
    /// Construct one numerical contribution after the runtime capability check.
    fn new(
        output_channel: usize,
        uvw_m: [f64; 3],
        frequency_hz: f64,
        phase_shift_m: f64,
        visibility: [f64; 2],
        imaging_weight: f64,
        spectral_factor: f64,
    ) -> Result<Self, SpectralOperatorError> {
        if uvw_m.iter().any(|value| !value.is_finite())
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !phase_shift_m.is_finite()
            || visibility.iter().any(|value| !value.is_finite())
            || !imaging_weight.is_finite()
            || imaging_weight < 0.0
            || !spectral_factor.is_finite()
            || spectral_factor == 0.0
        {
            return Err(SpectralOperatorError::InvalidSample);
        }
        Ok(Self {
            output_channel,
            uvw_m,
            frequency_hz,
            phase_shift_m,
            visibility: Complex64::new(visibility[0], visibility[1]),
            imaging_weight,
            spectral_factor,
        })
    }

    fn uv_lambda(self) -> [f64; 2] {
        let scale = self.frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        [self.uvw_m[0] * scale, self.uvw_m[1] * scale]
    }

    fn phase(self) -> Complex64 {
        let angle =
            std::f64::consts::TAU * self.phase_shift_m * self.frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        Complex64::from_polar(1.0, angle)
    }
}

/// One bounded output-channel slab and its sampler-required model halo.
///
/// Core channels own dirty, PSF, sum-weight, validity, and normal-state output.
/// Resident halo channels exist only so the paired forward operator can
/// evaluate every sparse stencil that contributes to a core channel. Slab
/// boundaries therefore cannot change the scientific result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpectralSlabPlan {
    total_channels: usize,
    core_start: usize,
    core_end: usize,
    resident_start: usize,
    resident_end: usize,
}

impl SpectralSlabPlan {
    fn compile(
        total_channels: usize,
        core_start: usize,
        core_depth: usize,
        kernel: SpectralKernel,
    ) -> Result<Self, SpectralOperatorError> {
        let core_end = core_start
            .checked_add(core_depth)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if total_channels == 0
            || core_depth == 0
            || core_start >= total_channels
            || core_end > total_channels
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let halo = match kernel {
            SpectralKernel::Identity | SpectralKernel::Nearest => 0,
            SpectralKernel::Linear => 1,
            SpectralKernel::Cubic => 3,
            SpectralKernel::ChannelIntegration { maximum_terms } => maximum_terms
                .checked_sub(1)
                .ok_or(SpectralOperatorError::InvalidSlab)?,
        };
        Ok(Self {
            total_channels,
            core_start,
            core_end,
            resident_start: core_start.saturating_sub(halo),
            resident_end: core_end.saturating_add(halo).min(total_channels),
        })
    }

    /// Return the total compiled output-channel count.
    #[must_use]
    pub const fn total_channels(self) -> usize {
        self.total_channels
    }

    /// Return the half-open output-channel range owned by this slab.
    #[must_use]
    pub const fn core_range(self) -> std::ops::Range<usize> {
        self.core_start..self.core_end
    }

    /// Return the half-open model-channel range resident for paired prediction.
    #[must_use]
    pub const fn resident_range(self) -> std::ops::Range<usize> {
        self.resident_start..self.resident_end
    }

    /// Return the number of output channels owned by this slab.
    #[must_use]
    pub const fn core_depth(self) -> usize {
        self.core_end - self.core_start
    }

    /// Return the number of model planes resident including sampler halo.
    #[must_use]
    pub const fn resident_depth(self) -> usize {
        self.resident_end - self.resident_start
    }

    fn owns(self, channel: usize) -> bool {
        self.core_range().contains(&channel)
    }

    fn resident_index(self, channel: usize) -> Option<usize> {
        self.resident_range()
            .contains(&channel)
            .then(|| channel - self.resident_start)
    }

    fn core_index(self, channel: usize) -> Option<usize> {
        self.owns(channel).then(|| channel - self.core_start)
    }
}

/// Immutable scientific specification for the serial CPU standard operator.
///
/// This value validates the supported geometry, polarization, reconstruction,
/// and numerical semantics. Physical allocations and execution nodes belong to
/// `casa-imaging-runtime` and are deliberately absent here.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralOperatorSpecification {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    finite_values: FiniteValuePolicy,
    image_shape: [usize; 2],
    grid_shape: [usize; 2],
    image_blc: [usize; 2],
    increment_rad: [f64; 2],
    slab: SpectralSlabPlan,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SpectralOperatorGeometry {
    pub(crate) image_shape: [usize; 2],
    pub(crate) grid_shape: [usize; 2],
    pub(crate) image_blc: [usize; 2],
    pub(crate) increment_rad: [f64; 2],
}

impl SpectralOperatorSpecification {
    /// Compile one full-depth single-field scalar Stokes-I spectral specification.
    pub fn new(problem: &CompiledProblem) -> Result<Self, SpectralOperatorError> {
        let channels = output_channel_count(problem)?;
        Self::for_slab(problem, 0, channels)
    }

    /// Compile one bounded output-channel slab.
    pub fn for_slab(
        problem: &CompiledProblem,
        core_start: usize,
        core_depth: usize,
    ) -> Result<Self, SpectralOperatorError> {
        let channels = output_channel_count(problem)?;
        if problem.geometry().domains().len() != 1
            || problem.reconstruction().polarization().coordinates()
                != [PolarizationCoordinate::StokesI]
            || problem
                .science()
                .measurement_equation()
                .instrument_response()
                != InstrumentResponse::Scalar
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let slab = SpectralSlabPlan::compile(
            channels,
            core_start,
            core_depth,
            problem.science().spectral().sampling().kernel(),
        )?;
        let domain = &problem.geometry().domains()[0];
        let image_shape = domain.shape().pixels();
        let direction = domain.direction();
        let grid_shape = [
            casa_composite_padded_len(image_shape[0], 1.2),
            casa_composite_padded_len(image_shape[1], 1.2),
        ];
        let reference_pixel = direction.reference_pixel();
        if direction.projection() != Projection::Sin
            || direction.pc() != [[1.0, 0.0], [0.0, 1.0]]
            || direction.pole_deg() != [180.0, 0.0]
            || reference_pixel
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0 || value.fract() != 0.0)
        {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let reference_pixel = [reference_pixel[0] as usize, reference_pixel[1] as usize];
        let image_blc = [
            grid_shape[0]
                .checked_div(2)
                .and_then(|centre| centre.checked_sub(reference_pixel[0]))
                .ok_or(SpectralOperatorError::UnsupportedGeometry)?,
            grid_shape[1]
                .checked_div(2)
                .and_then(|centre| centre.checked_sub(reference_pixel[1]))
                .ok_or(SpectralOperatorError::UnsupportedGeometry)?,
        ];
        if image_blc[0]
            .checked_add(image_shape[0])
            .is_none_or(|end| end > grid_shape[0])
            || image_blc[1]
                .checked_add(image_shape[1])
                .is_none_or(|end| end > grid_shape[1])
        {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        if !problem
            .numerics()
            .permitted_precisions()
            .contains(&NumericPrecision::F64)
            || problem.numerics().reduction() != ReductionPolicy::Compensated
            || !matches!(
                problem.numerics().finite_values(),
                FiniteValuePolicy::RejectAll | FiniteValuePolicy::FlagInputRejectGenerated
            )
        {
            return Err(SpectralOperatorError::UnsupportedNumerics);
        }
        Ok(Self {
            problem: problem.problem_id(),
            geometry: problem.geometry().geometry_id(),
            numerics: problem.numerics_id(),
            weighting_commitment: problem.weighting().commitment_id(),
            finite_values: problem.numerics().finite_values(),
            image_shape,
            grid_shape,
            image_blc,
            increment_rad: direction.increment_rad(),
            slab,
        })
    }

    /// Return `[width, height]` for the unpadded normal-state plane.
    #[must_use]
    pub const fn image_shape(&self) -> [usize; 2] {
        self.image_shape
    }

    /// Return the one shared padded grid shape.
    #[must_use]
    pub const fn grid_shape(&self) -> [usize; 2] {
        self.grid_shape
    }

    /// Return the exact compiled problem identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    pub(crate) const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    pub(crate) const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    pub(crate) const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.weighting_commitment
    }

    pub(crate) const fn finite_values(&self) -> FiniteValuePolicy {
        self.finite_values
    }

    /// Return the exact bounded slab compiled into this operator.
    #[must_use]
    pub const fn slab(&self) -> SpectralSlabPlan {
        self.slab
    }

    pub(crate) fn operator_geometry(&self) -> SpectralOperatorGeometry {
        SpectralOperatorGeometry {
            image_shape: self.image_shape,
            grid_shape: self.grid_shape,
            image_blc: self.image_blc,
            increment_rad: self.increment_rad,
        }
    }
}

fn output_channel_count(problem: &CompiledProblem) -> Result<usize, SpectralOperatorError> {
    match problem.reconstruction().basis() {
        ReconstructionBasis::Constant => Ok(1),
        ReconstructionBasis::ChannelLocal { channels }
            if channels == problem.geometry().spectral().output_channels() && channels > 0 =>
        {
            Ok(channels)
        }
        ReconstructionBasis::Taylor { .. } | ReconstructionBasis::ChannelLocal { .. } => {
            Err(SpectralOperatorError::UnsupportedProblem)
        }
    }
}

/// Runtime-facing workload dimensions for one serial operator instance.
///
/// The runtime converts these implementation counts into physical byte
/// allocations; reconstruction does not own that projection.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralOperatorPass {
    /// Produce dirty, PSF, and exact residual state without prior invariants.
    InitialMajor,
    /// Reuse invariant normal state and produce only the model-dependent residual.
    ResidualRefresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconstructionModelBinding {
    InitialCertifiedZero(ModelGenerationId),
    Evaluated(ModelGenerationId),
}

impl ReconstructionModelBinding {
    const fn generation(self) -> ModelGenerationId {
        match self {
            Self::InitialCertifiedZero(generation) | Self::Evaluated(generation) => generation,
        }
    }

    const fn is_evaluated(self) -> bool {
        matches!(self, Self::Evaluated(_))
    }
}

const fn reconstruction_model_binding(
    pass: SpectralOperatorPass,
    generation: ModelGenerationId,
    origin: ModelGenerationOrigin,
) -> ReconstructionModelBinding {
    match (pass, origin) {
        (SpectralOperatorPass::InitialMajor, ModelGenerationOrigin::Empty) => {
            ReconstructionModelBinding::InitialCertifiedZero(generation)
        }
        _ => ReconstructionModelBinding::Evaluated(generation),
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SpectralOperatorMeasurements {
    forward_fft_planes: u64,
    prediction_degrid_tap_visits: u64,
    dirty_grid_tap_visits: u64,
    residual_grid_tap_visits: u64,
    psf_grid_tap_visits: u64,
    final_visibility_samples: u64,
    inverse_dirty_fft_planes: u64,
    inverse_residual_fft_planes: u64,
    inverse_psf_fft_planes: u64,
}

#[cfg(test)]
fn record_measurement(counter: &mut u64, amount: u64) {
    *counter = counter
        .checked_add(amount)
        .expect("spectral operator test measurement overflowed");
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpectralOperatorWorkload {
    pass: SpectralOperatorPass,
    slab: SpectralSlabPlan,
    grid_shape: [usize; 2],
    grid_complex_values: usize,
    convolution_f64_values: usize,
    fft_resident_complex_values: usize,
    fft_planning_words: usize,
    forward_complex_values: usize,
    primitive_complex_values: usize,
    primitive_f64_values: usize,
    max_replay_block_samples: usize,
}

impl SpectralOperatorWorkload {
    /// Return the scientific major-pass role that fixes required state residency.
    #[must_use]
    pub const fn pass(self) -> SpectralOperatorPass {
        self.pass
    }

    #[must_use]
    pub const fn slab(self) -> SpectralSlabPlan {
        self.slab
    }

    #[must_use]
    pub const fn grid_shape(self) -> [usize; 2] {
        self.grid_shape
    }

    #[must_use]
    pub const fn grid_complex_values(self) -> usize {
        self.grid_complex_values
    }

    #[must_use]
    pub const fn convolution_f64_values(self) -> usize {
        self.convolution_f64_values
    }

    #[must_use]
    pub const fn fft_resident_complex_values(self) -> usize {
        self.fft_resident_complex_values
    }

    #[must_use]
    pub const fn fft_planning_words(self) -> usize {
        self.fft_planning_words
    }

    #[must_use]
    pub const fn forward_complex_values(self) -> usize {
        self.forward_complex_values
    }

    #[must_use]
    pub const fn primitive_complex_values(self) -> usize {
        self.primitive_complex_values
    }

    #[must_use]
    pub const fn primitive_f64_values(self) -> usize {
        self.primitive_f64_values
    }

    #[must_use]
    pub const fn max_replay_block_samples(self) -> usize {
        self.max_replay_block_samples
    }
}

/// Return implementation dimensions for the runtime's physical projection.
#[doc(hidden)]
pub fn spectral_operator_workload(
    specification: &SpectralOperatorSpecification,
    max_replay_block_samples: usize,
    pass: SpectralOperatorPass,
) -> Result<SpectralOperatorWorkload, SpectralOperatorError> {
    if max_replay_block_samples == 0 {
        return Err(SpectralOperatorError::UnsupportedProblem);
    }
    let cells = checked_cells(specification.grid_shape)?;
    let image_cells = checked_cells(specification.image_shape)?;
    let grid_complex_values = cells
        .checked_mul(match pass {
            SpectralOperatorPass::InitialMajor => 6,
            SpectralOperatorPass::ResidualRefresh => 2,
        })
        .and_then(|values| values.checked_mul(specification.slab.core_depth()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let convolution_f64_values = (OVERSAMPLING + 1)
        .checked_mul(TAP_COUNT)
        .and_then(|values| {
            values.checked_add(specification.grid_shape[0] + specification.grid_shape[1])
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let max_axis = specification.grid_shape.into_iter().max().unwrap_or(0);
    let opaque_plans = specification
        .grid_shape
        .into_iter()
        .try_fold(0_usize, |total, length| {
            length
                .checked_mul(FFT_PLAN_COMPLEX_BOUND_PER_AXIS)
                .and_then(|values| total.checked_add(values))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let reusable_lane_and_scratch = max_axis
        .checked_mul(FFT_PLAN_COMPLEX_BOUND_PER_AXIS + 1)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let fft_resident_complex_values = opaque_plans
        .checked_add(reusable_lane_and_scratch)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let fft_planning_words = specification
        .grid_shape
        .into_iter()
        .try_fold(0_usize, |total, length| {
            length
                .checked_mul(FFT_PLANNING_WORD_BOUND_PER_POINT)
                .and_then(|values| total.checked_add(values))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let forward_complex_values = cells
        .checked_mul(specification.slab.resident_depth())
        .and_then(|values| values.checked_add(max_replay_block_samples))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(SpectralOperatorWorkload {
        pass,
        slab: specification.slab,
        grid_shape: specification.grid_shape,
        grid_complex_values,
        convolution_f64_values,
        fft_resident_complex_values,
        fft_planning_words,
        forward_complex_values,
        primitive_complex_values: image_cells
            .checked_mul(3)
            .and_then(|values| values.checked_mul(specification.slab.core_depth()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        primitive_f64_values: image_cells
            .checked_mul(specification.slab.core_depth())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        max_replay_block_samples,
    })
}

/// FFT preparation retained by runtime between its explicit planning and replay nodes.
#[doc(hidden)]
pub struct PreparedSpectralOperator {
    specification: SpectralOperatorSpecification,
    workload: SpectralOperatorWorkload,
    fft: PreparedFft,
}

impl fmt::Debug for PreparedSpectralOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedSpectralOperator")
            .field("specification", &self.specification)
            .field("workload", &self.workload)
            .finish_non_exhaustive()
    }
}

impl PreparedSpectralOperator {
    /// Begin the opaque science owner from the exact frozen T18 generation.
    pub fn begin(
        self,
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
    ) -> Result<CompleteDataOwnerState, SpectralOperatorError> {
        CompleteDataOwnerState::new(problem, weighting, self)
    }

    /// Begin before a fused weighting stream has minted its terminal generation.
    pub fn begin_streaming(
        self,
        problem: &CompiledProblem,
    ) -> Result<CompleteDataOwnerState, SpectralOperatorError> {
        CompleteDataOwnerState::new_streaming(problem, self)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SpectralOperatorSpecification,
        SpectralOperatorWorkload,
        PreparedFft,
    ) {
        (self.specification, self.workload, self.fft)
    }
}

/// Prepare the reusable FFT implementation under runtime planning authority.
#[doc(hidden)]
pub fn prepare_spectral_operator(
    specification: SpectralOperatorSpecification,
    workload: SpectralOperatorWorkload,
) -> Result<PreparedSpectralOperator, SpectralOperatorError> {
    if workload
        != spectral_operator_workload(
            &specification,
            workload.max_replay_block_samples,
            workload.pass,
        )?
    {
        return Err(SpectralOperatorError::ProblemMismatch);
    }
    let fft = PreparedFft::new(
        specification.grid_shape,
        workload.fft_resident_complex_values,
    )?;
    Ok(PreparedSpectralOperator {
        specification,
        workload,
        fft,
    })
}

/// Unnormalized spectral primitives; these are not Product Graph artifacts.
#[derive(Debug)]
pub struct SpectralOperatorPrimitives {
    shape: [usize; 2],
    slab: SpectralSlabPlan,
    dirty: Box<[Complex64]>,
    invariant_dirty: Option<Box<[Complex64]>>,
    psf: Box<[Complex64]>,
    sensitivity: Box<[f64]>,
    sum_weights: Box<[f64]>,
    validity: Box<[SpectralChannelValidity]>,
    major_cycle_residual: Option<Box<[Complex64]>>,
    major_cycle_residual_promoted: bool,
    residual_model: Option<ModelGenerationId>,
    #[cfg(test)]
    measurements: SpectralOperatorMeasurements,
}

impl SpectralOperatorPrimitives {
    /// Return `[width, height]` shared by every plane.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// Return the exact output-channel core represented by the flattened planes.
    #[must_use]
    pub const fn slab(&self) -> SpectralSlabPlan {
        self.slab
    }

    /// Return the unnormalized dirty normal-state plane.
    #[must_use]
    pub const fn dirty(&self) -> &[Complex64] {
        &self.dirty
    }

    /// Return the unnormalized point-spread-function plane.
    #[must_use]
    pub const fn psf(&self) -> &[Complex64] {
        &self.psf
    }

    /// Return scalar-response sensitivity in normal-state units.
    #[must_use]
    pub const fn sensitivity(&self) -> &[f64] {
        &self.sensitivity
    }

    /// Return all exact accumulated sum weights in core-channel order.
    #[must_use]
    pub const fn sum_weights(&self) -> &[f64] {
        &self.sum_weights
    }

    /// Return channel validity in the same order as [`Self::sum_weights`].
    #[must_use]
    pub const fn channel_validity(&self) -> &[SpectralChannelValidity] {
        &self.validity
    }

    /// Return the exact scalar sum weight for the one-plane continuum case.
    ///
    /// Cube consumers use [`Self::sum_weights`] instead.
    #[must_use]
    pub fn sum_weight(&self) -> f64 {
        assert_eq!(
            self.sum_weights.len(),
            1,
            "cube normal state has per-channel weights"
        );
        self.sum_weights[0]
    }

    pub(crate) fn promote_major_cycle_residual(
        mut self,
        expected_model: ModelGenerationId,
    ) -> Result<Self, SpectralOperatorError> {
        if self.residual_model != Some(expected_model) {
            return Err(SpectralOperatorError::ModelMismatch);
        }
        if !self.major_cycle_residual_promoted {
            self.dirty = self
                .major_cycle_residual
                .take()
                .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?;
            self.major_cycle_residual_promoted = true;
        }
        Ok(self)
    }

    /// Derive the owner content identity of the exact unnormalized evidence.
    ///
    /// The identity binds every primitive value bit, so a Major Cycle names
    /// exact normal-state content without promising residency or density.
    #[must_use]
    pub fn normal_state_content_identity(&self) -> LogicalIdentity {
        let mut encoder = crate::Encoder::new(NORMAL_STATE_CONTENT_DOMAIN, 1);
        encoder.usize(self.shape[0]);
        encoder.usize(self.shape[1]);
        encoder.usize(self.slab.total_channels);
        encoder.usize(self.slab.core_start);
        encoder.usize(self.slab.core_end);
        for value in &self.dirty {
            encoder.u64(value.re.to_bits());
            encoder.u64(value.im.to_bits());
        }
        for value in &self.psf {
            encoder.u64(value.re.to_bits());
            encoder.u64(value.im.to_bits());
        }
        for value in &self.sensitivity {
            encoder.u64(canonical_f64_bits(*value));
        }
        for value in &self.sum_weights {
            encoder.u64(canonical_f64_bits(*value));
        }
        for validity in &self.validity {
            encoder.u8(match validity {
                SpectralChannelValidity::Valid => 0,
                SpectralChannelValidity::Blank => 1,
                SpectralChannelValidity::Unmapped => 2,
            });
        }
        LogicalIdentity::from_sha256(encoder.finish())
    }
}

pub(crate) struct ReusableNormalState {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    shape: [usize; 2],
    slab: SpectralSlabPlan,
    invariant_dirty: Option<Box<[Complex64]>>,
    psf: Box<[Complex64]>,
    sensitivity: Box<[f64]>,
    sum_weights: Box<[f64]>,
    validity: Box<[SpectralChannelValidity]>,
}

impl ReusableNormalState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        problem: CompiledProblemId,
        geometry: CompiledGeometryId,
        numerics: NumericsContractId,
        weighting_commitment: WeightingCommitmentId,
        weighting_generation: WeightingGenerationId,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
        primitives: SpectralOperatorPrimitives,
    ) -> Self {
        let SpectralOperatorPrimitives {
            shape,
            slab,
            invariant_dirty,
            psf,
            sensitivity,
            sum_weights,
            validity,
            ..
        } = primitives;
        Self {
            problem,
            geometry,
            numerics,
            weighting_commitment,
            weighting_generation,
            selected_generation,
            continuum_transform_generation,
            shape,
            slab,
            invariant_dirty,
            psf,
            sensitivity,
            sum_weights,
            validity,
        }
    }

    fn matches(&self, specification: &SpectralOperatorSpecification) -> bool {
        self.problem == specification.problem
            && self.geometry == specification.geometry
            && self.numerics == specification.numerics
            && self.weighting_commitment == specification.weighting_commitment
            && self.shape == specification.image_shape
            && self.slab == specification.slab
    }

    fn matches_replay(
        &self,
        weighting_generation: WeightingGenerationId,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> bool {
        self.weighting_generation == weighting_generation
            && self.selected_generation == selected_generation
            && self.continuum_transform_generation == continuum_transform_generation
    }
}

/// Normal-state validity for one output channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralChannelValidity {
    /// At least one mapped sample contributed positive finite weight.
    Valid,
    /// Samples mapped to the channel but all carried zero effective weight.
    Blank,
    /// No selected sample mapped to the output channel.
    Unmapped,
}

/// Versioned unnormalized primitive set produced by the nterms=1 continuum operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralPrimitiveCatalog {
    /// Dirty, PSF, sensitivity, and sum-weight primitives under the v1 contract.
    UnnormalizedPlaneV1,
    /// Channel-major dirty, PSF, sensitivity, sum-weight, and validity planes.
    UnnormalizedChannelSlabV1,
}

/// Opaque reconstruction-owned proof that one complete weighted replay reached A/A*.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataOwnerCompletion {
    pub(crate) problem: CompiledProblemId,
    pub(crate) geometry: CompiledGeometryId,
    pub(crate) numerics: NumericsContractId,
    pub(crate) weighting_commitment: WeightingCommitmentId,
    pub(crate) weighting_generation: WeightingGenerationId,
    pub(crate) replay: WeightingReplayId,
    pub(crate) coverage: WeightingReplayCoverageId,
    pub(crate) coverage_proof_bytes: u64,
    pub(crate) coverage_proof_hash_calls: u64,
    pub(crate) primitives: SpectralPrimitiveCatalog,
    pub(crate) selected_generation: SelectedObservationGenerationId,
    pub(crate) continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    pub(crate) sample_count: u64,
    pub(crate) block_count: u64,
}

impl CompleteDataOwnerCompletion {
    /// Return the exact Compiled Problem executed by this operator.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the compiled geometry/operator coordinate commitment.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the exact numerical contract.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the compiler-owned weighting commitment used by T18.
    #[must_use]
    pub const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.weighting_commitment
    }

    /// Return the frozen W generation carried by every accepted block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Return the unique terminal replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.replay
    }

    /// Return the exact T18 weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.coverage
    }

    /// Return bytes handed to coverage identity hashers by this operator pass.
    #[must_use]
    pub const fn coverage_proof_bytes(&self) -> u64 {
        self.coverage_proof_bytes
    }

    /// Return coverage identity hasher update calls by this operator pass.
    #[must_use]
    pub const fn coverage_proof_hash_calls(&self) -> u64 {
        self.coverage_proof_hash_calls
    }

    /// Return the versioned primitive set produced by the operator.
    #[must_use]
    pub const fn primitive_catalog(&self) -> SpectralPrimitiveCatalog {
        self.primitives
    }

    /// Return the exact authoritative T17 observation generation behind every
    /// weighted sample of this replay.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return the transformed visibility generation, when sequential subtraction ran.
    #[must_use]
    pub const fn continuum_transform_generation(&self) -> Option<ContinuumTransformGenerationId> {
        self.continuum_transform_generation
    }

    /// Return the exhaustive selected-sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return the exhaustive replay block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }
}

/// Complete unnormalized primitives paired with reconstruction-owned evidence.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataOwnerResult {
    pub(crate) primitives: SpectralOperatorPrimitives,
    pub(crate) completion: CompleteDataOwnerCompletion,
}

impl CompleteDataOwnerResult {
    /// Return dirty, PSF, sensitivity, and sum-weight normal-state primitives.
    #[must_use]
    pub const fn primitives(&self) -> &SpectralOperatorPrimitives {
        &self.primitives
    }

    /// Return the owner-minted complete-data proof for these exact primitives.
    #[must_use]
    pub const fn completion(&self) -> &CompleteDataOwnerCompletion {
        &self.completion
    }

    /// Consume the pairing without exposing either member separately outside
    /// this crate; the Major-Cycle owner is the only split consumer.
    #[must_use]
    pub(crate) fn into_parts(self) -> (SpectralOperatorPrimitives, CompleteDataOwnerCompletion) {
        (self.primitives, self.completion)
    }
}

/// One selected visibility predicted by the exact paired forward operator.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FinalVisibilitySample {
    address: SelectedSampleAddress,
    observed: Complex64,
    predicted: Complex64,
    residual: Complex64,
}

fn casa_persistent_complex(value: Complex64) -> Complex64 {
    Complex64::new(f64::from(value.re as f32), f64::from(value.im as f32))
}

impl FinalVisibilitySample {
    /// Return the exact selected row/channel/correlation address.
    #[must_use]
    pub const fn address(self) -> SelectedSampleAddress {
        self.address
    }

    /// Return the unweighted selected observed visibility.
    #[must_use]
    pub const fn observed(self) -> Complex64 {
        self.observed
    }

    /// Return the paired-operator model visibility.
    #[must_use]
    pub const fn predicted(self) -> Complex64 {
        self.predicted
    }

    /// Return `observed - predicted` for the same selected address.
    #[must_use]
    pub const fn residual(self) -> Complex64 {
        self.residual
    }
}

/// Reconstruction owner for one complete, ordered weighted replay.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataOwnerState {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: Option<WeightingGenerationId>,
    next_block_sequence: u64,
    sample_count: u64,
    coverage: CoverageEncoder,
    finite_values: FiniteValuePolicy,
    model_binding: Option<ReconstructionModelBinding>,
    emit_final_visibilities: bool,
    predicted_selected: Vec<FinalVisibilitySample>,
    operator: SpectralSlabOperator,
}

impl CompleteDataOwnerState {
    fn new(
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
        prepared: PreparedSpectralOperator,
    ) -> Result<Self, SpectralOperatorError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        if specification != prepared.specification || !weighting.matches_problem(problem) {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        Ok(Self {
            problem: specification.problem,
            geometry: specification.geometry,
            numerics: specification.numerics,
            weighting_commitment: specification.weighting_commitment,
            weighting_generation: Some(weighting.generation_id()),
            next_block_sequence: 0,
            sample_count: 0,
            coverage: CoverageEncoder::new(),
            finite_values: specification.finite_values,
            model_binding: None,
            emit_final_visibilities: false,
            predicted_selected: Vec::with_capacity(prepared.workload.max_replay_block_samples),
            operator: SpectralSlabOperator::new(specification, prepared.workload, prepared.fft),
        })
    }

    fn new_streaming(
        problem: &CompiledProblem,
        prepared: PreparedSpectralOperator,
    ) -> Result<Self, SpectralOperatorError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        if specification != prepared.specification {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        Ok(Self {
            problem: specification.problem,
            geometry: specification.geometry,
            numerics: specification.numerics,
            weighting_commitment: specification.weighting_commitment,
            weighting_generation: None,
            next_block_sequence: 0,
            sample_count: 0,
            coverage: CoverageEncoder::new(),
            finite_values: specification.finite_values,
            model_binding: None,
            emit_final_visibilities: false,
            predicted_selected: Vec::with_capacity(prepared.workload.max_replay_block_samples),
            operator: SpectralSlabOperator::new(specification, prepared.workload, prepared.fft),
        })
    }

    /// Bind a sealed first-pass coverage invariant before a derived later
    /// replay reaches the operator, including the empty-stream case.
    pub fn authorize_derived_coverage(
        &mut self,
        proof: FrozenWeightingCoverageProof,
    ) -> Result<(), SpectralOperatorError> {
        if !proof.matches_streaming_operator(self.problem, self.weighting_commitment)
            || self.weighting_generation.is_some_and(|generation| {
                !proof.matches_operator(self.problem, self.weighting_commitment, generation)
            })
        {
            return Err(SpectralOperatorError::WeightingGeneration);
        }
        self.weighting_generation = Some(proof.generation());
        self.coverage = CoverageEncoder::derived(proof.coverage());
        Ok(())
    }

    /// Bind the exact prepared final model before the exhaustive replay starts.
    pub fn bind_major_cycle_model(
        &mut self,
        generation: &ModelGeneration,
        prior_normal_state: Option<crate::FinalNormalState>,
    ) -> Result<(), SpectralOperatorError> {
        if self.sample_count != 0 || self.next_block_sequence != 0 || self.model_binding.is_some() {
            return Err(SpectralOperatorError::MajorCycleAlreadyBound);
        }
        self.model_binding = Some(self.operator.prepare_bound_residual_model(
            generation,
            prior_normal_state.map(crate::FinalNormalState::into_reusable),
        )?);
        Ok(())
    }

    /// Bind the terminal model for prediction-only selected output.
    pub fn bind_selected_output_model(
        &mut self,
        generation: &ModelGeneration,
    ) -> Result<(), SpectralOperatorError> {
        if self.sample_count != 0 || self.next_block_sequence != 0 || self.model_binding.is_some() {
            return Err(SpectralOperatorError::MajorCycleAlreadyBound);
        }
        self.operator.prepare_selected_output_model(generation)?;
        self.model_binding = Some(ReconstructionModelBinding::Evaluated(
            generation.generation_id(),
        ));
        Ok(())
    }

    /// Return the sole frozen T18 generation accepted by this owner.
    #[must_use]
    pub const fn weighting_generation(&self) -> Option<WeightingGenerationId> {
        self.weighting_generation
    }

    /// Request bounded final-visibility samples even when no residual model is bound.
    pub fn enable_final_visibility_samples(&mut self) {
        self.emit_final_visibilities = true;
    }

    /// Consume one reconstruction-owned T18 block in canonical replay order.
    pub fn consume_block(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<&[FinalVisibilitySample], SpectralOperatorError> {
        if block.sequence() != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.coverage.adopt(block.coverage_checkpoint());
        self.predicted_selected.clear();
        for weighted in block.samples() {
            let selected = weighted.selected();
            let visibility = match selected.visibility {
                SelectedVisibilitySample::Float32(value) => [f64::from(value), 0.0],
                SelectedVisibilitySample::Complex32([real, imaginary]) => {
                    [f64::from(real), f64::from(imaginary)]
                }
            };
            let contributes_to_stokes_i =
                selected.address.correlation_type.contributes_to_stokes_i();
            let accepted_input = self.accept_input(selected)?;
            let grids = contributes_to_stokes_i && accepted_input;
            let mut predicted_visibility = Complex64::default();
            let mut touches_core = false;
            let has_spectral_support = weighted.spectral_values().next().is_some();
            if contributes_to_stokes_i {
                let mut stencil = SmallVec::<[SpectralOperatorSample; 4]>::new();
                for spectral in weighted.spectral_values() {
                    let contribution = spectral.contribution();
                    stencil.push(SpectralOperatorSample::new(
                        usize::try_from(contribution.output_channel())
                            .map_err(|_| SpectralOperatorError::InvalidSample)?,
                        selected.transformed_uvw_m,
                        contribution.evaluation_frequency_hz(),
                        selected.phase_shift_m,
                        if grids { visibility } else { [0.0, 0.0] },
                        spectral.imaging_weight(),
                        contribution.factor(),
                    )?);
                }
                touches_core = stencil
                    .iter()
                    .any(|sample| self.operator.slab.owns(sample.output_channel));
                let predicts_residual = self
                    .model_binding
                    .is_some_and(ReconstructionModelBinding::is_evaluated);
                if predicts_residual && touches_core {
                    predicted_visibility = self.operator.predict_stencil(&stencil)?;
                }
                if grids {
                    for sample in stencil {
                        if predicts_residual {
                            self.operator
                                .push_with_residual(sample, predicted_visibility)?;
                        } else {
                            self.operator.push(sample)?;
                        }
                    }
                }
            }
            if self.emit_final_visibilities
                && has_spectral_support
                && (touches_core || self.operator.slab.total_channels() == 1)
            {
                let observed = Complex64::new(visibility[0], visibility[1]);
                // CASA degrids and writes the whole selected visibility
                // buffer. Flags and Stokes-I participation govern gridding,
                // not whether the selected MODEL_DATA destination is refreshed.
                let predicted = casa_persistent_complex(predicted_visibility);
                self.predicted_selected.push(FinalVisibilitySample {
                    address: selected.address,
                    observed,
                    predicted,
                    residual: observed - predicted,
                });
                #[cfg(test)]
                record_measurement(&mut self.operator.measurements.final_visibility_samples, 1);
            }
        }
        self.sample_count = self
            .sample_count
            .checked_add(
                u64::try_from(block.samples().len())
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            )
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        Ok(&self.predicted_selected)
    }

    /// Predict one ordered output block without accumulating residual science grids.
    pub fn predict_final_visibility_block(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<&[FinalVisibilitySample], SpectralOperatorError> {
        if self.model_binding.is_none() {
            return Err(SpectralOperatorError::MissingMajorCycleResidual);
        }
        if block.sequence() != self.next_block_sequence {
            return Err(SpectralOperatorError::BlockSequence);
        }
        if block.samples().len() > self.operator.workload.max_replay_block_samples {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.coverage.adopt(block.coverage_checkpoint());
        self.predicted_selected.clear();
        for weighted in block.samples() {
            let selected = weighted.selected();
            let _ = self.accept_input(selected)?;
            let visibility = match selected.visibility {
                SelectedVisibilitySample::Float32(value) => [f64::from(value), 0.0],
                SelectedVisibilitySample::Complex32([real, imaginary]) => {
                    [f64::from(real), f64::from(imaginary)]
                }
            };
            let mut predicted_visibility = Complex64::default();
            let mut touches_core = false;
            let has_spectral_support = weighted.spectral_values().next().is_some();
            if selected.address.correlation_type.contributes_to_stokes_i() {
                let mut stencil = SmallVec::<[SpectralOperatorSample; 4]>::new();
                for spectral in weighted.spectral_values() {
                    let contribution = spectral.contribution();
                    stencil.push(SpectralOperatorSample::new(
                        usize::try_from(contribution.output_channel())
                            .map_err(|_| SpectralOperatorError::InvalidSample)?,
                        selected.transformed_uvw_m,
                        contribution.evaluation_frequency_hz(),
                        selected.phase_shift_m,
                        [0.0, 0.0],
                        spectral.imaging_weight(),
                        contribution.factor(),
                    )?);
                }
                touches_core = stencil
                    .iter()
                    .any(|sample| self.operator.slab.owns(sample.output_channel));
                if touches_core
                    && self
                        .model_binding
                        .is_some_and(ReconstructionModelBinding::is_evaluated)
                {
                    predicted_visibility = self.operator.predict_stencil(&stencil)?;
                }
            }
            if has_spectral_support && (touches_core || self.operator.slab.total_channels() == 1) {
                let observed = Complex64::new(visibility[0], visibility[1]);
                let predicted = casa_persistent_complex(predicted_visibility);
                self.predicted_selected.push(FinalVisibilitySample {
                    address: selected.address,
                    observed,
                    predicted,
                    residual: observed - predicted,
                });
                #[cfg(test)]
                record_measurement(&mut self.operator.measurements.final_visibility_samples, 1);
            }
        }
        self.sample_count = self
            .sample_count
            .checked_add(
                u64::try_from(block.samples().len())
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            )
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        Ok(&self.predicted_selected)
    }

    /// Apply the paired forward operator to one opaque bounded T18 replay block.
    pub fn predict_block(
        &mut self,
        model: &[Complex64],
        block: &WeightingReplayChunk,
    ) -> Result<&[Complex64], SpectralOperatorError> {
        if self.model_binding.is_some() {
            return Err(SpectralOperatorError::PredictionAfterMajorCycleBinding);
        }
        if block.samples().len() > self.operator.workload.max_replay_block_samples {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.operator.prepare_prediction_grid(model)?;
        self.operator.prediction_len = 0;
        for weighted in block.samples() {
            let selected = weighted.selected();
            if !self.accept_input(selected)?
                || !selected.address.correlation_type.contributes_to_stokes_i()
            {
                continue;
            }
            let mut stencil = SmallVec::<[SpectralOperatorSample; 4]>::new();
            for spectral in weighted.spectral_values() {
                let contribution = spectral.contribution();
                stencil.push(SpectralOperatorSample::new(
                    usize::try_from(contribution.output_channel())
                        .map_err(|_| SpectralOperatorError::InvalidSample)?,
                    selected.transformed_uvw_m,
                    contribution.evaluation_frequency_hz(),
                    selected.phase_shift_m,
                    [0.0, 0.0],
                    spectral.imaging_weight(),
                    contribution.factor(),
                )?);
            }
            if stencil
                .iter()
                .any(|sample| self.operator.slab.owns(sample.output_channel))
            {
                self.operator.push_prediction(&stencil)?;
            }
        }
        Ok(&self.operator.predictions[..self.operator.prediction_len])
    }

    /// Consume terminal T18 algorithm evidence and mint complete-data evidence.
    ///
    /// The authoritative T17 observation generation arrives beside the replay
    /// proof so the minted completion binds data content and observation
    /// lineage inseparably.
    pub fn complete(
        self,
        replay: &WeightingReplaySummary,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<CompleteDataOwnerResult, SpectralOperatorError> {
        if self
            .weighting_generation
            .is_some_and(|generation| generation != replay.weighting_generation())
        {
            return Err(SpectralOperatorError::WeightingGeneration);
        }
        if self.sample_count != replay.sample_count()
            || self.next_block_sequence != replay.block_count()
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let (coverage, coverage_proof_work) = self
            .coverage
            .finish(replay.weighting_generation(), self.sample_count);
        if coverage != replay.coverage() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.operator.validate_reused_lineage(
            replay.weighting_generation(),
            selected_generation,
            continuum_transform_generation,
        )?;
        let primitives = if self.operator.slab.total_channels() == 1 {
            SpectralPrimitiveCatalog::UnnormalizedPlaneV1
        } else {
            SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1
        };
        Ok(CompleteDataOwnerResult {
            primitives: self.operator.finish_bound(self.model_binding)?,
            completion: CompleteDataOwnerCompletion {
                problem: self.problem,
                geometry: self.geometry,
                numerics: self.numerics,
                weighting_commitment: self.weighting_commitment,
                weighting_generation: replay.weighting_generation(),
                replay: replay.replay_id(),
                coverage,
                coverage_proof_bytes: coverage_proof_work.bytes,
                coverage_proof_hash_calls: coverage_proof_work.hash_calls,
                primitives,
                selected_generation,
                continuum_transform_generation,
                sample_count: replay.sample_count(),
                block_count: replay.block_count(),
            },
        })
    }

    fn accept_input(
        &self,
        sample: &crate::weighting::WeightingSelectedSample,
    ) -> Result<bool, SpectralOperatorError> {
        accept_weighted_input(sample, self.finite_values)
    }
}

pub(crate) fn accept_weighted_input(
    sample: &crate::weighting::WeightingSelectedSample,
    finite_values: FiniteValuePolicy,
) -> Result<bool, SpectralOperatorError> {
    let nonfinite = sample
        .transformed_uvw_m
        .iter()
        .any(|value| !value.is_finite())
        || !sample.phase_shift_m.is_finite()
        || !sample.address.frequency_centre_hz.is_finite()
        || !sample.input_weight.is_finite()
        || match sample.visibility {
            SelectedVisibilitySample::Float32(value) => !value.is_finite(),
            SelectedVisibilitySample::Complex32(value) => {
                value.into_iter().any(|component| !component.is_finite())
            }
        };
    apply_input_policy(
        nonfinite,
        sample.row_flag || sample.parallel_hand_group_flag,
        finite_values,
    )
}

fn apply_finite_value_policy(
    nonfinite_input: bool,
    policy: FiniteValuePolicy,
) -> Result<bool, SpectralOperatorError> {
    match (nonfinite_input, policy) {
        (false, _) => Ok(true),
        (true, FiniteValuePolicy::FlagInputRejectGenerated) => Ok(false),
        (true, FiniteValuePolicy::RejectAll) => Err(SpectralOperatorError::InvalidSample),
    }
}

fn apply_input_policy(
    nonfinite_input: bool,
    declared_flag: bool,
    policy: FiniteValuePolicy,
) -> Result<bool, SpectralOperatorError> {
    Ok(apply_finite_value_policy(nonfinite_input, policy)? && !declared_flag)
}

/// Reconstruction-owned serial CPU operator accumulator for one bounded slab.
pub(crate) struct SpectralSlabOperator {
    specification: Option<SpectralOperatorSpecification>,
    geometry: SpectralOperatorGeometry,
    slab: SpectralSlabPlan,
    workload: SpectralOperatorWorkload,
    gridder: StandardConvolution,
    fft: PreparedFft,
    dirty_grids: Option<Vec<Array2<Complex64>>>,
    dirty_compensations: Option<Vec<Array2<Complex64>>>,
    psf_grids: Option<Vec<Array2<Complex64>>>,
    psf_compensations: Option<Vec<Array2<Complex64>>>,
    residual_grids: Option<Vec<Array2<Complex64>>>,
    residual_compensations: Option<Vec<Array2<Complex64>>>,
    reused_normal_state: Option<ReusableNormalState>,
    forward_grids: Vec<Array2<Complex64>>,
    predictions: Box<[Complex64]>,
    prediction_len: usize,
    sum_weights: Vec<f64>,
    sum_weight_compensations: Vec<f64>,
    mapped_samples: Vec<u64>,
    #[cfg(test)]
    measurements: SpectralOperatorMeasurements,
}

impl fmt::Debug for SpectralSlabOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpectralSlabOperator")
            .field("geometry", &self.geometry)
            .field("slab", &self.slab)
            .field("workload", &self.workload)
            .field("sum_weights", &self.sum_weights)
            .finish_non_exhaustive()
    }
}

impl SpectralSlabOperator {
    /// Allocate the runtime-projected buffers and no worker-local grid duplicate.
    #[must_use]
    pub(crate) fn new(
        specification: SpectralOperatorSpecification,
        workload: SpectralOperatorWorkload,
        fft: PreparedFft,
    ) -> Self {
        let geometry = specification.operator_geometry();
        let slab = specification.slab;
        Self::new_inner(Some(specification), geometry, slab, workload, fft)
    }

    #[cfg(test)]
    fn new_with_geometry(
        geometry: SpectralOperatorGeometry,
        slab: SpectralSlabPlan,
        workload: SpectralOperatorWorkload,
        fft: PreparedFft,
    ) -> Self {
        Self::new_inner(None, geometry, slab, workload, fft)
    }

    fn new_inner(
        specification: Option<SpectralOperatorSpecification>,
        geometry: SpectralOperatorGeometry,
        slab: SpectralSlabPlan,
        workload: SpectralOperatorWorkload,
        fft: PreparedFft,
    ) -> Self {
        let gridder = StandardConvolution::new(&geometry);
        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let plane_grids =
            |depth: usize| (0..depth).map(|_| Array2::zeros(shape)).collect::<Vec<_>>();
        let initial = workload.pass == SpectralOperatorPass::InitialMajor;
        Self {
            specification,
            geometry,
            slab,
            workload,
            gridder,
            fft,
            dirty_grids: initial.then(|| plane_grids(slab.core_depth())),
            dirty_compensations: initial.then(|| plane_grids(slab.core_depth())),
            psf_grids: initial.then(|| plane_grids(slab.core_depth())),
            psf_compensations: initial.then(|| plane_grids(slab.core_depth())),
            residual_grids: None,
            residual_compensations: None,
            reused_normal_state: None,
            forward_grids: plane_grids(slab.resident_depth()),
            predictions: vec![Complex64::default(); workload.max_replay_block_samples]
                .into_boxed_slice(),
            prediction_len: 0,
            sum_weights: vec![0.0; slab.core_depth()],
            sum_weight_compensations: vec![0.0; slab.core_depth()],
            mapped_samples: vec![0; slab.core_depth()],
            #[cfg(test)]
            measurements: SpectralOperatorMeasurements::default(),
        }
    }

    /// Accumulate one already-weighted spectral contribution.
    fn push(&mut self, sample: SpectralOperatorSample) -> Result<(), SpectralOperatorError> {
        let Some(plane) = self.slab.core_index(sample.output_channel) else {
            return Ok(());
        };
        self.mapped_samples[plane] = self.mapped_samples[plane]
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        if sample.imaging_weight == 0.0 {
            return Ok(());
        }
        let Some(taps) = self.gridder.taps(sample.uv_lambda()) else {
            return Ok(());
        };
        let factor = sample.spectral_factor;
        let weighted_visibility =
            sample.visibility * sample.phase() * (sample.imaging_weight * factor);
        self.gridder.grid_compensated(
            &mut self
                .dirty_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            &mut self
                .dirty_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            taps,
            weighted_visibility,
        );
        #[cfg(test)]
        record_measurement(
            &mut self.measurements.dirty_grid_tap_visits,
            TAP_VISITS_PER_SAMPLE,
        );
        let psf_weight = sample.imaging_weight * factor * factor;
        self.gridder.grid_compensated(
            &mut self
                .psf_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            &mut self
                .psf_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            taps,
            Complex64::new(psf_weight, 0.0),
        );
        #[cfg(test)]
        record_measurement(
            &mut self.measurements.psf_grid_tap_visits,
            TAP_VISITS_PER_SAMPLE,
        );
        let corrected = psf_weight - self.sum_weight_compensations[plane];
        let updated = self.sum_weights[plane] + corrected;
        self.sum_weight_compensations[plane] = (updated - self.sum_weights[plane]) - corrected;
        self.sum_weights[plane] = updated;
        Ok(())
    }

    fn prepare_bound_residual_model(
        &mut self,
        generation: &ModelGeneration,
        reused_normal_state: Option<ReusableNormalState>,
    ) -> Result<ReconstructionModelBinding, SpectralOperatorError> {
        self.validate_model_generation(generation)?;
        match (self.workload.pass, reused_normal_state.as_ref()) {
            (SpectralOperatorPass::InitialMajor, None) => {}
            (SpectralOperatorPass::ResidualRefresh, Some(state))
                if self
                    .specification
                    .as_ref()
                    .is_some_and(|specification| state.matches(specification)) => {}
            _ => return Err(SpectralOperatorError::ReusableNormalStateMismatch),
        }
        self.reused_normal_state = reused_normal_state;
        let binding = reconstruction_model_binding(
            self.workload.pass,
            generation.generation_id(),
            generation.origin(),
        );
        if !binding.is_evaluated() {
            return Ok(binding);
        }
        self.prepare_forward_generation(generation)?;
        let grid_shape = (self.geometry.grid_shape[0], self.geometry.grid_shape[1]);
        self.residual_grids = Some(
            (0..self.slab.core_depth())
                .map(|_| Array2::zeros(grid_shape))
                .collect(),
        );
        self.residual_compensations = Some(
            (0..self.slab.core_depth())
                .map(|_| Array2::zeros(grid_shape))
                .collect(),
        );
        Ok(binding)
    }

    pub(crate) fn prepare_residual_model(
        &mut self,
        generation: &ModelGeneration,
        reused_normal_state: Option<ReusableNormalState>,
    ) -> Result<(), SpectralOperatorError> {
        if !self
            .prepare_bound_residual_model(generation, reused_normal_state)?
            .is_evaluated()
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        Ok(())
    }

    fn prepare_selected_output_model(
        &mut self,
        generation: &ModelGeneration,
    ) -> Result<(), SpectralOperatorError> {
        self.validate_model_generation(generation)?;
        self.reused_normal_state = None;
        self.prepare_forward_generation(generation)
    }

    fn validate_model_generation(
        &self,
        generation: &ModelGeneration,
    ) -> Result<(), SpectralOperatorError> {
        let shape = generation.shape();
        if shape.domains().len() != 1
            || shape.coefficients() != self.slab.total_channels()
            || shape.polarizations() != 1
            || shape.domains()[0].pixels() != self.geometry.image_shape
            || generation.samples().len() != shape.sample_count()
        {
            return Err(SpectralOperatorError::ModelShape);
        }
        Ok(())
    }

    fn prepare_forward_generation(
        &mut self,
        generation: &ModelGeneration,
    ) -> Result<(), SpectralOperatorError> {
        let width = self.geometry.image_shape[0];
        let height = self.geometry.image_shape[1];
        let cells = width * height;
        for (resident, output_channel) in self.slab.resident_range().enumerate() {
            let grid = &mut self.forward_grids[resident];
            grid.fill(Complex64::default());
            for y in 0..height {
                for x in 0..width {
                    let sample = generation.samples()[output_channel * cells + y * width + x];
                    if sample.support() == ModelSupport::Invalid {
                        continue;
                    }
                    let correction = self.gridder.image_correction(x, y);
                    grid[(
                        self.geometry.image_blc[0] + x,
                        self.geometry.image_blc[1] + y,
                    )] = Complex64::new(sample.value().value(), 0.0) * correction;
                }
            }
            self.fft.transform(grid, false);
            #[cfg(test)]
            record_measurement(&mut self.measurements.forward_fft_planes, 1);
        }
        if self
            .forward_grids
            .iter()
            .flatten()
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        Ok(())
    }

    fn push_with_residual(
        &mut self,
        sample: SpectralOperatorSample,
        predicted: Complex64,
    ) -> Result<(), SpectralOperatorError> {
        let Some(plane) = self.slab.core_index(sample.output_channel) else {
            return Ok(());
        };
        self.mapped_samples[plane] = self.mapped_samples[plane]
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        if sample.imaging_weight == 0.0 {
            return Ok(());
        }
        let Some(taps) = self.gridder.taps(sample.uv_lambda()) else {
            return Ok(());
        };
        let factor = sample.spectral_factor;
        if let (Some(dirty), Some(compensation)) =
            (&mut self.dirty_grids, &mut self.dirty_compensations)
        {
            let weighted_visibility =
                sample.visibility * sample.phase() * (sample.imaging_weight * factor);
            self.gridder.grid_compensated(
                &mut dirty[plane],
                &mut compensation[plane],
                taps,
                weighted_visibility,
            );
            #[cfg(test)]
            record_measurement(
                &mut self.measurements.dirty_grid_tap_visits,
                TAP_VISITS_PER_SAMPLE,
            );
        }
        let residual_visibility =
            (sample.visibility - predicted) * sample.phase() * (sample.imaging_weight * factor);
        self.gridder.grid_compensated(
            &mut self
                .residual_grids
                .as_mut()
                .expect("bound residual model owns its grids")[plane],
            &mut self
                .residual_compensations
                .as_mut()
                .expect("bound residual model owns compensation")[plane],
            taps,
            residual_visibility,
        );
        #[cfg(test)]
        record_measurement(
            &mut self.measurements.residual_grid_tap_visits,
            TAP_VISITS_PER_SAMPLE,
        );
        if let (Some(psf), Some(compensation)) = (&mut self.psf_grids, &mut self.psf_compensations)
        {
            let psf_weight = sample.imaging_weight * factor * factor;
            self.gridder.grid_compensated(
                &mut psf[plane],
                &mut compensation[plane],
                taps,
                Complex64::new(psf_weight, 0.0),
            );
            #[cfg(test)]
            record_measurement(
                &mut self.measurements.psf_grid_tap_visits,
                TAP_VISITS_PER_SAMPLE,
            );
            let corrected = psf_weight - self.sum_weight_compensations[plane];
            let updated = self.sum_weights[plane] + corrected;
            self.sum_weight_compensations[plane] = (updated - self.sum_weights[plane]) - corrected;
            self.sum_weights[plane] = updated;
        }
        Ok(())
    }

    fn validate_reused_lineage(
        &self,
        weighting_generation: WeightingGenerationId,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<(), SpectralOperatorError> {
        match (self.workload.pass, self.reused_normal_state.as_ref()) {
            (SpectralOperatorPass::InitialMajor, None) => Ok(()),
            (SpectralOperatorPass::ResidualRefresh, Some(state))
                if state.matches_replay(
                    weighting_generation,
                    selected_generation,
                    continuum_transform_generation,
                ) =>
            {
                Ok(())
            }
            _ => Err(SpectralOperatorError::ReusableNormalStateMismatch),
        }
    }

    /// Predict unweighted selected visibilities with the paired forward operator.
    #[cfg(test)]
    fn predict(
        &mut self,
        model: &[Complex64],
        samples: &[SpectralOperatorSample],
    ) -> Result<Box<[Complex64]>, SpectralOperatorError> {
        self.prepare_prediction_grid(model)?;
        samples
            .iter()
            .map(|sample| self.predict_one(*sample))
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
    }

    fn prepare_prediction_grid(
        &mut self,
        model: &[Complex64],
    ) -> Result<(), SpectralOperatorError> {
        let cells = checked_cells(self.geometry.image_shape)?;
        if model.len()
            != cells
                .checked_mul(self.slab.total_channels())
                .ok_or(SpectralOperatorError::ResidencyOverflow)?
        {
            return Err(SpectralOperatorError::ModelShape);
        }
        for (resident, output_channel) in self.slab.resident_range().enumerate() {
            let grid = &mut self.forward_grids[resident];
            grid.fill(Complex64::default());
            for x in 0..self.geometry.image_shape[0] {
                for y in 0..self.geometry.image_shape[1] {
                    let correction = self.gridder.image_correction(x, y);
                    grid[(
                        self.geometry.image_blc[0] + x,
                        self.geometry.image_blc[1] + y,
                    )] = model[output_channel * cells + x * self.geometry.image_shape[1] + y]
                        * correction;
                }
            }
            self.fft.transform(grid, false);
            #[cfg(test)]
            record_measurement(&mut self.measurements.forward_fft_planes, 1);
        }
        if self
            .forward_grids
            .iter()
            .flatten()
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        Ok(())
    }

    fn predict_one(
        &mut self,
        sample: SpectralOperatorSample,
    ) -> Result<Complex64, SpectralOperatorError> {
        let resident = self
            .slab
            .resident_index(sample.output_channel)
            .ok_or(SpectralOperatorError::IncompleteSpectralHalo)?;
        let Some(taps) = self.gridder.taps(sample.uv_lambda()) else {
            return Ok(Complex64::new(0.0, 0.0));
        };
        self.predict_one_with_taps(resident, sample, taps)
    }

    fn predict_one_with_taps(
        &mut self,
        resident: usize,
        sample: SpectralOperatorSample,
        taps: SampleTaps,
    ) -> Result<Complex64, SpectralOperatorError> {
        let predicted = self.gridder.degrid(&self.forward_grids[resident], taps)
            * sample.phase().conj()
            * sample.spectral_factor;
        #[cfg(test)]
        record_measurement(
            &mut self.measurements.prediction_degrid_tap_visits,
            TAP_VISITS_PER_SAMPLE,
        );
        if predicted.re.is_finite() && predicted.im.is_finite() {
            Ok(predicted)
        } else {
            Err(SpectralOperatorError::GeneratedNonfinite)
        }
    }

    fn predict_stencil(
        &mut self,
        samples: &[SpectralOperatorSample],
    ) -> Result<Complex64, SpectralOperatorError> {
        samples
            .iter()
            .try_fold(Complex64::default(), |sum, sample| {
                self.predict_one(*sample).map(|value| sum + value)
            })
    }

    fn push_prediction(
        &mut self,
        samples: &[SpectralOperatorSample],
    ) -> Result<(), SpectralOperatorError> {
        if self.prediction_len == self.predictions.len() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.predictions[self.prediction_len] = self.predict_stencil(samples)?;
        self.prediction_len += 1;
        Ok(())
    }

    pub(crate) fn predict_gridded_normal(
        &self,
        output_channel: usize,
        taps: SampleTaps,
        forward_scale: Complex64,
    ) -> Result<Complex64, SpectralOperatorError> {
        let resident = self
            .slab
            .resident_index(output_channel)
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        if !forward_scale.re.is_finite() || !forward_scale.im.is_finite() {
            return Err(SpectralOperatorError::GriddedRecordMismatch);
        }
        let predicted = self.gridder.degrid(&self.forward_grids[resident], taps) * forward_scale;
        if !predicted.re.is_finite() || !predicted.im.is_finite() {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        Ok(predicted)
    }

    pub(crate) fn apply_gridded_normal(
        &mut self,
        output_channel: usize,
        taps: SampleTaps,
        predicted: Complex64,
        adjoint_scale: Complex64,
    ) -> Result<(), SpectralOperatorError> {
        let plane = self
            .slab
            .core_index(output_channel)
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        let gridded = predicted * adjoint_scale;
        if !gridded.re.is_finite() || !gridded.im.is_finite() {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        self.gridder.grid_compensated(
            &mut self
                .residual_grids
                .as_mut()
                .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?[plane],
            &mut self
                .residual_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?[plane],
            taps,
            gridded,
        );
        Ok(())
    }

    pub(crate) fn finish_gridded_normal(
        mut self,
        residual_model: ModelGenerationId,
    ) -> Result<SpectralOperatorPrimitives, SpectralOperatorError> {
        if self.workload.pass != SpectralOperatorPass::ResidualRefresh {
            return Err(SpectralOperatorError::UnsupportedGriddedReplay);
        }
        let mut normal_grids = self
            .residual_grids
            .take()
            .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?;
        if normal_grids.len() != self.slab.core_depth() {
            return Err(SpectralOperatorError::MissingMajorCycleResidual);
        }
        for grid in &mut normal_grids {
            self.fft.transform(grid, true);
        }
        let reused = self
            .reused_normal_state
            .take()
            .ok_or(SpectralOperatorError::ReusableNormalStateMismatch)?;
        let cells = checked_cells(self.geometry.image_shape)?;
        let output_cells = cells
            .checked_mul(self.slab.core_depth())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let invariant_dirty = reused
            .invariant_dirty
            .ok_or(SpectralOperatorError::ReusableNormalStateMismatch)?;
        if invariant_dirty.len() != output_cells
            || reused.psf.len() != output_cells
            || reused.sensitivity.len() != output_cells
            || reused.sum_weights.len() != self.slab.core_depth()
            || reused.validity.len() != self.slab.core_depth()
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        let mut residual = Vec::with_capacity(output_cells);
        for (plane, normal_grid) in normal_grids.iter().enumerate() {
            for x in 0..self.geometry.image_shape[0] {
                for y in 0..self.geometry.image_shape[1] {
                    let correction = self.gridder.image_correction(x, y);
                    let normal = normal_grid[(
                        self.geometry.image_blc[0] + x,
                        self.geometry.image_blc[1] + y,
                    )] * correction;
                    let index = plane * cells + x * self.geometry.image_shape[1] + y;
                    residual.push(invariant_dirty[index] - normal);
                }
            }
        }
        if residual
            .iter()
            .chain(invariant_dirty.iter())
            .chain(reused.psf.iter())
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
            || reused.sensitivity.iter().any(|value| !value.is_finite())
            || reused.sum_weights.iter().any(|value| !value.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        Ok(SpectralOperatorPrimitives {
            shape: self.geometry.image_shape,
            slab: self.slab,
            dirty: residual.into_boxed_slice(),
            invariant_dirty: Some(invariant_dirty),
            psf: reused.psf,
            sensitivity: reused.sensitivity,
            sum_weights: reused.sum_weights,
            validity: reused.validity,
            major_cycle_residual: None,
            major_cycle_residual_promoted: true,
            residual_model: Some(residual_model),
            #[cfg(test)]
            measurements: self.measurements,
        })
    }

    /// Finish the paired inverse transforms and return unnormalized primitives.
    #[cfg(test)]
    pub fn finish(self) -> Result<SpectralOperatorPrimitives, SpectralOperatorError> {
        self.finish_bound(None)
    }

    fn finish_bound(
        mut self,
        model_binding: Option<ReconstructionModelBinding>,
    ) -> Result<SpectralOperatorPrimitives, SpectralOperatorError> {
        let residual_model = model_binding.map(ReconstructionModelBinding::generation);
        let initial_empty = model_binding.is_some_and(|binding| {
            matches!(binding, ReconstructionModelBinding::InitialCertifiedZero(_))
        });
        if initial_empty
            && (self.workload.pass != SpectralOperatorPass::InitialMajor
                || self.reused_normal_state.is_some()
                || self.residual_grids.is_some()
                || self.residual_compensations.is_some())
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        for plane in 0..self.slab.core_depth() {
            if let Some(dirty) = self.dirty_grids.as_mut() {
                self.fft.transform(&mut dirty[plane], true);
                #[cfg(test)]
                record_measurement(&mut self.measurements.inverse_dirty_fft_planes, 1);
            }
            if let Some(psf) = self.psf_grids.as_mut() {
                self.fft.transform(&mut psf[plane], true);
                #[cfg(test)]
                record_measurement(&mut self.measurements.inverse_psf_fft_planes, 1);
            }
            if let Some(residual) = self.residual_grids.as_mut() {
                self.fft.transform(&mut residual[plane], true);
                #[cfg(test)]
                record_measurement(&mut self.measurements.inverse_residual_fft_planes, 1);
            }
        }
        let cells = self.geometry.image_shape[0] * self.geometry.image_shape[1];
        let output_cells = cells
            .checked_mul(self.slab.core_depth())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if let Some(reused) = self.reused_normal_state.take() {
            if residual_model.is_none()
                || reused
                    .invariant_dirty
                    .as_ref()
                    .is_some_and(|dirty| dirty.len() != output_cells)
                || reused.psf.len() != output_cells
                || reused.sensitivity.len() != output_cells
                || reused.sum_weights.len() != self.slab.core_depth()
                || reused.validity.len() != self.slab.core_depth()
            {
                return Err(SpectralOperatorError::ReusableNormalStateMismatch);
            }
            let residual_grids = self
                .residual_grids
                .as_ref()
                .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?;
            let mut residual = Vec::with_capacity(output_cells);
            for residual_grid in residual_grids {
                for x in 0..self.geometry.image_shape[0] {
                    for y in 0..self.geometry.image_shape[1] {
                        let correction = self.gridder.image_correction(x, y);
                        residual.push(
                            residual_grid[(
                                self.geometry.image_blc[0] + x,
                                self.geometry.image_blc[1] + y,
                            )] * correction,
                        );
                    }
                }
            }
            if residual
                .iter()
                .chain(reused.psf.iter())
                .any(|value| !value.re.is_finite() || !value.im.is_finite())
                || reused.sensitivity.iter().any(|value| !value.is_finite())
                || reused.sum_weights.iter().any(|value| !value.is_finite())
            {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            return Ok(SpectralOperatorPrimitives {
                shape: self.geometry.image_shape,
                slab: self.slab,
                dirty: residual.into_boxed_slice(),
                invariant_dirty: reused.invariant_dirty,
                psf: reused.psf,
                sensitivity: reused.sensitivity,
                sum_weights: reused.sum_weights,
                validity: reused.validity,
                major_cycle_residual: None,
                major_cycle_residual_promoted: true,
                residual_model,
                #[cfg(test)]
                measurements: self.measurements,
            });
        }
        let dirty_grids = self
            .dirty_grids
            .as_ref()
            .ok_or(SpectralOperatorError::ReusableNormalStateMismatch)?;
        let psf_grids = self
            .psf_grids
            .as_ref()
            .ok_or(SpectralOperatorError::ReusableNormalStateMismatch)?;
        let mut dirty = Vec::with_capacity(output_cells);
        let mut psf = Vec::with_capacity(output_cells);
        let mut sensitivity = Vec::with_capacity(output_cells);
        let mut residual = self
            .residual_grids
            .as_ref()
            .map(|_| Vec::with_capacity(output_cells));
        for plane in 0..self.slab.core_depth() {
            for x in 0..self.geometry.image_shape[0] {
                for y in 0..self.geometry.image_shape[1] {
                    let correction = self.gridder.image_correction(x, y);
                    dirty.push(
                        dirty_grids[plane][(
                            self.geometry.image_blc[0] + x,
                            self.geometry.image_blc[1] + y,
                        )] * correction,
                    );
                    psf.push(
                        psf_grids[plane][(
                            self.geometry.image_blc[0] + x,
                            self.geometry.image_blc[1] + y,
                        )] * correction,
                    );
                    sensitivity.push(self.sum_weights[plane]);
                    if let (Some(grids), Some(values)) = (&self.residual_grids, residual.as_mut()) {
                        values.push(
                            grids[plane][(
                                self.geometry.image_blc[0] + x,
                                self.geometry.image_blc[1] + y,
                            )] * correction,
                        );
                    }
                }
            }
        }
        if dirty
            .iter()
            .chain(&psf)
            .chain(residual.iter().flatten())
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
            || self.sum_weights.iter().any(|value| !value.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let validity = self
            .mapped_samples
            .iter()
            .zip(&self.sum_weights)
            .map(|(mapped, weight)| {
                if *weight > 0.0 {
                    SpectralChannelValidity::Valid
                } else if *mapped > 0 {
                    SpectralChannelValidity::Blank
                } else {
                    SpectralChannelValidity::Unmapped
                }
            })
            .collect::<Vec<_>>();
        let dirty = dirty.into_boxed_slice();
        let invariant_dirty = Some(dirty.clone());
        Ok(SpectralOperatorPrimitives {
            shape: self.geometry.image_shape,
            slab: self.slab,
            invariant_dirty,
            dirty,
            psf: psf.into_boxed_slice(),
            sensitivity: sensitivity.into_boxed_slice(),
            sum_weights: self.sum_weights.into_boxed_slice(),
            validity: validity.into_boxed_slice(),
            major_cycle_residual: residual.map(Vec::into_boxed_slice),
            major_cycle_residual_promoted: initial_empty,
            residual_model,
            #[cfg(test)]
            measurements: self.measurements,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TapSpan {
    pub(crate) start: usize,
    pub(crate) weight_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SampleTaps {
    pub(crate) x: TapSpan,
    pub(crate) y: TapSpan,
}

pub(crate) struct StandardConvolution {
    grid_shape: [usize; 2],
    image_blc: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    weights: Box<[[f64; TAP_COUNT]]>,
    correction_x: Box<[f64]>,
    correction_y: Box<[f64]>,
}

impl StandardConvolution {
    pub(crate) fn new(geometry: &SpectralOperatorGeometry) -> Self {
        Self {
            grid_shape: geometry.grid_shape,
            image_blc: geometry.image_blc,
            du_lambda: 1.0 / (geometry.grid_shape[0] as f64 * geometry.increment_rad[0].abs()),
            dv_lambda: 1.0 / (geometry.grid_shape[1] as f64 * geometry.increment_rad[1].abs()),
            weights: build_normalized_tap_weights(),
            correction_x: build_correction_axis(geometry.grid_shape[0]),
            correction_y: build_correction_axis(geometry.grid_shape[1]),
        }
    }

    pub(crate) fn taps(&self, uv: [f64; 2]) -> Option<SampleTaps> {
        Some(SampleTaps {
            x: self.tap_span(
                uv[0] / self.du_lambda + self.grid_shape[0] as f64 / 2.0,
                self.grid_shape[0],
            )?,
            y: self.tap_span(
                -uv[1] / self.dv_lambda + self.grid_shape[1] as f64 / 2.0,
                self.grid_shape[1],
            )?,
        })
    }

    fn tap_span(&self, coordinate: f64, size: usize) -> Option<TapSpan> {
        if !coordinate.is_finite() {
            return None;
        }
        let anchor = coordinate.round() as isize;
        let offset = ((anchor as f64 - coordinate) * OVERSAMPLING as f64).round() as isize;
        let start = anchor - SUPPORT as isize;
        let end = anchor + SUPPORT as isize;
        let index = offset + OVERSAMPLING as isize / 2;
        (start >= 0 && end < size as isize && index >= 0 && index < self.weights.len() as isize)
            .then_some(TapSpan {
                start: start as usize,
                weight_index: index as usize,
            })
    }

    pub(crate) fn grid_compensated(
        &self,
        grid: &mut Array2<Complex64>,
        compensation: &mut Array2<Complex64>,
        taps: SampleTaps,
        value: Complex64,
    ) {
        debug_assert_eq!(grid.dim(), compensation.dim());
        let row_stride = grid.ncols();
        let grid = grid
            .as_slice_mut()
            .expect("spectral grids use standard contiguous layout");
        let compensation = compensation
            .as_slice_mut()
            .expect("spectral compensation uses standard contiguous layout");
        let x_weights = self.weights[taps.x.weight_index];
        let y_weights = self.weights[taps.y.weight_index];
        for (x, x_weight) in x_weights.into_iter().enumerate() {
            let start = (taps.x.start + x) * row_stride + taps.y.start;
            let grid_row = &mut grid[start..start + y_weights.len()];
            let compensation_row = &mut compensation[start..start + y_weights.len()];
            for ((grid_cell, compensation_cell), y_weight) in
                grid_row.iter_mut().zip(compensation_row).zip(y_weights)
            {
                let contribution = value * x_weight * y_weight - *compensation_cell;
                let updated = *grid_cell + contribution;
                *compensation_cell = (updated - *grid_cell) - contribution;
                *grid_cell = updated;
            }
        }
    }

    pub(crate) fn degrid(&self, grid: &Array2<Complex64>, taps: SampleTaps) -> Complex64 {
        let x_weights = self.weights[taps.x.weight_index];
        let y_weights = self.weights[taps.y.weight_index];
        let mut value = Complex64::new(0.0, 0.0);
        for (x, x_weight) in x_weights.into_iter().enumerate() {
            for (y, y_weight) in y_weights.into_iter().enumerate() {
                value += grid[(taps.x.start + x, taps.y.start + y)] * x_weight * y_weight;
            }
        }
        value
    }

    fn image_correction(&self, x: usize, y: usize) -> f64 {
        self.correction_x[self.image_blc[0] + x] * self.correction_y[self.image_blc[1] + y]
    }
}

pub(crate) struct PreparedFft {
    forward: [Arc<dyn Fft<f64>>; 2],
    inverse: [Arc<dyn Fft<f64>>; 2],
    lane: Vec<Complex64>,
    scratch: Vec<Complex64>,
}

impl PreparedFft {
    fn new(
        shape: [usize; 2],
        reserved_complex_values: usize,
    ) -> Result<Self, SpectralOperatorError> {
        let mut planner = FftPlanner::<f64>::new();
        let forward = [
            planner.plan_fft_forward(shape[0]),
            planner.plan_fft_forward(shape[1]),
        ];
        let inverse = [
            planner.plan_fft_inverse(shape[0]),
            planner.plan_fft_inverse(shape[1]),
        ];
        let lane_values = shape.into_iter().max().unwrap_or(0);
        let scratch_values = forward
            .iter()
            .chain(&inverse)
            .map(|fft| fft.get_inplace_scratch_len())
            .max()
            .unwrap_or(0);
        let opaque_plan_values = shape
            .into_iter()
            .try_fold(0_usize, |total, length| {
                length
                    .checked_mul(FFT_PLAN_COMPLEX_BOUND_PER_AXIS)
                    .and_then(|values| total.checked_add(values))
            })
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let required = lane_values
            .checked_add(scratch_values)
            .and_then(|values| values.checked_add(opaque_plan_values))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if required > reserved_complex_values {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(Self {
            forward,
            inverse,
            lane: vec![Complex64::default(); lane_values],
            scratch: vec![Complex64::default(); scratch_values],
        })
    }

    fn transform(&mut self, data: &mut Array2<Complex64>, inverse: bool) {
        shift_even(data);
        for axis in 0..2 {
            let fft = if inverse {
                &self.inverse[axis]
            } else {
                &self.forward[axis]
            };
            transform_axis(data, Axis(axis), fft, &mut self.lane, &mut self.scratch);
        }
        shift_even(data);
    }
}

impl fmt::Debug for PreparedFft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedFft")
    }
}

fn transform_axis(
    data: &mut Array2<Complex64>,
    axis: Axis,
    fft: &Arc<dyn Fft<f64>>,
    lane_workspace: &mut [Complex64],
    scratch_workspace: &mut [Complex64],
) {
    let length = data.len_of(axis);
    let values = &mut lane_workspace[..length];
    let scratch = &mut scratch_workspace[..fft.get_inplace_scratch_len()];
    for mut lane in data.lanes_mut(axis) {
        for (target, value) in values.iter_mut().zip(lane.iter()) {
            *target = *value;
        }
        fft.process_with_scratch(values, scratch);
        for (target, value) in lane.iter_mut().zip(values.iter()) {
            *target = *value;
        }
    }
}

fn shift_even(data: &mut Array2<Complex64>) {
    let [width, height] = [data.shape()[0], data.shape()[1]];
    debug_assert_eq!(width % 2, 0);
    debug_assert_eq!(height % 2, 0);
    for x in 0..width / 2 {
        for y in 0..height / 2 {
            data.swap((x, y), (x + width / 2, y + height / 2));
            data.swap((x + width / 2, y), (x, y + height / 2));
        }
    }
}

fn build_normalized_tap_weights() -> Box<[[f64; TAP_COUNT]]> {
    let half = OVERSAMPLING as isize / 2;
    (-half..=half)
        .map(|offset| {
            let mut weights = [0.0; TAP_COUNT];
            let mut sum = 0.0;
            for (tap, delta) in (-(SUPPORT as isize)..=SUPPORT as isize).enumerate() {
                let lookup = (delta * OVERSAMPLING as isize + offset).unsigned_abs();
                let value = if lookup < OVERSAMPLING * SUPPORT {
                    spheroidal_kernel(lookup as f64 / OVERSAMPLING as f64, SUPPORT as f64)
                } else {
                    0.0
                };
                weights[tap] = value;
                sum += value;
            }
            if sum > 0.0 {
                for value in &mut weights {
                    *value /= sum;
                }
            }
            weights
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn build_correction_axis(size: usize) -> Box<[f64]> {
    let center = size as f64 / 2.0;
    (0..size)
        .map(|index| {
            let nu = ((index as f64 - center).abs() / center).clamp(0.0, 1.0);
            let value = grdsf(nu);
            if value > 1.0e-6 {
                // casacore's ConvolveGridder correction vector is the raw
                // spheroidal response in each axis; image correction divides
                // by the product of those two vectors.  Do not renormalize to
                // the centre sample: grdsf(0) is close to, but not exactly,
                // one, and that changes the paired A/A* operator.
                1.0 / value
            } else {
                0.0
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn spheroidal_kernel(distance: f64, support: f64) -> f64 {
    if !(distance.is_finite() && distance <= support) {
        return 0.0;
    }
    let nu = distance / support;
    (1.0 - nu * nu) * grdsf(nu)
}

fn grdsf(nu: f64) -> f64 {
    const P0: [f64; 5] = [
        8.203_343e-2,
        -3.644_705e-1,
        6.278_660e-1,
        -5.335_581e-1,
        2.312_756e-1,
    ];
    const P1: [f64; 5] = [
        4.028_559e-3,
        -3.697_768e-2,
        1.021_332e-1,
        -1.201_436e-1,
        6.412_774e-2,
    ];
    const Q0: [f64; 3] = [1.0, 8.212_018e-1, 2.078_043e-1];
    const Q1: [f64; 3] = [1.0, 9.599_102e-1, 2.918_724e-1];
    if !(0.0..=1.0).contains(&nu) {
        return 0.0;
    }
    let (p, q, end) = if nu < 0.75 {
        (&P0, &Q0, 0.75)
    } else {
        (&P1, &Q1, 1.0)
    };
    let delta = nu * nu - end * end;
    let numerator = p
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    let denominator = q
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn casa_composite_padded_len(image_len: usize, factor: f64) -> usize {
    let mut padded = ((factor * image_len as f64 - 0.5).floor() as usize).max(image_len);
    if padded % 2 != 0 {
        padded += 1;
    }
    while !is_casa_composite_len(padded) {
        padded += 2;
    }
    padded
}

fn is_casa_composite_len(mut value: usize) -> bool {
    for factor in [2, 3, 5] {
        while value > 1 && value % factor == 0 {
            value /= factor;
        }
    }
    value == 1
}

fn checked_cells(shape: [usize; 2]) -> Result<usize, SpectralOperatorError> {
    shape[0]
        .checked_mul(shape[1])
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

/// Exact reason the spectral operator plan or operator rejected its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SpectralOperatorError {
    /// Runtime supplied science or weighting state for another compiled problem.
    #[error("spectral operator science and weighting state do not match the compiled problem")]
    ProblemMismatch,
    /// The problem is outside the scalar Stokes-I Constant/ChannelLocal surface.
    #[error(
        "spectral operator requires one scalar-response Stokes-I constant or channel-local domain"
    )]
    UnsupportedProblem,
    /// A requested output slab is empty or outside the compiled spectral axis.
    #[error("spectral operator slab is empty or outside the output spectral axis")]
    InvalidSlab,
    /// A sparse stencil contributing to the core escaped its sampler-derived halo.
    #[error("spectral operator slab lacks the complete paired spectral stencil halo")]
    IncompleteSpectralHalo,
    /// The current serial operator supports only centered identity-PC SIN geometry.
    #[error("spectral operator does not support this direction-coordinate geometry")]
    UnsupportedGeometry,
    /// The current serial operator requires permitted f64 compensated arithmetic.
    #[error("spectral operator requires f64 compensated numerical semantics")]
    UnsupportedNumerics,
    /// A resident-byte calculation overflowed.
    #[error("spectral operator residency cannot be represented")]
    ResidencyOverflow,
    /// A weighted contribution contains an invalid numerical value.
    #[error("spectral operator sample is non-finite or outside its numerical domain")]
    InvalidSample,
    /// The operator generated a non-finite value under a rejecting numerics contract.
    #[error("spectral operator generated a non-finite value")]
    GeneratedNonfinite,
    /// Weighted blocks arrived out of canonical replay order.
    #[error("spectral operator weighted block sequence is not canonical")]
    BlockSequence,
    /// Weighted blocks or completion carry different frozen W generations.
    #[error("spectral operator weighting generation changed during replay")]
    WeightingGeneration,
    /// A replay count could not be represented.
    #[error("spectral operator replay coverage overflowed")]
    CoverageOverflow,
    /// Terminal T18 evidence does not cover every consumed weighted block.
    #[error("spectral operator replay completion does not match consumed coverage")]
    IncompleteCoverage,
    /// A prediction model does not match the planned image shape.
    #[error("spectral operator model does not match the planned image shape")]
    ModelShape,
    /// A different model was named after residual replay was prepared.
    #[error("spectral operator residual belongs to another final model generation")]
    ModelMismatch,
    /// Residual replay was bound more than once or after samples were consumed.
    #[error("spectral operator major-cycle model must be bound exactly once before replay")]
    MajorCycleAlreadyBound,
    /// A diagnostic prediction attempted to replace the bound final-model grid.
    #[error("spectral operator prediction is unavailable after major-cycle model binding")]
    PredictionAfterMajorCycleBinding,
    /// T20 attempted to finalize T19 output that never accumulated an exact residual.
    #[error("spectral operator output lacks an exhaustive paired-operator residual")]
    MissingMajorCycleResidual,
    /// A later major pass did not carry the exact prior invariant normal state.
    #[error("spectral operator reusable normal state does not match the residual refresh")]
    ReusableNormalStateMismatch,
    /// The gridded replay seam currently accepts only constant-basis MFS.
    #[error("gridded normal-operator replay requires constant-basis MFS")]
    UnsupportedGriddedReplay,
    /// An opaque record, block, program, or invariant belongs to another replay.
    #[error("gridded normal-operator record does not match the sealed replay")]
    GriddedRecordMismatch,
    /// An opaque record has an unsupported, truncated, or invalid fixed encoding.
    #[error("gridded normal-operator record encoding is invalid")]
    InvalidGriddedRecord,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::time::Instant;

    use casa_imaging_model::{FiniteValuePolicy, LogicalIdentity, SpectralKernel};
    #[cfg(feature = "cpp-interop-tests")]
    use casa_test_support::gridder_interop::GridderOracle;
    use ndarray::Array2;
    use num_complex::Complex64;
    use sha2::{Digest, Sha256};

    #[cfg(feature = "cpp-interop-tests")]
    use super::{OVERSAMPLING, SPEED_OF_LIGHT_M_PER_S};
    use super::{
        PreparedFft, ReconstructionModelBinding, SUPPORT, SampleTaps, SpectralChannelValidity,
        SpectralOperatorError, SpectralOperatorGeometry, SpectralOperatorPass,
        SpectralOperatorPrimitives, SpectralOperatorSample, SpectralOperatorWorkload,
        SpectralSlabOperator, SpectralSlabPlan, StandardConvolution, TapSpan,
        apply_finite_value_policy, apply_input_policy, casa_persistent_complex, checked_cells,
        reconstruction_model_binding,
    };
    use crate::{ModelDeltaId, ModelGenerationId, ModelGenerationOrigin};

    fn geometry() -> SpectralOperatorGeometry {
        SpectralOperatorGeometry {
            image_shape: [8, 8],
            grid_shape: [10, 10],
            image_blc: [2, 2],
            increment_rad: [-2.0e-3, 2.0e-3],
        }
    }

    fn workload() -> SpectralOperatorWorkload {
        SpectralOperatorWorkload {
            pass: SpectralOperatorPass::InitialMajor,
            slab: SpectralSlabPlan {
                total_channels: 1,
                core_start: 0,
                core_end: 1,
                resident_start: 0,
                resident_end: 1,
            },
            grid_shape: [10, 10],
            grid_complex_values: 600,
            convolution_f64_values: 727,
            fft_resident_complex_values: 7_690,
            fft_planning_words: 320,
            forward_complex_values: 103,
            primitive_complex_values: 192,
            primitive_f64_values: 64,
            max_replay_block_samples: 3,
        }
    }

    fn operator() -> SpectralSlabOperator {
        let workload = workload();
        let fft = PreparedFft::new([10, 10], workload.fft_resident_complex_values)
            .expect("reserved FFT workspace");
        SpectralSlabOperator::new_with_geometry(geometry(), workload.slab, workload, fft)
    }

    fn samples(visibilities: &[[f64; 2]]) -> Vec<SpectralOperatorSample> {
        let coordinates = [[0.0, 0.0, 0.0], [18.0, -7.0, 0.0], [-11.0, 13.0, 0.0]];
        coordinates
            .into_iter()
            .zip(visibilities)
            .enumerate()
            .map(|(index, (uvw, visibility))| {
                SpectralOperatorSample::new(
                    0,
                    uvw,
                    1.0e9 + index as f64 * 1.0e7,
                    index as f64 * 0.01,
                    *visibility,
                    0.5 + index as f64,
                    0.75 + index as f64 * 0.1,
                )
                .expect("valid sample")
            })
            .collect()
    }

    fn inner(left: &[Complex64], right: &[Complex64]) -> Complex64 {
        left.iter()
            .zip(right)
            .map(|(left, right)| left.conj() * right)
            .sum()
    }

    fn cube_slab(core_start: usize, core_depth: usize) -> SpectralSlabPlan {
        SpectralSlabPlan::compile(4, core_start, core_depth, SpectralKernel::Linear)
            .expect("valid cube slab")
    }

    fn cube_operator(slab: SpectralSlabPlan) -> SpectralSlabOperator {
        cube_operator_with_geometry(slab, geometry())
    }

    fn cube_operator_with_geometry(
        slab: SpectralSlabPlan,
        geometry: SpectralOperatorGeometry,
    ) -> SpectralSlabOperator {
        let cells = 10 * 10;
        let workload = SpectralOperatorWorkload {
            pass: SpectralOperatorPass::InitialMajor,
            slab,
            grid_shape: [10, 10],
            grid_complex_values: 6 * cells * slab.core_depth(),
            convolution_f64_values: 727,
            fft_resident_complex_values: 7_690,
            fft_planning_words: 320,
            forward_complex_values: cells * slab.resident_depth() + 4,
            primitive_complex_values: 3 * 8 * 8 * slab.core_depth(),
            primitive_f64_values: 8 * 8 * slab.core_depth(),
            max_replay_block_samples: 4,
        };
        let fft = PreparedFft::new([10, 10], workload.fft_resident_complex_values)
            .expect("reserved FFT workspace");
        SpectralSlabOperator::new_with_geometry(geometry, slab, workload, fft)
    }

    fn cube_model() -> Vec<Complex64> {
        let cells = checked_cells(geometry().image_shape).expect("shape");
        (0..4)
            .flat_map(|channel| {
                (0..cells).map(move |pixel| {
                    Complex64::new(
                        channel as f64 * 0.2 + pixel as f64 * 0.003,
                        channel as f64 * -0.04 + pixel as f64 * 0.001,
                    )
                })
            })
            .collect()
    }

    fn cube_stencils() -> Vec<Vec<SpectralOperatorSample>> {
        let stencil = |terms: &[(usize, f64)], uvw, visibility, weight, frequency_hz| {
            terms
                .iter()
                .map(|(channel, factor)| {
                    SpectralOperatorSample::new(
                        *channel,
                        uvw,
                        frequency_hz,
                        0.017,
                        visibility,
                        weight,
                        *factor,
                    )
                    .expect("valid cube contribution")
                })
                .collect::<Vec<_>>()
        };
        vec![
            stencil(
                &[(0, 0.5), (1, 0.5)],
                [7.0, -3.0, 0.0],
                [1.0, -0.2],
                2.0,
                1.01e9,
            ),
            stencil(
                &[(1, 1.25), (2, -0.25)],
                [-5.0, 9.0, 0.0],
                [-0.3, 0.8],
                0.75,
                1.02e9,
            ),
            stencil(&[(2, 1.0)], [2.0, 4.0, 0.0], [0.4, 0.1], 0.0, 1.03e9),
        ]
    }

    fn cube_primitives(core_start: usize, core_depth: usize) -> SpectralOperatorPrimitives {
        let mut operator = cube_operator(cube_slab(core_start, core_depth));
        for stencil in cube_stencils() {
            for sample in stencil {
                operator.push(sample).expect("cube adjoint");
            }
        }
        operator.finish().expect("cube primitives")
    }

    #[test]
    fn forward_and_weighted_adjoint_share_one_operator() {
        let sample_values = samples(&[[0.4, -0.7], [-1.2, 0.3], [0.8, 1.1]]);
        let model = (0..checked_cells(geometry().image_shape).expect("shape"))
            .map(|index| Complex64::new(index as f64 * 0.01 - 0.2, index as f64 * -0.003))
            .collect::<Vec<_>>();
        let prediction = operator()
            .predict(&model, &sample_values)
            .expect("prediction");
        let visibility = sample_values
            .iter()
            .map(|sample| sample.visibility)
            .collect::<Vec<_>>();
        let weighted_visibility = sample_values
            .iter()
            .map(|sample| sample.visibility * sample.imaging_weight)
            .collect::<Vec<_>>();
        let mut adjoint = operator();
        for sample in &sample_values {
            adjoint.push(*sample).expect("adjoint sample");
        }
        let dirty = adjoint.finish().expect("finite primitives");
        let left = inner(&prediction, &weighted_visibility);
        let right = inner(&model, dirty.dirty());
        assert!((left - right).norm() <= 1.0e-9 * left.norm().max(right.norm()).max(1.0));
        assert_eq!(visibility.len(), prediction.len());
    }

    #[test]
    fn terminal_grid_support_remains_part_of_the_paired_operator() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let uv_lambda = [1.25 * gridder.du_lambda, -1.25 * gridder.dv_lambda];
        let taps = gridder
            .taps(uv_lambda)
            .expect("support ending on the terminal grid cells is valid");
        assert_eq!(taps.x.start + 2 * SUPPORT, geometry.grid_shape[0] - 1);
        assert_eq!(taps.y.start + 2 * SUPPORT, geometry.grid_shape[1] - 1);

        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let mut model = Array2::<Complex64>::zeros(shape);
        for x in taps.x.start..=taps.x.start + 2 * SUPPORT {
            for y in taps.y.start..=taps.y.start + 2 * SUPPORT {
                model[(x, y)] = Complex64::new(
                    0.13 * x as f64 - 0.07 * y as f64,
                    0.05 * x as f64 + 0.11 * y as f64,
                );
            }
        }
        let visibility = Complex64::new(1.25, -0.75);
        let prediction = gridder.degrid(&model, taps);
        let mut adjoint = Array2::<Complex64>::zeros(shape);
        let mut compensation = Array2::<Complex64>::zeros(shape);
        gridder.grid_compensated(&mut adjoint, &mut compensation, taps, visibility);

        assert_ne!(
            adjoint[(geometry.grid_shape[0] - 1, geometry.grid_shape[1] - 1)],
            Complex64::new(0.0, 0.0),
            "the terminal row and column must not be clipped from the convolution footprint"
        );
        let left = prediction.conj() * visibility;
        let right = model
            .iter()
            .zip(adjoint.iter())
            .map(|(model, adjoint)| model.conj() * adjoint)
            .sum::<Complex64>();
        assert!(
            (left - right).norm() <= 1.0e-13 * left.norm().max(right.norm()).max(1.0),
            "terminal-support A/A* mismatch: left={left:?} right={right:?}"
        );
    }

    #[test]
    fn deterministic_block_gridded_records_preserve_the_normal_operator() {
        let geometry = geometry();
        let gridder = StandardConvolution::new(&geometry);
        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let mut model_grid = Array2::<Complex64>::zeros(shape);
        for ((x, y), value) in model_grid.indexed_iter_mut() {
            *value = Complex64::new(
                (x * shape.1 + y) as f64 * 0.013 - 0.4,
                x as f64 * -0.017 + y as f64 * 0.009,
            );
        }
        let first = gridder.taps([0.0, 0.0]).expect("central taps");
        let second = gridder
            .taps([0.41 * gridder.du_lambda, -0.27 * gridder.dv_lambda])
            .expect("offset taps");
        let scalar_records = [
            (first, 0.75),
            (first, 1.25),
            (second, 0.4),
            (second, 0.6),
            (first, 0.125),
            (first, 0.375),
        ];

        let mut direct = Array2::<Complex64>::zeros(shape);
        let mut direct_compensation = Array2::<Complex64>::zeros(shape);
        for (taps, coefficient) in scalar_records {
            let predicted = gridder.degrid(&model_grid, taps);
            gridder.grid_compensated(
                &mut direct,
                &mut direct_compensation,
                taps,
                predicted * coefficient,
            );
        }

        let mut grouped = Array2::<Complex64>::zeros(shape);
        let mut grouped_compensation = Array2::<Complex64>::zeros(shape);
        let mut block_records = BTreeMap::<SampleTaps, f64>::new();
        for (taps, coefficient) in scalar_records {
            *block_records.entry(taps).or_default() += coefficient;
        }
        for (taps, coefficient) in &block_records {
            let predicted = gridder.degrid(&model_grid, *taps);
            gridder.grid_compensated(
                &mut grouped,
                &mut grouped_compensation,
                *taps,
                predicted * *coefficient,
            );
        }

        let squared_error = direct
            .iter()
            .zip(&grouped)
            .map(|(expected, actual)| (*actual - *expected).norm_sqr())
            .sum::<f64>();
        let squared_reference = direct.iter().map(Complex64::norm_sqr).sum::<f64>();
        let normalized_rms = (squared_error / squared_reference.max(f64::MIN_POSITIVE)).sqrt();
        assert!(normalized_rms <= 1.0e-15, "normalized RMS {normalized_rms}");
        assert_eq!(block_records.len(), 2);
        assert_eq!(scalar_records.len() / block_records.len(), 3);
    }

    #[test]
    #[ignore = "requires CASA_RS_IMPERF_REPLAY_ARTIFACT from the mounted replay discriminator"]
    fn mounted_gridded_replay_artifact_streams_through_the_normal_operator() {
        let path = std::env::var_os("CASA_RS_IMPERF_REPLAY_ARTIFACT")
            .expect("CASA_RS_IMPERF_REPLAY_ARTIFACT must name the discriminator artifact");
        let file = File::open(path).expect("open gridded replay artifact");
        let bytes = file.metadata().expect("read artifact metadata").len();
        assert_eq!(
            bytes % 16,
            0,
            "gridded replay artifact has a partial record"
        );
        let records = bytes / 16;

        let cell = 0.25 * std::f64::consts::PI / (180.0 * 3600.0);
        let geometry = SpectralOperatorGeometry {
            image_shape: [1024, 1024],
            grid_shape: [1024, 1024],
            image_blc: [0, 0],
            increment_rad: [-cell, cell],
        };
        let gridder = StandardConvolution::new(&geometry);
        let shape = (geometry.grid_shape[0], geometry.grid_shape[1]);
        let mut model_grid = Array2::<Complex64>::zeros(shape);
        for ((x, y), value) in model_grid.indexed_iter_mut() {
            *value = Complex64::new(
                ((x * shape.1 + y) % 997) as f64 * 1.0e-4 - 0.05,
                (x as f64 - y as f64) * 2.0e-5,
            );
        }
        let mut output = Array2::<Complex64>::zeros(shape);
        let mut compensation = Array2::<Complex64>::zeros(shape);
        let mut reader = BufReader::with_capacity(8 << 20, file);
        let mut record = [0_u8; 16];
        let started = Instant::now();
        for _ in 0..records {
            reader
                .read_exact(&mut record)
                .expect("read complete gridded replay record");
            let key = u64::from_le_bytes(record[..8].try_into().expect("tap key width"));
            let coefficient =
                f64::from_le_bytes(record[8..].try_into().expect("coefficient width"));
            assert!(coefficient.is_finite() && coefficient >= 0.0);
            let taps = SampleTaps {
                x: TapSpan {
                    start: (key & 0x0fff) as usize,
                    weight_index: ((key >> 24) & 0x7f) as usize,
                },
                y: TapSpan {
                    start: ((key >> 12) & 0x0fff) as usize,
                    weight_index: ((key >> 31) & 0x7f) as usize,
                },
            };
            let predicted = gridder.degrid(&model_grid, taps);
            gridder.grid_compensated(
                &mut output,
                &mut compensation,
                taps,
                predicted * coefficient,
            );
        }
        let elapsed = started.elapsed();
        let mut hasher = Sha256::new();
        for value in &output {
            hasher.update(value.re.to_le_bytes());
            hasher.update(value.im.to_le_bytes());
        }
        println!(
            "gridded_replay_apply records={records} bytes={bytes} wall_ms={:.6} sha256={:x}",
            elapsed.as_secs_f64() * 1_000.0,
            hasher.finalize()
        );
    }

    #[test]
    fn t37_channel_local_forward_and_adjoint_share_signed_sparse_stencils() {
        let stencils = cube_stencils();
        let model = cube_model();
        let mut forward = cube_operator(cube_slab(0, 4));
        forward
            .prepare_prediction_grid(&model)
            .expect("prepare cube model");
        let predictions = stencils
            .iter()
            .map(|stencil| forward.predict_stencil(stencil).expect("cube prediction"))
            .collect::<Vec<_>>();

        let mut adjoint = cube_operator(cube_slab(0, 4));
        for stencil in &stencils {
            for sample in stencil {
                adjoint.push(*sample).expect("cube adjoint");
            }
        }
        let dirty = adjoint.finish().expect("cube normal state");
        let left = predictions
            .iter()
            .zip(&stencils)
            .map(|(prediction, stencil)| {
                prediction.conj() * stencil[0].visibility * stencil[0].imaging_weight
            })
            .sum::<Complex64>();
        let right = inner(&model, dirty.dirty());
        assert!(
            (left - right).norm() <= 1.0e-9 * left.norm().max(right.norm()).max(1.0),
            "paired channel-local A/A* mismatch: left={left:?} right={right:?}"
        );
    }

    #[test]
    fn t37_slab_depths_one_two_and_full_are_bitwise_identical() {
        let full = cube_primitives(0, 4);
        let run_partition = |depth: usize| {
            let mut dirty = Vec::new();
            let mut psf = Vec::new();
            let mut sensitivity = Vec::new();
            let mut sum_weights = Vec::new();
            let mut validity = Vec::new();
            for start in (0..4).step_by(depth) {
                let slab = cube_primitives(start, depth.min(4 - start));
                dirty.extend_from_slice(slab.dirty());
                psf.extend_from_slice(slab.psf());
                sensitivity.extend_from_slice(slab.sensitivity());
                sum_weights.extend_from_slice(slab.sum_weights());
                validity.extend_from_slice(slab.channel_validity());
            }
            (dirty, psf, sensitivity, sum_weights, validity)
        };
        for depth in [1, 2] {
            let (dirty, psf, sensitivity, sum_weights, validity) = run_partition(depth);
            assert_eq!(dirty, full.dirty(), "dirty changed at slab depth {depth}");
            assert_eq!(psf, full.psf(), "PSF changed at slab depth {depth}");
            assert_eq!(
                sensitivity,
                full.sensitivity(),
                "sensitivity changed at slab depth {depth}"
            );
            assert_eq!(
                sum_weights,
                full.sum_weights(),
                "sum weight changed at slab depth {depth}"
            );
            assert_eq!(
                validity,
                full.channel_validity(),
                "validity changed at slab depth {depth}"
            );
        }
        assert_eq!(
            full.channel_validity(),
            &[
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Unmapped,
            ]
        );
    }

    #[test]
    fn t37_blank_and_unmapped_channels_remain_distinct() {
        let mut operator = cube_operator(cube_slab(2, 2));
        let zero_weight =
            SpectralOperatorSample::new(2, [2.0, 4.0, 0.0], 1.03e9, 0.0, [0.4, 0.1], 0.0, 1.0)
                .expect("mapped zero-weight sample");
        operator.push(zero_weight).expect("blank channel sample");
        let primitives = operator.finish().expect("blank normal state");
        assert_eq!(primitives.slab().core_range(), 2..4);
        assert_eq!(
            primitives.channel_validity(),
            &[
                SpectralChannelValidity::Blank,
                SpectralChannelValidity::Unmapped,
            ]
        );
        assert_eq!(primitives.sum_weights(), &[0.0, 0.0]);
    }

    #[test]
    fn t37_sampler_halo_makes_prediction_independent_of_core_partition() {
        let stencils = cube_stencils();
        let model = cube_model();
        let mut full = cube_operator(cube_slab(0, 4));
        full.prepare_prediction_grid(&model).expect("full model");
        let expected = stencils
            .iter()
            .map(|stencil| full.predict_stencil(stencil).expect("full prediction"))
            .collect::<Vec<_>>();
        for start in 0..4 {
            let slab = cube_slab(start, 1);
            let mut operator = cube_operator(slab);
            operator
                .prepare_prediction_grid(&model)
                .expect("slab model");
            for (stencil, expected) in stencils.iter().zip(&expected) {
                if stencil
                    .iter()
                    .any(|sample| slab.owns(sample.output_channel))
                {
                    assert_eq!(
                        operator.predict_stencil(stencil).expect("slab prediction"),
                        *expected
                    );
                }
            }
        }
    }

    #[cfg(feature = "cpp-interop-tests")]
    #[test]
    fn t37_casacore_cube_dirty_psf_sum_weight_and_normal_state_match() {
        const CHANNELS: usize = 4;
        const CELLS: usize = 8 * 8;
        const OUTPUT_FREQUENCIES_HZ: [f64; CHANNELS] = [1.03e9, 1.02e9, 1.01e9, 1.00e9];
        const SELECTED_OUTPUT_CHANNELS: [usize; 3] = [2, 0, 1];
        type OracleSample = ([f64; 3], [f64; 2], f64);

        let oracle_geometry = SpectralOperatorGeometry {
            image_blc: [1, 1],
            ..geometry()
        };
        let mut rust = cube_operator_with_geometry(cube_slab(0, CHANNELS), oracle_geometry);
        let mut per_channel: Vec<Vec<OracleSample>> = vec![Vec::new(); CHANNELS];
        let mut selected_sample_identities = Vec::new();
        for channel in SELECTED_OUTPUT_CHANNELS {
            let channel_samples = if channel == 1 {
                vec![([4.0, -6.0, 0.0], [9.0, -7.0], 0.0)]
            } else {
                vec![
                    (
                        [7.0 + channel as f64, -3.0 + channel as f64, 0.0],
                        [1.0 + channel as f64 * 0.25, -0.2],
                        0.75 + channel as f64 * 0.25,
                    ),
                    (
                        [-5.0 - channel as f64, 9.0 - channel as f64, 0.0],
                        [-0.3, 0.8 + channel as f64 * 0.125],
                        1.25 + channel as f64 * 0.5,
                    ),
                ]
            };
            for &(uvw, visibility, weight) in &channel_samples {
                let sample = SpectralOperatorSample::new(
                    channel,
                    uvw,
                    OUTPUT_FREQUENCIES_HZ[channel],
                    0.0,
                    visibility,
                    weight,
                    1.0,
                )
                .expect("valid CASA comparison sample");
                selected_sample_identities
                    .push((sample.output_channel, sample.frequency_hz.to_bits()));
                rust.push(sample).expect("grid CASA comparison sample");
            }
            per_channel[channel] = channel_samples;
        }
        let operator_dirty_grids = rust
            .dirty_grids
            .clone()
            .expect("initial pass owns dirty grids");
        let operator_psf_grids = rust.psf_grids.clone().expect("initial pass owns PSF grids");
        let primitives = rust.finish().expect("cube normal state");
        assert_eq!(primitives.slab(), cube_slab(0, CHANNELS));
        assert_eq!(
            OUTPUT_FREQUENCIES_HZ.map(f64::to_bits),
            [
                1.03e9_f64.to_bits(),
                1.02e9_f64.to_bits(),
                1.01e9_f64.to_bits(),
                1.00e9_f64.to_bits(),
            ],
            "logical output-channel identities must retain descending frequency order"
        );
        assert_eq!(
            selected_sample_identities,
            [
                (2, 1.01e9_f64.to_bits()),
                (2, 1.01e9_f64.to_bits()),
                (0, 1.03e9_f64.to_bits()),
                (0, 1.03e9_f64.to_bits()),
                (1, 1.02e9_f64.to_bits()),
            ],
            "selected source-sample identities must retain contribution order"
        );
        assert_eq!(
            primitives.channel_validity(),
            &[
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Blank,
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Unmapped,
            ]
        );

        let grid_shape = oracle_geometry.grid_shape;
        let image_shape = oracle_geometry.image_shape;
        let scale = [
            grid_shape[0] as f64 * oracle_geometry.increment_rad[0].abs(),
            grid_shape[1] as f64 * oracle_geometry.increment_rad[1].abs(),
        ];
        let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];
        let grid_array_shape = (grid_shape[0], grid_shape[1]);
        let rust_gridder = StandardConvolution::new(&oracle_geometry);
        let mut casa_grids = (0..CHANNELS)
            .map(|_| {
                (
                    Array2::<Complex64>::zeros(grid_array_shape),
                    Array2::<Complex64>::zeros(grid_array_shape),
                    Array2::<Complex64>::zeros(grid_array_shape),
                    Array2::<Complex64>::zeros(grid_array_shape),
                    0.0_f64,
                )
            })
            .collect::<Vec<_>>();
        for (channel, (samples, (dirty, psf, rust_dirty, rust_psf, sum_weight))) in
            per_channel.iter().zip(&mut casa_grids).enumerate()
        {
            let mut dirty_compensation = Array2::<Complex64>::zeros(grid_array_shape);
            let mut psf_compensation = Array2::<Complex64>::zeros(grid_array_shape);
            for (uvw, visibility, weight) in samples {
                let frequency_hz = OUTPUT_FREQUENCIES_HZ[channel];
                let uv_lambda = [
                    uvw[0] * frequency_hz / SPEED_OF_LIGHT_M_PER_S,
                    uvw[1] * frequency_hz / SPEED_OF_LIGHT_M_PER_S,
                ];
                let patch = GridderOracle::grid_unit_sample_2d(
                    grid_shape,
                    scale,
                    offset,
                    [uv_lambda[0], -uv_lambda[1]],
                )
                .expect("casacore convolution patch");
                assert_eq!(patch.support, SUPPORT as i32);
                assert_eq!(patch.sampling, OVERSAMPLING as i32);
                let weighted_visibility = Complex64::new(visibility[0], visibility[1]) * *weight;
                let taps = rust_gridder.taps(uv_lambda).expect("Rust convolution taps");
                rust_gridder.grid_compensated(
                    rust_dirty,
                    &mut dirty_compensation,
                    taps,
                    weighted_visibility,
                );
                rust_gridder.grid_compensated(
                    rust_psf,
                    &mut psf_compensation,
                    taps,
                    Complex64::new(*weight, 0.0),
                );
                for cell in patch.cells {
                    let tap = Complex64::new(f64::from(cell.re), f64::from(cell.im));
                    dirty[(cell.x, cell.y)] += weighted_visibility * tap;
                    psf[(cell.x, cell.y)] += *weight * tap;
                }
                *sum_weight += *weight;
            }
        }
        let mut casa_fft = PreparedFft::new(grid_shape, 7_690).expect("CASA comparison FFT");
        for (channel, (casa_dirty, casa_psf, rust_dirty_grid, rust_psf_grid, casa_sum_weight)) in
            casa_grids.iter_mut().enumerate()
        {
            let expected_validity = primitives.channel_validity()[channel];
            let rust_sum_weight = primitives.sum_weights()[channel];
            assert_eq!(*casa_sum_weight, rust_sum_weight);
            assert!(
                primitives.sensitivity()[channel * CELLS..(channel + 1) * CELLS]
                    .iter()
                    .all(|sensitivity| *sensitivity == rust_sum_weight)
            );
            if expected_validity != SpectralChannelValidity::Valid {
                assert_eq!(rust_sum_weight, 0.0);
                assert!(
                    operator_dirty_grids[channel]
                        .iter()
                        .chain(&operator_psf_grids[channel])
                        .chain(rust_dirty_grid.iter())
                        .chain(rust_psf_grid.iter())
                        .chain(casa_dirty.iter())
                        .chain(casa_psf.iter())
                        .all(|value| *value == Complex64::new(0.0, 0.0))
                );
                assert!(
                    primitives.dirty()[channel * CELLS..(channel + 1) * CELLS]
                        .iter()
                        .chain(&primitives.psf()[channel * CELLS..(channel + 1) * CELLS])
                        .all(|value| *value == Complex64::new(0.0, 0.0))
                );
                continue;
            }
            let operator_dirty_nrmse = operator_dirty_grids[channel]
                .iter()
                .zip(rust_dirty_grid.iter())
                .map(|(operator, reconstructed)| (*operator - *reconstructed).norm_sqr())
                .sum::<f64>()
                / rust_dirty_grid
                    .iter()
                    .map(|value| value.norm_sqr())
                    .sum::<f64>();
            let operator_psf_nrmse = operator_psf_grids[channel]
                .iter()
                .zip(rust_psf_grid.iter())
                .map(|(operator, reconstructed)| (*operator - *reconstructed).norm_sqr())
                .sum::<f64>()
                / rust_psf_grid
                    .iter()
                    .map(|value| value.norm_sqr())
                    .sum::<f64>();
            assert!(
                operator_dirty_nrmse.sqrt() <= 1.0e-12,
                "operator dirty grid must use the same metre-to-wavelength coordinates: {}",
                operator_dirty_nrmse.sqrt()
            );
            assert!(
                operator_psf_nrmse.sqrt() <= 1.0e-12,
                "operator PSF grid must use the same metre-to-wavelength coordinates: {}",
                operator_psf_nrmse.sqrt()
            );
            let grid_dirty_nrmse = rust_dirty_grid
                .iter()
                .zip(casa_dirty.iter())
                .map(|(rust, casa)| (*rust - *casa).norm_sqr())
                .sum::<f64>()
                / casa_dirty.iter().map(|value| value.norm_sqr()).sum::<f64>();
            let grid_psf_nrmse = rust_psf_grid
                .iter()
                .zip(casa_psf.iter())
                .map(|(rust, casa)| (*rust - *casa).norm_sqr())
                .sum::<f64>()
                / casa_psf.iter().map(|value| value.norm_sqr()).sum::<f64>();
            assert!(
                grid_dirty_nrmse.sqrt() <= 1.0e-3,
                "dirty convolution grid mismatch: {}",
                grid_dirty_nrmse.sqrt()
            );
            assert!(
                grid_psf_nrmse.sqrt() <= 1.0e-3,
                "PSF convolution grid mismatch: {}",
                grid_psf_nrmse.sqrt()
            );
            casa_fft.transform(casa_dirty, true);
            casa_fft.transform(casa_psf, true);
            let mut dirty_difference_energy = 0.0;
            let mut dirty_casa_energy = 0.0;
            let mut psf_difference_energy = 0.0;
            let mut psf_casa_energy = 0.0;
            for y in 0..image_shape[1] {
                let grid_y = oracle_geometry.image_blc[1] + y;
                let correction =
                    GridderOracle::correction_row_2d(grid_shape, scale, offset, grid_y)
                        .expect("casacore correction row");
                for x in 0..image_shape[0] {
                    let grid_x = oracle_geometry.image_blc[0] + x;
                    let inverse_correction = if correction[grid_x].abs() > 1.0e-6 {
                        1.0 / f64::from(correction[grid_x])
                    } else {
                        0.0
                    };
                    let pixel = x * image_shape[1] + y;
                    let casa_dirty = casa_dirty[(grid_x, grid_y)] * inverse_correction;
                    let casa_psf = casa_psf[(grid_x, grid_y)] * inverse_correction;
                    let rust_dirty = primitives.dirty()[channel * CELLS + pixel];
                    let rust_psf = primitives.psf()[channel * CELLS + pixel];
                    dirty_difference_energy += (rust_dirty - casa_dirty).norm_sqr();
                    dirty_casa_energy += casa_dirty.norm_sqr();
                    psf_difference_energy += (rust_psf - casa_psf).norm_sqr();
                    psf_casa_energy += casa_psf.norm_sqr();
                }
            }
            let dirty_normalized_rms = (dirty_difference_energy / dirty_casa_energy).sqrt();
            let psf_normalized_rms = (psf_difference_energy / psf_casa_energy).sqrt();
            assert!(
                dirty_normalized_rms <= 1.0e-3,
                "dirty channel {channel} exceeds CASA normalized-RMS contract: {dirty_normalized_rms}"
            );
            assert!(
                psf_normalized_rms <= 1.0e-3,
                "PSF channel {channel} exceeds CASA normalized-RMS contract: {psf_normalized_rms}"
            );
        }
    }

    #[test]
    fn evaluated_spectral_frame_is_shared_by_forward_and_adjoint() {
        let cells = checked_cells(geometry().image_shape).expect("shape");
        let mut model = vec![Complex64::default(); cells];
        model[2 * geometry().image_shape[1] + 5] = Complex64::new(0.75, -0.2);
        let sample_at = |frequency_hz| {
            SpectralOperatorSample::new(
                0,
                [18.0, -7.0, 0.0],
                frequency_hz,
                0.37,
                [0.4, -0.7],
                1.25,
                0.8,
            )
            .expect("valid frame-evaluated sample")
        };
        let native = sample_at(1.0e9);
        let shifted = sample_at(1.006e9);
        let native_prediction = operator()
            .predict(&model, &[native])
            .expect("native-frame prediction")[0];
        let shifted_prediction = operator()
            .predict(&model, &[shifted])
            .expect("shifted-frame prediction")[0];
        assert_ne!(
            native_prediction, shifted_prediction,
            "the evaluated operator frequency must affect A"
        );

        for (sample, prediction) in [(native, native_prediction), (shifted, shifted_prediction)] {
            let weighted_visibility = sample.visibility * sample.imaging_weight;
            let mut adjoint = operator();
            adjoint.push(sample).expect("adjoint sample");
            let dirty = adjoint.finish().expect("finite primitives");
            let left = prediction.conj() * weighted_visibility;
            let right = inner(&model, dirty.dirty());
            assert!(
                (left - right).norm() <= 1.0e-9 * left.norm().max(right.norm()).max(1.0),
                "A and A* must use the same owner-evaluated frequency"
            );
        }
    }

    #[test]
    fn forward_is_linear_and_a_unit_centre_source_is_constant() {
        let sample_values = samples(&[[0.0, 0.0]; 3]);
        let cells = checked_cells(geometry().image_shape).expect("shape");
        let mut first = vec![Complex64::new(0.0, 0.0); cells];
        let mut second = first.clone();
        first[3 * 8 + 3] = Complex64::new(1.0, 0.0);
        second[3 * 8 + 5] = Complex64::new(-0.25, 0.5);
        let sum = first
            .iter()
            .zip(&second)
            .map(|(first, second)| first + second)
            .collect::<Vec<_>>();
        let mut operator = operator();
        let first_prediction = operator.predict(&first, &sample_values).expect("first");
        let second_prediction = operator.predict(&second, &sample_values).expect("second");
        let sum_prediction = operator.predict(&sum, &sample_values).expect("sum");
        for ((sum, first), second) in sum_prediction
            .iter()
            .zip(&first_prediction)
            .zip(&second_prediction)
        {
            assert!((*sum - *first - *second).norm() <= 1.0e-12);
        }
        for (prediction, sample) in first_prediction.iter().zip(&sample_values) {
            let expected = sample.phase().conj() * sample.spectral_factor;
            assert!(
                (*prediction - expected).norm() <= 1.0e-12,
                "unit source mismatch: predicted={prediction:?} expected={expected:?}"
            );
        }
    }

    #[test]
    fn physical_block_partition_does_not_change_primitives() {
        let sample_values = samples(&[[0.4, -0.7], [-1.2, 0.3], [0.8, 1.1]]);
        let run = |partitions: &[&[SpectralOperatorSample]]| {
            let mut operator = operator();
            for sample in partitions.iter().flat_map(|partition| partition.iter()) {
                operator.push(*sample).expect("sample");
            }
            operator.finish().expect("finite primitives")
        };
        let one = run(&[&sample_values]);
        let split = run(&[&sample_values[..1], &sample_values[1..]]);
        assert_eq!(one.dirty(), split.dirty());
        assert_eq!(one.psf(), split.psf());
        assert_eq!(one.sensitivity(), split.sensitivity());
        assert_eq!(one.sum_weight(), split.sum_weight());
    }

    #[test]
    fn samples_outside_the_planned_grid_contribute_zero_to_both_operators() {
        let sample =
            SpectralOperatorSample::new(0, [1.0e9, -1.0e9, 0.0], 1.0e9, 0.0, [3.0, -2.0], 4.0, 1.0)
                .expect("finite sample");
        let model = vec![Complex64::new(1.0, -0.5); checked_cells(geometry().image_shape).unwrap()];
        let prediction = operator()
            .predict(&model, &[sample])
            .expect("out-of-grid prediction is defined");
        assert_eq!(prediction.as_ref(), &[Complex64::new(0.0, 0.0)]);

        let mut adjoint = operator();
        adjoint
            .push(sample)
            .expect("out-of-grid adjoint is defined");
        let primitives = adjoint.finish().expect("finite primitives");
        assert!(
            primitives
                .dirty()
                .iter()
                .all(|value| *value == Complex64::new(0.0, 0.0))
        );
        assert!(
            primitives
                .psf()
                .iter()
                .all(|value| *value == Complex64::new(0.0, 0.0))
        );
        assert_eq!(primitives.sum_weight(), 0.0);
    }

    #[test]
    fn workload_reports_every_runtime_projected_buffer_once() {
        let workload = workload();
        assert_eq!(workload.grid_complex_values(), 6 * 10 * 10);
        assert_eq!(workload.convolution_f64_values(), 101 * 7 + 20);
        assert!(workload.fft_resident_complex_values() >= 4 * 10);
        assert_eq!(workload.fft_planning_words(), 16 * 20);
        assert_eq!(workload.forward_complex_values(), 10 * 10 + 3);
        assert_eq!(workload.primitive_complex_values(), 3 * 8 * 8);
        assert_eq!(workload.primitive_f64_values(), 8 * 8);
    }

    #[test]
    fn finite_policy_flags_declared_input_but_rejects_generated_values() {
        assert_eq!(
            apply_finite_value_policy(true, FiniteValuePolicy::FlagInputRejectGenerated),
            Ok(false)
        );
        assert_eq!(
            apply_finite_value_policy(true, FiniteValuePolicy::RejectAll),
            Err(SpectralOperatorError::InvalidSample)
        );
        assert_eq!(
            apply_input_policy(true, true, FiniteValuePolicy::RejectAll),
            Err(SpectralOperatorError::InvalidSample),
            "RejectAll must reject a flagged non-finite input rather than silently skip it"
        );
        assert_eq!(
            apply_input_policy(true, true, FiniteValuePolicy::FlagInputRejectGenerated),
            Ok(false)
        );
        let mut model =
            vec![Complex64::new(0.0, 0.0); checked_cells(geometry().image_shape).expect("shape")];
        model[0] = Complex64::new(f64::NAN, 0.0);
        assert_eq!(
            operator().prepare_prediction_grid(&model),
            Err(SpectralOperatorError::GeneratedNonfinite)
        );
    }

    #[test]
    fn empty_model_residual_reproduces_the_dirty_plane_bit_exactly() {
        let values = samples(&[[1.0, 0.0], [2.0, -1.0], [0.5, 0.25]]);
        let mut state = operator();
        let model = vec![Complex64::default(); checked_cells(geometry().image_shape).unwrap()];
        state.prepare_prediction_grid(&model).expect("empty model");
        let shape = (geometry().grid_shape[0], geometry().grid_shape[1]);
        state.residual_grids = Some(vec![Array2::zeros(shape)]);
        state.residual_compensations = Some(vec![Array2::zeros(shape)]);
        for sample in values {
            state
                .push_with_residual(sample, Complex64::default())
                .expect("paired residual");
        }
        let primitives = state.finish_bound(None).expect("primitives");
        assert_eq!(
            primitives.major_cycle_residual.as_deref(),
            Some(primitives.dirty()),
            "an empty model must reproduce the data-side adjoint bit exactly"
        );
    }

    #[test]
    fn empty_initial_binding_matches_the_explicit_zero_operator_with_less_work() {
        let values = samples(&[[1.0, 0.0], [2.0, -1.0], [0.5, 0.25]]);
        let zero_model = vec![Complex64::default(); checked_cells(geometry().image_shape).unwrap()];
        let model = ModelGenerationId(LogicalIdentity::from_sha256([0; 32]));

        let mut explicit = operator();
        let predicted = explicit
            .predict(&zero_model, &values)
            .expect("explicit zero forward prediction");
        let shape = (geometry().grid_shape[0], geometry().grid_shape[1]);
        explicit.residual_grids = Some(vec![Array2::zeros(shape)]);
        explicit.residual_compensations = Some(vec![Array2::zeros(shape)]);
        for (sample, prediction) in values.iter().copied().zip(predicted) {
            explicit
                .push_with_residual(sample, prediction)
                .expect("explicit zero residual");
        }
        let explicit = explicit
            .finish_bound(Some(ReconstructionModelBinding::Evaluated(model)))
            .expect("explicit primitives");

        let mut optimized = operator();
        for sample in values {
            optimized.push(sample).expect("initial data and PSF");
        }
        let optimized = optimized
            .finish_bound(Some(ReconstructionModelBinding::InitialCertifiedZero(
                model,
            )))
            .expect("optimized primitives");

        assert_eq!(
            optimized.measurements,
            super::SpectralOperatorMeasurements {
                dirty_grid_tap_visits: 3 * 49,
                psf_grid_tap_visits: 3 * 49,
                inverse_dirty_fft_planes: 1,
                inverse_psf_fft_planes: 1,
                ..super::SpectralOperatorMeasurements::default()
            },
            "empty initial binding must perform only one dirty and one PSF adjoint"
        );
        assert_eq!(
            explicit.measurements,
            super::SpectralOperatorMeasurements {
                forward_fft_planes: 1,
                prediction_degrid_tap_visits: 3 * 49,
                dirty_grid_tap_visits: 3 * 49,
                residual_grid_tap_visits: 3 * 49,
                psf_grid_tap_visits: 3 * 49,
                inverse_dirty_fft_planes: 1,
                inverse_residual_fft_planes: 1,
                inverse_psf_fft_planes: 1,
                ..super::SpectralOperatorMeasurements::default()
            },
            "explicit zero evaluation is the independent work-count reference"
        );
        assert!(optimized.major_cycle_residual_promoted);
        assert!(optimized.major_cycle_residual.is_none());
        assert_eq!(optimized.residual_model, Some(model));
        assert_eq!(explicit.residual_model, Some(model));

        let explicit = explicit
            .promote_major_cycle_residual(model)
            .expect("promote explicit zero residual");
        let optimized = optimized
            .promote_major_cycle_residual(model)
            .expect("empty residual is already the dirty plane");
        assert_eq!(optimized.dirty(), explicit.dirty());
        assert_eq!(optimized.psf(), explicit.psf());
        assert_eq!(optimized.sensitivity(), explicit.sensitivity());
        assert_eq!(optimized.sum_weights(), explicit.sum_weights());
        assert_eq!(optimized.channel_validity(), explicit.channel_validity());
        assert_eq!(
            optimized.normal_state_content_identity(),
            explicit.normal_state_content_identity()
        );
    }

    #[test]
    fn only_empty_origin_on_the_initial_pass_is_certified_zero() {
        let generation = ModelGenerationId(LogicalIdentity::from_sha256([1; 32]));
        let source = LogicalIdentity::from_sha256([2; 32]);
        let delta = ModelDeltaId(LogicalIdentity::from_sha256([3; 32]));

        assert_eq!(
            reconstruction_model_binding(
                SpectralOperatorPass::InitialMajor,
                generation,
                ModelGenerationOrigin::Empty,
            ),
            ReconstructionModelBinding::InitialCertifiedZero(generation)
        );
        for (pass, origin) in [
            (
                SpectralOperatorPass::InitialMajor,
                ModelGenerationOrigin::Ingested {
                    source,
                    reprojection: None,
                },
            ),
            (
                SpectralOperatorPass::InitialMajor,
                ModelGenerationOrigin::Delta {
                    base: generation,
                    delta,
                },
            ),
            (
                SpectralOperatorPass::ResidualRefresh,
                ModelGenerationOrigin::Empty,
            ),
        ] {
            assert_eq!(
                reconstruction_model_binding(pass, generation, origin),
                ReconstructionModelBinding::Evaluated(generation)
            );
        }
    }

    #[test]
    fn residual_matches_explicit_paired_forward_then_weighted_adjoint() {
        let values = samples(&[[1.0, 0.0], [2.0, -1.0], [0.5, 0.25]]);
        let mut model = vec![Complex64::default(); checked_cells(geometry().image_shape).unwrap()];
        model[3 * geometry().image_shape[1] + 5] = Complex64::new(1.0, 0.0);

        let predicted = operator()
            .predict(&model, &values)
            .expect("paired forward prediction");
        let mut explicit = operator();
        for (mut sample, prediction) in values.iter().copied().zip(predicted.iter().copied()) {
            sample.visibility -= prediction;
            explicit.push(sample).expect("explicit residual adjoint");
        }
        let expected = explicit.finish().expect("explicit normal residual");

        let mut fused = operator();
        fused
            .prepare_prediction_grid(&model)
            .expect("prepare model");
        let shape = (geometry().grid_shape[0], geometry().grid_shape[1]);
        fused.residual_grids = Some(vec![Array2::zeros(shape)]);
        fused.residual_compensations = Some(vec![Array2::zeros(shape)]);
        for (sample, prediction) in values.into_iter().zip(predicted) {
            fused
                .push_with_residual(sample, prediction)
                .expect("fused residual");
        }
        let actual = fused.finish_bound(None).expect("fused normal residual");
        assert_eq!(
            actual.major_cycle_residual.as_deref(),
            Some(expected.dirty()),
            "T20 must equal the declared paired A*W(d-Ax) evaluation"
        );
    }

    #[test]
    fn final_visibility_publication_uses_the_exact_casa_complex32_value() {
        let native = Complex64::new(1.0 + f64::from(f32::EPSILON) / 3.0, -0.1_f64);
        let published = casa_persistent_complex(native);
        let observed = Complex64::new(4.0, -2.0);

        assert_eq!(published.re, f64::from(native.re as f32));
        assert_eq!(published.im, f64::from(native.im as f32));
        assert_ne!(
            published, native,
            "fixture must exercise f64 to f32 quantization"
        );
        assert_eq!(
            observed - published,
            Complex64::new(
                f64::from(observed.re as f32) - f64::from(native.re as f32),
                f64::from(observed.im as f32) - f64::from(native.im as f32),
            ),
            "published residual is derived from the exact persisted prediction"
        );
    }
}
