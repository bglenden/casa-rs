// SPDX-License-Identifier: LGPL-3.0-or-later

//! Serial CPU constant-basis MFS measurement operator and normal-state primitives.

use std::{fmt, mem::size_of, sync::Arc};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, CorrelationType, FiniteValuePolicy,
    InstrumentResponse, NumericPrecision, NumericsContractId, PolarizationCoordinate, Projection,
    ReconstructionBasis, ReductionPolicy, SelectedObservationGenerationId,
    SelectedVisibilitySample, WeightingCommitmentId,
};
use ndarray::{Array2, Axis};
use num_complex::Complex64;
use rustfft::{Fft, FftPlanner};
use thiserror::Error;

use crate::weighting::{
    CoverageEncoder, WeightingAlgorithmState, WeightingGenerationId, WeightingReplayChunk,
    WeightingReplayCoverageId, WeightingReplayId, WeightingReplaySummary,
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const SUPPORT: usize = 3;
const TAP_COUNT: usize = SUPPORT * 2 + 1;
const OVERSAMPLING: usize = 100;
// The pinned RustFFT mixed-radix planner stores at most one length-sized table
// per decomposition level. There are fewer than usize::BITS levels, two
// directions, and small node headers. Charging four complex values per level
// gives a hard, architecture-independent upper bound while the library keeps
// its plan internals opaque.
const FFT_PLAN_COMPLEX_BOUND_PER_AXIS: usize = 4 * usize::BITS as usize;

/// One already-weighted spectral contribution accepted by the T19 algorithm.
///
/// This value contains no weighting policy, density, taper, generation, or
/// numerics fields. Runtime supplies it only after unwrapping a T18-branded
/// weighted block; the terminal runtime completion remains the authority that
/// proves exhaustive coverage.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SerialMfsSample {
    uvw_m: [f64; 3],
    frequency_hz: f64,
    phase_shift_m: f64,
    visibility: Complex64,
    imaging_weight: f64,
    spectral_factor: f64,
}

impl SerialMfsSample {
    /// Construct one numerical contribution after the runtime capability check.
    fn new(
        uvw_m: [f64; 3],
        frequency_hz: f64,
        phase_shift_m: f64,
        visibility: [f64; 2],
        imaging_weight: f64,
        spectral_factor: f64,
    ) -> Result<Self, SerialMfsError> {
        if uvw_m.iter().any(|value| !value.is_finite())
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !phase_shift_m.is_finite()
            || visibility.iter().any(|value| !value.is_finite())
            || !imaging_weight.is_finite()
            || imaging_weight < 0.0
            || !spectral_factor.is_finite()
            || spectral_factor <= 0.0
            || spectral_factor > 1.0
        {
            return Err(SerialMfsError::InvalidSample);
        }
        Ok(Self {
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

/// Exact resident-byte projection for one serial operator instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SerialMfsResidency {
    grid_bytes: usize,
    convolution_cache_bytes: usize,
    fft_scratch_bytes: usize,
    forward_workspace_bytes: usize,
    primitive_output_bytes: usize,
    peak_bytes: usize,
}

impl SerialMfsResidency {
    /// Bytes for the dirty and PSF accumulation grids.
    #[must_use]
    pub const fn grid_bytes(self) -> usize {
        self.grid_bytes
    }

    /// Retained normalized convolution taps and image-correction axes.
    #[must_use]
    pub const fn convolution_cache_bytes(self) -> usize {
        self.convolution_cache_bytes
    }

    /// Reusable lane, library scratch, and conservative opaque FFT-plan residency.
    #[must_use]
    pub const fn fft_scratch_bytes(self) -> usize {
        self.fft_scratch_bytes
    }

    /// One forward padded grid and one bounded predicted replay block.
    #[must_use]
    pub const fn forward_workspace_bytes(self) -> usize {
        self.forward_workspace_bytes
    }

    /// Bytes retained by dirty, PSF, and sensitivity primitive planes.
    #[must_use]
    pub const fn primitive_output_bytes(self) -> usize {
        self.primitive_output_bytes
    }

    /// Conservative peak with no full-grid worker duplicate.
    #[must_use]
    pub const fn peak_bytes(self) -> usize {
        self.peak_bytes
    }
}

/// Immutable physical plan for the serial CPU standard convolutional operator.
#[derive(Debug, Clone, PartialEq)]
pub struct SerialMfsPlan {
    image_shape: [usize; 2],
    grid_shape: [usize; 2],
    image_blc: [usize; 2],
    increment_rad: [f64; 2],
    max_replay_block_samples: usize,
    residency: SerialMfsResidency,
}

impl SerialMfsPlan {
    /// Plan one single-field scalar Stokes-I constant-basis problem.
    pub fn new(
        problem: &CompiledProblem,
        max_replay_block_samples: usize,
    ) -> Result<Self, SerialMfsError> {
        if max_replay_block_samples == 0 {
            return Err(SerialMfsError::UnsupportedProblem);
        }
        if problem.geometry().domains().len() != 1
            || problem.reconstruction().basis() != ReconstructionBasis::Constant
            || problem.reconstruction().polarization().coordinates()
                != [PolarizationCoordinate::StokesI]
            || problem
                .science()
                .measurement_equation()
                .instrument_response()
                != InstrumentResponse::Scalar
        {
            return Err(SerialMfsError::UnsupportedProblem);
        }
        let domain = &problem.geometry().domains()[0];
        let image_shape = domain.shape().pixels();
        let direction = domain.direction();
        let supported_reference_pixel = [
            ((image_shape[0] - 1) / 2) as f64,
            ((image_shape[1] - 1) / 2) as f64,
        ];
        if direction.projection() != Projection::Sin
            || direction.pc() != [[1.0, 0.0], [0.0, 1.0]]
            || direction.reference_pixel() != supported_reference_pixel
            || direction.pole_deg() != [180.0, 0.0]
        {
            return Err(SerialMfsError::UnsupportedGeometry);
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
            return Err(SerialMfsError::UnsupportedNumerics);
        }
        let grid_shape = [
            casa_composite_padded_len(image_shape[0], 1.2),
            casa_composite_padded_len(image_shape[1], 1.2),
        ];
        let image_blc = [
            grid_shape[0] / 2 - supported_reference_pixel[0] as usize,
            grid_shape[1] / 2 - supported_reference_pixel[1] as usize,
        ];
        let cells = checked_cells(grid_shape)?;
        let image_cells = checked_cells(image_shape)?;
        let complex_bytes = size_of::<Complex64>();
        let grid_bytes = cells
            .checked_mul(complex_bytes)
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or(SerialMfsError::ResidencyOverflow)?;
        let fft_scratch_bytes = fft_workspace_bytes(grid_shape)?;
        let convolution_cache_bytes = (OVERSAMPLING + 1)
            .checked_mul(TAP_COUNT)
            .and_then(|values| values.checked_add(grid_shape[0] + grid_shape[1]))
            .and_then(|values| values.checked_mul(size_of::<f64>()))
            .ok_or(SerialMfsError::ResidencyOverflow)?;
        let forward_workspace_bytes = cells
            .checked_add(
                max_replay_block_samples
                    .checked_mul(2)
                    .ok_or(SerialMfsError::ResidencyOverflow)?,
            )
            .and_then(|values| values.checked_mul(complex_bytes))
            .ok_or(SerialMfsError::ResidencyOverflow)?;
        let primitive_output_bytes = image_cells
            .checked_mul(complex_bytes * 2 + size_of::<f64>())
            .ok_or(SerialMfsError::ResidencyOverflow)?;
        let peak_bytes = grid_bytes
            .checked_add(convolution_cache_bytes)
            .and_then(|bytes| bytes.checked_add(forward_workspace_bytes))
            .and_then(|bytes| bytes.checked_add(fft_scratch_bytes))
            .and_then(|bytes| bytes.checked_add(primitive_output_bytes))
            .ok_or(SerialMfsError::ResidencyOverflow)?;
        Ok(Self {
            image_shape,
            grid_shape,
            image_blc,
            increment_rad: direction.increment_rad(),
            max_replay_block_samples,
            residency: SerialMfsResidency {
                grid_bytes,
                convolution_cache_bytes,
                fft_scratch_bytes,
                forward_workspace_bytes,
                primitive_output_bytes,
                peak_bytes,
            },
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

    /// Return the complete resident-byte projection.
    #[must_use]
    pub const fn residency(&self) -> SerialMfsResidency {
        self.residency
    }
}

/// Unnormalized continuum primitives; these are not Product Graph artifacts.
#[derive(Debug)]
pub struct SerialMfsPrimitives {
    shape: [usize; 2],
    dirty: Box<[Complex64]>,
    psf: Box<[Complex64]>,
    sensitivity: Box<[f64]>,
    sum_weight: f64,
}

impl SerialMfsPrimitives {
    /// Return `[width, height]` shared by every plane.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
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

    /// Return the exact accumulated scalar sum weight.
    #[must_use]
    pub const fn sum_weight(&self) -> f64 {
        self.sum_weight
    }
}

/// Versioned unnormalized primitive set produced by the nterms=1 continuum operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuumPrimitiveCatalog {
    /// Dirty, PSF, sensitivity, and sum-weight primitives under the v1 contract.
    UnnormalizedNterms1V1,
}

/// Opaque reconstruction-owned proof that one complete weighted replay reached A/A*.
#[derive(Debug)]
pub struct CompleteDataCompletion {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    selected_generation: SelectedObservationGenerationId,
    coverage: WeightingReplayCoverageId,
    primitives: ContinuumPrimitiveCatalog,
    sample_count: u64,
    block_count: u64,
}

impl CompleteDataCompletion {
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

    /// Return the independently traversed selected-observation generation.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return the exact T18 weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.coverage
    }

    /// Return the versioned primitive set produced by the operator.
    #[must_use]
    pub const fn primitive_catalog(&self) -> ContinuumPrimitiveCatalog {
        self.primitives
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
#[derive(Debug)]
pub struct CompleteDataResult {
    primitives: SerialMfsPrimitives,
    completion: CompleteDataCompletion,
}

impl CompleteDataResult {
    /// Return dirty, PSF, sensitivity, and sum-weight normal-state primitives.
    #[must_use]
    pub const fn primitives(&self) -> &SerialMfsPrimitives {
        &self.primitives
    }

    /// Return the owner-minted complete-data proof for these exact primitives.
    #[must_use]
    pub const fn completion(&self) -> &CompleteDataCompletion {
        &self.completion
    }

    /// Consume the pairing without turning its primitives into Product Graph artifacts.
    #[must_use]
    pub fn into_parts(self) -> (SerialMfsPrimitives, CompleteDataCompletion) {
        (self.primitives, self.completion)
    }
}

/// Reconstruction owner for one complete, ordered weighted replay.
#[derive(Debug)]
pub struct CompleteDataState {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    next_block_sequence: u64,
    sample_count: u64,
    coverage: CoverageEncoder,
    operator: SerialMfsOperator,
}

impl CompleteDataState {
    fn new(
        problem: &CompiledProblem,
        weighting_generation: WeightingGenerationId,
        max_replay_block_samples: usize,
    ) -> Result<Self, SerialMfsError> {
        let plan = SerialMfsPlan::new(problem, max_replay_block_samples)?;
        Ok(Self {
            problem: problem.problem_id(),
            geometry: problem.geometry().geometry_id(),
            numerics: problem.numerics_id(),
            weighting_commitment: problem.weighting().commitment_id(),
            weighting_generation,
            next_block_sequence: 0,
            sample_count: 0,
            coverage: CoverageEncoder::new(weighting_generation),
            operator: SerialMfsOperator::new(plan),
        })
    }

    /// Return the sole frozen T18 generation accepted by this owner.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Consume one reconstruction-owned T18 block in canonical replay order.
    pub fn consume_block(&mut self, block: &WeightingReplayChunk) -> Result<(), SerialMfsError> {
        if block.sequence() != self.next_block_sequence {
            return Err(SerialMfsError::BlockSequence);
        }
        for weighted in block.samples() {
            self.coverage.push(weighted);
            let selected = weighted.selected();
            if stokes_i_parallel_hand(selected.address.correlation_type) {
                let visibility = match selected.visibility {
                    SelectedVisibilitySample::Float32(value) => [f64::from(value), 0.0],
                    SelectedVisibilitySample::Complex32([real, imaginary]) => {
                        [f64::from(real), f64::from(imaginary)]
                    }
                };
                for spectral in weighted.spectral_values() {
                    let contribution = spectral.contribution();
                    self.operator.push(SerialMfsSample::new(
                        selected.coordinates.transformed_uvw_m,
                        selected.address.frequency_centre_hz,
                        selected.coordinates.phase_shift_m,
                        visibility,
                        spectral.imaging_weight(),
                        f64::from(contribution.factor()),
                    )?)?;
                }
            }
        }
        self.sample_count = self
            .sample_count
            .checked_add(
                u64::try_from(block.samples().len())
                    .map_err(|_| SerialMfsError::CoverageOverflow)?,
            )
            .ok_or(SerialMfsError::CoverageOverflow)?;
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(1)
            .ok_or(SerialMfsError::CoverageOverflow)?;
        Ok(())
    }

    /// Apply the paired forward operator to one opaque bounded T18 replay block.
    pub fn predict_block(
        &self,
        model: &[Complex64],
        block: &WeightingReplayChunk,
    ) -> Result<Box<[Complex64]>, SerialMfsError> {
        if block.samples().len() > self.operator.plan.max_replay_block_samples {
            return Err(SerialMfsError::IncompleteCoverage);
        }
        let padded = self.operator.prediction_grid(model)?;
        let mut predicted = Vec::with_capacity(block.samples().len().saturating_mul(2));
        for weighted in block.samples() {
            let selected = weighted.selected();
            if !stokes_i_parallel_hand(selected.address.correlation_type) {
                continue;
            }
            for spectral in weighted.spectral_values() {
                let contribution = spectral.contribution();
                let sample = SerialMfsSample::new(
                    selected.coordinates.transformed_uvw_m,
                    selected.address.frequency_centre_hz,
                    selected.coordinates.phase_shift_m,
                    [0.0, 0.0],
                    spectral.imaging_weight(),
                    f64::from(contribution.factor()),
                )?;
                predicted.push(self.operator.predict_one(&padded, sample));
            }
        }
        Ok(predicted.into_boxed_slice())
    }

    /// Consume terminal T18 algorithm evidence and mint complete-data evidence.
    pub fn complete(
        self,
        replay: &WeightingReplaySummary,
        selected_generation: SelectedObservationGenerationId,
    ) -> Result<CompleteDataResult, SerialMfsError> {
        if self.weighting_generation != replay.weighting_generation() {
            return Err(SerialMfsError::WeightingGeneration);
        }
        if self.sample_count != replay.sample_count()
            || self.next_block_sequence != replay.block_count()
        {
            return Err(SerialMfsError::IncompleteCoverage);
        }
        let coverage = self.coverage.finish(self.sample_count);
        if coverage != replay.coverage() {
            return Err(SerialMfsError::IncompleteCoverage);
        }
        Ok(CompleteDataResult {
            primitives: self.operator.finish()?,
            completion: CompleteDataCompletion {
                problem: self.problem,
                geometry: self.geometry,
                numerics: self.numerics,
                weighting_commitment: self.weighting_commitment,
                weighting_generation: replay.weighting_generation(),
                replay: replay.replay_id(),
                selected_generation,
                coverage,
                primitives: ContinuumPrimitiveCatalog::UnnormalizedNterms1V1,
                sample_count: replay.sample_count(),
                block_count: replay.block_count(),
            },
        })
    }
}

impl WeightingAlgorithmState {
    /// Begin the T19 owner only from an opaque frozen T18 generation.
    pub fn begin_complete_data(
        &self,
        problem: &CompiledProblem,
    ) -> Result<CompleteDataState, SerialMfsError> {
        if self.commitment_id() != problem.weighting().commitment_id() {
            return Err(SerialMfsError::WeightingGeneration);
        }
        CompleteDataState::new(
            problem,
            self.generation_id(),
            self.max_replay_block_samples(),
        )
    }
}

fn stokes_i_parallel_hand(correlation: CorrelationType) -> bool {
    matches!(
        correlation,
        CorrelationType::StokesI
            | CorrelationType::LinearXx
            | CorrelationType::LinearYy
            | CorrelationType::CircularRr
            | CorrelationType::CircularLl
    )
}

/// Reconstruction-owned serial CPU operator accumulator.
struct SerialMfsOperator {
    plan: SerialMfsPlan,
    gridder: StandardConvolution,
    fft: PreparedFft,
    dirty_grid: Array2<Complex64>,
    dirty_compensation: Array2<Complex64>,
    psf_grid: Array2<Complex64>,
    psf_compensation: Array2<Complex64>,
    sum_weight: f64,
    sum_weight_compensation: f64,
}

impl fmt::Debug for SerialMfsOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SerialMfsOperator")
            .field("plan", &self.plan)
            .field("sum_weight", &self.sum_weight)
            .finish_non_exhaustive()
    }
}

impl SerialMfsOperator {
    /// Allocate the two planned accumulation grids and no worker-local duplicate.
    #[must_use]
    pub fn new(plan: SerialMfsPlan) -> Self {
        let gridder = StandardConvolution::new(&plan);
        let fft = PreparedFft::new(plan.grid_shape);
        let shape = (plan.grid_shape[0], plan.grid_shape[1]);
        Self {
            plan,
            gridder,
            fft,
            dirty_grid: Array2::zeros(shape),
            dirty_compensation: Array2::zeros(shape),
            psf_grid: Array2::zeros(shape),
            psf_compensation: Array2::zeros(shape),
            sum_weight: 0.0,
            sum_weight_compensation: 0.0,
        }
    }

    /// Accumulate one already-weighted spectral contribution.
    pub fn push(&mut self, sample: SerialMfsSample) -> Result<(), SerialMfsError> {
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
            &mut self.dirty_grid,
            &mut self.dirty_compensation,
            taps,
            weighted_visibility,
        );
        let psf_weight = sample.imaging_weight * factor * factor;
        self.gridder.grid_compensated(
            &mut self.psf_grid,
            &mut self.psf_compensation,
            taps,
            Complex64::new(psf_weight, 0.0),
        );
        let corrected = psf_weight - self.sum_weight_compensation;
        let updated = self.sum_weight + corrected;
        self.sum_weight_compensation = (updated - self.sum_weight) - corrected;
        self.sum_weight = updated;
        Ok(())
    }

    /// Predict unweighted selected visibilities with the paired forward operator.
    #[cfg(test)]
    pub fn predict(
        &self,
        model: &[Complex64],
        samples: &[SerialMfsSample],
    ) -> Result<Box<[Complex64]>, SerialMfsError> {
        let padded = self.prediction_grid(model)?;
        Ok(samples
            .iter()
            .map(|sample| self.predict_one(&padded, *sample))
            .collect::<Vec<_>>()
            .into_boxed_slice())
    }

    fn prediction_grid(&self, model: &[Complex64]) -> Result<Array2<Complex64>, SerialMfsError> {
        if model.len() != checked_cells(self.plan.image_shape)? {
            return Err(SerialMfsError::ModelShape);
        }
        let mut padded = Array2::zeros((self.plan.grid_shape[0], self.plan.grid_shape[1]));
        for x in 0..self.plan.image_shape[0] {
            for y in 0..self.plan.image_shape[1] {
                let correction = self.gridder.image_correction(x, y);
                padded[(self.plan.image_blc[0] + x, self.plan.image_blc[1] + y)] =
                    model[x * self.plan.image_shape[1] + y] * correction;
            }
        }
        self.fft.transform(&mut padded, false);
        Ok(padded)
    }

    fn predict_one(&self, padded: &Array2<Complex64>, sample: SerialMfsSample) -> Complex64 {
        let Some(taps) = self.gridder.taps(sample.uv_lambda()) else {
            return Complex64::new(0.0, 0.0);
        };
        self.gridder.degrid(padded, taps) * sample.phase().conj() * sample.spectral_factor
    }

    /// Finish the paired inverse transforms and return unnormalized primitives.
    pub fn finish(mut self) -> Result<SerialMfsPrimitives, SerialMfsError> {
        self.fft.transform(&mut self.dirty_grid, true);
        self.fft.transform(&mut self.psf_grid, true);
        let cells = self.plan.image_shape[0] * self.plan.image_shape[1];
        let mut dirty = Vec::with_capacity(cells);
        let mut psf = Vec::with_capacity(cells);
        for x in 0..self.plan.image_shape[0] {
            for y in 0..self.plan.image_shape[1] {
                let correction = self.gridder.image_correction(x, y);
                dirty.push(
                    self.dirty_grid[(self.plan.image_blc[0] + x, self.plan.image_blc[1] + y)]
                        * correction,
                );
                psf.push(
                    self.psf_grid[(self.plan.image_blc[0] + x, self.plan.image_blc[1] + y)]
                        * correction,
                );
            }
        }
        if dirty
            .iter()
            .chain(&psf)
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
            || !self.sum_weight.is_finite()
        {
            return Err(SerialMfsError::GeneratedNonfinite);
        }
        Ok(SerialMfsPrimitives {
            shape: self.plan.image_shape,
            dirty: dirty.into_boxed_slice(),
            psf: psf.into_boxed_slice(),
            sensitivity: vec![self.sum_weight; cells].into_boxed_slice(),
            sum_weight: self.sum_weight,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct TapSpan {
    start: usize,
    weight_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct SampleTaps {
    x: TapSpan,
    y: TapSpan,
}

struct StandardConvolution {
    grid_shape: [usize; 2],
    image_blc: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    weights: Box<[[f64; TAP_COUNT]]>,
    correction_x: Box<[f64]>,
    correction_y: Box<[f64]>,
}

impl StandardConvolution {
    fn new(plan: &SerialMfsPlan) -> Self {
        Self {
            grid_shape: plan.grid_shape,
            image_blc: plan.image_blc,
            du_lambda: 1.0 / (plan.grid_shape[0] as f64 * plan.increment_rad[0].abs()),
            dv_lambda: 1.0 / (plan.grid_shape[1] as f64 * plan.increment_rad[1].abs()),
            weights: build_normalized_tap_weights(),
            correction_x: build_correction_axis(plan.grid_shape[0]),
            correction_y: build_correction_axis(plan.grid_shape[1]),
        }
    }

    fn taps(&self, uv: [f64; 2]) -> Option<SampleTaps> {
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

    fn grid_compensated(
        &self,
        grid: &mut Array2<Complex64>,
        compensation: &mut Array2<Complex64>,
        taps: SampleTaps,
        value: Complex64,
    ) {
        let x_weights = self.weights[taps.x.weight_index];
        let y_weights = self.weights[taps.y.weight_index];
        for (x, x_weight) in x_weights.into_iter().enumerate() {
            for (y, y_weight) in y_weights.into_iter().enumerate() {
                let index = (taps.x.start + x, taps.y.start + y);
                let contribution = value * x_weight * y_weight - compensation[index];
                let updated = grid[index] + contribution;
                compensation[index] = (updated - grid[index]) - contribution;
                grid[index] = updated;
            }
        }
    }

    fn degrid(&self, grid: &Array2<Complex64>, taps: SampleTaps) -> Complex64 {
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

struct PreparedFft {
    forward: [Arc<dyn Fft<f64>>; 2],
    inverse: [Arc<dyn Fft<f64>>; 2],
}

impl PreparedFft {
    fn new(shape: [usize; 2]) -> Self {
        let mut planner = FftPlanner::<f64>::new();
        Self {
            forward: [
                planner.plan_fft_forward(shape[0]),
                planner.plan_fft_forward(shape[1]),
            ],
            inverse: [
                planner.plan_fft_inverse(shape[0]),
                planner.plan_fft_inverse(shape[1]),
            ],
        }
    }

    fn transform(&self, data: &mut Array2<Complex64>, inverse: bool) {
        shift_even(data);
        for axis in 0..2 {
            let fft = if inverse {
                &self.inverse[axis]
            } else {
                &self.forward[axis]
            };
            transform_axis(data, Axis(axis), fft);
        }
        shift_even(data);
    }
}

impl fmt::Debug for PreparedFft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedFft")
    }
}

fn transform_axis(data: &mut Array2<Complex64>, axis: Axis, fft: &Arc<dyn Fft<f64>>) {
    let length = data.len_of(axis);
    let mut values = vec![Complex64::default(); length];
    let mut scratch = vec![Complex64::default(); fft.get_inplace_scratch_len()];
    for mut lane in data.lanes_mut(axis) {
        for (target, value) in values.iter_mut().zip(lane.iter()) {
            *target = *value;
        }
        fft.process_with_scratch(&mut values, &mut scratch);
        for (target, value) in lane.iter_mut().zip(&values) {
            *target = *value;
        }
    }
}

fn fft_workspace_bytes(grid_shape: [usize; 2]) -> Result<usize, SerialMfsError> {
    let mut peak_elements = 0_usize;
    for length in grid_shape {
        for inverse in [false, true] {
            let mut planner = FftPlanner::<f64>::new();
            let fft = if inverse {
                planner.plan_fft_inverse(length)
            } else {
                planner.plan_fft_forward(length)
            };
            let elements = length
                .checked_add(fft.get_inplace_scratch_len())
                .ok_or(SerialMfsError::ResidencyOverflow)?;
            peak_elements = peak_elements.max(elements);
        }
    }
    let plan_elements = grid_shape
        .into_iter()
        .try_fold(0_usize, |sum, length| {
            length
                .checked_mul(FFT_PLAN_COMPLEX_BOUND_PER_AXIS)
                .and_then(|elements| sum.checked_add(elements))
        })
        .ok_or(SerialMfsError::ResidencyOverflow)?;
    peak_elements
        .checked_add(plan_elements)
        .ok_or(SerialMfsError::ResidencyOverflow)?
        .checked_mul(size_of::<Complex64>())
        .ok_or(SerialMfsError::ResidencyOverflow)
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
    let centre_response = grdsf(0.0);
    (0..size)
        .map(|index| {
            let nu = ((index as f64 - center).abs() / center).clamp(0.0, 1.0);
            let value = grdsf(nu);
            if value > 1.0e-6 {
                centre_response / value
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

fn checked_cells(shape: [usize; 2]) -> Result<usize, SerialMfsError> {
    shape[0]
        .checked_mul(shape[1])
        .ok_or(SerialMfsError::ResidencyOverflow)
}

/// Exact reason the serial MFS plan or operator rejected its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SerialMfsError {
    /// The problem is outside the T19 constant-basis single-field surface.
    #[error("serial MFS requires one scalar-response Stokes-I constant-basis domain")]
    UnsupportedProblem,
    /// The current serial operator supports only centered identity-PC SIN geometry.
    #[error("serial MFS does not support this direction-coordinate geometry")]
    UnsupportedGeometry,
    /// The current serial operator requires permitted f64 compensated arithmetic.
    #[error("serial MFS requires f64 compensated numerical semantics")]
    UnsupportedNumerics,
    /// A resident-byte calculation overflowed.
    #[error("serial MFS residency cannot be represented")]
    ResidencyOverflow,
    /// A weighted contribution contains an invalid numerical value.
    #[error("serial MFS sample is non-finite or outside its numerical domain")]
    InvalidSample,
    /// The operator generated a non-finite value under a rejecting numerics contract.
    #[error("serial MFS generated a non-finite value")]
    GeneratedNonfinite,
    /// Weighted blocks arrived out of canonical replay order.
    #[error("serial MFS weighted block sequence is not canonical")]
    BlockSequence,
    /// Weighted blocks or completion carry different frozen W generations.
    #[error("serial MFS weighting generation changed during replay")]
    WeightingGeneration,
    /// A replay count could not be represented.
    #[error("serial MFS replay coverage overflowed")]
    CoverageOverflow,
    /// Terminal T18 evidence does not cover every consumed weighted block.
    #[error("serial MFS replay completion does not match consumed coverage")]
    IncompleteCoverage,
    /// A prediction model does not match the planned image shape.
    #[error("serial MFS model does not match the planned image shape")]
    ModelShape,
}

#[cfg(test)]
mod tests {
    use num_complex::Complex64;

    use super::{
        SerialMfsOperator, SerialMfsPlan, SerialMfsResidency, SerialMfsSample, checked_cells,
    };

    fn plan() -> SerialMfsPlan {
        let image_shape = [8, 8];
        let grid_shape = [10, 10];
        let image_blc = [2, 2];
        SerialMfsPlan {
            image_shape,
            grid_shape,
            image_blc,
            increment_rad: [-2.0e-3, 2.0e-3],
            max_replay_block_samples: 3,
            residency: SerialMfsResidency {
                grid_bytes: 6_400,
                convolution_cache_bytes: 5_816,
                fft_scratch_bytes: 160,
                forward_workspace_bytes: 1_696,
                primitive_output_bytes: 2_560,
                peak_bytes: 16_632,
            },
        }
    }

    fn samples(visibilities: &[[f64; 2]]) -> Vec<SerialMfsSample> {
        let coordinates = [[0.0, 0.0, 0.0], [18.0, -7.0, 0.0], [-11.0, 13.0, 0.0]];
        coordinates
            .into_iter()
            .zip(visibilities)
            .enumerate()
            .map(|(index, (uvw, visibility))| {
                SerialMfsSample::new(
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

    #[test]
    fn forward_and_weighted_adjoint_share_one_operator() {
        let plan = plan();
        let sample_values = samples(&[[0.4, -0.7], [-1.2, 0.3], [0.8, 1.1]]);
        let model = (0..checked_cells(plan.image_shape).expect("shape"))
            .map(|index| Complex64::new(index as f64 * 0.01 - 0.2, index as f64 * -0.003))
            .collect::<Vec<_>>();
        let prediction = SerialMfsOperator::new(plan.clone())
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
        let mut adjoint = SerialMfsOperator::new(plan);
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
    fn forward_is_linear_and_a_unit_centre_source_is_constant() {
        let plan = plan();
        let sample_values = samples(&[[0.0, 0.0]; 3]);
        let cells = checked_cells(plan.image_shape).expect("shape");
        let mut first = vec![Complex64::new(0.0, 0.0); cells];
        let mut second = first.clone();
        first[3 * 8 + 3] = Complex64::new(1.0, 0.0);
        second[3 * 8 + 5] = Complex64::new(-0.25, 0.5);
        let sum = first
            .iter()
            .zip(&second)
            .map(|(first, second)| first + second)
            .collect::<Vec<_>>();
        let operator = SerialMfsOperator::new(plan);
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
        let plan = plan();
        let sample_values = samples(&[[0.4, -0.7], [-1.2, 0.3], [0.8, 1.1]]);
        let run = |partitions: &[&[SerialMfsSample]]| {
            let mut operator = SerialMfsOperator::new(plan.clone());
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
        let plan = plan();
        let sample = SerialMfsSample::new([1.0e9, -1.0e9, 0.0], 1.0e9, 0.0, [3.0, -2.0], 4.0, 1.0)
            .expect("finite sample");
        let model = vec![Complex64::new(1.0, -0.5); checked_cells(plan.image_shape).unwrap()];
        let prediction = SerialMfsOperator::new(plan.clone())
            .predict(&model, &[sample])
            .expect("out-of-grid prediction is defined");
        assert_eq!(prediction.as_ref(), &[Complex64::new(0.0, 0.0)]);

        let mut adjoint = SerialMfsOperator::new(plan);
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
    fn plan_charges_shared_grids_fft_scratch_and_all_outputs_once() {
        let residency = plan().residency();
        assert_eq!(residency.grid_bytes(), 4 * 10 * 10 * 16);
        assert!(residency.fft_scratch_bytes() >= 10 * 16);
        assert_eq!(residency.primitive_output_bytes(), 8 * 8 * 40);
        assert_eq!(
            residency.peak_bytes(),
            residency.grid_bytes()
                + residency.convolution_cache_bytes()
                + residency.fft_scratch_bytes()
                + residency.forward_workspace_bytes()
                + residency.primitive_output_bytes()
        );
    }
}
