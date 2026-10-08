// SPDX-License-Identifier: LGPL-3.0-or-later

//! Normal-state primitives: the unnormalised dirty, PSF, sensitivity and
//! sum-weight families a major-cycle pass produces for the minor cycle and
//! products, with their spectral basis, channel slab and validity.

pub(crate) mod normal_storage;
mod pass_state;
pub use pass_state::{PassImages, PassNormalState};

use std::{fmt, mem::size_of, sync::Mutex};

use casa_fft::{Fft2, FftScalar};
use casa_imaging_model::{
    CompiledGeometryId, CompiledProblemId, ImageDomainRole, LogicalIdentity, NumericsContractId,
    WeightingCommitmentId,
};
use ndarray::{ArrayBase, DataMut, Ix2};
use num_complex::{Complex, Complex64};
use thiserror::Error;

use crate::{
    ModelGenerationId, WeightingGenerationId, WeightingReplayId, block_normal::BlockNormalPlan,
    canonical_f64_bits,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum SpectralBasisPlan {
    ChannelLocal,
    Polynomial(BlockNormalPlan),
}

impl SpectralBasisPlan {
    const fn coefficient_terms(self, slab: SpectralSlabPlan) -> usize {
        match self {
            Self::ChannelLocal => slab.core_depth(),
            Self::Polynomial(plan) => plan.coefficient_term_count(),
        }
    }

    const fn normal_moments(self, slab: SpectralSlabPlan) -> usize {
        match self {
            Self::ChannelLocal => slab.core_depth(),
            Self::Polynomial(plan) => plan.normal_moment_count(),
        }
    }

    const fn polynomial(self) -> Option<BlockNormalPlan> {
        match self {
            Self::ChannelLocal => None,
            Self::Polynomial(plan) => Some(plan),
        }
    }

    fn normal_moment_index(self, row: usize, column: usize) -> Option<usize> {
        match self {
            Self::Polynomial(plan) => plan.normal_moment_index(row, column),
            Self::ChannelLocal => None,
        }
    }
}

pub(crate) const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const NORMAL_STATE_CONTENT_DOMAIN: &[u8] = b"casa-rs-normal-state-content";
// FFTW's native plan internals are opaque. Admission charges a full-grid
// planning buffer and a full-grid native plan allowance; sampled aggregate RSS
// remains the guard for native allocations.
const FFTW_PLANNING_SLACK_VALUES: usize = 64;

/// The output channels one normal state covers.
///
/// A constant or Taylor state covers its one channel; a channel-local state
/// covers its core range of the output axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpectralSlabPlan {
    total_channels: usize,
    core_start: usize,
    core_end: usize,
    resident_start: usize,
    resident_end: usize,
}

impl SpectralSlabPlan {
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
}

pub(crate) fn fft_resident_complex_values_for_shape(
    shape: [usize; 2],
) -> Result<usize, SpectralOperatorError> {
    shape[0]
        .checked_mul(shape[1])
        .and_then(|values| values.checked_mul(2))
        .and_then(|values| values.checked_add(FFTW_PLANNING_SLACK_VALUES))
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

/// Compact real outputs from the supported channel-local cube imager.
#[doc(hidden)]
#[derive(Debug)]
pub struct CubeRealFields {
    pub(crate) dirty: Box<[f32]>,
    pub(crate) invariant_dirty: Option<Box<[f32]>>,
    pub(crate) psf: Box<[f32]>,
}

impl CubeRealFields {
    /// Model-dependent residual or initial dirty plane values.
    pub fn dirty(&self) -> &[f32] {
        &self.dirty
    }

    /// Initial dirty values retained across model-dependent refreshes.
    pub fn invariant_dirty(&self) -> Option<&[f32]> {
        self.invariant_dirty.as_deref()
    }

    /// Unnormalized point-spread-function values.
    pub fn psf(&self) -> &[f32] {
        &self.psf
    }
}

/// Unnormalized spectral primitives; these are not Product Graph artifacts.
#[derive(Debug)]
pub struct SpectralOperatorPrimitives {
    shape: [usize; 2],
    slab: SpectralSlabPlan,
    basis: SpectralBasisPlan,
    polarizations: usize,
    dirty: Box<[Complex64]>,
    pub(crate) cube_real: Option<CubeRealFields>,
    invariant_dirty: Option<Box<[Complex64]>>,
    psf: Box<[Complex64]>,
    pub(crate) clark_workspace: Mutex<Option<crate::minor_cycle::ClarkRefreshWorkspace>>,
    sensitivity: Box<[f64]>,
    primary_beam_weighted_sum: Option<Box<[f64]>>,
    sum_weights: Box<[f64]>,
    published_sum_weights: Box<[f64]>,
    validity: Box<[SpectralChannelValidity]>,
    major_cycle_residual: Option<Box<[Complex64]>>,
    major_cycle_residual_promoted: bool,
    residual_model: Option<ModelGenerationId>,
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

    /// Return the number of model-coefficient residual terms.
    #[must_use]
    pub const fn coefficient_term_count(&self) -> usize {
        self.basis.coefficient_terms(self.slab)
    }

    /// Return the number of reconstruction polarization planes.
    #[must_use]
    pub const fn polarization_count(&self) -> usize {
        self.polarizations
    }

    /// Return the number of retained normal moments.
    #[must_use]
    pub const fn normal_moment_count(&self) -> usize {
        self.basis.normal_moments(self.slab)
    }

    /// Return the Taylor reference frequency, or `None` for channel-local state.
    #[must_use]
    pub const fn reference_frequency_hz(&self) -> Option<f64> {
        match self.basis {
            SpectralBasisPlan::Polynomial(plan) => Some(plan.reference_frequency_hz()),
            SpectralBasisPlan::ChannelLocal => None,
        }
    }

    /// Map one coefficient-block pair to its retained normal moment.
    #[must_use]
    pub fn normal_moment_index(&self, row: usize, column: usize) -> Option<usize> {
        self.basis.normal_moment_index(row, column)
    }

    /// Return the unnormalized dirty normal-state plane.
    #[must_use]
    pub fn dirty(&self) -> crate::NormalValues<'_> {
        self.cube_real
            .as_ref()
            .map_or(crate::NormalValues::Complex(&self.dirty), |real| {
                crate::NormalValues::Real(&real.dirty)
            })
    }

    /// Return the unnormalized point-spread-function plane.
    #[must_use]
    pub fn psf(&self) -> crate::NormalValues<'_> {
        self.cube_real
            .as_ref()
            .map_or(crate::NormalValues::Complex(&self.psf), |real| {
                crate::NormalValues::Real(&real.psf)
            })
    }

    /// Borrow compact real fields before the channel-local normal fold consumes them.
    #[doc(hidden)]
    pub fn cube_real_fields(&self) -> Option<&CubeRealFields> {
        self.cube_real.as_ref()
    }

    /// Return scalar-response sensitivity in normal-state units.
    #[must_use]
    pub fn sensitivity(&self) -> crate::SensitivityValues<'_> {
        if self.cube_real.is_some() {
            crate::SensitivityValues::PerPlane {
                weights: &self.sum_weights,
                cells: self.shape[0] * self.shape[1],
            }
        } else {
            crate::SensitivityValues::Dense(&self.sensitivity)
        }
    }

    /// Return `sum(W B)` in polarization-major image-plane order when a
    /// compiled scalar primary-beam response participated in the operator.
    #[must_use]
    pub fn primary_beam_weighted_sum(&self) -> Option<&[f64]> {
        self.primary_beam_weighted_sum.as_deref()
    }

    /// Return normal-moment-major, polarization-minor exact sum weights.
    ///
    /// The length is `normal_moment_count() * polarization_count()`.
    #[must_use]
    pub const fn sum_weights(&self) -> &[f64] {
        &self.sum_weights
    }

    /// Return CASA publication-statistic numerators in normal-moment-major,
    /// polarization-minor order.
    ///
    /// The length is `normal_moment_count() * polarization_count()`. Channel-
    /// major Taylor state retains `sum(W² x^t)` here so products can apply the
    /// single complete-family `sum(W)` denominator. Publication does not change
    /// scientific normalization.
    #[must_use]
    pub const fn published_sum_weights(&self) -> &[f64] {
        &self.published_sum_weights
    }

    /// Return support-entry-major, polarization-minor validity.
    ///
    /// The support-entry count is core-channel depth for channel-local state
    /// or one for Taylor state.
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

    /// Explicitly fingerprint unnormalized values for tests and diagnostics.
    /// Ordinary Major-Cycle completion and handoff do not invoke this pass.
    #[must_use]
    pub fn normal_state_content_identity(&self) -> LogicalIdentity {
        let taylor = self
            .basis
            .polynomial()
            .filter(|plan| plan.coefficient_term_count() > 1);
        let published_sum_weights_differ = self.published_sum_weights != self.sum_weights;
        let primary_beam = self.primary_beam_weighted_sum.is_some();
        let mut encoder = crate::Encoder::new(
            NORMAL_STATE_CONTENT_DOMAIN,
            if primary_beam {
                5
            } else if published_sum_weights_differ {
                4
            } else if taylor.is_some() {
                2
            } else {
                1
            },
        );
        encoder.usize(self.shape[0]);
        encoder.usize(self.shape[1]);
        encoder.usize(self.slab.total_channels);
        encoder.usize(self.slab.core_start);
        encoder.usize(self.slab.core_end);
        if let Some(plan) = taylor {
            encoder.u64(canonical_f64_bits(plan.reference_frequency_hz()));
            encoder.usize(plan.coefficient_term_count());
            encoder.usize(plan.normal_moment_count());
        }
        for value in self.dirty().iter() {
            encoder.u64(value.re.to_bits());
            encoder.u64(value.im.to_bits());
        }
        for value in self.psf().iter() {
            encoder.u64(value.re.to_bits());
            encoder.u64(value.im.to_bits());
        }
        for value in self.sensitivity().iter() {
            encoder.u64(canonical_f64_bits(value));
        }
        if let Some(values) = &self.primary_beam_weighted_sum {
            for value in values {
                encoder.u64(canonical_f64_bits(*value));
            }
        }
        for value in &self.sum_weights {
            encoder.u64(canonical_f64_bits(*value));
        }
        if published_sum_weights_differ {
            for value in &self.published_sum_weights {
                encoder.u64(canonical_f64_bits(*value));
            }
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
    /// Taylor-coefficient residuals and the `2T-1` signed block-normal moments.
    UnnormalizedTaylorBlockV1,
}

/// Opaque reconstruction-owned proof that one complete weighted pass reached A/A*.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataOwnerCompletion {
    pub(crate) problem: CompiledProblemId,
    pub(crate) geometry: CompiledGeometryId,
    pub(crate) numerics: NumericsContractId,
    pub(crate) weighting_commitment: WeightingCommitmentId,
    pub(crate) weighting_generation: WeightingGenerationId,
    pub(crate) replay: WeightingReplayId,

    pub(crate) primitives: SpectralPrimitiveCatalog,

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

    /// Return the compiler-owned weighting commitment.
    #[must_use]
    pub const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.weighting_commitment
    }

    /// Return the imaging-weight generation every pass used.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Return the identity of the traversal that formed the state.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.replay
    }

    /// Return the versioned primitive set produced by the operator.
    #[must_use]
    pub const fn primitive_catalog(&self) -> SpectralPrimitiveCatalog {
        self.primitives
    }

    /// Return the placed-sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return the source block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }
}

/// Complete unnormalized primitives of one image domain.
#[derive(Debug)]
pub(crate) struct SpectralDomainPrimitives {
    domain_ordinal: usize,
    domain_role: ImageDomainRole,
    primitives: SpectralOperatorPrimitives,
}

impl SpectralDomainPrimitives {
    pub(crate) fn new(
        domain_ordinal: usize,
        domain_role: ImageDomainRole,
        primitives: SpectralOperatorPrimitives,
    ) -> Self {
        Self {
            domain_ordinal,
            domain_role,
            primitives,
        }
    }

    pub(crate) const fn domain_ordinal(&self) -> usize {
        self.domain_ordinal
    }

    pub(crate) const fn domain_role(&self) -> &ImageDomainRole {
        &self.domain_role
    }

    pub(crate) const fn primitives(&self) -> &SpectralOperatorPrimitives {
        &self.primitives
    }
}

#[derive(Debug)]
pub(crate) struct SpectralPrimitiveDomains {
    domains: Box<[SpectralDomainPrimitives]>,
}

impl SpectralPrimitiveDomains {
    pub(crate) fn owned_bytes(&self) -> usize {
        self.iter()
            .map(|domain| {
                let p = domain.primitives();
                size_of::<SpectralDomainPrimitives>()
                    + std::mem::size_of_val(p.dirty.as_ref())
                    + p.cube_real.as_ref().map_or(0, |real| {
                        std::mem::size_of_val(real.dirty.as_ref())
                            + std::mem::size_of_val(real.psf.as_ref())
                            + real
                                .invariant_dirty
                                .as_deref()
                                .map_or(0, std::mem::size_of_val)
                    })
                    + p.invariant_dirty
                        .as_deref()
                        .map_or(0, std::mem::size_of_val)
                    + std::mem::size_of_val(p.psf.as_ref())
                    + std::mem::size_of_val(p.sensitivity.as_ref())
                    + p.primary_beam_weighted_sum
                        .as_deref()
                        .map_or(0, std::mem::size_of_val)
                    + std::mem::size_of_val(p.sum_weights())
                    + std::mem::size_of_val(p.published_sum_weights())
                    + std::mem::size_of_val(p.channel_validity())
                    + p.major_cycle_residual
                        .as_deref()
                        .map_or(0, std::mem::size_of_val)
                    + match &domain.domain_role {
                        ImageDomainRole::Main => 0,
                        ImageDomainRole::Outlier(name) => name.capacity(),
                    }
            })
            .sum()
    }

    pub(crate) fn new(
        domains: Box<[SpectralDomainPrimitives]>,
    ) -> Result<Self, SpectralOperatorError> {
        if domains.is_empty()
            || domains
                .iter()
                .enumerate()
                .any(|(ordinal, domain)| domain.domain_ordinal != ordinal)
        {
            return Err(SpectralOperatorError::DomainProjectionMismatch);
        }
        Ok(Self { domains })
    }

    pub(crate) fn len(&self) -> usize {
        self.domains.len()
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, SpectralDomainPrimitives> {
        self.domains.iter()
    }

    pub(crate) fn primary(&self) -> &SpectralOperatorPrimitives {
        &self.domains[0].primitives
    }

    pub(crate) fn get(&self, ordinal: usize) -> Option<&SpectralDomainPrimitives> {
        self.domains.get(ordinal)
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = SpectralDomainPrimitives> {
        self.domains.into_vec().into_iter()
    }

    pub(crate) fn promote_major_cycle_residual(
        self,
        expected_model: ModelGenerationId,
    ) -> Result<Self, SpectralOperatorError> {
        let domains = self
            .into_iter()
            .map(|domain| {
                domain
                    .primitives
                    .promote_major_cycle_residual(expected_model)
                    .map(|primitives| SpectralDomainPrimitives {
                        primitives,
                        ..domain
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        Self::new(domains)
    }

    pub(crate) fn normal_state_content_identity(&self) -> LogicalIdentity {
        let mut encoder = crate::Encoder::new(NORMAL_STATE_CONTENT_DOMAIN, 4);
        encoder.usize(self.domains.len());
        for domain in &self.domains {
            encoder.usize(domain.domain_ordinal);
            match &domain.domain_role {
                ImageDomainRole::Main => encoder.u8(0),
                ImageDomainRole::Outlier(name) => {
                    encoder.u8(1);
                    encoder.bytes(name.as_bytes());
                }
            }
            encoder.identity(domain.primitives.normal_state_content_identity().as_bytes());
        }
        LogicalIdentity::from_sha256(encoder.finish())
    }
}

impl std::ops::Deref for SpectralPrimitiveDomains {
    type Target = SpectralOperatorPrimitives;

    fn deref(&self) -> &Self::Target {
        &self.domains[0].primitives
    }
}

#[doc(hidden)]
pub struct PreparedFft<T: FftScalar = f64> {
    fft: Fft2<T>,
    column_major_fft: Option<Fft2<T>>,
    estimated: bool,
    threads: usize,
}

impl<T: FftScalar> PreparedFft<T> {
    pub(crate) fn new(
        shape: [usize; 2],
        reserved_complex_values: usize,
        threads: usize,
    ) -> Result<Self, SpectralOperatorError> {
        let required = fft_resident_complex_values_for_shape(shape)?;
        if required > reserved_complex_values {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(Self {
            fft: Fft2::with_threads(shape, threads)
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            column_major_fft: None,
            estimated: false,
            threads,
        })
    }

    pub(crate) fn transform<S: DataMut<Elem = Complex<T>>>(
        &mut self,
        data: &mut ArrayBase<S, Ix2>,
        inverse: bool,
    ) {
        shift_even(data);
        self.transform_unshifted(data, inverse);
        shift_even(data);
    }

    pub(crate) fn transform_unshifted<S: DataMut<Elem = Complex<T>>>(
        &mut self,
        data: &mut ArrayBase<S, Ix2>,
        inverse: bool,
    ) {
        let shape = [data.shape()[0], data.shape()[1]];
        assert_eq!(shape, self.fft.shape(), "FFTW plane shape mismatch");
        let column_major = data.strides() == [1, shape[0] as isize];
        let estimated = self.estimated;
        let threads = self.threads;
        let fft = if column_major && shape[0] != shape[1] {
            self.column_major_fft.get_or_insert_with(|| {
                let mut fft = Fft2::with_threads([shape[1], shape[0]], threads)
                    .expect("valid column-major FFT shape and threads");
                if estimated {
                    fft = fft.with_estimated_plan();
                }
                fft
            })
        } else {
            &mut self.fft
        };
        fft.transform(
            data.as_slice_memory_order_mut()
                .expect("FFTW plane must be contiguous"),
            inverse,
        )
        .expect("FFTW plan and plane must match");
    }
}

impl<T: FftScalar> fmt::Debug for PreparedFft<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedFft")
    }
}

fn shift_even<T, S: DataMut<Elem = T>>(data: &mut ArrayBase<S, Ix2>) {
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

/// The prolate spheroidal function of CASA `grdsf` (Schwab's rational
/// approximation, `synthesis/fortran/grdsf.f`) at `nu ∈ [0, 1]`.
pub(crate) fn grdsf(nu: f64) -> f64 {
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

fn checked_cells(shape: [usize; 2]) -> Result<usize, SpectralOperatorError> {
    shape[0]
        .checked_mul(shape[1])
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

/// Exact reason a normal state or its operators rejected their input.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SpectralOperatorError {
    /// A spatial transform (FFT) failed; the run cannot continue.
    #[error("spatial execution: {0}")]
    SpatialExecution(String),
    /// A bounded Normal State backing operation failed.
    #[error("normal-state storage: {0}")]
    NormalStorage(String),
    /// An authoritative model window could not be loaded.
    #[error(transparent)]
    ModelAccess(#[from] crate::ModelLifecycleError),
    /// An opt-in diagnostic requested an invalid or unbounded source-group capture.
    #[error(
        "invalid CASA_RS_TRACE_AW_GROUP: expected bounded start:end:ddid:spw:channels:4:accepted_hands"
    )]
    DiagnosticConfiguration,
    /// The prepared paired AW operator rejected its cache or row-local input.
    #[error("AW projection failed: {0}")]
    AwProjection(#[from] crate::AwOperatorError),
    /// Runtime supplied science or weighting state for another compiled problem.
    #[error("spectral operator science and weighting state do not match the compiled problem")]
    ProblemMismatch,
    /// The problem is outside the supported scalar-response Stokes-I bases.
    #[error("spectral operator requires a supported scalar-response Stokes-I reconstruction basis")]
    UnsupportedProblem,
    /// A requested output slab is empty or outside the compiled spectral axis.
    #[error("spectral operator slab is empty or outside the output spectral axis")]
    InvalidSlab,
    /// The current serial operator supports only centered identity-PC SIN geometry.
    #[error("spectral operator does not support this direction-coordinate geometry")]
    UnsupportedGeometry,
    /// A resident-byte calculation overflowed.
    #[error("spectral operator residency cannot be represented")]
    ResidencyOverflow,
    /// A weighted contribution contains an invalid numerical value.
    #[error("spectral operator sample is non-finite or outside its numerical domain")]
    InvalidSample,
    /// Selected row geometry did not provide one canonical projection per image domain.
    #[error("selected row image-domain projections do not match the compiled geometry")]
    DomainProjectionMismatch,
    /// The operator generated a non-finite value under a rejecting numerics contract.
    #[error("spectral operator generated a non-finite value")]
    GeneratedNonfinite,
    /// A pass's images do not cover the state's channels exactly once.
    #[error("spectral operator pass coverage does not match the normal state")]
    IncompleteCoverage,
    /// A prediction model does not match the planned image shape.
    #[error("spectral operator model does not match the planned image shape")]
    ModelShape,
    /// A different model was named after residual replay was prepared.
    #[error("spectral operator residual belongs to another final model generation")]
    ModelMismatch,
    /// T20 attempted to finalize output that never accumulated an exact residual.
    #[error("spectral operator output lacks an exhaustive paired-operator residual")]
    MissingMajorCycleResidual,
    /// A later major pass did not carry the exact prior invariant normal state.
    #[error("spectral operator reusable normal state does not match the residual refresh")]
    ReusableNormalStateMismatch,
}
