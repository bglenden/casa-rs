// SPDX-License-Identifier: LGPL-3.0-or-later

//! Normal-state primitives: the unnormalised dirty, PSF, sensitivity and
//! sum-weight families a major-cycle pass produces for the minor cycle and
//! products, with their spectral basis, channel slab and validity.

pub(crate) mod normal_storage;
mod pass_state;
pub use pass_state::{PassImages, PassNormalState};

use std::mem::size_of;

use casa_imaging_model::{ImageDomainRole, LogicalIdentity};
use num_complex::Complex64;
use thiserror::Error;

use crate::{ModelGenerationId, block_normal::BlockNormalPlan, canonical_f64_bits};

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

const NORMAL_STATE_CONTENT_DOMAIN: &[u8] = b"casa-rs-normal-state-content";

/// The output channels one normal state covers.
///
/// A constant or Taylor state covers its one channel; a channel-local state
/// covers its core range of the output axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpectralSlabPlan {
    total_channels: usize,
    core_start: usize,
    core_end: usize,
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

    /// Return the number of output channels owned by this slab.
    #[must_use]
    pub const fn core_depth(self) -> usize {
        self.core_end - self.core_start
    }
}

/// Compact real outputs from the supported channel-local cube imager.
#[doc(hidden)]
#[derive(Debug)]
pub struct CubeRealFields {
    pub(crate) dirty: Box<[f32]>,
    pub(crate) psf: Box<[f32]>,
}

impl CubeRealFields {
    /// Model-dependent residual or initial dirty plane values.
    pub fn dirty(&self) -> &[f32] {
        &self.dirty
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
    psf: Box<[Complex64]>,
    sensitivity: Box<[f64]>,
    sum_weights: Box<[f64]>,
    published_sum_weights: Box<[f64]>,
    validity: Box<[SpectralChannelValidity]>,
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

    /// Explicitly fingerprint unnormalized values for tests and diagnostics.
    /// Ordinary Major-Cycle completion and handoff do not invoke this pass.
    #[must_use]
    pub fn normal_state_content_identity(&self) -> LogicalIdentity {
        let taylor = self
            .basis
            .polynomial()
            .filter(|plan| plan.coefficient_term_count() > 1);
        let published_sum_weights_differ = self.published_sum_weights != self.sum_weights;
        let mut encoder = crate::Encoder::new(
            NORMAL_STATE_CONTENT_DOMAIN,
            if published_sum_weights_differ {
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
                SpectralChannelValidity::Unmapped => 2,
            });
        }
        LogicalIdentity::from_bytes(encoder.finish())
    }
}

/// Normal-state validity for one output channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralChannelValidity {
    /// At least one mapped sample contributed positive finite weight.
    Valid,
    /// No sample contributed positive weight to the output channel, whether
    /// none mapped to it or all that did carried zero effective weight.
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
    pub(crate) primitives: SpectralPrimitiveCatalog,
    pub(crate) sample_count: u64,
    pub(crate) block_count: u64,
}

impl CompleteDataOwnerCompletion {
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
                    })
                    + std::mem::size_of_val(p.psf.as_ref())
                    + std::mem::size_of_val(p.sensitivity.as_ref())
                    + std::mem::size_of_val(p.sum_weights())
                    + std::mem::size_of_val(p.published_sum_weights())
                    + std::mem::size_of_val(p.channel_validity())
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
        LogicalIdentity::from_bytes(encoder.finish())
    }
}

impl std::ops::Deref for SpectralPrimitiveDomains {
    type Target = SpectralOperatorPrimitives;

    fn deref(&self) -> &Self::Target {
        &self.domains[0].primitives
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
    /// A different model was named after residual replay was prepared.
    #[error("spectral operator residual belongs to another final model generation")]
    ModelMismatch,
    /// A later major pass did not carry the exact prior invariant normal state.
    #[error("spectral operator reusable normal state does not match the residual refresh")]
    ReusableNormalStateMismatch,
}
