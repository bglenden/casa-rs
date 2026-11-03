// SPDX-License-Identifier: LGPL-3.0-or-later

//! Normal-state primitives: the unnormalised dirty, PSF, sensitivity and
//! sum-weight families a major-cycle pass produces for the minor cycle and
//! products, with their spectral basis, channel slab and validity.

mod pass_state;
pub(crate) mod storage;
pub use pass_state::PassImages;
pub(crate) use pass_state::PassNormalState;

use std::mem::size_of;

use casa_imaging_model::ImageDomainRole;
use num_complex::Complex64;
use thiserror::Error;

/// The spectral basis of a normal state, which decides how its planes are
/// read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalBasis {
    /// One constant-basis plane per polarization.
    Constant,
    /// One plane per output channel and polarization.
    ChannelLocal,
    /// Taylor-coefficient residuals and the `2T−1` moments of the Hankel
    /// block normal.
    Taylor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpectralBasisPlan {
    ChannelLocal,
    /// A constant (one term) or Taylor basis: `terms` residual terms and
    /// the `2·terms − 1` moments of its Hankel block normal. `2·terms`
    /// fits in `usize` (checked where the plan is made).
    Polynomial {
        terms: usize,
    },
}

impl SpectralBasisPlan {
    const fn basis(self) -> NormalBasis {
        match self {
            Self::ChannelLocal => NormalBasis::ChannelLocal,
            Self::Polynomial { terms } if terms > 1 => NormalBasis::Taylor,
            Self::Polynomial { .. } => NormalBasis::Constant,
        }
    }

    const fn coefficient_terms(self, slab: SpectralSlabPlan) -> usize {
        match self {
            Self::ChannelLocal => slab.core_depth(),
            Self::Polynomial { terms } => terms,
        }
    }

    const fn normal_moments(self, slab: SpectralSlabPlan) -> usize {
        match self {
            Self::ChannelLocal => slab.core_depth(),
            Self::Polynomial { terms } => 2 * terms - 1,
        }
    }
}

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

/// A channel-local pass's planes as it gridded them, in `f32`.
#[derive(Debug)]
pub(crate) struct CubeRealFields {
    pub(crate) dirty: Box<[f32]>,
    pub(crate) psf: Box<[f32]>,
    /// Each plane's gridded weight image (CASA's per-channel `.weight` before
    /// its `sumwt` division), for kernel sets with a direction-dependent
    /// response such as the mosaic; `None` when the response is the plane's
    /// `sumwt` alone.
    pub(crate) sensitivity: Option<Box<[f32]>>,
}

/// One image domain's unnormalised residual, PSF moments, sensitivity and
/// sum weights over a channel slab; products are formed from them.
#[derive(Debug)]
pub(crate) struct NormalPrimitives {
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
}

/// Heap bytes per image cell and polarization of resident primitives with
/// `terms` residual terms: the residual as `Complex64`, and with `psf` the
/// `moments` PSF moments as `Complex64` and their sensitivity as `f64`. A
/// residual refresh keeps the PSF and sensitivity it started from, so it adds
/// only the residual.
#[must_use]
pub const fn resident_bytes_per_cell(terms: usize, moments: usize, psf: bool) -> u64 {
    let complex = size_of::<Complex64>() as u64;
    let mut bytes = terms as u64 * complex;
    if psf {
        bytes += moments as u64 * (complex + size_of::<f64>() as u64);
    }
    bytes
}

impl NormalPrimitives {
    /// Heap bytes these primitives hold.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        let cube_real = self.cube_real.as_ref().map_or(0, |fields| {
            size_of_val(&*fields.dirty)
                + size_of_val(&*fields.psf)
                + fields.sensitivity.as_deref().map_or(0, size_of_val)
        });
        (size_of_val(&*self.dirty)
            + cube_real
            + size_of_val(&*self.psf)
            + size_of_val(&*self.sensitivity)
            + size_of_val(&*self.sum_weights)
            + size_of_val(&*self.published_sum_weights)
            + size_of_val(&*self.validity)) as u64
    }

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

    /// Return the response sensitivity in normal-state units.
    #[must_use]
    pub fn sensitivity(&self) -> crate::SensitivityValues<'_> {
        match &self.cube_real {
            Some(CubeRealFields {
                sensitivity: Some(weight),
                ..
            }) => crate::SensitivityValues::Real(weight),
            Some(_) => crate::SensitivityValues::PerPlane {
                weights: &self.sum_weights,
                cells: self.shape[0] * self.shape[1],
            },
            None => crate::SensitivityValues::Dense(&self.sensitivity),
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
}

/// CASA's empty-plane bound on a plane's published `.sumwt`:
/// `SIImageStore::divideImageByWeightVal` divides a plane by its `Float`
/// `.sumwt` only when it exceeds `1e-07`, and otherwise sets the plane to
/// zero, so a plane at or below it is as empty as one no sample reached.
pub const EMPTY_PLANE_SUM_WEIGHT: f64 = 1e-7;

/// Whether CASA divides a plane whose published `.sumwt` is
/// `published_sum_weight` rather than zero it ([`EMPTY_PLANE_SUM_WEIGHT`]),
/// comparing the `Float` value CASA stores.
#[must_use]
pub fn casa_divides_plane(published_sum_weight: f64) -> bool {
    f64::from(published_sum_weight as f32) > EMPTY_PLANE_SUM_WEIGHT
}

/// Normal-state validity for one output channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralChannelValidity {
    /// At least one mapped sample contributed positive finite weight, and
    /// the published `.sumwt` exceeds CASA's empty-plane bound
    /// ([`EMPTY_PLANE_SUM_WEIGHT`]).
    Valid,
    /// No sample contributed positive weight to the output channel, whether
    /// none mapped to it or all that did carried zero effective weight; or
    /// the weight it holds is at most CASA's empty-plane bound, below which
    /// CASA zeroes the plane.
    Unmapped,
}

/// Complete unnormalized primitives of one image domain.
#[derive(Debug)]
pub(crate) struct SpectralDomainPrimitives {
    domain_ordinal: usize,
    domain_role: ImageDomainRole,
    primitives: NormalPrimitives,
}

impl SpectralDomainPrimitives {
    pub(crate) fn new(
        domain_ordinal: usize,
        domain_role: ImageDomainRole,
        primitives: NormalPrimitives,
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

    pub(crate) const fn primitives(&self) -> &NormalPrimitives {
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
                size_of::<SpectralDomainPrimitives>()
                    + domain.primitives().resident_bytes() as usize
                    + match &domain.domain_role {
                        ImageDomainRole::Main => 0,
                        ImageDomainRole::Outlier(name) => name.capacity(),
                    }
            })
            .sum()
    }

    pub(crate) fn new(domains: Box<[SpectralDomainPrimitives]>) -> Result<Self, NormalStateError> {
        if domains.is_empty()
            || domains
                .iter()
                .enumerate()
                .any(|(ordinal, domain)| domain.domain_ordinal != ordinal)
        {
            return Err(NormalStateError::DomainProjectionMismatch);
        }
        Ok(Self { domains })
    }

    pub(crate) fn len(&self) -> usize {
        self.domains.len()
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, SpectralDomainPrimitives> {
        self.domains.iter()
    }

    pub(crate) fn primary(&self) -> &NormalPrimitives {
        &self.domains[0].primitives
    }

    pub(crate) fn get(&self, ordinal: usize) -> Option<&SpectralDomainPrimitives> {
        self.domains.get(ordinal)
    }
}

impl std::ops::Deref for SpectralPrimitiveDomains {
    type Target = NormalPrimitives;

    fn deref(&self) -> &Self::Target {
        &self.domains[0].primitives
    }
}

fn checked_cells(shape: [usize; 2]) -> Result<usize, NormalStateError> {
    shape[0]
        .checked_mul(shape[1])
        .ok_or(NormalStateError::ResidencyOverflow)
}

/// Why a normal state rejected a pass's images or a read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NormalStateError {
    /// A bounded normal-state backing operation failed.
    #[error("normal-state storage: {0}")]
    NormalStorage(String),
    /// A pass's images, or a read, do not match the state's domains, shape,
    /// polarizations or basis, or the pass (PSF moments on a residual
    /// refresh).
    #[error("the images or the read do not match the normal state")]
    ProblemMismatch,
    /// The problem's spectral axis has no linear reference frequency for a
    /// constant or Taylor basis.
    #[error("a constant or Taylor basis needs a linear spectral axis")]
    UnsupportedProblem,
    /// A requested channel range is empty or outside the output spectral axis.
    #[error("the channel range is empty or outside the output spectral axis")]
    InvalidSlab,
    /// A resident-byte calculation overflowed.
    #[error("the normal state's size cannot be represented")]
    ResidencyOverflow,
    /// The image domains are not in canonical ordinal order.
    #[error("the image domains do not match the compiled geometry")]
    DomainProjectionMismatch,
    /// A pass generated a non-finite image value or `sumwt`.
    #[error("a major-cycle pass generated a non-finite value")]
    GeneratedNonfinite,
    /// A pass's images do not cover the state's channels exactly once.
    #[error("the pass's images do not cover the normal state's channels")]
    IncompleteCoverage,
    /// A residual refresh placed a different number of samples than the
    /// state it refreshed.
    #[error("the residual refresh placed a different number of samples than its normal state")]
    ReusableNormalStateMismatch,
}

#[cfg(test)]
mod empty_plane_tests {
    use super::{EMPTY_PLANE_SUM_WEIGHT, casa_divides_plane};

    /// CASA compares its `Float` `.sumwt` with the double `1e-07`: a stored
    /// 1e-7 rounds up in single precision and is divided, while the
    /// probe's 1.194e-8 and an exact zero are zeroed.
    #[test]
    fn casa_divides_only_float_sum_weights_above_the_bound() {
        assert!(f64::from(EMPTY_PLANE_SUM_WEIGHT as f32) > EMPTY_PLANE_SUM_WEIGHT);
        assert!(casa_divides_plane(EMPTY_PLANE_SUM_WEIGHT));
        assert!(casa_divides_plane(9.55e-7));
        assert!(!casa_divides_plane(1.194e-8));
        assert!(!casa_divides_plane(9.9e-8));
        assert!(!casa_divides_plane(0.0));
        assert!(!casa_divides_plane(-1.0));
    }
}
