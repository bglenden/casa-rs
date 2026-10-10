// SPDX-License-Identifier: LGPL-3.0-or-later

//! The major cycle: one pass forms the normal state with the final model.
//!
//! A [`MajorCycle`] owns its final model and the normal state its pass forms
//! with that model, and releases them together as one
//! [`MajorCycleCompletion`]: the final normal state, with its model-dependent
//! residual, and the final model generation.

use std::fmt;

use casa_imaging_model::{CompiledProblem, ImageDomainRole};

use crate::{
    ModelGeneration, ModelLifecycleError, PassImages, PassNormalState, PreparedFinalModel,
    SpectralOperatorError, SpectralPrimitiveCatalog,
    runtime_adapter::{CompleteDataNormalState, NormalStoragePlan},
    spectral_operator::normal_storage::{NormalStatePrimitives, NormalStateWindowPayload},
};

/// Heap bound for one domain's channel-local window and its backing-access
/// scratch. Covers residual, invariant dirty, PSF, sensitivity, optional PB,
/// exact channel metadata, and an overlapping scalar conversion/I/O pair.
#[doc(hidden)]
pub fn normal_state_window_residency_bytes(
    shape: [usize; 2],
    polarizations: usize,
    window_channels: usize,
) -> Result<u64, SpectralOperatorError> {
    let overflow = || SpectralOperatorError::ResidencyOverflow;
    let planes = window_channels
        .checked_mul(polarizations)
        .ok_or_else(overflow)?;
    let cells = shape[0].checked_mul(shape[1]).ok_or_else(overflow)?;
    let image_bytes = cells
        .checked_mul(planes)
        .and_then(|values| {
            values.checked_mul(5 * size_of::<num_complex::Complex64>() + 2 * size_of::<f64>())
        })
        .ok_or_else(overflow)?;
    let metadata = planes
        .checked_mul(2 * size_of::<f64>() + size_of::<crate::SpectralChannelValidity>())
        .and_then(|bytes| {
            bytes.checked_add(size_of::<crate::spectral_operator::SpectralDomainPrimitives>())
        })
        .ok_or_else(overflow)?;
    u64::try_from(image_bytes.checked_add(metadata).ok_or_else(overflow)?).map_err(|_| overflow())
}

/// Versioned Normal State Generation catalog minted by a Major Cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateCatalog {
    /// Unnormalized single-field Stokes-I constant-basis MFS normal state
    /// whose residual follows the exact paired `A* W (d - A x)` composition.
    UnnormalizedPlaneV1,
    /// Unnormalized channel-major normal-state slab whose planes follow the
    /// exact output-channel interval named by the paired spectral operator.
    UnnormalizedChannelSlabV1,
    /// Unnormalized Taylor residual terms and `2T-1` signed block-normal moments.
    UnnormalizedTaylorBlockV1,
}

/// The normal state a major cycle's pass formed with its final model.
///
/// It owns the model-dependent unnormalized residual with the PSF,
/// sensitivity and sum weights, and the sample and block counts of the
/// pass; it makes no promise that the state is fully resident, dense, or
/// shift-invariant. It is not a Product Graph artifact.
///
/// ```compile_fail
/// use casa_imaging_reconstruction::FinalNormalState;
///
/// let _ = FinalNormalState {};
/// ```
#[doc(hidden)]
#[derive(Debug)]
pub struct FinalNormalState {
    catalog: NormalStateCatalog,
    sample_count: u64,
    block_count: u64,
    primitives: NormalStatePrimitives,
}

impl FinalNormalState {
    pub(crate) const fn primitives(&self) -> &NormalStatePrimitives {
        &self.primitives
    }

    pub(crate) fn into_primitives(self) -> NormalStatePrimitives {
        self.primitives
    }

    /// Release a superseded channel-local residual epoch after its successor
    /// has completed. Shared invariant fields remain with that successor.
    #[doc(hidden)]
    pub fn retire_obsolete(self) -> Result<(), SpectralOperatorError> {
        self.primitives.retire_obsolete()
    }
    /// Live scalar payload already charged by its retained runtime allocation.
    /// This is metadata-only; it does not load or verify any array contents.
    #[doc(hidden)]
    pub fn retained_resident_bytes(&self) -> Result<u64, SpectralOperatorError> {
        self.primitives.retained_resident_bytes()
    }

    /// Maximum channel window supported by this generation's backing capability.
    #[doc(hidden)]
    #[must_use]
    pub fn maximum_read_channels(&self) -> usize {
        self.primitives.maximum_read_channels()
    }

    /// Return the canonical number of image-domain normal states in this one completion.
    #[must_use]
    pub fn domain_count(&self) -> usize {
        self.primitives.len()
    }

    /// Read one domain's shape without loading image payloads.
    #[must_use]
    pub fn domain_shape(&self, ordinal: usize) -> Option<[usize; 2]> {
        self.primitives.metadata(ordinal).map(|domain| domain.shape)
    }

    /// Read channel support metadata without loading image payloads.
    #[must_use]
    pub fn domain_channel_validity(
        &self,
        domain: usize,
        channel: usize,
        polarization: usize,
    ) -> Option<crate::SpectralChannelValidity> {
        let metadata = self.primitives.metadata(domain)?;
        if channel >= metadata.slab.core_depth() || polarization >= metadata.polarizations {
            return None;
        }
        metadata
            .validity
            .get(channel * metadata.polarizations + polarization)
            .copied()
    }

    /// Return the versioned Normal State Generation catalog.
    #[must_use]
    pub const fn catalog(&self) -> NormalStateCatalog {
        self.catalog
    }

    /// Return the number of samples the pass placed.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return the number of source blocks the pass read.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Return the exact unnormalized plane shape of every primitive.
    #[must_use]
    pub fn shape(&self) -> [usize; 2] {
        self.primitives.primary_metadata().shape
    }

    /// Return the exact output-channel slab represented by this state.
    #[must_use]
    pub fn slab(&self) -> crate::SpectralSlabPlan {
        self.primitives.primary_metadata().slab
    }

    /// Return the number of channel planes resident in this state.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.slab().core_depth()
    }

    /// Return the number of reconstruction polarization planes.
    #[must_use]
    pub fn polarization_count(&self) -> usize {
        self.primitives.primary_metadata().polarizations
    }

    /// Return the number of reconstruction-coefficient residual terms.
    #[must_use]
    pub fn coefficient_term_count(&self) -> usize {
        self.primitives.primary_metadata().coefficient_terms
    }

    /// Return the number of retained normal moments.
    #[must_use]
    pub fn normal_moment_count(&self) -> usize {
        self.primitives.primary_metadata().normal_moments
    }

    /// Return the polynomial reference frequency when this is continuum state.
    #[must_use]
    pub fn reference_frequency_hz(&self) -> Option<f64> {
        self.primitives.primary_metadata().reference_frequency_hz
    }

    /// Return the exact accumulated sum weight.
    #[must_use]
    pub fn sum_weight(&self) -> f64 {
        let weights = self.sum_weights();
        assert_eq!(
            weights.len(),
            1,
            "cube normal state has per-channel weights"
        );
        weights[0]
    }

    /// Return normal-moment-major, polarization-minor sum weights.
    ///
    /// The length is `normal_moment_count() * polarization_count()`.
    #[must_use]
    pub fn sum_weights(&self) -> &[f64] {
        self.primitives.primary_metadata().sum_weights
    }

    /// Return CASA publication-statistic numerators in normal-moment-major,
    /// polarization-minor order.
    ///
    /// The length is `normal_moment_count() * polarization_count()`. Product
    /// formation owns any complete-family denominator; publication does not
    /// change normal-state scaling.
    #[must_use]
    pub fn published_sum_weights(&self) -> &[f64] {
        self.primitives.primary_metadata().published_sum_weights
    }

    /// Return support-entry-major, polarization-minor validity.
    ///
    /// The support-entry count is core-channel depth for channel-local state,
    /// or one for Taylor state.
    #[must_use]
    pub fn channel_validity(&self) -> &[crate::SpectralChannelValidity] {
        self.primitives.primary_metadata().validity
    }

    /// Return the principal support state for a polynomial normal family.
    #[must_use]
    pub fn support_validity(&self) -> Option<crate::SpectralChannelValidity> {
        if !matches!(self.catalog, NormalStateCatalog::UnnormalizedTaylorBlockV1) {
            return None;
        }
        self.channel_validity().first().copied()
    }

    /// Load a channel-local window, or borrow the complete coupled coefficient
    /// family. The returned guard owns every channel-local payload it exposes.
    pub fn read_window(
        &self,
        channels: std::ops::Range<usize>,
    ) -> Result<FinalNormalStateWindow<'_>, SpectralOperatorError> {
        let primitives = self.primitives.read_window(channels)?;
        Ok(FinalNormalStateWindow {
            owner: self,
            primitives,
        })
    }

    /// Select one absolute channel and polarization without loading image data.
    ///
    /// Field reads are independently bounded to this plane and borrow this
    /// completion owner. Channel-local and resident constant-basis state are
    /// supported; multi-term Taylor families use their complete-family path.
    pub fn read_plane(
        &self,
        domain_ordinal: usize,
        absolute_channel: usize,
        polarization: usize,
    ) -> Result<crate::FinalNormalPlaneReader<'_>, SpectralOperatorError> {
        self.primitives
            .read_plane(domain_ordinal, absolute_channel, polarization)
    }

    /// Read the residual and point-spread function of one plane, the inputs
    /// of that plane's minor cycle.
    ///
    /// `domain_ordinal` names the image domain, `absolute_channel` the
    /// output channel (in the slab's absolute numbering) and `polarization`
    /// the polarization plane. Both fields are unnormalized, in their stored
    /// precision: a paged state reads only this plane, a resident state
    /// borrows it. The plane also carries its shape, validity and sum of
    /// weights.
    ///
    /// # Errors
    ///
    /// When the plane is outside the state or its storage cannot be read.
    pub fn read_reconstruction_plane(
        &self,
        domain_ordinal: usize,
        absolute_channel: usize,
        polarization: usize,
    ) -> Result<FinalNormalStatePlane<'_>, SpectralOperatorError> {
        let plane = self.read_plane(domain_ordinal, absolute_channel, polarization)?;
        let residual_real = plane.read_residual_real()?;
        let psf_real = plane.read_psf_real()?;
        Ok(FinalNormalStatePlane {
            owner: self,
            domain_ordinal,
            output_channel: absolute_channel,
            polarization,
            shape: plane.shape(),
            validity: plane.validity(),
            sum_weight: plane.sum_weight(),
            residual: if let Some(real) = residual_real {
                crate::normal_values::NormalPlane::Real(real)
            } else {
                crate::normal_values::NormalPlane::Complex(plane.read_residual()?)
            },
            psf: if let Some(real) = psf_real {
                crate::normal_values::NormalPlane::Real(real)
            } else {
                crate::normal_values::NormalPlane::Complex(plane.read_psf()?)
            },
        })
    }
}

/// Explicitly loaded Normal State data retaining its global completion owner.
/// Channel-local windows do not mint local completion or mask identities.
#[derive(Debug)]
pub struct FinalNormalStateWindow<'a> {
    owner: &'a FinalNormalState,
    primitives: NormalStateWindowPayload<'a>,
}

impl std::ops::Deref for FinalNormalStateWindow<'_> {
    type Target = FinalNormalState;
    fn deref(&self) -> &Self::Target {
        self.owner
    }
}

impl FinalNormalStateWindow<'_> {
    /// Global completion owner; channel windows have no separate authority.
    #[must_use]
    pub fn owner(&self) -> &FinalNormalState {
        self.owner
    }

    /// The exact loaded channel interval, not the global coverage interval.
    #[must_use]
    pub fn slab(&self) -> crate::SpectralSlabPlan {
        self.primitives.slab()
    }

    /// Number of loaded channels. Coupled state retains its complete basis.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.slab().core_depth()
    }

    /// Image-sized heap capacity owned by this guard.
    #[must_use]
    pub fn owned_bytes(&self) -> usize {
        match &self.primitives {
            NormalStateWindowPayload::ChannelLocal(domains) => domains.owned_bytes(),
            NormalStateWindowPayload::Coupled(_) => 0,
        }
    }

    /// Borrow one canonical image-domain view by ordinal.
    #[must_use]
    pub fn domain(&self, ordinal: usize) -> Option<FinalNormalDomainState<'_>> {
        self.primitives
            .get(ordinal)
            .map(|domain| FinalNormalDomainState {
                owner: self.owner,
                domain,
            })
    }

    /// Iterate image-domain normal states in compiled geometry order.
    pub fn domains(&self) -> impl ExactSizeIterator<Item = FinalNormalDomainState<'_>> {
        self.primitives.iter().map(|domain| FinalNormalDomainState {
            owner: self.owner,
            domain,
        })
    }

    /// Borrow the domain with this compiler-owned role.
    #[must_use]
    pub fn domain_by_role(&self, role: &ImageDomainRole) -> Option<FinalNormalDomainState<'_>> {
        self.domains().find(|domain| domain.role() == role)
    }

    /// Return the authoritative model-dependent residual plane.
    #[must_use]
    pub fn residual(&self) -> crate::NormalValues<'_> {
        self.primitives.dirty()
    }

    /// Return the normal approximation (the PSF) paired with the residual.
    #[must_use]
    pub fn normal_approximation(&self) -> crate::NormalValues<'_> {
        self.primitives.psf()
    }

    /// Return sensitivity state in unnormalized normal-state units.
    #[must_use]
    pub fn sensitivity(&self) -> crate::SensitivityValues<'_> {
        self.primitives.sensitivity()
    }

    /// Borrow one reconstruction-coefficient residual term.
    #[must_use]
    pub fn coefficient_term(
        &self,
        coefficient: usize,
    ) -> Option<FinalNormalStateCoefficientTerm<'_>> {
        if !matches!(self.catalog, NormalStateCatalog::UnnormalizedTaylorBlockV1) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = coefficient.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        Some(FinalNormalStateCoefficientTerm {
            owner: self.owner,
            coefficient,
            residual: self.primitives.dirty().complex()?.get(start..end)?,
        })
    }

    /// Borrow one retained normal moment.
    #[must_use]
    pub fn normal_moment(&self, moment: usize) -> Option<FinalNormalStateNormalMoment<'_>> {
        if !matches!(self.catalog, NormalStateCatalog::UnnormalizedTaylorBlockV1) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = moment.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        Some(FinalNormalStateNormalMoment {
            owner: self.owner,
            moment,
            normal_approximation: self.primitives.psf().complex()?.get(start..end)?,
            sensitivity: self.primitives.sensitivity().dense()?.get(start..end)?,
            sum_weight: *self.primitives.sum_weights().get(moment)?,
        })
    }

    /// Borrow the Hankel normal block `H[row,column] = P[row+column]`.
    #[must_use]
    pub fn normal_block(
        &self,
        row: usize,
        column: usize,
    ) -> Option<FinalNormalStateNormalMoment<'_>> {
        let moment = self.primitives.normal_moment_index(row, column)?;
        self.normal_moment(moment)
    }

    /// Borrow one channel/polarization plane from this bounded Normal State slab.
    #[must_use]
    pub fn polarization_plane(
        &self,
        local_channel: usize,
        polarization: usize,
    ) -> Option<FinalNormalStatePlane<'_>> {
        if matches!(self.catalog, NormalStateCatalog::UnnormalizedTaylorBlockV1) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let plane = local_channel
            .checked_mul(self.primitives.polarization_count())?
            .checked_add(polarization)?;
        let start = plane.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        if local_channel >= self.channel_count()
            || polarization >= self.primitives.polarization_count()
        {
            return None;
        }
        Some(FinalNormalStatePlane {
            owner: self.owner,
            domain_ordinal: self.primitives.get(0)?.domain_ordinal(),
            output_channel: self.primitives.slab().core_range().start + local_channel,
            polarization,
            shape: self.shape(),
            validity: *self.primitives.channel_validity().get(plane)?,
            sum_weight: *self.primitives.sum_weights().get(plane)?,
            residual: crate::normal_values::NormalPlane::borrowed(
                self.primitives.dirty().slice(start..end)?,
            ),
            psf: crate::normal_values::NormalPlane::borrowed(
                self.primitives.psf().slice(start..end)?,
            ),
        })
    }
}

/// Borrowed chart-local primitives from one shared Final Normal State completion.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalDomainState<'a> {
    owner: &'a FinalNormalState,
    domain: &'a crate::spectral_operator::SpectralDomainPrimitives,
}

impl<'a> FinalNormalDomainState<'a> {
    /// Return the shared completion owner.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the canonical compiled-domain ordinal.
    #[must_use]
    pub const fn ordinal(self) -> usize {
        self.domain.domain_ordinal()
    }

    /// Return the compiler-owned image-domain role.
    #[must_use]
    pub const fn role(self) -> &'a ImageDomainRole {
        self.domain.domain_role()
    }

    /// Return `[width, height]` for this chart.
    #[must_use]
    pub const fn shape(self) -> [usize; 2] {
        self.domain.primitives().shape()
    }

    /// Return this chart's model-dependent residual planes.
    #[must_use]
    pub fn residual(self) -> crate::NormalValues<'a> {
        self.domain.primitives().dirty()
    }

    /// Return this chart's normal approximation/PSF planes.
    #[must_use]
    pub fn normal_approximation(self) -> crate::NormalValues<'a> {
        self.domain.primitives().psf()
    }

    /// Return this chart's sensitivity planes.
    #[must_use]
    pub fn sensitivity(self) -> crate::SensitivityValues<'a> {
        self.domain.primitives().sensitivity()
    }

    /// Return this chart's normal-moment-major, polarization-minor weights.
    ///
    /// The length is `normal_moment_count() * polarization_count()`.
    #[must_use]
    pub const fn sum_weights(self) -> &'a [f64] {
        self.domain.primitives().sum_weights()
    }

    /// Return CASA publication-statistic numerators in normal-moment-major,
    /// polarization-minor order.
    ///
    /// The length is `normal_moment_count() * polarization_count()`; publication
    /// does not change normal-state scaling.
    #[must_use]
    pub const fn published_sum_weights(self) -> &'a [f64] {
        self.domain.primitives().published_sum_weights()
    }

    /// Return support-entry-major, polarization-minor validity for this chart.
    ///
    /// The support-entry count is core-channel depth for channel-local state,
    /// or one for Taylor state.
    #[must_use]
    pub const fn channel_validity(self) -> &'a [crate::SpectralChannelValidity] {
        self.domain.primitives().channel_validity()
    }

    /// Borrow one channel/polarization plane from this chart-local Normal State.
    #[must_use]
    pub fn polarization_plane(
        self,
        local_channel: usize,
        polarization: usize,
    ) -> Option<FinalNormalStatePlane<'a>> {
        let primitives = self.domain.primitives();
        let cells = primitives.shape()[0].checked_mul(primitives.shape()[1])?;
        let plane = local_channel
            .checked_mul(primitives.polarization_count())?
            .checked_add(polarization)?;
        let start = plane.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        if local_channel >= primitives.slab().core_depth()
            || polarization >= primitives.polarization_count()
        {
            return None;
        }
        Some(FinalNormalStatePlane {
            owner: self.owner,
            domain_ordinal: self.domain.domain_ordinal(),
            output_channel: primitives.slab().core_range().start + local_channel,
            polarization,
            shape: primitives.shape(),
            validity: *primitives.channel_validity().get(plane)?,
            sum_weight: *primitives.sum_weights().get(plane)?,
            residual: crate::normal_values::NormalPlane::borrowed(
                primitives.dirty().slice(start..end)?,
            ),
            psf: crate::normal_values::NormalPlane::borrowed(primitives.psf().slice(start..end)?),
        })
    }
}

/// Borrowed model-dependent residual for one reconstruction coefficient.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalStateCoefficientTerm<'a> {
    owner: &'a FinalNormalState,
    coefficient: usize,
    residual: &'a [num_complex::Complex64],
}

impl<'a> FinalNormalStateCoefficientTerm<'a> {
    /// Return the state owner.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the zero-based Taylor coefficient ordinal.
    #[must_use]
    pub const fn coefficient(self) -> usize {
        self.coefficient
    }

    /// Return this coefficient's unnormalized model-dependent residual.
    #[must_use]
    pub const fn residual(self) -> &'a [num_complex::Complex64] {
        self.residual
    }
}

/// Borrowed `P[k]` member of a polynomial block-normal family.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalStateNormalMoment<'a> {
    owner: &'a FinalNormalState,
    moment: usize,
    normal_approximation: &'a [num_complex::Complex64],
    sensitivity: &'a [f64],
    sum_weight: f64,
}

impl<'a> FinalNormalStateNormalMoment<'a> {
    /// Return the state owner.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the zero-based polynomial moment ordinal.
    #[must_use]
    pub const fn moment(self) -> usize {
        self.moment
    }

    /// Return this moment's unnormalized PSF approximation.
    #[must_use]
    pub const fn normal_approximation(self) -> &'a [num_complex::Complex64] {
        self.normal_approximation
    }

    /// Return this moment's unnormalized scalar sensitivity.
    #[must_use]
    pub const fn sensitivity(self) -> &'a [f64] {
        self.sensitivity
    }

    /// Return this moment's signed accumulated sum weight.
    #[must_use]
    pub const fn sum_weight(self) -> f64 {
        self.sum_weight
    }
}

/// Read-only reconstruction fields for one authoritative Normal State plane.
/// Resident fields borrow their backing; paged fields own only the selected plane.
#[derive(Debug)]
pub struct FinalNormalStatePlane<'a> {
    owner: &'a FinalNormalState,
    domain_ordinal: usize,
    output_channel: usize,
    polarization: usize,
    shape: [usize; 2],
    validity: crate::SpectralChannelValidity,
    sum_weight: f64,
    residual: crate::normal_values::NormalPlane<'a>,
    psf: crate::normal_values::NormalPlane<'a>,
}

impl<'a> FinalNormalStatePlane<'a> {
    /// Return the slab owner this view borrows.
    #[must_use]
    pub const fn owner(&self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the canonical image-domain ordinal of this plane.
    #[must_use]
    pub const fn domain_ordinal(&self) -> usize {
        self.domain_ordinal
    }

    /// Return the absolute output-channel ordinal.
    #[must_use]
    pub const fn output_channel(&self) -> usize {
        self.output_channel
    }

    /// Return the reconstruction polarization-plane ordinal.
    #[must_use]
    pub const fn polarization(&self) -> usize {
        self.polarization
    }

    /// Return this plane's model-dependent unnormalized residual.
    #[must_use]
    pub fn residual(&self) -> crate::NormalValues<'_> {
        self.residual.values()
    }

    /// Return this plane's unnormalized PSF approximation.
    #[must_use]
    pub fn normal_approximation(&self) -> crate::NormalValues<'_> {
        self.psf.values()
    }

    /// Return this plane's accumulated sum weight.
    #[must_use]
    pub const fn sum_weight(&self) -> f64 {
        self.sum_weight
    }

    /// Return the common direction-plane shape.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// Return weighted or unmapped channel validity.
    #[must_use]
    pub const fn validity(&self) -> crate::SpectralChannelValidity {
        self.validity
    }
}

/// One major cycle's result: the final normal state and the final model its
/// pass formed the residual with, released together.
///
/// A caller cannot assemble one from parts:
///
/// ```compile_fail
/// use casa_imaging_reconstruction::MajorCycleCompletion;
///
/// let _ = MajorCycleCompletion {};
/// ```
#[derive(Debug)]
pub struct MajorCycleCompletion {
    normal_state: FinalNormalState,
    final_model: ModelGeneration,
}

impl MajorCycleCompletion {
    /// Borrow the final normal state.
    #[must_use]
    pub const fn normal_state(&self) -> &FinalNormalState {
        &self.normal_state
    }

    /// Borrow the final model generation.
    #[must_use]
    pub const fn final_model(&self) -> &ModelGeneration {
        &self.final_model
    }

    /// Release the normal state and the final model, which the next major
    /// cycle refreshes and updates.
    #[must_use]
    pub fn into_parts(self) -> (FinalNormalState, ModelGeneration) {
        (self.normal_state, self.final_model)
    }
}

/// One major cycle: its final model and the normal state its pass forms
/// with that model.
///
/// Owning both pairs the residual with the model that formed it: the pass
/// reads the model and appends to the state through [`Self::parts`], and only
/// [`Self::finish`] releases them, together. The pass cannot take the state
/// out of its cycle, for example to exchange two cycles' states:
///
/// ```compile_fail,E0308
/// use casa_imaging_reconstruction::MajorCycle;
///
/// fn exchange_states(first: &mut MajorCycle, second: &mut MajorCycle) {
///     std::mem::swap(first.parts().1, second.parts().1);
/// }
/// ```
pub struct MajorCycle {
    model: PreparedFinalModel,
    state: PassNormalState,
}

/// The pass's access to its [`MajorCycle`]'s normal state: it appends
/// images and nothing else, so the state stays in its cycle, paired with
/// the model that formed it.
pub struct PassAppender<'a> {
    state: &'a mut PassNormalState,
}

impl PassAppender<'_> {
    /// Add one pass's images of one domain.
    ///
    /// A non-finite image value or `sumwt` is a generated non-finite value,
    /// rejected under every [`casa_imaging_model::FiniteValuePolicy`]: the
    /// policies differ only on non-finite inputs, which the source flags or
    /// rejects before gridding. Nothing is changed when an append fails.
    ///
    /// # Errors
    ///
    /// Images whose domain, shape, polarizations, channels or planes do not
    /// match the problem and the pass (PSF moments on a residual refresh, a
    /// channel range out of order), and generated non-finite values.
    pub fn append(&mut self, images: PassImages) -> Result<(), SpectralOperatorError> {
        self.state.append(images)
    }
}

impl MajorCycle {
    /// The initial major cycle of `problem`: its pass forms the data and PSF
    /// images with `model`, writing them as `storage` plans.
    pub fn initial(
        problem: &CompiledProblem,
        model: PreparedFinalModel,
        storage: NormalStoragePlan,
    ) -> Result<Self, MajorCycleError> {
        Ok(Self {
            state: PassNormalState::initial(problem, storage).map_err(MajorCycleError::Residual)?,
            model,
        })
    }

    /// A residual refresh of `previous` with `model`: the PSF, `sumwt` and
    /// validity carry over and every residual plane is formed again.
    pub fn refresh(
        problem: &CompiledProblem,
        previous: FinalNormalState,
        model: PreparedFinalModel,
        storage: NormalStoragePlan,
    ) -> Result<Self, MajorCycleError> {
        Ok(Self {
            state: PassNormalState::refresh(problem, previous, storage)
                .map_err(MajorCycleError::Residual)?,
            model,
        })
    }

    /// The final model the pass predicts and the appender its images go to.
    pub fn parts(&mut self) -> (&ModelGeneration, PassAppender<'_>) {
        (
            self.model.generation(),
            PassAppender {
                state: &mut self.state,
            },
        )
    }

    /// Finish the pass, which placed `samples` from `blocks` source blocks,
    /// and release the final normal state with the final model.
    ///
    /// # Errors
    ///
    /// A pass that placed no sample, left an image incomplete or refreshed
    /// fewer samples than the state it refreshed, a failed model update and
    /// generated non-finite residuals all fail with no partial result.
    pub fn finish(
        self,
        samples: u64,
        blocks: u64,
    ) -> Result<MajorCycleCompletion, MajorCycleError> {
        if samples == 0 || blocks == 0 {
            return Err(MajorCycleError::IncompleteCoverage);
        }
        let CompleteDataNormalState {
            primitives,
            completion,
        } = self
            .state
            .finish(samples, blocks)
            .map_err(MajorCycleError::Residual)?;
        let final_model = self.model.complete()?;
        Ok(MajorCycleCompletion {
            normal_state: FinalNormalState {
                catalog: match completion.primitive_catalog() {
                    SpectralPrimitiveCatalog::UnnormalizedPlaneV1 => {
                        NormalStateCatalog::UnnormalizedPlaneV1
                    }
                    SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1 => {
                        NormalStateCatalog::UnnormalizedChannelSlabV1
                    }
                    SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1 => {
                        NormalStateCatalog::UnnormalizedTaylorBlockV1
                    }
                },
                sample_count: completion.sample_count(),
                block_count: completion.block_count(),
                primitives,
            },
            final_model,
        })
    }
}

/// Exact reason a Major-Cycle reconciliation failed closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MajorCycleError {
    /// The pass placed no sample or read no source block.
    IncompleteCoverage,
    /// The final model's queued update could not be completed.
    Model(ModelLifecycleError),
    /// Reconciling the final model produced or consumed invalid numbers.
    Residual(SpectralOperatorError),
}

impl fmt::Display for MajorCycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteCoverage => {
                formatter.write_str("the major-cycle pass placed no sample")
            }
            Self::Model(error) => error.fmt(formatter),
            Self::Residual(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MajorCycleError {}

impl From<ModelLifecycleError> for MajorCycleError {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Model(error)
    }
}
