// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact channel-local Normal State backing, independent of physical storage.

use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

use casa_imaging_model::{ImageDomainRole, LogicalIdentity};
use num_complex::Complex64;

use super::{
    CompleteDataOwnerCompletion, NORMAL_STATE_CONTENT_DOMAIN, SpectralBasisPlan,
    SpectralChannelValidity, SpectralDomainPrimitives, SpectralOperatorError,
    SpectralOperatorPrimitives, SpectralPrimitiveDomains, SpectralSlabPlan, checked_cells,
};
use crate::{ModelGenerationId, canonical_f64_bits};

// num-complex 0.4.6 supplies Pod for its repr(C) real/imaginary pair.
// Checked casts preserve all floating-point bits without copying payloads.
fn complex_scalars(values: &[Complex64]) -> Result<&[f64], SpectralOperatorError> {
    bytemuck::try_cast_slice(values)
        .map_err(|error| SpectralOperatorError::NormalStorage(error.to_string()))
}

fn scalar_complex(values: Cow<'_, [f64]>) -> Result<Cow<'_, [Complex64]>, SpectralOperatorError> {
    match values {
        Cow::Borrowed(values) => bytemuck::try_cast_slice(values)
            .map(Cow::Borrowed)
            .map_err(|error| SpectralOperatorError::NormalStorage(error.to_string())),
        Cow::Owned(values) => bytemuck::allocation::try_cast_vec(values)
            .map(Cow::Owned)
            .map_err(|(error, _)| SpectralOperatorError::NormalStorage(error.to_string())),
    }
}

/// Physical scalar-array capability used only by the Normal State owner.
///
/// Complex values are stored as consecutive real/imaginary f64 values. Access
/// must not enlarge an admitted cache. The owner writes every logical value
/// before transferring the generation; storage handles must not permit
/// mutation through aliases retained outside this capability.
#[doc(hidden)]
pub trait NormalArrayStorage: fmt::Debug + Send + Sync {
    /// Logical scalar capacity, excluding physical tile padding.
    fn len(&self) -> usize;
    /// Actual live heap payload held under a retained runtime memory permit.
    /// Excludes unused reservation capacity, metadata and paged caches. Backings
    /// without that allocation/permit guarantee conservatively report zero.
    fn retained_resident_bytes(&self) -> usize {
        0
    }
    /// Whether the logical array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Return exactly `len` scalars. Resident windows borrow their backing and
    /// retained memory permit; paged windows own the bounded decoded allocation.
    /// Owned windows used as complex pairs must also have an even capacity so
    /// their allocation can transfer without repacking; invalid layouts fail.
    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError>;
    /// Read one image-plane complex window directly into the consuming shape.
    /// Compact real backings can widen here without a second scalar buffer.
    fn read_complex(
        &self,
        start: usize,
        values: usize,
    ) -> Result<Cow<'_, [Complex64]>, SpectralOperatorError> {
        scalar_complex(self.read(start, values * 2)?)
    }
    /// Borrow or load a real Float image directly when the backing stores one.
    /// Other normal families keep their existing complex access path.
    fn read_real(
        &self,
        _start: usize,
        _values: usize,
    ) -> Result<Option<Cow<'_, [f32]>>, SpectralOperatorError> {
        Ok(None)
    }
    /// Replace a bounded scalar window without resizing the array.
    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError>;
    /// Write a real Float image window at complex-scalar offsets. Other normal
    /// implementations may widen through their scalar interface; the managed
    /// cube backing writes Float planes directly.
    fn write_real(&mut self, start: usize, values: &[f32]) -> Result<(), SpectralOperatorError> {
        if !start.is_multiple_of(2) {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let mut widened = [0.0_f64; 512];
        for (chunk_index, chunk) in values.chunks(256).enumerate() {
            let offset = chunk_index
                .checked_mul(widened.len())
                .and_then(|offset| start.checked_add(offset))
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            for (pair, &value) in widened.as_chunks_mut::<2>().0.iter_mut().zip(chunk) {
                pair[0] = f64::from(value);
            }
            self.write(offset, &widened[..chunk.len() * 2])?;
        }
        Ok(())
    }

    /// Release a superseded epoch after its replacement is complete.
    fn retire(self: Box<Self>) -> Result<(), SpectralOperatorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Runtime allocation capability for an exact logical Normal State array.
#[doc(hidden)]
pub trait NormalStorageFactory: fmt::Debug + Send + Sync {
    /// The natural-weight cube's sensitivity is one scalar per channel; no
    /// image-sized backing is needed for that repeated value.
    fn scalar_sensitivity(&self) -> bool {
        false
    }
    /// Allocate storage whose complete logical contents will be owner-written.
    /// Slots are `2 * domain` for epoch arrays and `2 * domain + 1` for invariants.
    fn create(
        &self,
        domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError>;
}

/// Physical backing and the admitted maximum channel window.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct NormalStoragePlan {
    factory: Arc<dyn NormalStorageFactory>,
    window_channels: usize,
}

impl NormalStoragePlan {
    /// Bind one explicit allocation capability and positive channel bound.
    pub fn new(
        factory: Arc<dyn NormalStorageFactory>,
        window_channels: usize,
    ) -> Result<Self, SpectralOperatorError> {
        if window_channels == 0 {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        Ok(Self {
            factory,
            window_channels,
        })
    }

    /// Select fully admitted resident storage, using the same window lifecycle.
    pub fn resident(window_channels: usize) -> Result<Self, SpectralOperatorError> {
        Self::new(Arc::new(ResidentNormalStorage), window_channels)
    }
}

/// Fully covered normal primitives and their inseparable complete-data proof.
/// Channel-local payloads have been written to the admitted backing; coupled
/// coefficient families retain their distinct scientific representation.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataNormalState {
    pub(crate) primitives: NormalStatePrimitives,
    pub(crate) completion: CompleteDataOwnerCompletion,
}

impl CompleteDataNormalState {
    /// Exact complete-data proof retained through sealing.
    #[must_use]
    pub const fn completion(&self) -> &CompleteDataOwnerCompletion {
        &self.completion
    }

    /// Load explicit diagnostic data without changing completion authority.
    pub fn read_window(
        &self,
        channels: Range<usize>,
    ) -> Result<CompleteDataNormalWindow<'_>, SpectralOperatorError> {
        Ok(CompleteDataNormalWindow {
            primitives: self.primitives.read_window(channels)?,
        })
    }

    /// Explicit diagnostic fingerprint, independent of the storage window.
    /// This reads the arrays and is not part of ordinary completion or handoff.
    pub fn diagnostic_content_identity(&self) -> Result<LogicalIdentity, SpectralOperatorError> {
        self.primitives.content_identity()
    }
}

/// Loaded complete-data normal primitives. This is not a Final Normal State
/// completion and cannot authorize model updates or product publication.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataNormalWindow<'a> {
    primitives: NormalStateWindowPayload<'a>,
}

impl CompleteDataNormalWindow<'_> {
    /// Borrow the primary domain's explicitly loaded primitive window.
    #[must_use]
    pub fn primitives(&self) -> &SpectralOperatorPrimitives {
        self.primitives.primary()
    }
}

#[derive(Debug)]
pub(crate) enum NormalStatePrimitives {
    ChannelLocal(Box<[StoredChannelNormalDomain]>),
    Coupled(SpectralPrimitiveDomains),
}

/// Metadata-only projection used by generation owners and resource planning.
pub(crate) struct NormalDomainMetadata<'a> {
    pub(crate) shape: [usize; 2],
    pub(crate) slab: SpectralSlabPlan,
    pub(crate) polarizations: usize,
    pub(crate) coefficient_terms: usize,
    pub(crate) normal_moments: usize,
    pub(crate) reference_frequency_hz: Option<f64>,
    pub(crate) sum_weights: &'a [f64],
    pub(crate) published_sum_weights: &'a [f64],
    pub(crate) validity: &'a [SpectralChannelValidity],
}

impl NormalStatePrimitives {
    /// The resident domains of a coupled coefficient family.
    pub(crate) fn into_coupled(
        self,
    ) -> Result<Box<[SpectralDomainPrimitives]>, SpectralOperatorError> {
        match self {
            Self::Coupled(domains) => Ok(domains.domains),
            Self::ChannelLocal(_) => Err(SpectralOperatorError::ProblemMismatch),
        }
    }

    pub(crate) fn retire_obsolete(self) -> Result<(), SpectralOperatorError> {
        if let Self::ChannelLocal(domains) = self {
            for domain in domains {
                domain.storage.retire()?;
            }
        }
        Ok(())
    }
    pub(crate) fn retained_resident_bytes(&self) -> Result<u64, SpectralOperatorError> {
        match self {
            Self::ChannelLocal(domains) => domains.iter().try_fold(0u64, |bytes, domain| {
                bytes
                    .checked_add(domain.storage.retained_resident_bytes() as u64)
                    .and_then(|bytes| {
                        bytes.checked_add(domain.invariants.retained_resident_bytes() as u64)
                    })
                    .ok_or(SpectralOperatorError::ResidencyOverflow)
            }),
            Self::Coupled(_) => Ok(0),
        }
    }

    pub(crate) fn read_plane(
        &self,
        ordinal: usize,
        channel: usize,
        polarization: usize,
    ) -> Result<FinalNormalPlaneReader<'_>, SpectralOperatorError> {
        let (backing, plane, cells) = match self {
            Self::ChannelLocal(domains) => {
                let domain = domains
                    .get(ordinal)
                    .ok_or(SpectralOperatorError::InvalidSlab)?;
                let (plane, cells) = domain.validate_plane(channel, polarization)?;
                (NormalPlaneBacking::Stored(domain), plane, cells)
            }
            Self::Coupled(domains) => {
                let domain = domains
                    .get(ordinal)
                    .ok_or(SpectralOperatorError::InvalidSlab)?
                    .primitives();
                if !matches!(domain.basis, SpectralBasisPlan::Polynomial(plan) if plan.coefficient_term_count() == 1)
                {
                    return Err(SpectralOperatorError::NormalStorage("selective plane reads require channel-local or constant-polynomial normal state".into()));
                }
                if domain.slab.core_depth() != 1
                    || !domain.slab.core_range().contains(&channel)
                    || polarization >= domain.polarizations
                {
                    return Err(SpectralOperatorError::InvalidSlab);
                }
                let cells = checked_cells(domain.shape)?;
                let values = cells
                    .checked_mul(domain.polarizations)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                if domain.dirty.len() != values
                    || domain.psf.len() != values
                    || domain.sensitivity.len() != values
                    || domain.sum_weights.len() != domain.polarizations
                    || domain.published_sum_weights.len() != domain.polarizations
                    || domain.validity.len() != domain.polarizations
                {
                    return Err(SpectralOperatorError::ProblemMismatch);
                }
                (NormalPlaneBacking::Resident(domain), polarization, cells)
            }
        };
        Ok(FinalNormalPlaneReader {
            backing,
            channel,
            plane,
            cells,
        })
    }

    pub(crate) fn maximum_read_channels(&self) -> usize {
        match self {
            Self::ChannelLocal(domains) => domains
                .iter()
                .map(|domain| domain.window_channels)
                .min()
                .expect("normal state has domains"),
            Self::Coupled(_) => self.primary_metadata().slab.core_depth(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::ChannelLocal(domains) => domains.len(),
            Self::Coupled(domains) => domains.len(),
        }
    }

    pub(crate) fn metadata(&self, ordinal: usize) -> Option<NormalDomainMetadata<'_>> {
        Some(match self {
            Self::ChannelLocal(domains) => {
                let d = domains.get(ordinal)?;
                NormalDomainMetadata {
                    shape: d.shape,
                    slab: SpectralSlabPlan {
                        total_channels: d.total_channels,
                        core_start: 0,
                        core_end: d.total_channels,
                        resident_start: 0,
                        resident_end: d.total_channels,
                    },
                    polarizations: d.polarizations,
                    coefficient_terms: d.total_channels,
                    normal_moments: d.total_channels,
                    reference_frequency_hz: None,
                    sum_weights: &d.sum_weights,
                    published_sum_weights: &d.published_sum_weights,
                    validity: &d.validity,
                }
            }
            Self::Coupled(domains) => {
                let d = domains.get(ordinal)?;
                let p = d.primitives();
                NormalDomainMetadata {
                    shape: p.shape(),
                    slab: p.slab(),
                    polarizations: p.polarization_count(),
                    coefficient_terms: p.coefficient_term_count(),
                    normal_moments: p.normal_moment_count(),
                    reference_frequency_hz: p.reference_frequency_hz(),
                    sum_weights: p.sum_weights(),
                    published_sum_weights: p.published_sum_weights(),
                    validity: p.channel_validity(),
                }
            }
        })
    }

    pub(crate) fn primary_metadata(&self) -> NormalDomainMetadata<'_> {
        self.metadata(0)
            .expect("completed normal state has a primary image domain")
    }

    pub(crate) fn read_window(
        &self,
        channels: Range<usize>,
    ) -> Result<NormalStateWindowPayload<'_>, SpectralOperatorError> {
        match self {
            Self::ChannelLocal(domains) => {
                let windows = domains
                    .iter()
                    .map(|d| d.read_window(channels.clone()))
                    .collect::<Result<Box<[_]>, _>>()?;
                Ok(NormalStateWindowPayload::ChannelLocal(
                    SpectralPrimitiveDomains::new(windows)?,
                ))
            }
            Self::Coupled(domains) => {
                if channels != domains.slab().core_range() {
                    return Err(SpectralOperatorError::InvalidSlab);
                }
                Ok(NormalStateWindowPayload::Coupled(domains))
            }
        }
    }

    pub(crate) fn promote_major_cycle_residual(
        self,
        model: ModelGenerationId,
    ) -> Result<Self, SpectralOperatorError> {
        match self {
            Self::ChannelLocal(mut domains) => {
                for domain in &mut domains {
                    domain.promote_major_cycle_residual(model)?;
                }
                Ok(Self::ChannelLocal(domains))
            }
            Self::Coupled(domains) => {
                Ok(Self::Coupled(domains.promote_major_cycle_residual(model)?))
            }
        }
    }

    pub(crate) fn content_identity(&self) -> Result<LogicalIdentity, SpectralOperatorError> {
        match self {
            Self::Coupled(domains) => Ok(domains.normal_state_content_identity()),
            Self::ChannelLocal(domains) => {
                let mut encoder = crate::Encoder::new(NORMAL_STATE_CONTENT_DOMAIN, 4);
                encoder.usize(domains.len());
                for d in domains {
                    encoder.usize(d.ordinal);
                    match &d.role {
                        ImageDomainRole::Main => encoder.u8(0),
                        ImageDomainRole::Outlier(name) => {
                            encoder.u8(1);
                            encoder.bytes(name.as_bytes());
                        }
                    }
                    encoder.identity(d.content_identity()?.as_bytes());
                }
                Ok(LogicalIdentity::from_sha256(encoder.finish()))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum NormalStateWindowPayload<'a> {
    ChannelLocal(SpectralPrimitiveDomains),
    Coupled(&'a SpectralPrimitiveDomains),
}

impl std::ops::Deref for NormalStateWindowPayload<'_> {
    type Target = SpectralPrimitiveDomains;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::ChannelLocal(domains) => domains,
            Self::Coupled(domains) => domains,
        }
    }
}

#[derive(Debug)]
struct ResidentNormalStorage;

impl NormalStorageFactory for ResidentNormalStorage {
    fn create(
        &self,
        _domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
        Ok(Box::new(vec![0.0; scalars].into_boxed_slice()))
    }
}

impl NormalArrayStorage for Box<[f64]> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
        let end = start
            .checked_add(len)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        Ok(Cow::Borrowed(
            self.get(start..end)
                .ok_or(SpectralOperatorError::InvalidSlab)?,
        ))
    }

    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
        let end = start
            .checked_add(values.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        self.get_mut(start..end)
            .ok_or(SpectralOperatorError::InvalidSlab)?
            .copy_from_slice(values);
        Ok(())
    }
}

/// Logical field offsets with epoch arrays before immutable arrays. Physical
/// invariant offsets subtract `epoch_scalars`; the two allocations never alias.
#[derive(Debug, Clone)]
struct ChannelNormalFields {
    dirty: Range<usize>,
    invariant_dirty: Option<Range<usize>>,
    psf: Range<usize>,
    sensitivity: Range<usize>,
    scalar_sensitivity: bool,
    major_cycle_residual: Option<Range<usize>>,
    scalars: usize,
    epoch_scalars: usize,
}

impl ChannelNormalFields {
    fn new(
        values: usize,
        invariant: bool,
        residual: bool,
        scalar_sensitivity: bool,
    ) -> Result<Self, SpectralOperatorError> {
        let complex = values
            .checked_mul(2)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let mut end = 0_usize;
        let mut field = |len: usize| -> Result<Range<usize>, SpectralOperatorError> {
            let start = end;
            end = end
                .checked_add(len)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            Ok(start..end)
        };
        let dirty = field(complex)?;
        let major_cycle_residual = residual.then(|| field(complex)).transpose()?;
        let epoch_scalars = major_cycle_residual.as_ref().unwrap_or(&dirty).end;
        let invariant_dirty = invariant.then(|| field(complex)).transpose()?;
        let psf = field(complex)?;
        let sensitivity = field(if scalar_sensitivity { 0 } else { values })?;
        Ok(Self {
            dirty,
            invariant_dirty,
            psf,
            sensitivity,
            scalar_sensitivity,
            major_cycle_residual,
            scalars: end,
            epoch_scalars,
        })
    }
}

/// One global channel-local domain. No image-sized array is retained here.
#[derive(Debug)]
pub(crate) struct StoredChannelNormalDomain {
    ordinal: usize,
    role: ImageDomainRole,
    shape: [usize; 2],
    total_channels: usize,
    polarizations: usize,
    sum_weights: Box<[f64]>,
    published_sum_weights: Box<[f64]>,
    validity: Box<[SpectralChannelValidity]>,
    residual_model: Option<ModelGenerationId>,
    major_cycle_residual_promoted: bool,
    fields: ChannelNormalFields,
    storage: Box<dyn NormalArrayStorage>,
    invariants: Arc<Box<dyn NormalArrayStorage>>,
    window_channels: usize,
    next_channel: usize,
}

/// Metadata-only selection of one completed channel-local or constant-basis plane.
///
/// Each field read loads only that field and polarization. The reader borrows
/// the global completion owner and retains no image payload or cache.
/// Multi-term Taylor families use their existing complete-family readers.
#[derive(Debug)]
pub struct FinalNormalPlaneReader<'a> {
    backing: NormalPlaneBacking<'a>,
    channel: usize,
    plane: usize,
    cells: usize,
}

#[derive(Debug)]
enum NormalPlaneBacking<'a> {
    Stored(&'a StoredChannelNormalDomain),
    Resident(&'a SpectralOperatorPrimitives),
}

impl<'a> FinalNormalPlaneReader<'a> {
    /// Direction-plane dimensions, without reading image payloads.
    #[must_use]
    pub fn shape(&self) -> [usize; 2] {
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.shape,
            NormalPlaneBacking::Resident(d) => d.shape,
        }
    }

    /// Absolute output-channel ordinal.
    #[must_use]
    pub fn output_channel(&self) -> usize {
        self.channel
    }

    /// Accumulated normal-equation weight for this polarization plane.
    #[must_use]
    pub fn sum_weight(&self) -> f64 {
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.sum_weights[self.plane],
            NormalPlaneBacking::Resident(d) => d.sum_weights[self.plane],
        }
    }

    /// Publication weight for this polarization plane.
    #[must_use]
    pub fn published_sum_weight(&self) -> f64 {
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.published_sum_weights[self.plane],
            NormalPlaneBacking::Resident(d) => d.published_sum_weights[self.plane],
        }
    }

    /// Mapped, blank, or unmapped support for this polarization plane.
    #[must_use]
    pub fn validity(&self) -> SpectralChannelValidity {
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.validity[self.plane],
            NormalPlaneBacking::Resident(d) => d.validity[self.plane],
        }
    }

    /// Load the authoritative dirty/residual field, including promoted residuals.
    pub fn read_residual(&self) -> Result<Cow<'a, [Complex64]>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_complex(&d.fields.dirty, offset, self.cells),
            NormalPlaneBacking::Resident(d) => {
                let values = d
                    .dirty()
                    .complex()
                    .ok_or(SpectralOperatorError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }

    pub(crate) fn read_residual_real(
        &self,
    ) -> Result<Option<Cow<'a, [f32]>>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_real(&d.fields.dirty, offset, self.cells),
            NormalPlaneBacking::Resident(_) => Ok(None),
        }
    }

    /// Load only the selected unnormalized point-spread-function plane.
    pub fn read_psf(&self) -> Result<Cow<'a, [Complex64]>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_complex(&d.fields.psf, offset, self.cells),
            NormalPlaneBacking::Resident(d) => {
                let values = d
                    .psf()
                    .complex()
                    .ok_or(SpectralOperatorError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }

    pub(crate) fn read_psf_real(&self) -> Result<Option<Cow<'a, [f32]>>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_real(&d.fields.psf, offset, self.cells),
            NormalPlaneBacking::Resident(_) => Ok(None),
        }
    }

    /// Load only the selected unnormalized sensitivity plane.
    pub fn read_sensitivity(&self) -> Result<Cow<'a, [f64]>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => {
                if d.fields.scalar_sensitivity {
                    Ok(Cow::Owned(vec![d.sum_weights[self.plane]; self.cells]))
                } else {
                    let start = d.fields.sensitivity.start + offset;
                    d.read_scalars(start..start + self.cells)
                }
            }
            NormalPlaneBacking::Resident(d) => {
                let values = d
                    .sensitivity()
                    .dense()
                    .ok_or(SpectralOperatorError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }
}

impl StoredChannelNormalDomain {
    pub(crate) fn refresh(
        &self,
        model: ModelGenerationId,
        plan: &NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        if !self.is_complete() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if !self.major_cycle_residual_promoted
            || self.fields.invariant_dirty.is_some()
            || self.fields.major_cycle_residual.is_some()
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let mut fields = self.fields.clone();
        fields.dirty = 0..self.fields.dirty.len();
        let storage = plan.factory.create(self.ordinal * 2, fields.dirty.len())?;
        if storage.len() != fields.dirty.len() {
            return Err(SpectralOperatorError::NormalStorage(
                "allocated scalar capacity differs from the admitted layout".into(),
            ));
        }
        Ok(Self {
            ordinal: self.ordinal,
            role: self.role.clone(),
            shape: self.shape,
            total_channels: self.total_channels,
            polarizations: self.polarizations,
            sum_weights: self.sum_weights.clone(),
            published_sum_weights: self.published_sum_weights.clone(),
            validity: self.validity.clone(),
            residual_model: Some(model),
            major_cycle_residual_promoted: true,
            fields,
            storage,
            invariants: self.invariants.clone(),
            window_channels: plan.window_channels,
            next_channel: 0,
        })
    }

    /// Write the refreshed residual planes `[channel][pol]` (x-major) of the
    /// next channel range, formed with model generation `model`.
    pub(crate) fn append_residual_planes(
        &mut self,
        range: Range<usize>,
        shape: [usize; 2],
        model: ModelGenerationId,
        residual: &[f32],
    ) -> Result<(), SpectralOperatorError> {
        if range.start != self.next_channel
            || range.start >= range.end
            || range.end > self.total_channels
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if self.window_channels == 0 {
            return Err(SpectralOperatorError::NormalStorage(
                "normal-state write exceeds the admitted channel window".into(),
            ));
        }
        if shape != self.shape
            || self.residual_model != Some(model)
            || residual.len() != range.len() * self.polarizations * checked_cells(self.shape)?
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let plane_values = self.polarizations * checked_cells(self.shape)?;
        for (index, values) in residual
            .chunks(self.window_channels * plane_values)
            .enumerate()
        {
            self.storage.write_real(
                (range.start * plane_values + index * self.window_channels * plane_values) * 2,
                values,
            )?;
        }
        self.next_channel = range.end;
        Ok(())
    }

    fn validate_plane(
        &self,
        channel: usize,
        polarization: usize,
    ) -> Result<(usize, usize), SpectralOperatorError> {
        if !self.is_complete() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if channel >= self.total_channels || polarization >= self.polarizations {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        if self.window_channels == 0 {
            return Err(SpectralOperatorError::NormalStorage(
                "normal-state read exceeds the admitted channel window".into(),
            ));
        }
        Ok((
            channel * self.polarizations + polarization,
            checked_cells(self.shape)?,
        ))
    }

    pub(crate) fn begin(
        first: SpectralDomainPrimitives,
        plan: &NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let p = first.primitives();
        let total_channels = p.slab.total_channels();
        if p.slab.core_range().start != 0 || p.polarizations == 0 {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let planes = total_channels
            .checked_mul(p.polarizations)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let values = planes
            .checked_mul(checked_cells(p.shape)?)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let fields = ChannelNormalFields::new(
            values,
            p.invariant_dirty.is_some()
                || p.cube_real
                    .as_ref()
                    .is_some_and(|real| real.invariant_dirty.is_some()),
            p.major_cycle_residual.is_some(),
            plan.factory.scalar_sensitivity(),
        )?;
        let storage = plan
            .factory
            .create(first.domain_ordinal * 2, fields.epoch_scalars)?;
        let invariants = plan.factory.create(
            first.domain_ordinal * 2 + 1,
            fields.scalars - fields.epoch_scalars,
        )?;
        if storage.len() != fields.epoch_scalars
            || invariants.len() != fields.scalars - fields.epoch_scalars
        {
            return Err(SpectralOperatorError::NormalStorage(
                "allocated scalar capacity differs from the admitted layout".into(),
            ));
        }
        let mut result = Self {
            ordinal: first.domain_ordinal,
            role: first.domain_role.clone(),
            shape: p.shape,
            total_channels,
            polarizations: p.polarizations,
            sum_weights: vec![0.0; planes].into_boxed_slice(),
            published_sum_weights: vec![0.0; planes].into_boxed_slice(),
            validity: vec![SpectralChannelValidity::Unmapped; planes].into_boxed_slice(),
            residual_model: p.residual_model,
            major_cycle_residual_promoted: p.major_cycle_residual_promoted,
            fields,
            storage,
            invariants: Arc::new(invariants),
            window_channels: plan.window_channels,
            next_channel: 0,
        };
        result.append(first)?;
        Ok(result)
    }

    pub(crate) fn append(
        &mut self,
        domain: SpectralDomainPrimitives,
    ) -> Result<(), SpectralOperatorError> {
        let p = domain.primitives();
        let compact = p.cube_real.as_ref();
        let range = p.slab.core_range();
        if range.start != self.next_channel || range.end > self.total_channels {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if range.len() > self.window_channels {
            return Err(SpectralOperatorError::NormalStorage(
                "normal-state write exceeds the admitted channel window".into(),
            ));
        }
        if domain.domain_ordinal != self.ordinal
            || domain.domain_role != self.role
            || p.shape != self.shape
            || p.slab.total_channels() != self.total_channels
            || p.polarizations != self.polarizations
            || p.basis != SpectralBasisPlan::ChannelLocal
            || p.residual_model != self.residual_model
            || p.major_cycle_residual_promoted != self.major_cycle_residual_promoted
            || (p.invariant_dirty.is_some()
                || compact.is_some_and(|real| real.invariant_dirty.is_some()))
                != self.fields.invariant_dirty.is_some()
            || p.major_cycle_residual.is_some() != self.fields.major_cycle_residual.is_some()
            || p.primary_beam_weighted_sum.is_some()
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let cells = checked_cells(self.shape)?;
        let plane_offset = range.start * self.polarizations;
        let planes = range.len() * self.polarizations;
        let values = planes * cells;
        if compact.map_or_else(
            || p.dirty.len() != values || p.psf.len() != values,
            |real| {
                !p.dirty.is_empty()
                    || !p.psf.is_empty()
                    || real.dirty.len() != values
                    || real.psf.len() != values
            },
        ) || (!self.fields.scalar_sensitivity
            && compact.is_none()
            && p.sensitivity.len() != values)
            || p.invariant_dirty
                .as_ref()
                .is_some_and(|v| v.len() != values)
            || compact
                .and_then(|real| real.invariant_dirty.as_ref())
                .is_some_and(|v| v.len() != values)
            || p.major_cycle_residual
                .as_ref()
                .is_some_and(|v| v.len() != values)
            || p.sum_weights.len() != planes
            || p.published_sum_weights.len() != planes
            || p.validity.len() != planes
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let offset = plane_offset * cells;
        if let Some(real) = compact {
            self.storage
                .write_real(self.fields.dirty.start + 2 * offset, &real.dirty)?;
            if let (Some(field), Some(source)) =
                (&self.fields.invariant_dirty, &real.invariant_dirty)
            {
                Arc::get_mut(&mut self.invariants)
                    .ok_or(SpectralOperatorError::IncompleteCoverage)?
                    .write_real(field.start - self.fields.epoch_scalars + 2 * offset, source)?;
            }
            Arc::get_mut(&mut self.invariants)
                .ok_or(SpectralOperatorError::IncompleteCoverage)?
                .write_real(
                    self.fields.psf.start - self.fields.epoch_scalars + 2 * offset,
                    &real.psf,
                )?;
        }
        for (field, source) in [
            (Some(&self.fields.dirty), Some(p.dirty.as_ref())),
            (
                self.fields.invariant_dirty.as_ref(),
                p.invariant_dirty.as_deref(),
            ),
            (Some(&self.fields.psf), Some(p.psf.as_ref())),
            (
                self.fields.major_cycle_residual.as_ref(),
                p.major_cycle_residual.as_deref(),
            ),
        ] {
            if let (Some(field), Some(source)) = (field, source)
                && !source.is_empty()
            {
                let scalars = complex_scalars(source)?;
                let start = field.start + 2 * offset;
                if field.start >= self.fields.epoch_scalars {
                    Arc::get_mut(&mut self.invariants)
                        .ok_or(SpectralOperatorError::IncompleteCoverage)?
                        .write(start - self.fields.epoch_scalars, scalars)?;
                } else {
                    self.storage.write(start, scalars)?;
                }
            }
        }
        if !self.fields.scalar_sensitivity {
            let invariant = Arc::get_mut(&mut self.invariants)
                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
            let start = self.fields.sensitivity.start - self.fields.epoch_scalars + offset;
            if compact.is_some() {
                let mut window = [0.0; 512];
                for (plane, &weight) in p.sum_weights.iter().enumerate() {
                    window.fill(weight);
                    for chunk in (0..cells).step_by(window.len()) {
                        invariant.write(
                            start + plane * cells + chunk,
                            &window[..window.len().min(cells - chunk)],
                        )?;
                    }
                }
            } else {
                invariant.write(start, &p.sensitivity)?;
            }
        }
        self.sum_weights[plane_offset..plane_offset + planes].copy_from_slice(&p.sum_weights);
        self.published_sum_weights[plane_offset..plane_offset + planes]
            .copy_from_slice(&p.published_sum_weights);
        self.validity[plane_offset..plane_offset + planes].copy_from_slice(&p.validity);
        self.next_channel = range.end;
        Ok(())
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.next_channel == self.total_channels
    }

    pub(crate) fn promote_major_cycle_residual(
        &mut self,
        expected: ModelGenerationId,
    ) -> Result<(), SpectralOperatorError> {
        if !self.is_complete() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if self.residual_model != Some(expected) {
            return Err(SpectralOperatorError::ModelMismatch);
        }
        if !self.major_cycle_residual_promoted {
            self.fields.dirty = self
                .fields
                .major_cycle_residual
                .take()
                .ok_or(SpectralOperatorError::MissingMajorCycleResidual)?;
            self.major_cycle_residual_promoted = true;
        }
        Ok(())
    }

    fn read_scalars(&self, range: Range<usize>) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
        let result = if range.start >= self.fields.epoch_scalars {
            self.invariants
                .read(range.start - self.fields.epoch_scalars, range.len())?
        } else {
            self.storage.read(range.start, range.len())?
        };
        if result.len() != range.len() {
            return Err(SpectralOperatorError::NormalStorage(
                "normal backing returned an incorrect window length".into(),
            ));
        }
        Ok(result)
    }

    fn read_complex(
        &self,
        field: &Range<usize>,
        offset: usize,
        values: usize,
    ) -> Result<Cow<'_, [Complex64]>, SpectralOperatorError> {
        let start = field.start + offset * 2;
        let result = if start >= self.fields.epoch_scalars {
            self.invariants
                .read_complex(start - self.fields.epoch_scalars, values)
        } else {
            self.storage.read_complex(start, values)
        }?;
        if result.len() != values {
            return Err(SpectralOperatorError::NormalStorage(
                "normal backing returned an incorrect complex window length".into(),
            ));
        }
        Ok(result)
    }

    fn read_real(
        &self,
        field: &Range<usize>,
        offset: usize,
        values: usize,
    ) -> Result<Option<Cow<'_, [f32]>>, SpectralOperatorError> {
        let start = field.start + 2 * offset;
        let result = if field.start >= self.fields.epoch_scalars {
            self.invariants
                .read_real(start - self.fields.epoch_scalars, values)
        } else {
            self.storage.read_real(start, values)
        }?;
        if result.as_ref().is_some_and(|window| window.len() != values) {
            return Err(SpectralOperatorError::NormalStorage(
                "normal backing returned an incorrect real window length".into(),
            ));
        }
        Ok(result)
    }

    pub(crate) fn read_window(
        &self,
        range: Range<usize>,
    ) -> Result<SpectralDomainPrimitives, SpectralOperatorError> {
        if !self.is_complete() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if range.start >= range.end || range.end > self.total_channels {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        if range.len() > self.window_channels {
            return Err(SpectralOperatorError::NormalStorage(
                "normal-state read exceeds the admitted channel window".into(),
            ));
        }
        let cells = checked_cells(self.shape)?;
        let plane_range = range.start * self.polarizations..range.end * self.polarizations;
        let offset = plane_range.start * cells;
        let values = plane_range.len() * cells;
        Ok(SpectralDomainPrimitives::new(
            self.ordinal,
            self.role.clone(),
            SpectralOperatorPrimitives {
                shape: self.shape,
                clark_workspace: std::sync::Mutex::new(None),
                slab: SpectralSlabPlan {
                    total_channels: self.total_channels,
                    core_start: range.start,
                    core_end: range.end,
                    resident_start: range.start,
                    resident_end: range.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations: self.polarizations,
                dirty: self
                    .read_complex(&self.fields.dirty, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                cube_real: None,
                invariant_dirty: self
                    .fields
                    .invariant_dirty
                    .as_ref()
                    .map(|f| {
                        self.read_complex(f, offset, values)
                            .map(|v| v.into_owned().into_boxed_slice())
                    })
                    .transpose()?,
                psf: self
                    .read_complex(&self.fields.psf, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                sensitivity: if self.fields.scalar_sensitivity {
                    self.sum_weights[plane_range.clone()]
                        .iter()
                        .flat_map(|&weight| std::iter::repeat_n(weight, cells))
                        .collect()
                } else {
                    self.read_scalars(
                        self.fields.sensitivity.start + offset
                            ..self.fields.sensitivity.start + offset + values,
                    )?
                    .into_owned()
                    .into_boxed_slice()
                },
                primary_beam_weighted_sum: None,
                sum_weights: self.sum_weights[plane_range.clone()].into(),
                published_sum_weights: self.published_sum_weights[plane_range.clone()].into(),
                validity: self.validity[plane_range].into(),
                major_cycle_residual: self
                    .fields
                    .major_cycle_residual
                    .as_ref()
                    .map(|f| {
                        self.read_complex(f, offset, values)
                            .map(|v| v.into_owned().into_boxed_slice())
                    })
                    .transpose()?,
                major_cycle_residual_promoted: self.major_cycle_residual_promoted,
                residual_model: self.residual_model,
            },
        ))
    }

    pub(crate) fn content_identity(&self) -> Result<LogicalIdentity, SpectralOperatorError> {
        if !self.is_complete() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let published_differ = self.published_sum_weights != self.sum_weights;
        let mut encoder = crate::Encoder::new(
            NORMAL_STATE_CONTENT_DOMAIN,
            if published_differ { 4 } else { 1 },
        );
        encoder.usize(self.shape[0]);
        encoder.usize(self.shape[1]);
        encoder.usize(self.total_channels);
        encoder.usize(0);
        encoder.usize(self.total_channels);
        let window_values = checked_cells(self.shape)?
            .checked_mul(self.polarizations)
            .and_then(|n| n.checked_mul(self.window_channels.min(self.total_channels)))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        for field in [&self.fields.dirty, &self.fields.psf] {
            let width = window_values
                .checked_mul(2)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            for start in (field.start..field.end).step_by(width) {
                for &value in self
                    .read_scalars(start..start.saturating_add(width).min(field.end))?
                    .iter()
                {
                    encoder.u64(value.to_bits());
                }
            }
        }
        if self.fields.scalar_sensitivity {
            let cells = checked_cells(self.shape)?;
            for &weight in &self.sum_weights {
                for _ in 0..cells {
                    encoder.u64(canonical_f64_bits(weight));
                }
            }
        } else {
            let field = &self.fields.sensitivity;
            for start in (field.start..field.end).step_by(window_values) {
                for &value in self
                    .read_scalars(start..start.saturating_add(window_values).min(field.end))?
                    .iter()
                {
                    encoder.u64(canonical_f64_bits(value));
                }
            }
        }
        for &value in &self.sum_weights {
            encoder.u64(canonical_f64_bits(value));
        }
        if published_differ {
            for &value in &self.published_sum_weights {
                encoder.u64(canonical_f64_bits(value));
            }
        }
        for validity in &self.validity {
            encoder.u8(match validity {
                SpectralChannelValidity::Valid => 0,
                SpectralChannelValidity::Blank => 1,
                SpectralChannelValidity::Unmapped => 2,
            });
        }
        Ok(LogicalIdentity::from_sha256(encoder.finish()))
    }
}
