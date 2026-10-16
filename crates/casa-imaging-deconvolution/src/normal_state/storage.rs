// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact channel-local normal-state backing, independent of physical storage.
//!
//! [`NormalArrayStorage`] and [`NormalStorageFactory`] are the seam behind
//! which `casa-imaging-runtime` pages a cube's normal state; the resident
//! backing here is their other implementation.

use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

use casa_imaging_model::ImageDomainRole;
use num_complex::Complex64;

use super::{
    NormalPrimitives, NormalStateError, SpectralBasisPlan, SpectralChannelValidity,
    SpectralDomainPrimitives, SpectralPrimitiveDomains, SpectralSlabPlan, checked_cells,
};

// num-complex 0.4.6 supplies Pod for its repr(C) real/imaginary pair.
// Checked casts preserve all floating-point bits without copying payloads.
fn complex_scalars(values: &[Complex64]) -> Result<&[f64], NormalStateError> {
    bytemuck::try_cast_slice(values)
        .map_err(|error| NormalStateError::NormalStorage(error.to_string()))
}

fn scalar_complex(values: Cow<'_, [f64]>) -> Result<Cow<'_, [Complex64]>, NormalStateError> {
    match values {
        Cow::Borrowed(values) => bytemuck::try_cast_slice(values)
            .map(Cow::Borrowed)
            .map_err(|error| NormalStateError::NormalStorage(error.to_string())),
        Cow::Owned(values) => bytemuck::allocation::try_cast_vec(values)
            .map(Cow::Owned)
            .map_err(|(error, _)| NormalStateError::NormalStorage(error.to_string())),
    }
}

/// One array of a normal state's scalars, written and read only by the
/// normal state that allocated it.
///
/// Complex values are stored as consecutive real/imaginary f64 values. Access
/// must not enlarge an admitted cache. The owner writes every logical value
/// before transferring the generation; storage handles must not permit
/// mutation through aliases retained outside this capability. The trait is
/// the dependency boundary for `casa-imaging-runtime`'s paged storage, the
/// second implementation beside the resident `Box<[f64]>`.
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
    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, NormalStateError>;
    /// Read one image-plane complex window directly into the consuming shape.
    /// Compact real backings can widen here without a second scalar buffer.
    fn read_complex(
        &self,
        start: usize,
        values: usize,
    ) -> Result<Cow<'_, [Complex64]>, NormalStateError> {
        scalar_complex(self.read(start, values * 2)?)
    }
    /// Borrow or load a real Float image directly when the backing stores one.
    /// Other normal families keep their existing complex access path.
    fn read_real(
        &self,
        _start: usize,
        _values: usize,
    ) -> Result<Option<Cow<'_, [f32]>>, NormalStateError> {
        Ok(None)
    }
    /// Replace a bounded scalar window without resizing the array.
    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), NormalStateError>;
    /// Write a real Float image window at complex-scalar offsets. Other normal
    /// implementations may widen through their scalar interface; the managed
    /// cube backing writes Float planes directly.
    fn write_real(&mut self, start: usize, values: &[f32]) -> Result<(), NormalStateError> {
        if !start.is_multiple_of(2) {
            return Err(NormalStateError::InvalidSlab);
        }
        let mut widened = [0.0_f64; 512];
        for (chunk_index, chunk) in values.chunks(256).enumerate() {
            let offset = chunk_index
                .checked_mul(widened.len())
                .and_then(|offset| start.checked_add(offset))
                .ok_or(NormalStateError::ResidencyOverflow)?;
            for (pair, &value) in widened.as_chunks_mut::<2>().0.iter_mut().zip(chunk) {
                pair[0] = f64::from(value);
            }
            self.write(offset, &widened[..chunk.len() * 2])?;
        }
        Ok(())
    }

    /// Release a superseded epoch after its replacement is complete.
    fn retire(self: Box<Self>) -> Result<(), NormalStateError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Allocates a normal state's arrays at their exact logical sizes: resident
/// here, paged in `casa-imaging-runtime` (the dependency boundary).
pub trait NormalStorageFactory: fmt::Debug + Send + Sync {
    /// Allocate storage whose complete logical contents will be owner-written.
    /// Slots are `2 * domain` for epoch arrays and `2 * domain + 1` for invariants.
    fn create(
        &self,
        domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, NormalStateError>;
}

/// Where a normal state's arrays live, and the most channels a read or
/// write may hold at once.
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
    ) -> Result<Self, NormalStateError> {
        if window_channels == 0 {
            return Err(NormalStateError::InvalidSlab);
        }
        Ok(Self {
            factory,
            window_channels,
        })
    }

    /// Select fully admitted resident storage, using the same window lifecycle.
    pub fn resident(window_channels: usize) -> Result<Self, NormalStateError> {
        Self::new(Arc::new(ResidentNormalStorage), window_channels)
    }
}

/// A completed normal state's domains: channel-local planes written to
/// their backing, or a constant or Taylor family held resident.
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
    pub(crate) sum_weights: &'a [f64],
    pub(crate) published_sum_weights: &'a [f64],
    pub(crate) validity: &'a [SpectralChannelValidity],
}

impl NormalStatePrimitives {
    pub(crate) fn basis(&self) -> super::NormalBasis {
        match self {
            Self::ChannelLocal(_) => super::NormalBasis::ChannelLocal,
            Self::Coupled(domains) => domains.primary().basis.basis(),
        }
    }

    /// The resident domains of a coupled coefficient family.
    pub(crate) fn into_coupled(self) -> Result<Box<[SpectralDomainPrimitives]>, NormalStateError> {
        match self {
            Self::Coupled(domains) => Ok(domains.domains),
            Self::ChannelLocal(_) => Err(NormalStateError::ProblemMismatch),
        }
    }

    pub(crate) fn retire_obsolete(self) -> Result<(), NormalStateError> {
        if let Self::ChannelLocal(domains) = self {
            for domain in domains {
                domain.storage.retire()?;
            }
        }
        Ok(())
    }
    pub(crate) fn resident_bytes(&self) -> Result<u64, NormalStateError> {
        match self {
            Self::ChannelLocal(domains) => domains.iter().try_fold(0u64, |bytes, domain| {
                bytes
                    .checked_add(domain.storage.retained_resident_bytes() as u64)
                    .and_then(|bytes| {
                        bytes.checked_add(domain.invariants.retained_resident_bytes() as u64)
                    })
                    .ok_or(NormalStateError::ResidencyOverflow)
            }),
            Self::Coupled(domains) => Ok(domains.owned_bytes() as u64),
        }
    }

    pub(crate) fn read_plane(
        &self,
        ordinal: usize,
        channel: usize,
        polarization: usize,
    ) -> Result<FinalNormalPlaneReader<'_>, NormalStateError> {
        let (backing, plane, cells) = match self {
            Self::ChannelLocal(domains) => {
                let domain = domains.get(ordinal).ok_or(NormalStateError::InvalidSlab)?;
                let (plane, cells) = domain.validate_plane(channel, polarization)?;
                (NormalPlaneBacking::Stored(domain), plane, cells)
            }
            Self::Coupled(domains) => {
                let domain = domains
                    .get(ordinal)
                    .ok_or(NormalStateError::InvalidSlab)?
                    .primitives();
                if domain.basis != (SpectralBasisPlan::Polynomial { terms: 1 }) {
                    return Err(NormalStateError::NormalStorage(
                        "a single plane is read only from a channel-local or constant-basis normal state"
                            .into(),
                    ));
                }
                if domain.slab.core_depth() != 1
                    || !domain.slab.core_range().contains(&channel)
                    || polarization >= domain.polarizations
                {
                    return Err(NormalStateError::InvalidSlab);
                }
                let cells = checked_cells(domain.shape)?;
                let values = cells
                    .checked_mul(domain.polarizations)
                    .ok_or(NormalStateError::ResidencyOverflow)?;
                if domain.dirty.len() != values
                    || domain.psf.len() != values
                    || domain.sensitivity.len() != values
                    || domain.sum_weights.len() != domain.polarizations
                    || domain.published_sum_weights.len() != domain.polarizations
                    || domain.validity.len() != domain.polarizations
                {
                    return Err(NormalStateError::ProblemMismatch);
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
                    },
                    polarizations: d.polarizations,
                    coefficient_terms: d.total_channels,
                    normal_moments: d.total_channels,
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
    ) -> Result<NormalStateWindowPayload<'_>, NormalStateError> {
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
                    return Err(NormalStateError::InvalidSlab);
                }
                Ok(NormalStateWindowPayload::Coupled(domains))
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
    ) -> Result<Box<dyn NormalArrayStorage>, NormalStateError> {
        Ok(Box::new(vec![0.0; scalars].into_boxed_slice()))
    }
}

impl NormalArrayStorage for Box<[f64]> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, NormalStateError> {
        let end = start
            .checked_add(len)
            .ok_or(NormalStateError::ResidencyOverflow)?;
        Ok(Cow::Borrowed(
            self.get(start..end).ok_or(NormalStateError::InvalidSlab)?,
        ))
    }

    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), NormalStateError> {
        let end = start
            .checked_add(values.len())
            .ok_or(NormalStateError::ResidencyOverflow)?;
        self.get_mut(start..end)
            .ok_or(NormalStateError::InvalidSlab)?
            .copy_from_slice(values);
        Ok(())
    }
}

/// Logical field offsets with epoch arrays before immutable arrays. Physical
/// invariant offsets subtract `epoch_scalars`; the two allocations never alias.
///
/// Every field is a complex-scalar span, so a paged backing holds whole
/// planes. A real field (the compact residual, PSF and weight image) keeps
/// its values at even offsets. The sensitivity is either such a real weight
/// image beside the PSF, which a residual refresh shares, or empty when it is
/// each plane's `sumwt` alone.
#[derive(Debug, Clone)]
struct ChannelNormalFields {
    dirty: Range<usize>,
    psf: Range<usize>,
    sensitivity: Range<usize>,
    scalars: usize,
    epoch_scalars: usize,
}

impl ChannelNormalFields {
    fn new(values: usize, dense_sensitivity: bool) -> Result<Self, NormalStateError> {
        let complex = values
            .checked_mul(2)
            .ok_or(NormalStateError::ResidencyOverflow)?;
        let mut end = 0_usize;
        let mut field = |len: usize| -> Result<Range<usize>, NormalStateError> {
            let start = end;
            end = end
                .checked_add(len)
                .ok_or(NormalStateError::ResidencyOverflow)?;
            Ok(start..end)
        };
        let dirty = field(complex)?;
        let epoch_scalars = dirty.end;
        let psf = field(complex)?;
        let sensitivity = field(if dense_sensitivity { complex } else { 0 })?;
        Ok(Self {
            dirty,
            psf,
            sensitivity,
            scalars: end,
            epoch_scalars,
        })
    }

    /// Whether the sensitivity is a stored image rather than each plane's
    /// `sumwt`.
    fn dense_sensitivity(&self) -> bool {
        !self.sensitivity.is_empty()
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
    Resident(&'a NormalPrimitives),
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

    /// Weighted or unmapped support for this polarization plane.
    #[must_use]
    pub fn validity(&self) -> SpectralChannelValidity {
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.validity[self.plane],
            NormalPlaneBacking::Resident(d) => d.validity[self.plane],
        }
    }

    /// Load only the selected dirty or residual plane.
    pub fn read_residual(&self) -> Result<Cow<'a, [Complex64]>, NormalStateError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_complex(&d.fields.dirty, offset, self.cells),
            NormalPlaneBacking::Resident(d) => {
                let values = d
                    .dirty()
                    .complex()
                    .ok_or(NormalStateError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }

    pub(crate) fn read_residual_real(&self) -> Result<Option<Cow<'a, [f32]>>, NormalStateError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_real(&d.fields.dirty, offset, self.cells),
            NormalPlaneBacking::Resident(_) => Ok(None),
        }
    }

    /// Load only the selected unnormalized point-spread-function plane.
    pub fn read_psf(&self) -> Result<Cow<'a, [Complex64]>, NormalStateError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_complex(&d.fields.psf, offset, self.cells),
            NormalPlaneBacking::Resident(d) => {
                let values = d.psf().complex().ok_or(NormalStateError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }

    pub(crate) fn read_psf_real(&self) -> Result<Option<Cow<'a, [f32]>>, NormalStateError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_real(&d.fields.psf, offset, self.cells),
            NormalPlaneBacking::Resident(_) => Ok(None),
        }
    }

    /// Load only the selected unnormalized sensitivity plane.
    pub fn read_sensitivity(&self) -> Result<Cow<'a, [f64]>, NormalStateError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => Ok(Cow::Owned(if d.fields.dense_sensitivity() {
                d.read_widened(&d.fields.sensitivity, offset, self.cells)?
            } else {
                vec![d.sum_weights[self.plane]; self.cells]
            })),
            NormalPlaneBacking::Resident(d) => {
                let values = d
                    .sensitivity()
                    .dense()
                    .ok_or(NormalStateError::ProblemMismatch)?;
                Ok(Cow::Borrowed(&values[offset..offset + self.cells]))
            }
        }
    }
}

impl StoredChannelNormalDomain {
    pub(crate) fn refresh(&self, plan: &NormalStoragePlan) -> Result<Self, NormalStateError> {
        if !self.is_complete() {
            return Err(NormalStateError::IncompleteCoverage);
        }
        let fields = self.fields.clone();
        let storage = plan.factory.create(self.ordinal * 2, fields.dirty.len())?;
        if storage.len() != fields.dirty.len() {
            return Err(NormalStateError::NormalStorage(
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
            fields,
            storage,
            invariants: self.invariants.clone(),
            window_channels: plan.window_channels,
            next_channel: 0,
        })
    }

    /// Write the refreshed residual planes `[channel][pol]` (x-major) of the
    /// next channel range.
    pub(crate) fn append_residual_planes(
        &mut self,
        range: Range<usize>,
        shape: [usize; 2],
        residual: &[f32],
    ) -> Result<(), NormalStateError> {
        if range.start != self.next_channel
            || range.start >= range.end
            || range.end > self.total_channels
        {
            return Err(NormalStateError::IncompleteCoverage);
        }
        if shape != self.shape
            || residual.len() != range.len() * self.polarizations * checked_cells(self.shape)?
        {
            return Err(NormalStateError::ProblemMismatch);
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
    ) -> Result<(usize, usize), NormalStateError> {
        if !self.is_complete() {
            return Err(NormalStateError::IncompleteCoverage);
        }
        if channel >= self.total_channels || polarization >= self.polarizations {
            return Err(NormalStateError::InvalidSlab);
        }
        Ok((
            channel * self.polarizations + polarization,
            checked_cells(self.shape)?,
        ))
    }

    pub(crate) fn begin(
        first: SpectralDomainPrimitives,
        plan: &NormalStoragePlan,
    ) -> Result<Self, NormalStateError> {
        let p = first.primitives();
        let total_channels = p.slab.total_channels();
        if p.slab.core_range().start != 0 || p.polarizations == 0 {
            return Err(NormalStateError::InvalidSlab);
        }
        let planes = total_channels
            .checked_mul(p.polarizations)
            .ok_or(NormalStateError::ResidencyOverflow)?;
        let values = planes
            .checked_mul(checked_cells(p.shape)?)
            .ok_or(NormalStateError::ResidencyOverflow)?;
        // The first window decides: a gridded weight image makes the
        // sensitivity dense, its absence one `sumwt` per plane.
        let dense = p
            .cube_real
            .as_ref()
            .map_or(!p.sensitivity.is_empty(), |real| real.sensitivity.is_some());
        let fields = ChannelNormalFields::new(values, dense)?;
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
            return Err(NormalStateError::NormalStorage(
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
    ) -> Result<(), NormalStateError> {
        let p = domain.primitives();
        let compact = p.cube_real.as_ref();
        let range = p.slab.core_range();
        if range.start != self.next_channel {
            return Err(NormalStateError::IncompleteCoverage);
        }
        let dense = compact.map_or(!p.sensitivity.is_empty(), |real| real.sensitivity.is_some());
        if dense != self.fields.dense_sensitivity() {
            return Err(NormalStateError::ProblemMismatch);
        }
        if range.len() > self.window_channels {
            return Err(NormalStateError::NormalStorage(
                "normal-state write exceeds the admitted channel window".into(),
            ));
        }
        let cells = checked_cells(self.shape)?;
        let plane_offset = range.start * self.polarizations;
        let planes = range.len() * self.polarizations;
        let offset = plane_offset * cells;
        if let Some(real) = compact {
            self.storage
                .write_real(self.fields.dirty.start + 2 * offset, &real.dirty)?;
            Arc::get_mut(&mut self.invariants)
                .ok_or(NormalStateError::IncompleteCoverage)?
                .write_real(
                    self.fields.psf.start - self.fields.epoch_scalars + 2 * offset,
                    &real.psf,
                )?;
        }
        for (field, source) in [
            (&self.fields.dirty, p.dirty.as_ref()),
            (&self.fields.psf, p.psf.as_ref()),
        ] {
            if !source.is_empty() {
                let scalars = complex_scalars(source)?;
                let start = field.start + 2 * offset;
                if field.start >= self.fields.epoch_scalars {
                    Arc::get_mut(&mut self.invariants)
                        .ok_or(NormalStateError::IncompleteCoverage)?
                        .write(start - self.fields.epoch_scalars, scalars)?;
                } else {
                    self.storage.write(start, scalars)?;
                }
            }
        }
        if dense {
            let invariant =
                Arc::get_mut(&mut self.invariants).ok_or(NormalStateError::IncompleteCoverage)?;
            let start = self.fields.sensitivity.start - self.fields.epoch_scalars + 2 * offset;
            match compact.and_then(|real| real.sensitivity.as_deref()) {
                Some(weight) => invariant.write_real(start, weight)?,
                None => {
                    // Complete-data windows carry the widened f64 response;
                    // write it as (value, 0) pairs on the real field's span.
                    let mut pairs = [0.0_f64; 512];
                    for (index, chunk) in p.sensitivity.chunks(pairs.len() / 2).enumerate() {
                        for (pair, &value) in pairs.as_chunks_mut::<2>().0.iter_mut().zip(chunk) {
                            *pair = [value, 0.0];
                        }
                        invariant.write(start + index * pairs.len(), &pairs[..chunk.len() * 2])?;
                    }
                }
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

    /// A real field's values widened to f64: the paged backing's Float plane
    /// as it is, otherwise the real parts of the resident `(value, 0)` pairs.
    fn read_widened(
        &self,
        field: &Range<usize>,
        offset: usize,
        values: usize,
    ) -> Result<Vec<f64>, NormalStateError> {
        if let Some(real) = self.read_real(field, offset, values)? {
            return Ok(real.iter().map(|&value| f64::from(value)).collect());
        }
        Ok(self
            .read_complex(field, offset, values)?
            .iter()
            .map(|value| value.re)
            .collect())
    }

    fn read_complex(
        &self,
        field: &Range<usize>,
        offset: usize,
        values: usize,
    ) -> Result<Cow<'_, [Complex64]>, NormalStateError> {
        let start = field.start + offset * 2;
        let result = if start >= self.fields.epoch_scalars {
            self.invariants
                .read_complex(start - self.fields.epoch_scalars, values)
        } else {
            self.storage.read_complex(start, values)
        }?;
        if result.len() != values {
            return Err(NormalStateError::NormalStorage(
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
    ) -> Result<Option<Cow<'_, [f32]>>, NormalStateError> {
        let start = field.start + 2 * offset;
        let result = if field.start >= self.fields.epoch_scalars {
            self.invariants
                .read_real(start - self.fields.epoch_scalars, values)
        } else {
            self.storage.read_real(start, values)
        }?;
        if result.as_ref().is_some_and(|window| window.len() != values) {
            return Err(NormalStateError::NormalStorage(
                "normal backing returned an incorrect real window length".into(),
            ));
        }
        Ok(result)
    }

    pub(crate) fn read_window(
        &self,
        range: Range<usize>,
    ) -> Result<SpectralDomainPrimitives, NormalStateError> {
        if !self.is_complete() {
            return Err(NormalStateError::IncompleteCoverage);
        }
        if range.start >= range.end || range.end > self.total_channels {
            return Err(NormalStateError::InvalidSlab);
        }
        if range.len() > self.window_channels {
            return Err(NormalStateError::NormalStorage(
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
            NormalPrimitives {
                shape: self.shape,
                slab: SpectralSlabPlan {
                    total_channels: self.total_channels,
                    core_start: range.start,
                    core_end: range.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations: self.polarizations,
                dirty: self
                    .read_complex(&self.fields.dirty, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                cube_real: None,
                psf: self
                    .read_complex(&self.fields.psf, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                sensitivity: if self.fields.dense_sensitivity() {
                    self.read_widened(&self.fields.sensitivity, offset, values)?
                        .into_boxed_slice()
                } else {
                    self.sum_weights[plane_range.clone()]
                        .iter()
                        .flat_map(|&weight| std::iter::repeat_n(weight, cells))
                        .collect()
                },
                sum_weights: self.sum_weights[plane_range.clone()].into(),
                published_sum_weights: self.published_sum_weights[plane_range.clone()].into(),
                validity: self.validity[plane_range].into(),
            },
        ))
    }
}
