// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact channel-local Normal State backing, independent of physical storage.

use std::{fmt, ops::Range, sync::Arc};

use casa_imaging_model::{ImageDomainRole, LogicalIdentity};
use num_complex::Complex64;

#[cfg(test)]
use super::SpectralOperatorMeasurements;
use super::{
    CompleteDataOwnerCompletion, CompleteDataOwnerResult, NORMAL_STATE_CONTENT_DOMAIN,
    SpectralBasisPlan, SpectralChannelValidity, SpectralDomainPrimitives, SpectralOperatorError,
    SpectralOperatorPrimitives, SpectralOperatorSpecification, SpectralPrimitiveDomains,
    SpectralSlabPlan, checked_cells, same_complete_data_authority,
};
use crate::{ModelGenerationId, canonical_f64_bits};

/// Physical scalar-array capability used only by the Normal State owner.
///
/// Complex values are stored as consecutive real/imaginary f64 values. Access
/// must preserve every bit and must not enlarge an admitted cache. The owner
/// writes every logical value before sealing a generation; storage handles
/// must not permit mutation through aliases retained outside this capability.
#[doc(hidden)]
pub trait NormalArrayStorage: fmt::Debug + Send + Sync {
    /// Logical scalar capacity, excluding physical tile padding.
    fn len(&self) -> usize;
    /// Whether the logical array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Read into an already allocated, bounded scalar window.
    fn read(&self, start: usize, values: &mut [f64]) -> Result<(), SpectralOperatorError>;
    /// Replace a bounded scalar window without resizing the array.
    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const CHANNELS: usize = 5;
    const CELLS: usize = 6;
    const POLARIZATIONS: usize = 2;

    fn model() -> ModelGenerationId {
        ModelGenerationId(LogicalIdentity::from_sha256([19; 32]))
    }

    fn domain(range: Range<usize>, published_differ: bool) -> SpectralDomainPrimitives {
        let planes = range.start * POLARIZATIONS..range.end * POLARIZATIONS;
        let values = planes.start * CELLS..planes.end * CELLS;
        let complex = |bias: f64| -> Box<[Complex64]> {
            values
                .clone()
                .map(|i| {
                    Complex64::new(
                        i as f64 + bias,
                        if i == 0 { -0.0 } else { -(i as f64) / 8.0 },
                    )
                })
                .collect()
        };
        SpectralDomainPrimitives::new(
            0,
            ImageDomainRole::Main,
            SpectralOperatorPrimitives {
                shape: [3, 2],
                slab: SpectralSlabPlan {
                    total_channels: CHANNELS,
                    core_start: range.start,
                    core_end: range.end,
                    resident_start: range.start,
                    resident_end: range.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations: POLARIZATIONS,
                joint_line_term_by_channel: vec![None; CHANNELS].into(),
                dirty: complex(0.25),
                invariant_dirty: Some(complex(0.5)),
                common_residual: None,
                invariant_common_dirty: None,
                psf: complex(-0.125),
                sensitivity: values.clone().map(|i| i as f64 * 0.25).collect(),
                primary_beam_weighted_sum: None,
                sum_weights: planes.clone().map(|i| (i + 1) as f64).collect(),
                published_sum_weights: planes
                    .clone()
                    .map(|i| (i + 1) as f64 + if published_differ { 0.5 } else { 0.0 })
                    .collect(),
                channel_sum_weights: Box::new([]),
                validity: planes
                    .map(|i| match i % 3 {
                        0 => SpectralChannelValidity::Valid,
                        1 => SpectralChannelValidity::Blank,
                        _ => SpectralChannelValidity::Unmapped,
                    })
                    .collect(),
                major_cycle_residual: Some(complex(0.75)),
                major_cycle_residual_promoted: false,
                residual_model: Some(model()),
                measurements: SpectralOperatorMeasurements::default(),
            },
        )
    }

    #[derive(Debug)]
    struct ObservedFactory {
        maximum_access: Arc<AtomicUsize>,
        allowed: usize,
    }

    #[derive(Debug)]
    struct ObservedStorage {
        values: Box<[f64]>,
        maximum_access: Arc<AtomicUsize>,
        allowed: usize,
    }

    impl NormalStorageFactory for ObservedFactory {
        fn create(
            &self,
            _domain: usize,
            scalars: usize,
        ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
            Ok(Box::new(ObservedStorage {
                values: vec![0.0; scalars].into(),
                maximum_access: self.maximum_access.clone(),
                allowed: self.allowed,
            }))
        }
    }

    impl NormalArrayStorage for ObservedStorage {
        fn len(&self) -> usize {
            self.values.len()
        }
        fn read(&self, start: usize, values: &mut [f64]) -> Result<(), SpectralOperatorError> {
            assert!(values.len() <= self.allowed);
            self.maximum_access
                .fetch_max(values.len(), Ordering::Relaxed);
            self.values.read(start, values)
        }
        fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
            assert!(values.len() <= self.allowed);
            self.maximum_access
                .fetch_max(values.len(), Ordering::Relaxed);
            self.values.write(start, values)
        }
    }

    #[test]
    fn t55_normal_storage_preserves_field_order_bits_and_promoted_identity_across_windows() {
        for published_differ in [false, true] {
            let expected = domain(0..CHANNELS, published_differ).primitives;
            let expected_identity = expected.normal_state_content_identity();
            let expected_final = expected.promote_major_cycle_residual(model()).unwrap();
            for width in [1, 2, 3, CHANNELS] {
                let maximum_access = Arc::new(AtomicUsize::new(0));
                let allowed = 2 * CELLS * POLARIZATIONS * width;
                let plan = NormalStoragePlan::new(
                    Arc::new(ObservedFactory {
                        maximum_access: maximum_access.clone(),
                        allowed,
                    }),
                    width,
                )
                .unwrap();
                let mut stored =
                    StoredChannelNormalDomain::begin(domain(0..width, published_differ), &plan)
                        .unwrap();
                for start in (width..CHANNELS).step_by(width) {
                    assert_eq!(
                        stored.content_identity(),
                        Err(SpectralOperatorError::IncompleteCoverage)
                    );
                    assert_eq!(
                        stored.read_window(0..1).unwrap_err(),
                        SpectralOperatorError::IncompleteCoverage
                    );
                    stored
                        .append(domain(
                            start..(start + width).min(CHANNELS),
                            published_differ,
                        ))
                        .unwrap();
                }
                assert_eq!(stored.content_identity().unwrap(), expected_identity);
                stored.promote_major_cycle_residual(model()).unwrap();
                assert_eq!(
                    stored.content_identity().unwrap(),
                    expected_final.normal_state_content_identity()
                );
                for start in (0..CHANNELS).step_by(width) {
                    let end = (start + width).min(CHANNELS);
                    let window = stored.read_window(start..end).unwrap().primitives;
                    let expected = domain(start..end, published_differ)
                        .primitives
                        .promote_major_cycle_residual(model())
                        .unwrap();
                    assert_eq!(
                        window.normal_state_content_identity(),
                        expected.normal_state_content_identity()
                    );
                    assert_eq!(window.invariant_dirty, expected.invariant_dirty);
                    assert_eq!(window.dirty, expected.dirty);
                    assert_eq!(window.dirty[0].im.to_bits(), expected.dirty[0].im.to_bits());
                }
                assert_eq!(maximum_access.load(Ordering::Relaxed), allowed);
                assert!(stored.read_window(CHANNELS..CHANNELS + 1).is_err());
                if width < CHANNELS {
                    assert!(stored.read_window(0..CHANNELS).is_err());
                }
                assert_eq!(
                    stored.append(domain(0..1, published_differ)),
                    Err(SpectralOperatorError::IncompleteCoverage)
                );
            }
        }
    }
}

/// Runtime allocation capability for an exact logical Normal State array.
#[doc(hidden)]
pub trait NormalStorageFactory: fmt::Debug + Send + Sync {
    /// Allocate storage whose complete logical contents will be owner-written.
    fn create(
        &self,
        domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError>;
}

/// Owner-derived upper bounds for one exact channel-local domain backing.
/// The bound includes both invariant dirty and an unpromoted major residual;
/// those fields may be absent, but cannot require a larger physical array.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelNormalStorageRequirement {
    scalar_capacity: usize,
    complex_plane_scalars: usize,
    maximum_window_scalars: usize,
    retained_metadata_bytes: usize,
}

impl ChannelNormalStorageRequirement {
    /// Derive one requirement per domain in the specification's canonical order.
    /// Coupled coefficient families use their separately admitted representation.
    pub fn for_specification(
        specification: &SpectralOperatorSpecification,
        window_channels: usize,
    ) -> Result<Box<[Self]>, SpectralOperatorError> {
        let channels = specification.slab.total_channels();
        if specification.basis != SpectralBasisPlan::ChannelLocal
            || window_channels == 0
            || window_channels > channels
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let polarizations = specification.polarization_count();
        specification
            .domains()
            .iter()
            .map(|domain| {
                let plane_values = checked_cells(domain.image_shape())?
                    .checked_mul(polarizations)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let complex_plane_scalars = plane_values
                    .checked_mul(2)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let values = plane_values
                    .checked_mul(channels)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let fields = ChannelNormalFields::new(values, true, true)?;
                let metadata_values = channels
                    .checked_mul(polarizations)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let role_bytes = match &domain.role {
                    ImageDomainRole::Main => 0,
                    ImageDomainRole::Outlier(name) => name.capacity(),
                };
                let retained_metadata_bytes = metadata_values
                    .checked_mul(2 * size_of::<f64>() + size_of::<SpectralChannelValidity>())
                    .and_then(|bytes| bytes.checked_add(size_of::<StoredChannelNormalDomain>()))
                    .and_then(|bytes| bytes.checked_add(role_bytes))
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                Ok(Self {
                    scalar_capacity: fields.scalars,
                    complex_plane_scalars,
                    maximum_window_scalars: complex_plane_scalars
                        .checked_mul(window_channels)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?,
                    retained_metadata_bytes,
                })
            })
            .collect()
    }

    /// Maximum f64 scalar count in the field-major physical array.
    #[must_use]
    pub const fn scalar_capacity(self) -> usize {
        self.scalar_capacity
    }

    /// Scalars in one complex image channel, including every polarization.
    #[must_use]
    pub const fn complex_plane_scalars(self) -> usize {
        self.complex_plane_scalars
    }

    /// Largest single read/write request issued to the physical capability.
    #[must_use]
    pub const fn maximum_window_scalars(self) -> usize {
        self.maximum_window_scalars
    }

    /// Domain descriptor, role, and complete-axis weight/validity allocations.
    #[must_use]
    pub const fn retained_metadata_bytes(self) -> usize {
        self.retained_metadata_bytes
    }
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

    /// Canonical complete-state identity, independent of the storage window.
    pub fn content_identity(&self) -> Result<LogicalIdentity, SpectralOperatorError> {
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

impl CompleteDataOwnerResult {
    /// Seal a complete-axis result into its admitted Normal State backing.
    /// A partial channel prefix must instead pass through the ordered slab fold.
    pub fn seal(
        self,
        plan: &NormalStoragePlan,
    ) -> Result<CompleteDataNormalState, SpectralOperatorError> {
        let slab = self.primitives().slab();
        if slab.core_range() != (0..slab.total_channels()) {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let primitives = if self.domains.primary().basis == SpectralBasisPlan::ChannelLocal {
            let fold = StoredChannelNormalFold::begin(self, plan)?;
            return fold.finish();
        } else {
            NormalStatePrimitives::Coupled(self.domains)
        };
        Ok(CompleteDataNormalState {
            primitives,
            completion: self.completion,
        })
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
    pub(crate) joint_continuum_terms: Option<usize>,
    pub(crate) sum_weights: &'a [f64],
    pub(crate) published_sum_weights: &'a [f64],
    pub(crate) channel_sum_weights: &'a [f64],
    pub(crate) validity: &'a [SpectralChannelValidity],
}

impl NormalStatePrimitives {
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
                    joint_continuum_terms: None,
                    sum_weights: &d.sum_weights,
                    published_sum_weights: &d.published_sum_weights,
                    channel_sum_weights: &[],
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
                    joint_continuum_terms: p.joint_continuum_term_count(),
                    sum_weights: p.sum_weights(),
                    published_sum_weights: p.published_sum_weights(),
                    channel_sum_weights: p.channel_sum_weights(),
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

    pub(crate) fn into_window(
        self,
        channels: Range<usize>,
    ) -> Result<SpectralPrimitiveDomains, SpectralOperatorError> {
        match self {
            Self::ChannelLocal(domains) => SpectralPrimitiveDomains::new(
                domains
                    .iter()
                    .map(|d| d.read_window(channels.clone()))
                    .collect::<Result<Box<[_]>, _>>()?,
            ),
            Self::Coupled(domains) => {
                if channels != domains.slab().core_range() {
                    return Err(SpectralOperatorError::InvalidSlab);
                }
                Ok(domains)
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
pub(crate) struct StoredChannelNormalFold {
    domains: Box<[StoredChannelNormalDomain]>,
    completion: CompleteDataOwnerCompletion,
    next_channel: usize,
}

impl StoredChannelNormalFold {
    pub(crate) fn begin(
        first: CompleteDataOwnerResult,
        plan: &NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let next_channel = first.primitives().slab().core_range().end;
        let domains = first
            .domains
            .into_iter()
            .map(|domain| StoredChannelNormalDomain::begin(domain, plan))
            .collect::<Result<Box<[_]>, _>>()?;
        Ok(Self {
            domains,
            completion: first.completion,
            next_channel,
        })
    }

    pub(crate) fn extend(
        mut self,
        next: CompleteDataOwnerResult,
    ) -> Result<Self, SpectralOperatorError> {
        let range = next.primitives().slab().core_range();
        if range.start != self.next_channel
            || !same_complete_data_authority(&self.completion, &next.completion)
            || self.domains.len() != next.domains.len()
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        for (domain, next) in self.domains.iter_mut().zip(next.domains.into_iter()) {
            domain.append(next)?;
        }
        self.next_channel = range.end;
        Ok(self)
    }

    pub(crate) fn finish(self) -> Result<CompleteDataNormalState, SpectralOperatorError> {
        if self.domains.iter().any(|domain| !domain.is_complete()) {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        Ok(CompleteDataNormalState {
            primitives: NormalStatePrimitives::ChannelLocal(self.domains),
            completion: self.completion,
        })
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

    fn read(&self, start: usize, values: &mut [f64]) -> Result<(), SpectralOperatorError> {
        let end = start
            .checked_add(values.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        values.copy_from_slice(
            self.get(start..end)
                .ok_or(SpectralOperatorError::InvalidSlab)?,
        );
        Ok(())
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

/// Offsets in one domain's field-major scalar array. Weight and validity
/// vectors are channel metadata, retained separately from image-sized fields.
#[derive(Debug, Clone)]
struct ChannelNormalFields {
    dirty: Range<usize>,
    invariant_dirty: Option<Range<usize>>,
    psf: Range<usize>,
    sensitivity: Range<usize>,
    major_cycle_residual: Option<Range<usize>>,
    scalars: usize,
}

impl ChannelNormalFields {
    fn new(values: usize, invariant: bool, residual: bool) -> Result<Self, SpectralOperatorError> {
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
        let invariant_dirty = invariant.then(|| field(complex)).transpose()?;
        let psf = field(complex)?;
        let sensitivity = field(values)?;
        let major_cycle_residual = residual.then(|| field(complex)).transpose()?;
        Ok(Self {
            dirty,
            invariant_dirty,
            psf,
            sensitivity,
            major_cycle_residual,
            scalars: end,
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
    window_channels: usize,
    next_channel: usize,
}

impl StoredChannelNormalDomain {
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
            p.invariant_dirty.is_some(),
            p.major_cycle_residual.is_some(),
        )?;
        let storage = plan.factory.create(first.domain_ordinal, fields.scalars)?;
        if storage.len() != fields.scalars {
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
            || p.invariant_dirty.is_some() != self.fields.invariant_dirty.is_some()
            || p.major_cycle_residual.is_some() != self.fields.major_cycle_residual.is_some()
            || p.common_residual.is_some()
            || p.invariant_common_dirty.is_some()
            || p.primary_beam_weighted_sum.is_some()
            || !p.channel_sum_weights.is_empty()
            || p.joint_line_term_by_channel.len() != self.total_channels
            || p.joint_line_term_by_channel.iter().any(Option::is_some)
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let cells = checked_cells(self.shape)?;
        let plane_offset = range.start * self.polarizations;
        let planes = range.len() * self.polarizations;
        let values = planes * cells;
        if p.dirty.len() != values
            || p.psf.len() != values
            || p.sensitivity.len() != values
            || p.invariant_dirty
                .as_ref()
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
            if let (Some(field), Some(source)) = (field, source) {
                let scalars: Vec<_> = source.iter().flat_map(|v| [v.re, v.im]).collect();
                self.storage.write(field.start + 2 * offset, &scalars)?;
            }
        }
        self.storage
            .write(self.fields.sensitivity.start + offset, &p.sensitivity)?;
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

    fn read_scalars(&self, range: Range<usize>) -> Result<Box<[f64]>, SpectralOperatorError> {
        let mut result = vec![0.0; range.len()].into_boxed_slice();
        self.storage.read(range.start, &mut result)?;
        Ok(result)
    }

    fn read_complex(
        &self,
        field: &Range<usize>,
        offset: usize,
        values: usize,
    ) -> Result<Box<[Complex64]>, SpectralOperatorError> {
        let start = field.start + offset * 2;
        let scalars = self.read_scalars(start..start + values * 2)?;
        Ok(scalars
            .chunks_exact(2)
            .map(|v| Complex64::new(v[0], v[1]))
            .collect())
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
                slab: SpectralSlabPlan {
                    total_channels: self.total_channels,
                    core_start: range.start,
                    core_end: range.end,
                    resident_start: range.start,
                    resident_end: range.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations: self.polarizations,
                joint_line_term_by_channel: vec![None; self.total_channels].into_boxed_slice(),
                dirty: self.read_complex(&self.fields.dirty, offset, values)?,
                invariant_dirty: self
                    .fields
                    .invariant_dirty
                    .as_ref()
                    .map(|f| self.read_complex(f, offset, values))
                    .transpose()?,
                common_residual: None,
                invariant_common_dirty: None,
                psf: self.read_complex(&self.fields.psf, offset, values)?,
                sensitivity: self.read_scalars(
                    self.fields.sensitivity.start + offset
                        ..self.fields.sensitivity.start + offset + values,
                )?,
                primary_beam_weighted_sum: None,
                sum_weights: self.sum_weights[plane_range.clone()].into(),
                published_sum_weights: self.published_sum_weights[plane_range.clone()].into(),
                channel_sum_weights: Box::new([]),
                validity: self.validity[plane_range].into(),
                major_cycle_residual: self
                    .fields
                    .major_cycle_residual
                    .as_ref()
                    .map(|f| self.read_complex(f, offset, values))
                    .transpose()?,
                major_cycle_residual_promoted: self.major_cycle_residual_promoted,
                residual_model: self.residual_model,
                #[cfg(test)]
                measurements: SpectralOperatorMeasurements::default(),
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
        for (field, complex) in [
            (&self.fields.dirty, true),
            (&self.fields.psf, true),
            (&self.fields.sensitivity, false),
        ] {
            let width = if complex {
                window_values
                    .checked_mul(2)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?
            } else {
                window_values
            };
            for start in (field.start..field.end).step_by(width) {
                for value in self.read_scalars(start..start.saturating_add(width).min(field.end))? {
                    encoder.u64(if complex {
                        value.to_bits()
                    } else {
                        canonical_f64_bits(value)
                    });
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
