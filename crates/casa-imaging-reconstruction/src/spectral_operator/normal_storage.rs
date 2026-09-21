// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact channel-local Normal State backing, independent of physical storage.

use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

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
/// must preserve every bit and must not enlarge an admitted cache. The owner
/// writes every logical value before sealing a generation; storage handles
/// must not permit mutation through aliases retained outside this capability.
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

    #[test]
    fn complex_scalar_windows_preserve_bits_and_allocation_identity() {
        let bits = [0, (-0.0_f64).to_bits(), 0x7ff8_0000_0000_0042, 1];
        let values: Vec<_> = bits.into_iter().map(f64::from_bits).collect();
        let borrowed = scalar_complex(Cow::Borrowed(&values)).unwrap();
        assert!(matches!(borrowed, Cow::Borrowed(_)));
        assert_eq!(borrowed.as_ptr().cast::<f64>(), values.as_ptr());
        assert_eq!(
            complex_scalars(&borrowed).unwrap().as_ptr(),
            values.as_ptr()
        );
        assert_eq!(
            complex_scalars(&borrowed)
                .unwrap()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            bits
        );
        drop(borrowed);
        let pointer = values.as_ptr();
        let capacity = values.capacity();
        let owned = scalar_complex(Cow::Owned(values)).unwrap();
        assert_eq!(owned.as_ptr().cast::<f64>(), pointer);
        let Cow::Owned(owned) = owned else {
            panic!("paged allocation must stay owned")
        };
        assert_eq!(owned.capacity() * 2, capacity);
        assert_eq!(
            complex_scalars(&owned)
                .unwrap()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            bits
        );
        assert!(scalar_complex(Cow::Borrowed(&[1.0])).is_err());
        let mut odd_capacity = Vec::with_capacity(3);
        odd_capacity.extend_from_slice(&[1.0, 2.0]);
        assert!(scalar_complex(Cow::Owned(odd_capacity)).is_err());
        assert!(scalar_complex(Cow::Owned(Vec::new())).unwrap().is_empty());
    }

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
        fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
            assert!(len <= self.allowed);
            self.maximum_access.fetch_max(len, Ordering::Relaxed);
            self.values.read(start, len)
        }
        fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
            assert!(values.len() <= self.allowed);
            self.maximum_access
                .fetch_max(values.len(), Ordering::Relaxed);
            self.values.write(start, values)
        }
    }

    #[derive(Debug)]
    struct ReadObservedStorage {
        storage: Box<dyn NormalArrayStorage>,
        reads: Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
    }

    impl NormalArrayStorage for ReadObservedStorage {
        fn len(&self) -> usize {
            self.storage.len()
        }
        fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
            self.reads.lock().unwrap().push((start, len));
            self.storage.read(start, len)
        }
        fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
            self.storage.write(start, values)
        }
    }

    #[test]
    fn selective_normal_plane_reads_only_requested_domain_channel_polarization_and_field() {
        for promoted in [false, true] {
            let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
            let reads = [
                Arc::new(std::sync::Mutex::new(Vec::new())),
                Arc::new(std::sync::Mutex::new(Vec::new())),
            ];
            let mut domains = Vec::new();
            for (ordinal, log) in reads.iter().enumerate() {
                let mut input = domain(0..CHANNELS, true);
                input.domain_ordinal = ordinal;
                if ordinal == 1 {
                    input.domain_role = ImageDomainRole::Outlier("second".into());
                    input.primitives.shape = [2, 3];
                    for value in &mut input.primitives.psf {
                        value.re += 100.0;
                    }
                }
                let mut stored = StoredChannelNormalDomain::begin(input, &plan).unwrap();
                if promoted {
                    stored.promote_major_cycle_residual(model()).unwrap();
                }
                stored.storage = Box::new(ReadObservedStorage {
                    storage: stored.storage,
                    reads: log.clone(),
                });
                stored.invariants = Arc::new(Box::new(ReadObservedStorage {
                    storage: Arc::try_unwrap(stored.invariants).unwrap(),
                    reads: log.clone(),
                }));
                domains.push(stored);
            }
            let fields = domains[1].fields.clone();
            let primitives = NormalStatePrimitives::ChannelLocal(domains.into_boxed_slice());
            let selected = primitives.read_plane(1, 3, 1).unwrap();
            assert_eq!(selected.shape(), [2, 3]);
            assert_eq!(selected.output_channel(), 3);
            assert_eq!(selected.sum_weight(), 8.0);
            assert_eq!(selected.published_sum_weight(), 8.5);
            assert_eq!(selected.validity(), SpectralChannelValidity::Blank);
            for invalid in [
                (2, 3, 1),
                (1, CHANNELS, 1),
                (1, 3, POLARIZATIONS),
                (1, usize::MAX, 0),
            ] {
                assert!(matches!(
                    primitives.read_plane(invalid.0, invalid.1, invalid.2),
                    Err(SpectralOperatorError::InvalidSlab)
                ));
            }
            assert!(reads.iter().all(|log| log.lock().unwrap().is_empty()));

            let offset = (3 * POLARIZATIONS + 1) * CELLS;
            let psf = selected.read_psf().unwrap();
            assert!(matches!(psf, Cow::Borrowed(_)));
            assert_eq!(
                *reads[1].lock().unwrap(),
                vec![(
                    fields.psf.start - fields.epoch_scalars + 2 * offset,
                    2 * CELLS
                )]
            );
            assert_eq!(psf[0].re, offset as f64 - 0.125 + 100.0);
            assert!(reads[0].lock().unwrap().is_empty());
            reads[1].lock().unwrap().clear();

            let residual = selected.read_residual().unwrap();
            assert!(matches!(residual, Cow::Borrowed(_)));
            assert_eq!(
                *reads[1].lock().unwrap(),
                vec![(fields.dirty.start + 2 * offset, 2 * CELLS)]
            );
            assert_eq!(
                residual[0].re,
                offset as f64 + if promoted { 0.75 } else { 0.25 }
            );
            reads[1].lock().unwrap().clear();
            let sensitivity = selected.read_sensitivity().unwrap();
            assert!(matches!(sensitivity, Cow::Borrowed(_)));
            assert_eq!(
                *reads[1].lock().unwrap(),
                vec![(
                    fields.sensitivity.start - fields.epoch_scalars + offset,
                    CELLS
                )]
            );
            assert!(reads[0].lock().unwrap().is_empty());

            let full = primitives.read_window(3..4).unwrap();
            let expected = full.get(1).unwrap().primitives();
            assert_eq!(psf.as_ref(), &expected.psf()[CELLS..2 * CELLS]);
            assert_eq!(residual.as_ref(), &expected.dirty()[CELLS..2 * CELLS]);
            assert_eq!(
                sensitivity.as_ref(),
                &expected.sensitivity()[CELLS..2 * CELLS]
            );
        }
    }

    #[test]
    fn selective_normal_plane_requires_complete_admitted_backing() {
        let plan = NormalStoragePlan::resident(1).unwrap();
        let incomplete = NormalStatePrimitives::ChannelLocal(
            vec![StoredChannelNormalDomain::begin(domain(0..1, false), &plan).unwrap()]
                .into_boxed_slice(),
        );
        assert!(matches!(
            incomplete.read_plane(0, 0, 0),
            Err(SpectralOperatorError::IncompleteCoverage)
        ));

        let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
        let mut stored =
            StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
        // A one-channel allowance suffices even with several polarizations.
        stored.window_channels = 1;
        let primitives = NormalStatePrimitives::ChannelLocal(vec![stored].into_boxed_slice());
        assert_eq!(
            primitives
                .read_plane(0, 4, 1)
                .unwrap()
                .read_psf()
                .unwrap()
                .len(),
            CELLS
        );
        assert!(primitives.read_window(0..CHANNELS).is_err());
    }

    #[test]
    fn selective_normal_plane_borrows_constant_polynomial_planes_exactly() {
        for promoted in [false, true] {
            let domains = (0..2)
                .map(|ordinal| {
                    let mut input = domain(0..1, true);
                    input.domain_ordinal = ordinal;
                    input.primitives.slab.total_channels = 1;
                    input.primitives.basis = SpectralBasisPlan::Polynomial(
                        super::super::BlockNormalPlan::constant(1.0e9).unwrap(),
                    );
                    input.primitives.joint_line_term_by_channel = vec![None].into();
                    if ordinal == 1 {
                        input.domain_role = ImageDomainRole::Outlier("constant".into());
                        input.primitives.shape = [2, 3];
                        for value in &mut input.primitives.psf {
                            value.re += 100.0;
                        }
                    }
                    if promoted {
                        input.primitives = input
                            .primitives
                            .promote_major_cycle_residual(model())
                            .unwrap();
                    }
                    input
                })
                .collect();
            let primitives =
                NormalStatePrimitives::Coupled(SpectralPrimitiveDomains::new(domains).unwrap());
            let selected = primitives.read_plane(1, 0, 1).unwrap();
            assert_eq!(selected.shape(), [2, 3]);
            assert_eq!(selected.output_channel(), 0);
            assert_eq!(selected.sum_weight(), 2.0);
            assert_eq!(selected.published_sum_weight(), 2.5);
            assert_eq!(selected.validity(), SpectralChannelValidity::Blank);
            let window = primitives.read_window(0..1).unwrap();
            let expected = window.get(1).unwrap().primitives();
            let residual = selected.read_residual().unwrap();
            let psf = selected.read_psf().unwrap();
            let sensitivity = selected.read_sensitivity().unwrap();
            assert!(matches!(residual, Cow::Borrowed(_)));
            assert!(matches!(psf, Cow::Borrowed(_)));
            assert!(matches!(sensitivity, Cow::Borrowed(_)));
            assert!(std::ptr::eq(
                residual.as_ptr(),
                expected.dirty()[CELLS..].as_ptr()
            ));
            assert_eq!(residual.as_ref(), &expected.dirty()[CELLS..]);
            assert_eq!(psf.as_ref(), &expected.psf()[CELLS..]);
            assert_eq!(sensitivity.as_ref(), &expected.sensitivity()[CELLS..]);
            assert_eq!(
                residual[0].re,
                CELLS as f64 + if promoted { 0.75 } else { 0.25 }
            );
            for invalid in [
                (2, 0, 1),
                (1, 1, 1),
                (1, 0, POLARIZATIONS),
                (1, usize::MAX, 0),
            ] {
                assert!(matches!(
                    primitives.read_plane(invalid.0, invalid.1, invalid.2),
                    Err(SpectralOperatorError::InvalidSlab)
                ));
            }
        }
    }

    #[test]
    fn selective_normal_plane_rejects_nonconstant_families_and_propagates_storage_errors() {
        let mut input = domain(0..1, false);
        input.primitives.basis =
            SpectralBasisPlan::Polynomial(super::super::BlockNormalPlan::taylor(1.0e9, 2).unwrap());
        let coupled = NormalStatePrimitives::Coupled(
            SpectralPrimitiveDomains::new(vec![input].into()).unwrap(),
        );
        assert!(matches!(
            coupled.read_plane(0, 0, 0),
            Err(SpectralOperatorError::NormalStorage(_))
        ));

        #[derive(Debug)]
        struct FailedRead;
        impl NormalArrayStorage for FailedRead {
            fn len(&self) -> usize {
                0
            }
            fn read(&self, _: usize, _: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
                Err(SpectralOperatorError::NormalStorage(
                    "selected read failed".into(),
                ))
            }
            fn write(&mut self, _: usize, _: &[f64]) -> Result<(), SpectralOperatorError> {
                unreachable!()
            }
        }
        let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
        let mut domain =
            StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
        domain.storage = Box::new(FailedRead);
        domain.invariants = Arc::new(Box::new(FailedRead));
        let primitives = NormalStatePrimitives::ChannelLocal(vec![domain].into());
        let plane = primitives.read_plane(0, 2, 1).unwrap();
        let error = SpectralOperatorError::NormalStorage("selected read failed".into());
        assert_eq!(plane.read_psf(), Err(error.clone()));
        assert_eq!(plane.read_residual(), Err(error.clone()));
        assert_eq!(plane.read_sensitivity(), Err(error));
    }

    #[test]
    fn resident_normal_reads_borrow_exact_bit_windows() {
        let bits = [0, (-0.0_f64).to_bits(), 0x7ff8000000000042, 1];
        let mut storage: Box<[f64]> = bits.into_iter().map(f64::from_bits).collect();
        let window = storage.read(1, 3).unwrap();
        assert!(matches!(window, Cow::Borrowed(_)));
        assert_eq!(window.as_ptr(), storage[1..].as_ptr());
        assert_eq!(
            window
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            bits[1..]
        );
        drop(window);
        storage.write(1, &[2.0, 3.0, 4.0]).unwrap();
        assert!(storage.read(4, 0).unwrap().is_empty());
        assert_eq!(storage.read(5, 0), Err(SpectralOperatorError::InvalidSlab));
        assert_eq!(storage.read(3, 2), Err(SpectralOperatorError::InvalidSlab));
        assert_eq!(
            storage.read(usize::MAX, 1),
            Err(SpectralOperatorError::ResidencyOverflow)
        );
    }

    #[test]
    fn normal_owner_rejects_incorrect_owned_window_length() {
        #[derive(Debug)]
        struct ShortRead;
        impl NormalArrayStorage for ShortRead {
            fn len(&self) -> usize {
                3
            }
            fn read(&self, _: usize, _: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
                Ok(Cow::Owned(vec![1.0, 2.0]))
            }
            fn write(&mut self, _: usize, _: &[f64]) -> Result<(), SpectralOperatorError> {
                unreachable!("fixture is installed after generation")
            }
        }
        let plan = NormalStoragePlan::new(Arc::new(ResidentNormalStorage), CHANNELS).unwrap();
        let mut stored =
            StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
        stored.storage = Box::new(ShortRead);
        assert!(matches!(
            stored.read_scalars(0..3),
            Err(SpectralOperatorError::NormalStorage(_))
        ));
    }

    #[derive(Debug)]
    struct EpochObservedStorage {
        storage: Box<dyn NormalArrayStorage>,
        accesses: Arc<AtomicUsize>,
        _alive: Arc<()>,
    }

    impl NormalArrayStorage for EpochObservedStorage {
        fn len(&self) -> usize {
            self.storage.len()
        }

        fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
            self.accesses.fetch_add(1, Ordering::Relaxed);
            self.storage.read(start, len)
        }

        fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
            self.accesses.fetch_add(1, Ordering::Relaxed);
            self.storage.write(start, values)
        }
    }

    fn streaming_domain() -> SpectralDomainPrimitives {
        let mut input = domain(0..CHANNELS, true);
        input.primitives.invariant_dirty = None;
        input.primitives.major_cycle_residual = None;
        input.primitives.major_cycle_residual_promoted = true;
        input
    }

    #[test]
    fn residual_epochs_share_only_invariants_without_old_array_access_or_owner_chain() {
        let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
        let mut old = StoredChannelNormalDomain::begin(streaming_domain(), &plan).unwrap();
        assert_eq!(old.storage.len(), CHANNELS * POLARIZATIONS * CELLS * 2);
        assert_eq!(old.invariants.len(), CHANNELS * POLARIZATIONS * CELLS * 3);
        let expected = old.read_window(0..CHANNELS).unwrap();
        let accesses = Arc::new(AtomicUsize::new(0));
        let epoch_alive = Arc::new(());
        old.storage = Box::new(EpochObservedStorage {
            storage: old.storage,
            accesses: accesses.clone(),
            _alive: epoch_alive.clone(),
        });
        old.invariants = Arc::new(Box::new(EpochObservedStorage {
            storage: Arc::try_unwrap(old.invariants).unwrap(),
            accesses: accesses.clone(),
            _alive: Arc::new(()),
        }));
        let invariants = Arc::downgrade(&old.invariants);
        for epoch in 1..=3 {
            let next_model = ModelGenerationId(LogicalIdentity::from_sha256([epoch; 32]));
            let mut next = old.refresh(next_model, &plan).unwrap();
            let values: Box<[_]> = (0..CHANNELS * POLARIZATIONS * CELLS)
                .map(|index| Complex64::new(index as f64 + f64::from(epoch), -0.0))
                .collect();
            next.append_residual(&crate::streaming_cube::band::CubeResidual {
                shape: [3, 2],
                core: 0..CHANNELS,
                total_channels: CHANNELS,
                model: next_model,
                values: values.clone(),
            })
            .unwrap();
            assert!(next.is_complete());
            assert_eq!(accesses.load(Ordering::Relaxed), 0);
            assert!(Arc::ptr_eq(&old.invariants, &next.invariants));
            drop(old);
            assert_eq!(Arc::strong_count(&epoch_alive), 1);
            assert_eq!(invariants.strong_count(), 1);
            let observed = next.read_window(0..CHANNELS).unwrap();
            let p = observed.primitives();
            assert_eq!(p.psf, expected.primitives().psf);
            assert_eq!(p.sensitivity, expected.primitives().sensitivity);
            assert_eq!(p.sum_weights, expected.primitives().sum_weights);
            assert_eq!(
                p.published_sum_weights,
                expected.primitives().published_sum_weights
            );
            assert_eq!(p.validity, expected.primitives().validity);
            for (actual, expected) in p.dirty.iter().zip(values.iter()) {
                assert_eq!(actual.re.to_bits(), expected.re.to_bits());
                assert_eq!(actual.im.to_bits(), expected.im.to_bits());
            }
            accesses.store(0, Ordering::Relaxed);
            old = next;
        }
        drop(old);
        assert!(invariants.upgrade().is_none());
    }

    #[test]
    fn failed_residual_refresh_leaves_previous_epoch_readable() {
        #[derive(Debug)]
        struct WriteFailure;
        impl NormalArrayStorage for WriteFailure {
            fn len(&self) -> usize {
                CHANNELS * POLARIZATIONS * CELLS * 2
            }
            fn read(&self, _: usize, _: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
                unreachable!("failed candidate must not be published")
            }
            fn write(&mut self, _: usize, _: &[f64]) -> Result<(), SpectralOperatorError> {
                Err(SpectralOperatorError::NormalStorage(
                    "injected write failure".into(),
                ))
            }
        }
        let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
        let old = StoredChannelNormalDomain::begin(streaming_domain(), &plan).unwrap();
        let expected = old.content_identity().unwrap();
        let mut next = old.refresh(model(), &plan).unwrap();
        let mut residual = crate::streaming_cube::band::CubeResidual {
            shape: [3, 2],
            core: 1..CHANNELS,
            total_channels: CHANNELS,
            model: model(),
            values: vec![Complex64::new(3.0, -0.0); (CHANNELS - 1) * POLARIZATIONS * CELLS].into(),
        };
        assert_eq!(
            next.append_residual(&residual),
            Err(SpectralOperatorError::IncompleteCoverage)
        );
        residual.core = 0..CHANNELS - 1;
        next.append_residual(&residual).unwrap();
        assert!(!next.is_complete());
        next.storage = Box::new(WriteFailure);
        residual.core = CHANNELS - 1..CHANNELS;
        residual.values = vec![Complex64::new(3.0, -0.0); POLARIZATIONS * CELLS].into();
        assert!(matches!(
            next.append_residual(&residual),
            Err(SpectralOperatorError::NormalStorage(_))
        ));
        assert!(!next.is_complete());
        drop(next);
        assert_eq!(old.content_identity().unwrap(), expected);
        assert!(old.read_window(0..CHANNELS).is_ok());
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
    /// Slots are `2 * domain` for epoch arrays and `2 * domain + 1` for invariants.
    fn create(
        &self,
        domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError>;
}

/// Owner-derived bounds for one physical allocation of a channel-local domain.
/// Epoch arrays and invariant arrays have independent ordered allocation slots.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelNormalStorageRequirement {
    allocation_ordinal: usize,
    scalar_capacity: usize,
    complex_plane_scalars: usize,
    maximum_window_scalars: usize,
    retained_metadata_bytes: usize,
}

impl ChannelNormalStorageRequirement {
    /// Derive epoch and invariant requirements for each domain in canonical order.
    /// Includes optional invariant dirty and unpromoted major residual capacity.
    /// Coupled coefficient families use their separately admitted representation.
    pub fn for_specification(
        specification: &SpectralOperatorSpecification,
        window_channels: usize,
    ) -> Result<Box<[Self]>, SpectralOperatorError> {
        Self::for_fields(specification, window_channels, true, true, false)
    }

    /// Native initial-empty imaging followed by residual refresh retains only
    /// the promoted residual, PSF and sensitivity. This bound is specific to
    /// that chain; nonempty full-normal construction uses `for_specification`.
    /// The storage factory rejects allocations exceeding the projected capacity.
    #[doc(hidden)]
    pub fn for_streaming_cube(
        specification: &SpectralOperatorSpecification,
        window_channels: usize,
    ) -> Result<Box<[Self]>, SpectralOperatorError> {
        Self::for_fields(specification, window_channels, false, false, false)
    }

    /// Allocate only the new complex residual; imported invariants retain their
    /// original physical owner and resource permit.
    pub fn for_streaming_cube_refresh(
        specification: &SpectralOperatorSpecification,
        window_channels: usize,
    ) -> Result<Box<[Self]>, SpectralOperatorError> {
        Self::for_fields(specification, window_channels, false, false, true)
    }

    fn for_fields(
        specification: &SpectralOperatorSpecification,
        window_channels: usize,
        invariant: bool,
        residual: bool,
        residual_only: bool,
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
            .enumerate()
            .map(|(ordinal, domain)| {
                let plane_values = checked_cells(domain.image_shape())?
                    .checked_mul(polarizations)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let complex_plane_scalars = plane_values
                    .checked_mul(2)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let values = plane_values
                    .checked_mul(channels)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                let fields = ChannelNormalFields::new(values, invariant, residual)?;
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
                let epoch = Self {
                    allocation_ordinal: ordinal * 2,
                    scalar_capacity: fields.epoch_scalars,
                    complex_plane_scalars,
                    maximum_window_scalars: complex_plane_scalars
                        .checked_mul(window_channels)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?,
                    retained_metadata_bytes,
                };
                let mut allocations = vec![epoch];
                if !residual_only {
                    allocations.push(Self {
                        allocation_ordinal: ordinal * 2 + 1,
                        scalar_capacity: fields.scalars - fields.epoch_scalars,
                        retained_metadata_bytes: size_of::<Box<dyn NormalArrayStorage>>()
                            + 2 * size_of::<usize>(),
                        ..epoch
                    });
                }
                Ok(allocations)
            })
            .collect::<Result<Vec<_>, SpectralOperatorError>>()
            .map(|domains| domains.into_iter().flatten().collect())
    }

    /// Canonical physical allocation slot, independent of omitted shared arrays.
    #[must_use]
    pub const fn allocation_ordinal(self) -> usize {
        self.allocation_ordinal
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
    pub(crate) fn refresh(
        previous: &NormalStatePrimitives,
        completion: CompleteDataOwnerCompletion,
        model: ModelGenerationId,
        plan: &NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let NormalStatePrimitives::ChannelLocal(domains) = previous else {
            return Err(SpectralOperatorError::ProblemMismatch);
        };
        if domains.len() != 1 {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let domain = domains[0].refresh(model, plan)?;
        Ok(Self {
            domains: vec![domain].into_boxed_slice(),
            completion,
            next_channel: 0,
        })
    }

    pub(crate) fn append_residual(
        &mut self,
        residual: crate::streaming_cube::band::CubeResidual,
    ) -> Result<(), SpectralOperatorError> {
        if self.domains.len() != 1 || residual.core.start != self.next_channel {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.domains[0].append_residual(&residual)?;
        self.next_channel = residual.core.end;
        Ok(())
    }

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
    major_cycle_residual: Option<Range<usize>>,
    scalars: usize,
    epoch_scalars: usize,
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
        let major_cycle_residual = residual.then(|| field(complex)).transpose()?;
        let epoch_scalars = major_cycle_residual.as_ref().unwrap_or(&dirty).end;
        let invariant_dirty = invariant.then(|| field(complex)).transpose()?;
        let psf = field(complex)?;
        let sensitivity = field(values)?;
        Ok(Self {
            dirty,
            invariant_dirty,
            psf,
            sensitivity,
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
/// Multi-term Taylor and joint families use their existing complete-family readers.
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
                Ok(Cow::Borrowed(&d.dirty()[offset..offset + self.cells]))
            }
        }
    }

    /// Load only the selected unnormalized point-spread-function plane.
    pub fn read_psf(&self) -> Result<Cow<'a, [Complex64]>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => d.read_complex(&d.fields.psf, offset, self.cells),
            NormalPlaneBacking::Resident(d) => {
                Ok(Cow::Borrowed(&d.psf()[offset..offset + self.cells]))
            }
        }
    }

    /// Load only the selected unnormalized sensitivity plane.
    pub fn read_sensitivity(&self) -> Result<Cow<'a, [f64]>, SpectralOperatorError> {
        let offset = self.plane * self.cells;
        match self.backing {
            NormalPlaneBacking::Stored(d) => {
                let start = d.fields.sensitivity.start + offset;
                d.read_scalars(start..start + self.cells)
            }
            NormalPlaneBacking::Resident(d) => {
                Ok(Cow::Borrowed(&d.sensitivity()[offset..offset + self.cells]))
            }
        }
    }
}

impl StoredChannelNormalDomain {
    fn refresh(
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

    fn append_residual(
        &mut self,
        residual: &crate::streaming_cube::band::CubeResidual,
    ) -> Result<(), SpectralOperatorError> {
        let range = &residual.core;
        if range.start != self.next_channel
            || range.start >= range.end
            || range.end > self.total_channels
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        if range.len() > self.window_channels {
            return Err(SpectralOperatorError::NormalStorage(
                "normal-state write exceeds the admitted channel window".into(),
            ));
        }
        if residual.shape != self.shape
            || residual.total_channels != self.total_channels
            || self.residual_model != Some(residual.model)
            || residual.values.len()
                != range.len() * self.polarizations * checked_cells(self.shape)?
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let scalars = complex_scalars(&residual.values)?;
        self.storage.write(
            range.start * self.polarizations * checked_cells(self.shape)? * 2,
            scalars,
        )?;
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
            p.invariant_dirty.is_some(),
            p.major_cycle_residual.is_some(),
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
        Arc::get_mut(&mut self.invariants)
            .ok_or(SpectralOperatorError::IncompleteCoverage)?
            .write(
                self.fields.sensitivity.start - self.fields.epoch_scalars + offset,
                &p.sensitivity,
            )?;
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
        let scalars = self.read_scalars(start..start + values * 2)?;
        scalar_complex(scalars)
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
                dirty: self
                    .read_complex(&self.fields.dirty, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                invariant_dirty: self
                    .fields
                    .invariant_dirty
                    .as_ref()
                    .map(|f| {
                        self.read_complex(f, offset, values)
                            .map(|v| v.into_owned().into_boxed_slice())
                    })
                    .transpose()?,
                common_residual: None,
                invariant_common_dirty: None,
                psf: self
                    .read_complex(&self.fields.psf, offset, values)?
                    .into_owned()
                    .into_boxed_slice(),
                sensitivity: self
                    .read_scalars(
                        self.fields.sensitivity.start + offset
                            ..self.fields.sensitivity.start + offset + values,
                    )?
                    .into_owned()
                    .into_boxed_slice(),
                primary_beam_weighted_sum: None,
                sum_weights: self.sum_weights[plane_range.clone()].into(),
                published_sum_weights: self.published_sum_weights[plane_range.clone()].into(),
                channel_sum_weights: Box::new([]),
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
                for &value in self
                    .read_scalars(start..start.saturating_add(width).min(field.end))?
                    .iter()
                {
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
