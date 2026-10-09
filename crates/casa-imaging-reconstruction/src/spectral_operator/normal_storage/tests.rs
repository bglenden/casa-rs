// SPDX-License-Identifier: LGPL-3.0-or-later

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
    ModelGenerationId(LogicalIdentity::from_bytes([19; 32]))
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
            },
            basis: SpectralBasisPlan::ChannelLocal,
            polarizations: POLARIZATIONS,
            dirty: complex(0.25),
            cube_real: None,
            psf: complex(-0.125),
            sensitivity: values.clone().map(|i| i as f64 * 0.25).collect(),
            sum_weights: planes.clone().map(|i| (i + 1) as f64).collect(),
            published_sum_weights: planes
                .clone()
                .map(|i| (i + 1) as f64 + if published_differ { 0.5 } else { 0.0 })
                .collect(),
            validity: planes
                .map(|i| {
                    if i % 2 == 0 {
                        SpectralChannelValidity::Valid
                    } else {
                        SpectralChannelValidity::Unmapped
                    }
                })
                .collect(),
            residual_model: Some(model()),
        },
    )
}

#[derive(Debug)]
struct ObservedFactory {
    maximum_access: Arc<AtomicUsize>,
    allowed: usize,
    scalar_sensitivity: bool,
}

#[derive(Debug)]
struct ObservedStorage {
    values: Box<[f64]>,
    maximum_access: Arc<AtomicUsize>,
    allowed: usize,
}

impl NormalStorageFactory for ObservedFactory {
    fn scalar_sensitivity(&self) -> bool {
        self.scalar_sensitivity
    }

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
    assert_eq!(selected.validity(), SpectralChannelValidity::Unmapped);
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
    assert_eq!(residual[0].re, offset as f64 + 0.25);
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
    assert_eq!(
        psf.as_ref(),
        &expected.psf().complex().unwrap()[CELLS..2 * CELLS]
    );
    assert_eq!(
        residual.as_ref(),
        &expected.dirty().complex().unwrap()[CELLS..2 * CELLS]
    );
    assert_eq!(
        sensitivity.as_ref(),
        &expected.sensitivity().dense().unwrap()[CELLS..2 * CELLS]
    );
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
    let mut stored = StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
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
    let domains = (0..2)
        .map(|ordinal| {
            let mut input = domain(0..1, true);
            input.domain_ordinal = ordinal;
            input.primitives.slab.total_channels = 1;
            input.primitives.basis =
                SpectralBasisPlan::Polynomial(super::super::BlockNormalPlan::constant(1.0e9));
            if ordinal == 1 {
                input.domain_role = ImageDomainRole::Outlier("constant".into());
                input.primitives.shape = [2, 3];
                for value in &mut input.primitives.psf {
                    value.re += 100.0;
                }
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
    assert_eq!(selected.validity(), SpectralChannelValidity::Unmapped);
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
        expected.dirty().complex().unwrap()[CELLS..].as_ptr()
    ));
    assert_eq!(
        residual.as_ref(),
        &expected.dirty().complex().unwrap()[CELLS..]
    );
    assert_eq!(psf.as_ref(), &expected.psf().complex().unwrap()[CELLS..]);
    assert_eq!(
        sensitivity.as_ref(),
        &expected.sensitivity().dense().unwrap()[CELLS..]
    );
    assert_eq!(residual[0].re, CELLS as f64 + 0.25);
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

#[test]
fn selective_normal_plane_rejects_nonconstant_families_and_propagates_storage_errors() {
    let mut input = domain(0..1, false);
    input.primitives.basis =
        SpectralBasisPlan::Polynomial(super::super::BlockNormalPlan::taylor(1.0e9, 2).unwrap());
    let coupled =
        NormalStatePrimitives::Coupled(SpectralPrimitiveDomains::new(vec![input].into()).unwrap());
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
    let mut domain = StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
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
    let mut stored = StoredChannelNormalDomain::begin(domain(0..CHANNELS, false), &plan).unwrap();
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

#[test]
fn residual_epochs_share_only_invariants_without_old_array_access_or_owner_chain() {
    let plan = NormalStoragePlan::resident(CHANNELS).unwrap();
    let mut old = StoredChannelNormalDomain::begin(domain(0..CHANNELS, true), &plan).unwrap();
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
        let next_model = ModelGenerationId(LogicalIdentity::from_bytes([epoch; 32]));
        let mut next = old.refresh(next_model, &plan).unwrap();
        let values: Box<[_]> = (0..CHANNELS * POLARIZATIONS * CELLS)
            .map(|index| index as f32 + f32::from(epoch))
            .collect();
        next.append_residual_planes(0..CHANNELS, [3, 2], next_model, &values)
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
            assert_eq!(actual.re, f64::from(*expected));
            assert_eq!(actual.im, 0.0);
        }
        accesses.store(0, Ordering::Relaxed);
        old = next;
    }
    drop(old);
    assert!(invariants.upgrade().is_none());
}

#[test]
fn owned_residual_wave_writes_in_admitted_windows_without_partition_copies() {
    let old_plan = NormalStoragePlan::resident(CHANNELS).unwrap();
    let old = StoredChannelNormalDomain::begin(domain(0..CHANNELS, true), &old_plan).unwrap();
    let maximum_access = Arc::new(AtomicUsize::new(0));
    let allowed = 2 * CELLS * POLARIZATIONS;
    let plan = NormalStoragePlan::new(
        Arc::new(ObservedFactory {
            maximum_access: maximum_access.clone(),
            allowed,
            scalar_sensitivity: false,
        }),
        1,
    )
    .unwrap();
    let mut next = old.refresh(model(), &plan).unwrap();
    let values: Box<[_]> = (0..CHANNELS * POLARIZATIONS * CELLS)
        .map(|n| n as f32 + 0.5)
        .collect();
    next.append_residual_planes(0..CHANNELS, [3, 2], model(), &values)
        .unwrap();
    assert!(next.is_complete());
    assert!(maximum_access.load(Ordering::Relaxed) <= allowed);
    for channel in 0..CHANNELS {
        let window = next.read_window(channel..channel + 1).unwrap();
        for (actual, &expected) in
            window.primitives().dirty.iter().zip(
                &values[channel * POLARIZATIONS * CELLS..(channel + 1) * POLARIZATIONS * CELLS],
            )
        {
            assert_eq!(actual.re, f64::from(expected));
            assert_eq!(actual.im, 0.0);
        }
    }
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
    let old = StoredChannelNormalDomain::begin(domain(0..CHANNELS, true), &plan).unwrap();
    let expected = old.content_identity().unwrap();
    let mut next = old.refresh(model(), &plan).unwrap();
    let leading = vec![3.0; (CHANNELS - 1) * POLARIZATIONS * CELLS];
    assert_eq!(
        next.append_residual_planes(1..CHANNELS, [3, 2], model(), &leading),
        Err(SpectralOperatorError::IncompleteCoverage)
    );
    next.append_residual_planes(0..CHANNELS - 1, [3, 2], model(), &leading)
        .unwrap();
    assert!(!next.is_complete());
    next.storage = Box::new(WriteFailure);
    assert!(matches!(
        next.append_residual_planes(
            CHANNELS - 1..CHANNELS,
            [3, 2],
            model(),
            &[3.0; POLARIZATIONS * CELLS]
        ),
        Err(SpectralOperatorError::NormalStorage(_))
    ));
    assert!(!next.is_complete());
    drop(next);
    assert_eq!(old.content_identity().unwrap(), expected);
    assert!(old.read_window(0..CHANNELS).is_ok());
}

#[test]
fn normal_storage_preserves_field_order_bits_and_identity_across_windows() {
    for published_differ in [false, true] {
        let expected_identity = domain(0..CHANNELS, published_differ)
            .primitives
            .normal_state_content_identity();
        for width in [1, 2, 3, CHANNELS] {
            let maximum_access = Arc::new(AtomicUsize::new(0));
            let allowed = 2 * CELLS * POLARIZATIONS * width;
            let plan = NormalStoragePlan::new(
                Arc::new(ObservedFactory {
                    maximum_access: maximum_access.clone(),
                    allowed,
                    scalar_sensitivity: false,
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
            for start in (0..CHANNELS).step_by(width) {
                let end = (start + width).min(CHANNELS);
                let window = stored.read_window(start..end).unwrap().primitives;
                let expected = domain(start..end, published_differ).primitives;
                assert_eq!(
                    window.normal_state_content_identity(),
                    expected.normal_state_content_identity()
                );
                assert_eq!(window.psf, expected.psf);
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

#[test]
fn natural_cube_scalar_sensitivity_reconstructs_each_plane_without_a_dense_backing() {
    let scalar_domain = |range: Range<usize>| {
        let mut domain = domain(range, false);
        domain.primitives.sensitivity = domain
            .primitives
            .sum_weights
            .iter()
            .flat_map(|&weight| std::iter::repeat_n(weight, CELLS))
            .collect();
        domain
    };
    let expected = scalar_domain(0..CHANNELS)
        .primitives
        .normal_state_content_identity();
    let plan = NormalStoragePlan::new(
        Arc::new(ObservedFactory {
            maximum_access: Arc::new(AtomicUsize::new(0)),
            allowed: 2 * CELLS * POLARIZATIONS,
            scalar_sensitivity: true,
        }),
        1,
    )
    .unwrap();
    let mut stored = StoredChannelNormalDomain::begin(scalar_domain(0..1), &plan).unwrap();
    assert!(stored.fields.scalar_sensitivity);
    assert!(stored.fields.sensitivity.is_empty());
    for channel in 1..CHANNELS {
        stored.append(scalar_domain(channel..channel + 1)).unwrap();
    }
    assert_eq!(stored.content_identity().unwrap(), expected);
    let state = NormalStatePrimitives::ChannelLocal(vec![stored].into());
    let reader = state.read_plane(0, 3, 1).unwrap();
    assert_eq!(reader.read_sensitivity().unwrap().as_ref(), &[8.0; CELLS]);
}
