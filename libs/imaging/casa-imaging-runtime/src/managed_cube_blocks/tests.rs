// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_lattices::TiledFileIoStats;
use num_complex::Complex32;

fn fixture<T: LatticeElement + TilePixel>(
    planes: usize,
    slots: usize,
    fill: Option<T>,
) -> (TempDir, Arc<CubeResidency>, ManagedPlaneArray<T>) {
    let root = tempfile::tempdir().unwrap();
    let manager = CubeResidency::new(1 << 24).unwrap();
    let array =
        ManagedPlaneArray::create(manager.clone(), root.path(), 2, 2, planes, fill).unwrap();
    {
        let mut state = manager.state.lock().unwrap();
        state.limit = state.fixed_bytes()
            + state.backends[&array.backend_id].staging_bytes
            + slots * state.entries[&array.blocks[0].0].bytes
            + operation_metadata_bytes(slots).unwrap();
    }
    (root, manager, array)
}

fn write_bytes(stats: TiledFileIoStats) -> usize {
    stats.flat_flush_write_bytes
        + stats.lru_flush_write_bytes
        + stats.lru_batch_flush_bytes
        + stats.direct_tile_write_bytes
}

#[test]
fn real_typed_spill_reload_and_clean_eviction_reconcile_physical_owners() {
    fn exercise<T: LatticeElement + TilePixel + PartialEq + std::fmt::Debug>(values: [T; 4]) {
        let (_root, manager, array) = fixture(3, 1, Some(T::default()));
        let pins = manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let mut view = array.write(&pins, 0, 0..4).unwrap();
        view.copy_from_slice(&values);
        view.finish();
        assert_eq!(
            array.blocks[0]
                .1
                .state
                .read()
                .unwrap()
                .initialized
                .heap_bytes(),
            0
        );
        drop(pins);
        drop(
            manager
                .admit(&[array.request(1, false).unwrap()], 0)
                .unwrap(),
        );
        assert!(!array.blocks[0].1.resident());
        let written = write_bytes(array._backend.array.lock().unwrap().io_stats());
        assert!(written > 0);
        drop(
            manager
                .admit(&[array.request(2, false).unwrap()], 0)
                .unwrap(),
        );
        assert_eq!(
            write_bytes(array._backend.array.lock().unwrap().io_stats()),
            written
        );
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(&*array.read(&pins, 0, 0..4).unwrap(), &values);
        {
            let state = manager.state.lock().unwrap();
            let owned = array
                ._backend
                .array
                .lock()
                .unwrap()
                .owned_persistent_heap_bytes()
                .unwrap();
            let backend = &state.backends[&array.backend_id];
            assert!(owned <= backend.owner_bytes + backend.staging_bytes);
            assert!(state.used <= state.limit);
            let block = array.blocks[0].1.state.read().unwrap();
            let actual = block.values.as_ref().unwrap().capacity() * size_of::<T>()
                + block.initialized.heap_bytes();
            assert!(actual <= state.entries[&array.blocks[0].0].bytes);
        }
        drop(pins);
        manager.evict_unpinned(array.blocks[0].0).unwrap();
        let before = array
            ._backend
            .array
            .lock()
            .unwrap()
            .owned_persistent_heap_bytes()
            .unwrap();
        manager.reclaim_idle_staging().unwrap();
        let after = array
            ._backend
            .array
            .lock()
            .unwrap()
            .owned_persistent_heap_bytes()
            .unwrap();
        assert!(after < before);
        assert!(array._backend.array.lock().unwrap().is_temp_closed());
        assert_eq!(
            manager.used_bytes(),
            manager.state.lock().unwrap().fixed_bytes()
        );
    }
    exercise([1.0_f32, 2.0, 3.0, 4.0]);
    exercise([1.0_f64, 2.0, 3.0, 4.0]);
    exercise([Complex32::new(2.0, -1.0); 4]);
    exercise([false, true, false, true]);
}

#[test]
fn cancelled_ranges_stay_invalid_after_spill_and_reload() {
    for fill in [Some(0.0_f32), None] {
        let (_root, manager, array) = fixture(2, 1, fill);
        // One unfinished plane's bitmap may remain while another is active.
        manager.state.lock().unwrap().limit += Coverage::bytes(4).unwrap();
        let pins = manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let mut first = array.write(&pins, 0, 0..2).unwrap();
        first.copy_from_slice(&[1.0, 2.0]);
        first.finish();
        let mut cancelled = array.write(&pins, 0, 2..4).unwrap();
        cancelled[0] = 9.0;
        drop(cancelled);
        assert!(array.read(&pins, 0, 2..4).is_err());
        drop(pins);
        drop(
            manager
                .admit(&[array.request(1, true).unwrap()], 0)
                .unwrap(),
        );
        assert_eq!(
            manager.state.lock().unwrap().entries[&array.blocks[0].0].cold_bytes,
            8
        );
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(&*array.read(&pins, 0, 0..2).unwrap(), &[1.0, 2.0]);
        assert!(array.read(&pins, 0, 2..4).is_err());
        drop(pins);
        let pins = manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        drop(array.write(&pins, 0, 0..4).unwrap());
        drop(pins);
        drop(
            manager
                .admit(&[array.request(1, true).unwrap()], 0)
                .unwrap(),
        );
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert!(array.read(&pins, 0, 0..4).is_err());
    }
}

#[test]
fn heterogeneous_arrays_share_one_complete_operation_budget() {
    let root = tempfile::tempdir().unwrap();
    let manager = CubeResidency::new(1 << 20).unwrap();
    let real =
        ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 3, Some(0.0)).unwrap();
    let complex = ManagedPlaneArray::<Complex32>::create(
        manager.clone(),
        root.path(),
        2,
        2,
        2,
        Some(Complex32::default()),
    )
    .unwrap();
    {
        let mut state = manager.state.lock().unwrap();
        state.limit = state.fixed_bytes()
            + state
                .backends
                .values()
                .map(|backend| backend.staging_bytes)
                .sum::<usize>()
            + state.entries[&real.blocks[0].0].bytes
            + state.entries[&complex.blocks[0].0].bytes
            + operation_metadata_bytes(2).unwrap();
    }
    let pins = manager
        .admit(
            &[
                real.request(0, true).unwrap(),
                complex.request(0, true).unwrap(),
            ],
            0,
        )
        .unwrap();
    assert_eq!(manager.used_bytes(), manager.state.lock().unwrap().limit);
    let mut real_view = real.write(&pins, 0, 0..4).unwrap();
    let mut complex_view = complex.write(&pins, 0, 0..4).unwrap();
    real_view.fill(7.0);
    complex_view.fill(Complex32::new(2.0, -3.0));
    real_view.finish();
    complex_view.finish();
    assert_eq!(
        manager
            .try_admit(&[real.request(1, false).unwrap()], 0)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(pins);
    manager.evict_unpinned(real.blocks[0].0).unwrap();
    manager.evict_unpinned(complex.blocks[0].0).unwrap();
    manager.reclaim_idle_staging().unwrap();
    let pins = manager
        .admit(
            &[
                real.request(0, false).unwrap(),
                complex.request(0, false).unwrap(),
            ],
            0,
        )
        .unwrap();
    assert_eq!(real.read(&pins, 0, 0..4).unwrap()[0], 7.0);
    assert_eq!(
        complex.read(&pins, 0, 0..4).unwrap()[0],
        Complex32::new(2.0, -3.0)
    );
}

#[test]
fn complete_access_sets_exclude_reverse_writes_and_allow_shared_reads() {
    let (_root, manager, array) = fixture(2, 2, Some(0.0_f32));
    let first = manager
        .admit(
            &[
                array.request(0, true).unwrap(),
                array.request(1, true).unwrap(),
            ],
            0,
        )
        .unwrap();
    assert_eq!(
        manager
            .try_admit(
                &[
                    array.request(1, true).unwrap(),
                    array.request(0, true).unwrap()
                ],
                0
            )
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(
        manager
            .try_admit(&[array.request(0, false).unwrap()], 0)
            .is_err()
    );
    drop(first);
    let second = manager
        .admit(
            &[
                array.request(1, true).unwrap(),
                array.request(0, true).unwrap(),
            ],
            0,
        )
        .unwrap();
    drop(second);
    let first_read = manager
        .admit(&[array.request(0, false).unwrap()], 0)
        .unwrap();
    let second_read = manager
        .admit(&[array.request(0, false).unwrap()], 0)
        .unwrap();
    let view = array.read(&first_read, 0, 0..4).unwrap();
    assert!(array.blocks[0].1.state.try_write().is_err());
    assert!(array.write(&second_read, 0, 0..4).is_err());
    drop(view);
    drop(first_read);
    drop(second_read);
}

#[test]
fn waiting_admission_observes_cancellation_and_later_progresses() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_root, manager, array) = fixture(2, 1, Some(0.0_f32));
    let first = manager
        .admit(&[array.request(0, true).unwrap()], 0)
        .unwrap();
    let cancelled = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let (ready, started) = std::sync::mpsc::channel();
        let manager = &manager;
        let array = &array;
        let cancelled = &cancelled;
        let worker = scope.spawn(move || {
            manager
                .admit_until(&[array.request(1, false).unwrap()], 0, || {
                    let _ = ready.send(());
                    cancelled.load(Ordering::Relaxed)
                })
                .err()
                .unwrap()
                .kind()
        });
        started
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        cancelled.store(true, Ordering::Relaxed);
        assert_eq!(worker.join().unwrap(), io::ErrorKind::Interrupted);
    });
    drop(first);
    let pins = manager
        .admit(&[array.request(1, false).unwrap()], 0)
        .unwrap();
    drop(pins);
    let pins = manager
        .admit(&[array.request(0, false).unwrap()], 0)
        .unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            manager
                .admit(&[array.request(1, false).unwrap()], 0)
                .unwrap()
        });
        drop(pins);
        drop(worker.join().unwrap());
    });
}

#[test]
fn retirement_releases_real_owners_before_credit_and_reuses_registry_capacity() {
    let (root, manager, array) = fixture(1, 1, Some(0.0_f32));
    drop(
        manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap(),
    );
    let before = manager.used_bytes();
    let weak_block = Arc::downgrade(&array.blocks[0].1);
    let weak_backend = Arc::downgrade(&array._backend);
    array
        .retire_dead_after_release(|| {
            assert!(weak_block.upgrade().is_none());
            assert!(weak_backend.upgrade().is_none());
            assert_eq!(manager.used_bytes(), before);
            assert!(manager.state.lock().unwrap().admitting);
        })
        .unwrap();
    let retained_registry = manager.used_bytes();
    assert_eq!(
        retained_registry,
        manager.state.lock().unwrap().fixed_bytes()
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    manager.state.lock().unwrap().limit = 1 << 24;
    let replacement =
        ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 1, None).unwrap();
    replacement.retire_dead().unwrap();
    assert_eq!(manager.used_bytes(), retained_registry);
}

#[test]
fn backing_failure_and_impossible_construction_leave_no_stranded_permit() {
    let (_root, manager, array) = fixture(1, 1, None::<f32>);
    assert!(
        manager
            .try_admit(&[array.request(0, false).unwrap()], 0)
            .is_err()
    );
    let pins = manager
        .admit(&[array.request(0, true).unwrap()], 0)
        .unwrap();
    let mut view = array.write(&pins, 0, 0..4).unwrap();
    view.fill(3.0);
    view.finish();
    drop(pins);
    manager.evict_unpinned(array.blocks[0].0).unwrap();
    manager.reclaim_idle_staging().unwrap();
    std::fs::remove_file(
        array
            ._backend
            ._directory
            .path()
            .join("values/table.f0_TSM0"),
    )
    .unwrap();
    assert!(
        manager
            .try_admit(&[array.request(0, false).unwrap()], 0)
            .is_err()
    );
    assert!(!manager.state.lock().unwrap().admitting);
    manager.reclaim_idle_staging().unwrap();
    assert_eq!(
        manager.used_bytes(),
        manager.state.lock().unwrap().fixed_bytes()
    );
    let root = tempfile::tempdir().unwrap();
    let minimum = size_of::<CubeResidency>() + 2 * size_of::<usize>();
    let tiny = CubeResidency::new(minimum).unwrap();
    assert!(ManagedPlaneArray::<f32>::create(tiny.clone(), root.path(), 2, 2, 1, None).is_err());
    assert_eq!(tiny.used_bytes(), minimum);
    assert!(tiny.state.lock().unwrap().entries.0.is_empty());
    assert!(tiny.state.lock().unwrap().backends.0.is_empty());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn codec_scratch_and_exact_operation_bound_are_admitted() {
    for planes in [1, 3] {
        let (_root, manager, array) = fixture(planes, 1, Some(false));
        let layout = PagedArray::<bool>::storage_layout(
            TiledShape::with_tile_shape(vec![2, 2, planes], vec![2, 2, 1]).unwrap(),
            4,
        )
        .unwrap();
        let state = manager.state.lock().unwrap();
        let stage = state.backends[&array.backend_id].staging_bytes;
        assert!(stage > layout.cache_payload_bytes().unwrap());
        assert!(
            stage >= layout.cache_payload_bytes().unwrap() + layout.flush_scratch_bytes().unwrap()
        );
        drop(state);
        manager.state.lock().unwrap().limit -= 1;
        assert_eq!(
            manager
                .try_admit(&[array.request(0, false).unwrap()], 0)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        manager.state.lock().unwrap().limit += 1;
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(manager.used_bytes(), manager.state.lock().unwrap().limit);
        drop(pins);
    }
}

#[test]
fn uniform_large_cube_coverage_has_no_per_pixel_resident_bitmap() {
    let maps: Vec<_> = (0..2048)
        .map(|_| Coverage::new(2048 * 2048, true).unwrap())
        .collect();
    assert_eq!(maps.iter().map(Coverage::heap_bytes).sum::<usize>(), 0);
    let mut map = Coverage::new(130, false).unwrap();
    map.update(&(2..129), true);
    assert!(map.covers(&(2..129)));
    assert!(!map.covers(&(1..130)));
    assert_eq!(map.heap_bytes(), Coverage::bytes(130).unwrap());
    map.update(&(0..2), true);
    map.update(&(129..130), true);
    assert!(map.covers(&(0..130)));
    assert_eq!(map.heap_bytes(), 0);
    map.update(&(0..130), false);
    assert_eq!(map.heap_bytes(), 0);
    assert!(!map.covers(&(0..130)));
    assert!(Coverage::bytes(usize::MAX).is_err());
}

#[test]
fn large_spatial_plane_evicts_and_reloads_under_one_plane_budget() {
    let root = tempfile::tempdir().unwrap();
    let manager = CubeResidency::new(128 << 20).unwrap();
    let cells = 2048 * 2048;
    let array =
        ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2048, 2048, 2, Some(0.0))
            .unwrap();
    {
        let mut state = manager.state.lock().unwrap();
        state.limit = state.fixed_bytes()
            + state.backends[&array.backend_id].staging_bytes
            + state.entries[&array.blocks[0].0].bytes
            + operation_metadata_bytes(1).unwrap();
    }
    let pins = manager
        .admit(&[array.request(0, true).unwrap()], 0)
        .unwrap();
    let mut edge = array.write(&pins, 0, cells - 4..cells).unwrap();
    edge.copy_from_slice(&[1.0, -2.0, 3.0, 4.0]);
    edge.finish();
    drop(pins);
    drop(
        manager
            .admit(&[array.request(1, false).unwrap()], 0)
            .unwrap(),
    );
    assert!(!array.blocks[0].1.resident());
    let pins = manager
        .admit(&[array.request(0, false).unwrap()], 0)
        .unwrap();
    assert_eq!(
        &*array.read(&pins, 0, cells - 4..cells).unwrap(),
        &[1.0, -2.0, 3.0, 4.0]
    );
    assert!(manager.used_bytes() <= manager.limit_bytes());
}

#[test]
fn scratch_only_workspaces_wait_without_partial_pins() {
    let manager = CubeResidency::new(4096).unwrap();
    let available = {
        let state = manager.state.lock().unwrap();
        state.limit - state.used
    };
    let first = manager.admit(&[], available).unwrap();
    assert_eq!(
        manager.try_admit(&[], 1).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(first);
    let next = manager.admit(&[], available).unwrap();
    drop(next);
    assert_eq!(manager.state.lock().unwrap().scratch, 0);
}
