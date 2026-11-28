// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::Cell;

use super::*;
use crate::{ArrayLattice, near_tol};

#[test]
fn interrupted_order_partial_is_not_appended() {
    let mut values = vec![3.0, 1.0];
    let partial = vec![2.0, -1.0];
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        merge_order_values(&mut values, partial.clone(), Some(&cancelled)),
        Err(LatticeError::Interrupted)
    ));
    assert_eq!(values, [3.0, 1.0]);

    let fresh = Cancellation::new();
    merge_order_values(&mut values, partial, Some(&fresh)).unwrap();
    assert_eq!(values, [3.0, 1.0, 2.0, -1.0]);
}

#[test]
fn paged_statistic_columns_match_memory_and_release_their_scratch() {
    let data = ArrayD::from_shape_fn(IxDyn(&[8, 4, 3]), |index| {
        (5 * index[0]) as f64 - (2 * index[1]) as f64 + index[2] as f64
    });
    let image = ArrayLattice::new(data);
    let scratch = tempfile::tempdir().unwrap();
    let mut memory = LatticeStatistics::new(&image);
    let mut paged = LatticeStatistics::new(&image);
    for stats in [&mut memory, &mut paged] {
        stats.set_axes(vec![0]);
        stats.set_include_range(0.0, 29.0);
    }
    paged.set_cache_storage(TempStoragePolicy::Paged {
        scratch: crate::ScratchSpace::Directory(scratch.path().to_owned()),
    });
    for statistic in [
        Statistic::Npts,
        Statistic::Sum,
        Statistic::Variance,
        Statistic::Median,
        Statistic::MedAbsDevMed,
    ] {
        assert_eq!(
            paged.get_statistic(statistic).unwrap(),
            memory.get_statistic(statistic).unwrap()
        );
    }
    let caches = std::fs::read_dir(scratch.path())
        .unwrap()
        .map(|entry| entry.unwrap().path().join("TempLattice.table"))
        .collect::<Vec<_>>();
    assert_eq!(caches.len(), 2);
    assert!(caches.iter().all(|path| path.is_dir()));
    paged.clear_axes();
    assert!(std::fs::read_dir(scratch.path()).unwrap().next().is_none());
}

struct CountingLattice<T: LatticeElement> {
    inner: ArrayLattice<T>,
    slice_calls: Cell<usize>,
}

impl<T: LatticeElement> CountingLattice<T> {
    fn new(data: ArrayD<T>) -> Self {
        Self {
            inner: ArrayLattice::new(data),
            slice_calls: Cell::new(0),
        }
    }

    fn slice_calls(&self) -> usize {
        self.slice_calls.get()
    }
}

impl<T: LatticeElement> Lattice<T> for CountingLattice<T> {
    fn shape(&self) -> &[usize] {
        self.inner.shape()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.inner.get_at(position)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        self.slice_calls.set(self.slice_calls.get() + 1);
        self.inner.get_slice(start, shape, stride)
    }
}

#[test]
fn traversal_global_stats_match_expected_values() {
    let data = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data);
    let stats = LatticeStatistics::new(&lat);

    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        64.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Sum).unwrap()[IxDyn(&[0])],
        2016.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Median).unwrap()[IxDyn(&[0])],
        31.5
    );
}

#[test]
fn traversal_axis_stats_match_expected_values() {
    let data = ArrayD::from_shape_fn(IxDyn(&[64, 20]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data);
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_axes(vec![0]);

    let mean = stats.get_statistic(Statistic::Mean).unwrap();
    let q1 = stats.get_statistic(Statistic::Q1).unwrap();
    assert_eq!(mean.shape(), &[20]);
    assert!(mean.iter().all(|value| near_tol(*value, 31.5, 1e-8)));
    assert!(q1.iter().all(|value| *value == 15.0));
}

#[test]
fn traversal_min_max_respects_mask_and_range() {
    let data = ArrayD::from_shape_fn(IxDyn(&[8]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data);
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_include_range(2.0, 6.0);
    stats.set_pixel_mask(ArrayD::from_shape_fn(IxDyn(&[8]), |idx| idx[0] != 6));

    let (min_pos, max_pos) = stats.get_min_max_pos().unwrap();
    assert_eq!(min_pos, Some(vec![2]));
    assert_eq!(max_pos, Some(vec![5]));
}

#[test]
fn global_stats_respect_non_standard_mask_layout() {
    let data = ArrayD::from_shape_fn(IxDyn(&[2, 3]), |idx| (idx[0] + 10 * idx[1]) as f32);
    let mut mask_source = ArrayD::from_elem(IxDyn(&[3, 2]), false);
    mask_source[IxDyn(&[0, 0])] = true;
    mask_source[IxDyn(&[2, 1])] = true;
    let mask = mask_source.permuted_axes(IxDyn(&[1, 0]));

    let lat = ArrayLattice::new(data);
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_pixel_mask(mask);

    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        2.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Sum).unwrap()[IxDyn(&[0])],
        21.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Median).unwrap()[IxDyn(&[0])],
        10.5
    );
}

#[test]
fn repeated_basic_stats_reuse_single_traversal() {
    let lat = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[256]), |idx| idx[0] as f32));
    let stats = LatticeStatistics::new(&lat);

    let mean = stats.get_statistic(Statistic::Mean).unwrap();
    let sigma = stats.get_statistic(Statistic::Sigma).unwrap();

    assert_eq!(mean[IxDyn(&[0])], 127.5);
    assert!(sigma[IxDyn(&[0])] > 0.0);
    assert_eq!(lat.slice_calls(), 1);
}

#[test]
fn repeated_order_stats_reuse_single_traversal() {
    let lat = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[256]), |idx| idx[0] as f32));
    let stats = LatticeStatistics::new(&lat);

    let median = stats.get_statistic(Statistic::Median).unwrap();
    let q1 = stats.get_statistic(Statistic::Q1).unwrap();
    let q3 = stats.get_statistic(Statistic::Q3).unwrap();
    let mad = stats.get_statistic(Statistic::MedAbsDevMed).unwrap();

    assert_eq!(median[IxDyn(&[0])], 127.5);
    assert_eq!(q1[IxDyn(&[0])], 63.0);
    assert_eq!(q3[IxDyn(&[0])], 191.0);
    assert_eq!(mad[IxDyn(&[0])], 64.0);
    assert_eq!(lat.slice_calls(), 1);
}

#[test]
fn cache_invalidates_when_configuration_changes() {
    let lat = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32));
    let mut stats = LatticeStatistics::new(&lat);

    let baseline = stats.get_statistic(Statistic::Mean).unwrap();
    stats.set_include_range(8.0, 15.0);
    let filtered = stats.get_statistic(Statistic::Mean).unwrap();

    assert_eq!(baseline[IxDyn(&[0])], 31.5);
    assert_eq!(filtered[IxDyn(&[0])], 11.5);
    assert_eq!(lat.slice_calls(), 2);
}

#[test]
fn cache_invalidates_when_execution_policy_changes() {
    let lat = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32));
    let mut stats = LatticeStatistics::new(&lat);

    let baseline = stats.get_statistic(Statistic::Mean).unwrap();
    stats.set_execution_policy(ExecutionPolicy::Serial);
    let rebuilt = stats.get_statistic(Statistic::Mean).unwrap();

    assert_eq!(baseline[IxDyn(&[0])], rebuilt[IxDyn(&[0])]);
    assert_eq!(lat.slice_calls(), 2);
}

#[test]
fn explicit_parallel_policy_chunks_in_memory_lattices() {
    let shape = IxDyn(&[1024, 1024, 4]);
    let lat = CountingLattice::new(ArrayD::from_shape_fn(shape, |idx| {
        (idx[0] + idx[1] * 1024 + idx[2] * 1024 * 1024) as f32
    }));
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_axes(vec![0, 1]);
    stats.set_execution_policy(ExecutionPolicy::Parallel {
        workers: 2,
        prefetch_depth: 2,
    });

    let mean = stats.get_statistic(Statistic::Mean).unwrap();

    assert_eq!(mean.shape(), &[4]);
    assert!(lat.slice_calls() > 1);
}

#[test]
fn min_max_positions_reuse_basic_cache() {
    let lat = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32));
    let stats = LatticeStatistics::new(&lat);

    let _ = stats.get_statistic(Statistic::Mean).unwrap();
    let (min_pos, max_pos) = stats.get_min_max_pos().unwrap();

    assert_eq!(min_pos, Some(vec![0]));
    assert_eq!(max_pos, Some(vec![63]));
    assert_eq!(lat.slice_calls(), 1);
}

#[test]
fn duplicate_or_out_of_bounds_axes_error_cleanly() {
    let lat = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[4, 5]), |idx| {
        (idx[0] * 10 + idx[1]) as f32
    }));
    let mut stats = LatticeStatistics::new(&lat);

    stats.set_axes(vec![0, 0]);
    assert!(matches!(
        stats.get_statistic(Statistic::Mean),
        Err(LatticeError::InvalidTraversal(message))
            if message.contains("duplicate axis")
    ));

    stats.set_axes(vec![2]);
    assert!(matches!(
        stats.get_statistic(Statistic::Mean),
        Err(LatticeError::IndexOutOfBounds { .. })
    ));
}

#[test]
fn empty_selection_returns_nan_stats_and_no_positions() {
    let lat = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[8]), |idx| idx[0] as f32));
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_include_range(100.0, 200.0);

    let mean = stats.get_statistic(Statistic::Mean).unwrap();
    let sigma = stats.get_statistic(Statistic::Sigma).unwrap();
    let median = stats.get_statistic(Statistic::Median).unwrap();
    let (min_pos, max_pos) = stats.get_min_max_pos().unwrap();

    assert!(stats.get_statistic(Statistic::Npts).unwrap().is_empty());
    assert!(mean.is_empty());
    assert!(sigma.is_empty());
    assert!(median.is_empty());
    assert_eq!(min_pos, None);
    assert_eq!(max_pos, None);
}

#[test]
fn exclude_range_and_clear_range_restore_unfiltered_results() {
    let lat = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[6]), |idx| idx[0] as f32));
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_include_range(1.0, 4.0);
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        2.5
    );

    stats.set_exclude_range(1.0, 4.0);
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        2.5
    );

    stats.clear_range();
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        2.5
    );
    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        6.0
    );
}

#[test]
fn clear_pixel_mask_restores_masked_values() {
    let lat = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[4]), |idx| idx[0] as f32));
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_pixel_mask(
        ArrayD::from_shape_vec(IxDyn(&[4]), vec![true, false, false, true]).unwrap(),
    );

    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        2.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        1.5
    );

    stats.clear_pixel_mask();
    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        4.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        1.5
    );
}

#[test]
fn set_new_lattice_resets_filters_but_keeps_policy() {
    let first = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32));
    let second = CountingLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| {
        (idx[0] * 2) as f32
    }));
    let mut stats = LatticeStatistics::new(&first);
    stats.set_include_range(8.0, 15.0);
    stats.set_execution_policy(ExecutionPolicy::Serial);

    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        11.5
    );
    assert!(first.slice_calls() >= 1);

    stats.set_new_lattice(&second);
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        63.0
    );
    assert!(second.slice_calls() >= 1);
}

#[test]
fn bool_statistics_match_expected_fraction() {
    let lat = ArrayLattice::new(
        ArrayD::from_shape_vec(IxDyn(&[6]), vec![true, false, true, false, true, false]).unwrap(),
    );
    let stats = LatticeStatistics::new(&lat);

    assert_eq!(
        stats.get_statistic(Statistic::Npts).unwrap()[IxDyn(&[0])],
        6.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Sum).unwrap()[IxDyn(&[0])],
        3.0
    );
    assert_eq!(
        stats.get_statistic(Statistic::Mean).unwrap()[IxDyn(&[0])],
        0.5
    );
    assert_eq!(
        stats.get_statistic(Statistic::Median).unwrap()[IxDyn(&[0])],
        0.5
    );
}
