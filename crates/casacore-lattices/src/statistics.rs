// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice statistics computation.
//!
//! Provides [`LatticeStatistics<T>`], the Rust equivalent of C++
//! `LatticeStatistics<T>` from
//! `casacore/lattices/LatticeMath/LatticeStatistics.h`.
//!
//! The implementation follows the C++ classical algorithm: a single pass
//! accumulates `npts`, `sum`, `sumsq`, `min`, `max`; derived quantities
//! (`mean`, `rms`, `variance`, `sigma`) are computed from those; order
//! statistics use quantile selection rather than full repeated sorts, while
//! preserving the relevant casacore-style median and fractile semantics.
//!
//! # Execution policy
//!
//! Rust exposes an explicit execution-policy knob for large workloads via
//! [`ExecutionPolicy`]. This is intentionally broader than the C++ casacore
//! API, which largely hides iterator/cache/threading choices behind internal
//! implementation details.
//!
//! The policy controls *how* the lattice is traversed and reduced, not *what*
//! statistic is returned:
//!
//! - [`ExecutionPolicy::Auto`] keeps the existing behavior of choosing a
//!   strategy from the lattice kind, traversal shape, and work size.
//! - [`ExecutionPolicy::Serial`] forces a single-threaded pass. This is often
//!   best for small in-memory arrays where chunking or extra threads would add
//!   overhead.
//! - [`ExecutionPolicy::Pipelined`] overlaps chunk reads with reduction work
//!   using a bounded producer/consumer pipeline. This is mainly useful for
//!   paged or persistent lattices where tile I/O is a visible cost.
//! - [`ExecutionPolicy::Parallel`] uses the same producer/read overlap but
//!   fans computation out across worker threads. This is usually the best
//!   choice for large compute-heavy workloads.
//!
//! Policy selection only affects future cache builds. Once a statistic family
//! is cached, subsequent calls reuse the cached results regardless of the
//! policy that produced them.
//!
//! # Relationship to C++
//!
//! | Rust                                  | C++ equivalent                          |
//! |---------------------------------------|-----------------------------------------|
//! | `LatticeStatistics::new`              | `LatticeStatistics::LatticeStatistics`  |
//! | `LatticeStatistics::set_axes`         | `LatticeStatistics::setAxes`            |
//! | `LatticeStatistics::set_include_range`| `LatticeStatistics::setInExCludeRange(include, {})` |
//! | `LatticeStatistics::set_exclude_range`| `LatticeStatistics::setInExCludeRange({}, exclude)` |
//! | `LatticeStatistics::set_pixel_mask`   | `MaskedLattice::setPixelMask`           |
//! | `LatticeStatistics::get_statistic`    | `LatticeStatistics::getStatistic`       |
//! | `LatticeStatistics::get_min_max_pos`  | `LatticeStatistics::getMinMaxPos`       |
//! | `Statistic` enum                      | `LatticeStatsBase::StatisticsTypes`     |

use std::cell::RefCell;
use std::cmp::Ordering;

use ndarray::{Array1, ArrayD, Dimension, IxDyn};
use ndarray_stats::{
    Quantile1dExt,
    interpolate::{Lower, Midpoint},
};
use noisy_float::types::n64;
use ordered_float::OrderedFloat;

use crate::execution::{
    ChunkTask, ParallelReadChunkConfig, PipelinedReadChunkConfig, ReadChunkExecutionStrategy,
    try_reduce_read_chunks,
};
use crate::{Lattice, LatticeElement, LatticeError, TraversalCursorIter, TraversalSpec};

/// Marker trait for lattice element types that support numerical statistics.
///
/// Implemented for all numeric casacore-native types (bool, integers, floats).
/// Not implemented for [`String`], [`num_complex::Complex32`], or
/// [`num_complex::Complex64`]; those require separate handling.
///
/// Mirrors C++ `NumericTraits<T>::PrecisionType` by providing a conversion to
/// `f64` for accumulation.
pub trait StatsElement: LatticeElement {
    /// Convert self to `f64` for statistical accumulation.
    fn to_f64_stats(&self) -> f64;
}

impl StatsElement for bool {
    fn to_f64_stats(&self) -> f64 {
        if *self { 1.0 } else { 0.0 }
    }
}
impl StatsElement for u8 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for i16 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for u16 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for i32 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for u32 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for i64 {
    fn to_f64_stats(&self) -> f64 {
        *self as f64
    }
}
impl StatsElement for f32 {
    fn to_f64_stats(&self) -> f64 {
        f64::from(*self)
    }
}
impl StatsElement for f64 {
    fn to_f64_stats(&self) -> f64 {
        *self
    }
}

/// Statistics quantities that [`LatticeStatistics`] can compute.
///
/// Mirrors C++ `LatticeStatsBase::StatisticsTypes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Statistic {
    /// Number of unmasked, unfiltered pixels.
    Npts,
    /// Sum of pixel values.
    Sum,
    /// Sum of squared pixel values.
    SumSq,
    /// Minimum pixel value.
    Min,
    /// Maximum pixel value.
    Max,
    /// Arithmetic mean.
    Mean,
    /// Root-mean-square: `sqrt(sumsq / npts)`.
    Rms,
    /// Bessel-corrected sample variance.
    Variance,
    /// Bessel-corrected sample standard deviation (`sqrt(variance)`).
    Sigma,
    /// Median, averaging the two middle values for even-sized samples.
    Median,
    /// First quartile (25th percentile).
    Q1,
    /// Third quartile (75th percentile).
    Q3,
    /// Interquartile range `Q3 − Q1`.
    Quartile,
    /// Median absolute deviation from the median.
    MedAbsDevMed,
}

/// Execution policy for large lattice-statistics traversals.
///
/// This is a Rust-specific extension beyond C++ casacore's public
/// `LatticeStatistics` API. It lets callers choose whether statistics should
/// run serially, overlap tile I/O with reduction work, or also use worker
/// threads for compute.
///
/// The policy does not change statistical semantics or result shapes; it only
/// changes the execution strategy used to build the internal caches.
///
/// # Variants
///
/// - [`Auto`](Self::Auto): choose a strategy from the lattice type and work
///   size. This is the default.
/// - [`Serial`](Self::Serial): use one thread and the most direct traversal.
/// - [`Pipelined`](Self::Pipelined): use one producer and one consumer thread
///   with a bounded queue. This is mainly useful for paged lattices, where the
///   next tile can be read while the previous tile is being reduced.
/// - [`Parallel`](Self::Parallel): use one producer plus a worker pool. This
///   combines the same I/O overlap as `Pipelined` with threaded compute.
///
/// `workers` and `prefetch_depth` are normalized internally so that obviously
/// degenerate values still behave sensibly:
///
/// - `prefetch_depth == 0` behaves as `1`
/// - `workers == 0` behaves as `1`
/// - `Parallel { workers: 1, .. }` behaves like `Pipelined`
///
/// # Examples
///
/// Force threaded compute for a large in-memory lattice:
///
/// ```
/// use casacore_lattices::{ArrayLattice, ExecutionPolicy, LatticeStatistics, Statistic};
/// use ndarray::{ArrayD, IxDyn};
///
/// let data = ArrayD::from_shape_fn(IxDyn(&[64, 64, 16]), |idx| {
///     (idx[0] + idx[1] * 64 + idx[2] * 64 * 64) as f32
/// });
/// let lattice = ArrayLattice::new(data);
/// let mut stats = LatticeStatistics::new(&lattice);
/// stats.set_axes(vec![0, 1]);
/// stats.set_execution_policy(ExecutionPolicy::Parallel {
///     workers: 4,
///     prefetch_depth: 8,
/// });
/// let mean = stats.get_statistic(Statistic::Mean).unwrap();
/// assert_eq!(mean.shape(), &[16]);
/// ```
///
/// For a large paged lattice, overlap-only execution can be a useful middle
/// ground when tile I/O matters more than raw compute:
///
/// ```no_run
/// use casacore_lattices::{ExecutionPolicy, LatticeStatistics, PagedArray, Statistic};
///
/// # fn demo(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
/// let lattice = PagedArray::<f32>::open(path)?;
/// let mut stats = LatticeStatistics::new(&lattice);
/// stats.set_axes(vec![0, 1]);
/// stats.set_execution_policy(ExecutionPolicy::Pipelined { prefetch_depth: 4 });
/// let sigma = stats.get_statistic(Statistic::Sigma)?;
/// println!("first plane sigma = {}", sigma[[0]]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExecutionPolicy {
    /// Choose an execution strategy automatically from the workload.
    #[default]
    Auto,
    /// Force a plain serial traversal and reduction.
    Serial,
    /// Overlap chunk reads and reduction work with a bounded queue.
    Pipelined { prefetch_depth: usize },
    /// Overlap reads and fan reduction work out across multiple workers.
    Parallel {
        workers: usize,
        prefetch_depth: usize,
    },
}

/// Computes statistics over an N-dimensional lattice.
///
/// Mirrors C++ `LatticeStatistics<T>` from
/// `casacore/lattices/LatticeMath/LatticeStatistics.h`.
///
/// By default statistics are computed over all pixels in the lattice.
/// Call [`set_axes`](Self::set_axes) to restrict the collapse to a subset of
/// dimensions; the result array is then indexed by the remaining axes.
///
/// Results are cached internally. The first request for any basic statistic
/// computes the whole basic family (`npts`, `sum`, `sumsq`, `min`, `max`,
/// `mean`, `rms`, `variance`, `sigma`) in one traversal. Likewise, the first
/// request for any order statistic computes the whole order-stat family in one
/// traversal and reuses it across subsequent calls.
///
/// The execution strategy used to build those caches is controlled by
/// [`ExecutionPolicy`]. Changing the execution policy invalidates the existing
/// caches so the next request rebuilds them with the new strategy.
pub struct LatticeStatistics<'a, T> {
    lattice: &'a dyn Lattice<T>,
    axes: Option<Vec<usize>>,
    include_range: Option<[f64; 2]>,
    exclude_range: Option<[f64; 2]>,
    mask: Option<ArrayD<bool>>,
    execution_policy: ExecutionPolicy,
    basic_cache: RefCell<Option<BasicStatsCache>>,
    order_cache: RefCell<Option<OrderStatsCache>>,
}

impl<'a, T: StatsElement> LatticeStatistics<'a, T> {
    /// Create a new statistics object wrapping `lattice`.
    pub fn new(lattice: &'a dyn Lattice<T>) -> Self {
        Self {
            lattice,
            axes: None,
            include_range: None,
            exclude_range: None,
            mask: None,
            execution_policy: ExecutionPolicy::Auto,
            basic_cache: RefCell::new(None),
            order_cache: RefCell::new(None),
        }
    }

    /// Specify which axes to collapse when computing statistics.
    pub fn set_axes(&mut self, axes: Vec<usize>) {
        self.axes = Some(axes);
        self.invalidate_caches();
    }

    /// Remove the axis restriction; subsequent calls produce global statistics.
    pub fn clear_axes(&mut self) {
        self.axes = None;
        self.invalidate_caches();
    }

    /// Only include pixels whose value falls in `[min, max]` (inclusive).
    ///
    /// Clears any previously set exclude range.
    pub fn set_include_range(&mut self, min: f64, max: f64) {
        self.include_range = Some([min, max]);
        self.exclude_range = None;
        self.invalidate_caches();
    }

    /// Exclude pixels whose value falls in `[min, max]` (inclusive).
    ///
    /// Clears any previously set include range.
    pub fn set_exclude_range(&mut self, min: f64, max: f64) {
        self.exclude_range = Some([min, max]);
        self.include_range = None;
        self.invalidate_caches();
    }

    /// Remove any include/exclude range filter.
    pub fn clear_range(&mut self) {
        self.include_range = None;
        self.exclude_range = None;
        self.invalidate_caches();
    }

    /// Apply a boolean pixel mask; pixels where `mask == false` are excluded.
    pub fn set_pixel_mask(&mut self, mask: ArrayD<bool>) {
        self.mask = Some(mask);
        self.invalidate_caches();
    }

    /// Remove the pixel mask.
    pub fn clear_pixel_mask(&mut self) {
        self.mask = None;
        self.invalidate_caches();
    }

    /// Set the execution policy used for future cache builds.
    ///
    /// This is a Rust-specific performance control with no direct C++
    /// `LatticeStatistics` equivalent. It only affects how statistics are
    /// computed, not the returned values.
    ///
    /// Changing the policy clears the cached statistic families so the next
    /// request rebuilds them using the new strategy.
    pub fn set_execution_policy(&mut self, policy: ExecutionPolicy) {
        self.execution_policy = policy;
        self.invalidate_caches();
    }

    /// Switch to a different lattice, resetting all data-selection state.
    ///
    /// The current execution policy is preserved so callers can keep a
    /// workload-specific performance preference while retargeting the
    /// statistics object to a different lattice.
    pub fn set_new_lattice(&mut self, lattice: &'a dyn Lattice<T>) {
        self.lattice = lattice;
        self.axes = None;
        self.include_range = None;
        self.exclude_range = None;
        self.mask = None;
        self.invalidate_caches();
    }

    /// Compute the requested statistic over the lattice.
    ///
    /// Returns an `ArrayD<f64>` with shape `[1]` for global statistics, or the
    /// shape of the non-collapsed dimensions when axes are set.
    pub fn get_statistic(&self, stat: Statistic) -> Result<ArrayD<f64>, LatticeError> {
        if stat.is_order_stat() {
            self.ensure_order_cache()?;
            Ok(self
                .order_cache
                .borrow()
                .as_ref()
                .expect("order cache initialized")
                .get(stat)
                .clone())
        } else {
            self.ensure_basic_cache()?;
            Ok(self
                .basic_cache
                .borrow()
                .as_ref()
                .expect("basic cache initialized")
                .get(stat)
                .clone())
        }
    }

    /// Return the multi-dimensional positions of the global minimum and maximum.
    #[allow(clippy::type_complexity)]
    pub fn get_min_max_pos(
        &self,
    ) -> Result<(Option<Vec<usize>>, Option<Vec<usize>>), LatticeError> {
        self.ensure_basic_cache()?;
        let cache = self.basic_cache.borrow();
        let cache = cache.as_ref().expect("basic cache initialized");
        Ok((cache.global_min_pos.clone(), cache.global_max_pos.clone()))
    }

    fn invalidate_caches(&mut self) {
        *self.basic_cache.get_mut() = None;
        *self.order_cache.get_mut() = None;
    }

    fn ensure_basic_cache(&self) -> Result<(), LatticeError> {
        if self.basic_cache.borrow().is_none() {
            let cache = self.build_basic_cache()?;
            *self.basic_cache.borrow_mut() = Some(cache);
        }
        Ok(())
    }

    fn ensure_order_cache(&self) -> Result<(), LatticeError> {
        if self.order_cache.borrow().is_none() {
            let cache = self.build_order_cache()?;
            *self.order_cache.borrow_mut() = Some(cache);
        }
        Ok(())
    }

    fn filter_state<'b>(&'b self, mask_strides: Option<&'b [usize]>) -> FilterState<'b> {
        FilterState {
            include_range: self.include_range,
            exclude_range: self.exclude_range,
            mask: self.mask.as_ref(),
            mask_slice: self
                .mask
                .as_ref()
                .and_then(|mask| mask.as_slice_memory_order()),
            mask_strides,
        }
    }

    fn basic_execution_strategy(
        &self,
        per_worker_state_bytes: usize,
    ) -> ReadChunkExecutionStrategy {
        self.execution_strategy(per_worker_state_bytes, 32 * 1024 * 1024, 64 * 1024 * 1024)
    }

    fn order_execution_strategy(
        &self,
        per_worker_state_bytes: usize,
    ) -> ReadChunkExecutionStrategy {
        self.execution_strategy(per_worker_state_bytes, 16 * 1024 * 1024, 256 * 1024 * 1024)
    }

    fn build_basic_cache(&self) -> Result<BasicStatsCache, LatticeError> {
        match &self.axes {
            None => self.build_global_basic_cache(),
            Some(axes) => match self.axis_layout(axes)? {
                None => self.build_global_basic_cache(),
                Some(layout) => self.build_axis_basic_cache(&layout),
            },
        }
    }

    fn build_order_cache(&self) -> Result<OrderStatsCache, LatticeError> {
        match &self.axes {
            None => self.build_global_order_cache(),
            Some(axes) => match self.axis_layout(axes)? {
                None => self.build_global_order_cache(),
                Some(layout) => self.build_axis_order_cache(&layout),
            },
        }
    }

    fn build_global_basic_cache(&self) -> Result<BasicStatsCache, LatticeError> {
        let mask_strides = self.mask.as_ref().and_then(|mask| {
            mask.as_slice_memory_order()
                .map(|_| c_order_strides(self.lattice.shape()))
        });
        let filters = self.filter_state(mask_strides.as_deref());

        let partial = try_reduce_read_chunks(
            self.lattice,
            self.traversal_spec_for_strategy(
                self.basic_execution_strategy(std::mem::size_of::<GlobalBasicPartial>()),
            ),
            self.basic_execution_strategy(std::mem::size_of::<GlobalBasicPartial>()),
            GlobalBasicPartial::default,
            |partial, chunk| accumulate_global_basic_chunk(partial, &chunk, &filters),
            |partial, other| {
                partial.merge(other);
                Ok(())
            },
        )?;

        if partial.accum.npts == 0 {
            return Ok(BasicStatsCache::empty_global());
        }

        Ok(BasicStatsCache::from_accumulators(
            &[1],
            &[partial.accum],
            partial.min_pos,
            partial.max_pos,
        ))
    }

    fn build_axis_basic_cache(&self, layout: &AxisLayout) -> Result<BasicStatsCache, LatticeError> {
        let mask_strides = self.mask.as_ref().and_then(|mask| {
            mask.as_slice_memory_order()
                .map(|_| c_order_strides(self.lattice.shape()))
        });
        let filters = self.filter_state(mask_strides.as_deref());
        let per_worker_state_bytes = layout
            .n_out
            .saturating_mul(std::mem::size_of::<RunningStats>());
        let strategy = self.basic_execution_strategy(per_worker_state_bytes);
        let accumulators = try_reduce_read_chunks(
            self.lattice,
            self.traversal_spec_for_strategy(strategy),
            strategy,
            || vec![RunningStats::default(); layout.n_out],
            |accumulators, chunk| {
                accumulate_axis_basic_chunk(accumulators, layout, &chunk, &filters)
            },
            |accumulators, other| {
                merge_running_stats(accumulators, other);
                Ok(())
            },
        )?;

        Ok(BasicStatsCache::from_accumulators(
            &layout.out_shape,
            &accumulators,
            None,
            None,
        ))
    }

    fn build_global_order_cache(&self) -> Result<OrderStatsCache, LatticeError> {
        let mask_strides = self.mask.as_ref().and_then(|mask| {
            mask.as_slice_memory_order()
                .map(|_| c_order_strides(self.lattice.shape()))
        });
        let filters = self.filter_state(mask_strides.as_deref());
        let reserve = global_order_reserve(self.lattice.nelements(), self.has_sparse_filter());
        let strategy =
            self.order_execution_strategy(reserve.saturating_mul(std::mem::size_of::<f64>()));
        let values = try_reduce_read_chunks(
            self.lattice,
            self.traversal_spec_for_strategy(strategy),
            strategy,
            || Vec::with_capacity(reserve),
            |values, chunk| accumulate_global_order_chunk(values, &chunk, &filters),
            |values, other| {
                values.extend(other);
                Ok(())
            },
        )?;

        if values.is_empty() {
            return Ok(OrderStatsCache::empty_global());
        }

        let summary = compute_order_summary(&values)?;
        Ok(OrderStatsCache::from_summaries(&[1], &[summary]))
    }

    fn build_axis_order_cache(&self, layout: &AxisLayout) -> Result<OrderStatsCache, LatticeError> {
        let mask_strides = self.mask.as_ref().and_then(|mask| {
            mask.as_slice_memory_order()
                .map(|_| c_order_strides(self.lattice.shape()))
        });
        let filters = self.filter_state(mask_strides.as_deref());
        let reserve_per_bucket = axis_order_reserve(
            layout.collapsed_size,
            self.has_sparse_filter(),
            layout.n_out,
        );
        let per_worker_state_bytes = layout
            .n_out
            .saturating_mul(reserve_per_bucket)
            .saturating_mul(std::mem::size_of::<f64>());
        let strategy = self.order_execution_strategy(per_worker_state_bytes);
        let buckets = try_reduce_read_chunks(
            self.lattice,
            self.traversal_spec_for_strategy(strategy),
            strategy,
            || {
                (0..layout.n_out)
                    .map(|_| Vec::with_capacity(reserve_per_bucket))
                    .collect::<Vec<_>>()
            },
            |buckets, chunk| accumulate_axis_order_chunk(buckets, layout, &chunk, &filters),
            |buckets, other| {
                merge_order_buckets(buckets, other);
                Ok(())
            },
        )?;

        let mut summaries = Vec::with_capacity(layout.n_out);
        for values in buckets {
            if values.is_empty() {
                summaries.push(OrderStatsSummary::nan());
            } else {
                summaries.push(compute_order_summary(&values)?);
            }
        }

        Ok(OrderStatsCache::from_summaries(
            &layout.out_shape,
            &summaries,
        ))
    }

    fn execution_strategy(
        &self,
        per_worker_state_bytes: usize,
        large_work_threshold: usize,
        max_parallel_state_bytes: usize,
    ) -> ReadChunkExecutionStrategy {
        match self.execution_policy {
            ExecutionPolicy::Auto => self.auto_execution_strategy(
                per_worker_state_bytes,
                large_work_threshold,
                max_parallel_state_bytes,
            ),
            ExecutionPolicy::Serial => ReadChunkExecutionStrategy::Serial,
            ExecutionPolicy::Pipelined { prefetch_depth } => {
                ReadChunkExecutionStrategy::Pipelined(PipelinedReadChunkConfig {
                    prefetch_depth: prefetch_depth.max(1),
                })
            }
            ExecutionPolicy::Parallel {
                workers,
                prefetch_depth,
            } => {
                let workers = workers.max(1);
                if workers == 1 {
                    ReadChunkExecutionStrategy::Pipelined(PipelinedReadChunkConfig {
                        prefetch_depth: prefetch_depth.max(1),
                    })
                } else {
                    ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
                        workers,
                        prefetch_depth: prefetch_depth.max(workers),
                    })
                }
            }
        }
    }

    fn auto_execution_strategy(
        &self,
        per_worker_state_bytes: usize,
        large_work_threshold: usize,
        max_parallel_state_bytes: usize,
    ) -> ReadChunkExecutionStrategy {
        let available = thread_parallelism();
        let task_count = self.chunked_task_count();
        if task_count < 2 || self.lattice.nelements() < large_work_threshold {
            return ReadChunkExecutionStrategy::Serial;
        }

        let io_bound = self.lattice.is_paged() || self.lattice.is_persistent();
        if available < 2 {
            return if io_bound {
                ReadChunkExecutionStrategy::Pipelined(PipelinedReadChunkConfig {
                    prefetch_depth: 2,
                })
            } else {
                ReadChunkExecutionStrategy::Serial
            };
        }

        let total_state_bytes = per_worker_state_bytes.saturating_mul(available);
        if total_state_bytes > max_parallel_state_bytes {
            return if io_bound {
                ReadChunkExecutionStrategy::Pipelined(PipelinedReadChunkConfig {
                    prefetch_depth: available.max(2),
                })
            } else {
                ReadChunkExecutionStrategy::Serial
            };
        }

        let workers = available.min(task_count.max(1));
        ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
            workers,
            prefetch_depth: workers * 2,
        })
    }

    fn traversal_spec_for_strategy(&self, strategy: ReadChunkExecutionStrategy) -> TraversalSpec {
        if self.lattice.is_paged() || self.lattice.is_persistent() {
            TraversalSpec::tiles()
        } else if matches!(strategy, ReadChunkExecutionStrategy::Serial) {
            TraversalSpec::chunks(self.lattice.shape().to_vec())
        } else {
            TraversalSpec::chunks(self.parallel_cursor_shape())
        }
    }

    fn parallel_cursor_shape(&self) -> Vec<usize> {
        let full_shape = self.lattice.shape();
        self.lattice
            .nice_cursor_shape()
            .into_iter()
            .zip(full_shape.iter())
            .map(|(cursor, &extent)| cursor.clamp(1, extent.max(1)))
            .collect()
    }

    fn chunked_task_count(&self) -> usize {
        let full_shape = self.lattice.shape().to_vec();
        let cursor_shape = if self.lattice.is_paged() || self.lattice.is_persistent() {
            self.lattice.nice_cursor_shape()
        } else {
            self.parallel_cursor_shape()
        };
        let spec = if self.lattice.is_paged() || self.lattice.is_persistent() {
            TraversalSpec::tiles()
        } else {
            TraversalSpec::chunks(cursor_shape.clone())
        };
        TraversalCursorIter::new(full_shape, cursor_shape, spec)
            .size_hint()
            .1
            .unwrap_or(0)
    }

    fn axis_layout(&self, axes: &[usize]) -> Result<Option<AxisLayout>, LatticeError> {
        let axes = self.validated_axes(axes)?;
        let shape = self.lattice.shape();
        let ndim = shape.len();
        let out_axes: Vec<usize> = (0..ndim).filter(|axis| !axes.contains(axis)).collect();
        if out_axes.is_empty() {
            return Ok(None);
        }

        let out_shape: Vec<usize> = out_axes.iter().map(|&axis| shape[axis]).collect();
        let collapsed_size = axes.iter().map(|&axis| shape[axis]).product();
        Ok(Some(AxisLayout {
            out_axes,
            out_shape: out_shape.clone(),
            out_strides: c_order_strides(&out_shape),
            n_out: out_shape.iter().product(),
            collapsed_size,
        }))
    }

    fn validated_axes(&self, axes: &[usize]) -> Result<Vec<usize>, LatticeError> {
        let ndim = self.lattice.ndim();
        let mut seen = vec![false; ndim];
        let mut normalized = Vec::with_capacity(axes.len());

        for &axis in axes {
            if axis >= ndim {
                return Err(LatticeError::IndexOutOfBounds {
                    index: vec![axis],
                    shape: vec![ndim],
                });
            }
            if seen[axis] {
                return Err(LatticeError::InvalidTraversal(format!(
                    "statistics axes contain duplicate axis {axis}"
                )));
            }
            seen[axis] = true;
            normalized.push(axis);
        }

        Ok(normalized)
    }

    fn has_sparse_filter(&self) -> bool {
        self.mask.is_some() || self.include_range.is_some() || self.exclude_range.is_some()
    }
}

#[derive(Clone, Copy)]
struct FilterState<'a> {
    include_range: Option<[f64; 2]>,
    exclude_range: Option<[f64; 2]>,
    mask: Option<&'a ArrayD<bool>>,
    mask_slice: Option<&'a [bool]>,
    mask_strides: Option<&'a [usize]>,
}

fn accumulate_global_basic_chunk<T: StatsElement>(
    partial: &mut GlobalBasicPartial,
    chunk: &ChunkTask<T>,
    filters: &FilterState<'_>,
) -> Result<(), LatticeError> {
    for_each_accepted_value_in_chunk(chunk, filters, |idx, value| {
        if partial.accum.npts == 0 || value < partial.accum.min {
            partial.min_pos = Some(idx.to_vec());
        }
        if partial.accum.npts == 0 || value > partial.accum.max {
            partial.max_pos = Some(idx.to_vec());
        }
        partial.accum.push(value);
    })
}

fn accumulate_axis_basic_chunk<T: StatsElement>(
    accumulators: &mut [RunningStats],
    layout: &AxisLayout,
    chunk: &ChunkTask<T>,
    filters: &FilterState<'_>,
) -> Result<(), LatticeError> {
    for_each_accepted_value_in_chunk(chunk, filters, |idx, value| {
        let out_flat = out_flat_index(idx, &layout.out_axes, &layout.out_strides);
        accumulators[out_flat].push(value);
    })
}

fn accumulate_global_order_chunk<T: StatsElement>(
    values: &mut Vec<f64>,
    chunk: &ChunkTask<T>,
    filters: &FilterState<'_>,
) -> Result<(), LatticeError> {
    for_each_accepted_value_in_chunk(chunk, filters, |_, value| values.push(value))
}

fn accumulate_axis_order_chunk<T: StatsElement>(
    buckets: &mut [Vec<f64>],
    layout: &AxisLayout,
    chunk: &ChunkTask<T>,
    filters: &FilterState<'_>,
) -> Result<(), LatticeError> {
    for_each_accepted_value_in_chunk(chunk, filters, |idx, value| {
        let out_flat = out_flat_index(idx, &layout.out_axes, &layout.out_strides);
        buckets[out_flat].push(value);
    })
}

fn for_each_accepted_value_in_chunk<T: StatsElement>(
    chunk: &ChunkTask<T>,
    filters: &FilterState<'_>,
    mut f: impl FnMut(&[usize], f64),
) -> Result<(), LatticeError> {
    let origin = &chunk.cursor.position;

    if let Some(values) = chunk.data.as_slice_memory_order() {
        let axis_order = memory_order_axis_path(chunk.data.strides());
        let mut full_idx = origin.clone();
        let last = values.len().saturating_sub(1);
        for (i, value) in values.iter().enumerate() {
            let fv = value.to_f64_stats();
            if accept_idx_fast(filters, fv, &full_idx) {
                f(&full_idx, fv);
            }
            if i != last {
                advance_position_in_axis_order(
                    &mut full_idx,
                    origin,
                    &chunk.cursor.shape,
                    &axis_order,
                );
            }
        }
        return Ok(());
    }

    let mut full_idx = origin.clone();
    for (local_idx, value) in chunk.data.indexed_iter() {
        write_global_idx(&mut full_idx, origin, local_idx.slice());
        let fv = value.to_f64_stats();
        if accept_idx_fast(filters, fv, &full_idx) {
            f(&full_idx, fv);
        }
    }
    Ok(())
}

fn accept_idx_fast(filters: &FilterState<'_>, value: f64, idx: &[usize]) -> bool {
    if let Some(mask) = filters.mask {
        let accept = if let (Some(mask_slice), Some(mask_strides)) =
            (filters.mask_slice, filters.mask_strides)
        {
            let flat = flat_index(idx, mask_strides);
            mask_slice[flat]
        } else {
            mask[IxDyn(idx)]
        };
        if !accept {
            return false;
        }
    }

    accept_value(filters, value)
}

fn accept_value(filters: &FilterState<'_>, value: f64) -> bool {
    if let Some([lo, hi]) = filters.include_range
        && (value < lo || value > hi)
    {
        return false;
    }
    if let Some([lo, hi]) = filters.exclude_range
        && value >= lo
        && value <= hi
    {
        return false;
    }
    true
}

fn global_order_reserve(nelements: usize, sparse: bool) -> usize {
    if sparse {
        nelements.min(4096)
    } else {
        nelements.min(262_144)
    }
}

fn axis_order_reserve(collapsed_size: usize, sparse: bool, n_out: usize) -> usize {
    if sparse {
        collapsed_size.saturating_div(4).clamp(1, 4096)
    } else if n_out <= 1 {
        collapsed_size.min(262_144)
    } else {
        collapsed_size.min(8192)
    }
}

#[derive(Clone)]
struct BasicStatsCache {
    npts: ArrayD<f64>,
    sum: ArrayD<f64>,
    sumsq: ArrayD<f64>,
    min: ArrayD<f64>,
    max: ArrayD<f64>,
    mean: ArrayD<f64>,
    rms: ArrayD<f64>,
    variance: ArrayD<f64>,
    sigma: ArrayD<f64>,
    global_min_pos: Option<Vec<usize>>,
    global_max_pos: Option<Vec<usize>>,
}

impl BasicStatsCache {
    fn empty_global() -> Self {
        let empty = empty_stat_array();
        Self {
            npts: empty.clone(),
            sum: empty.clone(),
            sumsq: empty.clone(),
            min: empty.clone(),
            max: empty.clone(),
            mean: empty.clone(),
            rms: empty.clone(),
            variance: empty.clone(),
            sigma: empty,
            global_min_pos: None,
            global_max_pos: None,
        }
    }

    fn from_accumulators(
        shape: &[usize],
        accumulators: &[RunningStats],
        global_min_pos: Option<Vec<usize>>,
        global_max_pos: Option<Vec<usize>>,
    ) -> Self {
        Self {
            npts: array_from_accumulators(shape, accumulators, Statistic::Npts),
            sum: array_from_accumulators(shape, accumulators, Statistic::Sum),
            sumsq: array_from_accumulators(shape, accumulators, Statistic::SumSq),
            min: array_from_accumulators(shape, accumulators, Statistic::Min),
            max: array_from_accumulators(shape, accumulators, Statistic::Max),
            mean: array_from_accumulators(shape, accumulators, Statistic::Mean),
            rms: array_from_accumulators(shape, accumulators, Statistic::Rms),
            variance: array_from_accumulators(shape, accumulators, Statistic::Variance),
            sigma: array_from_accumulators(shape, accumulators, Statistic::Sigma),
            global_min_pos,
            global_max_pos,
        }
    }

    fn get(&self, stat: Statistic) -> &ArrayD<f64> {
        match stat {
            Statistic::Npts => &self.npts,
            Statistic::Sum => &self.sum,
            Statistic::SumSq => &self.sumsq,
            Statistic::Min => &self.min,
            Statistic::Max => &self.max,
            Statistic::Mean => &self.mean,
            Statistic::Rms => &self.rms,
            Statistic::Variance => &self.variance,
            Statistic::Sigma => &self.sigma,
            _ => unreachable!("basic cache only stores basic statistics"),
        }
    }
}

#[derive(Clone)]
struct OrderStatsCache {
    median: ArrayD<f64>,
    q1: ArrayD<f64>,
    q3: ArrayD<f64>,
    quartile: ArrayD<f64>,
    mad: ArrayD<f64>,
}

impl OrderStatsCache {
    fn empty_global() -> Self {
        let empty = empty_stat_array();
        Self {
            median: empty.clone(),
            q1: empty.clone(),
            q3: empty.clone(),
            quartile: empty.clone(),
            mad: empty,
        }
    }

    fn from_summaries(shape: &[usize], summaries: &[OrderStatsSummary]) -> Self {
        let mk = |f: fn(&OrderStatsSummary) -> f64| {
            ArrayD::from_shape_vec(IxDyn(shape), summaries.iter().map(f).collect::<Vec<_>>())
                .expect("shape/data length match")
        };
        Self {
            median: mk(|summary| summary.median),
            q1: mk(|summary| summary.q1),
            q3: mk(|summary| summary.q3),
            quartile: mk(|summary| summary.quartile),
            mad: mk(|summary| summary.mad),
        }
    }

    fn get(&self, stat: Statistic) -> &ArrayD<f64> {
        match stat {
            Statistic::Median => &self.median,
            Statistic::Q1 => &self.q1,
            Statistic::Q3 => &self.q3,
            Statistic::Quartile => &self.quartile,
            Statistic::MedAbsDevMed => &self.mad,
            _ => unreachable!("order cache only stores order statistics"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RunningStats {
    npts: usize,
    sum: f64,
    sumsq: f64,
    min: f64,
    max: f64,
}

impl Default for RunningStats {
    fn default() -> Self {
        Self {
            npts: 0,
            sum: 0.0,
            sumsq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

impl RunningStats {
    fn push(&mut self, value: f64) {
        self.npts += 1;
        self.sum += value;
        self.sumsq += value * value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn merge(&mut self, other: Self) {
        if other.npts == 0 {
            return;
        }
        if self.npts == 0 {
            *self = other;
            return;
        }
        self.npts += other.npts;
        self.sum += other.sum;
        self.sumsq += other.sumsq;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    fn finish(self, stat: Statistic) -> f64 {
        debug_assert!(self.npts > 0);
        let n = self.npts as f64;
        match stat {
            Statistic::Npts => n,
            Statistic::Sum => self.sum,
            Statistic::SumSq => self.sumsq,
            Statistic::Min => self.min,
            Statistic::Max => self.max,
            Statistic::Mean => self.sum / n,
            Statistic::Rms => (self.sumsq / n).sqrt(),
            Statistic::Variance => {
                if self.npts < 2 {
                    f64::NAN
                } else {
                    let mean = self.sum / n;
                    (self.sumsq - n * mean * mean) / (n - 1.0)
                }
            }
            Statistic::Sigma => {
                if self.npts < 2 {
                    f64::NAN
                } else {
                    self.finish(Statistic::Variance).sqrt()
                }
            }
            _ => unreachable!("order statistics require retained values"),
        }
    }
}

#[derive(Clone, Copy)]
struct OrderStatsSummary {
    median: f64,
    q1: f64,
    q3: f64,
    quartile: f64,
    mad: f64,
}

impl OrderStatsSummary {
    fn nan() -> Self {
        Self {
            median: f64::NAN,
            q1: f64::NAN,
            q3: f64::NAN,
            quartile: f64::NAN,
            mad: f64::NAN,
        }
    }
}

#[derive(Clone)]
struct AxisLayout {
    out_axes: Vec<usize>,
    out_shape: Vec<usize>,
    out_strides: Vec<usize>,
    n_out: usize,
    collapsed_size: usize,
}

#[derive(Default)]
struct GlobalBasicPartial {
    accum: RunningStats,
    min_pos: Option<Vec<usize>>,
    max_pos: Option<Vec<usize>>,
}

impl GlobalBasicPartial {
    fn merge(&mut self, other: Self) {
        if other.accum.npts == 0 {
            return;
        }
        if self.accum.npts == 0 {
            *self = other;
            return;
        }
        if other.accum.min < self.accum.min {
            self.min_pos = other.min_pos;
        }
        if other.accum.max > self.accum.max {
            self.max_pos = other.max_pos;
        }
        self.accum.merge(other.accum);
    }
}

fn merge_running_stats(accumulators: &mut [RunningStats], other: Vec<RunningStats>) {
    for (lhs, rhs) in accumulators.iter_mut().zip(other) {
        lhs.merge(rhs);
    }
}

fn merge_order_buckets(buckets: &mut [Vec<f64>], other: Vec<Vec<f64>>) {
    for (lhs, rhs) in buckets.iter_mut().zip(other) {
        lhs.extend(rhs);
    }
}

fn array_from_accumulators(
    shape: &[usize],
    accumulators: &[RunningStats],
    stat: Statistic,
) -> ArrayD<f64> {
    ArrayD::from_shape_vec(
        IxDyn(shape),
        accumulators
            .iter()
            .map(|accum| {
                if accum.npts == 0 {
                    f64::NAN
                } else {
                    accum.finish(stat)
                }
            })
            .collect(),
    )
    .expect("shape/data length match")
}

fn empty_stat_array() -> ArrayD<f64> {
    ArrayD::from_shape_vec(IxDyn(&[0]), vec![]).expect("empty array")
}

fn advance_position_in_axis_order(
    full_idx: &mut [usize],
    origin: &[usize],
    shape: &[usize],
    axis_order: &[usize],
) {
    for (order_pos, &axis) in axis_order.iter().enumerate() {
        let limit = origin[axis] + shape[axis];
        if full_idx[axis] + 1 < limit {
            full_idx[axis] += 1;
            for &reset_axis in axis_order.iter().take(order_pos) {
                full_idx[reset_axis] = origin[reset_axis];
            }
            return;
        }
    }
}

fn memory_order_axis_path(strides: &[isize]) -> Vec<usize> {
    let mut axes: Vec<usize> = (0..strides.len()).collect();
    axes.sort_by_key(|&axis| (strides[axis].unsigned_abs(), axis));
    axes
}

fn thread_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
}

pub(crate) fn c_order_strides(shape: &[usize]) -> Vec<usize> {
    let mut strides = vec![1; shape.len()];
    let mut stride = 1usize;
    for axis in (0..shape.len()).rev() {
        strides[axis] = stride;
        stride *= shape[axis];
    }
    strides
}

fn flat_index(idx: &[usize], strides: &[usize]) -> usize {
    idx.iter()
        .zip(strides.iter())
        .map(|(&index, &stride)| index * stride)
        .sum()
}

fn out_flat_index(full_idx: &[usize], out_axes: &[usize], out_strides: &[usize]) -> usize {
    out_axes
        .iter()
        .zip(out_strides.iter())
        .map(|(&axis, &stride)| full_idx[axis] * stride)
        .sum()
}

fn write_global_idx(full_idx: &mut [usize], chunk_origin: &[usize], local_idx: &[usize]) {
    for ((dst, &origin), &offset) in full_idx
        .iter_mut()
        .zip(chunk_origin.iter())
        .zip(local_idx.iter())
    {
        *dst = origin + offset;
    }
}

/// Compute a single statistic from a non-empty slice of accepted pixel values.
#[allow(dead_code)]
pub(crate) fn compute_statistic(values: &[f64], stat: Statistic) -> f64 {
    debug_assert!(!values.is_empty());

    if stat.is_order_stat() {
        let summary = compute_order_summary(values).expect("valid order-stat summary");
        return match stat {
            Statistic::Median => summary.median,
            Statistic::Q1 => summary.q1,
            Statistic::Q3 => summary.q3,
            Statistic::Quartile => summary.quartile,
            Statistic::MedAbsDevMed => summary.mad,
            _ => unreachable!("order-stat dispatch"),
        };
    }

    let mut accum = RunningStats::default();
    for &value in values {
        accum.push(value);
    }
    accum.finish(stat)
}

impl Statistic {
    fn is_order_stat(self) -> bool {
        matches!(
            self,
            Self::Median | Self::Q1 | Self::Q3 | Self::Quartile | Self::MedAbsDevMed
        )
    }
}

fn compute_order_summary(values: &[f64]) -> Result<OrderStatsSummary, LatticeError> {
    if values.iter().any(|value| value.is_nan()) {
        return Ok(legacy_order_summary(values));
    }

    let mut ordered = Array1::from_vec(values.iter().copied().map(OrderedFloat).collect());
    let quantiles = Array1::from_vec(vec![n64(0.25), n64(0.75)]);
    let quartiles = ordered
        .view_mut()
        .quantiles_mut(&quantiles.view(), &Lower)
        .map_err(|err| {
            LatticeError::InvalidTraversal(format!("quantile selection failed: {err}"))
        })?;
    let median = ordered
        .view_mut()
        .quantile_mut(n64(0.5), &Midpoint)
        .map_err(|err| {
            LatticeError::InvalidTraversal(format!("quantile selection failed: {err}"))
        })?;

    let mut deviations = Array1::from_vec(
        values
            .iter()
            .map(|value| OrderedFloat((value - median.0).abs()))
            .collect(),
    );
    let mad = deviations
        .view_mut()
        .quantile_mut(n64(0.5), &Midpoint)
        .map_err(|err| {
            LatticeError::InvalidTraversal(format!("quantile selection failed: {err}"))
        })?;

    let q1 = quartiles[0].0;
    let q3 = quartiles[1].0;
    Ok(OrderStatsSummary {
        median: median.0,
        q1,
        q3,
        quartile: q3 - q1,
        mad: mad.0,
    })
}

fn legacy_order_summary(values: &[f64]) -> OrderStatsSummary {
    let median = casacore_median(values);
    let q1 = casacore_fractile(values, 0.25);
    let q3 = casacore_fractile(values, 0.75);
    OrderStatsSummary {
        median,
        q1,
        q3,
        quartile: q3 - q1,
        mad: casacore_madfm(values),
    }
}

/// Median matching C++ `casacore::median(arr, takeEvenMean=true)`.
pub(crate) fn casacore_median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(nan_safe_cmp);
    let n = sorted.len();
    let n2 = (n - 1) / 2;
    if n % 2 == 0 {
        (sorted[n2] + sorted[n2 + 1]) / 2.0
    } else {
        sorted[n2]
    }
}

/// Fractile (percentile) matching C++ `casacore::fractile(arr, fraction)`.
pub(crate) fn casacore_fractile(values: &[f64], frac: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(nan_safe_cmp);
    let n = sorted.len();
    let n2 = ((n - 1) as f64 * frac + 0.01) as usize;
    sorted[n2.min(n - 1)]
}

/// Median absolute deviation from the median, matching C++ `casacore::madfm`.
pub(crate) fn casacore_madfm(values: &[f64]) -> f64 {
    let med = casacore_median(values);
    let mut devs: Vec<f64> = values.iter().map(|value| (value - med).abs()).collect();
    devs.sort_by(nan_safe_cmp);
    let n = devs.len();
    let n2 = (n - 1) / 2;
    if n % 2 == 0 {
        (devs[n2] + devs[n2 + 1]) / 2.0
    } else {
        devs[n2]
    }
}

/// NaN-safe comparator for f64 sorting (NaNs sort to the end).
fn nan_safe_cmp(a: &f64, b: &f64) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::{ArrayLattice, near_tol};

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
        stats.set_execution_policy(ExecutionPolicy::Pipelined { prefetch_depth: 2 });
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
            prefetch_depth: 4,
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
}
