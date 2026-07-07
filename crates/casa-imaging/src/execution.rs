// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal imaging execution plans and CPU workspaces.

#[cfg(all(target_os = "macos", not(coverage)))]
use std::env;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    ops::Range,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    ImageGeometry, ImagingError, StandardMfsExecutionConfig, StandardMfsObservabilityCallback,
    StandardMfsObservabilityEvent, StandardMfsPairCollapseTransform,
    StandardMfsPlannedWeightedSample, StandardMfsPlannedWeightedSampleRunBlock,
    StandardMfsQueueProgress, StandardMfsQueueProgressConfidence, StandardMfsRoutedVisibilityRow,
    StandardMfsRoutedVisibilityRun, StandardMfsStreamingWeightingPlan,
    StandardMfsVisibilityPolarization, VisibilityBatch,
    gridder::{
        PositiveTapSet, STANDARD_GRIDDER_SUPPORT, STANDARD_GRIDDER_TAP_COUNT, StandardGridder,
        StandardMfsTapCensus, StandardMfsTapSkipReason, TapAxisSpan,
    },
    profile,
    types::{StandardMfsRoutedSample, StandardMfsRoutedSampleRunBlock},
};
#[cfg(all(target_os = "macos", not(coverage)))]
use crate::{gridder::DensityCellConvention, weighting::StandardMfsStreamingReweightPlan};

/// Internal backend selection for standard MFS execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StandardMfsBackend {
    /// Synchronous CPU execution using the native Rust gridder.
    Cpu,
    /// Preview marker for the future macOS Metal standard-MFS backend.
    #[allow(dead_code)]
    Metal,
    /// Reserved marker for future backends that must fail before execution.
    #[allow(dead_code)]
    Reserved(&'static str),
}

fn unsupported_standard_mfs_backend(name: &str) -> ImagingError {
    ImagingError::Unsupported(format!("standard MFS backend '{name}' is not implemented"))
}

/// CPU-only executor for standard MFS imaging.
pub(crate) struct StandardMfsCpuExecutor<'a> {
    gridder: &'a StandardGridder,
    plan: StandardMfsVisibilityPlan,
    workspace: StandardMfsWorkspace,
}

/// CPU executor for streaming standard MFS dirty accumulation.
///
/// This owns the standard gridder and reusable grids while each call to
/// `accumulate_batches` builds only a borrowed plan for the current frontend
/// row block.
pub(crate) struct StandardMfsDirtyCpuExecutor {
    gridder: StandardGridder,
    workspace: StandardMfsWorkspace,
    normalization_sumwt: f64,
    reported_sumwt: f64,
    gridded_samples: usize,
    skipped_samples: usize,
    max_abs_w_lambda: f64,
}

/// Immutable summary of accumulated streaming standard MFS dirty samples.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct StandardMfsDirtyAccumulation {
    /// PSF and residual normalization sum from accepted samples.
    pub(crate) normalization_sumwt: f64,
    /// CASA-style reported sumwt from accepted samples.
    pub(crate) reported_sumwt: f64,
    /// Number of samples accepted by the standard gridder.
    pub(crate) gridded_samples: usize,
    /// Number of samples rejected by flags, weights, or gridder bounds.
    pub(crate) skipped_samples: usize,
    /// Maximum absolute `w` coordinate seen in wavelengths.
    pub(crate) max_abs_w_lambda: f64,
}

impl StandardMfsDirtyAccumulation {
    fn add(&mut self, other: Self) {
        self.normalization_sumwt += other.normalization_sumwt;
        self.reported_sumwt += other.reported_sumwt;
        self.gridded_samples += other.gridded_samples;
        self.skipped_samples += other.skipped_samples;
        self.max_abs_w_lambda = self.max_abs_w_lambda.max(other.max_abs_w_lambda);
    }
}

const STANDARD_MFS_TILE_BUCKET_PROBE_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_BUCKET_PROBE";
const STANDARD_MFS_TILE_EDGE_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_EDGE";
const STANDARD_MFS_TILE_ANCHOR_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_ANCHOR";
const STANDARD_MFS_TILE_FLUSH_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_FLUSH";
const STANDARD_MFS_TILE_RESIDENT_LIMIT_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT";
const STANDARD_MFS_GRID_THREADS_ENV: &str = "CASA_RS_STANDARD_MFS_GRID_THREADS";
const STANDARD_MFS_FORCE_TILED_ONE_WORKER_ENV: &str = "CASA_RS_STANDARD_MFS_FORCE_TILED_ONE_WORKER";
const DEFAULT_STANDARD_MFS_TILE_EDGE: usize = 256;
const STANDARD_MFS_TILE_QUEUE_INITIAL_RUN_CAP: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StandardMfsTileId(u32);

impl StandardMfsTileId {
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsTileExtent {
    pub(crate) x0: usize,
    pub(crate) x1: usize,
    pub(crate) y0: usize,
    pub(crate) y1: usize,
}

impl StandardMfsTileExtent {
    fn width(self) -> usize {
        self.x1.saturating_sub(self.x0)
    }

    fn height(self) -> usize {
        self.y1.saturating_sub(self.y0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsFixedTile {
    pub(crate) id: StandardMfsTileId,
    pub(crate) interior: StandardMfsTileExtent,
    pub(crate) halo: StandardMfsTileExtent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsFixedTilePartition {
    grid_shape: [usize; 2],
    tile_shape: [usize; 2],
    tile_origin: [usize; 2],
    anchor_label: &'static str,
    x_bounds: Vec<usize>,
    y_bounds: Vec<usize>,
    tiles_y: usize,
    tiles: Vec<StandardMfsFixedTile>,
}

impl StandardMfsFixedTilePartition {
    pub(crate) fn new(
        grid_shape: [usize; 2],
        tile_shape: [usize; 2],
        halo: usize,
    ) -> Result<Self, ImagingError> {
        Self::new_with_origin(grid_shape, tile_shape, halo, [0, 0], "zero")
    }

    pub(crate) fn new_center_boundary(
        gridder: &StandardGridder,
        tile_shape: [usize; 2],
        halo: usize,
    ) -> Result<Self, ImagingError> {
        let center = gridder.positive_tap_grid_center();
        Self::new_with_origin(
            gridder.grid_shape(),
            tile_shape,
            halo,
            [center[0] % tile_shape[0], center[1] % tile_shape[1]],
            "center_boundary",
        )
    }

    pub(crate) fn new_center_quadrants(
        gridder: &StandardGridder,
        halo: usize,
    ) -> Result<Self, ImagingError> {
        let grid_shape = gridder.grid_shape();
        let center = gridder.positive_tap_grid_center();
        let tile_shape = [
            center[0]
                .max(grid_shape[0].saturating_sub(center[0]))
                .max(1),
            center[1]
                .max(grid_shape[1].saturating_sub(center[1]))
                .max(1),
        ];
        Self::new_with_origin(
            grid_shape,
            tile_shape,
            halo,
            [
                center[0].min(grid_shape[0] - 1),
                center[1].min(grid_shape[1] - 1),
            ],
            "center_quadrants",
        )
    }

    pub(crate) fn halo_cell_count(&self) -> usize {
        self.tiles
            .iter()
            .map(|tile| tile.halo.width().saturating_mul(tile.halo.height()))
            .sum()
    }

    pub(crate) fn interior_cell_count(&self) -> usize {
        self.tiles
            .iter()
            .map(|tile| tile.interior.width().saturating_mul(tile.interior.height()))
            .sum()
    }

    pub(crate) fn new_with_origin(
        grid_shape: [usize; 2],
        tile_shape: [usize; 2],
        halo: usize,
        tile_origin: [usize; 2],
        anchor_label: &'static str,
    ) -> Result<Self, ImagingError> {
        if grid_shape[0] == 0 || grid_shape[1] == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile grid shape must be non-empty".to_string(),
            ));
        }
        if tile_shape[0] == 0 || tile_shape[1] == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile shape must be non-empty".to_string(),
            ));
        }
        if tile_origin[0] >= grid_shape[0] || tile_origin[1] >= grid_shape[1] {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile origin must be inside the grid".to_string(),
            ));
        }

        let x_bounds = tile_bounds_from_origin(grid_shape[0], tile_shape[0], tile_origin[0]);
        let y_bounds = tile_bounds_from_origin(grid_shape[1], tile_shape[1], tile_origin[1]);
        Self::new_with_axis_bounds(
            grid_shape,
            tile_shape,
            halo,
            tile_origin,
            anchor_label,
            x_bounds,
            y_bounds,
        )
    }

    fn new_with_axis_bounds(
        grid_shape: [usize; 2],
        tile_shape: [usize; 2],
        halo: usize,
        tile_origin: [usize; 2],
        anchor_label: &'static str,
        x_bounds: Vec<usize>,
        y_bounds: Vec<usize>,
    ) -> Result<Self, ImagingError> {
        validate_axis_bounds(grid_shape[0], &x_bounds, "x")?;
        validate_axis_bounds(grid_shape[1], &y_bounds, "y")?;
        let tiles_x = x_bounds.len() - 1;
        let tiles_y = y_bounds.len() - 1;
        let tile_count = tiles_x
            .checked_mul(tiles_y)
            .filter(|count| *count <= u32::MAX as usize)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS tile partition has too many tiles".to_string(),
                )
            })?;
        let mut tiles = Vec::with_capacity(tile_count);
        for tile_x in 0..tiles_x {
            let interior_x0 = x_bounds[tile_x];
            let interior_x1 = x_bounds[tile_x + 1];
            for tile_y in 0..tiles_y {
                let interior_y0 = y_bounds[tile_y];
                let interior_y1 = y_bounds[tile_y + 1];
                let id = StandardMfsTileId((tile_x * tiles_y + tile_y) as u32);
                let interior = StandardMfsTileExtent {
                    x0: interior_x0,
                    x1: interior_x1,
                    y0: interior_y0,
                    y1: interior_y1,
                };
                let halo_extent = StandardMfsTileExtent {
                    x0: interior_x0.saturating_sub(halo),
                    x1: (interior_x1 + halo).min(grid_shape[0]),
                    y0: interior_y0.saturating_sub(halo),
                    y1: (interior_y1 + halo).min(grid_shape[1]),
                };
                tiles.push(StandardMfsFixedTile {
                    id,
                    interior,
                    halo: halo_extent,
                });
            }
        }

        Ok(Self {
            grid_shape,
            tile_shape,
            tile_origin,
            anchor_label,
            x_bounds,
            y_bounds,
            tiles_y,
            tiles,
        })
    }

    pub(crate) fn tile_count(&self) -> usize {
        self.tiles.len()
    }

    pub(crate) fn tile(&self, id: StandardMfsTileId) -> Option<&StandardMfsFixedTile> {
        self.tiles.get(id.index())
    }

    pub(crate) fn tile_shape(&self) -> [usize; 2] {
        self.tile_shape
    }

    pub(crate) fn tile_origin(&self) -> [usize; 2] {
        self.tile_origin
    }

    pub(crate) fn anchor_label(&self) -> &'static str {
        self.anchor_label
    }

    pub(crate) fn owner(&self, center_x: usize, center_y: usize) -> Option<StandardMfsTileId> {
        if center_x >= self.grid_shape[0] || center_y >= self.grid_shape[1] {
            return None;
        }
        let tile_x = owner_from_bounds(center_x, &self.x_bounds)?;
        let tile_y = owner_from_bounds(center_y, &self.y_bounds)?;
        Some(StandardMfsTileId((tile_x * self.tiles_y + tile_y) as u32))
    }

    pub(crate) fn resident_tile_bytes(
        &self,
        id: StandardMfsTileId,
        stage_grid_count: usize,
    ) -> Option<usize> {
        let tile = self.tile(id)?;
        Some(
            tile.halo
                .width()
                .saturating_mul(tile.halo.height())
                .saturating_mul(stage_grid_count)
                .saturating_mul(std::mem::size_of::<Complex64>()),
        )
    }
}

fn validate_axis_bounds(grid_len: usize, bounds: &[usize], axis: &str) -> Result<(), ImagingError> {
    if bounds.len() < 2 || bounds.first() != Some(&0) || bounds.last() != Some(&grid_len) {
        return Err(ImagingError::InvalidRequest(format!(
            "standard MFS {axis}-tile bounds must span the full grid"
        )));
    }
    if bounds.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ImagingError::InvalidRequest(format!(
            "standard MFS {axis}-tile bounds must be strictly increasing"
        )));
    }
    Ok(())
}

fn owner_from_bounds(coord: usize, bounds: &[usize]) -> Option<usize> {
    if coord >= *bounds.last()? {
        return None;
    }
    match bounds.binary_search(&coord) {
        Ok(index) => (index < bounds.len() - 1).then_some(index),
        Err(index) => index.checked_sub(1),
    }
}

fn tile_bounds_from_origin(grid_len: usize, edge: usize, origin: usize) -> Vec<usize> {
    let tile_count = tile_count_1d(grid_len, edge, origin);
    let mut bounds = Vec::with_capacity(tile_count + 1);
    for tile_index in 0..tile_count {
        let (start, end) = tile_bounds_1d(tile_index, grid_len, edge, origin);
        if tile_index == 0 {
            bounds.push(start);
        }
        bounds.push(end);
    }
    bounds
}

fn tile_count_1d(grid_len: usize, edge: usize, origin: usize) -> usize {
    if grid_len == 0 {
        0
    } else if origin == 0 {
        grid_len.div_ceil(edge)
    } else if grid_len <= origin {
        1
    } else {
        1 + (grid_len - origin).div_ceil(edge)
    }
}

fn tile_bounds_1d(
    tile_index: usize,
    grid_len: usize,
    edge: usize,
    origin: usize,
) -> (usize, usize) {
    if origin == 0 {
        let start = tile_index * edge;
        (start, (start + edge).min(grid_len))
    } else if tile_index == 0 {
        (0, origin.min(grid_len))
    } else {
        let start = origin + (tile_index - 1) * edge;
        (start, (start + edge).min(grid_len))
    }
}

pub(crate) const STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY: u16 = 1 << 0;
pub(crate) const STANDARD_MFS_TILE_FLAG_PSF_ONLY: u16 = 1 << 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsSampleRef {
    pub(crate) batch_index: u32,
    pub(crate) sample_index: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StandardMfsSampleClassification {
    valid_for_density: bool,
    valid_for_psf: bool,
    valid_for_dirty_visibility: bool,
    valid_for_residual_visibility: bool,
    valid_geometry: bool,
    valid_weight: bool,
    finite_visibility: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StandardMfsRowBlockByteLedger {
    storage_bytes: usize,
    sample_ref_bytes: usize,
    bucket_sample_bytes: usize,
    tile_task_bytes: usize,
    tile_range_bytes: usize,
    scalar_record_bytes: usize,
    allocator_slop_bytes: usize,
}

#[allow(dead_code)]
impl StandardMfsRowBlockByteLedger {
    fn total_bytes(self) -> usize {
        self.storage_bytes
            .saturating_add(self.sample_ref_bytes)
            .saturating_add(self.bucket_sample_bytes)
            .saturating_add(self.tile_task_bytes)
            .saturating_add(self.tile_range_bytes)
            .saturating_add(self.scalar_record_bytes)
            .saturating_add(self.allocator_slop_bytes)
    }

    fn add(mut self, other: Self) -> Self {
        self.storage_bytes = self.storage_bytes.saturating_add(other.storage_bytes);
        self.sample_ref_bytes = self.sample_ref_bytes.saturating_add(other.sample_ref_bytes);
        self.bucket_sample_bytes = self
            .bucket_sample_bytes
            .saturating_add(other.bucket_sample_bytes);
        self.tile_task_bytes = self.tile_task_bytes.saturating_add(other.tile_task_bytes);
        self.tile_range_bytes = self.tile_range_bytes.saturating_add(other.tile_range_bytes);
        self.scalar_record_bytes = self
            .scalar_record_bytes
            .saturating_add(other.scalar_record_bytes);
        self.allocator_slop_bytes = self
            .allocator_slop_bytes
            .saturating_add(other.allocator_slop_bytes);
        self
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct StandardMfsRowBlockId(u64);

#[allow(dead_code)]
#[derive(Debug, Default)]
struct StandardMfsRowBlockIdAllocator {
    next: u64,
}

#[allow(dead_code)]
impl StandardMfsRowBlockIdAllocator {
    fn next(&mut self) -> StandardMfsRowBlockId {
        let id = StandardMfsRowBlockId(self.next);
        self.next = self.next.saturating_add(1);
        id
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
enum StandardMfsRowBlockStorage {
    BatchBacked(Vec<VisibilityBatch>),
}

#[allow(dead_code)]
impl StandardMfsRowBlockStorage {
    fn batches(&self) -> &[VisibilityBatch] {
        match self {
            Self::BatchBacked(batches) => batches,
        }
    }

    fn sample_count(&self) -> usize {
        self.batches().iter().map(VisibilityBatch::len).sum()
    }

    fn validate(&self) -> Result<(), ImagingError> {
        for batch in self.batches() {
            batch.validate()?;
        }
        Ok(())
    }

    fn byte_ledger(&self) -> StandardMfsRowBlockByteLedger {
        StandardMfsRowBlockByteLedger {
            storage_bytes: self
                .batches()
                .iter()
                .map(visibility_batch_capacity_bytes)
                .sum(),
            ..StandardMfsRowBlockByteLedger::default()
        }
    }
}

#[allow(dead_code)]
fn visibility_batch_capacity_bytes(batch: &VisibilityBatch) -> usize {
    batch
        .u_lambda
        .capacity()
        .saturating_mul(std::mem::size_of::<f64>())
        .saturating_add(
            batch
                .v_lambda
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            batch
                .w_lambda
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            batch
                .weight
                .capacity()
                .saturating_mul(std::mem::size_of::<f32>()),
        )
        .saturating_add(
            batch
                .sumwt_factor
                .capacity()
                .saturating_mul(std::mem::size_of::<f32>()),
        )
        .saturating_add(
            batch
                .gridable
                .capacity()
                .saturating_mul(std::mem::size_of::<bool>()),
        )
        .saturating_add(
            batch
                .visibility
                .capacity()
                .saturating_mul(std::mem::size_of::<Complex32>()),
        )
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default, PartialEq)]
struct StandardMfsTaskScalarRecord {
    dirty: StandardMfsDirtyAccumulation,
    residual: StandardMfsTiledResidualAccumulation,
}

#[allow(dead_code)]
impl StandardMfsTaskScalarRecord {
    fn byte_ledger(&self) -> StandardMfsRowBlockByteLedger {
        StandardMfsRowBlockByteLedger {
            scalar_record_bytes: std::mem::size_of_val(self),
            ..StandardMfsRowBlockByteLedger::default()
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
struct PreparedTileRowBlock {
    block_id: StandardMfsRowBlockId,
    storage: StandardMfsRowBlockStorage,
    buckets: StandardMfsBlockTileBuckets,
    scalar_record: StandardMfsTaskScalarRecord,
    byte_ledger: StandardMfsRowBlockByteLedger,
}

#[allow(dead_code)]
impl PreparedTileRowBlock {
    fn batch_backed(
        block_id: StandardMfsRowBlockId,
        batches: Vec<VisibilityBatch>,
        buckets: StandardMfsBlockTileBuckets,
    ) -> Result<Option<Self>, ImagingError> {
        if buckets.accepted_samples() == 0 {
            return Ok(None);
        }
        let storage = StandardMfsRowBlockStorage::BatchBacked(batches);
        storage.validate()?;
        if storage.sample_count() != buckets.row_sample_count() {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS prepared tile row block sample count mismatch: storage={} buckets={}",
                storage.sample_count(),
                buckets.row_sample_count()
            )));
        }
        let scalar_record = StandardMfsTaskScalarRecord::default();
        let byte_ledger = storage
            .byte_ledger()
            .add(buckets.byte_ledger())
            .add(scalar_record.byte_ledger());
        Ok(Some(Self {
            block_id,
            storage,
            buckets,
            scalar_record,
            byte_ledger,
        }))
    }

    fn byte_ledger(&self) -> StandardMfsRowBlockByteLedger {
        self.byte_ledger
    }
}

trait StandardMfsRowBlockSampleAccess {
    fn batches(&self) -> &[VisibilityBatch];
    fn sample_ref(&self, sample_id: u32) -> Result<StandardMfsSampleRef, ImagingError>;

    fn visibility(&self, sample_id: u32) -> Result<Complex32, ImagingError> {
        let sample_ref = self.sample_ref(sample_id)?;
        self.batches()
            .get(sample_ref.batch_index as usize)
            .and_then(|batch| batch.visibility.get(sample_ref.sample_index as usize))
            .copied()
            .ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS row-block sample id {} references missing visibility",
                    sample_id
                ))
            })
    }
}

struct BorrowedStandardMfsRowBlock<'a> {
    batches: &'a [VisibilityBatch],
    buckets: &'a StandardMfsBlockTileBuckets,
}

impl StandardMfsRowBlockSampleAccess for BorrowedStandardMfsRowBlock<'_> {
    fn batches(&self) -> &[VisibilityBatch] {
        self.batches
    }

    fn sample_ref(&self, sample_id: u32) -> Result<StandardMfsSampleRef, ImagingError> {
        self.buckets.sample_ref(sample_id)
    }
}

impl StandardMfsRowBlockSampleAccess for PreparedTileRowBlock {
    fn batches(&self) -> &[VisibilityBatch] {
        self.storage.batches()
    }

    fn sample_ref(&self, sample_id: u32) -> Result<StandardMfsSampleRef, ImagingError> {
        self.buckets.sample_ref(sample_id)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct StandardMfsTileBucketSample {
    pub(crate) sample_id: u32,
    pub(crate) center_x: u32,
    pub(crate) center_y: u32,
    pub(crate) kernel_u: u16,
    pub(crate) kernel_v: u16,
    pub(crate) support_id: u16,
    pub(crate) flags: u16,
    pub(crate) grid_weight: f32,
    pub(crate) tap_count: u8,
}

impl StandardMfsTileBucketSample {
    pub(crate) fn finite_visibility(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY != 0
    }

    pub(crate) fn psf_only(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_PSF_ONLY != 0
    }

    pub(crate) fn positive_taps(self) -> Result<PositiveTapSet, ImagingError> {
        if self.support_id != 0 {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS tile bucket sample has unsupported tap support id {}",
                self.support_id
            )));
        }
        let center_x = self.center_x as usize;
        let center_y = self.center_y as usize;
        let Some(x_start) = center_x.checked_sub(STANDARD_GRIDDER_SUPPORT) else {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile bucket sample has invalid x tap center".to_string(),
            ));
        };
        let Some(y_start) = center_y.checked_sub(STANDARD_GRIDDER_SUPPORT) else {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile bucket sample has invalid y tap center".to_string(),
            ));
        };
        Ok(PositiveTapSet {
            x: TapAxisSpan {
                start: x_start,
                weight_index: usize::from(self.kernel_u),
            },
            y: TapAxisSpan {
                start: y_start,
                weight_index: usize::from(self.kernel_v),
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StandardMfsBlockTileBuckets {
    sample_refs: Vec<StandardMfsSampleRef>,
    samples: Vec<StandardMfsTileBucketSample>,
    tile_offsets: Vec<u32>,
    nonempty_tiles: Vec<StandardMfsTileId>,
    accepted_samples: usize,
    skipped_samples: usize,
}

impl StandardMfsBlockTileBuckets {
    pub(crate) fn build_for_dirty(
        gridder: &StandardGridder,
        partition: &StandardMfsFixedTilePartition,
        batches: &[VisibilityBatch],
    ) -> Result<Self, ImagingError> {
        let total_samples = batches.iter().map(VisibilityBatch::len).sum::<usize>();
        if total_samples > u32::MAX as usize {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile bucket block has too many samples".to_string(),
            ));
        }
        for batch in batches {
            batch.validate()?;
        }

        let mut sample_refs = Vec::with_capacity(total_samples);
        for (batch_index, batch) in batches.iter().enumerate() {
            if batch.len() > u32::MAX as usize || batch_index > u32::MAX as usize {
                return Err(ImagingError::InvalidRequest(
                    "standard MFS tile bucket block has too many batch samples".to_string(),
                ));
            }
            for sample_index in 0..batch.len() {
                sample_refs.push(StandardMfsSampleRef {
                    batch_index: batch_index as u32,
                    sample_index: sample_index as u32,
                });
            }
        }

        let tile_count = partition.tile_count();
        let mut counts = vec![0usize; tile_count];
        let mut accepted_samples = 0usize;
        let mut skipped_samples = 0usize;

        for batch in batches {
            for sample_index in 0..batch.len() {
                let Some(planned) = plan_dirty_tile_sample(gridder, partition, batch, sample_index)
                else {
                    skipped_samples += 1;
                    continue;
                };
                counts[planned.tile_id.index()] += 1;
                accepted_samples += 1;
            }
        }

        let mut tile_offsets = Vec::with_capacity(tile_count + 1);
        tile_offsets.push(0);
        let mut running = 0usize;
        for count in &counts {
            running += *count;
            tile_offsets.push(running as u32);
        }
        let mut fill_offsets: Vec<usize> = tile_offsets[..tile_count]
            .iter()
            .map(|offset| *offset as usize)
            .collect();
        let mut samples = vec![
            StandardMfsTileBucketSample {
                sample_id: 0,
                center_x: 0,
                center_y: 0,
                kernel_u: 0,
                kernel_v: 0,
                support_id: 0,
                flags: 0,
                grid_weight: 0.0,
                tap_count: 0,
            };
            accepted_samples
        ];

        let mut flat_sample_index = 0usize;
        for batch in batches {
            for sample_index in 0..batch.len() {
                let Some(planned) = plan_dirty_tile_sample(gridder, partition, batch, sample_index)
                else {
                    flat_sample_index += 1;
                    continue;
                };
                let output_index = fill_offsets[planned.tile_id.index()];
                fill_offsets[planned.tile_id.index()] += 1;
                samples[output_index] = StandardMfsTileBucketSample {
                    sample_id: flat_sample_index as u32,
                    center_x: planned.center[0] as u32,
                    center_y: planned.center[1] as u32,
                    kernel_u: planned.kernel[0],
                    kernel_v: planned.kernel[1],
                    support_id: 0,
                    flags: planned.flags,
                    grid_weight: planned.grid_weight,
                    tap_count: planned.tap_count,
                };
                flat_sample_index += 1;
            }
        }

        let nonempty_tiles = counts
            .iter()
            .enumerate()
            .filter_map(|(index, count)| (*count > 0).then_some(StandardMfsTileId(index as u32)))
            .collect();

        Ok(Self {
            sample_refs,
            samples,
            tile_offsets,
            nonempty_tiles,
            accepted_samples,
            skipped_samples,
        })
    }

    pub(crate) fn build_for_residual_refresh(
        gridder: &StandardGridder,
        partition: &StandardMfsFixedTilePartition,
        batches: &[VisibilityBatch],
    ) -> Result<(Self, StandardMfsTiledResidualAccumulation), ImagingError> {
        let total_samples = batches.iter().map(VisibilityBatch::len).sum::<usize>();
        if total_samples > u32::MAX as usize {
            return Err(ImagingError::InvalidRequest(
                "standard MFS residual tile bucket block has too many samples".to_string(),
            ));
        }
        for batch in batches {
            batch.validate()?;
        }
        let mut sample_refs = Vec::with_capacity(total_samples);
        for (batch_index, batch) in batches.iter().enumerate() {
            if batch.len() > u32::MAX as usize || batch_index > u32::MAX as usize {
                return Err(ImagingError::InvalidRequest(
                    "standard MFS residual tile bucket block has too many batch samples"
                        .to_string(),
                ));
            }
            for sample_index in 0..batch.len() {
                sample_refs.push(StandardMfsSampleRef {
                    batch_index: batch_index as u32,
                    sample_index: sample_index as u32,
                });
            }
        }
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut per_tile = vec![Vec::<StandardMfsTileBucketSample>::new(); partition.tile_count()];
        let mut flat_sample_index = 0usize;
        for batch in batches {
            for sample_index in 0..batch.len() {
                let weight = batch.weight[sample_index];
                let observed_visibility = batch.visibility[sample_index];
                if !batch.gridable[sample_index] {
                    accumulation.skipped_not_gridable += 1;
                    flat_sample_index += 1;
                    continue;
                }
                if !(weight.is_finite() && weight > 0.0) {
                    accumulation.skipped_invalid_weight += 1;
                    flat_sample_index += 1;
                    continue;
                }
                if !finite_visibility(observed_visibility) {
                    accumulation.skipped_nonfinite_visibility += 1;
                    flat_sample_index += 1;
                    continue;
                }
                accumulation.valid_samples += 1;
                let Some(taps) = gridder
                    .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                else {
                    accumulation.skipped_out_of_grid += 1;
                    flat_sample_index += 1;
                    continue;
                };
                accumulation.planned_samples += 1;
                let sumwt_factor = batch.sumwt_factor[sample_index];
                if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                    accumulation.skipped_invalid_sumwt += 1;
                    flat_sample_index += 1;
                    continue;
                }
                let residual_weight = weight * sumwt_factor;
                if !(residual_weight.is_finite() && residual_weight > 0.0) {
                    accumulation.skipped_invalid_sumwt += 1;
                    flat_sample_index += 1;
                    continue;
                }
                let center = taps.center();
                let Some(tile_id) = partition.owner(center[0], center[1]) else {
                    accumulation.skipped_out_of_grid += 1;
                    flat_sample_index += 1;
                    continue;
                };
                let kernel_u = u16::try_from(taps.x.weight_index).map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS residual tile bucket tap weight index exceeds u16"
                            .to_string(),
                    )
                })?;
                let kernel_v = u16::try_from(taps.y.weight_index).map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS residual tile bucket tap weight index exceeds u16"
                            .to_string(),
                    )
                })?;
                per_tile[tile_id.index()].push(StandardMfsTileBucketSample {
                    sample_id: flat_sample_index as u32,
                    center_x: center[0] as u32,
                    center_y: center[1] as u32,
                    kernel_u,
                    kernel_v,
                    support_id: 0,
                    flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                    grid_weight: residual_weight,
                    tap_count: STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT)
                        as u8,
                });
                flat_sample_index += 1;
            }
        }

        let mut samples = Vec::<StandardMfsTileBucketSample>::new();
        let mut tile_offsets = Vec::with_capacity(partition.tile_count() + 1);
        let mut nonempty_tiles = Vec::<StandardMfsTileId>::new();
        tile_offsets.push(0);
        for (tile_index, mut tile_samples) in per_tile.into_iter().enumerate() {
            if !tile_samples.is_empty() {
                nonempty_tiles.push(StandardMfsTileId(tile_index as u32));
            }
            samples.append(&mut tile_samples);
            tile_offsets.push(samples.len() as u32);
        }
        let accepted_samples = samples.len();
        let skipped_samples = accumulation
            .skipped_not_gridable
            .saturating_add(accumulation.skipped_invalid_weight)
            .saturating_add(accumulation.skipped_invalid_sumwt)
            .saturating_add(accumulation.skipped_out_of_grid)
            .saturating_add(accumulation.skipped_nonfinite_visibility);
        Ok((
            Self {
                sample_refs,
                samples,
                tile_offsets,
                nonempty_tiles,
                accepted_samples,
                skipped_samples,
            },
            accumulation,
        ))
    }

    pub(crate) fn samples(&self) -> &[StandardMfsTileBucketSample] {
        &self.samples
    }

    pub(crate) fn sample_ref(&self, sample_id: u32) -> Result<StandardMfsSampleRef, ImagingError> {
        self.sample_refs
            .get(sample_id as usize)
            .copied()
            .ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile bucket sample id {} is out of row-block range",
                    sample_id
                ))
            })
    }

    pub(crate) fn accepted_samples(&self) -> usize {
        self.accepted_samples
    }

    pub(crate) fn skipped_samples(&self) -> usize {
        self.skipped_samples
    }

    #[allow(dead_code)]
    fn row_sample_count(&self) -> usize {
        self.sample_refs.len()
    }

    pub(crate) fn nonempty_tiles(&self) -> &[StandardMfsTileId] {
        &self.nonempty_tiles
    }

    pub(crate) fn tile_samples(&self, id: StandardMfsTileId) -> &[StandardMfsTileBucketSample] {
        let index = id.index();
        let start = self.tile_offsets[index] as usize;
        let end = self.tile_offsets[index + 1] as usize;
        &self.samples[start..end]
    }

    pub(crate) fn tile_tasks_descending(&self) -> Vec<StandardMfsTileTask> {
        let mut tasks = self
            .nonempty_tiles
            .iter()
            .map(|&tile_id| {
                let samples = self.tile_samples(tile_id);
                let estimated_tap_visits = samples
                    .iter()
                    .map(|sample| usize::from(sample.tap_count))
                    .sum::<usize>();
                StandardMfsTileTask {
                    tile_id,
                    sample_count: samples.len(),
                    estimated_tap_visits,
                }
            })
            .collect::<Vec<_>>();
        tasks.sort_unstable_by(|lhs, rhs| {
            rhs.estimated_tap_visits
                .cmp(&lhs.estimated_tap_visits)
                .then_with(|| rhs.sample_count.cmp(&lhs.sample_count))
                .then_with(|| lhs.tile_id.cmp(&rhs.tile_id))
        });
        tasks
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.byte_ledger().total_bytes()
    }

    fn byte_ledger(&self) -> StandardMfsRowBlockByteLedger {
        StandardMfsRowBlockByteLedger {
            storage_bytes: 0,
            sample_ref_bytes: self
                .sample_refs
                .capacity()
                .saturating_mul(std::mem::size_of::<StandardMfsSampleRef>()),
            bucket_sample_bytes: self
                .samples
                .capacity()
                .saturating_mul(std::mem::size_of::<StandardMfsTileBucketSample>()),
            tile_task_bytes: 0,
            tile_range_bytes: self
                .tile_offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>())
                .saturating_add(
                    self.nonempty_tiles
                        .capacity()
                        .saturating_mul(std::mem::size_of::<StandardMfsTileId>()),
                ),
            scalar_record_bytes: 0,
            allocator_slop_bytes: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsTileTaskDesc {
    pub(crate) tile_id: StandardMfsTileId,
    pub(crate) sample_count: usize,
    pub(crate) estimated_tap_visits: usize,
}

pub(crate) type StandardMfsTileTask = StandardMfsTileTaskDesc;

#[derive(Clone, Copy, Debug, PartialEq)]
struct PlannedDirtyTileSample {
    tile_id: StandardMfsTileId,
    center: [usize; 2],
    kernel: [u16; 2],
    flags: u16,
    grid_weight: f32,
    tap_count: u8,
}

fn plan_dirty_tile_sample(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batch: &VisibilityBatch,
    sample_index: usize,
) -> Option<PlannedDirtyTileSample> {
    let classification = classify_standard_mfs_sample(batch, sample_index);
    if !classification.valid_for_psf {
        return None;
    }
    let taps =
        gridder.plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])?;
    let center = taps.center();
    let tile_id = partition.owner(center[0], center[1])?;
    let weight = batch.weight[sample_index];
    let sumwt_factor = batch.sumwt_factor[sample_index];
    let grid_weight = weight * sumwt_factor;
    if !(grid_weight.is_finite() && grid_weight > 0.0) {
        return None;
    }
    let flags = if finite_visibility(batch.visibility[sample_index]) {
        STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
    } else {
        STANDARD_MFS_TILE_FLAG_PSF_ONLY
    };
    let kernel_u = u16::try_from(taps.x.weight_index).ok()?;
    let kernel_v = u16::try_from(taps.y.weight_index).ok()?;
    Some(PlannedDirtyTileSample {
        tile_id,
        center,
        kernel: [kernel_u, kernel_v],
        flags,
        grid_weight,
        tap_count: STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT) as u8,
    })
}

fn classify_standard_mfs_sample(
    batch: &VisibilityBatch,
    sample_index: usize,
) -> StandardMfsSampleClassification {
    let gridable = batch.gridable[sample_index];
    let weight = batch.weight[sample_index];
    let sumwt_factor = batch.sumwt_factor[sample_index];
    let valid_weight =
        weight.is_finite() && weight > 0.0 && sumwt_factor.is_finite() && sumwt_factor > 0.0;
    let finite_visibility = finite_visibility(batch.visibility[sample_index]);
    let valid_geometry = gridable
        && batch.u_lambda[sample_index].is_finite()
        && batch.v_lambda[sample_index].is_finite()
        && batch.w_lambda[sample_index].is_finite();
    let valid_for_psf = valid_geometry && valid_weight;
    StandardMfsSampleClassification {
        valid_for_density: valid_for_psf,
        valid_for_psf,
        valid_for_dirty_visibility: valid_for_psf && finite_visibility,
        valid_for_residual_visibility: valid_for_psf && finite_visibility,
        valid_geometry,
        valid_weight,
        finite_visibility,
    }
}

#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StandardMfsTileSampleRouteMode {
    DensityNoData,
    PsfNoData,
    DirtyWithData,
    ResidualWithData,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StandardMfsTileSampleRouteSkip {
    NotGridable,
    InvalidWeight,
    InvalidSumwt,
    NonfiniteVisibility,
    OutOfGrid,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq)]
enum StandardMfsTileSampleRouteDecision {
    Density(StandardMfsTileId),
    Enqueue(StandardMfsTileId, StandardMfsTileQueueSample),
    Skip(StandardMfsTileSampleRouteSkip),
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StandardMfsSampleLocation {
    tile_id: StandardMfsTileId,
    center: [usize; 2],
}

#[allow(dead_code)]
struct StandardMfsTileSampleRouter<'a> {
    gridder: &'a StandardGridder,
    partition: &'a StandardMfsFixedTilePartition,
    mode: StandardMfsTileSampleRouteMode,
}

#[allow(dead_code)]
impl<'a> StandardMfsTileSampleRouter<'a> {
    fn new(
        gridder: &'a StandardGridder,
        partition: &'a StandardMfsFixedTilePartition,
        mode: StandardMfsTileSampleRouteMode,
    ) -> Self {
        Self {
            gridder,
            partition,
            mode,
        }
    }

    fn route_batch_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
        input_seq: u64,
    ) -> Result<StandardMfsTileSampleRouteDecision, ImagingError> {
        match self.mode {
            StandardMfsTileSampleRouteMode::DensityNoData => {
                self.route_density_sample(batch, sample_index)
            }
            StandardMfsTileSampleRouteMode::PsfNoData => {
                self.route_psf_sample(batch, sample_index, input_seq)
            }
            StandardMfsTileSampleRouteMode::DirtyWithData => {
                self.route_dirty_sample(batch, sample_index, input_seq)
            }
            StandardMfsTileSampleRouteMode::ResidualWithData => {
                self.route_residual_sample(batch, sample_index, input_seq)
            }
        }
    }

    fn locate_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
    ) -> Result<Option<StandardMfsSampleLocation>, ImagingError> {
        let Some(center) = self
            .gridder
            .locate_positive_tap_center(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
        else {
            return Ok(None);
        };
        let Some(tile_id) = self.partition.owner(center[0], center[1]) else {
            return Ok(None);
        };
        Ok(Some(StandardMfsSampleLocation { tile_id, center }))
    }

    fn route_density_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
    ) -> Result<StandardMfsTileSampleRouteDecision, ImagingError> {
        let classification = classify_standard_mfs_sample(batch, sample_index);
        if !classification.valid_geometry {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::NotGridable,
            ));
        }
        if !classification.valid_weight {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidWeight,
            ));
        }
        let Some(location) = self.locate_sample(batch, sample_index)? else {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::OutOfGrid,
            ));
        };
        Ok(StandardMfsTileSampleRouteDecision::Density(
            location.tile_id,
        ))
    }

    fn route_psf_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
        input_seq: u64,
    ) -> Result<StandardMfsTileSampleRouteDecision, ImagingError> {
        let classification = classify_standard_mfs_sample(batch, sample_index);
        if !classification.valid_geometry {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::NotGridable,
            ));
        }
        if !classification.valid_weight {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidWeight,
            ));
        }
        let Some(location) = self.locate_sample(batch, sample_index)? else {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::OutOfGrid,
            ));
        };
        Ok(StandardMfsTileSampleRouteDecision::Enqueue(
            location.tile_id,
            StandardMfsTileQueueSample {
                center_x: location.center[0] as u32,
                center_y: location.center[1] as u32,
                flags: STANDARD_MFS_TILE_FLAG_PSF_ONLY,
                raw_weight: batch.weight[sample_index],
                sumwt_factor: batch.sumwt_factor[sample_index],
                u_lambda: batch.u_lambda[sample_index],
                v_lambda: batch.v_lambda[sample_index],
                w_lambda: batch.w_lambda[sample_index],
                visibility: Complex32::new(0.0, 0.0),
                input_seq,
            },
        ))
    }

    fn route_dirty_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
        input_seq: u64,
    ) -> Result<StandardMfsTileSampleRouteDecision, ImagingError> {
        let classification = classify_standard_mfs_sample(batch, sample_index);
        if !classification.valid_geometry {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::NotGridable,
            ));
        }
        if !classification.valid_weight {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidWeight,
            ));
        }
        let Some(location) = self.locate_sample(batch, sample_index)? else {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::OutOfGrid,
            ));
        };
        let flags = if classification.finite_visibility {
            STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
        } else {
            STANDARD_MFS_TILE_FLAG_PSF_ONLY
        };
        Ok(StandardMfsTileSampleRouteDecision::Enqueue(
            location.tile_id,
            StandardMfsTileQueueSample {
                center_x: location.center[0] as u32,
                center_y: location.center[1] as u32,
                flags,
                raw_weight: batch.weight[sample_index],
                sumwt_factor: batch.sumwt_factor[sample_index],
                u_lambda: batch.u_lambda[sample_index],
                v_lambda: batch.v_lambda[sample_index],
                w_lambda: batch.w_lambda[sample_index],
                visibility: batch.visibility[sample_index],
                input_seq,
            },
        ))
    }

    fn route_residual_sample(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
        input_seq: u64,
    ) -> Result<StandardMfsTileSampleRouteDecision, ImagingError> {
        if !batch.gridable[sample_index] {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::NotGridable,
            ));
        }
        let weight = batch.weight[sample_index];
        if !(weight.is_finite() && weight > 0.0) {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidWeight,
            ));
        }
        let observed_visibility = batch.visibility[sample_index];
        if !finite_visibility(observed_visibility) {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::NonfiniteVisibility,
            ));
        }
        let Some(location) = self.locate_sample(batch, sample_index)? else {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::OutOfGrid,
            ));
        };
        let sumwt_factor = batch.sumwt_factor[sample_index];
        if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidSumwt,
            ));
        }
        let residual_weight = weight * sumwt_factor;
        if !(residual_weight.is_finite() && residual_weight > 0.0) {
            return Ok(StandardMfsTileSampleRouteDecision::Skip(
                StandardMfsTileSampleRouteSkip::InvalidSumwt,
            ));
        }
        Ok(StandardMfsTileSampleRouteDecision::Enqueue(
            location.tile_id,
            StandardMfsTileQueueSample {
                center_x: location.center[0] as u32,
                center_y: location.center[1] as u32,
                flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                raw_weight: weight,
                sumwt_factor,
                u_lambda: batch.u_lambda[sample_index],
                v_lambda: batch.v_lambda[sample_index],
                w_lambda: batch.w_lambda[sample_index],
                visibility: observed_visibility,
                input_seq,
            },
        ))
    }
}

fn maybe_probe_standard_mfs_tile_buckets(
    gridder: &StandardGridder,
    batches: &[VisibilityBatch],
) -> Result<(), ImagingError> {
    if !standard_mfs_tile_bucket_probe_enabled() {
        return Ok(());
    }
    let partition = standard_mfs_tile_partition_for_gridder(gridder)?;
    probe_standard_mfs_tile_buckets_with_partition(gridder, &partition, batches)
}

fn probe_standard_mfs_tile_buckets_with_partition(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batches: &[VisibilityBatch],
) -> Result<(), ImagingError> {
    if !standard_mfs_tile_bucket_probe_enabled() {
        return Ok(());
    }

    let grid_shape = gridder.grid_shape();
    let tile_shape = partition.tile_shape();
    let tile_origin = partition.tile_origin();
    let halo = gridder.positive_tap_halo();
    let buckets = StandardMfsBlockTileBuckets::build_for_dirty(gridder, partition, batches)?;

    let mut finite_visibility_samples = 0usize;
    let mut psf_only_samples = 0usize;
    for sample in buckets.samples() {
        if sample.finite_visibility() {
            finite_visibility_samples += 1;
        }
        if sample.psf_only() {
            psf_only_samples += 1;
        }
    }

    let mut all_tile_counts = vec![0usize; partition.tile_count()];
    let mut max_bucket_samples = 0usize;
    let mut resident_bytes_if_all_nonempty = 0usize;
    let mut interior_cells_if_all_nonempty = 0usize;
    for &tile_id in buckets.nonempty_tiles() {
        let bucket_samples = buckets.tile_samples(tile_id).len();
        all_tile_counts[tile_id.index()] = bucket_samples;
        max_bucket_samples = max_bucket_samples.max(bucket_samples);
        resident_bytes_if_all_nonempty = resident_bytes_if_all_nonempty
            .saturating_add(partition.resident_tile_bytes(tile_id, 2).unwrap_or(0));
        if let Some(tile) = partition.tile(tile_id) {
            debug_assert_eq!(tile.id, tile_id);
            interior_cells_if_all_nonempty = interior_cells_if_all_nonempty
                .saturating_add(tile.interior.width().saturating_mul(tile.interior.height()));
        }
    }
    let touched_tile_counts = all_tile_counts
        .iter()
        .copied()
        .filter(|count| *count > 0)
        .collect::<Vec<_>>();
    let all_distribution = tile_bucket_distribution_stats(&all_tile_counts);
    let touched_distribution = tile_bucket_distribution_stats(&touched_tile_counts);
    let top_tile_counts = top_tile_bucket_counts(&all_tile_counts, 8);
    let per_block_summary = per_row_block_tile_probe_summary(gridder, partition, batches)?;
    let near_origin_summary = near_origin_tile_probe_summary(gridder, partition, batches);

    eprintln!(
        "standard_mfs_tile_bucket_probe \
         grid_shape={}x{} \
         tile_shape={}x{} \
         tile_anchor={} \
         tile_origin={}x{} \
         gridder_center={}x{} \
         halo={} \
         tiles={} \
         accepted_samples={} \
         skipped_samples={} \
         finite_visibility_samples={} \
         psf_only_samples={} \
         bucket_bytes={} \
         nonempty_tiles={} \
         max_bucket_samples={} \
         all_tile_mean={:.3} \
         all_tile_p50={} \
         all_tile_p90={} \
         all_tile_p99={} \
         all_tile_zero_fraction={:.6} \
         all_tile_max_over_mean={:.3} \
         all_tile_gini={:.6} \
         touched_tile_mean={:.3} \
         touched_tile_p50={} \
         touched_tile_p90={} \
         touched_tile_p99={} \
         touched_tile_max_over_mean={:.3} \
         touched_tile_gini={:.6} \
         top_tile_counts={} \
         per_row_block={} \
         near_origin={} \
         interior_cells_if_all_nonempty={} \
         resident_bytes_if_all_nonempty={}",
        grid_shape[0],
        grid_shape[1],
        tile_shape[0],
        tile_shape[1],
        partition.anchor_label(),
        tile_origin[0],
        tile_origin[1],
        gridder.positive_tap_grid_center()[0],
        gridder.positive_tap_grid_center()[1],
        halo,
        partition.tile_count(),
        buckets.accepted_samples(),
        buckets.skipped_samples(),
        finite_visibility_samples,
        psf_only_samples,
        buckets.estimated_bytes(),
        buckets.nonempty_tiles().len(),
        max_bucket_samples,
        all_distribution.mean,
        all_distribution.p50,
        all_distribution.p90,
        all_distribution.p99,
        all_distribution.zero_fraction,
        all_distribution.max_over_mean,
        all_distribution.gini,
        touched_distribution.mean,
        touched_distribution.p50,
        touched_distribution.p90,
        touched_distribution.p99,
        touched_distribution.max_over_mean,
        touched_distribution.gini,
        top_tile_counts,
        per_block_summary,
        near_origin_summary,
        interior_cells_if_all_nonempty,
        resident_bytes_if_all_nonempty
    );

    Ok(())
}

fn per_row_block_tile_probe_summary(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batches: &[VisibilityBatch],
) -> Result<String, ImagingError> {
    if batches.is_empty() {
        return Ok("blocks=0".to_string());
    }
    let mut nonempty_tiles = Vec::with_capacity(batches.len());
    let mut hottest_share_bps = Vec::with_capacity(batches.len());
    let mut top4_share_bps = Vec::with_capacity(batches.len());
    let mut top8_share_bps = Vec::with_capacity(batches.len());
    let mut task_counts = Vec::with_capacity(batches.len());
    let mut largest_task_samples = Vec::with_capacity(batches.len());
    let mut largest_task_tap_visits = Vec::with_capacity(batches.len());

    for batch in batches {
        let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
            gridder,
            partition,
            std::slice::from_ref(batch),
        )?;
        let accepted = buckets.accepted_samples().max(1);
        let mut counts = buckets
            .nonempty_tiles()
            .iter()
            .map(|&tile_id| buckets.tile_samples(tile_id).len())
            .collect::<Vec<_>>();
        counts.sort_unstable_by(|lhs, rhs| rhs.cmp(lhs));
        nonempty_tiles.push(counts.len());
        task_counts.push(counts.len());
        largest_task_samples.push(counts.first().copied().unwrap_or(0));
        hottest_share_bps
            .push(counts.first().copied().unwrap_or(0).saturating_mul(10_000) / accepted);
        top4_share_bps.push(counts.iter().take(4).sum::<usize>().saturating_mul(10_000) / accepted);
        top8_share_bps.push(counts.iter().take(8).sum::<usize>().saturating_mul(10_000) / accepted);
        largest_task_tap_visits.push(
            buckets
                .tile_tasks_descending()
                .first()
                .map(|task| task.estimated_tap_visits)
                .unwrap_or(0),
        );
    }

    Ok(format!(
        "blocks={},nonempty_tiles={},hottest_bps={},top4_bps={},top8_bps={},task_count={},largest_task_samples={},largest_task_tap_visits={}",
        batches.len(),
        stats_triplet(&nonempty_tiles),
        stats_triplet(&hottest_share_bps),
        stats_triplet(&top4_share_bps),
        stats_triplet(&top8_share_bps),
        stats_triplet(&task_counts),
        stats_triplet(&largest_task_samples),
        stats_triplet(&largest_task_tap_visits),
    ))
}

fn near_origin_tile_probe_summary(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batches: &[VisibilityBatch],
) -> String {
    let center = gridder.positive_tap_grid_center();
    let mut window_counts = [0usize; 25];
    let mut quadrant_counts = [0usize; 4];
    let mut quadrant_owner_counts = BTreeMap::<(usize, usize), usize>::new();

    for batch in batches {
        for sample_index in 0..batch.len() {
            let Some(planned) = plan_dirty_tile_sample(gridder, partition, batch, sample_index)
            else {
                continue;
            };
            let tile_id = planned.tile_id;
            let sample_center = planned.center;
            let dx = sample_center[0] as isize - center[0] as isize;
            let dy = sample_center[1] as isize - center[1] as isize;
            if !((-2..=2).contains(&dx) && (-2..=2).contains(&dy)) {
                continue;
            }
            let window_index = ((dx + 2) as usize) * 5 + (dy + 2) as usize;
            window_counts[window_index] += 1;
            let quadrant = match (
                batch.u_lambda[sample_index].is_sign_negative(),
                batch.v_lambda[sample_index].is_sign_negative(),
            ) {
                (false, false) => 0,
                (true, false) => 1,
                (true, true) => 2,
                (false, true) => 3,
            };
            quadrant_counts[quadrant] += 1;
            *quadrant_owner_counts
                .entry((quadrant, tile_id.index()))
                .or_insert(0) += 1;
        }
    }

    let mut window = Vec::new();
    for dx in -2isize..=2 {
        for dy in -2isize..=2 {
            let count = window_counts[((dx + 2) as usize) * 5 + (dy + 2) as usize];
            if count > 0 {
                window.push(format!("{dx}:{dy}:{count}"));
            }
        }
    }
    let mut owner_counts = quadrant_owner_counts
        .into_iter()
        .map(|((quadrant, tile), count)| (quadrant, tile, count))
        .collect::<Vec<_>>();
    owner_counts.sort_unstable_by(|lhs, rhs| {
        rhs.2
            .cmp(&lhs.2)
            .then_with(|| lhs.0.cmp(&rhs.0))
            .then_with(|| lhs.1.cmp(&rhs.1))
    });
    let owners = owner_counts
        .into_iter()
        .take(12)
        .map(|(quadrant, tile, count)| format!("q{quadrant}:t{tile}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "center={}x{},window={},quadrants={},{},{},{},owners={}",
        center[0],
        center[1],
        window.join(","),
        quadrant_counts[0],
        quadrant_counts[1],
        quadrant_counts[2],
        quadrant_counts[3],
        owners,
    )
}

#[derive(Clone, Copy, Debug, Default)]
struct TileBucketDistributionStats {
    mean: f64,
    p50: usize,
    p90: usize,
    p99: usize,
    zero_fraction: f64,
    max_over_mean: f64,
    gini: f64,
}

fn tile_bucket_distribution_stats(counts: &[usize]) -> TileBucketDistributionStats {
    if counts.is_empty() {
        return TileBucketDistributionStats::default();
    }
    let mut sorted = counts.to_vec();
    sorted.sort_unstable();
    let total = sorted.iter().sum::<usize>();
    let mean = total as f64 / sorted.len() as f64;
    let max = sorted.last().copied().unwrap_or(0);
    let zero_count = sorted.iter().take_while(|count| **count == 0).count();
    TileBucketDistributionStats {
        mean,
        p50: percentile_sorted_usize(&sorted, 0.50),
        p90: percentile_sorted_usize(&sorted, 0.90),
        p99: percentile_sorted_usize(&sorted, 0.99),
        zero_fraction: zero_count as f64 / sorted.len() as f64,
        max_over_mean: if mean > 0.0 { max as f64 / mean } else { 0.0 },
        gini: gini_sorted_usize(&sorted, total),
    }
}

fn percentile_sorted_usize(sorted: &[usize], percentile: f64) -> usize {
    debug_assert!(!sorted.is_empty());
    let rank = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn stats_triplet(values: &[usize]) -> String {
    if values.is_empty() {
        return "p50:0,p90:0,p99:0,max:0".to_string();
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    format!(
        "p50:{},p90:{},p99:{},max:{}",
        percentile_sorted_usize(&sorted, 0.50),
        percentile_sorted_usize(&sorted, 0.90),
        percentile_sorted_usize(&sorted, 0.99),
        sorted.last().copied().unwrap_or(0)
    )
}

fn percentile_sorted_f64(sorted: &[f64], percentile: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let rank = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn f64_stats_triplet(values: &[f64], unit: &str) -> String {
    if values.is_empty() {
        return format!("p50_{unit}:0.000,p90_{unit}:0.000,p99_{unit}:0.000,max_{unit}:0.000");
    }
    let mut sorted = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return format!("p50_{unit}:0.000,p90_{unit}:0.000,p99_{unit}:0.000,max_{unit}:0.000");
    }
    sorted.sort_by(|left, right| left.total_cmp(right));
    format!(
        "p50_{unit}:{:.3},p90_{unit}:{:.3},p99_{unit}:{:.3},max_{unit}:{:.3}",
        percentile_sorted_f64(&sorted, 0.50),
        percentile_sorted_f64(&sorted, 0.90),
        percentile_sorted_f64(&sorted, 0.99),
        sorted.last().copied().unwrap_or(0.0)
    )
}

fn percentile_sorted_duration(sorted: &[Duration], percentile: f64) -> Duration {
    debug_assert!(!sorted.is_empty());
    let rank = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn duration_stats_triplet(values: &[Duration]) -> String {
    if values.is_empty() {
        return "p50_ms:0.000,p90_ms:0.000,p99_ms:0.000,max_ms:0.000".to_string();
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    format!(
        "p50_ms:{:.3},p90_ms:{:.3},p99_ms:{:.3},max_ms:{:.3}",
        profile::millis(percentile_sorted_duration(&sorted, 0.50)),
        profile::millis(percentile_sorted_duration(&sorted, 0.90)),
        profile::millis(percentile_sorted_duration(&sorted, 0.99)),
        profile::millis(sorted.last().copied().unwrap_or(Duration::ZERO))
    )
}

fn duration_total_ms(values: &[Duration]) -> f64 {
    profile::millis(
        values
            .iter()
            .copied()
            .fold(Duration::ZERO, |total, value| total + value),
    )
}

fn percent_or_zero(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator * 100.0
    } else {
        0.0
    }
}

fn ratio_or_zero(numerator: usize, denominator: usize) -> f64 {
    if denominator > 0 {
        numerator as f64 / denominator as f64
    } else {
        0.0
    }
}

fn duration_option_ms(value: Option<Duration>) -> f64 {
    value.map(profile::millis).unwrap_or(0.0)
}

fn top_tile_profile_counts<F>(
    profiles: &[StandardMfsTileInboxTileProfile],
    limit: usize,
    mut value: F,
) -> String
where
    F: FnMut(&StandardMfsTileInboxTileProfile) -> usize,
{
    let mut keyed = profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| (value(profile), index))
        .collect::<Vec<_>>();
    keyed.sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    keyed
        .into_iter()
        .take(limit)
        .map(|(count, index)| format!("{index}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn top_tile_profile_durations<F>(
    profiles: &[StandardMfsTileInboxTileProfile],
    limit: usize,
    mut value: F,
) -> String
where
    F: FnMut(&StandardMfsTileInboxTileProfile) -> Duration,
{
    let mut keyed = profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| (value(profile), index))
        .collect::<Vec<_>>();
    keyed.sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    keyed
        .into_iter()
        .take(limit)
        .map(|(duration, index)| format!("{index}:{:.3}", profile::millis(duration)))
        .collect::<Vec<_>>()
        .join(",")
}

fn top_tile_profile_optional_durations<F>(
    profiles: &[StandardMfsTileInboxTileProfile],
    limit: usize,
    mut value: F,
) -> String
where
    F: FnMut(&StandardMfsTileInboxTileProfile) -> Option<Duration>,
{
    let mut keyed = profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| (duration_option_ms(value(profile)), index))
        .collect::<Vec<_>>();
    keyed.sort_unstable_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(CmpOrdering::Equal)
            .then_with(|| left.1.cmp(&right.1))
    });
    keyed
        .into_iter()
        .take(limit)
        .map(|(duration_ms, index)| format!("{index}:{duration_ms:.3}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn per_second_or_zero(count: usize, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    }
}

fn gini_sorted_usize(sorted: &[usize], total: usize) -> f64 {
    if sorted.is_empty() || total == 0 {
        return 0.0;
    }
    let weighted_sum = sorted
        .iter()
        .enumerate()
        .map(|(index, count)| (index + 1) as f64 * *count as f64)
        .sum::<f64>();
    (2.0 * weighted_sum) / (sorted.len() as f64 * total as f64)
        - (sorted.len() as f64 + 1.0) / sorted.len() as f64
}

fn top_tile_bucket_counts(counts: &[usize], limit: usize) -> String {
    let mut ranked = counts
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, count)| *count > 0)
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|lhs, rhs| rhs.1.cmp(&lhs.1).then_with(|| lhs.0.cmp(&rhs.0)));
    ranked
        .into_iter()
        .take(limit)
        .map(|(tile_index, count)| format!("{tile_index}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn standard_mfs_tile_bucket_probe_enabled() -> bool {
    std::env::var(STANDARD_MFS_TILE_BUCKET_PROBE_ENV)
        .map(|value| {
            let value = value.trim();
            !(value.is_empty()
                || value == "0"
                || value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off"))
        })
        .unwrap_or(false)
}

fn standard_mfs_tile_edge_with_config(config_edge: Option<usize>) -> usize {
    std::env::var(STANDARD_MFS_TILE_EDGE_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|edge| *edge > 0)
        .or(config_edge.filter(|edge| *edge > 0))
        .unwrap_or(DEFAULT_STANDARD_MFS_TILE_EDGE)
}

fn standard_mfs_tile_partition_for_gridder(
    gridder: &StandardGridder,
) -> Result<StandardMfsFixedTilePartition, ImagingError> {
    standard_mfs_tile_partition_for_gridder_with_config(
        gridder,
        StandardMfsExecutionConfig::default(),
    )
}

fn standard_mfs_tile_partition_for_gridder_with_config(
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
) -> Result<StandardMfsFixedTilePartition, ImagingError> {
    let grid_shape = gridder.grid_shape();
    let tile_edge = standard_mfs_tile_edge_with_config(execution_config.fixed_tile_edge)
        .min(grid_shape[0].max(grid_shape[1]));
    let tile_shape = [tile_edge, tile_edge];
    let halo = gridder.positive_tap_halo();
    if let Ok(anchor) = std::env::var(STANDARD_MFS_TILE_ANCHOR_ENV) {
        match anchor.trim().to_ascii_lowercase().as_str() {
            "zero" | "grid_zero" | "origin" => {
                return StandardMfsFixedTilePartition::new(grid_shape, tile_shape, halo);
            }
            "center_boundary" | "center-boundary" | "center" => {
                return StandardMfsFixedTilePartition::new_center_boundary(
                    gridder, tile_shape, halo,
                );
            }
            "center_quadrants" | "center-quadrants" | "center_quadrant" | "center-quadrant"
            | "quadrants" | "quadrant" | "four" | "4" => {
                return StandardMfsFixedTilePartition::new_center_quadrants(gridder, halo);
            }
            other => {
                return Err(ImagingError::InvalidRequest(format!(
                    "unsupported standard MFS tile anchor '{other}'"
                )));
            }
        }
    }
    if execution_config.fixed_tile_center_boundary {
        StandardMfsFixedTilePartition::new_center_boundary(gridder, tile_shape, halo)
    } else {
        StandardMfsFixedTilePartition::new(grid_shape, tile_shape, halo)
    }
}

fn standard_mfs_tile_resident_limit(
    partition: &StandardMfsFixedTilePartition,
    resident_bytes: Option<usize>,
) -> usize {
    let tile_count = partition.tile_count();
    if let Some(limit) = std::env::var(STANDARD_MFS_TILE_RESIDENT_LIMIT_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|limit| *limit > 0)
    {
        return limit.min(tile_count).max(1);
    }
    resident_bytes
        .and_then(|bytes| {
            let max_tile_bytes = partition
                .tiles
                .iter()
                .filter_map(|tile| partition.resident_tile_bytes(tile.id, 2))
                .max()?;
            Some((bytes / max_tile_bytes).max(1))
        })
        .unwrap_or(tile_count)
        .min(tile_count)
        .max(1)
}

fn standard_mfs_grid_threads() -> usize {
    std::env::var(STANDARD_MFS_GRID_THREADS_ENV)
        .ok()
        .and_then(|value| {
            let value = value.trim();
            if value.eq_ignore_ascii_case("auto") {
                Some(std::thread::available_parallelism().map_or(1, |value| value.get()))
            } else {
                value.parse::<usize>().ok()
            }
        })
        .filter(|threads| *threads > 0)
        .unwrap_or(1)
}

fn standard_mfs_force_tiled_one_worker() -> bool {
    std::env::var(STANDARD_MFS_FORCE_TILED_ONE_WORKER_ENV)
        .map(|value| {
            let value = value.trim();
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

fn standard_mfs_per_block_flush_enabled() -> bool {
    std::env::var(STANDARD_MFS_TILE_FLUSH_ENV)
        .map(|value| {
            let value = value.trim();
            value.eq_ignore_ascii_case("per_block") || value.eq_ignore_ascii_case("per-block")
        })
        .unwrap_or(false)
}

/// CPU executor for bounded fixed-tile standard MFS gridding.
///
/// This executor routes only the current caller-supplied block into compact
/// tile buckets and never owns a stage-scope visibility/tap plan.
pub(crate) struct StandardMfsTiledCpuExecutor<'a> {
    gridder: &'a StandardGridder,
    partition: StandardMfsFixedTilePartition,
    resident_tile_limit: usize,
    observability_callback: Option<StandardMfsObservabilityCallback>,
}

/// Immutable summary of tiled standard-MFS residual refresh samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StandardMfsTiledResidualAccumulation {
    /// Samples with finite visibility, positive weight, and gridable input.
    pub(crate) valid_samples: usize,
    /// Valid samples that also planned positive taps.
    pub(crate) planned_samples: usize,
    /// Planned samples with finite positive `sumwt_factor` gridded into residual.
    pub(crate) gridded_residual_samples: usize,
    /// Samples skipped because the frontend marked them not gridable.
    pub(crate) skipped_not_gridable: usize,
    /// Samples skipped because their weight was not finite positive.
    pub(crate) skipped_invalid_weight: usize,
    /// Planned samples skipped because `sumwt_factor` was not finite positive.
    pub(crate) skipped_invalid_sumwt: usize,
    /// Valid samples skipped because their taps fell outside the grid.
    pub(crate) skipped_out_of_grid: usize,
    /// Samples skipped because the observed visibility was not finite.
    pub(crate) skipped_nonfinite_visibility: usize,
}

impl StandardMfsTiledResidualAccumulation {
    fn add_residual(&mut self, other: Self) {
        self.valid_samples += other.valid_samples;
        self.planned_samples += other.planned_samples;
        self.gridded_residual_samples += other.gridded_residual_samples;
        self.skipped_not_gridable += other.skipped_not_gridable;
        self.skipped_invalid_weight += other.skipped_invalid_weight;
        self.skipped_invalid_sumwt += other.skipped_invalid_sumwt;
        self.skipped_out_of_grid += other.skipped_out_of_grid;
        self.skipped_nonfinite_visibility += other.skipped_nonfinite_visibility;
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StandardMfsTileTaskTiming {
    local_alloc_zero: Duration,
    worker_replan_grid: Duration,
}

impl StandardMfsTileTaskTiming {
    fn add(&mut self, other: Self) {
        self.local_alloc_zero += other.local_alloc_zero;
        self.worker_replan_grid += other.worker_replan_grid;
    }

    fn active(self) -> Duration {
        self.local_alloc_zero + self.worker_replan_grid
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StandardMfsTileWorkerProfile {
    task_count: usize,
    sample_count: usize,
    tap_visits: usize,
    active: Duration,
    elapsed: Duration,
}

impl StandardMfsTileWorkerProfile {
    fn record_task(&mut self, task: StandardMfsTileTask, timing: StandardMfsTileTaskTiming) {
        self.task_count += 1;
        self.sample_count += task.sample_count;
        self.tap_visits += task.estimated_tap_visits;
        self.active += timing.active();
    }

    fn finish(&mut self, started_at: Instant) {
        self.elapsed = started_at.elapsed();
    }
}

const STANDARD_MFS_QUEUE_SAMPLE_SLOP_BYTES: usize = 16;
const STANDARD_MFS_QUEUE_RUN_SLOP_BYTES: usize = 64;
const STANDARD_MFS_INBOX_READY_SAMPLE_MIN_DEFAULT: usize = 1024;

fn standard_mfs_tile_inbox_ready_sample_min() -> usize {
    std::env::var("CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(STANDARD_MFS_INBOX_READY_SAMPLE_MIN_DEFAULT)
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq)]
struct StandardMfsTileQueueSample {
    center_x: u32,
    center_y: u32,
    flags: u16,
    raw_weight: f32,
    sumwt_factor: f32,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    visibility: Complex32,
    input_seq: u64,
}

impl StandardMfsTileQueueSample {
    #[allow(dead_code)]
    #[inline]
    fn from_routed(sample: StandardMfsRoutedSample, psf_only: bool, input_seq: u64) -> Self {
        let flags = if psf_only {
            STANDARD_MFS_TILE_FLAG_PSF_ONLY
        } else if sample.finite_visibility() {
            STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
        } else {
            STANDARD_MFS_TILE_FLAG_PSF_ONLY
        };
        Self {
            center_x: sample.center_x,
            center_y: sample.center_y,
            flags,
            raw_weight: sample.natural_weight,
            sumwt_factor: sample.sumwt_factor,
            u_lambda: sample.u_lambda,
            v_lambda: sample.v_lambda,
            w_lambda: sample.w_lambda,
            visibility: if psf_only {
                Complex32::new(0.0, 0.0)
            } else {
                sample.visibility
            },
            input_seq,
        }
    }

    #[allow(dead_code)]
    #[inline]
    fn finite_visibility(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY != 0
    }

    #[allow(dead_code)]
    #[inline]
    fn psf_only(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_PSF_ONLY != 0
    }

    #[allow(dead_code)]
    #[inline]
    fn grid_weight(self) -> f32 {
        self.raw_weight * self.sumwt_factor
    }

    fn queue_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(STANDARD_MFS_QUEUE_SAMPLE_SLOP_BYTES)
    }

    #[inline]
    fn estimated_work(self) -> usize {
        usize::from(self.tap_count())
    }

    #[inline]
    fn tap_count(self) -> u8 {
        STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT) as u8
    }

    #[inline]
    fn positive_taps(self, gridder: &StandardGridder) -> Result<PositiveTapSet, ImagingError> {
        let taps = gridder
            .plan_positive_taps(self.u_lambda, self.v_lambda)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox sample no longer maps to positive taps".to_string(),
                )
            })?;
        if taps.center() != [self.center_x as usize, self.center_y as usize] {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile inbox sample center changed during worker tap planning"
                    .to_string(),
            ));
        }
        Ok(taps)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StandardMfsRoutedQueueVisibility {
    Finite(Complex32),
    PsfOnly,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct StandardMfsRoutedQueueSample {
    center_x: u32,
    center_y: u32,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    natural_weight: f32,
    sumwt_factor: f32,
    visibility: StandardMfsRoutedQueueVisibility,
}

impl StandardMfsRoutedQueueSample {
    fn weighted_grid_weight(
        self,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<Option<f32>, ImagingError> {
        if !(self.natural_weight.is_finite()
            && self.natural_weight > 0.0
            && self.sumwt_factor.is_finite()
            && self.sumwt_factor > 0.0)
        {
            return Ok(None);
        }
        let weight =
            weighting_plan.weight_sample(self.u_lambda, self.v_lambda, self.natural_weight)?;
        let grid_weight = weight * self.sumwt_factor;
        if grid_weight.is_finite() && grid_weight > 0.0 {
            Ok(Some(grid_weight))
        } else {
            Ok(None)
        }
    }

    fn positive_taps(self, gridder: &StandardGridder) -> Result<PositiveTapSet, ImagingError> {
        let taps = gridder
            .plan_positive_taps(self.u_lambda, self.v_lambda)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox routed row sample no longer maps to positive taps"
                        .to_string(),
                )
            })?;
        if taps.center() != [self.center_x as usize, self.center_y as usize] {
            return Err(ImagingError::InvalidRequest(
                "standard MFS tile inbox routed row sample center changed during worker tap planning"
                    .to_string(),
            ));
        }
        Ok(taps)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsTileVisibilityRun {
    row: Option<Arc<StandardMfsRoutedVisibilityRow>>,
    source_slot_range: Range<usize>,
    selected_correlations: Arc<[usize]>,
    tap_centers: Arc<[[u32; 2]]>,
    tap_center_range: Range<usize>,
    first_input_seq: u64,
    samples: Vec<StandardMfsTileQueueSample>,
    bytes: usize,
    estimated_work: usize,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct StandardMfsTileQueueChunk {
    runs: Vec<StandardMfsTileVisibilityRun>,
    len: usize,
    estimated_work: usize,
}

impl StandardMfsTileQueueChunk {
    fn with_run_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity),
            len: 0,
            estimated_work: 0,
        }
    }

    fn push_run(&mut self, run: StandardMfsTileVisibilityRun) {
        if run.is_empty() {
            return;
        }
        self.len = self.len.saturating_add(run.len());
        self.estimated_work = self.estimated_work.saturating_add(run.estimated_work);
        self.runs.push(run);
    }

    fn runs(&self) -> &[StandardMfsTileVisibilityRun] {
        &self.runs
    }

    fn len(&self) -> usize {
        self.len
    }

    fn estimated_work(&self) -> usize {
        self.estimated_work
    }

    fn first_input_seq(&self) -> u64 {
        self.runs
            .first()
            .map(|run| run.first_input_seq)
            .unwrap_or(u64::MAX)
    }
}

impl StandardMfsTileVisibilityRun {
    fn empty() -> Self {
        Self::with_capacity(0, u64::MAX)
    }

    fn with_capacity(capacity: usize, first_input_seq: u64) -> Self {
        Self {
            row: None,
            source_slot_range: 0..0,
            selected_correlations: Arc::from([]),
            tap_centers: Arc::from([]),
            tap_center_range: 0..0,
            first_input_seq,
            samples: Vec::with_capacity(capacity),
            bytes: 0,
            estimated_work: 0,
        }
    }

    fn from_routed_visibility_run(
        run: &StandardMfsRoutedVisibilityRun,
        local_range: Range<usize>,
        first_input_seq: u64,
    ) -> Self {
        let source_slot_range = run.source_slot_range.start + local_range.start
            ..run.source_slot_range.start + local_range.end;
        let tap_center_range = local_range;
        let len = source_slot_range
            .end
            .saturating_sub(source_slot_range.start);
        let bytes = std::mem::size_of::<Self>().saturating_add(STANDARD_MFS_QUEUE_RUN_SLOP_BYTES);
        Self {
            row: Some(Arc::clone(&run.row)),
            source_slot_range,
            selected_correlations: Arc::from([]),
            tap_centers: Arc::clone(&run.tap_centers),
            tap_center_range,
            first_input_seq,
            samples: Vec::new(),
            bytes,
            estimated_work: len
                .saturating_mul(STANDARD_GRIDDER_TAP_COUNT)
                .saturating_mul(STANDARD_GRIDDER_TAP_COUNT),
        }
    }

    fn push_sample(&mut self, sample: StandardMfsTileQueueSample) {
        if self.is_empty() {
            self.first_input_seq = sample.input_seq;
        }
        let estimated_work = sample.estimated_work();
        self.samples.push(sample);
        self.bytes = self
            .bytes
            .saturating_add(StandardMfsTileQueueSample::queue_bytes());
        self.estimated_work = self.estimated_work.saturating_add(estimated_work);
    }

    fn append_run(&mut self, mut run: StandardMfsTileVisibilityRun) {
        if run.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = run;
            return;
        }
        if self.row.is_some() || run.row.is_some() {
            return;
        }
        self.samples.append(&mut run.samples);
        self.bytes = self.bytes.saturating_add(run.bytes);
        self.estimated_work = self.estimated_work.saturating_add(run.estimated_work);
    }

    fn can_append_run(&self, run: &StandardMfsTileVisibilityRun) -> bool {
        self.row.is_none() && run.row.is_none()
    }

    fn queue_bytes(&self) -> usize {
        self.bytes.saturating_add(STANDARD_MFS_QUEUE_RUN_SLOP_BYTES)
    }

    fn len(&self) -> usize {
        if self.row.is_some() {
            self.source_slot_range
                .end
                .saturating_sub(self.source_slot_range.start)
        } else {
            self.samples.len()
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn finite_visibility_at(&self, sample_index: usize) -> bool {
        self.samples[sample_index].finite_visibility()
    }

    #[inline]
    fn psf_only_at(&self, sample_index: usize) -> bool {
        self.samples[sample_index].psf_only()
    }

    #[inline]
    fn grid_weight_at(&self, sample_index: usize) -> f32 {
        self.samples[sample_index].grid_weight()
    }

    #[inline]
    fn visibility_at(&self, sample_index: usize) -> Complex32 {
        self.samples[sample_index].visibility
    }

    #[inline]
    fn positive_taps_at(
        &self,
        sample_index: usize,
        gridder: &StandardGridder,
    ) -> Result<PositiveTapSet, ImagingError> {
        self.samples[sample_index].positive_taps(gridder)
    }

    fn routed_queue_sample_at(
        &self,
        sample_index: usize,
        allow_psf_only: bool,
    ) -> Result<Option<StandardMfsRoutedQueueSample>, ImagingError> {
        if self.row.is_none() {
            let sample = self.samples[sample_index];
            let visibility = if sample.finite_visibility() {
                StandardMfsRoutedQueueVisibility::Finite(sample.visibility)
            } else if allow_psf_only && sample.psf_only() {
                StandardMfsRoutedQueueVisibility::PsfOnly
            } else {
                return Ok(None);
            };
            return Ok(Some(StandardMfsRoutedQueueSample {
                center_x: sample.center_x,
                center_y: sample.center_y,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                natural_weight: sample.raw_weight,
                sumwt_factor: sample.sumwt_factor,
                visibility,
            }));
        }

        let row = self.row.as_ref().expect("checked above");
        if !row.gridable {
            return Ok(None);
        }
        let source_slot = self
            .source_slot_range
            .start
            .checked_add(sample_index)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS routed row sample index overflowed".to_string(),
                )
            })?;
        let source_channel = *row.source_channel_indices.get(source_slot).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS routed row source slot {source_slot} is out of bounds"
            ))
        })?;
        let local_channel = source_channel
            .checked_sub(row.channel_origin)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS routed row source channel {source_channel} precedes loaded channel origin {}",
                    row.channel_origin
                ))
            })?;
        let lambda_scale = *row.channel_lambda_scales.get(source_slot).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS routed row lambda scale slot {source_slot} is out of bounds"
            ))
        })?;
        let tap_center_index = self
            .tap_center_range
            .start
            .checked_add(sample_index)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS routed row tap-center index overflowed".to_string(),
                )
            })?;
        let center = *self.tap_centers.get(tap_center_index).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS routed row tap center {tap_center_index} is out of bounds"
            ))
        })?;
        let (visibility, natural_weight, sumwt_factor) = match row.polarization {
            StandardMfsVisibilityPolarization::Explicit {
                corr_index,
                sumwt_factor,
            } => {
                if *row.flag.get((corr_index, local_channel)).ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "standard MFS routed FLAG index [{corr_index}, {source_channel}] is out of bounds"
                    ))
                })? {
                    return Ok(None);
                }
                let visibility = *row.data.get((corr_index, local_channel)).ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "standard MFS routed DATA index [{corr_index}, {source_channel}] is out of bounds"
                    ))
                })?;
                let visibility = if visibility.re.is_finite() && visibility.im.is_finite() {
                    StandardMfsRoutedQueueVisibility::Finite(visibility)
                } else if allow_psf_only {
                    StandardMfsRoutedQueueVisibility::PsfOnly
                } else {
                    return Ok(None);
                };
                let natural_weight = if let Some(weight_spectrum) = &row.weight_spectrum {
                    *weight_spectrum
                        .get((corr_index, local_channel))
                        .ok_or_else(|| {
                            ImagingError::InvalidRequest(format!(
                                "standard MFS routed WEIGHT_SPECTRUM index [{corr_index}, {source_channel}] is out of bounds"
                            ))
                        })?
                } else {
                    *row.weight.get(corr_index).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS routed WEIGHT correlation {corr_index} is out of bounds"
                        ))
                    })?
                };
                (visibility, natural_weight, sumwt_factor)
            }
            StandardMfsVisibilityPolarization::CollapsedPair {
                first_corr_index,
                second_corr_index,
                transform,
                sumwt_factor,
            } => {
                let first_flagged =
                    *row.flag
                        .get((first_corr_index, local_channel))
                        .ok_or_else(|| {
                            ImagingError::InvalidRequest(format!(
                                "standard MFS routed FLAG index [{first_corr_index}, {source_channel}] is out of bounds"
                            ))
                        })?;
                let second_flagged =
                    *row.flag
                        .get((second_corr_index, local_channel))
                        .ok_or_else(|| {
                            ImagingError::InvalidRequest(format!(
                                "standard MFS routed FLAG index [{second_corr_index}, {source_channel}] is out of bounds"
                            ))
                        })?;
                if first_flagged || second_flagged {
                    return Ok(None);
                }
                let first_visibility =
                    *row.data
                        .get((first_corr_index, local_channel))
                        .ok_or_else(|| {
                            ImagingError::InvalidRequest(format!(
                                "standard MFS routed DATA index [{first_corr_index}, {source_channel}] is out of bounds"
                            ))
                        })?;
                let second_visibility =
                    *row.data
                        .get((second_corr_index, local_channel))
                        .ok_or_else(|| {
                            ImagingError::InvalidRequest(format!(
                                "standard MFS routed DATA index [{second_corr_index}, {source_channel}] is out of bounds"
                            ))
                        })?;
                let visibility = collapse_standard_mfs_pair_visibility(
                    first_visibility,
                    second_visibility,
                    transform,
                );
                if !(visibility.re.is_finite() && visibility.im.is_finite()) {
                    return Ok(None);
                }
                let (first_weight, second_weight) = if let Some(weight_spectrum) =
                    &row.weight_spectrum
                {
                    (
                            *weight_spectrum
                                .get((first_corr_index, local_channel))
                                .ok_or_else(|| {
                                    ImagingError::InvalidRequest(format!(
                                        "standard MFS routed WEIGHT_SPECTRUM index [{first_corr_index}, {source_channel}] is out of bounds"
                                    ))
                                })?,
                            *weight_spectrum
                                .get((second_corr_index, local_channel))
                                .ok_or_else(|| {
                                    ImagingError::InvalidRequest(format!(
                                        "standard MFS routed WEIGHT_SPECTRUM index [{second_corr_index}, {source_channel}] is out of bounds"
                                    ))
                                })?,
                        )
                } else {
                    (
                            *row.weight.get(first_corr_index).ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS routed WEIGHT correlation {first_corr_index} is out of bounds"
                                ))
                            })?,
                            *row.weight.get(second_corr_index).ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS routed WEIGHT correlation {second_corr_index} is out of bounds"
                                ))
                            })?,
                        )
                };
                if !(first_weight.is_finite()
                    && first_weight > 0.0
                    && second_weight.is_finite()
                    && second_weight > 0.0)
                {
                    return Ok(None);
                }
                (
                    StandardMfsRoutedQueueVisibility::Finite(visibility),
                    0.5 * (first_weight + second_weight),
                    sumwt_factor,
                )
            }
        };
        Ok(Some(StandardMfsRoutedQueueSample {
            center_x: center[0],
            center_y: center[1],
            u_lambda: row.uvw_m[0] * lambda_scale,
            v_lambda: row.uvw_m[1] * lambda_scale,
            w_lambda: row.uvw_m[2] * lambda_scale,
            natural_weight,
            sumwt_factor,
            visibility,
        }))
    }
}

fn collapse_standard_mfs_pair_visibility(
    first_visibility: Complex32,
    second_visibility: Complex32,
    transform: StandardMfsPairCollapseTransform,
) -> Complex32 {
    match transform {
        StandardMfsPairCollapseTransform::HalfSum => (first_visibility + second_visibility) * 0.5,
        StandardMfsPairCollapseTransform::HalfDifference => {
            (first_visibility - second_visibility) * 0.5
        }
        StandardMfsPairCollapseTransform::PositiveHalfImagDifference => {
            (first_visibility - second_visibility) * Complex32::new(0.0, 0.5)
        }
        StandardMfsPairCollapseTransform::NegativeHalfImagDifference => {
            (first_visibility - second_visibility) * Complex32::new(0.0, -0.5)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsTileInboxQueueState {
    runs: VecDeque<StandardMfsTileVisibilityRun>,
    active: bool,
    ready_enqueued: bool,
    generation: u64,
    queued_samples: usize,
    queued_bytes: usize,
    queued_work_estimate: usize,
    profile: StandardMfsTileInboxTileProfile,
}

impl StandardMfsTileInboxQueueState {
    fn new() -> Self {
        Self {
            runs: VecDeque::new(),
            active: false,
            ready_enqueued: false,
            generation: 0,
            queued_samples: 0,
            queued_bytes: 0,
            queued_work_estimate: 0,
            profile: StandardMfsTileInboxTileProfile::default(),
        }
    }

    fn ready_head(
        &mut self,
        tile_id: StandardMfsTileId,
        started_at: Instant,
    ) -> Option<StandardMfsTileInboxReadyHead> {
        let head = self.runs.front().map(|run| StandardMfsTileInboxReadyHead {
            tile_id,
            generation: self.generation,
            first_input_seq: run.first_input_seq,
            estimated_work: self.queued_work_estimate,
        });
        if head.is_some() {
            self.profile.ready_heads += 1;
            let elapsed = started_at.elapsed();
            self.profile.first_ready.get_or_insert(elapsed);
        }
        head
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct StandardMfsTileInboxTileProfile {
    enqueued_runs: usize,
    enqueued_samples: usize,
    enqueued_bytes: usize,
    enqueued_tap_visits: usize,
    ready_heads: usize,
    drains: usize,
    worker_runs: usize,
    worker_samples: usize,
    worker_tap_visits: usize,
    worker_active: Duration,
    first_ready: Option<Duration>,
    last_finish: Option<Duration>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsTileInboxRuntime {
    tile_id: StandardMfsTileId,
    queue: Mutex<StandardMfsTileInboxQueueState>,
}

impl StandardMfsTileInboxRuntime {
    fn new(tile_id: StandardMfsTileId) -> Self {
        Self {
            tile_id,
            queue: Mutex::new(StandardMfsTileInboxQueueState::new()),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StandardMfsTileInboxReadyHead {
    tile_id: StandardMfsTileId,
    generation: u64,
    first_input_seq: u64,
    estimated_work: usize,
}

impl PartialOrd for StandardMfsTileInboxReadyHead {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for StandardMfsTileInboxReadyHead {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.estimated_work
            .cmp(&other.estimated_work)
            .then_with(|| other.first_input_seq.cmp(&self.first_input_seq))
            .then_with(|| other.tile_id.cmp(&self.tile_id))
            .then_with(|| other.generation.cmp(&self.generation))
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct StandardMfsTileInboxSchedulerStats {
    ready_sample_min: usize,
    enqueued_runs: usize,
    enqueued_samples: usize,
    enqueued_bytes: usize,
    ready_deferred_runs: usize,
    ready_deferred_samples: usize,
    pending_runs: usize,
    pending_bytes: usize,
    pending_bytes_high_water: usize,
    try_lock_misses: usize,
    current_queued_bytes: usize,
    queued_bytes_high_water: usize,
    ready_heads_pushed: usize,
    worker_drains: usize,
    worker_runs: usize,
    worker_samples: usize,
    worker_tap_visits: usize,
    active_tile_skips: usize,
    stale_heap_entries: usize,
    wait_with_queued_bytes_events: usize,
    wait_with_producer_active_events: usize,
    producer_active: Duration,
    producer_worker_overlap: Duration,
    worker_active_union: Duration,
    neither_active: Duration,
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsTileInboxReadyState {
    heap: BinaryHeap<StandardMfsTileInboxReadyHead>,
    active_tasks: usize,
    closed: bool,
    error: Option<String>,
    producer_active: bool,
    started_at: Instant,
    last_activity_update: Instant,
    stats: StandardMfsTileInboxSchedulerStats,
}

impl StandardMfsTileInboxReadyState {
    fn new(ready_sample_min: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            active_tasks: 0,
            closed: false,
            error: None,
            producer_active: false,
            started_at: Instant::now(),
            last_activity_update: Instant::now(),
            stats: StandardMfsTileInboxSchedulerStats {
                ready_sample_min,
                ..Default::default()
            },
        }
    }

    fn update_activity(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_activity_update);
        if self.producer_active {
            self.stats.producer_active += elapsed;
        }
        if self.active_tasks > 0 {
            self.stats.worker_active_union += elapsed;
        }
        if self.producer_active && self.active_tasks > 0 {
            self.stats.producer_worker_overlap += elapsed;
        }
        if !self.producer_active && self.active_tasks == 0 {
            self.stats.neither_active += elapsed;
        }
        self.last_activity_update = now;
    }

    fn set_producer_active(&mut self, active: bool) {
        self.update_activity(Instant::now());
        self.producer_active = active;
    }

    fn push_ready_head(&mut self, head: StandardMfsTileInboxReadyHead) {
        self.heap.push(head);
        self.stats.ready_heads_pushed += 1;
    }
}

#[allow(dead_code)]
struct StandardMfsDrainedTileWork {
    tile_id: StandardMfsTileId,
    first_input_seq: u64,
    samples: StandardMfsTileQueueChunk,
    run_count: usize,
    estimated_work: usize,
    bytes: usize,
}

impl StandardMfsDrainedTileWork {
    fn task(&self) -> StandardMfsTileTask {
        StandardMfsTileTask {
            tile_id: self.tile_id,
            sample_count: self.samples.len(),
            estimated_tap_visits: self.estimated_work,
        }
    }
}

#[allow(dead_code)]
struct StandardMfsTileInboxTaskOutput<T> {
    tile_id: StandardMfsTileId,
    first_input_seq: u64,
    output: T,
    timing: StandardMfsTileTaskTiming,
}

#[allow(dead_code)]
struct StandardMfsTileInboxSchedulerOutput<T> {
    task_outputs: Vec<StandardMfsTileInboxTaskOutput<T>>,
    worker_profiles: Vec<StandardMfsTileWorkerProfile>,
    tile_profiles: Vec<StandardMfsTileInboxTileProfile>,
    stats: StandardMfsTileInboxSchedulerStats,
}

#[derive(Debug, Default)]
struct StandardMfsTileInboxPublishStats {
    runs: usize,
    samples: usize,
    bytes: usize,
    estimated_work: usize,
    ready_head: Option<StandardMfsTileInboxReadyHead>,
}

impl StandardMfsTileInboxPublishStats {
    fn add_deferred(&mut self, other: Self) {
        debug_assert!(other.ready_head.is_none());
        self.runs = self.runs.saturating_add(other.runs);
        self.samples = self.samples.saturating_add(other.samples);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.estimated_work = self.estimated_work.saturating_add(other.estimated_work);
    }
}

#[allow(dead_code)]
struct StandardMfsTileInboxShared {
    tiles: Vec<Arc<StandardMfsTileInboxRuntime>>,
    ready: Arc<(Mutex<StandardMfsTileInboxReadyState>, Condvar)>,
    ready_sample_min: usize,
    started_at: Instant,
    stage: &'static str,
    worker_capacity: usize,
    observability_callback: Option<StandardMfsObservabilityCallback>,
}

impl StandardMfsTileInboxShared {
    fn new(
        tile_count: usize,
        ready_sample_min: usize,
        stage: &'static str,
        worker_capacity: usize,
        observability_callback: Option<StandardMfsObservabilityCallback>,
    ) -> Self {
        let started_at = Instant::now();
        Self {
            tiles: (0..tile_count)
                .map(|index| {
                    Arc::new(StandardMfsTileInboxRuntime::new(StandardMfsTileId(
                        index as u32,
                    )))
                })
                .collect(),
            ready: Arc::new((
                Mutex::new(StandardMfsTileInboxReadyState::new(ready_sample_min)),
                Condvar::new(),
            )),
            ready_sample_min,
            started_at,
            stage,
            worker_capacity,
            observability_callback,
        }
    }

    fn emit_observability(&self) {
        let Some(callback) = self.observability_callback.as_ref() else {
            return;
        };
        let (ready_lock, _) = &*self.ready;
        let Ok(ready) = ready_lock.lock() else {
            return;
        };
        let heap_len = ready.heap.len();
        let active_tasks = ready.active_tasks;
        let producer_active = ready.producer_active;
        let current_queued_bytes = ready.stats.current_queued_bytes;
        let queued_bytes_high_water = ready.stats.queued_bytes_high_water;
        let blocked_count = ready
            .stats
            .wait_with_queued_bytes_events
            .saturating_add(ready.stats.wait_with_producer_active_events);
        drop(ready);

        let mut queued_runs = 0usize;
        let mut queued_bytes = 0usize;
        let mut active_tile_queues = 0usize;
        for tile in &self.tiles {
            let Ok(queue) = tile.queue.lock() else {
                return;
            };
            queued_runs = queued_runs.saturating_add(queue.runs.len());
            queued_bytes = queued_bytes.saturating_add(queue.queued_bytes);
            active_tile_queues = active_tile_queues.saturating_add(usize::from(queue.active));
        }

        let reserved_bytes = current_queued_bytes.max(queued_bytes);
        callback(StandardMfsObservabilityEvent {
            stage: self.stage.to_string(),
            active_workers: active_tasks.max(active_tile_queues),
            worker_capacity: self.worker_capacity,
            queues: vec![StandardMfsQueueProgress {
                id: format!("standard-mfs-tile-inbox-{}", self.stage),
                label: format!("Tile inbox {}", self.stage),
                len: Some(
                    queued_runs
                        .saturating_add(heap_len)
                        .saturating_add(active_tasks),
                ),
                capacity: Some(self.worker_capacity.max(self.tiles.len())),
                bytes: Some(reserved_bytes),
                high_water_bytes: Some(queued_bytes_high_water.max(reserved_bytes)),
                producers_active: producer_active,
                consumers_active: active_tasks > 0 || active_tile_queues > 0,
                blocked_count,
                confidence: StandardMfsQueueProgressConfidence::Measured,
                note: Some("measured from standard-MFS fixed-tile inbox scheduler".to_string()),
            }],
        });
    }

    fn queue_is_ready_for_workers(&self, queue: &StandardMfsTileInboxQueueState) -> bool {
        queue.queued_samples >= self.ready_sample_min
    }

    fn publish_runs_locked(
        &self,
        tile_id: StandardMfsTileId,
        queue: &mut StandardMfsTileInboxQueueState,
        runs: &mut VecDeque<StandardMfsTileVisibilityRun>,
        force_ready: bool,
    ) -> StandardMfsTileInboxPublishStats {
        let mut stats = StandardMfsTileInboxPublishStats::default();
        if !runs.is_empty() && queue.runs.capacity() == 0 {
            queue.runs.reserve(STANDARD_MFS_TILE_QUEUE_INITIAL_RUN_CAP);
        }
        while let Some(run) = runs.pop_front() {
            let run_bytes = run.queue_bytes();
            stats.runs += 1;
            stats.samples += run.len();
            stats.bytes = stats.bytes.saturating_add(run_bytes);
            stats.estimated_work = stats.estimated_work.saturating_add(run.estimated_work);
            queue.profile.enqueued_runs += 1;
            queue.profile.enqueued_samples += run.len();
            queue.profile.enqueued_bytes = queue.profile.enqueued_bytes.saturating_add(run_bytes);
            queue.profile.enqueued_tap_visits = queue
                .profile
                .enqueued_tap_visits
                .saturating_add(run.estimated_work);
            queue.queued_samples += run.len();
            queue.queued_bytes += run_bytes;
            queue.queued_work_estimate += run.estimated_work;
            queue.runs.push_back(run);
        }
        stats.ready_head = if stats.runs > 0
            && !queue.active
            && !queue.ready_enqueued
            && !queue.runs.is_empty()
            && (force_ready || self.queue_is_ready_for_workers(queue))
        {
            queue.ready_enqueued = true;
            queue.generation = queue.generation.saturating_add(1);
            queue.ready_head(tile_id, self.started_at)
        } else {
            None
        };
        stats
    }

    fn publish_run_locked(
        &self,
        tile_id: StandardMfsTileId,
        queue: &mut StandardMfsTileInboxQueueState,
        run: StandardMfsTileVisibilityRun,
        force_ready: bool,
    ) -> StandardMfsTileInboxPublishStats {
        let run_bytes = run.queue_bytes();
        let run_len = run.len();
        let estimated_work = run.estimated_work;
        if queue.runs.capacity() == 0 {
            queue.runs.reserve(STANDARD_MFS_TILE_QUEUE_INITIAL_RUN_CAP);
        }
        queue.profile.enqueued_runs += 1;
        queue.profile.enqueued_samples += run_len;
        queue.profile.enqueued_bytes = queue.profile.enqueued_bytes.saturating_add(run_bytes);
        queue.profile.enqueued_tap_visits = queue
            .profile
            .enqueued_tap_visits
            .saturating_add(estimated_work);
        queue.queued_samples += run_len;
        queue.queued_bytes += run_bytes;
        queue.queued_work_estimate += estimated_work;
        queue.runs.push_back(run);

        let ready_head = if !queue.active
            && !queue.ready_enqueued
            && (force_ready || self.queue_is_ready_for_workers(queue))
        {
            queue.ready_enqueued = true;
            queue.generation = queue.generation.saturating_add(1);
            queue.ready_head(tile_id, self.started_at)
        } else {
            None
        };

        StandardMfsTileInboxPublishStats {
            runs: 1,
            samples: run_len,
            bytes: run_bytes,
            estimated_work,
            ready_head,
        }
    }

    fn record_published_runs_locked(
        &self,
        ready: &mut StandardMfsTileInboxReadyState,
        published: StandardMfsTileInboxPublishStats,
    ) {
        if published.runs == 0 {
            return;
        }
        ready.stats.enqueued_runs += published.runs;
        ready.stats.enqueued_samples += published.samples;
        ready.stats.enqueued_bytes += published.bytes;
        ready.stats.current_queued_bytes += published.bytes;
        ready.stats.queued_bytes_high_water = ready
            .stats
            .queued_bytes_high_water
            .max(ready.stats.current_queued_bytes);
        if let Some(head) = published.ready_head {
            ready.push_ready_head(head);
        } else if published.runs > 0 {
            ready.stats.ready_deferred_runs += published.runs;
            ready.stats.ready_deferred_samples += published.samples;
        }
    }

    fn record_published_runs(
        &self,
        published: StandardMfsTileInboxPublishStats,
    ) -> Result<(), ImagingError> {
        if published.runs == 0 {
            return Ok(());
        }
        let notify = published.ready_head.is_some();
        let (lock, cvar) = &*self.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        self.record_published_runs_locked(&mut ready, published);
        if notify {
            cvar.notify_one();
        }
        drop(ready);
        self.emit_observability();
        Ok(())
    }

    fn try_enqueue_runs(
        &self,
        tile_id: StandardMfsTileId,
        runs: &mut VecDeque<StandardMfsTileVisibilityRun>,
    ) -> Result<Option<StandardMfsTileInboxPublishStats>, ImagingError> {
        let tile = self.tiles.get(tile_id.index()).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        debug_assert_eq!(tile.tile_id, tile_id);

        match tile.queue.try_lock() {
            Ok(mut queue) => {
                let published = self.publish_runs_locked(tile_id, &mut queue, runs, false);
                Ok(Some(published))
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let (lock, _) = &*self.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                ready.stats.try_lock_misses += 1;
                Ok(None)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(ImagingError::InvalidRequest(format!(
                    "standard MFS tile inbox {} lock was poisoned",
                    tile_id.index()
                )))
            }
        }
    }

    fn enqueue_runs_blocking(
        &self,
        tile_id: StandardMfsTileId,
        runs: &mut VecDeque<StandardMfsTileVisibilityRun>,
    ) -> Result<StandardMfsTileInboxPublishStats, ImagingError> {
        if runs.is_empty() {
            return Ok(StandardMfsTileInboxPublishStats::default());
        }
        let tile = self.tiles.get(tile_id.index()).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        debug_assert_eq!(tile.tile_id, tile_id);
        let published = {
            let mut queue = tile.queue.lock().map_err(|_| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile inbox {} lock was poisoned",
                    tile_id.index()
                ))
            })?;
            self.publish_runs_locked(tile_id, &mut queue, runs, false)
        };
        Ok(published)
    }

    fn pop_work(&self) -> Result<Option<StandardMfsDrainedTileWork>, ImagingError> {
        loop {
            let head = {
                let (lock, cvar) = &*self.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                loop {
                    if ready.error.is_some() {
                        return Ok(None);
                    }
                    if let Some(head) = ready.heap.pop() {
                        break head;
                    }
                    if ready.closed && ready.active_tasks == 0 {
                        drop(ready);
                        if self.recover_ready_heads_for_nonempty_queues()? == 0 {
                            return Ok(None);
                        }
                        let (lock, _) = &*self.ready;
                        ready = lock.lock().map_err(|_| {
                            ImagingError::InvalidRequest(
                                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                            )
                        })?;
                        continue;
                    }
                    if ready.stats.current_queued_bytes > 0 {
                        ready.stats.wait_with_queued_bytes_events += 1;
                    }
                    if ready.producer_active {
                        ready.stats.wait_with_producer_active_events += 1;
                    }
                    ready = cvar.wait(ready).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS tile inbox scheduler wait was poisoned".to_string(),
                        )
                    })?;
                }
            };

            let tile = self.tiles.get(head.tile_id.index()).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile id {} is out of range",
                    head.tile_id.index()
                ))
            })?;
            let mut queue = tile.queue.lock().map_err(|_| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile inbox {} lock was poisoned",
                    head.tile_id.index()
                ))
            })?;
            let valid = queue.ready_enqueued
                && queue.generation == head.generation
                && !queue.active
                && !queue.runs.is_empty();
            if !valid {
                drop(queue);
                let (lock, _) = &*self.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                if head.tile_id.index() < self.tiles.len() {
                    ready.stats.active_tile_skips += 1;
                } else {
                    ready.stats.stale_heap_entries += 1;
                }
                continue;
            }

            queue.ready_enqueued = false;
            queue.active = true;
            let mut samples = StandardMfsTileQueueChunk::with_run_capacity(queue.runs.len());
            let mut run_count = 0usize;
            let mut bytes = 0usize;
            while let Some(run) = queue.runs.pop_front() {
                run_count += 1;
                bytes = bytes.saturating_add(run.queue_bytes());
                samples.push_run(run);
            }
            let first_input_seq = samples.first_input_seq();
            let estimated_work = samples.estimated_work();
            queue.profile.drains += 1;
            queue.profile.worker_runs += run_count;
            queue.profile.worker_samples += samples.len();
            queue.profile.worker_tap_visits = queue
                .profile
                .worker_tap_visits
                .saturating_add(estimated_work);
            queue.queued_samples = queue.queued_samples.saturating_sub(samples.len());
            queue.queued_bytes = queue.queued_bytes.saturating_sub(bytes);
            queue.queued_work_estimate = queue.queued_work_estimate.saturating_sub(estimated_work);
            drop(queue);

            let (lock, _) = &*self.ready;
            let mut ready = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                )
            })?;
            ready.update_activity(Instant::now());
            ready.active_tasks += 1;
            ready.stats.worker_drains += 1;
            ready.stats.worker_runs += run_count;
            ready.stats.worker_samples += samples.len();
            ready.stats.worker_tap_visits += estimated_work;
            drop(ready);
            self.emit_observability();

            return Ok(Some(StandardMfsDrainedTileWork {
                tile_id: head.tile_id,
                first_input_seq,
                samples,
                run_count,
                estimated_work,
                bytes,
            }));
        }
    }

    fn recover_ready_heads_for_nonempty_queues(&self) -> Result<usize, ImagingError> {
        let mut heads = Vec::new();
        for tile in &self.tiles {
            let mut queue = tile.queue.lock().map_err(|_| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile inbox {} lock was poisoned",
                    tile.tile_id.index()
                ))
            })?;
            if !queue.active && !queue.ready_enqueued && !queue.runs.is_empty() {
                queue.ready_enqueued = true;
                queue.generation = queue.generation.saturating_add(1);
                if let Some(head) = queue.ready_head(tile.tile_id, self.started_at) {
                    heads.push(head);
                }
            }
        }
        if heads.is_empty() {
            return Ok(0);
        }
        let (lock, cvar) = &*self.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        for head in heads {
            ready.push_ready_head(head);
        }
        cvar.notify_all();
        Ok(ready.heap.len())
    }

    fn finish_work(
        &self,
        task: StandardMfsTileTask,
        timing: StandardMfsTileTaskTiming,
        bytes: usize,
    ) -> Result<(), ImagingError> {
        let tile_id = task.tile_id;
        let tile = self.tiles.get(tile_id.index()).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let maybe_head = {
            let mut queue = tile.queue.lock().map_err(|_| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile inbox {} lock was poisoned",
                    tile_id.index()
                ))
            })?;
            queue.profile.worker_active += timing.active();
            queue.profile.last_finish = Some(self.started_at.elapsed());
            queue.active = false;
            if !queue.runs.is_empty()
                && !queue.ready_enqueued
                && self.queue_is_ready_for_workers(&queue)
            {
                queue.ready_enqueued = true;
                queue.generation = queue.generation.saturating_add(1);
                queue.ready_head(tile_id, self.started_at)
            } else {
                None
            }
        };

        let (lock, cvar) = &*self.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        ready.update_activity(Instant::now());
        ready.active_tasks = ready.active_tasks.saturating_sub(1);
        ready.stats.current_queued_bytes = ready.stats.current_queued_bytes.saturating_sub(bytes);
        if let Some(head) = maybe_head {
            ready.push_ready_head(head);
        }
        cvar.notify_all();
        drop(ready);
        self.emit_observability();
        Ok(())
    }

    fn abort_with(&self, error: ImagingError) -> Result<(), ImagingError> {
        let (lock, cvar) = &*self.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        ready.error = Some(error.to_string());
        ready.closed = true;
        cvar.notify_all();
        Ok(())
    }
}

struct StandardMfsTileInboxProducer {
    shared: Arc<StandardMfsTileInboxShared>,
    pending: Vec<VecDeque<StandardMfsTileVisibilityRun>>,
    deferred_published: StandardMfsTileInboxPublishStats,
}

impl StandardMfsTileInboxProducer {
    fn new(shared: Arc<StandardMfsTileInboxShared>) -> Self {
        let pending = (0..shared.tiles.len()).map(|_| VecDeque::new()).collect();
        Self {
            shared,
            pending,
            deferred_published: StandardMfsTileInboxPublishStats::default(),
        }
    }

    fn record_published_runs(
        &mut self,
        published: StandardMfsTileInboxPublishStats,
    ) -> Result<(), ImagingError> {
        if published.runs == 0 {
            return Ok(());
        }
        if published.ready_head.is_none() {
            self.deferred_published.add_deferred(published);
            return Ok(());
        }

        let deferred = std::mem::take(&mut self.deferred_published);
        let (lock, cvar) = &*self.shared.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        self.shared
            .record_published_runs_locked(&mut ready, deferred);
        self.shared
            .record_published_runs_locked(&mut ready, published);
        cvar.notify_one();
        Ok(())
    }

    fn flush_deferred_published(&mut self) -> Result<(), ImagingError> {
        let deferred = std::mem::take(&mut self.deferred_published);
        self.shared.record_published_runs(deferred)
    }

    fn enqueue_run(
        &mut self,
        tile_id: StandardMfsTileId,
        run: StandardMfsTileVisibilityRun,
    ) -> Result<(), ImagingError> {
        let run_bytes = run.queue_bytes();
        let pending = self.pending.get_mut(tile_id.index()).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        if pending.is_empty() {
            let tile = Arc::clone(self.shared.tiles.get(tile_id.index()).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile id {} is out of range",
                    tile_id.index()
                ))
            })?);
            debug_assert_eq!(tile.tile_id, tile_id);
            match tile.queue.try_lock() {
                Ok(mut queue) => {
                    let published = self
                        .shared
                        .publish_run_locked(tile_id, &mut queue, run, false);
                    drop(queue);
                    self.record_published_runs(published)?;
                    return Ok(());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let (lock, _) = &*self.shared.ready;
                    let mut ready = lock.lock().map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                        )
                    })?;
                    ready.stats.try_lock_misses += 1;
                    pending.push_back(run);
                    ready.stats.pending_runs += 1;
                    ready.stats.pending_bytes += run_bytes;
                    ready.stats.pending_bytes_high_water = ready
                        .stats
                        .pending_bytes_high_water
                        .max(ready.stats.pending_bytes);
                    return Ok(());
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(ImagingError::InvalidRequest(format!(
                        "standard MFS tile inbox {} lock was poisoned",
                        tile_id.index()
                    )));
                }
            }
        }

        let mut runs = VecDeque::new();
        let old_pending_runs = pending.len();
        let old_pending_bytes = pending.iter().map(|run| run.queue_bytes()).sum::<usize>();
        runs.append(pending);
        runs.push_back(run);
        if let Some(published) = self.shared.try_enqueue_runs(tile_id, &mut runs)? {
            if old_pending_runs > 0 || old_pending_bytes > 0 {
                let (lock, _) = &*self.shared.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                ready.stats.pending_runs =
                    ready.stats.pending_runs.saturating_sub(old_pending_runs);
                ready.stats.pending_bytes =
                    ready.stats.pending_bytes.saturating_sub(old_pending_bytes);
            }
            self.record_published_runs(published)?;
            return Ok(());
        }
        *pending = runs;
        let (lock, _) = &*self.shared.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
        ready.stats.pending_runs += 1;
        ready.stats.pending_bytes += run_bytes;
        ready.stats.pending_bytes_high_water = ready
            .stats
            .pending_bytes_high_water
            .max(ready.stats.pending_bytes);
        Ok(())
    }

    fn flush_pending_blocking(&mut self) -> Result<(), ImagingError> {
        for tile_index in 0..self.pending.len() {
            if self.pending[tile_index].is_empty() {
                continue;
            }
            let mut runs = VecDeque::new();
            std::mem::swap(&mut runs, &mut self.pending[tile_index]);
            let pending_bytes = runs.iter().map(|run| run.queue_bytes()).sum::<usize>();
            let pending_runs = runs.len();
            let published = self
                .shared
                .enqueue_runs_blocking(StandardMfsTileId(tile_index as u32), &mut runs)?;
            let (lock, _) = &*self.shared.ready;
            let mut ready = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                )
            })?;
            ready.stats.pending_runs = ready.stats.pending_runs.saturating_sub(pending_runs);
            ready.stats.pending_bytes = ready.stats.pending_bytes.saturating_sub(pending_bytes);
            drop(ready);
            self.record_published_runs(published)?;
        }
        Ok(())
    }
}

struct StandardMfsTileRunAccumulator<'a> {
    current_tile: Option<StandardMfsTileId>,
    current_run: StandardMfsTileVisibilityRun,
    enqueue: &'a mut dyn FnMut(
        StandardMfsTileId,
        StandardMfsTileVisibilityRun,
    ) -> Result<(), ImagingError>,
}

impl<'a> StandardMfsTileRunAccumulator<'a> {
    fn new(
        enqueue: &'a mut dyn FnMut(
            StandardMfsTileId,
            StandardMfsTileVisibilityRun,
        ) -> Result<(), ImagingError>,
    ) -> Self {
        Self {
            current_tile: None,
            current_run: StandardMfsTileVisibilityRun::empty(),
            enqueue,
        }
    }

    fn push_sample(
        &mut self,
        tile_id: StandardMfsTileId,
        sample: StandardMfsTileQueueSample,
    ) -> Result<(), ImagingError> {
        if self.current_tile == Some(tile_id) {
            self.current_run.push_sample(sample);
            return Ok(());
        }
        self.flush()?;
        let mut run = StandardMfsTileVisibilityRun::with_capacity(1, sample.input_seq);
        run.push_sample(sample);
        self.current_tile = Some(tile_id);
        self.current_run = run;
        Ok(())
    }

    fn push_run(
        &mut self,
        tile_id: StandardMfsTileId,
        run: StandardMfsTileVisibilityRun,
    ) -> Result<(), ImagingError> {
        if run.is_empty() {
            return Ok(());
        }
        if self.current_tile == Some(tile_id) && self.current_run.can_append_run(&run) {
            self.current_run.append_run(run);
            return Ok(());
        }
        self.flush()?;
        self.current_tile = Some(tile_id);
        self.current_run = run;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), ImagingError> {
        let Some(tile_id) = self.current_tile.take() else {
            return Ok(());
        };
        let run = std::mem::replace(&mut self.current_run, StandardMfsTileVisibilityRun::empty());
        if !run.is_empty() {
            (self.enqueue)(tile_id, run)?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
fn run_standard_mfs_tile_inbox_scheduler<T, P, E>(
    partition: &StandardMfsFixedTilePartition,
    worker_count: usize,
    stage: &'static str,
    observability_callback: Option<&StandardMfsObservabilityCallback>,
    mut produce_runs: P,
    execute_work: E,
) -> Result<StandardMfsTileInboxSchedulerOutput<T>, ImagingError>
where
    T: Send,
    P: FnMut(
        &mut dyn FnMut(StandardMfsTileId, StandardMfsTileVisibilityRun) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
    E: Fn(
            StandardMfsTileId,
            &StandardMfsTileQueueChunk,
        ) -> Result<(T, StandardMfsTileTaskTiming), ImagingError>
        + Sync,
{
    let worker_count = worker_count.max(1);
    let ready_sample_min = standard_mfs_tile_inbox_ready_sample_min();
    let shared = Arc::new(StandardMfsTileInboxShared::new(
        partition.tile_count(),
        ready_sample_min,
        stage,
        worker_count,
        observability_callback.cloned(),
    ));
    let mut all_outputs = Vec::<StandardMfsTileInboxTaskOutput<T>>::new();
    let mut all_worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let shared = Arc::clone(&shared);
            let execute_work = &execute_work;
            handles.push(scope.spawn(move || {
                let worker_started = Instant::now();
                let mut worker_profile = StandardMfsTileWorkerProfile::default();
                let mut outputs = Vec::<StandardMfsTileInboxTaskOutput<T>>::new();
                loop {
                    let Some(work) = shared.pop_work()? else {
                        worker_profile.finish(worker_started);
                        return Ok::<_, ImagingError>((outputs, worker_profile));
                    };
                    let task = work.task();
                    let execution = execute_work(work.tile_id, &work.samples);
                    let bytes = work.bytes;
                    match execution {
                        Ok((output, timing)) => {
                            drop(work.samples);
                            worker_profile.record_task(task, timing);
                            shared.finish_work(task, timing, bytes)?;
                            outputs.push(StandardMfsTileInboxTaskOutput {
                                tile_id: task.tile_id,
                                first_input_seq: work.first_input_seq,
                                output,
                                timing,
                            });
                        }
                        Err(error) => {
                            drop(work.samples);
                            shared.finish_work(
                                task,
                                StandardMfsTileTaskTiming::default(),
                                bytes,
                            )?;
                            shared.abort_with(error)?;
                            worker_profile.finish(worker_started);
                            return Ok::<_, ImagingError>((outputs, worker_profile));
                        }
                    }
                }
            }));
        }

        let produce_result = {
            let shared_for_producer = Arc::clone(&shared);
            {
                let (lock, cvar) = &*shared.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                ready.set_producer_active(true);
                cvar.notify_all();
            }
            shared.emit_observability();
            let mut producer = StandardMfsTileInboxProducer::new(shared_for_producer);
            let result = produce_runs(&mut |tile_id, run| producer.enqueue_run(tile_id, run));
            if let Err(error) = result {
                Err(error)
            } else {
                producer.flush_pending_blocking()?;
                producer.flush_deferred_published()
            }
        };

        {
            let (lock, cvar) = &*shared.ready;
            let mut ready = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                )
            })?;
            ready.set_producer_active(false);
            if let Err(error) = &produce_result {
                ready.error = Some(error.to_string());
            }
            ready.closed = true;
            ready.update_activity(Instant::now());
            cvar.notify_all();
        }
        shared.emit_observability();

        for handle in handles {
            let (mut outputs, worker_profile) = handle.join().map_err(|_| {
                ImagingError::InvalidRequest("standard MFS tile inbox worker panicked".to_string())
            })??;
            all_outputs.append(&mut outputs);
            all_worker_profiles.push(worker_profile);
        }
        produce_result
    })?;

    let (lock, _) = &*shared.ready;
    let ready = lock.lock().map_err(|_| {
        ImagingError::InvalidRequest(
            "standard MFS tile inbox scheduler lock was poisoned".to_string(),
        )
    })?;
    if let Some(error) = ready.error.clone() {
        return Err(ImagingError::InvalidRequest(error));
    }
    let stats = StandardMfsTileInboxSchedulerStats {
        ready_sample_min: ready.stats.ready_sample_min,
        enqueued_runs: ready.stats.enqueued_runs,
        enqueued_samples: ready.stats.enqueued_samples,
        enqueued_bytes: ready.stats.enqueued_bytes,
        ready_deferred_runs: ready.stats.ready_deferred_runs,
        ready_deferred_samples: ready.stats.ready_deferred_samples,
        pending_runs: ready.stats.pending_runs,
        pending_bytes: ready.stats.pending_bytes,
        pending_bytes_high_water: ready.stats.pending_bytes_high_water,
        try_lock_misses: ready.stats.try_lock_misses,
        current_queued_bytes: ready.stats.current_queued_bytes,
        queued_bytes_high_water: ready.stats.queued_bytes_high_water,
        ready_heads_pushed: ready.stats.ready_heads_pushed,
        worker_drains: ready.stats.worker_drains,
        worker_runs: ready.stats.worker_runs,
        worker_samples: ready.stats.worker_samples,
        worker_tap_visits: ready.stats.worker_tap_visits,
        active_tile_skips: ready.stats.active_tile_skips,
        stale_heap_entries: ready.stats.stale_heap_entries,
        wait_with_queued_bytes_events: ready.stats.wait_with_queued_bytes_events,
        wait_with_producer_active_events: ready.stats.wait_with_producer_active_events,
        producer_active: ready.stats.producer_active,
        producer_worker_overlap: ready.stats.producer_worker_overlap,
        worker_active_union: ready.stats.worker_active_union,
        neither_active: ready.stats.neither_active,
    };
    drop(ready);
    let tile_profiles = shared
        .tiles
        .iter()
        .map(|tile| {
            tile.queue
                .lock()
                .map(|queue| queue.profile.clone())
                .map_err(|_| {
                    ImagingError::InvalidRequest(format!(
                        "standard MFS tile inbox {} lock was poisoned",
                        tile.tile_id.index()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    all_outputs.sort_by_key(|output| output.tile_id);
    Ok(StandardMfsTileInboxSchedulerOutput {
        task_outputs: all_outputs,
        worker_profiles: all_worker_profiles,
        tile_profiles,
        stats,
    })
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct StandardMfsPersistentReadyTask {
    block: Arc<PreparedTileRowBlock>,
    task: StandardMfsTileTask,
    sequence: u64,
}

impl PartialEq for StandardMfsPersistentReadyTask {
    fn eq(&self, other: &Self) -> bool {
        self.block.block_id == other.block.block_id
            && self.task.tile_id == other.task.tile_id
            && self.sequence == other.sequence
    }
}

impl Eq for StandardMfsPersistentReadyTask {}

impl PartialOrd for StandardMfsPersistentReadyTask {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for StandardMfsPersistentReadyTask {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .block
            .block_id
            .cmp(&self.block.block_id)
            .then_with(|| {
                self.task
                    .estimated_tap_visits
                    .cmp(&other.task.estimated_tap_visits)
            })
            .then_with(|| self.task.sample_count.cmp(&other.task.sample_count))
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct StandardMfsPersistentSchedulerStats {
    published_blocks: usize,
    queued_tasks: usize,
    active_tile_skips: usize,
    stale_heap_entries: usize,
    max_live_row_blocks_observed: usize,
    max_live_row_block_bytes: usize,
    producer_active: Duration,
    producer_worker_overlap: Duration,
    producer_blocked_on_memory: Duration,
    worker_active_union: Duration,
    neither_active: Duration,
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsPersistentSchedulerState {
    ready: BinaryHeap<StandardMfsPersistentReadyTask>,
    active_tiles: Vec<bool>,
    live_blocks: BTreeMap<StandardMfsRowBlockId, usize>,
    live_block_bytes: BTreeMap<StandardMfsRowBlockId, usize>,
    active_tasks: usize,
    next_sequence: u64,
    closed: bool,
    error: Option<String>,
    producer_active: bool,
    last_activity_update: Instant,
    stats: StandardMfsPersistentSchedulerStats,
}

#[allow(dead_code)]
impl StandardMfsPersistentSchedulerState {
    fn new(tile_count: usize) -> Self {
        Self {
            ready: BinaryHeap::new(),
            active_tiles: vec![false; tile_count],
            live_blocks: BTreeMap::new(),
            live_block_bytes: BTreeMap::new(),
            active_tasks: 0,
            next_sequence: 0,
            closed: false,
            error: None,
            producer_active: false,
            last_activity_update: Instant::now(),
            stats: StandardMfsPersistentSchedulerStats::default(),
        }
    }

    fn update_activity(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_activity_update);
        if self.producer_active {
            self.stats.producer_active += elapsed;
        }
        if self.active_tasks > 0 {
            self.stats.worker_active_union += elapsed;
        }
        if self.producer_active && self.active_tasks > 0 {
            self.stats.producer_worker_overlap += elapsed;
        }
        if !self.producer_active && self.active_tasks == 0 {
            self.stats.neither_active += elapsed;
        }
        self.last_activity_update = now;
    }

    fn set_producer_active(&mut self, active: bool) {
        self.update_activity(Instant::now());
        self.producer_active = active;
    }

    fn publish(&mut self, block: Arc<PreparedTileRowBlock>) {
        let tasks = block.buckets.tile_tasks_descending();
        if tasks.is_empty() {
            return;
        }
        let block_bytes = block.byte_ledger().total_bytes();
        self.live_blocks.insert(block.block_id, tasks.len());
        self.live_block_bytes.insert(block.block_id, block_bytes);
        self.stats.published_blocks += 1;
        self.stats.queued_tasks += tasks.len();
        self.stats.max_live_row_blocks_observed = self
            .stats
            .max_live_row_blocks_observed
            .max(self.live_blocks.len());
        let live_bytes = self.live_block_bytes.values().copied().sum::<usize>();
        self.stats.max_live_row_block_bytes = self.stats.max_live_row_block_bytes.max(live_bytes);
        for task in tasks {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.saturating_add(1);
            self.ready.push(StandardMfsPersistentReadyTask {
                block: Arc::clone(&block),
                task,
                sequence,
            });
        }
    }

    fn pop_ready(&mut self) -> Option<StandardMfsPersistentReadyTask> {
        let mut parked = Vec::new();
        let selected = loop {
            let Some(candidate) = self.ready.pop() else {
                break None;
            };
            let tile_index = candidate.task.tile_id.index();
            if tile_index >= self.active_tiles.len() {
                self.stats.stale_heap_entries += 1;
                continue;
            }
            if self.active_tiles[tile_index] {
                self.stats.active_tile_skips += 1;
                parked.push(candidate);
                continue;
            }
            self.update_activity(Instant::now());
            self.active_tiles[tile_index] = true;
            self.active_tasks += 1;
            break Some(candidate);
        };
        for task in parked {
            self.ready.push(task);
        }
        selected
    }

    fn complete(&mut self, task: &StandardMfsPersistentReadyTask) {
        self.update_activity(Instant::now());
        let tile_index = task.task.tile_id.index();
        if tile_index < self.active_tiles.len() {
            self.active_tiles[tile_index] = false;
        }
        self.active_tasks = self.active_tasks.saturating_sub(1);
        if let Some(remaining) = self.live_blocks.get_mut(&task.block.block_id) {
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                self.live_blocks.remove(&task.block.block_id);
                self.live_block_bytes.remove(&task.block.block_id);
            }
        }
    }
}

#[allow(dead_code)]
struct StandardMfsPersistentTaskOutput<T> {
    block_id: StandardMfsRowBlockId,
    tile_id: StandardMfsTileId,
    output: T,
    timing: StandardMfsTileTaskTiming,
}

#[allow(dead_code)]
struct StandardMfsPersistentSchedulerOutput<T> {
    task_outputs: Vec<StandardMfsPersistentTaskOutput<T>>,
    worker_profiles: Vec<StandardMfsTileWorkerProfile>,
    stats: StandardMfsPersistentSchedulerStats,
}

#[allow(dead_code)]
fn run_standard_mfs_persistent_tile_scheduler<T, P, E>(
    partition: &StandardMfsFixedTilePartition,
    worker_count: usize,
    max_live_row_blocks: usize,
    mut publish_blocks: P,
    execute_task: E,
) -> Result<StandardMfsPersistentSchedulerOutput<T>, ImagingError>
where
    T: Send,
    P: FnMut(
        &mut dyn FnMut(PreparedTileRowBlock) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
    E: Fn(
            &PreparedTileRowBlock,
            StandardMfsTileTask,
        ) -> Result<(T, StandardMfsTileTaskTiming), ImagingError>
        + Sync,
{
    let worker_count = worker_count.max(1);
    let max_live_row_blocks = max_live_row_blocks.clamp(1, 2);
    let shared = Arc::new((
        Mutex::new(StandardMfsPersistentSchedulerState::new(
            partition.tile_count(),
        )),
        Condvar::new(),
    ));
    let mut all_outputs = Vec::<StandardMfsPersistentTaskOutput<T>>::new();
    let mut all_worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let shared = Arc::clone(&shared);
            let execute_task = &execute_task;
            handles.push(scope.spawn(move || {
                let worker_started = Instant::now();
                let mut worker_profile = StandardMfsTileWorkerProfile::default();
                let mut outputs = Vec::<StandardMfsPersistentTaskOutput<T>>::new();
                loop {
                    let task = {
                        let (lock, cvar) = &*shared;
                        let mut guard = lock.lock().map_err(|_| {
                            ImagingError::InvalidRequest(
                                "standard MFS persistent scheduler lock was poisoned".to_string(),
                            )
                        })?;
                        loop {
                            if guard.error.is_some() {
                                return Ok::<_, ImagingError>((outputs, worker_profile));
                            }
                            if let Some(task) = guard.pop_ready() {
                                break task;
                            }
                            if guard.closed && guard.active_tasks == 0 && guard.ready.is_empty() {
                                worker_profile.finish(worker_started);
                                return Ok::<_, ImagingError>((outputs, worker_profile));
                            }
                            guard = cvar.wait(guard).map_err(|_| {
                                ImagingError::InvalidRequest(
                                    "standard MFS persistent scheduler wait was poisoned"
                                        .to_string(),
                                )
                            })?;
                        }
                    };

                    let execution = execute_task(&task.block, task.task);
                    let (lock, cvar) = &*shared;
                    let mut guard = lock.lock().map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS persistent scheduler lock was poisoned".to_string(),
                        )
                    })?;
                    match execution {
                        Ok((output, timing)) => {
                            worker_profile.record_task(task.task, timing);
                            guard.complete(&task);
                            outputs.push(StandardMfsPersistentTaskOutput {
                                block_id: task.block.block_id,
                                tile_id: task.task.tile_id,
                                output,
                                timing,
                            });
                        }
                        Err(error) => {
                            guard.error = Some(error.to_string());
                            guard.closed = true;
                        }
                    }
                    cvar.notify_all();
                }
            }));
        }

        let publish_result = {
            let shared = Arc::clone(&shared);
            {
                let (lock, cvar) = &*shared;
                let mut guard = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS persistent scheduler lock was poisoned".to_string(),
                    )
                })?;
                guard.set_producer_active(true);
                cvar.notify_all();
            }
            publish_blocks(&mut |block| {
                let block = Arc::new(block);
                let (lock, cvar) = &*shared;
                let mut guard = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS persistent scheduler lock was poisoned".to_string(),
                    )
                })?;
                let mut blocked_on_memory_started = None;
                while guard.error.is_none() && guard.live_blocks.len() >= max_live_row_blocks {
                    if blocked_on_memory_started.is_none() {
                        blocked_on_memory_started = Some(Instant::now());
                    }
                    guard = cvar.wait(guard).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS persistent scheduler wait was poisoned".to_string(),
                        )
                    })?;
                }
                if let Some(started) = blocked_on_memory_started {
                    guard.stats.producer_blocked_on_memory += started.elapsed();
                }
                if let Some(error) = guard.error.clone() {
                    return Err(ImagingError::InvalidRequest(error));
                }
                guard.publish(block);
                cvar.notify_all();
                Ok(())
            })
        };

        {
            let (lock, cvar) = &*shared;
            let mut guard = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS persistent scheduler lock was poisoned".to_string(),
                )
            })?;
            guard.set_producer_active(false);
            cvar.notify_all();
        }

        {
            let (lock, cvar) = &*shared;
            let mut guard = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS persistent scheduler lock was poisoned".to_string(),
                )
            })?;
            while guard.error.is_none()
                && (!guard.live_blocks.is_empty()
                    || guard.active_tasks > 0
                    || !guard.ready.is_empty())
            {
                guard = cvar.wait(guard).map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS persistent scheduler wait was poisoned".to_string(),
                    )
                })?;
            }
            guard.closed = true;
            guard.update_activity(Instant::now());
            cvar.notify_all();
        }

        for handle in handles {
            let (mut outputs, worker_profile) = handle.join().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS persistent tile worker panicked".to_string(),
                )
            })??;
            all_outputs.append(&mut outputs);
            all_worker_profiles.push(worker_profile);
        }
        publish_result
    })?;

    let (lock, _) = &*shared;
    let guard = lock.lock().map_err(|_| {
        ImagingError::InvalidRequest(
            "standard MFS persistent scheduler lock was poisoned".to_string(),
        )
    })?;
    if let Some(error) = guard.error.clone() {
        return Err(ImagingError::InvalidRequest(error));
    }
    let stats = StandardMfsPersistentSchedulerStats {
        published_blocks: guard.stats.published_blocks,
        queued_tasks: guard.stats.queued_tasks,
        active_tile_skips: guard.stats.active_tile_skips,
        stale_heap_entries: guard.stats.stale_heap_entries,
        max_live_row_blocks_observed: guard.stats.max_live_row_blocks_observed,
        max_live_row_block_bytes: guard.stats.max_live_row_block_bytes,
        producer_active: guard.stats.producer_active,
        producer_worker_overlap: guard.stats.producer_worker_overlap,
        producer_blocked_on_memory: guard.stats.producer_blocked_on_memory,
        worker_active_union: guard.stats.worker_active_union,
        neither_active: guard.stats.neither_active,
    };
    drop(guard);
    all_outputs.sort_by(|lhs, rhs| {
        lhs.block_id
            .cmp(&rhs.block_id)
            .then_with(|| lhs.tile_id.cmp(&rhs.tile_id))
    });
    Ok(StandardMfsPersistentSchedulerOutput {
        task_outputs: all_outputs,
        worker_profiles: all_worker_profiles,
        stats,
    })
}

#[allow(dead_code)]
struct StandardMfsPersistentSchedulerLogInputs<'a, T> {
    stage: &'static str,
    partition: &'a StandardMfsFixedTilePartition,
    requested_threads: usize,
    max_live_row_blocks: usize,
    output: &'a StandardMfsPersistentSchedulerOutput<T>,
    stage_total: Duration,
    producer_preprocess: Duration,
    bucket_build: Duration,
}

#[allow(dead_code)]
fn log_persistent_tile_scheduler_summary<T>(
    inputs: StandardMfsPersistentSchedulerLogInputs<'_, T>,
) {
    if !profile::standard_mfs_profile_detail_enabled() {
        return;
    }
    let worker_active = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.active)
        .collect::<Vec<_>>();
    let worker_task_counts = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.task_count)
        .collect::<Vec<_>>();
    let worker_sample_counts = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.sample_count)
        .collect::<Vec<_>>();
    let worker_tap_visits = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.tap_visits)
        .collect::<Vec<_>>();
    let worker_active_total_ms = duration_total_ms(&worker_active);
    let stage_total_ms = profile::millis(inputs.stage_total);
    let worker_capacity_ms = stage_total_ms * inputs.requested_threads.max(1) as f64;
    let producer_active = inputs.output.stats.producer_active;
    let worker_active_union = inputs.output.stats.worker_active_union;
    let producer_worker_overlap = inputs.output.stats.producer_worker_overlap;
    let producer_only = producer_active.saturating_sub(producer_worker_overlap);
    let worker_only = worker_active_union.saturating_sub(producer_worker_overlap);
    let neither_active = inputs
        .stage_total
        .saturating_sub(producer_worker_overlap)
        .saturating_sub(producer_only)
        .saturating_sub(worker_only);
    eprintln!(
        "standard_mfs_tile_persistent_scheduler_summary stage={} requested_threads={} actual_threads={} tile_shape={}x{} tile_anchor={} tile_origin={}x{} tile_count={} max_live_row_blocks={} max_live_row_blocks_observed={} published_blocks={} queued_tasks={} task_outputs={} producer_active_ms={:.3} worker_active_union_ms={:.3} producer_worker_overlap_ms={:.3} producer_only_ms={:.3} worker_only_ms={:.3} neither_active_ms={:.3} producer_blocked_on_memory_ms={:.3} producer_preprocess_total_ms={:.3} bucket_build_total_ms={:.3} worker_task_count={} worker_samples={} worker_tap_visits={} worker_active_total_ms={:.3} worker_active={} worker_capacity_ms={:.3} worker_utilization_pct={:.3} worker_tail_idle_ms={:.3} active_tile_skips={} stale_heap_entries={} live_row_block_bytes_max={} stage_total_ms={:.3}",
        inputs.stage,
        inputs.requested_threads,
        inputs.output.worker_profiles.len(),
        inputs.partition.tile_shape()[0],
        inputs.partition.tile_shape()[1],
        inputs.partition.anchor_label(),
        inputs.partition.tile_origin()[0],
        inputs.partition.tile_origin()[1],
        inputs.partition.tile_count(),
        inputs.max_live_row_blocks,
        inputs.output.stats.max_live_row_blocks_observed,
        inputs.output.stats.published_blocks,
        inputs.output.stats.queued_tasks,
        inputs.output.task_outputs.len(),
        profile::millis(producer_active),
        profile::millis(worker_active_union),
        profile::millis(producer_worker_overlap),
        profile::millis(producer_only),
        profile::millis(worker_only),
        profile::millis(neither_active),
        profile::millis(inputs.output.stats.producer_blocked_on_memory),
        profile::millis(inputs.producer_preprocess),
        profile::millis(inputs.bucket_build),
        stats_triplet(&worker_task_counts),
        stats_triplet(&worker_sample_counts),
        stats_triplet(&worker_tap_visits),
        worker_active_total_ms,
        duration_stats_triplet(&worker_active),
        worker_capacity_ms,
        percent_or_zero(worker_active_total_ms, worker_capacity_ms),
        (worker_capacity_ms - worker_active_total_ms).max(0.0),
        inputs.output.stats.active_tile_skips,
        inputs.output.stats.stale_heap_entries,
        inputs.output.stats.max_live_row_block_bytes,
        stage_total_ms,
    );
}

#[allow(dead_code)]
struct StandardMfsTileInboxSchedulerLogInputs<'a, T> {
    stage: &'static str,
    partition: &'a StandardMfsFixedTilePartition,
    requested_threads: usize,
    output: &'a StandardMfsTileInboxSchedulerOutput<T>,
    stage_total: Duration,
    producer_preprocess: Duration,
}

fn log_tile_inbox_scheduler_summary<T>(inputs: StandardMfsTileInboxSchedulerLogInputs<'_, T>) {
    if !profile::standard_mfs_profile_detail_enabled() {
        return;
    }
    let worker_active = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.active)
        .collect::<Vec<_>>();
    let worker_task_counts = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.task_count)
        .collect::<Vec<_>>();
    let worker_sample_counts = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.sample_count)
        .collect::<Vec<_>>();
    let worker_tap_visits = inputs
        .output
        .worker_profiles
        .iter()
        .map(|worker| worker.tap_visits)
        .collect::<Vec<_>>();
    let worker_active_total_ms = duration_total_ms(&worker_active);
    let stage_total_ms = profile::millis(inputs.stage_total);
    let worker_capacity_ms = stage_total_ms * inputs.requested_threads.max(1) as f64;
    let producer_active = inputs.output.stats.producer_active;
    let worker_active_union = inputs.output.stats.worker_active_union;
    let producer_worker_overlap = inputs.output.stats.producer_worker_overlap;
    let producer_only = producer_active.saturating_sub(producer_worker_overlap);
    let worker_only = worker_active_union.saturating_sub(producer_worker_overlap);
    let neither_active = inputs
        .stage_total
        .saturating_sub(producer_worker_overlap)
        .saturating_sub(producer_only)
        .saturating_sub(worker_only);
    let tile_worker_active_total = inputs
        .output
        .tile_profiles
        .iter()
        .map(|tile| tile.worker_active)
        .fold(Duration::ZERO, |total, value| total + value);
    let tile_worker_active_max = inputs
        .output
        .tile_profiles
        .iter()
        .map(|tile| tile.worker_active)
        .max()
        .unwrap_or(Duration::ZERO);
    let hot_tile_samples = inputs
        .output
        .tile_profiles
        .iter()
        .map(|tile| tile.worker_samples)
        .max()
        .unwrap_or(0);
    eprintln!(
        "standard_mfs_tile_inbox_scheduler_summary stage={} requested_threads={} actual_threads={} tile_shape={}x{} tile_anchor={} tile_origin={}x{} tile_count={} tile_interior_cells={} tile_halo_cells={} tile_halo_overhead_pct={:.3} inbox_worker_count={} ready_sample_min={} enqueued_runs={} enqueued_samples={} enqueued_bytes={} queued_bytes_high_water={} ready_deferred_runs={} ready_deferred_samples={} pending_runs={} pending_bytes={} pending_bytes_high_water={} try_lock_misses={} ready_heads_pushed={} worker_drains={} worker_runs={} worker_samples={} worker_tap_visits={} avg_runs_per_drain={:.3} avg_samples_per_run={:.3} producer_active_ms={:.3} worker_active_union_ms={:.3} producer_worker_overlap_ms={:.3} producer_only_ms={:.3} worker_only_ms={:.3} neither_active_ms={:.3} producer_preprocess_total_ms={:.3} worker_task_count={} worker_samples_by_worker={} worker_tap_visits_by_worker={} worker_active_total_ms={:.3} worker_active={} worker_capacity_ms={:.3} worker_utilization_pct={:.3} worker_tail_idle_ms={:.3} tile_worker_active_total_ms={:.3} tile_worker_active_max_ms={:.3} tile_active_bound_pct={:.3} hot_tile_sample_share_pct={:.3} top_tile_enqueued_samples={} top_tile_worker_samples={} top_tile_worker_tap_visits={} top_tile_worker_active_ms={} top_tile_first_ready_ms={} top_tile_last_finish_ms={} active_tile_skips={} stale_heap_entries={} wait_with_queued_bytes_events={} wait_with_producer_active_events={} task_outputs={} stage_total_ms={:.3}",
        inputs.stage,
        inputs.requested_threads,
        inputs.output.worker_profiles.len(),
        inputs.partition.tile_shape()[0],
        inputs.partition.tile_shape()[1],
        inputs.partition.anchor_label(),
        inputs.partition.tile_origin()[0],
        inputs.partition.tile_origin()[1],
        inputs.partition.tile_count(),
        inputs.partition.interior_cell_count(),
        inputs.partition.halo_cell_count(),
        percent_or_zero(
            inputs
                .partition
                .halo_cell_count()
                .saturating_sub(inputs.partition.interior_cell_count()) as f64,
            inputs.partition.interior_cell_count() as f64,
        ),
        inputs.requested_threads.max(1),
        inputs.output.stats.ready_sample_min,
        inputs.output.stats.enqueued_runs,
        inputs.output.stats.enqueued_samples,
        inputs.output.stats.enqueued_bytes,
        inputs.output.stats.queued_bytes_high_water,
        inputs.output.stats.ready_deferred_runs,
        inputs.output.stats.ready_deferred_samples,
        inputs.output.stats.pending_runs,
        inputs.output.stats.pending_bytes,
        inputs.output.stats.pending_bytes_high_water,
        inputs.output.stats.try_lock_misses,
        inputs.output.stats.ready_heads_pushed,
        inputs.output.stats.worker_drains,
        inputs.output.stats.worker_runs,
        inputs.output.stats.worker_samples,
        inputs.output.stats.worker_tap_visits,
        ratio_or_zero(
            inputs.output.stats.worker_runs,
            inputs.output.stats.worker_drains
        ),
        ratio_or_zero(
            inputs.output.stats.enqueued_samples,
            inputs.output.stats.enqueued_runs
        ),
        profile::millis(producer_active),
        profile::millis(worker_active_union),
        profile::millis(producer_worker_overlap),
        profile::millis(producer_only),
        profile::millis(worker_only),
        profile::millis(neither_active),
        profile::millis(inputs.producer_preprocess),
        stats_triplet(&worker_task_counts),
        stats_triplet(&worker_sample_counts),
        stats_triplet(&worker_tap_visits),
        worker_active_total_ms,
        duration_stats_triplet(&worker_active),
        worker_capacity_ms,
        percent_or_zero(worker_active_total_ms, worker_capacity_ms),
        (worker_capacity_ms - worker_active_total_ms).max(0.0),
        profile::millis(tile_worker_active_total),
        profile::millis(tile_worker_active_max),
        percent_or_zero(
            profile::millis(tile_worker_active_total),
            profile::millis(tile_worker_active_max) * inputs.requested_threads.max(1) as f64,
        ),
        percent_or_zero(
            hot_tile_samples as f64,
            inputs.output.stats.worker_samples as f64
        ),
        top_tile_profile_counts(&inputs.output.tile_profiles, 8, |tile| tile
            .enqueued_samples),
        top_tile_profile_counts(&inputs.output.tile_profiles, 8, |tile| tile.worker_samples),
        top_tile_profile_counts(&inputs.output.tile_profiles, 8, |tile| tile
            .worker_tap_visits),
        top_tile_profile_durations(&inputs.output.tile_profiles, 8, |tile| tile.worker_active),
        top_tile_profile_optional_durations(&inputs.output.tile_profiles, 8, |tile| {
            tile.first_ready
        }),
        top_tile_profile_optional_durations(&inputs.output.tile_profiles, 8, |tile| {
            tile.last_finish
        }),
        inputs.output.stats.active_tile_skips,
        inputs.output.stats.stale_heap_entries,
        inputs.output.stats.wait_with_queued_bytes_events,
        inputs.output.stats.wait_with_producer_active_events,
        inputs.output.task_outputs.len(),
        stage_total_ms,
    );
}

#[derive(Clone, Debug, Default)]
struct StandardMfsTileSchedulerBlockProfile {
    requested_threads: usize,
    actual_threads: usize,
    task_count: usize,
    sample_count: usize,
    tap_visits: usize,
    largest_task_samples: usize,
    largest_task_tap_visits: usize,
    bucket_bytes: usize,
    bucket_build: Duration,
    local_alloc_zero: Duration,
    worker_replan_grid: Duration,
    block_wall: Duration,
    merge: Duration,
    merged_tiles: usize,
    worker_profiles: Vec<StandardMfsTileWorkerProfile>,
}

struct StandardMfsTileSchedulerBlockInputs<'a> {
    worker_count: usize,
    tasks: &'a [StandardMfsTileTask],
    buckets: &'a StandardMfsBlockTileBuckets,
    bucket_build: Duration,
    task_timing: StandardMfsTileTaskTiming,
    block_wall: Duration,
    merge: Duration,
    merged_tiles: usize,
    worker_profiles: Vec<StandardMfsTileWorkerProfile>,
}

#[derive(Debug)]
struct StandardMfsTileSchedulerStageProfile {
    stage: &'static str,
    tile_count: usize,
    tile_shape: [usize; 2],
    tile_origin: [usize; 2],
    tile_anchor: &'static str,
    resident_tile_limit: usize,
    blocks: Vec<StandardMfsTileSchedulerBlockProfile>,
    replay_gap_duration: Duration,
    batch_preprocess_duration: Duration,
    flush_duration: Duration,
    tile_flush_count: usize,
    tile_eviction_count: usize,
    started_at: Instant,
    last_event_at: Instant,
}

impl StandardMfsTileSchedulerStageProfile {
    fn new(
        stage: &'static str,
        partition: &StandardMfsFixedTilePartition,
        resident_tile_limit: usize,
    ) -> Self {
        let started_at = Instant::now();
        Self {
            stage,
            tile_count: partition.tile_count(),
            tile_shape: partition.tile_shape(),
            tile_origin: partition.tile_origin(),
            tile_anchor: partition.anchor_label(),
            resident_tile_limit,
            blocks: Vec::new(),
            replay_gap_duration: Duration::ZERO,
            batch_preprocess_duration: Duration::ZERO,
            flush_duration: Duration::ZERO,
            tile_flush_count: 0,
            tile_eviction_count: 0,
            started_at,
            last_event_at: started_at,
        }
    }

    fn record_replay_gap_now(&mut self) {
        let now = Instant::now();
        self.replay_gap_duration += now.saturating_duration_since(self.last_event_at);
        self.last_event_at = now;
    }

    fn add_batch_preprocess_duration(&mut self, duration: Duration) {
        self.batch_preprocess_duration += duration;
        self.last_event_at = Instant::now();
    }

    fn record(&mut self, block: StandardMfsTileSchedulerBlockProfile) {
        if profile::standard_mfs_profile_block_detail_enabled() {
            log_tiled_scheduler_block(self.stage, &block);
        }
        self.blocks.push(block);
        self.last_event_at = Instant::now();
    }

    fn add_flush_duration(&mut self, duration: Duration) {
        self.flush_duration += duration;
        self.last_event_at = Instant::now();
    }

    fn set_cache_counters(&mut self, tile_flush_count: usize, tile_eviction_count: usize) {
        self.tile_flush_count = tile_flush_count;
        self.tile_eviction_count = tile_eviction_count;
    }

    fn log(&self) {
        if !profile::standard_mfs_profile_detail_enabled() {
            return;
        }
        let requested_threads = self
            .blocks
            .iter()
            .map(|block| block.requested_threads)
            .max()
            .unwrap_or_else(standard_mfs_grid_threads);
        let actual_threads = self
            .blocks
            .iter()
            .map(|block| block.actual_threads)
            .collect::<Vec<_>>();
        let task_counts = self
            .blocks
            .iter()
            .map(|block| block.task_count)
            .collect::<Vec<_>>();
        let sample_counts = self
            .blocks
            .iter()
            .map(|block| block.sample_count)
            .collect::<Vec<_>>();
        let tap_visits = self
            .blocks
            .iter()
            .map(|block| block.tap_visits)
            .collect::<Vec<_>>();
        let largest_task_samples = self
            .blocks
            .iter()
            .map(|block| block.largest_task_samples)
            .collect::<Vec<_>>();
        let largest_task_tap_visits = self
            .blocks
            .iter()
            .map(|block| block.largest_task_tap_visits)
            .collect::<Vec<_>>();
        let bucket_build = self
            .blocks
            .iter()
            .map(|block| block.bucket_build)
            .collect::<Vec<_>>();
        let local_alloc_zero = self
            .blocks
            .iter()
            .map(|block| block.local_alloc_zero)
            .collect::<Vec<_>>();
        let worker_replan_grid = self
            .blocks
            .iter()
            .map(|block| block.worker_replan_grid)
            .collect::<Vec<_>>();
        let block_wall = self
            .blocks
            .iter()
            .map(|block| block.block_wall)
            .collect::<Vec<_>>();
        let merge = self
            .blocks
            .iter()
            .map(|block| block.merge)
            .collect::<Vec<_>>();
        let samples_total = sample_counts.iter().sum::<usize>();
        let tap_visits_total = tap_visits.iter().sum::<usize>();
        let bucket_bytes_max = self
            .blocks
            .iter()
            .map(|block| block.bucket_bytes)
            .max()
            .unwrap_or(0);
        let bucket_bytes_total = self
            .blocks
            .iter()
            .map(|block| block.bucket_bytes)
            .sum::<usize>();
        let worker_task_counts = self
            .blocks
            .iter()
            .flat_map(|block| block.worker_profiles.iter().map(|worker| worker.task_count))
            .collect::<Vec<_>>();
        let worker_sample_counts = self
            .blocks
            .iter()
            .flat_map(|block| {
                block
                    .worker_profiles
                    .iter()
                    .map(|worker| worker.sample_count)
            })
            .collect::<Vec<_>>();
        let worker_tap_visits = self
            .blocks
            .iter()
            .flat_map(|block| block.worker_profiles.iter().map(|worker| worker.tap_visits))
            .collect::<Vec<_>>();
        let worker_tap_visits_per_s = self
            .blocks
            .iter()
            .flat_map(|block| {
                block
                    .worker_profiles
                    .iter()
                    .map(|worker| per_second_or_zero(worker.tap_visits, worker.active))
            })
            .collect::<Vec<_>>();
        let worker_samples_per_s = self
            .blocks
            .iter()
            .flat_map(|block| {
                block
                    .worker_profiles
                    .iter()
                    .map(|worker| per_second_or_zero(worker.sample_count, worker.active))
            })
            .collect::<Vec<_>>();
        let worker_active = self
            .blocks
            .iter()
            .flat_map(|block| block.worker_profiles.iter().map(|worker| worker.active))
            .collect::<Vec<_>>();
        let worker_elapsed = self
            .blocks
            .iter()
            .flat_map(|block| block.worker_profiles.iter().map(|worker| worker.elapsed))
            .collect::<Vec<_>>();
        let worker_active_total_ms = duration_total_ms(&worker_active);
        let worker_capacity_ms = self
            .blocks
            .iter()
            .map(|block| profile::millis(block.block_wall) * block.actual_threads.max(1) as f64)
            .sum::<f64>();
        let worker_utilization_pct = percent_or_zero(worker_active_total_ms, worker_capacity_ms);
        let worker_tail_idle_ms = (worker_capacity_ms - worker_active_total_ms).max(0.0);
        let stage_total = self.started_at.elapsed();
        let stage_total_ms = profile::millis(stage_total);
        let block_wall_total_ms = duration_total_ms(&block_wall);
        let stage_nonworker_wall_ms = (stage_total_ms - block_wall_total_ms).max(0.0);
        let replay_gap_total_ms = profile::millis(self.replay_gap_duration);
        let batch_preprocess_total_ms = profile::millis(self.batch_preprocess_duration);
        let accounted_stage_ms = replay_gap_total_ms
            + batch_preprocess_total_ms
            + duration_total_ms(&bucket_build)
            + block_wall_total_ms
            + profile::millis(self.flush_duration);
        let stage_unaccounted_ms = (stage_total_ms - accounted_stage_ms).max(0.0);
        let stage_worker_capacity_ms = stage_total_ms * requested_threads.max(1) as f64;
        let stage_worker_utilization_pct =
            percent_or_zero(worker_active_total_ms, stage_worker_capacity_ms);
        let stage_worker_idle_ms = (stage_worker_capacity_ms - worker_active_total_ms).max(0.0);
        let stage_tap_visits_per_s = per_second_or_zero(tap_visits_total, stage_total);
        let stage_samples_per_s = per_second_or_zero(samples_total, stage_total);
        let active_weighted_tap_visits_per_s = if worker_active_total_ms > 0.0 {
            tap_visits_total as f64 / (worker_active_total_ms / 1000.0)
        } else {
            0.0
        };
        let active_weighted_samples_per_s = if worker_active_total_ms > 0.0 {
            samples_total as f64 / (worker_active_total_ms / 1000.0)
        } else {
            0.0
        };
        eprintln!(
            "standard_mfs_tile_scheduler_summary stage={} requested_threads={} actual_threads={} tile_shape={}x{} tile_anchor={} tile_origin={}x{} tile_count={} resident_tile_limit={} max_live_row_blocks=1 block_count={} task_count={} samples_total={} tap_visits_total={} task_samples={} task_tap_visits={} largest_task_samples={} largest_task_tap_visits={} replay_gap_total_ms={:.3} batch_preprocess_total_ms={:.3} bucket_bytes_total={} bucket_bytes_max={} bucket_build_total_ms={:.3} bucket_build={} local_alloc_zero_total_ms={:.3} local_alloc_zero={} worker_replan_grid_total_ms={:.3} worker_replan_grid={} block_wall_total_ms={:.3} block_wall={} stage_nonworker_wall_ms={:.3} stage_accounted_ms={:.3} stage_unaccounted_ms={:.3} merge_total_ms={:.3} merge={} worker_task_count={} worker_samples={} worker_tap_visits={} worker_tap_visits_per_s={} worker_samples_per_s={} worker_active_total_ms={:.3} worker_active={} worker_elapsed={} worker_capacity_ms={:.3} worker_utilization_pct={:.3} worker_tail_idle_ms={:.3} stage_worker_capacity_ms={:.3} stage_worker_utilization_pct={:.3} stage_worker_idle_ms={:.3} stage_tap_visits_per_s={:.3} stage_samples_per_s={:.3} active_weighted_tap_visits_per_s={:.3} active_weighted_samples_per_s={:.3} tile_flush_ms={:.3} tile_flush_count={} tile_eviction_count={} merged_tiles={} active_tile_wait_events=0 tasks_skipped_due_to_active_tile=0 stage_total_ms={:.3}",
            self.stage,
            requested_threads,
            stats_triplet(&actual_threads),
            self.tile_shape[0],
            self.tile_shape[1],
            self.tile_anchor,
            self.tile_origin[0],
            self.tile_origin[1],
            self.tile_count,
            self.resident_tile_limit,
            self.blocks.len(),
            task_counts.iter().sum::<usize>(),
            samples_total,
            tap_visits_total,
            stats_triplet(&sample_counts),
            stats_triplet(&tap_visits),
            stats_triplet(&largest_task_samples),
            stats_triplet(&largest_task_tap_visits),
            replay_gap_total_ms,
            batch_preprocess_total_ms,
            bucket_bytes_total,
            bucket_bytes_max,
            duration_total_ms(&bucket_build),
            duration_stats_triplet(&bucket_build),
            duration_total_ms(&local_alloc_zero),
            duration_stats_triplet(&local_alloc_zero),
            duration_total_ms(&worker_replan_grid),
            duration_stats_triplet(&worker_replan_grid),
            block_wall_total_ms,
            duration_stats_triplet(&block_wall),
            stage_nonworker_wall_ms,
            accounted_stage_ms,
            stage_unaccounted_ms,
            duration_total_ms(&merge),
            duration_stats_triplet(&merge),
            stats_triplet(&worker_task_counts),
            stats_triplet(&worker_sample_counts),
            stats_triplet(&worker_tap_visits),
            f64_stats_triplet(&worker_tap_visits_per_s, "per_s"),
            f64_stats_triplet(&worker_samples_per_s, "per_s"),
            worker_active_total_ms,
            duration_stats_triplet(&worker_active),
            duration_stats_triplet(&worker_elapsed),
            worker_capacity_ms,
            worker_utilization_pct,
            worker_tail_idle_ms,
            stage_worker_capacity_ms,
            stage_worker_utilization_pct,
            stage_worker_idle_ms,
            stage_tap_visits_per_s,
            stage_samples_per_s,
            active_weighted_tap_visits_per_s,
            active_weighted_samples_per_s,
            profile::millis(self.flush_duration),
            self.tile_flush_count,
            self.tile_eviction_count,
            self.blocks
                .iter()
                .map(|block| block.merged_tiles)
                .sum::<usize>(),
            stage_total_ms,
        );
    }
}

impl<'a> StandardMfsTiledCpuExecutor<'a> {
    pub(crate) fn new_with_execution_config(
        gridder: &'a StandardGridder,
        execution_config: StandardMfsExecutionConfig,
    ) -> Result<Self, ImagingError> {
        let partition =
            standard_mfs_tile_partition_for_gridder_with_config(gridder, execution_config.clone())?;
        let resident_tile_limit = standard_mfs_tile_resident_limit(
            &partition,
            execution_config.fixed_tile_resident_bytes,
        );
        Ok(Self {
            gridder,
            partition,
            resident_tile_limit,
            observability_callback: execution_config.observability_callback,
        })
    }

    pub(crate) fn has_all_resident_tiles(&self) -> bool {
        self.resident_tile_limit >= self.partition.tile_count()
    }

    /// Accumulate dirty PSF and residual grids through resident halo tiles.
    pub(crate) fn accumulate_dirty_grids(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        probe_standard_mfs_tile_buckets_with_partition(self.gridder, &self.partition, batches)?;
        if self.resident_tile_limit >= self.partition.tile_count() {
            return self.accumulate_dirty_grids_direct(batches, psf_grid, residual_grid);
        }
        let mut cache = DirtyTileCache::new(
            &self.partition,
            self.resident_tile_limit,
            psf_grid,
            residual_grid,
        );
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "dirty",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(self.gridder, &self.partition, batches)?;
        let bucket_build = bucket_started.elapsed();
        accumulation.skipped_samples += buckets.skipped_samples();
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_dirty_block(
                batches,
                &buckets,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        if standard_mfs_per_block_flush_enabled() {
            let flush_started = Instant::now();
            cache.flush_all();
            scheduler_profile.add_flush_duration(flush_started.elapsed());
        }
        let flush_started = Instant::now();
        cache.flush_all();
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(cache.flushed_tiles(), cache.evicted_tiles());
        scheduler_profile.log();
        Ok(accumulation)
    }

    /// Accumulate only the PSF grid through resident halo tiles.
    pub(crate) fn accumulate_psf_grid(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        if self.resident_tile_limit >= self.partition.tile_count() {
            return self.accumulate_psf_grid_direct(batches, psf_grid);
        }
        let mut cache = PsfTileCache::new(&self.partition, self.resident_tile_limit, psf_grid);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "psf",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(self.gridder, &self.partition, batches)?;
        let bucket_build = bucket_started.elapsed();
        accumulation.skipped_samples += buckets.skipped_samples();
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_psf_block(
                batches,
                &buckets,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        if standard_mfs_per_block_flush_enabled() {
            let flush_started = Instant::now();
            cache.flush_all();
            scheduler_profile.add_flush_duration(flush_started.elapsed());
        }
        let flush_started = Instant::now();
        cache.flush_all();
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(cache.flushed_tiles(), cache.evicted_tiles());
        scheduler_profile.log();
        Ok(accumulation)
    }

    fn accumulate_dirty_block(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        cache: &mut DirtyTileCache<'_, '_>,
        accumulation: &mut StandardMfsDirtyAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            let mut merge_duration = Duration::ZERO;
            let mut merged_count = 0usize;
            for task in &tasks {
                let (buffer, task_accumulation, timing) =
                    self.grid_dirty_tile_task(batches, buckets, task.tile_id)?;
                let merge_started = Instant::now();
                merge_dirty_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.add(task_accumulation);
                task_timing.add(timing);
                merged_count += 1;
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: merge_duration,
                    merged_tiles: merged_count,
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut outputs = Vec::<
            Vec<(
                DirtyTileBuffer,
                StandardMfsDirtyAccumulation,
                StandardMfsTileTaskTiming,
            )>,
        >::new();
        let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs = Vec::<(
                        DirtyTileBuffer,
                        StandardMfsDirtyAccumulation,
                        StandardMfsTileTaskTiming,
                    )>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output = self.grid_dirty_tile_task(batches, buckets, task.tile_id)?;
                        worker_profile.record_task(*task, output.2);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled dirty worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(())
        })?;

        let merge_started = Instant::now();
        let mut merged = outputs.into_iter().flatten().collect::<Vec<_>>();
        merged.sort_unstable_by_key(|(buffer, _, _)| buffer.id);
        let merged_count = merged.len();
        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (buffer, task_accumulation, timing) in merged {
            merge_dirty_tile_buffer_into_cache(cache, buffer)?;
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        let merge_duration = merge_started.elapsed();
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: merge_duration,
                merged_tiles: merged_count,
                worker_profiles,
            },
        ))
    }

    fn accumulate_dirty_grids_direct(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self.accumulate_dirty_grids_global_serial(batches, psf_grid, residual_grid);
        }
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "dirty",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(self.gridder, &self.partition, batches)?;
        let bucket_build = bucket_started.elapsed();
        accumulation.skipped_samples += buckets.skipped_samples();
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_dirty_block_direct(
                batches,
                &buckets,
                &store,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_dirty_grids_direct_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self.accumulate_dirty_grids_global_serial_replay(
                replay_weighted_batches,
                psf_grid,
                residual_grid,
            );
        }
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "dirty",
            &self.partition,
            self.resident_tile_limit,
        );

        replay_weighted_batches(&mut |batches| {
            scheduler_profile.record_replay_gap_now();
            let preprocess_started = Instant::now();
            for batch in batches {
                batch.validate()?;
                accumulation.max_abs_w_lambda = batch
                    .w_lambda
                    .iter()
                    .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                        max_value.max(value.abs())
                    });
            }
            scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                batches,
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            if buckets.accepted_samples() > 0 {
                let block_profile = self.accumulate_dirty_block_direct(
                    batches,
                    &buckets,
                    &store,
                    &mut accumulation,
                    bucket_build,
                )?;
                scheduler_profile.record(block_profile);
            }
            Ok(())
        })?;

        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    fn enqueue_dirty_batches_to_tile_inbox(
        &self,
        batches: &[VisibilityBatch],
        mode: StandardMfsTileSampleRouteMode,
        next_input_seq: &mut u64,
        enqueue: &mut dyn FnMut(
            StandardMfsTileId,
            StandardMfsTileVisibilityRun,
        ) -> Result<(), ImagingError>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let router = StandardMfsTileSampleRouter::new(self.gridder, &self.partition, mode);
        let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            for sample_index in 0..batch.len() {
                match router.route_batch_sample(batch, sample_index, *next_input_seq)? {
                    StandardMfsTileSampleRouteDecision::Enqueue(tile_id, sample) => {
                        *next_input_seq = (*next_input_seq).saturating_add(1);
                        run_accumulator.push_sample(tile_id, sample)?;
                    }
                    StandardMfsTileSampleRouteDecision::Density(_) => {
                        accumulation.skipped_samples += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(_) => {
                        accumulation.skipped_samples += 1;
                    }
                }
            }
        }
        run_accumulator.flush()?;
        Ok(accumulation)
    }

    fn enqueue_planned_dirty_samples_to_tile_inbox(
        &self,
        samples: &[StandardMfsPlannedWeightedSample],
        psf_only: bool,
        next_input_seq: &mut u64,
        enqueue: &mut dyn FnMut(
            StandardMfsTileId,
            StandardMfsTileVisibilityRun,
        ) -> Result<(), ImagingError>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.planned_samples_single_owner_tile(samples, false) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let flags = if psf_only {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                } else if sample.finite_visibility() {
                    STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
                } else {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                };
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: if psf_only {
                        Complex32::new(0.0, 0.0)
                    } else {
                        sample.visibility
                    },
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            enqueue(tile_id, run)?;
            return Ok(accumulation);
        }
        let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
        for &sample in samples {
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_samples += 1;
                continue;
            };
            if !(sample.grid_weight.is_finite() && sample.grid_weight > 0.0) {
                accumulation.skipped_samples += 1;
                continue;
            }
            accumulation.max_abs_w_lambda =
                accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
            let flags = if psf_only {
                STANDARD_MFS_TILE_FLAG_PSF_ONLY
            } else if sample.finite_visibility() {
                STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
            } else {
                STANDARD_MFS_TILE_FLAG_PSF_ONLY
            };
            let queued = StandardMfsTileQueueSample {
                center_x: sample.center_x,
                center_y: sample.center_y,
                flags,
                raw_weight: sample.grid_weight,
                sumwt_factor: 1.0,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                visibility: if psf_only {
                    Complex32::new(0.0, 0.0)
                } else {
                    sample.visibility
                },
                input_seq: *next_input_seq,
            };
            *next_input_seq = (*next_input_seq).saturating_add(1);
            run_accumulator.push_sample(tile_id, queued)?;
        }
        run_accumulator.flush()?;
        Ok(accumulation)
    }

    fn planned_samples_single_owner_tile(
        &self,
        samples: &[StandardMfsPlannedWeightedSample],
        require_finite_visibility: bool,
    ) -> Option<StandardMfsTileId> {
        let first = *samples.first()?;
        if !(first.grid_weight.is_finite() && first.grid_weight > 0.0) {
            return None;
        }
        if require_finite_visibility && !first.finite_visibility() {
            return None;
        }
        let tile_id = self
            .partition
            .owner(first.center_x as usize, first.center_y as usize)?;
        let tile = self.partition.tile(tile_id)?;
        for sample in samples {
            if !(sample.grid_weight.is_finite() && sample.grid_weight > 0.0) {
                return None;
            }
            if require_finite_visibility && !sample.finite_visibility() {
                return None;
            }
            let center_x = sample.center_x as usize;
            let center_y = sample.center_y as usize;
            if center_x < tile.interior.x0
                || center_x >= tile.interior.x1
                || center_y < tile.interior.y0
                || center_y >= tile.interior.y1
            {
                return None;
            }
        }
        Some(tile_id)
    }

    #[allow(dead_code)]
    fn routed_samples_single_owner_tile(
        &self,
        samples: &[StandardMfsRoutedSample],
        require_finite_visibility: bool,
    ) -> Option<StandardMfsTileId> {
        let first = *samples.first()?;
        if !(first.natural_weight.is_finite()
            && first.natural_weight > 0.0
            && first.sumwt_factor.is_finite()
            && first.sumwt_factor > 0.0)
        {
            return None;
        }
        if require_finite_visibility && !first.finite_visibility() {
            return None;
        }
        let tile_id = self
            .partition
            .owner(first.center_x as usize, first.center_y as usize)?;
        let tile = self.partition.tile(tile_id)?;
        for sample in samples {
            if !(sample.natural_weight.is_finite()
                && sample.natural_weight > 0.0
                && sample.sumwt_factor.is_finite()
                && sample.sumwt_factor > 0.0)
            {
                return None;
            }
            if require_finite_visibility && !sample.finite_visibility() {
                return None;
            }
            let center_x = sample.center_x as usize;
            let center_y = sample.center_y as usize;
            if center_x < tile.interior.x0
                || center_x >= tile.interior.x1
                || center_y < tile.interior.y0
                || center_y >= tile.interior.y1
            {
                return None;
            }
        }
        Some(tile_id)
    }

    fn push_planned_dirty_samples_to_run_accumulator(
        &self,
        samples: &[StandardMfsPlannedWeightedSample],
        psf_only: bool,
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.planned_samples_single_owner_tile(samples, false) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let flags = if psf_only {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                } else if sample.finite_visibility() {
                    STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
                } else {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                };
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: if psf_only {
                        Complex32::new(0.0, 0.0)
                    } else {
                        sample.visibility
                    },
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            return Ok(accumulation);
        }

        let mut index = 0usize;
        while index < samples.len() {
            let sample = samples[index];
            if !(sample.grid_weight.is_finite() && sample.grid_weight > 0.0) {
                accumulation.skipped_samples += 1;
                index += 1;
                continue;
            }
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_samples += 1;
                index += 1;
                continue;
            };

            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < samples.len() {
                let candidate = samples[segment_end];
                if !(candidate.grid_weight.is_finite() && candidate.grid_weight > 0.0) {
                    break;
                }
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate.center_x as usize, candidate.center_y as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }

            let mut run = StandardMfsTileVisibilityRun::with_capacity(
                segment_end - segment_start,
                *next_input_seq,
            );
            for &sample in &samples[segment_start..segment_end] {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let flags = if psf_only {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                } else if sample.finite_visibility() {
                    STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
                } else {
                    STANDARD_MFS_TILE_FLAG_PSF_ONLY
                };
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: if psf_only {
                        Complex32::new(0.0, 0.0)
                    } else {
                        sample.visibility
                    },
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    #[allow(dead_code)]
    fn push_routed_dirty_samples_to_run_accumulator(
        &self,
        samples: &[StandardMfsRoutedSample],
        psf_only: bool,
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.routed_samples_single_owner_tile(samples, false) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                run.push_sample(StandardMfsTileQueueSample::from_routed(
                    sample,
                    psf_only,
                    *next_input_seq,
                ));
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            return Ok(accumulation);
        }

        let mut index = 0usize;
        while index < samples.len() {
            let sample = samples[index];
            if !(sample.natural_weight.is_finite()
                && sample.natural_weight > 0.0
                && sample.sumwt_factor.is_finite()
                && sample.sumwt_factor > 0.0)
            {
                accumulation.skipped_samples += 1;
                index += 1;
                continue;
            }
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_samples += 1;
                index += 1;
                continue;
            };

            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < samples.len() {
                let candidate = samples[segment_end];
                if !(candidate.natural_weight.is_finite()
                    && candidate.natural_weight > 0.0
                    && candidate.sumwt_factor.is_finite()
                    && candidate.sumwt_factor > 0.0)
                {
                    break;
                }
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate.center_x as usize, candidate.center_y as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }

            let mut run = StandardMfsTileVisibilityRun::with_capacity(
                segment_end - segment_start,
                *next_input_seq,
            );
            for &sample in &samples[segment_start..segment_end] {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                run.push_sample(StandardMfsTileQueueSample::from_routed(
                    sample,
                    psf_only,
                    *next_input_seq,
                ));
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    fn push_routed_visibility_run_to_accumulator(
        &self,
        routed_run: &StandardMfsRoutedVisibilityRun,
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        if routed_run.is_empty() {
            return Ok(accumulation);
        }
        let mut index = 0usize;
        while index < routed_run.len() {
            let center = routed_run.tap_centers[index];
            let Some(tile_id) = self.partition.owner(center[0] as usize, center[1] as usize) else {
                accumulation.skipped_samples += 1;
                index += 1;
                continue;
            };
            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < routed_run.len() {
                let candidate = routed_run.tap_centers[segment_end];
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate[0] as usize, candidate[1] as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }
            let run = StandardMfsTileVisibilityRun::from_routed_visibility_run(
                routed_run,
                segment_start..segment_end,
                *next_input_seq,
            );
            accumulation.max_abs_w_lambda = accumulation.max_abs_w_lambda.max(
                routed_run.row.uvw_m[2].abs()
                    * routed_run.row.channel_lambda_scales
                        [routed_run.source_slot_range.start + segment_start]
                        .abs(),
            );
            *next_input_seq = (*next_input_seq).saturating_add(run.len() as u64);
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    fn grid_dirty_tile_queue_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        store: &DirectDirtyTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct dirty tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let taps = run.positive_taps_at(sample_index, self.gridder)?;
                let grid_weight = run.grid_weight_at(sample_index);
                if !(grid_weight.is_finite() && grid_weight > 0.0) {
                    return Err(ImagingError::InvalidRequest(
                        "standard MFS tile inbox dirty sample has invalid queued weight"
                            .to_string(),
                    ));
                }
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                if run.finite_visibility_at(sample_index) {
                    let visibility = run.visibility_at(sample_index);
                    let residual = Complex64::new(
                        f64::from(visibility.re) * grid_weight,
                        f64::from(visibility.im) * grid_weight,
                    );
                    self.gridder
                        .grid_sample_taps_real_complex_pair_planned_f64_with_offset(
                            &mut buffer.psf_grid,
                            grid_weight,
                            &mut buffer.residual_grid,
                            residual,
                            &taps,
                            offset,
                        );
                } else {
                    debug_assert!(run.psf_only_at(sample_index));
                    self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                        &mut buffer.psf_grid,
                        &taps,
                        grid_weight,
                        offset,
                    );
                }
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn grid_dirty_tile_queue_routed_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        store: &DirectDirtyTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct dirty tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let Some(sample) = run.routed_queue_sample_at(sample_index, true)? else {
                    accumulation.skipped_samples += 1;
                    continue;
                };
                let taps = sample.positive_taps(self.gridder)?;
                let Some(grid_weight) = sample.weighted_grid_weight(weighting_plan)? else {
                    accumulation.skipped_samples += 1;
                    continue;
                };
                if !(grid_weight.is_finite() && grid_weight > 0.0) {
                    accumulation.skipped_samples += 1;
                    continue;
                }
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                if let StandardMfsRoutedQueueVisibility::Finite(visibility) = sample.visibility {
                    let residual = Complex64::new(
                        f64::from(visibility.re) * grid_weight,
                        f64::from(visibility.im) * grid_weight,
                    );
                    self.gridder
                        .grid_sample_taps_real_complex_pair_planned_f64_with_offset(
                            &mut buffer.psf_grid,
                            grid_weight,
                            &mut buffer.residual_grid,
                            residual,
                            &taps,
                            offset,
                        );
                } else {
                    self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                        &mut buffer.psf_grid,
                        &taps,
                        grid_weight,
                        offset,
                    );
                }
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_dirty_grids_direct_owned_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
        _max_live_row_blocks: usize,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            let mut borrowed_replay =
                |consumer: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                    replay_weighted_batches(&mut |batches| consumer(&batches))
                };
            return self.accumulate_dirty_grids_global_serial_replay(
                &mut borrowed_replay,
                psf_grid,
                residual_grid,
            );
        }
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "dirty",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_batches(&mut |batches| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_dirty_batches_to_tile_inbox(
                        &batches,
                        StandardMfsTileSampleRouteMode::DirtyWithData,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    producer_preprocess += started.elapsed();
                    accumulation.add(block_accumulation);
                    Ok(())
                })
            },
            |tile_id, samples| self.grid_dirty_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        let flush_duration = flush_started.elapsed();
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "dirty",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            stage_total: stage_started.elapsed(),
            producer_preprocess,
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=dirty tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_duration),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity, dead_code)]
    pub(crate) fn accumulate_dirty_grids_direct_planned_replay(
        &self,
        replay_weighted_samples: &mut dyn FnMut(
            &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_dirty",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_samples(&mut |samples| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_planned_dirty_samples_to_tile_inbox(
                        samples,
                        false,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    producer_preprocess += started.elapsed();
                    accumulation.add(block_accumulation);
                    Ok(())
                })
            },
            |tile_id, samples| self.grid_dirty_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_dirty",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_dirty tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_dirty_grids_direct_planned_run_replay(
        &self,
        replay_weighted_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsPlannedWeightedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_run_dirty",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_weighted_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_planned_dirty_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                false,
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| self.grid_dirty_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_run_dirty",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_run_dirty tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    #[allow(dead_code)]
    pub(crate) fn accumulate_dirty_grids_direct_routed_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_run_dirty",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_routed_dirty_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                false,
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_dirty_tile_queue_routed_samples(tile_id, samples, weighting_plan, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_run_dirty",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_run_dirty tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_dirty_grids_direct_routed_visibility_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedVisibilityRun) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_visibility_run_dirty",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |routed_run| {
                    let started = Instant::now();
                    let block_accumulation = self.push_routed_visibility_run_to_accumulator(
                        routed_run,
                        &mut next_input_seq,
                        &mut run_accumulator,
                    )?;
                    accumulation.add(block_accumulation);
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_dirty_tile_queue_routed_samples(tile_id, samples, weighting_plan, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid, residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_visibility_run_dirty",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_visibility_run_dirty tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    fn accumulate_psf_grid_direct(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self.accumulate_psf_grid_global_serial(batches, psf_grid);
        }
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let store = DirectPsfTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "psf",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(self.gridder, &self.partition, batches)?;
        let bucket_build = bucket_started.elapsed();
        accumulation.skipped_samples += buckets.skipped_samples();
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_psf_block_direct(
                batches,
                &buckets,
                &store,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_psf_grid_direct_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self
                .accumulate_psf_grid_global_serial_replay(replay_weighted_batches, psf_grid);
        }
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let store = DirectPsfTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "psf",
            &self.partition,
            self.resident_tile_limit,
        );

        replay_weighted_batches(&mut |batches| {
            scheduler_profile.record_replay_gap_now();
            let preprocess_started = Instant::now();
            for batch in batches {
                batch.validate()?;
                accumulation.max_abs_w_lambda = batch
                    .w_lambda
                    .iter()
                    .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                        max_value.max(value.abs())
                    });
            }
            scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                batches,
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            if buckets.accepted_samples() > 0 {
                let block_profile = self.accumulate_psf_block_direct(
                    batches,
                    &buckets,
                    &store,
                    &mut accumulation,
                    bucket_build,
                )?;
                scheduler_profile.record(block_profile);
            }
            Ok(())
        })?;

        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity, dead_code)]
    pub(crate) fn accumulate_psf_grid_direct_planned_replay(
        &self,
        replay_weighted_samples: &mut dyn FnMut(
            &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectPsfTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_psf",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_samples(&mut |samples| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_planned_dirty_samples_to_tile_inbox(
                        samples,
                        true,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    producer_preprocess += started.elapsed();
                    accumulation.add(block_accumulation);
                    Ok(())
                })
            },
            |tile_id, samples| self.grid_psf_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_psf",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_psf tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_psf_grid_direct_planned_run_replay(
        &self,
        replay_weighted_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsPlannedWeightedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectPsfTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_run_psf",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_weighted_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_planned_dirty_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                true,
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| self.grid_psf_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_run_psf",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_run_psf tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    #[allow(dead_code)]
    pub(crate) fn accumulate_psf_grid_direct_routed_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectPsfTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_run_psf",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_routed_dirty_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                true,
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_psf_tile_queue_routed_samples(tile_id, samples, weighting_plan, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_run_psf",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_run_psf tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_psf_grid_direct_routed_visibility_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedVisibilityRun) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let store = DirectPsfTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_visibility_run_psf",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |routed_run| {
                    let started = Instant::now();
                    let block_accumulation = self.push_routed_visibility_run_to_accumulator(
                        routed_run,
                        &mut next_input_seq,
                        &mut run_accumulator,
                    )?;
                    accumulation.add(block_accumulation);
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_psf_tile_queue_routed_samples(tile_id, samples, weighting_plan, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_visibility_run_psf",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_visibility_run_psf tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    fn grid_psf_tile_queue_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        store: &DirectPsfTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard.as_mut().expect("direct PSF tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let taps = run.positive_taps_at(sample_index, self.gridder)?;
                let grid_weight = run.grid_weight_at(sample_index);
                if !(grid_weight.is_finite() && grid_weight > 0.0) {
                    return Err(ImagingError::InvalidRequest(
                        "standard MFS tile inbox PSF sample has invalid queued weight".to_string(),
                    ));
                }
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                    &mut buffer.psf_grid,
                    &taps,
                    grid_weight,
                    offset,
                );
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn grid_psf_tile_queue_routed_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        store: &DirectPsfTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard.as_mut().expect("direct PSF tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let Some(sample) = run.routed_queue_sample_at(sample_index, true)? else {
                    accumulation.skipped_samples += 1;
                    continue;
                };
                let taps = sample.positive_taps(self.gridder)?;
                let Some(grid_weight) = sample.weighted_grid_weight(weighting_plan)? else {
                    accumulation.skipped_samples += 1;
                    continue;
                };
                if !(grid_weight.is_finite() && grid_weight > 0.0) {
                    accumulation.skipped_samples += 1;
                    continue;
                }
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                    &mut buffer.psf_grid,
                    &taps,
                    grid_weight,
                    offset,
                );
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_psf_grid_direct_owned_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
        _max_live_row_blocks: usize,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            let mut borrowed_replay =
                |consumer: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                    replay_weighted_batches(&mut |batches| consumer(&batches))
                };
            return self.accumulate_psf_grid_global_serial_replay(&mut borrowed_replay, psf_grid);
        }
        let store = DirectPsfTileStore::new(&self.partition);
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "psf",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_batches(&mut |batches| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_dirty_batches_to_tile_inbox(
                        &batches,
                        StandardMfsTileSampleRouteMode::PsfNoData,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    producer_preprocess += started.elapsed();
                    accumulation.add(block_accumulation);
                    Ok(())
                })
            },
            |tile_id, samples| self.grid_psf_tile_queue_samples(tile_id, samples, &store),
        )?;
        for task_output in &output.task_outputs {
            accumulation.add(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(psf_grid)?;
        let flush_duration = flush_started.elapsed();
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "psf",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            stage_total: stage_started.elapsed(),
            producer_preprocess,
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=psf tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_duration),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    fn grid_dirty_tile_task(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        tile_id: StandardMfsTileId,
    ) -> Result<
        (
            DirtyTileBuffer,
            StandardMfsDirtyAccumulation,
            StandardMfsTileTaskTiming,
        ),
        ImagingError,
    > {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let shape = (tile.halo.width(), tile.halo.height());
        let offset = tile_offset(tile);
        let alloc_started = profile::maybe_profile_now();
        let mut buffer = DirtyTileBuffer {
            id: tile_id,
            psf_grid: Array2::zeros(shape),
            residual_grid: Array2::zeros(shape),
        };
        let local_alloc_zero = profile::elapsed_since(alloc_started);
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let row_block = BorrowedStandardMfsRowBlock { batches, buckets };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            if sample.finite_visibility() {
                let observed_visibility = row_block.visibility(sample.sample_id)?;
                let residual = Complex64::new(
                    f64::from(observed_visibility.re) * grid_weight,
                    f64::from(observed_visibility.im) * grid_weight,
                );
                self.gridder
                    .grid_sample_taps_real_complex_pair_planned_f64_with_offset(
                        &mut buffer.psf_grid,
                        grid_weight,
                        &mut buffer.residual_grid,
                        residual,
                        &taps,
                        offset,
                    );
            } else {
                self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                    &mut buffer.psf_grid,
                    &taps,
                    grid_weight,
                    offset,
                );
            }
        }
        Ok((
            buffer,
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn accumulate_dirty_block_direct(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        store: &DirectDirtyTileStore<'_>,
        accumulation: &mut StandardMfsDirtyAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        let mut outputs =
            Vec::<Vec<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>>::new();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            for task in &tasks {
                let (task_accumulation, timing) =
                    self.grid_dirty_tile_task_direct(batches, buckets, task.tile_id, store)?;
                accumulation.add(task_accumulation);
                task_timing.add(timing);
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs =
                        Vec::<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output = self.grid_dirty_tile_task_direct(
                            batches,
                            buckets,
                            task.tile_id,
                            store,
                        )?;
                        worker_profile.record_task(*task, output.1);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled dirty worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(())
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (task_accumulation, timing) in outputs.into_iter().flatten() {
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
                worker_profiles,
            },
        ))
    }

    fn grid_dirty_tile_task_direct(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        tile_id: StandardMfsTileId,
        store: &DirectDirtyTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct dirty tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let row_block = BorrowedStandardMfsRowBlock { batches, buckets };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            if sample.finite_visibility() {
                let observed_visibility = row_block.visibility(sample.sample_id)?;
                let residual = Complex64::new(
                    f64::from(observed_visibility.re) * grid_weight,
                    f64::from(observed_visibility.im) * grid_weight,
                );
                self.gridder
                    .grid_sample_taps_real_complex_pair_planned_f64_with_offset(
                        &mut buffer.psf_grid,
                        grid_weight,
                        &mut buffer.residual_grid,
                        residual,
                        &taps,
                        offset,
                    );
            } else {
                self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                    &mut buffer.psf_grid,
                    &taps,
                    grid_weight,
                    offset,
                );
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn plan_dirty_sample_taps(
        &self,
        batch: &VisibilityBatch,
        sample_index: usize,
        accumulation: &mut StandardMfsDirtyAccumulation,
    ) -> Option<PositiveTapSet> {
        if !batch.gridable[sample_index] {
            accumulation.skipped_samples += 1;
            return None;
        }
        let weight = batch.weight[sample_index];
        let sumwt_factor = batch.sumwt_factor[sample_index];
        if !(weight.is_finite() && weight > 0.0 && sumwt_factor.is_finite() && sumwt_factor > 0.0) {
            accumulation.skipped_samples += 1;
            return None;
        }
        self.gridder
            .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
            .or_else(|| {
                accumulation.skipped_samples += 1;
                None
            })
    }

    fn accumulate_dirty_grids_global_serial(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut replay =
            |consume: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                consume(batches)
            };
        self.accumulate_dirty_grids_global_serial_replay(&mut replay, psf_grid, residual_grid)
    }

    fn accumulate_dirty_grids_global_serial_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        let stage_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        let mut finite_visibility_samples = 0usize;
        let mut psf_only_samples = 0usize;

        if let (Some(psf_storage), Some(residual_storage)) = (
            psf_grid.as_slice_memory_order_mut(),
            residual_grid.as_slice_memory_order_mut(),
        ) {
            replay_weighted_batches(&mut |batches| {
                for batch in batches {
                    batch.validate()?;
                    accumulation.max_abs_w_lambda = batch
                        .w_lambda
                        .iter()
                        .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                            max_value.max(value.abs())
                        });
                    let samples = batch
                        .gridable
                        .iter()
                        .copied()
                        .zip(batch.weight.iter().copied())
                        .zip(batch.sumwt_factor.iter().copied())
                        .zip(batch.u_lambda.iter().copied())
                        .zip(batch.v_lambda.iter().copied())
                        .zip(batch.visibility.iter().copied());
                    for (
                        ((((gridable, weight), sumwt_factor), u_lambda), v_lambda),
                        observed_visibility,
                    ) in samples
                    {
                        if !gridable {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        if !(weight.is_finite()
                            && weight > 0.0
                            && sumwt_factor.is_finite()
                            && sumwt_factor > 0.0)
                        {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        let Some(taps) = self.gridder.plan_positive_taps(u_lambda, v_lambda) else {
                            accumulation.skipped_samples += 1;
                            continue;
                        };
                        let grid_weight = weight * sumwt_factor;
                        if !(grid_weight.is_finite() && grid_weight > 0.0) {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        let grid_weight = f64::from(grid_weight);
                        accumulation.normalization_sumwt += grid_weight;
                        accumulation.reported_sumwt += grid_weight;
                        accumulation.gridded_samples += 1;
                        if finite_visibility(observed_visibility) {
                            finite_visibility_samples += 1;
                            let residual = Complex64::new(
                                f64::from(observed_visibility.re) * grid_weight,
                                f64::from(observed_visibility.im) * grid_weight,
                            );
                            self.gridder
                                .grid_sample_taps_real_complex_pair_planned_f64_storage(
                                    psf_storage,
                                    grid_weight,
                                    residual_storage,
                                    residual,
                                    &taps,
                                );
                        } else {
                            psf_only_samples += 1;
                            self.gridder.grid_sample_taps_real_planned_f64_storage(
                                psf_storage,
                                &taps,
                                grid_weight,
                            );
                        }
                    }
                }
                Ok(())
            })?;
        } else {
            replay_weighted_batches(&mut |batches| {
                for batch in batches {
                    batch.validate()?;
                    accumulation.max_abs_w_lambda = batch
                        .w_lambda
                        .iter()
                        .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                            max_value.max(value.abs())
                        });
                    let samples = batch
                        .gridable
                        .iter()
                        .copied()
                        .zip(batch.weight.iter().copied())
                        .zip(batch.sumwt_factor.iter().copied())
                        .zip(batch.u_lambda.iter().copied())
                        .zip(batch.v_lambda.iter().copied())
                        .zip(batch.visibility.iter().copied());
                    for (
                        ((((gridable, weight), sumwt_factor), u_lambda), v_lambda),
                        observed_visibility,
                    ) in samples
                    {
                        if !gridable {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        if !(weight.is_finite()
                            && weight > 0.0
                            && sumwt_factor.is_finite()
                            && sumwt_factor > 0.0)
                        {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        let Some(taps) = self.gridder.plan_positive_taps(u_lambda, v_lambda) else {
                            accumulation.skipped_samples += 1;
                            continue;
                        };
                        let grid_weight = weight * sumwt_factor;
                        if !(grid_weight.is_finite() && grid_weight > 0.0) {
                            accumulation.skipped_samples += 1;
                            continue;
                        }
                        let grid_weight = f64::from(grid_weight);
                        accumulation.normalization_sumwt += grid_weight;
                        accumulation.reported_sumwt += grid_weight;
                        accumulation.gridded_samples += 1;
                        if finite_visibility(observed_visibility) {
                            finite_visibility_samples += 1;
                            let residual = Complex64::new(
                                f64::from(observed_visibility.re) * grid_weight,
                                f64::from(observed_visibility.im) * grid_weight,
                            );
                            self.gridder.grid_sample_taps_real_complex_pair_planned_f64(
                                psf_grid,
                                grid_weight,
                                residual_grid,
                                residual,
                                &taps,
                            );
                        } else {
                            psf_only_samples += 1;
                            self.gridder.grid_sample_taps_real_planned_f64(
                                psf_grid,
                                &taps,
                                grid_weight,
                            );
                        }
                    }
                }
                Ok(())
            })?;
        }

        let stage_duration = profile::elapsed_since(stage_started);
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "fixed_tile_one_worker_global_dirty",
            samples_total: accumulation
                .gridded_samples
                .saturating_add(accumulation.skipped_samples),
            finite_visibility_samples,
            nonfinite_visibility_samples: psf_only_samples,
            planned_samples: accumulation.gridded_samples,
            model_grid_present_samples: 0,
            model_grid_absent_samples: 0,
            degrid_tap_visits: 0,
            grid_tap_visits: accumulation.gridded_samples.saturating_mul(49),
            stage_duration,
        });
        Ok(accumulation)
    }

    fn accumulate_psf_grid_global_serial(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut replay =
            |consume: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                consume(batches)
            };
        self.accumulate_psf_grid_global_serial_replay(&mut replay, psf_grid)
    }

    fn accumulate_psf_grid_global_serial_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        let stage_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        replay_weighted_batches(&mut |batches| {
            for batch in batches {
                batch.validate()?;
                accumulation.max_abs_w_lambda = batch
                    .w_lambda
                    .iter()
                    .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                        max_value.max(value.abs())
                    });
                for sample_index in 0..batch.len() {
                    let Some(taps) =
                        self.plan_dirty_sample_taps(batch, sample_index, &mut accumulation)
                    else {
                        continue;
                    };
                    let weight = batch.weight[sample_index];
                    let sumwt_factor = batch.sumwt_factor[sample_index];
                    let grid_weight = weight * sumwt_factor;
                    if !(grid_weight.is_finite() && grid_weight > 0.0) {
                        accumulation.skipped_samples += 1;
                        continue;
                    }
                    let grid_weight = f64::from(grid_weight);
                    accumulation.normalization_sumwt += grid_weight;
                    accumulation.reported_sumwt += grid_weight;
                    accumulation.gridded_samples += 1;
                    self.gridder
                        .grid_sample_taps_real_planned_f64(psf_grid, &taps, grid_weight);
                }
            }
            Ok(())
        })?;

        let stage_duration = profile::elapsed_since(stage_started);
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "fixed_tile_one_worker_global_psf",
            samples_total: accumulation
                .gridded_samples
                .saturating_add(accumulation.skipped_samples),
            finite_visibility_samples: accumulation.gridded_samples,
            nonfinite_visibility_samples: 0,
            planned_samples: accumulation.gridded_samples,
            model_grid_present_samples: 0,
            model_grid_absent_samples: 0,
            degrid_tap_visits: 0,
            grid_tap_visits: accumulation.gridded_samples.saturating_mul(49),
            stage_duration,
        });
        Ok(accumulation)
    }

    fn accumulate_psf_block(
        &self,
        _batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        cache: &mut PsfTileCache<'_, '_>,
        accumulation: &mut StandardMfsDirtyAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            let mut merge_duration = Duration::ZERO;
            let mut merged_count = 0usize;
            for task in &tasks {
                let (buffer, task_accumulation, timing) =
                    self.grid_psf_tile_task(buckets, task.tile_id)?;
                let merge_started = Instant::now();
                merge_psf_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.add(task_accumulation);
                task_timing.add(timing);
                merged_count += 1;
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: merge_duration,
                    merged_tiles: merged_count,
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut outputs = Vec::<
            Vec<(
                PsfTileBuffer,
                StandardMfsDirtyAccumulation,
                StandardMfsTileTaskTiming,
            )>,
        >::new();
        let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs = Vec::<(
                        PsfTileBuffer,
                        StandardMfsDirtyAccumulation,
                        StandardMfsTileTaskTiming,
                    )>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output = self.grid_psf_tile_task(buckets, task.tile_id)?;
                        worker_profile.record_task(*task, output.2);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled PSF worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(())
        })?;

        let merge_started = Instant::now();
        let mut merged = outputs.into_iter().flatten().collect::<Vec<_>>();
        merged.sort_unstable_by_key(|(buffer, _, _)| buffer.id);
        let merged_count = merged.len();
        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (buffer, task_accumulation, timing) in merged {
            merge_psf_tile_buffer_into_cache(cache, buffer)?;
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        let merge_duration = merge_started.elapsed();
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: merge_duration,
                merged_tiles: merged_count,
                worker_profiles,
            },
        ))
    }

    fn grid_psf_tile_task(
        &self,
        buckets: &StandardMfsBlockTileBuckets,
        tile_id: StandardMfsTileId,
    ) -> Result<
        (
            PsfTileBuffer,
            StandardMfsDirtyAccumulation,
            StandardMfsTileTaskTiming,
        ),
        ImagingError,
    > {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let alloc_started = profile::maybe_profile_now();
        let mut buffer = PsfTileBuffer {
            id: tile_id,
            psf_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
        };
        let local_alloc_zero = profile::elapsed_since(alloc_started);
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                &mut buffer.psf_grid,
                &taps,
                grid_weight,
                offset,
            );
        }
        Ok((
            buffer,
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn accumulate_psf_block_direct(
        &self,
        _batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        store: &DirectPsfTileStore<'_>,
        accumulation: &mut StandardMfsDirtyAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        let mut outputs =
            Vec::<Vec<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>>::new();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            for task in &tasks {
                let (task_accumulation, timing) =
                    self.grid_psf_tile_task_direct(buckets, task.tile_id, store)?;
                accumulation.add(task_accumulation);
                task_timing.add(timing);
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs =
                        Vec::<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output =
                            self.grid_psf_tile_task_direct(buckets, task.tile_id, store)?;
                        worker_profile.record_task(*task, output.1);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled PSF worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(())
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (task_accumulation, timing) in outputs.into_iter().flatten() {
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
                worker_profiles,
            },
        ))
    }

    fn grid_psf_tile_task_direct(
        &self,
        buckets: &StandardMfsBlockTileBuckets,
        tile_id: StandardMfsTileId,
        store: &DirectPsfTileStore<'_>,
    ) -> Result<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard.as_mut().expect("direct PSF tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            self.gridder.grid_sample_taps_real_planned_f64_with_offset(
                &mut buffer.psf_grid,
                &taps,
                grid_weight,
                offset,
            );
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    /// Accumulate a residual-refresh grid through resident halo tiles.
    pub(crate) fn accumulate_residual_grid(
        &self,
        batches: &[VisibilityBatch],
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if self.resident_tile_limit >= self.partition.tile_count() {
            return self.accumulate_residual_grid_direct(batches, model_grid, residual_grid);
        }
        let mut cache =
            ResidualTileCache::new(&self.partition, self.resident_tile_limit, residual_grid);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "residual",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let (buckets, block_accumulation) =
            StandardMfsBlockTileBuckets::build_for_residual_refresh(
                self.gridder,
                &self.partition,
                batches,
            )?;
        let bucket_build = bucket_started.elapsed();
        accumulation.add_residual(block_accumulation);
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_residual_block(
                batches,
                &buckets,
                model_grid,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        if standard_mfs_per_block_flush_enabled() {
            let flush_started = Instant::now();
            cache.flush_all();
            scheduler_profile.add_flush_duration(flush_started.elapsed());
        }
        let flush_started = Instant::now();
        cache.flush_all();
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(cache.flushed_tiles(), cache.evicted_tiles());
        scheduler_profile.log();
        Ok(accumulation)
    }

    fn accumulate_residual_block(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        model_grid: Option<&Array2<Complex32>>,
        cache: &mut ResidualTileCache<'_, '_>,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            let mut merge_duration = Duration::ZERO;
            let mut merged_count = 0usize;
            for task in &tasks {
                let (buffer, gridded_samples, timing) =
                    self.grid_residual_tile_task(batches, buckets, model_grid, task.tile_id)?;
                let merge_started = Instant::now();
                merge_residual_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.gridded_residual_samples += gridded_samples;
                task_timing.add(timing);
                merged_count += 1;
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: merge_duration,
                    merged_tiles: merged_count,
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut outputs = Vec::<Vec<(ResidualTileBuffer, usize, StandardMfsTileTaskTiming)>>::new();
        let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs =
                        Vec::<(ResidualTileBuffer, usize, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output = self.grid_residual_tile_task(
                            batches,
                            buckets,
                            model_grid,
                            task.tile_id,
                        )?;
                        worker_profile.record_task(*task, output.2);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled residual worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(())
        })?;

        let merge_started = Instant::now();
        let mut merged = outputs.into_iter().flatten().collect::<Vec<_>>();
        merged.sort_unstable_by_key(|(buffer, _, _)| buffer.id);
        let merged_count = merged.len();
        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (buffer, gridded_samples, timing) in merged {
            merge_residual_tile_buffer_into_cache(cache, buffer)?;
            accumulation.gridded_residual_samples += gridded_samples;
            task_timing.add(timing);
        }
        let merge_duration = merge_started.elapsed();
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: merge_duration,
                merged_tiles: merged_count,
                worker_profiles,
            },
        ))
    }

    fn grid_residual_tile_task(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        model_grid: Option<&Array2<Complex32>>,
        tile_id: StandardMfsTileId,
    ) -> Result<(ResidualTileBuffer, usize, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let alloc_started = profile::maybe_profile_now();
        let mut buffer = ResidualTileBuffer {
            id: tile_id,
            residual_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
        };
        let local_alloc_zero = profile::elapsed_since(alloc_started);
        let worker_started = profile::maybe_profile_now();
        let mut gridded_samples = 0usize;
        let row_block = BorrowedStandardMfsRowBlock { batches, buckets };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let observed_visibility = row_block.visibility(sample.sample_id)?;
            let residual_weight = f64::from(sample.grid_weight);
            if let Some(model_grid) = model_grid {
                self.gridder
                    .degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
                        model_grid,
                        &mut buffer.residual_grid,
                        &taps,
                        observed_visibility,
                        residual_weight,
                        offset,
                    );
            } else {
                let residual = Complex64::new(
                    f64::from(observed_visibility.re) * residual_weight,
                    f64::from(observed_visibility.im) * residual_weight,
                );
                self.gridder.grid_sample_taps_planned_f64_with_offset(
                    &mut buffer.residual_grid,
                    &taps,
                    residual,
                    offset,
                );
            }
            gridded_samples += 1;
        }
        Ok((
            buffer,
            gridded_samples,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn accumulate_residual_grid_direct(
        &self,
        batches: &[VisibilityBatch],
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self.accumulate_residual_grid_global_serial(batches, model_grid, residual_grid);
        }
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let store = DirectResidualTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "residual",
            &self.partition,
            self.resident_tile_limit,
        );

        scheduler_profile.record_replay_gap_now();
        let preprocess_started = Instant::now();
        for batch in batches {
            batch.validate()?;
        }
        scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
        let bucket_started = Instant::now();
        let (buckets, block_accumulation) =
            StandardMfsBlockTileBuckets::build_for_residual_refresh(
                self.gridder,
                &self.partition,
                batches,
            )?;
        let bucket_build = bucket_started.elapsed();
        accumulation.add_residual(block_accumulation);
        if buckets.accepted_samples() > 0 {
            let block_profile = self.accumulate_residual_block_direct(
                batches,
                &buckets,
                model_grid,
                &store,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_residual_grid_direct_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            return self.accumulate_residual_grid_global_serial_replay(
                replay_weighted_batches,
                model_grid,
                residual_grid,
            );
        }
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let store = DirectResidualTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "residual",
            &self.partition,
            self.resident_tile_limit,
        );

        replay_weighted_batches(&mut |batches| {
            scheduler_profile.record_replay_gap_now();
            let preprocess_started = Instant::now();
            for batch in batches {
                batch.validate()?;
            }
            scheduler_profile.add_batch_preprocess_duration(preprocess_started.elapsed());
            let bucket_started = Instant::now();
            let (buckets, block_accumulation) =
                StandardMfsBlockTileBuckets::build_for_residual_refresh(
                    self.gridder,
                    &self.partition,
                    batches,
                )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.add_residual(block_accumulation);
            if buckets.accepted_samples() > 0 {
                let block_profile = self.accumulate_residual_block_direct(
                    batches,
                    &buckets,
                    model_grid,
                    &store,
                    &mut accumulation,
                    bucket_build,
                )?;
                scheduler_profile.record(block_profile);
            }
            Ok(())
        })?;

        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        scheduler_profile.add_flush_duration(flush_started.elapsed());
        scheduler_profile.set_cache_counters(flushed_tiles, 0);
        scheduler_profile.log();
        Ok(accumulation)
    }

    fn enqueue_residual_batches_to_tile_inbox(
        &self,
        batches: &[VisibilityBatch],
        next_input_seq: &mut u64,
        enqueue: &mut dyn FnMut(
            StandardMfsTileId,
            StandardMfsTileVisibilityRun,
        ) -> Result<(), ImagingError>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let router = StandardMfsTileSampleRouter::new(
            self.gridder,
            &self.partition,
            StandardMfsTileSampleRouteMode::ResidualWithData,
        );
        let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
        for batch in batches {
            batch.validate()?;
            for sample_index in 0..batch.len() {
                match router.route_batch_sample(batch, sample_index, *next_input_seq)? {
                    StandardMfsTileSampleRouteDecision::Enqueue(tile_id, sample) => {
                        accumulation.valid_samples += 1;
                        accumulation.planned_samples += 1;
                        *next_input_seq = (*next_input_seq).saturating_add(1);
                        run_accumulator.push_sample(tile_id, sample)?;
                    }
                    StandardMfsTileSampleRouteDecision::Density(_) => {
                        accumulation.skipped_out_of_grid += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(
                        StandardMfsTileSampleRouteSkip::NotGridable,
                    ) => {
                        accumulation.skipped_not_gridable += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(
                        StandardMfsTileSampleRouteSkip::InvalidWeight,
                    ) => {
                        accumulation.skipped_invalid_weight += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(
                        StandardMfsTileSampleRouteSkip::InvalidSumwt,
                    ) => {
                        accumulation.valid_samples += 1;
                        accumulation.planned_samples += 1;
                        accumulation.skipped_invalid_sumwt += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(
                        StandardMfsTileSampleRouteSkip::NonfiniteVisibility,
                    ) => {
                        accumulation.skipped_nonfinite_visibility += 1;
                    }
                    StandardMfsTileSampleRouteDecision::Skip(
                        StandardMfsTileSampleRouteSkip::OutOfGrid,
                    ) => {
                        accumulation.valid_samples += 1;
                        accumulation.skipped_out_of_grid += 1;
                    }
                }
            }
        }
        run_accumulator.flush()?;
        Ok(accumulation)
    }

    fn grid_residual_tile_queue_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        model_grid: Option<&Array2<Complex32>>,
        store: &DirectResidualTileStore<'_>,
    ) -> Result<(usize, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct residual tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut gridded_samples = 0usize;
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let taps = run.positive_taps_at(sample_index, self.gridder)?;
                let residual_weight = run.grid_weight_at(sample_index);
                if !(residual_weight.is_finite() && residual_weight > 0.0) {
                    return Err(ImagingError::InvalidRequest(
                        "standard MFS tile inbox residual sample has invalid queued weight"
                            .to_string(),
                    ));
                }
                let residual_weight = f64::from(residual_weight);
                if let Some(model_grid) = model_grid {
                    self.gridder
                        .degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
                            model_grid,
                            &mut buffer.residual_grid,
                            &taps,
                            run.visibility_at(sample_index),
                            residual_weight,
                            offset,
                        );
                } else {
                    let visibility = run.visibility_at(sample_index);
                    let residual = Complex64::new(
                        f64::from(visibility.re) * residual_weight,
                        f64::from(visibility.im) * residual_weight,
                    );
                    self.gridder.grid_sample_taps_planned_f64_with_offset(
                        &mut buffer.residual_grid,
                        &taps,
                        residual,
                        offset,
                    );
                }
                gridded_samples += 1;
            }
        }
        Ok((
            gridded_samples,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn grid_residual_tile_queue_routed_samples(
        &self,
        tile_id: StandardMfsTileId,
        samples: &StandardMfsTileQueueChunk,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        store: &DirectResidualTileStore<'_>,
    ) -> Result<
        (
            StandardMfsTiledResidualAccumulation,
            StandardMfsTileTaskTiming,
        ),
        ImagingError,
    > {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct residual tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        for run in samples.runs() {
            for sample_index in 0..run.len() {
                let Some(sample) = run.routed_queue_sample_at(sample_index, false)? else {
                    accumulation.skipped_nonfinite_visibility += 1;
                    continue;
                };
                let StandardMfsRoutedQueueVisibility::Finite(visibility) = sample.visibility else {
                    accumulation.skipped_nonfinite_visibility += 1;
                    continue;
                };
                let taps = sample.positive_taps(self.gridder)?;
                let Some(residual_weight) = sample.weighted_grid_weight(weighting_plan)? else {
                    accumulation.skipped_invalid_weight += 1;
                    continue;
                };
                let residual_weight = f64::from(residual_weight);
                if let Some(model_grid) = model_grid {
                    self.gridder
                        .degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
                            model_grid,
                            &mut buffer.residual_grid,
                            &taps,
                            visibility,
                            residual_weight,
                            offset,
                        );
                } else {
                    let residual = Complex64::new(
                        f64::from(visibility.re) * residual_weight,
                        f64::from(visibility.im) * residual_weight,
                    );
                    self.gridder.grid_sample_taps_planned_f64_with_offset(
                        &mut buffer.residual_grid,
                        &taps,
                        residual,
                        offset,
                    );
                }
                accumulation.gridded_residual_samples += 1;
            }
        }
        Ok((
            accumulation,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_residual_grid_direct_owned_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
        _max_live_row_blocks: usize,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        if standard_mfs_grid_threads() <= 1 && !standard_mfs_force_tiled_one_worker() {
            let mut borrowed_replay =
                |consumer: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                    replay_weighted_batches(&mut |batches| consumer(&batches))
                };
            return self.accumulate_residual_grid_global_serial_replay(
                &mut borrowed_replay,
                model_grid,
                residual_grid,
            );
        }
        let store = DirectResidualTileStore::new(&self.partition);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "residual",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_batches(&mut |batches| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_residual_batches_to_tile_inbox(
                        &batches,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    producer_preprocess += started.elapsed();
                    accumulation.add_residual(block_accumulation);
                    Ok(())
                })
            },
            |tile_id, samples| {
                self.grid_residual_tile_queue_samples(tile_id, samples, model_grid, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.gridded_residual_samples += task_output.output;
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        let flush_duration = flush_started.elapsed();
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "residual",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            stage_total: stage_started.elapsed(),
            producer_preprocess,
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=residual tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_duration),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity, dead_code)]
    pub(crate) fn accumulate_residual_grid_direct_planned_replay(
        &self,
        replay_weighted_samples: &mut dyn FnMut(
            &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let store = DirectResidualTileStore::new(&self.partition);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_residual",
            self.observability_callback.as_ref(),
            |enqueue| {
                replay_weighted_samples(&mut |samples| {
                    let started = Instant::now();
                    let block_accumulation = self.enqueue_planned_residual_samples_to_tile_inbox(
                        samples,
                        &mut next_input_seq,
                        enqueue,
                    )?;
                    accumulation.add_residual(block_accumulation);
                    producer_preprocess += started.elapsed();
                    Ok(())
                })
            },
            |tile_id, samples| {
                self.grid_residual_tile_queue_samples(tile_id, samples, model_grid, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.gridded_residual_samples += task_output.output;
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_residual",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_residual tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_residual_grid_direct_planned_run_replay(
        &self,
        replay_weighted_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsPlannedWeightedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let store = DirectResidualTileStore::new(&self.partition);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "planned_run_residual",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_weighted_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_planned_residual_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add_residual(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_residual_tile_queue_samples(tile_id, samples, model_grid, &store)
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.gridded_residual_samples += task_output.output;
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "planned_run_residual",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=planned_run_residual tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    #[allow(dead_code)]
    pub(crate) fn accumulate_residual_grid_direct_routed_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedSampleRunBlock) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let store = DirectResidualTileStore::new(&self.partition);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_run_residual",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |run_block| {
                    let started = Instant::now();
                    for run in run_block.runs() {
                        let block_accumulation = self
                            .push_routed_residual_samples_to_run_accumulator(
                                &run_block.samples()[run.clone()],
                                &mut next_input_seq,
                                &mut run_accumulator,
                            )?;
                        accumulation.add_residual(block_accumulation);
                    }
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_residual_tile_queue_routed_samples(
                    tile_id,
                    samples,
                    weighting_plan,
                    model_grid,
                    &store,
                )
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add_residual(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_run_residual",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_run_residual tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn accumulate_residual_grid_direct_routed_visibility_run_replay(
        &self,
        replay_routed_runs: &mut dyn FnMut(
            &mut dyn FnMut(&StandardMfsRoutedVisibilityRun) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let store = DirectResidualTileStore::new(&self.partition);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut producer_preprocess = Duration::ZERO;
        let mut next_input_seq = 0u64;
        let worker_count = standard_mfs_grid_threads();
        let stage_started = Instant::now();
        let output = run_standard_mfs_tile_inbox_scheduler(
            &self.partition,
            worker_count,
            "routed_visibility_run_residual",
            self.observability_callback.as_ref(),
            |enqueue| {
                let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
                replay_routed_runs(&mut |routed_run| {
                    let started = Instant::now();
                    let block_accumulation = self
                        .push_routed_visibility_residual_run_to_accumulator(
                            routed_run,
                            &mut next_input_seq,
                            &mut run_accumulator,
                        )?;
                    accumulation.add_residual(block_accumulation);
                    producer_preprocess += started.elapsed();
                    Ok(())
                })?;
                run_accumulator.flush()
            },
            |tile_id, samples| {
                self.grid_residual_tile_queue_routed_samples(
                    tile_id,
                    samples,
                    weighting_plan,
                    model_grid,
                    &store,
                )
            },
        )?;
        for task_output in &output.task_outputs {
            accumulation.add_residual(task_output.output);
        }
        let flush_started = Instant::now();
        let flushed_tiles = store.flush_all(residual_grid)?;
        log_tile_inbox_scheduler_summary(StandardMfsTileInboxSchedulerLogInputs {
            stage: "routed_visibility_run_residual",
            partition: &self.partition,
            requested_threads: worker_count,
            output: &output,
            producer_preprocess,
            stage_total: stage_started.elapsed(),
        });
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_tile_persistent_flush stage=routed_visibility_run_residual tile_flush_ms={:.3} tile_flush_count={}",
                profile::millis(flush_started.elapsed()),
                flushed_tiles
            );
        }
        Ok(accumulation)
    }

    fn enqueue_planned_residual_samples_to_tile_inbox(
        &self,
        samples: &[StandardMfsPlannedWeightedSample],
        next_input_seq: &mut u64,
        enqueue: &mut dyn FnMut(
            StandardMfsTileId,
            StandardMfsTileVisibilityRun,
        ) -> Result<(), ImagingError>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.planned_samples_single_owner_tile(samples, true) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: sample.visibility,
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            enqueue(tile_id, run)?;
            return Ok(accumulation);
        }

        let mut run_accumulator = StandardMfsTileRunAccumulator::new(enqueue);
        for &sample in samples {
            if !(sample.grid_weight.is_finite() && sample.grid_weight > 0.0) {
                accumulation.skipped_invalid_weight += 1;
                continue;
            }
            if !sample.finite_visibility() {
                accumulation.skipped_nonfinite_visibility += 1;
                continue;
            }
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_out_of_grid += 1;
                continue;
            };
            accumulation.valid_samples += 1;
            accumulation.planned_samples += 1;
            let queued = StandardMfsTileQueueSample {
                center_x: sample.center_x,
                center_y: sample.center_y,
                flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                raw_weight: sample.grid_weight,
                sumwt_factor: 1.0,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                visibility: sample.visibility,
                input_seq: *next_input_seq,
            };
            *next_input_seq = (*next_input_seq).saturating_add(1);
            run_accumulator.push_sample(tile_id, queued)?;
        }
        run_accumulator.flush()?;
        Ok(accumulation)
    }

    fn push_planned_residual_samples_to_run_accumulator(
        &self,
        samples: &[StandardMfsPlannedWeightedSample],
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.planned_samples_single_owner_tile(samples, true) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: sample.visibility,
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            return Ok(accumulation);
        }

        let mut index = 0usize;
        while index < samples.len() {
            let sample = samples[index];
            if !(sample.grid_weight.is_finite() && sample.grid_weight > 0.0) {
                accumulation.skipped_invalid_weight += 1;
                index += 1;
                continue;
            }
            if !sample.finite_visibility() {
                accumulation.skipped_nonfinite_visibility += 1;
                index += 1;
                continue;
            }
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_out_of_grid += 1;
                index += 1;
                continue;
            };

            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < samples.len() {
                let candidate = samples[segment_end];
                if !(candidate.grid_weight.is_finite()
                    && candidate.grid_weight > 0.0
                    && candidate.finite_visibility())
                {
                    break;
                }
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate.center_x as usize, candidate.center_y as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }

            let mut run = StandardMfsTileVisibilityRun::with_capacity(
                segment_end - segment_start,
                *next_input_seq,
            );
            for &sample in &samples[segment_start..segment_end] {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run.push_sample(StandardMfsTileQueueSample {
                    center_x: sample.center_x,
                    center_y: sample.center_y,
                    flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                    raw_weight: sample.grid_weight,
                    sumwt_factor: 1.0,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    visibility: sample.visibility,
                    input_seq: *next_input_seq,
                });
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    #[allow(dead_code)]
    fn push_routed_residual_samples_to_run_accumulator(
        &self,
        samples: &[StandardMfsRoutedSample],
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if samples.is_empty() {
            return Ok(accumulation);
        }
        if let Some(tile_id) = self.routed_samples_single_owner_tile(samples, true) {
            let mut run =
                StandardMfsTileVisibilityRun::with_capacity(samples.len(), *next_input_seq);
            for &sample in samples {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run.push_sample(StandardMfsTileQueueSample::from_routed(
                    sample,
                    false,
                    *next_input_seq,
                ));
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            return Ok(accumulation);
        }

        let mut index = 0usize;
        while index < samples.len() {
            let sample = samples[index];
            if !(sample.natural_weight.is_finite()
                && sample.natural_weight > 0.0
                && sample.sumwt_factor.is_finite()
                && sample.sumwt_factor > 0.0)
            {
                accumulation.skipped_invalid_weight += 1;
                index += 1;
                continue;
            }
            if !sample.finite_visibility() {
                accumulation.skipped_nonfinite_visibility += 1;
                index += 1;
                continue;
            }
            let Some(tile_id) = self
                .partition
                .owner(sample.center_x as usize, sample.center_y as usize)
            else {
                accumulation.skipped_out_of_grid += 1;
                index += 1;
                continue;
            };

            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < samples.len() {
                let candidate = samples[segment_end];
                if !(candidate.natural_weight.is_finite()
                    && candidate.natural_weight > 0.0
                    && candidate.sumwt_factor.is_finite()
                    && candidate.sumwt_factor > 0.0
                    && candidate.finite_visibility())
                {
                    break;
                }
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate.center_x as usize, candidate.center_y as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }

            let mut run = StandardMfsTileVisibilityRun::with_capacity(
                segment_end - segment_start,
                *next_input_seq,
            );
            for &sample in &samples[segment_start..segment_end] {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run.push_sample(StandardMfsTileQueueSample::from_routed(
                    sample,
                    false,
                    *next_input_seq,
                ));
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    fn push_routed_visibility_residual_run_to_accumulator(
        &self,
        routed_run: &StandardMfsRoutedVisibilityRun,
        next_input_seq: &mut u64,
        run_accumulator: &mut StandardMfsTileRunAccumulator<'_>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if routed_run.is_empty() {
            return Ok(accumulation);
        }
        let mut index = 0usize;
        while index < routed_run.len() {
            let center = routed_run.tap_centers[index];
            let Some(tile_id) = self.partition.owner(center[0] as usize, center[1] as usize) else {
                accumulation.skipped_out_of_grid += 1;
                index += 1;
                continue;
            };
            let segment_start = index;
            let mut segment_end = index + 1;
            while segment_end < routed_run.len() {
                let candidate = routed_run.tap_centers[segment_end];
                let Some(candidate_tile_id) = self
                    .partition
                    .owner(candidate[0] as usize, candidate[1] as usize)
                else {
                    break;
                };
                if candidate_tile_id != tile_id {
                    break;
                }
                segment_end += 1;
            }
            let run = StandardMfsTileVisibilityRun::from_routed_visibility_run(
                routed_run,
                segment_start..segment_end,
                *next_input_seq,
            );
            accumulation.valid_samples += run.len();
            accumulation.planned_samples += run.len();
            *next_input_seq = (*next_input_seq).saturating_add(run.len() as u64);
            run_accumulator.push_run(tile_id, run)?;
            index = segment_end;
        }
        Ok(accumulation)
    }

    fn accumulate_residual_block_direct(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        model_grid: Option<&Array2<Complex32>>,
        store: &DirectResidualTileStore<'_>,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        bucket_build: Duration,
    ) -> Result<StandardMfsTileSchedulerBlockProfile, ImagingError> {
        let tasks = buckets.tile_tasks_descending();
        let worker_count = standard_mfs_grid_threads().min(tasks.len().max(1));
        let started = Instant::now();
        let mut outputs = Vec::<Vec<(usize, StandardMfsTileTaskTiming)>>::new();
        if worker_count <= 1 || tasks.len() <= 1 {
            let mut task_timing = StandardMfsTileTaskTiming::default();
            for task in &tasks {
                let (gridded_samples, timing) = self.grid_residual_tile_task_direct(
                    batches,
                    buckets,
                    model_grid,
                    task.tile_id,
                    store,
                )?;
                accumulation.gridded_residual_samples += gridded_samples;
                task_timing.add(timing);
            }
            let block_wall = started.elapsed();
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall,
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                    worker_profiles: serial_tile_worker_profiles(&tasks, task_timing, block_wall),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let worker_profiles = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            let mut worker_profiles = Vec::<StandardMfsTileWorkerProfile>::new();
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let worker_started = Instant::now();
                    let mut worker_profile = StandardMfsTileWorkerProfile::default();
                    let mut worker_outputs = Vec::<(usize, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        let output = self.grid_residual_tile_task_direct(
                            batches,
                            buckets,
                            model_grid,
                            task.tile_id,
                            store,
                        )?;
                        worker_profile.record_task(*task, output.1);
                        worker_outputs.push(output);
                    }
                    worker_profile.finish(worker_started);
                    Ok::<_, ImagingError>((worker_outputs, worker_profile))
                }));
            }
            for handle in handles {
                let (worker_outputs, worker_profile) = handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled residual worker panicked".to_string(),
                    )
                })??;
                outputs.push(worker_outputs);
                worker_profiles.push(worker_profile);
            }
            Ok::<_, ImagingError>(worker_profiles)
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (gridded_samples, timing) in outputs.into_iter().flatten() {
            accumulation.gridded_residual_samples += gridded_samples;
            task_timing.add(timing);
        }
        let block_wall = started.elapsed();
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall,
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
                worker_profiles,
            },
        ))
    }

    fn grid_residual_tile_task_direct(
        &self,
        batches: &[VisibilityBatch],
        buckets: &StandardMfsBlockTileBuckets,
        model_grid: Option<&Array2<Complex32>>,
        tile_id: StandardMfsTileId,
        store: &DirectResidualTileStore<'_>,
    ) -> Result<(usize, StandardMfsTileTaskTiming), ImagingError> {
        let tile = self.partition.tile(tile_id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let offset = tile_offset(tile);
        let (mut guard, local_alloc_zero) = store.lock_tile(tile_id)?;
        let buffer = guard
            .as_mut()
            .expect("direct residual tile should be resident");
        let worker_started = profile::maybe_profile_now();
        let mut gridded_samples = 0usize;
        let row_block = BorrowedStandardMfsRowBlock { batches, buckets };
        for sample in buckets.tile_samples(tile_id) {
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let observed_visibility = row_block.visibility(sample.sample_id)?;
            let residual_weight = f64::from(sample.grid_weight);
            if let Some(model_grid) = model_grid {
                self.gridder
                    .degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
                        model_grid,
                        &mut buffer.residual_grid,
                        &taps,
                        observed_visibility,
                        residual_weight,
                        offset,
                    );
            } else {
                let residual = Complex64::new(
                    f64::from(observed_visibility.re) * residual_weight,
                    f64::from(observed_visibility.im) * residual_weight,
                );
                self.gridder.grid_sample_taps_planned_f64_with_offset(
                    &mut buffer.residual_grid,
                    &taps,
                    residual,
                    offset,
                );
            }
            gridded_samples += 1;
        }
        Ok((
            gridded_samples,
            StandardMfsTileTaskTiming {
                local_alloc_zero,
                worker_replan_grid: profile::elapsed_since(worker_started),
            },
        ))
    }

    fn accumulate_residual_grid_global_serial(
        &self,
        batches: &[VisibilityBatch],
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        let mut replay =
            |consume: &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>| {
                consume(batches)
            };
        self.accumulate_residual_grid_global_serial_replay(&mut replay, model_grid, residual_grid)
    }

    fn accumulate_residual_grid_global_serial_replay<F>(
        &self,
        replay_weighted_batches: &mut F,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError>
    where
        F: FnMut(
            &mut dyn FnMut(&[VisibilityBatch]) -> Result<(), ImagingError>,
        ) -> Result<(), ImagingError>,
    {
        let stage_started = profile::maybe_profile_now();
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        if let (Some(model_storage), Some(residual_storage)) = (
            model_grid.and_then(|grid| grid.as_slice_memory_order()),
            residual_grid.as_slice_memory_order_mut(),
        ) {
            replay_weighted_batches(&mut |batches| {
                for batch in batches {
                    batch.validate()?;
                    let samples = batch
                        .gridable
                        .iter()
                        .copied()
                        .zip(batch.weight.iter().copied())
                        .zip(batch.sumwt_factor.iter().copied())
                        .zip(batch.u_lambda.iter().copied())
                        .zip(batch.v_lambda.iter().copied())
                        .zip(batch.visibility.iter().copied());
                    for (
                        ((((gridable, weight), sumwt_factor), u_lambda), v_lambda),
                        observed_visibility,
                    ) in samples
                    {
                        if !gridable {
                            accumulation.skipped_not_gridable += 1;
                            continue;
                        }
                        if !(weight.is_finite() && weight > 0.0) {
                            accumulation.skipped_invalid_weight += 1;
                            continue;
                        }
                        if !finite_visibility(observed_visibility) {
                            accumulation.skipped_nonfinite_visibility += 1;
                            continue;
                        }
                        accumulation.valid_samples += 1;
                        let Some(taps) = self.gridder.plan_positive_taps(u_lambda, v_lambda) else {
                            accumulation.skipped_out_of_grid += 1;
                            continue;
                        };
                        accumulation.planned_samples += 1;
                        if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        let residual_weight = weight * sumwt_factor;
                        if !(residual_weight.is_finite() && residual_weight > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        self.gridder
                            .degrid_model_and_grid_residual_taps_planned_f64_storage(
                                model_storage,
                                residual_storage,
                                &taps,
                                observed_visibility,
                                f64::from(residual_weight),
                            );
                        accumulation.gridded_residual_samples += 1;
                    }
                }
                Ok(())
            })?;
        } else if model_grid.is_none()
            && let Some(residual_storage) = residual_grid.as_slice_memory_order_mut()
        {
            replay_weighted_batches(&mut |batches| {
                for batch in batches {
                    batch.validate()?;
                    let samples = batch
                        .gridable
                        .iter()
                        .copied()
                        .zip(batch.weight.iter().copied())
                        .zip(batch.sumwt_factor.iter().copied())
                        .zip(batch.u_lambda.iter().copied())
                        .zip(batch.v_lambda.iter().copied())
                        .zip(batch.visibility.iter().copied());
                    for (
                        ((((gridable, weight), sumwt_factor), u_lambda), v_lambda),
                        observed_visibility,
                    ) in samples
                    {
                        if !gridable {
                            accumulation.skipped_not_gridable += 1;
                            continue;
                        }
                        if !(weight.is_finite() && weight > 0.0) {
                            accumulation.skipped_invalid_weight += 1;
                            continue;
                        }
                        if !finite_visibility(observed_visibility) {
                            accumulation.skipped_nonfinite_visibility += 1;
                            continue;
                        }
                        accumulation.valid_samples += 1;
                        let Some(taps) = self.gridder.plan_positive_taps(u_lambda, v_lambda) else {
                            accumulation.skipped_out_of_grid += 1;
                            continue;
                        };
                        accumulation.planned_samples += 1;
                        if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        let residual_weight = weight * sumwt_factor;
                        if !(residual_weight.is_finite() && residual_weight > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        let residual_weight = f64::from(residual_weight);
                        let residual = Complex64::new(
                            f64::from(observed_visibility.re) * residual_weight,
                            f64::from(observed_visibility.im) * residual_weight,
                        );
                        self.gridder.grid_sample_taps_planned_f64_storage(
                            residual_storage,
                            &taps,
                            residual,
                        );
                        accumulation.gridded_residual_samples += 1;
                    }
                }
                Ok(())
            })?;
        } else {
            replay_weighted_batches(&mut |batches| {
                for batch in batches {
                    batch.validate()?;
                    let samples = batch
                        .gridable
                        .iter()
                        .copied()
                        .zip(batch.weight.iter().copied())
                        .zip(batch.sumwt_factor.iter().copied())
                        .zip(batch.u_lambda.iter().copied())
                        .zip(batch.v_lambda.iter().copied())
                        .zip(batch.visibility.iter().copied());
                    for (
                        ((((gridable, weight), sumwt_factor), u_lambda), v_lambda),
                        observed_visibility,
                    ) in samples
                    {
                        if !gridable {
                            accumulation.skipped_not_gridable += 1;
                            continue;
                        }
                        if !(weight.is_finite() && weight > 0.0) {
                            accumulation.skipped_invalid_weight += 1;
                            continue;
                        }
                        if !finite_visibility(observed_visibility) {
                            accumulation.skipped_nonfinite_visibility += 1;
                            continue;
                        }
                        accumulation.valid_samples += 1;
                        let Some(taps) = self.gridder.plan_positive_taps(u_lambda, v_lambda) else {
                            accumulation.skipped_out_of_grid += 1;
                            continue;
                        };
                        accumulation.planned_samples += 1;
                        if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        let residual_weight = weight * sumwt_factor;
                        if !(residual_weight.is_finite() && residual_weight > 0.0) {
                            accumulation.skipped_invalid_sumwt += 1;
                            continue;
                        }
                        let residual_weight = f64::from(residual_weight);
                        if let Some(model_grid) = model_grid {
                            self.gridder
                                .degrid_model_and_grid_residual_taps_planned_f64(
                                    model_grid,
                                    residual_grid,
                                    &taps,
                                    observed_visibility,
                                    residual_weight,
                                );
                        } else {
                            let residual = Complex64::new(
                                f64::from(observed_visibility.re) * residual_weight,
                                f64::from(observed_visibility.im) * residual_weight,
                            );
                            self.gridder.grid_sample_taps_planned_f64(
                                residual_grid,
                                &taps,
                                residual,
                            );
                        }
                        accumulation.gridded_residual_samples += 1;
                    }
                }
                Ok(())
            })?;
        }

        let stage_duration = profile::elapsed_since(stage_started);
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "fixed_tile_one_worker_global_residual_refresh",
            samples_total: accumulation.valid_samples
                + accumulation.skipped_not_gridable
                + accumulation.skipped_invalid_weight
                + accumulation.skipped_nonfinite_visibility,
            finite_visibility_samples: accumulation.valid_samples,
            nonfinite_visibility_samples: accumulation.skipped_nonfinite_visibility,
            planned_samples: accumulation.planned_samples,
            model_grid_present_samples: if model_grid.is_some() {
                accumulation.gridded_residual_samples
            } else {
                0
            },
            model_grid_absent_samples: if model_grid.is_some() {
                0
            } else {
                accumulation.gridded_residual_samples
            },
            degrid_tap_visits: if model_grid.is_some() {
                accumulation.gridded_residual_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: accumulation.gridded_residual_samples.saturating_mul(49),
            stage_duration,
        });
        Ok(accumulation)
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct StandardMfsMetalExecutor<'a> {
    gridder: &'a StandardGridder,
    partition: StandardMfsFixedTilePartition,
    backend: MetalDirtyBackend,
}

#[cfg(all(target_os = "macos", not(coverage)))]
const _: () = {
    assert!(STANDARD_GRIDDER_SUPPORT == 3);
    assert!(STANDARD_GRIDDER_TAP_COUNT == 7);
};

#[cfg(all(target_os = "macos", not(coverage)))]
impl<'a> StandardMfsMetalExecutor<'a> {
    pub(crate) fn new_with_resident_bytes(
        gridder: &'a StandardGridder,
        resident_bytes: Option<usize>,
    ) -> Result<Self, ImagingError> {
        Self::new_with_options(gridder, resident_bytes, false)
    }

    pub(crate) fn new_with_initial_dirty_grouped(
        gridder: &'a StandardGridder,
        resident_bytes: Option<usize>,
    ) -> Result<Self, ImagingError> {
        Self::new_with_options(gridder, resident_bytes, true)
    }

    fn new_with_options(
        gridder: &'a StandardGridder,
        _resident_bytes: Option<usize>,
        enable_initial_dirty_grouped: bool,
    ) -> Result<Self, ImagingError> {
        let partition = standard_mfs_tile_partition_for_gridder(gridder)?;
        Ok(Self {
            gridder,
            partition,
            backend: if enable_initial_dirty_grouped {
                MetalDirtyBackend::new_with_initial_dirty_grouped(true)?
            } else {
                MetalDirtyBackend::new()?
            },
        })
    }

    pub(crate) fn accumulate_dirty_grids(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut accumulation = StandardMfsDirtyAccumulation {
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            gridded_samples: 0,
            skipped_samples: 0,
            max_abs_w_lambda: 0.0,
        };

        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                std::slice::from_ref(batch),
            )?;
            accumulation.skipped_samples += buckets.skipped_samples();
            for &tile_id in buckets.nonempty_tiles() {
                let tile = self.partition.tile(tile_id).ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "standard MFS tile id {} is out of range",
                        tile_id.index()
                    ))
                })?;
                let tile_bucket_samples = buckets.tile_samples(tile_id);
                for sample in tile_bucket_samples {
                    let grid_weight = f64::from(sample.grid_weight);
                    accumulation.normalization_sumwt += grid_weight;
                    accumulation.reported_sumwt += grid_weight;
                    accumulation.gridded_samples += 1;
                }
                let samples = self.metal_dirty_samples(batch, tile_bucket_samples)?;
                let (tile_psf_grid, tile_residual_grid) =
                    self.backend.grid_dirty_tile(tile, &samples)?;
                add_tile_grid(tile, &tile_psf_grid, psf_grid);
                add_tile_grid(tile, &tile_residual_grid, residual_grid);
            }
        }

        Ok(accumulation)
    }

    pub(crate) fn accumulate_psf_grid(
        &self,
        batches: &[VisibilityBatch],
        psf_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        let mut residual_grid = Array2::<Complex64>::zeros(psf_grid.raw_dim());
        self.accumulate_dirty_grids(batches, psf_grid, &mut residual_grid)
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_mtmfs_psf_grids(
        &self,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        term_count: usize,
    ) -> Result<MtmfsMetalPsfAccumulation, ImagingError> {
        self.backend.accumulate_mtmfs_psf_grids(
            self.gridder,
            batches,
            sample_frequency_batches_hz,
            reffreq_hz,
            term_count,
        )
    }

    pub(crate) fn prepare_mtmfs_input_cache(
        &self,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        reported_term_count: usize,
    ) -> Result<MtmfsMetalInputCache, ImagingError> {
        self.backend.prepare_mtmfs_input_cache(
            self.gridder,
            batches,
            sample_frequency_batches_hz,
            reffreq_hz,
            reported_term_count,
        )
    }

    pub(crate) fn accumulate_mtmfs_psf_grids_from_cache(
        &self,
        cache: &MtmfsMetalInputCache,
        term_count: usize,
    ) -> Result<MtmfsMetalPsfAccumulation, ImagingError> {
        self.backend
            .accumulate_mtmfs_psf_grids_from_cache(self.gridder, cache, term_count)
    }

    #[allow(dead_code)]
    pub(crate) fn accumulate_mtmfs_residual_grids(
        &self,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        term_count: usize,
        model_grids: Option<&[Array2<Complex32>]>,
    ) -> Result<MtmfsMetalResidualAccumulation, ImagingError> {
        self.backend.accumulate_mtmfs_residual_grids(
            self.gridder,
            batches,
            sample_frequency_batches_hz,
            reffreq_hz,
            term_count,
            model_grids,
        )
    }

    pub(crate) fn accumulate_mtmfs_residual_grids_from_cache(
        &self,
        cache: &MtmfsMetalInputCache,
        term_count: usize,
        model_grids: Option<&[Array2<Complex32>]>,
    ) -> Result<MtmfsMetalResidualAccumulation, ImagingError> {
        self.backend.accumulate_mtmfs_residual_grids_from_cache(
            self.gridder,
            cache,
            term_count,
            model_grids,
        )
    }

    pub(crate) fn accumulate_residual_grid_direct_routed_visibility_run_replay(
        &self,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        self.backend.grid_residual_refresh_routed_visibility_runs(
            self.gridder,
            replay_routed_runs,
            weighting_plan,
            model_grid,
            residual_grid,
        )
    }

    pub(crate) fn accumulate_residual_grid_direct_routed_visibility_run_replay_row_run(
        &self,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        self.backend.grid_residual_refresh_row_runs(
            self.gridder,
            replay_routed_runs,
            weighting_plan,
            model_grid,
            residual_grid,
        )
    }

    pub(crate) fn accumulate_residual_grid_direct_routed_visibility_run_replay_row_run_grouped(
        &self,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
        input_cache: Option<&mut StandardMfsMetalGroupedInputCache>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        self.backend.grid_residual_refresh_row_runs_grouped(
            self.gridder,
            replay_routed_runs,
            weighting_plan,
            model_grid,
            residual_grid,
            input_cache,
        )
    }

    pub(crate) fn begin_grouped_input_cache_fill(
        &self,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsMetalGroupedInputCacheFill, ImagingError> {
        self.backend
            .begin_grouped_input_cache_fill(self.gridder, weighting_plan)
    }

    pub(crate) fn begin_grouped_initial_dirty_accumulation(
        &self,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<MetalInitialDirtyGroupedState, ImagingError> {
        self.backend
            .begin_grouped_initial_dirty_state(self.gridder, weighting_plan)
    }

    pub(crate) fn append_grouped_initial_dirty_run(
        &self,
        state: &mut MetalInitialDirtyGroupedState,
        routed_run: &StandardMfsRoutedVisibilityRun,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<(), ImagingError> {
        self.backend
            .append_grouped_initial_dirty_run(state, routed_run, weighting_plan)
    }

    pub(crate) fn finish_grouped_initial_dirty_accumulation(
        &self,
        state: MetalInitialDirtyGroupedState,
        cache: &mut StandardMfsMetalGroupedInputCache,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        self.backend
            .finish_grouped_initial_dirty_state(state, cache, psf_grid, residual_grid)
    }

    pub(crate) fn accumulate_initial_dirty_grids_from_grouped_input_cache(
        &self,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        cache: &mut StandardMfsMetalGroupedInputCache,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<Option<StandardMfsDirtyAccumulation>, ImagingError> {
        self.backend.grid_initial_dirty_from_grouped_input_cache(
            self.gridder,
            weighting_plan,
            cache,
            psf_grid,
            residual_grid,
        )
    }

    pub(crate) fn append_grouped_input_cache_run(
        &self,
        fill: &mut StandardMfsMetalGroupedInputCacheFill,
        routed_run: &StandardMfsRoutedVisibilityRun,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<(), ImagingError> {
        self.backend
            .append_grouped_input_cache_run(routed_run, fill, weighting_plan)
    }

    pub(crate) fn finish_grouped_input_cache_fill(
        &self,
        fill: StandardMfsMetalGroupedInputCacheFill,
        cache: &mut StandardMfsMetalGroupedInputCache,
    ) -> Result<(), ImagingError> {
        self.backend.finish_grouped_input_cache_fill(fill, cache)
    }

    fn metal_dirty_samples(
        &self,
        batch: &VisibilityBatch,
        tile_bucket_samples: &[StandardMfsTileBucketSample],
    ) -> Result<Vec<MetalDirtySample>, ImagingError> {
        let mut samples = Vec::with_capacity(tile_bucket_samples.len());
        for sample in tile_bucket_samples {
            let sample_index = sample.sample_id as usize;
            let taps = sample.positive_taps()?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let visibility = batch.visibility[sample_index];
            let (x_weights, y_weights) = self.gridder.positive_tap_axis_weights(&taps);
            samples.push(MetalDirtySample {
                center_x: sample.center_x,
                center_y: sample.center_y,
                flags: u32::from(sample.flags),
                _pad0: 0,
                grid_weight: sample.grid_weight,
                visibility_re: visibility.re,
                visibility_im: visibility.im,
                _pad1: 0.0,
                x_weights,
                y_weights,
            });
        }
        Ok(samples)
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalDirtySample {
    center_x: u32,
    center_y: u32,
    flags: u32,
    _pad0: u32,
    grid_weight: f32,
    visibility_re: f32,
    visibility_im: f32,
    _pad1: f32,
    x_weights: [f32; STANDARD_GRIDDER_TAP_COUNT],
    y_weights: [f32; STANDARD_GRIDDER_TAP_COUNT],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalResidualSample {
    grid_x: f32,
    grid_y: f32,
    grid_weight: f32,
    _pad0: f32,
    visibility_re: f32,
    visibility_im: f32,
    _pad1: [f32; 2],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalMtmfsSample {
    positive_center_x: u32,
    positive_center_y: u32,
    positive_x_weight_base: u32,
    positive_y_weight_base: u32,
    negative_center_x: u32,
    negative_center_y: u32,
    negative_x_weight_base: u32,
    negative_y_weight_base: u32,
    weight: f32,
    sumwt_factor: f32,
    taylor_x: f32,
    _pad0: f32,
    visibility_re: f32,
    visibility_im: f32,
    _pad1: [f32; 2],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalMtmfsParams {
    sample_count: u32,
    grid_width: u32,
    grid_height: u32,
    term_count: u32,
    model_term_count: u32,
    _pad0: [u32; 3],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct MetalMtmfsGroupedResidualLane {
    positive_center_x: u32,
    positive_center_y: u32,
    positive_x_weight_base: u32,
    positive_y_weight_base: u32,
    negative_center_x: u32,
    negative_center_y: u32,
    negative_x_weight_base: u32,
    negative_y_weight_base: u32,
    residual0_re: f32,
    residual0_im: f32,
    residual1_re: f32,
    residual1_im: f32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct MtmfsMetalPsfAccumulation {
    pub(crate) psf_grids: Vec<Array2<Complex32>>,
    pub(crate) normalization_sumwt: f64,
    pub(crate) reported_sumwt_terms: Vec<f64>,
    pub(crate) gridded_samples: usize,
    pub(crate) skipped_samples: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct MtmfsMetalResidualAccumulation {
    pub(crate) residual_grids: Vec<Array2<Complex32>>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct MtmfsMetalInputCache {
    // Keep this field before `samples` so the no-copy Metal buffer is dropped
    // before the host Vec whose allocation backs it.
    sample_buffer: MetalBuffer,
    samples: Vec<MetalMtmfsSample>,
    grouped_chunks: Vec<MtmfsMetalGroupedChunk>,
    pub(crate) reported_sumwt_terms: Vec<f64>,
    pub(crate) normalization_sumwt: f64,
    pub(crate) gridded_samples: usize,
    pub(crate) skipped_samples: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MtmfsMetalInputCache {
    pub(crate) fn sample_count(&self) -> usize {
        self.samples.len()
    }

    pub(crate) fn host_bytes(&self) -> usize {
        self.grouped_chunks.iter().fold(
            std::mem::size_of_val(self.samples.as_slice()),
            |bytes, chunk| bytes.saturating_add(chunk.host_bytes()),
        )
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
struct MtmfsMetalGroupedChunk {
    sample_start: usize,
    sample_count: usize,
    group_descs: Vec<MetalResidualGroupedTileDesc>,
    lane_refs: Vec<u32>,
    max_halo_cells: usize,
    group_cell_count: u64,
    group_scan_tests: u64,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MtmfsMetalGroupedChunk {
    fn is_empty(&self) -> bool {
        self.sample_count == 0 || self.group_descs.is_empty()
    }

    fn host_bytes(&self) -> usize {
        std::mem::size_of_val(self.group_descs.as_slice())
            .saturating_add(std::mem::size_of_val(self.lane_refs.as_slice()))
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalResidualRowRunDesc {
    u_m: f32,
    v_m: f32,
    sumwt_factor: f32,
    w_m: f32,
    lane_offset: u32,
    lane_count: u32,
    data_offset: u32,
    flag_offset: u32,
    weight_offset: u32,
    corr_count: u32,
    polarization_mode: u32,
    transform: u32,
    corr0: u32,
    corr1: u32,
    _pad1: [u32; 2],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalResidualRowRunLane {
    lambda_scale: f32,
    center_x: u32,
    center_y: u32,
    _pad0: u32,
    grid_x: f32,
    grid_y: f32,
    _pad1: [f32; 2],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct MetalResidualGroupedLane {
    center_x: u32,
    center_y: u32,
    x_weight_base: u32,
    y_weight_base: u32,
    residual_re: f32,
    residual_im: f32,
    grid_weight: f32,
    _pad0: f32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct MetalInitialDirtyGroupedLane {
    center_x: u32,
    center_y: u32,
    x_weight_base: u32,
    y_weight_base: u32,
    dirty_re: f32,
    dirty_im: f32,
    grid_weight: f32,
    dirty_valid: f32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct MetalInitialDirtyRunAccum {
    sumwt: f32,
    max_abs_w_lambda: f32,
    gridded: u32,
    skipped: u32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct MetalResidualGroupedTileDesc {
    ref_offset: u32,
    ref_count: u32,
    halo_x0: u32,
    halo_y0: u32,
    halo_width: u32,
    halo_height: u32,
    _pad0: [u32; 2],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
struct MetalResidualGroupedTilePartition {
    grid_width: usize,
    grid_height: usize,
    edge: usize,
    tile_count_x: usize,
    tile_count_y: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalResidualGroupedTilePartition {
    fn new(grid_width: usize, grid_height: usize, edge: usize) -> Result<Self, ImagingError> {
        if grid_width == 0 || grid_height == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run grid shape must be non-empty".to_string(),
            ));
        }
        if edge == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run tile edge must be greater than zero"
                    .to_string(),
            ));
        }
        let tile_count_x = grid_width.div_ceil(edge);
        let tile_count_y = grid_height.div_ceil(edge);
        Ok(Self {
            grid_width,
            grid_height,
            edge,
            tile_count_x,
            tile_count_y,
        })
    }

    fn tile_count(&self) -> usize {
        self.tile_count_x.saturating_mul(self.tile_count_y)
    }

    fn owner(&self, center_x: u32, center_y: u32) -> Option<usize> {
        let x = center_x as usize;
        let y = center_y as usize;
        if x >= self.grid_width || y >= self.grid_height {
            return None;
        }
        let tile_x = x / self.edge;
        let tile_y = y / self.edge;
        Some(tile_x * self.tile_count_y + tile_y)
    }

    fn tile_desc(
        &self,
        tile_index: usize,
        ref_offset: usize,
        ref_count: usize,
    ) -> Result<MetalResidualGroupedTileDesc, ImagingError> {
        let tile_x = tile_index / self.tile_count_y;
        let tile_y = tile_index % self.tile_count_y;
        let x0 = tile_x.saturating_mul(self.edge);
        let y0 = tile_y.saturating_mul(self.edge);
        let x1 = (x0 + self.edge).min(self.grid_width);
        let y1 = (y0 + self.edge).min(self.grid_height);
        let halo_x0 = x0.saturating_sub(STANDARD_GRIDDER_SUPPORT);
        let halo_y0 = y0.saturating_sub(STANDARD_GRIDDER_SUPPORT);
        let halo_x1 = (x1 + STANDARD_GRIDDER_SUPPORT).min(self.grid_width);
        let halo_y1 = (y1 + STANDARD_GRIDDER_SUPPORT).min(self.grid_height);
        Ok(MetalResidualGroupedTileDesc {
            ref_offset: u32::try_from(ref_offset).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run ref offset exceeds u32".to_string(),
                )
            })?,
            ref_count: u32::try_from(ref_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run ref count exceeds u32".to_string(),
                )
            })?,
            halo_x0: u32::try_from(halo_x0).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run halo x origin exceeds u32".to_string(),
                )
            })?,
            halo_y0: u32::try_from(halo_y0).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run halo y origin exceeds u32".to_string(),
                )
            })?,
            halo_width: u32::try_from(halo_x1.saturating_sub(halo_x0)).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run halo width exceeds u32".to_string(),
                )
            })?,
            halo_height: u32::try_from(halo_y1.saturating_sub(halo_y0)).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run halo height exceeds u32".to_string(),
                )
            })?,
            _pad0: [0; 2],
        })
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalTileParams {
    sample_count: u32,
    halo_x0: u32,
    halo_y0: u32,
    halo_width: u32,
    halo_height: u32,
    _pad0: [u32; 3],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalResidualParams {
    sample_count: u32,
    grid_width: u32,
    grid_height: u32,
    oversampling: u32,
    tap_weight_count: u32,
    _pad0: [u32; 3],
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalResidualRowRunParams {
    run_count: u32,
    max_lane_count: u32,
    grid_width: u32,
    grid_height: u32,
    oversampling: u32,
    tap_weight_count: u32,
    weighting_mode: u32,
    density_convention: u32,
    density_width: u32,
    density_height: u32,
    diagnostic_mode: u32,
    _pad0: u32,
    du_lambda: f32,
    dv_lambda: f32,
    density_center_x: f32,
    density_center_y: f32,
    density_u_scale: f32,
    density_v_scale: f32,
    briggs_f2: f32,
    _pad1: f32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetalResidualRowRunDiagnosticMode {
    Exact,
    DegridOnly,
    GridOnly,
    SingleTap,
    TapPlanOnly,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalResidualRowRunDiagnosticMode {
    fn from_env() -> Result<Self, ImagingError> {
        match env::var("CASA_RS_STANDARD_MFS_METAL_ROW_RUN_DIAGNOSTIC") {
            Ok(value) => {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    "" | "exact" | "off" | "none" => Ok(Self::Exact),
                    "degrid-only" | "degrid_only" | "degrid" | "model-read" | "model_read" => {
                        Ok(Self::DegridOnly)
                    }
                    "grid-only" | "grid_only" | "atomics" | "atomic" => Ok(Self::GridOnly),
                    "single-tap" | "single_tap" | "one-tap" | "one_tap" => Ok(Self::SingleTap),
                    "tap-plan-only" | "tap_plan_only" | "tap-plan" | "tap_plan" => {
                        Ok(Self::TapPlanOnly)
                    }
                    _ => Err(ImagingError::Unsupported(format!(
                        "standard MFS Metal row-run diagnostic mode '{value}' is not recognized; expected exact, degrid-only, grid-only, single-tap, or tap-plan-only"
                    ))),
                }
            }
            Err(_) => Ok(Self::Exact),
        }
    }

    fn code(self) -> u32 {
        match self {
            Self::Exact => 0,
            Self::DegridOnly => 1,
            Self::GridOnly => 2,
            Self::SingleTap => 3,
            Self::TapPlanOnly => 4,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::DegridOnly => "degrid-only",
            Self::GridOnly => "grid-only",
            Self::SingleTap => "single-tap",
            Self::TapPlanOnly => "tap-plan-only",
        }
    }

    fn uses_diagnostic_pipeline(self) -> bool {
        self != Self::Exact
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
type MetalResidualRunReplay<'a> = dyn FnMut(
        &mut dyn FnMut(&StandardMfsRoutedVisibilityRun) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>
    + 'a;

#[cfg(all(target_os = "macos", not(coverage)))]
type MetalBuffer = objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLBuffer>>;

#[cfg(all(target_os = "macos", not(coverage)))]
type MetalPipeline =
    objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>>;

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetalInputBufferCopyMode {
    Copy,
    NoCopy,
}

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalResidualDispatchBuffers<'a> {
    sample_buffer: &'a MetalBuffer,
    params_buffer: &'a MetalBuffer,
    tap_weights: &'a MetalBuffer,
    oversampling: u32,
    tap_weight_count: u32,
    model_re: &'a MetalBuffer,
    model_im: &'a MetalBuffer,
    grid_re: &'a MetalBuffer,
    grid_im: &'a MetalBuffer,
}

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalResidualRowRunSharedBuffers<'a> {
    tap_weights: &'a MetalBuffer,
    density: &'a MetalBuffer,
    model_re: &'a MetalBuffer,
    model_im: &'a MetalBuffer,
    grid_re: &'a MetalBuffer,
    grid_im: &'a MetalBuffer,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MetalResidualRowRunChunk {
    runs: Vec<MetalResidualRowRunDesc>,
    lanes: Vec<MetalResidualRowRunLane>,
    data: Vec<MetalComplex32>,
    flags: Vec<u8>,
    weights: Vec<f32>,
    logical_lanes: usize,
    max_lane_count: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalResidualRowRunChunk {
    fn clear(&mut self) {
        self.runs.clear();
        self.lanes.clear();
        self.data.clear();
        self.flags.clear();
        self.weights.clear();
        self.logical_lanes = 0;
        self.max_lane_count = 0;
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn staged_bytes(&self) -> usize {
        std::mem::size_of_val(self.runs.as_slice())
            .saturating_add(std::mem::size_of_val(self.lanes.as_slice()))
            .saturating_add(std::mem::size_of_val(self.data.as_slice()))
            .saturating_add(std::mem::size_of_val(self.flags.as_slice()))
            .saturating_add(std::mem::size_of_val(self.weights.as_slice()))
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn build_mtmfs_metal_grouped_chunks(
    samples: &[MetalMtmfsSample],
    grid_width: usize,
    grid_height: usize,
    chunk_capacity: usize,
) -> Result<Vec<MtmfsMetalGroupedChunk>, ImagingError> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let partition = MetalResidualGroupedTilePartition::new(
        grid_width,
        grid_height,
        standard_mfs_metal_group_tile_edge(),
    )?;
    let mut chunks = Vec::new();
    for (chunk_index, chunk_samples) in samples.chunks(chunk_capacity).enumerate() {
        let sample_start = chunk_index.checked_mul(chunk_capacity).ok_or_else(|| {
            ImagingError::InvalidRequest(
                "MTMFS Metal grouped chunk offset is too large".to_string(),
            )
        })?;
        let side_count = chunk_samples.len().checked_mul(2).ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal grouped side count is too large".to_string())
        })?;
        let mut group_counts = vec![0_u32; partition.tile_count()];
        let mut lane_group_ids = Vec::with_capacity(side_count);
        for sample in chunk_samples {
            let positive_group = partition
                .owner(sample.positive_center_x, sample.positive_center_y)
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "MTMFS Metal grouped positive sample center is outside the grid"
                            .to_string(),
                    )
                })?;
            let negative_group = partition
                .owner(sample.negative_center_x, sample.negative_center_y)
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "MTMFS Metal grouped negative sample center is outside the grid"
                            .to_string(),
                    )
                })?;
            group_counts[positive_group] =
                group_counts[positive_group].checked_add(1).ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "MTMFS Metal grouped positive lane count exceeds u32".to_string(),
                    )
                })?;
            group_counts[negative_group] =
                group_counts[negative_group].checked_add(1).ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "MTMFS Metal grouped negative lane count exceeds u32".to_string(),
                    )
                })?;
            lane_group_ids.push(u32::try_from(positive_group).map_err(|_| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped positive group id exceeds u32".to_string(),
                )
            })?);
            lane_group_ids.push(u32::try_from(negative_group).map_err(|_| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped negative group id exceeds u32".to_string(),
                )
            })?);
        }

        let mut group_offsets = vec![0usize; group_counts.len()];
        let mut group_descs = Vec::new();
        let mut max_halo_cells = 0usize;
        let mut group_cell_count = 0u64;
        let mut group_scan_tests = 0u64;
        let mut ref_offset = 0usize;
        for (group_index, &count) in group_counts.iter().enumerate() {
            group_offsets[group_index] = ref_offset;
            if count == 0 {
                continue;
            }
            let desc = partition.tile_desc(group_index, ref_offset, count as usize)?;
            let halo_cells = (desc.halo_width as usize).saturating_mul(desc.halo_height as usize);
            max_halo_cells = max_halo_cells.max(halo_cells);
            group_cell_count = group_cell_count.saturating_add(halo_cells as u64);
            group_scan_tests =
                group_scan_tests.saturating_add((halo_cells as u64).saturating_mul(count as u64));
            group_descs.push(desc);
            ref_offset = ref_offset.saturating_add(count as usize);
        }

        let mut lane_refs = vec![0_u32; side_count];
        let mut cursors = group_offsets;
        for (lane_index, &group_id) in lane_group_ids.iter().enumerate() {
            let group_index = group_id as usize;
            let slot = cursors.get_mut(group_index).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "MTMFS Metal grouped group id {group_id} is out of range"
                ))
            })?;
            lane_refs[*slot] = u32::try_from(lane_index).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grouped lane ref exceeds u32".to_string())
            })?;
            *slot = (*slot).saturating_add(1);
        }

        chunks.push(MtmfsMetalGroupedChunk {
            sample_start,
            sample_count: chunk_samples.len(),
            group_descs,
            lane_refs,
            max_halo_cells,
            group_cell_count,
            group_scan_tests,
        });
    }
    Ok(chunks)
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
struct MetalResidualGroupedRowRunChunk {
    row_runs: MetalResidualRowRunChunk,
    lane_group_ids: Vec<u32>,
    group_counts: Vec<u32>,
    group_descs: Vec<MetalResidualGroupedTileDesc>,
    lane_refs: Vec<u32>,
    max_halo_cells: usize,
    group_cell_count: u64,
    group_scan_tests: u64,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalResidualGroupedRowRunChunk {
    fn new(tile_count: usize) -> Self {
        Self {
            row_runs: MetalResidualRowRunChunk::default(),
            lane_group_ids: Vec::new(),
            group_counts: vec![0; tile_count],
            group_descs: Vec::new(),
            lane_refs: Vec::new(),
            max_halo_cells: 0,
            group_cell_count: 0,
            group_scan_tests: 0,
        }
    }

    fn clear(&mut self) {
        self.row_runs.clear();
        self.lane_group_ids.clear();
        self.group_counts.fill(0);
        self.group_descs.clear();
        self.lane_refs.clear();
        self.max_halo_cells = 0;
        self.group_cell_count = 0;
        self.group_scan_tests = 0;
    }

    fn clear_group_scratch_after_finalize(&mut self) {
        self.lane_group_ids.clear();
        self.group_counts.clear();
    }

    fn is_empty(&self) -> bool {
        self.row_runs.is_empty()
    }

    fn staged_bytes(&self) -> usize {
        self.row_runs
            .staged_bytes()
            .saturating_add(std::mem::size_of_val(self.group_descs.as_slice()))
            .saturating_add(std::mem::size_of_val(self.lane_refs.as_slice()))
            .saturating_add(
                self.row_runs
                    .lanes
                    .len()
                    .saturating_mul(std::mem::size_of::<MetalResidualGroupedLane>()),
            )
    }

    fn host_cache_bytes(&self) -> usize {
        self.row_runs
            .staged_bytes()
            .saturating_add(std::mem::size_of_val(self.lane_group_ids.as_slice()))
            .saturating_add(std::mem::size_of_val(self.group_counts.as_slice()))
            .saturating_add(std::mem::size_of_val(self.group_descs.as_slice()))
            .saturating_add(std::mem::size_of_val(self.lane_refs.as_slice()))
    }

    fn finalize_groups(
        &mut self,
        partition: &MetalResidualGroupedTilePartition,
    ) -> Result<(), ImagingError> {
        self.group_descs.clear();
        self.lane_refs.clear();
        self.max_halo_cells = 0;
        self.group_cell_count = 0;
        self.group_scan_tests = 0;
        if self.lane_group_ids.len() != self.row_runs.lanes.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal grouped row-run lane/group length mismatch: {} lanes, {} group ids",
                self.row_runs.lanes.len(),
                self.lane_group_ids.len()
            )));
        }
        if self.row_runs.lanes.is_empty() {
            return Ok(());
        }

        let mut group_offsets = vec![0usize; self.group_counts.len()];
        let mut ref_offset = 0usize;
        for (group_index, &count) in self.group_counts.iter().enumerate() {
            group_offsets[group_index] = ref_offset;
            if count == 0 {
                continue;
            }
            let desc = partition.tile_desc(group_index, ref_offset, count as usize)?;
            let halo_cells = (desc.halo_width as usize).saturating_mul(desc.halo_height as usize);
            self.max_halo_cells = self.max_halo_cells.max(halo_cells);
            self.group_cell_count = self.group_cell_count.saturating_add(halo_cells as u64);
            self.group_scan_tests = self
                .group_scan_tests
                .saturating_add((halo_cells as u64).saturating_mul(count as u64));
            self.group_descs.push(desc);
            ref_offset = ref_offset.saturating_add(count as usize);
        }
        self.lane_refs.resize(self.row_runs.lanes.len(), 0);
        let mut cursors = group_offsets;
        for (lane_index, &group_id) in self.lane_group_ids.iter().enumerate() {
            let group_index = group_id as usize;
            let slot = cursors.get_mut(group_index).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS Metal grouped row-run group id {group_id} is out of range"
                ))
            })?;
            self.lane_refs[*slot] = u32::try_from(lane_index).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run lane index exceeds u32".to_string(),
                )
            })?;
            *slot = (*slot).saturating_add(1);
        }
        Ok(())
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MetalResidualGroupedInputCacheKey {
    lane_layout_version: u32,
    grid_width: usize,
    grid_height: usize,
    oversampling: usize,
    tap_weight_count: usize,
    weighting_mode: u32,
    density_convention: u32,
    density_width: usize,
    density_height: usize,
    briggs_f2_bits: u32,
    group_tile_edge: usize,
    group_tile_count: usize,
    chunk_lane_capacity: usize,
    du_lambda_bits: u32,
    dv_lambda_bits: u32,
    density_center_x_bits: u32,
    density_center_y_bits: u32,
    density_u_scale_bits: u32,
    density_v_scale_bits: u32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
const METAL_RESIDUAL_ROW_RUN_LANE_LAYOUT_VERSION: u32 = 2;

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
struct MetalResidualGroupedCachedChunk {
    params: MetalResidualRowRunParams,
    metrics: MetalResidualGroupedChunkMetrics,
    host: Option<MetalResidualGroupedRowRunChunk>,
    buffers: Option<MetalResidualGroupedCachedBuffers>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, Default)]
struct MetalResidualGroupedChunkMetrics {
    runs: usize,
    logical_lanes: usize,
    group_descs: usize,
    lane_refs: usize,
    max_halo_cells: usize,
    group_cell_count: u64,
    group_scan_tests: u64,
    staged_bytes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalResidualGroupedChunkMetrics {
    fn from_chunk(chunk: &MetalResidualGroupedRowRunChunk) -> Self {
        Self {
            runs: chunk.row_runs.runs.len(),
            logical_lanes: chunk.row_runs.logical_lanes,
            group_descs: chunk.group_descs.len(),
            lane_refs: chunk.lane_refs.len(),
            max_halo_cells: chunk.max_halo_cells,
            group_cell_count: chunk.group_cell_count,
            group_scan_tests: chunk.group_scan_tests,
            staged_bytes: chunk.staged_bytes(),
        }
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
struct MetalResidualGroupedCachedBuffers {
    run: MetalBuffer,
    lane: MetalBuffer,
    data: MetalBuffer,
    flag: MetalBuffer,
    weight: MetalBuffer,
    group_desc: MetalBuffer,
    lane_ref: MetalBuffer,
    grouped_lane: MetalBuffer,
    params: MetalBuffer,
}

#[cfg(all(target_os = "macos", not(coverage)))]
type MetalCommandBuffer =
    objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLCommandBuffer>>;

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalInitialDirtyGroupedPendingDispatch {
    command_buffer: MetalCommandBuffer,
    _run: MetalBuffer,
    _lane: MetalBuffer,
    _data: MetalBuffer,
    _flag: MetalBuffer,
    _weight: MetalBuffer,
    _group_desc: MetalBuffer,
    _lane_ref: MetalBuffer,
    _grouped_lane: MetalBuffer,
    _run_accum: MetalBuffer,
    _params: MetalBuffer,
    metrics: MetalResidualGroupedChunkMetrics,
}

#[cfg(all(target_os = "macos", not(coverage)))]
/// Opaque grouped row-run cache for the experimental Metal standard-MFS path.
///
/// The cache owns host-side grouped row/channel payloads for one standard-MFS
/// geometry and finalized weighting plan. It is optional acceleration state:
/// callers may pass it back into the standard-MFS streaming runner to avoid
/// replaying and repacking the same routed visibility runs for initial dirty
/// and residual-refresh stages.
#[derive(Debug, Default)]
pub(crate) struct StandardMfsMetalGroupedInputCache {
    key: Option<MetalResidualGroupedInputCacheKey>,
    chunks: Vec<MetalResidualGroupedCachedChunk>,
    accumulation: StandardMfsTiledResidualAccumulation,
    dirty_accumulation: Option<StandardMfsDirtyAccumulation>,
    host_bytes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl StandardMfsMetalGroupedInputCache {
    fn matches(&self, key: MetalResidualGroupedInputCacheKey) -> bool {
        self.key == Some(key) && !self.chunks.is_empty()
    }

    fn replace(
        &mut self,
        key: MetalResidualGroupedInputCacheKey,
        chunks: Vec<MetalResidualGroupedCachedChunk>,
        accumulation: StandardMfsTiledResidualAccumulation,
        dirty_accumulation: Option<StandardMfsDirtyAccumulation>,
    ) {
        self.host_bytes = chunks
            .iter()
            .map(|chunk| {
                chunk
                    .host
                    .as_ref()
                    .map_or(chunk.metrics.staged_bytes, |host| host.host_cache_bytes())
            })
            .sum();
        self.key = Some(key);
        self.chunks = chunks;
        self.accumulation = accumulation;
        self.dirty_accumulation = dirty_accumulation;
    }
}

/// Incremental builder for a Metal grouped input cache.
///
/// This lets a MeasurementSet frontend append routed row/channel runs while it
/// is already streaming the density pass. Finalization is delayed until the
/// streaming weighting plan has computed Uniform/Briggs density statistics.
#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct StandardMfsMetalGroupedInputCachePrefill {
    gridder: StandardGridder,
    backend: MetalDirtyBackend,
    partition: MetalResidualGroupedTilePartition,
    chunk_lane_capacity: usize,
    chunks: Vec<MetalResidualGroupedRowRunChunk>,
    chunk: MetalResidualGroupedRowRunChunk,
    accumulation: StandardMfsTiledResidualAccumulation,
    append_detail: MetalGroupedAppendDetail,
    runs: usize,
    logical_lanes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl StandardMfsMetalGroupedInputCachePrefill {
    /// Create an empty prefill builder for one standard-MFS image geometry.
    pub fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        let gridder = StandardGridder::new(geometry)?;
        let [grid_width, grid_height] = gridder.grid_shape();
        let group_tile_edge = standard_mfs_metal_group_tile_edge();
        let partition =
            MetalResidualGroupedTilePartition::new(grid_width, grid_height, group_tile_edge)?;
        Ok(Self {
            gridder,
            backend: MetalDirtyBackend::new()?,
            chunk_lane_capacity: standard_mfs_metal_residual_chunk_samples(),
            chunk: MetalResidualGroupedRowRunChunk::new(partition.tile_count()),
            partition,
            chunks: Vec::new(),
            accumulation: StandardMfsTiledResidualAccumulation::default(),
            append_detail: MetalGroupedAppendDetail::default(),
            runs: 0,
            logical_lanes: 0,
        })
    }

    /// Append one routed visibility run to the prefill cache.
    pub fn append_run(
        &mut self,
        routed_run: &StandardMfsRoutedVisibilityRun,
    ) -> Result<(), ImagingError> {
        self.append_row_run(
            routed_run.row.as_ref(),
            routed_run.source_slot_range.clone(),
            routed_run.tap_centers.as_ref(),
        )
    }

    fn append_row_run(
        &mut self,
        row: &StandardMfsRoutedVisibilityRow,
        source_slot_range: Range<usize>,
        tap_centers: &[[u32; 2]],
    ) -> Result<(), ImagingError> {
        let lane_count = source_slot_range
            .end
            .saturating_sub(source_slot_range.start);
        if !self.chunk.is_empty()
            && self.chunk.row_runs.logical_lanes.saturating_add(lane_count)
                > self.chunk_lane_capacity
        {
            self.finish_current_chunk()?;
        }
        let parts = MetalRowRunParts {
            row,
            source_slot_range,
            tap_centers,
            grid_width: self.partition.grid_width,
            grid_height: self.partition.grid_height,
            du_lambda: self.gridder.grid_spacing_lambda()[0],
            dv_lambda: self.gridder.grid_spacing_lambda()[1],
        };
        self.backend.append_metal_residual_grouped_row_run_parts(
            parts,
            &self.partition,
            &mut self.accumulation,
            &mut self.chunk,
            Some(&mut self.append_detail),
        )?;
        self.runs = self.runs.saturating_add(1);
        self.logical_lanes = self.logical_lanes.saturating_add(lane_count);
        if self.chunk.row_runs.logical_lanes >= self.chunk_lane_capacity {
            self.finish_current_chunk()?;
        }
        Ok(())
    }

    /// Number of routed runs appended so far.
    pub fn run_count(&self) -> usize {
        self.runs
    }

    /// Number of logical channel lanes appended so far.
    pub fn logical_lanes(&self) -> usize {
        self.logical_lanes
    }

    /// Conservative host byte estimate for finalized and open grouped chunks.
    pub fn estimated_host_bytes(&self) -> usize {
        self.chunks
            .iter()
            .map(MetalResidualGroupedRowRunChunk::host_cache_bytes)
            .sum::<usize>()
            .saturating_add(self.chunk.host_cache_bytes())
    }

    /// Finalize into a reusable grouped input cache after weighting is known.
    pub fn finish(
        mut self,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsMetalGroupedInputCache, ImagingError> {
        self.finish_current_chunk()?;
        let fill = self
            .backend
            .begin_grouped_input_cache_fill(&self.gridder, weighting_plan)?;
        let mut cached_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks {
            let params = grouped_row_run_params_from_fill_and_chunk(&fill, &chunk)?;
            let metrics = MetalResidualGroupedChunkMetrics::from_chunk(&chunk);
            cached_chunks.push(MetalResidualGroupedCachedChunk {
                params,
                metrics,
                host: Some(chunk),
                buffers: None,
            });
        }
        let mut cache = StandardMfsMetalGroupedInputCache::default();
        cache.replace(fill.key, cached_chunks, self.accumulation, None);
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_grouped_input_cache_prefill_append_detail setup_ms={:.3} lane_push_ms={:.3} data_flag_copy_ms={:.3} run_desc_ms={:.3} group_assign_ms={:.3} group_finalize_ms={:.3}",
                profile::millis(self.append_detail.setup),
                profile::millis(self.append_detail.lane_push),
                profile::millis(self.append_detail.data_flag_copy),
                profile::millis(self.append_detail.run_desc),
                profile::millis(self.append_detail.group_assign),
                profile::millis(self.append_detail.group_finalize),
            );
        }
        Ok(cache)
    }

    fn finish_current_chunk(&mut self) -> Result<(), ImagingError> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let finalize_started = Instant::now();
        self.chunk.finalize_groups(&self.partition)?;
        self.append_detail.group_finalize += finalize_started.elapsed();
        self.chunk.clear_group_scratch_after_finalize();
        let finalized_chunk = std::mem::replace(
            &mut self.chunk,
            MetalResidualGroupedRowRunChunk::new(self.partition.tile_count()),
        );
        self.chunks.push(finalized_chunk);
        Ok(())
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, Default)]
struct MetalGroupedAppendDetail {
    setup: Duration,
    lane_push: Duration,
    data_flag_copy: Duration,
    run_desc: Duration,
    group_assign: Duration,
    group_finalize: Duration,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone)]
struct MetalRowRunParts<'a> {
    row: &'a StandardMfsRoutedVisibilityRow,
    source_slot_range: Range<usize>,
    tap_centers: &'a [[u32; 2]],
    grid_width: usize,
    grid_height: usize,
    du_lambda: f64,
    dv_lambda: f64,
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct StandardMfsMetalGroupedInputCacheFill {
    key: MetalResidualGroupedInputCacheKey,
    partition: MetalResidualGroupedTilePartition,
    chunk_lane_capacity: usize,
    grid_width: usize,
    grid_height: usize,
    oversampling: usize,
    tap_weight_count: usize,
    weighting_mode: u32,
    density_convention: u32,
    density_width: usize,
    density_height: usize,
    briggs_f2: f32,
    du_lambda: f32,
    dv_lambda: f32,
    density_center_x: f32,
    density_center_y: f32,
    density_u_scale: f32,
    density_v_scale: f32,
    chunks: Vec<MetalResidualGroupedCachedChunk>,
    chunk: MetalResidualGroupedRowRunChunk,
    accumulation: StandardMfsTiledResidualAccumulation,
    dirty_accumulation: StandardMfsDirtyAccumulation,
    collect_dirty_accumulation: bool,
    append_detail: MetalGroupedAppendDetail,
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) struct MetalInitialDirtyGroupedState {
    fill: StandardMfsMetalGroupedInputCacheFill,
    density: MetalBuffer,
    tap_weights: MetalBuffer,
    psf_re: MetalBuffer,
    psf_im: MetalBuffer,
    dirty_re: MetalBuffer,
    dirty_im: MetalBuffer,
    pending: Vec<MetalInitialDirtyGroupedPendingDispatch>,
    storage_options: objc2_metal::MTLResourceOptions,
    append_grouped_row_run: Duration,
    append_detail: MetalGroupedAppendDetail,
    dirty_accumulation: Duration,
    chunk_finalize_dispatch: Duration,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy, Debug, Default)]
struct MetalInitialDirtyGroupedFinishMetrics {
    wait: Duration,
    gpu: Duration,
    kernel: Duration,
    chunks: usize,
    runs: usize,
    logical_lanes: usize,
    group_descs: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn grouped_row_run_params_from_fill(
    fill: &StandardMfsMetalGroupedInputCacheFill,
) -> Result<MetalResidualRowRunParams, ImagingError> {
    grouped_row_run_params_from_fill_and_chunk(fill, &fill.chunk)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn grouped_row_run_params_from_fill_and_chunk(
    fill: &StandardMfsMetalGroupedInputCacheFill,
    chunk: &MetalResidualGroupedRowRunChunk,
) -> Result<MetalResidualRowRunParams, ImagingError> {
    Ok(MetalResidualRowRunParams {
        run_count: u32::try_from(chunk.row_runs.runs.len()).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run chunk has too many runs".to_string(),
            )
        })?,
        max_lane_count: u32::try_from(chunk.row_runs.max_lane_count).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run chunk has too many lanes per run".to_string(),
            )
        })?,
        grid_width: u32::try_from(fill.grid_width).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run grid width exceeds u32".to_string(),
            )
        })?,
        grid_height: u32::try_from(fill.grid_height).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run grid height exceeds u32".to_string(),
            )
        })?,
        oversampling: u32::try_from(fill.oversampling).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run oversampling exceeds u32".to_string(),
            )
        })?,
        tap_weight_count: u32::try_from(fill.tap_weight_count).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run tap table exceeds u32".to_string(),
            )
        })?,
        weighting_mode: fill.weighting_mode,
        density_convention: fill.density_convention,
        density_width: u32::try_from(fill.density_width).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run density width exceeds u32".to_string(),
            )
        })?,
        density_height: u32::try_from(fill.density_height).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run density height exceeds u32".to_string(),
            )
        })?,
        diagnostic_mode: 0,
        _pad0: 0,
        du_lambda: fill.du_lambda,
        dv_lambda: fill.dv_lambda,
        density_center_x: fill.density_center_x,
        density_center_y: fill.density_center_y,
        density_u_scale: fill.density_u_scale,
        density_v_scale: fill.density_v_scale,
        briggs_f2: fill.briggs_f2,
        _pad1: 0.0,
    })
}

#[cfg(any(not(target_os = "macos"), coverage))]
/// Placeholder grouped input cache on platforms without Metal.
#[derive(Debug, Default)]
pub(crate) struct StandardMfsMetalGroupedInputCache;

#[cfg(any(not(target_os = "macos"), coverage))]
/// Placeholder grouped input cache prefill on platforms without Metal.
#[derive(Debug)]
pub(crate) struct StandardMfsMetalGroupedInputCachePrefill;

#[cfg(any(not(target_os = "macos"), coverage))]
impl StandardMfsMetalGroupedInputCachePrefill {
    /// Return an unsupported error on non-macOS platforms.
    pub fn new(_geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Err(ImagingError::Unsupported(
            "standard MFS Metal grouped input cache prefill requires macOS Metal".to_string(),
        ))
    }

    /// Return an unsupported error on non-macOS platforms.
    pub fn append_run(
        &mut self,
        _routed_run: &StandardMfsRoutedVisibilityRun,
    ) -> Result<(), ImagingError> {
        Err(ImagingError::Unsupported(
            "standard MFS Metal grouped input cache prefill requires macOS Metal".to_string(),
        ))
    }

    /// Number of routed runs appended so far.
    pub fn run_count(&self) -> usize {
        0
    }

    /// Number of logical channel lanes appended so far.
    pub fn logical_lanes(&self) -> usize {
        0
    }

    /// Conservative host byte estimate for finalized and open grouped chunks.
    pub fn estimated_host_bytes(&self) -> usize {
        0
    }

    /// Return an unsupported error on non-macOS platforms.
    pub fn finish(
        self,
        _weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsMetalGroupedInputCache, ImagingError> {
        Err(ImagingError::Unsupported(
            "standard MFS Metal grouped input cache prefill requires macOS Metal".to_string(),
        ))
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MetalResidualRefreshTimings {
    model_pack: Duration,
    model_buffer: Duration,
    grid_buffer: Duration,
    replay: Duration,
    append_total: Duration,
    run_wrap: Duration,
    sample_decode_sampled: Duration,
    weight_sampled: Duration,
    tap_plan_sampled: Duration,
    axis_weight_sampled: Duration,
    push_sampled: Duration,
    dispatch_sample_buffer: Duration,
    dispatch_params_buffer: Duration,
    dispatch_encode: Duration,
    dispatch_wait: Duration,
    readback: Duration,
    sampled_samples: usize,
    staged_bytes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MetalResidualRowRunRefreshTimings {
    model_pack: Duration,
    model_buffer: Duration,
    density_buffer: Duration,
    grid_buffer: Duration,
    replay: Duration,
    append_total: Duration,
    append_detail: MetalGroupedAppendDetail,
    dispatch_input_buffers: Duration,
    dispatch_params_buffer: Duration,
    dispatch_encode: Duration,
    dispatch_wait: Duration,
    dispatch_gpu: Duration,
    dispatch_kernel: Duration,
    readback: Duration,
    staged_bytes: usize,
    diagnostic_output_bytes: usize,
    candidate_tap_visits: u64,
    candidate_model_reads: u64,
    candidate_grid_atomic_adds: u64,
    candidate_group_cell_atomic_adds: u64,
    candidate_group_scan_tests: u64,
    runs: usize,
    logical_lanes: usize,
    unsupported_runs: usize,
    group_tile_edge: usize,
    group_descs: usize,
    lane_refs: usize,
    input_cache_hit: bool,
    input_cache_fill: bool,
    input_cache_chunks: usize,
    input_cache_host_bytes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MetalResidualDispatchTiming {
    sample_buffer: Duration,
    params_buffer: Duration,
    encode: Duration,
    wait: Duration,
    gpu: Duration,
    kernel: Duration,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MtmfsMetalTermTimings {
    input_buffer: Duration,
    params_buffer: Duration,
    tap_buffer: Duration,
    grid_buffer: Duration,
    model_pack: Duration,
    model_buffer: Duration,
    dispatch_sample_buffer: Duration,
    dispatch_params_buffer: Duration,
    dispatch_encode: Duration,
    dispatch_wait: Duration,
    dispatch_gpu: Duration,
    dispatch_kernel: Duration,
    readback: Duration,
    staged_bytes: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug, Default)]
struct MtmfsMetalDispatchTiming {
    sample_buffer: Duration,
    params_buffer: Duration,
    encode: Duration,
    wait: Duration,
    gpu: Duration,
    kernel: Duration,
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn record_metal_grouped_residual_dispatch(
    chunk_number: usize,
    metrics: MetalResidualGroupedChunkMetrics,
    dispatch_timing: MetalResidualDispatchTiming,
    timings: &mut MetalResidualRowRunRefreshTimings,
) {
    timings.dispatch_input_buffers += dispatch_timing.sample_buffer;
    timings.dispatch_params_buffer += dispatch_timing.params_buffer;
    timings.dispatch_encode += dispatch_timing.encode;
    timings.dispatch_wait += dispatch_timing.wait;
    timings.dispatch_gpu += dispatch_timing.gpu;
    timings.dispatch_kernel += dispatch_timing.kernel;
    if profile::standard_mfs_profile_detail_enabled() {
        eprintln!(
            "standard_mfs_metal_row_run_grouped_chunk chunk={} runs={} logical_lanes={} group_descs={} lane_refs={} group_cell_count={} group_scan_tests={} input_buffers_ms={:.3} params_ms={:.3} encode_ms={:.3} wait_ms={:.3} gpu_ms={:.3} kernel_ms={:.3} staged_bytes={}",
            chunk_number,
            metrics.runs,
            metrics.logical_lanes,
            metrics.group_descs,
            metrics.lane_refs,
            metrics.group_cell_count,
            metrics.group_scan_tests,
            profile::millis(dispatch_timing.sample_buffer),
            profile::millis(dispatch_timing.params_buffer),
            profile::millis(dispatch_timing.encode),
            profile::millis(dispatch_timing.wait),
            profile::millis(dispatch_timing.gpu),
            profile::millis(dispatch_timing.kernel),
            metrics.staged_bytes,
        );
    }
    timings.staged_bytes = timings.staged_bytes.saturating_add(metrics.staged_bytes);
    let candidate_tap_visits = (metrics.logical_lanes as u64)
        .saturating_mul(STANDARD_GRIDDER_TAP_COUNT as u64)
        .saturating_mul(STANDARD_GRIDDER_TAP_COUNT as u64);
    timings.candidate_tap_visits = timings
        .candidate_tap_visits
        .saturating_add(candidate_tap_visits);
    timings.candidate_model_reads = timings
        .candidate_model_reads
        .saturating_add(candidate_tap_visits.saturating_mul(2));
    timings.candidate_grid_atomic_adds = timings
        .candidate_grid_atomic_adds
        .saturating_add(candidate_tap_visits.saturating_mul(2));
    timings.candidate_group_cell_atomic_adds = timings
        .candidate_group_cell_atomic_adds
        .saturating_add(metrics.group_cell_count.saturating_mul(2));
    timings.candidate_group_scan_tests = timings
        .candidate_group_scan_tests
        .saturating_add(metrics.group_scan_tests);
    timings.runs = timings.runs.saturating_add(metrics.runs);
    timings.logical_lanes = timings.logical_lanes.saturating_add(metrics.logical_lanes);
    timings.group_descs = timings.group_descs.saturating_add(metrics.group_descs);
    timings.lane_refs = timings.lane_refs.saturating_add(metrics.lane_refs);
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalComplex32 {
    re: f32,
    im: f32,
}

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalDirtyBackend {
    device: objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLDevice>>,
    queue: objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLCommandQueue>>,
    pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    residual_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    #[allow(dead_code)]
    mtmfs_psf_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    mtmfs_residual_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    mtmfs_grouped_psf_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    mtmfs_grouped_residual_prepare_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    mtmfs_grouped_residual_accumulate_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    residual_row_run_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    residual_row_run_diagnostic_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    residual_row_run_grouped_prepare_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    residual_row_run_grouped_accumulate_pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
    initial_dirty_grouped_prepare_pipeline: Option<
        objc2::rc::Retained<
            objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
        >,
    >,
    initial_dirty_grouped_accumulate_pipeline: Option<
        objc2::rc::Retained<
            objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
        >,
    >,
    initial_dirty_grouped_run_accum_pipeline: Option<
        objc2::rc::Retained<
            objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
        >,
    >,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(clippy::too_many_arguments)]
fn mtmfs_metal_sample(
    gridder: &StandardGridder,
    u_lambda: f64,
    v_lambda: f64,
    weight: f32,
    sumwt_factor: f32,
    frequency_hz: f64,
    reffreq_hz: f64,
    visibility: Complex32,
) -> Result<Option<MetalMtmfsSample>, ImagingError> {
    let Some(positive_taps) = gridder.plan_positive_taps(u_lambda, v_lambda) else {
        return Ok(None);
    };
    let Some(negative_taps) = gridder.plan_positive_taps(-u_lambda, -v_lambda) else {
        return Ok(None);
    };
    let positive_center = [
        u32::try_from(positive_taps.x.center()).map_err(|_| {
            ImagingError::InvalidRequest("MTMFS Metal positive x center exceeds u32".to_string())
        })?,
        u32::try_from(positive_taps.y.center()).map_err(|_| {
            ImagingError::InvalidRequest("MTMFS Metal positive y center exceeds u32".to_string())
        })?,
    ];
    let negative_center = [
        u32::try_from(negative_taps.x.center()).map_err(|_| {
            ImagingError::InvalidRequest("MTMFS Metal negative x center exceeds u32".to_string())
        })?,
        u32::try_from(negative_taps.y.center()).map_err(|_| {
            ImagingError::InvalidRequest("MTMFS Metal negative y center exceeds u32".to_string())
        })?,
    ];
    let scaled = (frequency_hz - reffreq_hz) / reffreq_hz;
    Ok(Some(MetalMtmfsSample {
        positive_center_x: positive_center[0],
        positive_center_y: positive_center[1],
        positive_x_weight_base: mtmfs_metal_weight_base(positive_taps.x)?,
        positive_y_weight_base: mtmfs_metal_weight_base(positive_taps.y)?,
        negative_center_x: negative_center[0],
        negative_center_y: negative_center[1],
        negative_x_weight_base: mtmfs_metal_weight_base(negative_taps.x)?,
        negative_y_weight_base: mtmfs_metal_weight_base(negative_taps.y)?,
        weight,
        sumwt_factor,
        taylor_x: scaled as f32,
        _pad0: 0.0,
        visibility_re: visibility.re,
        visibility_im: visibility.im,
        _pad1: [0.0; 2],
    }))
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn mtmfs_metal_weight_base(span: TapAxisSpan) -> Result<u32, ImagingError> {
    span.weight_index
        .checked_mul(STANDARD_GRIDDER_TAP_COUNT)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal tap weight base exceeds u32".to_string())
        })
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn mtmfs_taylor_power(taylor_x: f32, order: usize) -> f32 {
    if order == 0 {
        1.0
    } else {
        taylor_x.powi(order as i32)
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn read_mtmfs_term_grids(
    grid_re: &[u32],
    grid_im: &[u32],
    term_count: usize,
    cell_count: usize,
    grid_width: usize,
    grid_height: usize,
) -> Vec<Array2<Complex32>> {
    let mut grids = Vec::with_capacity(term_count);
    for term in 0..term_count {
        let offset = term * cell_count;
        let mut grid = Array2::<Complex32>::zeros((grid_width, grid_height));
        for ((cell, &re_bits), &im_bits) in grid
            .as_slice_memory_order_mut()
            .expect("fresh MTMFS Metal term grid should be contiguous")
            .iter_mut()
            .zip(&grid_re[offset..offset + cell_count])
            .zip(&grid_im[offset..offset + cell_count])
        {
            *cell = Complex32::new(f32::from_bits(re_bits), f32::from_bits(im_bits));
        }
        grids.push(grid);
    }
    grids
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalDirtyBackend {
    fn new() -> Result<Self, ImagingError> {
        Self::new_with_initial_dirty_grouped(false)
    }

    fn new_with_initial_dirty_grouped(
        enable_initial_dirty_grouped: bool,
    ) -> Result<Self, ImagingError> {
        use objc2_metal::{
            MTLCreateSystemDefaultDevice, MTLDevice, MTLLibrary, MTLResourceOptions,
        };

        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not find a default Metal device".to_string(),
            )
        })?;
        let queue = device.newCommandQueue().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a Metal command queue".to_string(),
            )
        })?;
        let source = objc2_foundation::NSString::from_str(METAL_DIRTY_SHADER);
        let library = device
            .newLibraryWithSource_options_error(&source, None)
            .map_err(|error| metal_error("compile dirty tile shader", error))?;
        let function_name = objc2_foundation::NSString::from_str("grid_dirty_tile_cell_owner");
        let function = library.newFunctionWithName(&function_name).ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' dirty tile shader entry point was not found"
                    .to_string(),
            )
        })?;
        let pipeline = device
            .newComputePipelineStateWithFunction_error(&function)
            .map_err(|error| metal_error("create dirty tile pipeline", error))?;
        let residual_function_name =
            objc2_foundation::NSString::from_str("residual_refresh_global_atomic_exact");
        let residual_function = library
            .newFunctionWithName(&residual_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' residual refresh shader entry point was not found"
                        .to_string(),
                )
            })?;
        let residual_pipeline = device
            .newComputePipelineStateWithFunction_error(&residual_function)
            .map_err(|error| metal_error("create residual refresh pipeline", error))?;
        let mtmfs_psf_function_name =
            objc2_foundation::NSString::from_str("mtmfs_psf_terms_global_atomic");
        let mtmfs_psf_function = library
            .newFunctionWithName(&mtmfs_psf_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' MTMFS PSF shader entry point was not found"
                        .to_string(),
                )
            })?;
        let mtmfs_psf_pipeline = device
            .newComputePipelineStateWithFunction_error(&mtmfs_psf_function)
            .map_err(|error| metal_error("create MTMFS PSF pipeline", error))?;
        let mtmfs_residual_function_name =
            objc2_foundation::NSString::from_str("mtmfs_residual_terms_global_atomic");
        let mtmfs_residual_function = library
            .newFunctionWithName(&mtmfs_residual_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' MTMFS residual shader entry point was not found"
                        .to_string(),
                )
            })?;
        let mtmfs_residual_pipeline = device
            .newComputePipelineStateWithFunction_error(&mtmfs_residual_function)
            .map_err(|error| metal_error("create MTMFS residual pipeline", error))?;
        let mtmfs_grouped_psf_function_name =
            objc2_foundation::NSString::from_str("mtmfs_psf_terms_grouped_accumulate");
        let mtmfs_grouped_psf_function = library
            .newFunctionWithName(&mtmfs_grouped_psf_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' grouped MTMFS PSF shader entry point was not found"
                        .to_string(),
                )
            })?;
        let mtmfs_grouped_psf_pipeline = device
            .newComputePipelineStateWithFunction_error(&mtmfs_grouped_psf_function)
            .map_err(|error| metal_error("create grouped MTMFS PSF pipeline", error))?;
        let mtmfs_grouped_residual_prepare_function_name =
            objc2_foundation::NSString::from_str("mtmfs_residual_terms_grouped_prepare_nterms2");
        let mtmfs_grouped_residual_prepare_function = library
            .newFunctionWithName(&mtmfs_grouped_residual_prepare_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' grouped MTMFS residual prepare shader entry point was not found"
                        .to_string(),
                )
            })?;
        let mtmfs_grouped_residual_prepare_pipeline = device
            .newComputePipelineStateWithFunction_error(&mtmfs_grouped_residual_prepare_function)
            .map_err(|error| {
                metal_error("create grouped MTMFS residual prepare pipeline", error)
            })?;
        let mtmfs_grouped_residual_accumulate_function_name =
            objc2_foundation::NSString::from_str("mtmfs_residual_terms_grouped_accumulate_nterms2");
        let mtmfs_grouped_residual_accumulate_function = library
            .newFunctionWithName(&mtmfs_grouped_residual_accumulate_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' grouped MTMFS residual accumulate shader entry point was not found"
                        .to_string(),
                )
            })?;
        let mtmfs_grouped_residual_accumulate_pipeline = device
            .newComputePipelineStateWithFunction_error(&mtmfs_grouped_residual_accumulate_function)
            .map_err(|error| {
                metal_error("create grouped MTMFS residual accumulate pipeline", error)
            })?;
        let residual_row_run_function_name =
            objc2_foundation::NSString::from_str("residual_refresh_row_run_global_atomic_exact");
        let residual_row_run_function = library
            .newFunctionWithName(&residual_row_run_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run' residual refresh shader entry point was not found"
                        .to_string(),
                )
            })?;
        let residual_row_run_pipeline = device
            .newComputePipelineStateWithFunction_error(&residual_row_run_function)
            .map_err(|error| metal_error("create residual row-run refresh pipeline", error))?;
        let residual_row_run_diagnostic_function_name =
            objc2_foundation::NSString::from_str("residual_refresh_row_run_diagnostic");
        let residual_row_run_diagnostic_function = library
            .newFunctionWithName(&residual_row_run_diagnostic_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run' diagnostic residual refresh shader entry point was not found"
                        .to_string(),
                )
            })?;
        let residual_row_run_diagnostic_pipeline = device
            .newComputePipelineStateWithFunction_error(&residual_row_run_diagnostic_function)
            .map_err(|error| {
                metal_error("create residual row-run diagnostic refresh pipeline", error)
            })?;
        let residual_row_run_grouped_prepare_function_name =
            objc2_foundation::NSString::from_str("residual_refresh_row_run_grouped_prepare");
        let residual_row_run_grouped_prepare_function = library
            .newFunctionWithName(&residual_row_run_grouped_prepare_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run-grouped' prepare shader entry point was not found"
                        .to_string(),
                )
            })?;
        let residual_row_run_grouped_prepare_pipeline = device
            .newComputePipelineStateWithFunction_error(&residual_row_run_grouped_prepare_function)
            .map_err(|error| {
                metal_error("create residual row-run grouped prepare pipeline", error)
            })?;
        let residual_row_run_grouped_accumulate_function_name =
            objc2_foundation::NSString::from_str("residual_refresh_row_run_grouped_accumulate");
        let residual_row_run_grouped_accumulate_function = library
            .newFunctionWithName(&residual_row_run_grouped_accumulate_function_name)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run-grouped' accumulate shader entry point was not found"
                        .to_string(),
                )
            })?;
        let residual_row_run_grouped_accumulate_pipeline = device
            .newComputePipelineStateWithFunction_error(
                &residual_row_run_grouped_accumulate_function,
            )
            .map_err(|error| {
                metal_error("create residual row-run grouped accumulate pipeline", error)
            })?;
        let (
            initial_dirty_grouped_prepare_pipeline,
            initial_dirty_grouped_accumulate_pipeline,
            initial_dirty_grouped_run_accum_pipeline,
        ) = if enable_initial_dirty_grouped {
            let initial_dirty_grouped_prepare_function_name =
                objc2_foundation::NSString::from_str("initial_dirty_psf_row_run_grouped_prepare");
            let initial_dirty_grouped_prepare_function = library
                    .newFunctionWithName(&initial_dirty_grouped_prepare_function_name)
                    .ok_or_else(|| {
                        ImagingError::Unsupported(
                            "standard MFS backend 'metal-row-run-grouped' initial dirty/PSF prepare shader entry point was not found"
                                .to_string(),
                        )
                    })?;
            let initial_dirty_grouped_prepare_pipeline = device
                .newComputePipelineStateWithFunction_error(&initial_dirty_grouped_prepare_function)
                .map_err(|error| {
                    metal_error(
                        "create initial dirty/PSF row-run grouped prepare pipeline",
                        error,
                    )
                })?;
            let initial_dirty_grouped_accumulate_function_name =
                objc2_foundation::NSString::from_str(
                    "initial_dirty_psf_row_run_grouped_accumulate",
                );
            let initial_dirty_grouped_accumulate_function = library
                    .newFunctionWithName(&initial_dirty_grouped_accumulate_function_name)
                    .ok_or_else(|| {
                        ImagingError::Unsupported(
                            "standard MFS backend 'metal-row-run-grouped' initial dirty/PSF accumulate shader entry point was not found"
                                .to_string(),
                        )
                    })?;
            let initial_dirty_grouped_accumulate_pipeline = device
                .newComputePipelineStateWithFunction_error(
                    &initial_dirty_grouped_accumulate_function,
                )
                .map_err(|error| {
                    metal_error(
                        "create initial dirty/PSF row-run grouped accumulate pipeline",
                        error,
                    )
                })?;
            let initial_dirty_grouped_run_accum_function_name =
                objc2_foundation::NSString::from_str(
                    "initial_dirty_psf_row_run_grouped_accumulate_runs",
                );
            let initial_dirty_grouped_run_accum_function = library
                    .newFunctionWithName(&initial_dirty_grouped_run_accum_function_name)
                    .ok_or_else(|| {
                        ImagingError::Unsupported(
                            "standard MFS backend 'metal-row-run-grouped' initial dirty/PSF run-accumulation shader entry point was not found"
                                .to_string(),
                        )
                    })?;
            let initial_dirty_grouped_run_accum_pipeline = device
                .newComputePipelineStateWithFunction_error(
                    &initial_dirty_grouped_run_accum_function,
                )
                .map_err(|error| {
                    metal_error(
                        "create initial dirty/PSF row-run grouped run-accumulation pipeline",
                        error,
                    )
                })?;
            (
                Some(initial_dirty_grouped_prepare_pipeline),
                Some(initial_dirty_grouped_accumulate_pipeline),
                Some(initial_dirty_grouped_run_accum_pipeline),
            )
        } else {
            (None, None, None)
        };
        let _ = MTLResourceOptions::StorageModeShared;
        Ok(Self {
            device,
            queue,
            pipeline,
            residual_pipeline,
            mtmfs_psf_pipeline,
            mtmfs_residual_pipeline,
            mtmfs_grouped_psf_pipeline,
            mtmfs_grouped_residual_prepare_pipeline,
            mtmfs_grouped_residual_accumulate_pipeline,
            residual_row_run_pipeline,
            residual_row_run_diagnostic_pipeline,
            residual_row_run_grouped_prepare_pipeline,
            residual_row_run_grouped_accumulate_pipeline,
            initial_dirty_grouped_prepare_pipeline,
            initial_dirty_grouped_accumulate_pipeline,
            initial_dirty_grouped_run_accum_pipeline,
        })
    }

    fn grid_dirty_tile(
        &self,
        tile: &StandardMfsFixedTile,
        samples: &[MetalDirtySample],
    ) -> Result<(Array2<Complex64>, Array2<Complex64>), ImagingError> {
        use std::{mem, slice};

        use objc2_metal::{
            MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
            MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState, MTLDevice,
            MTLResourceOptions, MTLSize,
        };

        let width = tile.halo.width();
        let height = tile.halo.height();
        let cell_count = width.checked_mul(height).ok_or_else(|| {
            ImagingError::InvalidRequest("standard MFS Metal tile is too large".to_string())
        })?;
        if cell_count == 0 {
            return Ok((
                Array2::<Complex64>::zeros((width, height)),
                Array2::<Complex64>::zeros((width, height)),
            ));
        }
        let sample_count = u32::try_from(samples.len()).map_err(|_| {
            ImagingError::InvalidRequest("standard MFS Metal tile has too many samples".to_string())
        })?;
        let params = MetalTileParams {
            sample_count,
            halo_x0: u32::try_from(tile.halo.x0).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal tile x origin exceeds u32".to_string(),
                )
            })?,
            halo_y0: u32::try_from(tile.halo.y0).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal tile y origin exceeds u32".to_string(),
                )
            })?,
            halo_width: u32::try_from(width).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal tile width exceeds u32".to_string(),
                )
            })?,
            halo_height: u32::try_from(height).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal tile height exceeds u32".to_string(),
                )
            })?,
            _pad0: [0; 3],
        };

        let storage_options = MTLResourceOptions::StorageModeShared;
        let sample_buffer = self.buffer_from_slice(samples, storage_options)?;
        let params_buffer = self.buffer_from_slice(slice::from_ref(&params), storage_options)?;
        let output_bytes = cell_count
            .checked_mul(mem::size_of::<MetalComplex32>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal tile output is too large".to_string(),
                )
            })?;
        let psf_buffer = self
            .device
            .newBufferWithLength_options(output_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate PSF tile buffer".to_string(),
                )
            })?;
        let residual_buffer = self
            .device
            .newBufferWithLength_options(output_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate residual tile buffer"
                        .to_string(),
                )
            })?;

        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a command buffer".to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a compute encoder".to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&sample_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&psf_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&residual_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&params_buffer), 0, 3);
        }
        let thread_count = cell_count.min(usize::try_from(u32::MAX).unwrap());
        let thread_width = self.pipeline.threadExecutionWidth().max(1);
        let max_threads = self.pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let threads_per_group = thread_width.min(max_threads).min(thread_count);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: thread_count,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: threads_per_group,
                height: 1,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        command_buffer.waitUntilCompleted();
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' dirty tile command failed: {message}"
            )));
        }

        let psf_output = unsafe {
            slice::from_raw_parts(
                psf_buffer.contents().as_ptr().cast::<MetalComplex32>(),
                cell_count,
            )
        };
        let residual_output = unsafe {
            slice::from_raw_parts(
                residual_buffer.contents().as_ptr().cast::<MetalComplex32>(),
                cell_count,
            )
        };
        let mut psf_grid = Array2::<Complex64>::zeros((width, height));
        let mut residual_grid = Array2::<Complex64>::zeros((width, height));
        for (cell, value) in psf_grid
            .as_slice_memory_order_mut()
            .expect("fresh tile grid should be contiguous")
            .iter_mut()
            .zip(psf_output)
        {
            *cell = Complex64::new(f64::from(value.re), f64::from(value.im));
        }
        for (cell, value) in residual_grid
            .as_slice_memory_order_mut()
            .expect("fresh tile grid should be contiguous")
            .iter_mut()
            .zip(residual_output)
        {
            *cell = Complex64::new(f64::from(value.re), f64::from(value.im));
        }

        Ok((psf_grid, residual_grid))
    }

    #[allow(dead_code)]
    fn accumulate_mtmfs_psf_grids(
        &self,
        gridder: &StandardGridder,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        term_count: usize,
    ) -> Result<MtmfsMetalPsfAccumulation, ImagingError> {
        let cache = self.prepare_mtmfs_input_cache(
            gridder,
            batches,
            sample_frequency_batches_hz,
            reffreq_hz,
            term_count,
        )?;
        self.accumulate_mtmfs_psf_grids_from_cache(gridder, &cache, term_count)
    }

    fn prepare_mtmfs_input_cache(
        &self,
        gridder: &StandardGridder,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        reported_term_count: usize,
    ) -> Result<MtmfsMetalInputCache, ImagingError> {
        if reported_term_count == 0 {
            return Err(ImagingError::InvalidRequest(
                "MTMFS Metal input cache requires at least one Taylor term".to_string(),
            ));
        }
        let collect_profile = profile::standard_mfs_profile_detail_enabled();
        let total_started = collect_profile.then(Instant::now);
        let mut validate_time = Duration::ZERO;
        let mut sample_plan_time = Duration::ZERO;
        let mut sumwt_time = Duration::ZERO;
        let mut grouping_time = Duration::ZERO;
        let mut samples = Vec::<MetalMtmfsSample>::new();
        let mut normalization_sumwt = 0.0_f64;
        let mut reported_sumwt_terms = vec![0.0_f64; reported_term_count];
        let mut gridded_samples = 0usize;
        let mut skipped_samples = 0usize;
        for (batch_index, batch) in batches.iter().enumerate() {
            let validate_started = collect_profile.then(Instant::now);
            batch.validate()?;
            if let Some(started) = validate_started {
                validate_time += started.elapsed();
            }
            let frequencies_hz = sample_frequency_batches_hz
                .get(batch_index)
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "missing MTMFS sample-frequency batch for visibility batch {batch_index}"
                    ))
                })?;
            samples.reserve(frequencies_hz.len().min(batch.len()));
            for (index, &frequency_hz) in frequencies_hz.iter().enumerate().take(batch.len()) {
                let weight = batch.weight[index];
                let sumwt_factor = batch.sumwt_factor[index];
                if !(batch.gridable[index]
                    && weight.is_finite()
                    && weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0
                    && frequency_hz.is_finite()
                    && frequency_hz > 0.0)
                {
                    skipped_samples = skipped_samples.saturating_add(1);
                    continue;
                }
                let sample_plan_started = collect_profile.then(Instant::now);
                let Some(sample) = mtmfs_metal_sample(
                    gridder,
                    batch.u_lambda[index],
                    batch.v_lambda[index],
                    weight,
                    sumwt_factor,
                    frequency_hz,
                    reffreq_hz,
                    batch.visibility[index],
                )?
                else {
                    if let Some(started) = sample_plan_started {
                        sample_plan_time += started.elapsed();
                    }
                    skipped_samples = skipped_samples.saturating_add(1);
                    continue;
                };
                if let Some(started) = sample_plan_started {
                    sample_plan_time += started.elapsed();
                }
                let sumwt_started = collect_profile.then(Instant::now);
                normalization_sumwt += 2.0 * f64::from(weight);
                for (order, sumwt) in reported_sumwt_terms.iter_mut().enumerate() {
                    *sumwt += f64::from(weight)
                        * f64::from(mtmfs_taylor_power(sample.taylor_x, order))
                        * f64::from(sumwt_factor);
                }
                if let Some(started) = sumwt_started {
                    sumwt_time += started.elapsed();
                }
                samples.push(sample);
                gridded_samples = gridded_samples.saturating_add(1);
            }
        }
        let sample_buffer_started = collect_profile.then(Instant::now);
        let sample_buffer = self.buffer_from_slice_no_copy(
            &samples,
            objc2_metal::MTLResourceOptions::StorageModeShared,
        )?;
        let sample_buffer_time =
            sample_buffer_started.map_or(Duration::ZERO, |started| started.elapsed());
        let [grid_width, grid_height] = gridder.grid_shape();
        let grouped_chunks = if mtmfs_metal_grouped_terms_enabled() {
            let grouping_started = collect_profile.then(Instant::now);
            let chunks = build_mtmfs_metal_grouped_chunks(
                &samples,
                grid_width,
                grid_height,
                standard_mfs_metal_residual_chunk_samples(),
            )?;
            if let Some(started) = grouping_started {
                grouping_time += started.elapsed();
            }
            chunks
        } else {
            Vec::new()
        };
        if collect_profile {
            let grouped_host_bytes = grouped_chunks.iter().fold(0usize, |bytes, chunk| {
                bytes.saturating_add(chunk.host_bytes())
            });
            let grouped_refs = grouped_chunks.iter().fold(0usize, |count, chunk| {
                count.saturating_add(chunk.lane_refs.len())
            });
            let grouped_descs = grouped_chunks.iter().fold(0usize, |count, chunk| {
                count.saturating_add(chunk.group_descs.len())
            });
            let grouped_cells = grouped_chunks.iter().fold(0u64, |count, chunk| {
                count.saturating_add(chunk.group_cell_count)
            });
            let grouped_scan_tests = grouped_chunks.iter().fold(0u64, |count, chunk| {
                count.saturating_add(chunk.group_scan_tests)
            });
            eprintln!(
                "mtmfs_metal_input_cache samples={} reported_terms={} host_bytes={} skipped_samples={} grouped_chunks={} grouped_descs={} grouped_refs={} grouped_host_bytes={} grouped_cells={} grouped_scan_tests={} total_ms={:.3} validate_ms={:.3} sample_plan_ms={:.3} sumwt_ms={:.3} sample_buffer_ms={:.3} grouping_ms={:.3}",
                samples.len(),
                reported_term_count,
                std::mem::size_of_val(samples.as_slice()),
                skipped_samples,
                grouped_chunks.len(),
                grouped_descs,
                grouped_refs,
                grouped_host_bytes,
                grouped_cells,
                grouped_scan_tests,
                total_started.map_or(0.0, |started| profile::millis(started.elapsed())),
                profile::millis(validate_time),
                profile::millis(sample_plan_time),
                profile::millis(sumwt_time),
                profile::millis(sample_buffer_time),
                profile::millis(grouping_time),
            );
        }
        Ok(MtmfsMetalInputCache {
            sample_buffer,
            samples,
            grouped_chunks,
            reported_sumwt_terms,
            normalization_sumwt,
            gridded_samples,
            skipped_samples,
        })
    }

    fn accumulate_mtmfs_psf_grids_from_cache(
        &self,
        gridder: &StandardGridder,
        cache: &MtmfsMetalInputCache,
        term_count: usize,
    ) -> Result<MtmfsMetalPsfAccumulation, ImagingError> {
        use std::{mem, slice};

        use objc2_metal::{MTLBuffer, MTLDevice, MTLResourceOptions};

        if term_count == 0 {
            return Err(ImagingError::InvalidRequest(
                "MTMFS Metal PSF requires at least one Taylor term".to_string(),
            ));
        }
        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal PSF grid is too large".to_string())
        })?;
        let output_cells = term_count.checked_mul(cell_count).ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal PSF term grid is too large".to_string())
        })?;
        let storage_options = MTLResourceOptions::StorageModeShared;
        let mut timings = MtmfsMetalTermTimings::default();
        let chunk_capacity = standard_mfs_metal_residual_chunk_samples();
        let params_buffer_started = Instant::now();
        let params_buffer = self
            .device
            .newBufferWithLength_options(mem::size_of::<MetalMtmfsParams>(), storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate MTMFS PSF params buffer"
                        .to_string(),
                )
            })?;
        timings.params_buffer += params_buffer_started.elapsed();
        let tap_buffer_started = Instant::now();
        let tap_weights_buffer =
            self.buffer_from_slice_no_copy(gridder.normalized_tap_weights(), storage_options)?;
        timings.tap_buffer += tap_buffer_started.elapsed();
        let grid_buffer_started = Instant::now();
        let zero_grid = vec![0_u32; output_cells];
        let grid_re_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        let grid_im_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        timings.grid_buffer += grid_buffer_started.elapsed();
        let use_grouped_psf =
            mtmfs_metal_grouped_terms_enabled() && !cache.grouped_chunks.is_empty();
        let mut chunks_dispatched = 0usize;
        if use_grouped_psf {
            for chunk in &cache.grouped_chunks {
                let dispatch_timing = self.dispatch_mtmfs_grouped_psf_chunk(
                    chunk,
                    grid_width,
                    grid_height,
                    term_count,
                    &cache.sample_buffer,
                    &params_buffer,
                    &tap_weights_buffer,
                    &grid_re_buffer,
                    &grid_im_buffer,
                )?;
                timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                timings.dispatch_encode += dispatch_timing.encode;
                timings.dispatch_wait += dispatch_timing.wait;
                timings.dispatch_gpu += dispatch_timing.gpu;
                timings.dispatch_kernel += dispatch_timing.kernel;
                timings.staged_bytes = timings.staged_bytes.saturating_add(chunk.host_bytes());
                chunks_dispatched = chunks_dispatched.saturating_add(1);
            }
        } else {
            for (chunk_index, chunk) in cache.samples.chunks(chunk_capacity).enumerate() {
                let sample_buffer_offset = chunk_index
                    .checked_mul(chunk_capacity)
                    .and_then(|offset| offset.checked_mul(mem::size_of::<MetalMtmfsSample>()))
                    .ok_or_else(|| {
                        ImagingError::InvalidRequest(
                            "MTMFS Metal PSF sample buffer offset is too large".to_string(),
                        )
                    })?;
                let dispatch_timing = self.dispatch_mtmfs_psf_chunk(
                    chunk.len(),
                    sample_buffer_offset,
                    grid_width,
                    grid_height,
                    term_count,
                    &cache.sample_buffer,
                    &params_buffer,
                    &tap_weights_buffer,
                    &grid_re_buffer,
                    &grid_im_buffer,
                )?;
                timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                timings.dispatch_encode += dispatch_timing.encode;
                timings.dispatch_wait += dispatch_timing.wait;
                timings.dispatch_gpu += dispatch_timing.gpu;
                timings.dispatch_kernel += dispatch_timing.kernel;
                timings.staged_bytes = timings
                    .staged_bytes
                    .saturating_add(std::mem::size_of_val(chunk));
                chunks_dispatched = chunks_dispatched.saturating_add(1);
            }
        }

        let readback_started = Instant::now();
        let grid_re = unsafe {
            slice::from_raw_parts(
                grid_re_buffer.contents().as_ptr().cast::<u32>(),
                output_cells,
            )
        };
        let grid_im = unsafe {
            slice::from_raw_parts(
                grid_im_buffer.contents().as_ptr().cast::<u32>(),
                output_cells,
            )
        };
        let psf_grids = read_mtmfs_term_grids(
            grid_re,
            grid_im,
            term_count,
            cell_count,
            grid_width,
            grid_height,
        );
        timings.readback += readback_started.elapsed();
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "mtmfs_metal_psf_terms strategy={} chunks={} chunk_capacity={} terms={} gridded_samples={} skipped_samples={} input_buffer_ms={:.3} params_buffer_ms={:.3} tap_buffer_ms={:.3} grid_buffer_ms={:.3} dispatch_sample_buffer_ms={:.3} dispatch_params_buffer_ms={:.3} dispatch_encode_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3} staged_bytes={}",
                if use_grouped_psf {
                    "grouped"
                } else {
                    "global_atomic"
                },
                chunks_dispatched,
                chunk_capacity,
                term_count,
                cache.gridded_samples,
                cache.skipped_samples,
                profile::millis(timings.input_buffer),
                profile::millis(timings.params_buffer),
                profile::millis(timings.tap_buffer),
                profile::millis(timings.grid_buffer),
                profile::millis(timings.dispatch_sample_buffer),
                profile::millis(timings.dispatch_params_buffer),
                profile::millis(timings.dispatch_encode),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
                timings.staged_bytes,
            );
        }
        Ok(MtmfsMetalPsfAccumulation {
            psf_grids,
            normalization_sumwt: cache.normalization_sumwt,
            reported_sumwt_terms: cache
                .reported_sumwt_terms
                .iter()
                .copied()
                .take(term_count)
                .collect(),
            gridded_samples: cache.gridded_samples,
            skipped_samples: cache.skipped_samples,
        })
    }

    #[allow(dead_code)]
    fn accumulate_mtmfs_residual_grids(
        &self,
        gridder: &StandardGridder,
        batches: &[VisibilityBatch],
        sample_frequency_batches_hz: &[Vec<f64>],
        reffreq_hz: f64,
        term_count: usize,
        model_grids: Option<&[Array2<Complex32>]>,
    ) -> Result<MtmfsMetalResidualAccumulation, ImagingError> {
        let cache = self.prepare_mtmfs_input_cache(
            gridder,
            batches,
            sample_frequency_batches_hz,
            reffreq_hz,
            term_count,
        )?;
        self.accumulate_mtmfs_residual_grids_from_cache(gridder, &cache, term_count, model_grids)
    }

    fn accumulate_mtmfs_residual_grids_from_cache(
        &self,
        gridder: &StandardGridder,
        cache: &MtmfsMetalInputCache,
        term_count: usize,
        model_grids: Option<&[Array2<Complex32>]>,
    ) -> Result<MtmfsMetalResidualAccumulation, ImagingError> {
        use std::{mem, slice};

        use objc2_metal::{MTLBuffer, MTLDevice, MTLResourceOptions};

        if term_count == 0 {
            return Err(ImagingError::InvalidRequest(
                "MTMFS Metal residual requires at least one Taylor term".to_string(),
            ));
        }
        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal residual grid is too large".to_string())
        })?;
        let output_cells = term_count.checked_mul(cell_count).ok_or_else(|| {
            ImagingError::InvalidRequest("MTMFS Metal residual term grid is too large".to_string())
        })?;
        let storage_options = MTLResourceOptions::StorageModeShared;
        let mut timings = MtmfsMetalTermTimings::default();
        let chunk_capacity = standard_mfs_metal_residual_chunk_samples();
        let params_buffer_started = Instant::now();
        let params_buffer = self
            .device
            .newBufferWithLength_options(mem::size_of::<MetalMtmfsParams>(), storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate MTMFS residual params buffer"
                        .to_string(),
                )
            })?;
        timings.params_buffer += params_buffer_started.elapsed();
        let tap_buffer_started = Instant::now();
        let tap_weights_buffer =
            self.buffer_from_slice_no_copy(gridder.normalized_tap_weights(), storage_options)?;
        timings.tap_buffer += tap_buffer_started.elapsed();
        let model_term_count = model_grids.map_or(0, |grids| grids.len());
        let model_pack_started = Instant::now();
        let mut model_re = Vec::<f32>::with_capacity(model_term_count.max(1) * cell_count);
        let mut model_im = Vec::<f32>::with_capacity(model_term_count.max(1) * cell_count);
        if let Some(model_grids) = model_grids {
            if model_grids.len() != term_count {
                return Err(ImagingError::InvalidRequest(format!(
                    "MTMFS Metal residual expected {term_count} model terms but got {}",
                    model_grids.len()
                )));
            }
            for grid in model_grids {
                if grid.shape() != [grid_width, grid_height] {
                    return Err(ImagingError::InvalidRequest(format!(
                        "MTMFS Metal model grid shape {:?} differs from gridder shape {:?}",
                        grid.shape(),
                        [grid_width, grid_height]
                    )));
                }
                for value in grid.as_slice_memory_order().ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "MTMFS Metal model grids must be contiguous".to_string(),
                    )
                })? {
                    model_re.push(value.re);
                    model_im.push(value.im);
                }
            }
        } else {
            model_re.push(0.0);
            model_im.push(0.0);
        }
        timings.model_pack += model_pack_started.elapsed();
        let model_buffer_started = Instant::now();
        let model_re_buffer = self.buffer_from_slice(&model_re, storage_options)?;
        let model_im_buffer = self.buffer_from_slice(&model_im, storage_options)?;
        timings.model_buffer += model_buffer_started.elapsed();
        let grid_buffer_started = Instant::now();
        let zero_grid = vec![0_u32; output_cells];
        let grid_re_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        let grid_im_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        timings.grid_buffer += grid_buffer_started.elapsed();
        let use_grouped_residual = term_count == 2
            && mtmfs_metal_grouped_terms_enabled()
            && !cache.grouped_chunks.is_empty();
        let mut chunks_dispatched = 0usize;
        if use_grouped_residual {
            for chunk in &cache.grouped_chunks {
                let dispatch_timing = self.dispatch_mtmfs_grouped_residual_chunk(
                    chunk,
                    grid_width,
                    grid_height,
                    model_term_count,
                    &cache.sample_buffer,
                    &params_buffer,
                    &tap_weights_buffer,
                    &model_re_buffer,
                    &model_im_buffer,
                    &grid_re_buffer,
                    &grid_im_buffer,
                )?;
                timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                timings.dispatch_encode += dispatch_timing.encode;
                timings.dispatch_wait += dispatch_timing.wait;
                timings.dispatch_gpu += dispatch_timing.gpu;
                timings.dispatch_kernel += dispatch_timing.kernel;
                timings.staged_bytes = timings.staged_bytes.saturating_add(chunk.host_bytes());
                chunks_dispatched = chunks_dispatched.saturating_add(1);
            }
        } else {
            for (chunk_index, chunk) in cache.samples.chunks(chunk_capacity).enumerate() {
                let sample_buffer_offset = chunk_index
                    .checked_mul(chunk_capacity)
                    .and_then(|offset| offset.checked_mul(mem::size_of::<MetalMtmfsSample>()))
                    .ok_or_else(|| {
                        ImagingError::InvalidRequest(
                            "MTMFS Metal residual sample buffer offset is too large".to_string(),
                        )
                    })?;
                let dispatch_timing = self.dispatch_mtmfs_residual_chunk(
                    chunk.len(),
                    sample_buffer_offset,
                    grid_width,
                    grid_height,
                    term_count,
                    model_term_count,
                    &cache.sample_buffer,
                    &params_buffer,
                    &tap_weights_buffer,
                    &model_re_buffer,
                    &model_im_buffer,
                    &grid_re_buffer,
                    &grid_im_buffer,
                )?;
                timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                timings.dispatch_encode += dispatch_timing.encode;
                timings.dispatch_wait += dispatch_timing.wait;
                timings.dispatch_gpu += dispatch_timing.gpu;
                timings.dispatch_kernel += dispatch_timing.kernel;
                timings.staged_bytes = timings
                    .staged_bytes
                    .saturating_add(std::mem::size_of_val(chunk));
                chunks_dispatched = chunks_dispatched.saturating_add(1);
            }
        }

        let readback_started = Instant::now();
        let grid_re = unsafe {
            slice::from_raw_parts(
                grid_re_buffer.contents().as_ptr().cast::<u32>(),
                output_cells,
            )
        };
        let grid_im = unsafe {
            slice::from_raw_parts(
                grid_im_buffer.contents().as_ptr().cast::<u32>(),
                output_cells,
            )
        };
        let residual_grids = read_mtmfs_term_grids(
            grid_re,
            grid_im,
            term_count,
            cell_count,
            grid_width,
            grid_height,
        );
        timings.readback += readback_started.elapsed();
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "mtmfs_metal_residual_terms strategy={} chunks={} chunk_capacity={} terms={} model_terms={} gridded_samples={} skipped_samples={} input_buffer_ms={:.3} params_buffer_ms={:.3} tap_buffer_ms={:.3} model_pack_ms={:.3} model_buffer_ms={:.3} grid_buffer_ms={:.3} dispatch_sample_buffer_ms={:.3} dispatch_params_buffer_ms={:.3} dispatch_encode_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3} staged_bytes={}",
                if use_grouped_residual {
                    "grouped_nterms2"
                } else {
                    "global_atomic"
                },
                chunks_dispatched,
                chunk_capacity,
                term_count,
                model_term_count,
                cache.gridded_samples,
                cache.skipped_samples,
                profile::millis(timings.input_buffer),
                profile::millis(timings.params_buffer),
                profile::millis(timings.tap_buffer),
                profile::millis(timings.model_pack),
                profile::millis(timings.model_buffer),
                profile::millis(timings.grid_buffer),
                profile::millis(timings.dispatch_sample_buffer),
                profile::millis(timings.dispatch_params_buffer),
                profile::millis(timings.dispatch_encode),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
                timings.staged_bytes,
            );
        }
        Ok(MtmfsMetalResidualAccumulation { residual_grids })
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_mtmfs_grouped_psf_chunk(
        &self,
        chunk: &MtmfsMetalGroupedChunk,
        grid_width: usize,
        grid_height: usize,
        term_count: usize,
        sample_buffer: &MetalBuffer,
        params_buffer: &MetalBuffer,
        tap_weights: &MetalBuffer,
        grid_re: &MetalBuffer,
        grid_im: &MetalBuffer,
    ) -> Result<MtmfsMetalDispatchTiming, ImagingError> {
        use std::ptr;

        use objc2_metal::{
            MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
            MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState, MTLResourceOptions,
            MTLSize,
        };

        let mut timing = MtmfsMetalDispatchTiming::default();
        if chunk.is_empty() {
            return Ok(timing);
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let input_buffers_started = Instant::now();
        let group_desc_buffer = self.buffer_from_slice(&chunk.group_descs, storage_options)?;
        let lane_ref_buffer = self.buffer_from_slice(&chunk.lane_refs, storage_options)?;
        timing.sample_buffer += input_buffers_started.elapsed();
        let params = MetalMtmfsParams {
            sample_count: u32::try_from(chunk.sample_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped PSF chunk has too many samples".to_string(),
                )
            })?,
            grid_width: u32::try_from(grid_width).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid width exceeds u32".to_string())
            })?,
            grid_height: u32::try_from(grid_height).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid height exceeds u32".to_string())
            })?,
            term_count: u32::try_from(term_count).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal term count exceeds u32".to_string())
            })?,
            model_term_count: 0,
            _pad0: [0; 3],
        };
        let params_buffer_started = Instant::now();
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(params).cast::<u8>(),
                params_buffer.contents().as_ptr().cast::<u8>(),
                std::mem::size_of::<MetalMtmfsParams>(),
            );
        }
        timing.params_buffer += params_buffer_started.elapsed();

        let sample_buffer_offset = chunk
            .sample_start
            .checked_mul(std::mem::size_of::<MetalMtmfsSample>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped PSF sample buffer offset is too large".to_string(),
                )
            })?;
        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a grouped MTMFS PSF command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a grouped MTMFS PSF compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.mtmfs_grouped_psf_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(sample_buffer), sample_buffer_offset, 0);
            encoder.setBuffer_offset_atIndex(Some(&group_desc_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&lane_ref_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(params_buffer), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(tap_weights), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(grid_re), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(grid_im), 0, 6);
        }
        let accumulate_width = chunk.max_halo_cells.max(1);
        let accumulate_height = chunk.group_descs.len().max(1);
        let thread_width = self
            .mtmfs_grouped_psf_pipeline
            .threadExecutionWidth()
            .max(1);
        let max_threads = self
            .mtmfs_grouped_psf_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let group_width = thread_width.min(accumulate_width).max(1);
        let group_height = (max_threads / group_width)
            .max(1)
            .min(accumulate_height)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: accumulate_width,
                height: accumulate_height,
                depth: term_count,
            },
            MTLSize {
                width: group_width,
                height: group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' grouped MTMFS PSF command failed: {message}"
            )));
        }
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        Ok(timing)
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_mtmfs_grouped_residual_chunk(
        &self,
        chunk: &MtmfsMetalGroupedChunk,
        grid_width: usize,
        grid_height: usize,
        model_term_count: usize,
        sample_buffer: &MetalBuffer,
        params_buffer: &MetalBuffer,
        tap_weights: &MetalBuffer,
        model_re: &MetalBuffer,
        model_im: &MetalBuffer,
        grid_re: &MetalBuffer,
        grid_im: &MetalBuffer,
    ) -> Result<MtmfsMetalDispatchTiming, ImagingError> {
        use std::ptr;

        use objc2_metal::{
            MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
            MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState, MTLDevice,
            MTLResourceOptions, MTLSize,
        };

        let mut timing = MtmfsMetalDispatchTiming::default();
        if chunk.is_empty() {
            return Ok(timing);
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let input_buffers_started = Instant::now();
        let group_desc_buffer = self.buffer_from_slice(&chunk.group_descs, storage_options)?;
        let lane_ref_buffer = self.buffer_from_slice(&chunk.lane_refs, storage_options)?;
        let residual_lane_bytes = chunk
            .sample_count
            .checked_mul(std::mem::size_of::<MetalMtmfsGroupedResidualLane>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped residual lane buffer is too large".to_string(),
                )
            })?;
        let residual_lane_buffer = self
            .device
            .newBufferWithLength_options(residual_lane_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate grouped MTMFS residual lane buffer"
                        .to_string(),
                )
            })?;
        timing.sample_buffer += input_buffers_started.elapsed();
        let params = MetalMtmfsParams {
            sample_count: u32::try_from(chunk.sample_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped residual chunk has too many samples".to_string(),
                )
            })?,
            grid_width: u32::try_from(grid_width).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid width exceeds u32".to_string())
            })?,
            grid_height: u32::try_from(grid_height).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid height exceeds u32".to_string())
            })?,
            term_count: 2,
            model_term_count: u32::try_from(model_term_count).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal model term count exceeds u32".to_string())
            })?,
            _pad0: [0; 3],
        };
        let params_buffer_started = Instant::now();
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(params).cast::<u8>(),
                params_buffer.contents().as_ptr().cast::<u8>(),
                std::mem::size_of::<MetalMtmfsParams>(),
            );
        }
        timing.params_buffer += params_buffer_started.elapsed();

        let sample_buffer_offset = chunk
            .sample_start
            .checked_mul(std::mem::size_of::<MetalMtmfsSample>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "MTMFS Metal grouped residual sample buffer offset is too large".to_string(),
                )
            })?;
        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a grouped MTMFS residual command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a grouped MTMFS residual compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.mtmfs_grouped_residual_prepare_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(sample_buffer), sample_buffer_offset, 0);
            encoder.setBuffer_offset_atIndex(Some(params_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(tap_weights), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(model_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(model_im), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&residual_lane_buffer), 0, 5);
        }
        let prepare_thread_width = self
            .mtmfs_grouped_residual_prepare_pipeline
            .threadExecutionWidth()
            .max(1);
        let prepare_group_width = prepare_thread_width.min(chunk.sample_count).max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: chunk.sample_count,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: prepare_group_width,
                height: 1,
                depth: 1,
            },
        );

        encoder.setComputePipelineState(&self.mtmfs_grouped_residual_accumulate_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&residual_lane_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&group_desc_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&lane_ref_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(params_buffer), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(tap_weights), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(grid_re), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(grid_im), 0, 6);
        }
        let accumulate_width = chunk.max_halo_cells.max(1);
        let accumulate_height = chunk.group_descs.len().max(1);
        let accumulate_thread_width = self
            .mtmfs_grouped_residual_accumulate_pipeline
            .threadExecutionWidth()
            .max(1);
        let accumulate_max_threads = self
            .mtmfs_grouped_residual_accumulate_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let accumulate_group_width = accumulate_thread_width.min(accumulate_width).max(1);
        let accumulate_group_height = (accumulate_max_threads / accumulate_group_width)
            .max(1)
            .min(accumulate_height)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: accumulate_width,
                height: accumulate_height,
                depth: 2,
            },
            MTLSize {
                width: accumulate_group_width,
                height: accumulate_group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' grouped MTMFS residual command failed: {message}"
            )));
        }
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        Ok(timing)
    }

    #[allow(dead_code, clippy::too_many_arguments)]
    fn dispatch_mtmfs_psf_chunk(
        &self,
        sample_count: usize,
        sample_buffer_offset: usize,
        grid_width: usize,
        grid_height: usize,
        term_count: usize,
        sample_buffer: &MetalBuffer,
        params_buffer: &MetalBuffer,
        tap_weights: &MetalBuffer,
        grid_re: &MetalBuffer,
        grid_im: &MetalBuffer,
    ) -> Result<MtmfsMetalDispatchTiming, ImagingError> {
        self.dispatch_mtmfs_chunk(
            sample_count,
            sample_buffer_offset,
            grid_width,
            grid_height,
            term_count,
            0,
            sample_buffer,
            params_buffer,
            tap_weights,
            None,
            None,
            grid_re,
            grid_im,
            &self.mtmfs_psf_pipeline,
            "PSF",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_mtmfs_residual_chunk(
        &self,
        sample_count: usize,
        sample_buffer_offset: usize,
        grid_width: usize,
        grid_height: usize,
        term_count: usize,
        model_term_count: usize,
        sample_buffer: &MetalBuffer,
        params_buffer: &MetalBuffer,
        tap_weights: &MetalBuffer,
        model_re: &MetalBuffer,
        model_im: &MetalBuffer,
        grid_re: &MetalBuffer,
        grid_im: &MetalBuffer,
    ) -> Result<MtmfsMetalDispatchTiming, ImagingError> {
        self.dispatch_mtmfs_chunk(
            sample_count,
            sample_buffer_offset,
            grid_width,
            grid_height,
            term_count,
            model_term_count,
            sample_buffer,
            params_buffer,
            tap_weights,
            Some(model_re),
            Some(model_im),
            grid_re,
            grid_im,
            &self.mtmfs_residual_pipeline,
            "residual",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_mtmfs_chunk(
        &self,
        sample_count: usize,
        sample_buffer_offset: usize,
        grid_width: usize,
        grid_height: usize,
        term_count: usize,
        model_term_count: usize,
        sample_buffer: &MetalBuffer,
        params_buffer: &MetalBuffer,
        tap_weights: &MetalBuffer,
        model_re: Option<&MetalBuffer>,
        model_im: Option<&MetalBuffer>,
        grid_re: &MetalBuffer,
        grid_im: &MetalBuffer,
        pipeline: &MetalPipeline,
        label: &str,
    ) -> Result<MtmfsMetalDispatchTiming, ImagingError> {
        use std::ptr;

        use objc2_metal::{
            MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
            MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState, MTLSize,
        };

        let mut timing = MtmfsMetalDispatchTiming::default();
        if sample_count == 0 {
            return Ok(timing);
        }
        let params = MetalMtmfsParams {
            sample_count: u32::try_from(sample_count).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal chunk has too many samples".to_string())
            })?,
            grid_width: u32::try_from(grid_width).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid width exceeds u32".to_string())
            })?,
            grid_height: u32::try_from(grid_height).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal grid height exceeds u32".to_string())
            })?,
            term_count: u32::try_from(term_count).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal term count exceeds u32".to_string())
            })?,
            model_term_count: u32::try_from(model_term_count).map_err(|_| {
                ImagingError::InvalidRequest("MTMFS Metal model term count exceeds u32".to_string())
            })?,
            _pad0: [0; 3],
        };
        let params_buffer_started = Instant::now();
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(params).cast::<u8>(),
                params_buffer.contents().as_ptr().cast::<u8>(),
                std::mem::size_of::<MetalMtmfsParams>(),
            );
        }
        timing.params_buffer += params_buffer_started.elapsed();
        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' could not create an MTMFS {label} command buffer"
            ))
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' could not create an MTMFS {label} compute encoder"
            ))
        })?;
        encoder.setComputePipelineState(pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(sample_buffer), sample_buffer_offset, 0);
            encoder.setBuffer_offset_atIndex(Some(params_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(tap_weights), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(grid_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(grid_im), 0, 4);
            if let (Some(model_re), Some(model_im)) = (model_re, model_im) {
                encoder.setBuffer_offset_atIndex(Some(model_re), 0, 5);
                encoder.setBuffer_offset_atIndex(Some(model_im), 0, 6);
            }
        }
        let thread_width = pipeline.threadExecutionWidth().max(1);
        let max_threads = pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let group_width = thread_width.min(sample_count).max(1);
        let group_height = (max_threads / group_width).max(1).min(term_count).max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: sample_count,
                height: term_count,
                depth: 1,
            },
            MTLSize {
                width: group_width,
                height: group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' MTMFS {label} command failed: {message}"
            )));
        }
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        Ok(timing)
    }

    fn grid_residual_refresh_routed_visibility_runs(
        &self,
        gridder: &StandardGridder,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        use std::{mem, slice};

        use objc2_metal::{MTLBuffer, MTLDevice, MTLResourceOptions};

        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest(
                "standard MFS Metal residual grid is too large".to_string(),
            )
        })?;
        if residual_grid.shape() != [grid_width, grid_height] {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal residual grid shape {:?} differs from gridder shape {:?}",
                residual_grid.shape(),
                [grid_width, grid_height]
            )));
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let mut timings = MetalResidualRefreshTimings::default();
        let model_pack_started = Instant::now();
        let mut model_re = Vec::<f32>::with_capacity(cell_count);
        let mut model_im = Vec::<f32>::with_capacity(cell_count);
        if let Some(model_grid) = model_grid {
            if model_grid.shape() != [grid_width, grid_height] {
                return Err(ImagingError::InvalidRequest(format!(
                    "standard MFS Metal model grid shape {:?} differs from gridder shape {:?}",
                    model_grid.shape(),
                    [grid_width, grid_height]
                )));
            }
            for value in model_grid.as_slice_memory_order().ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal model grid must be contiguous".to_string(),
                )
            })? {
                model_re.push(value.re);
                model_im.push(value.im);
            }
        } else {
            model_re.resize(cell_count, 0.0);
            model_im.resize(cell_count, 0.0);
        }
        timings.model_pack += model_pack_started.elapsed();
        let model_buffer_started = Instant::now();
        let model_re_buffer = self.buffer_from_slice(&model_re, storage_options)?;
        let model_im_buffer = self.buffer_from_slice(&model_im, storage_options)?;
        timings.model_buffer += model_buffer_started.elapsed();
        let grid_buffer_started = Instant::now();
        let zero_grid = vec![0_u32; cell_count];
        let grid_re_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        let grid_im_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        timings.grid_buffer += grid_buffer_started.elapsed();
        let tap_weights_buffer =
            self.buffer_from_slice(gridder.normalized_tap_weights(), storage_options)?;
        let oversampling = u32::try_from(gridder.oversampling()).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal residual gridder oversampling exceeds u32".to_string(),
            )
        })?;
        let tap_weight_count =
            u32::try_from(gridder.normalized_tap_weights().len()).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal residual tap-weight table exceeds u32".to_string(),
                )
            })?;
        let chunk_capacity = standard_mfs_metal_residual_chunk_samples();
        let sample_buffer_bytes = chunk_capacity
            .checked_mul(mem::size_of::<MetalResidualSample>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal residual chunk buffer is too large".to_string(),
                )
            })?;
        let sample_buffer = self
            .device
            .newBufferWithLength_options(sample_buffer_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate residual sample buffer"
                        .to_string(),
                )
            })?;
        let params_buffer = self
            .device
            .newBufferWithLength_options(mem::size_of::<MetalResidualParams>(), storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal' could not allocate residual params buffer"
                        .to_string(),
                )
            })?;
        let sample_stride = standard_mfs_metal_residual_staging_sample_stride();
        let mut chunk = Vec::<MetalResidualSample>::with_capacity(chunk_capacity);
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut chunks_dispatched = 0usize;
        let mut prepared_samples = 0usize;
        let prepare_started = Instant::now();

        let replay_started = Instant::now();
        replay_routed_runs(&mut |routed_run| {
            let append_started = Instant::now();
            self.append_metal_residual_samples_from_routed_run(
                gridder,
                routed_run,
                weighting_plan,
                &mut accumulation,
                &mut chunk,
                sample_stride,
                &mut timings,
            )?;
            timings.append_total += append_started.elapsed();
            if chunk.len() >= chunk_capacity {
                let mut dispatched = 0usize;
                while dispatched < chunk.len() {
                    let end = (dispatched + chunk_capacity).min(chunk.len());
                    let chunk_slice = &chunk[dispatched..end];
                    let dispatch_timing = self.dispatch_residual_refresh_chunk(
                        chunk_slice,
                        grid_width,
                        grid_height,
                        MetalResidualDispatchBuffers {
                            sample_buffer: &sample_buffer,
                            params_buffer: &params_buffer,
                            tap_weights: &tap_weights_buffer,
                            oversampling,
                            tap_weight_count,
                            model_re: &model_re_buffer,
                            model_im: &model_im_buffer,
                            grid_re: &grid_re_buffer,
                            grid_im: &grid_im_buffer,
                        },
                    )?;
                    timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                    timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                    timings.dispatch_encode += dispatch_timing.encode;
                    timings.dispatch_wait += dispatch_timing.wait;
                    timings.staged_bytes = timings
                        .staged_bytes
                        .saturating_add(std::mem::size_of_val(chunk_slice));
                    prepared_samples = prepared_samples.saturating_add(chunk_slice.len());
                    chunks_dispatched = chunks_dispatched.saturating_add(1);
                    dispatched = end;
                }
                chunk.clear();
            }
            Ok(())
        })?;
        timings.replay += replay_started.elapsed();
        if !chunk.is_empty() {
            let mut dispatched = 0usize;
            while dispatched < chunk.len() {
                let end = (dispatched + chunk_capacity).min(chunk.len());
                let chunk_slice = &chunk[dispatched..end];
                let dispatch_timing = self.dispatch_residual_refresh_chunk(
                    chunk_slice,
                    grid_width,
                    grid_height,
                    MetalResidualDispatchBuffers {
                        sample_buffer: &sample_buffer,
                        params_buffer: &params_buffer,
                        tap_weights: &tap_weights_buffer,
                        oversampling,
                        tap_weight_count,
                        model_re: &model_re_buffer,
                        model_im: &model_im_buffer,
                        grid_re: &grid_re_buffer,
                        grid_im: &grid_im_buffer,
                    },
                )?;
                timings.dispatch_sample_buffer += dispatch_timing.sample_buffer;
                timings.dispatch_params_buffer += dispatch_timing.params_buffer;
                timings.dispatch_encode += dispatch_timing.encode;
                timings.dispatch_wait += dispatch_timing.wait;
                timings.staged_bytes = timings
                    .staged_bytes
                    .saturating_add(std::mem::size_of_val(chunk_slice));
                prepared_samples = prepared_samples.saturating_add(chunk_slice.len());
                chunks_dispatched = chunks_dispatched.saturating_add(1);
                dispatched = end;
            }
            chunk.clear();
        }

        let readback_started = Instant::now();
        let grid_re = unsafe {
            slice::from_raw_parts(grid_re_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let grid_im = unsafe {
            slice::from_raw_parts(grid_im_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        for ((cell, &re_bits), &im_bits) in residual_grid
            .as_slice_memory_order_mut()
            .expect("standard MFS residual grid should be contiguous")
            .iter_mut()
            .zip(grid_re)
            .zip(grid_im)
        {
            *cell = Complex64::new(
                f64::from(f32::from_bits(re_bits)),
                f64::from(f32::from_bits(im_bits)),
            );
        }
        timings.readback += readback_started.elapsed();
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_residual_refresh chunks={} chunk_capacity={} prepared_samples={} prepare_plus_dispatch_ms={:.3} dispatch_wait_ms={:.3} readback_ms={:.3}",
                chunks_dispatched,
                chunk_capacity,
                prepared_samples,
                profile::millis(prepare_started.elapsed()),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.readback),
            );
            eprintln!(
                "standard_mfs_metal_residual_refresh_detail model_pack_ms={:.3} model_buffer_ms={:.3} grid_buffer_ms={:.3} replay_ms={:.3} append_total_ms={:.3} run_wrap_ms={:.3} dispatch_sample_buffer_ms={:.3} dispatch_params_buffer_ms={:.3} dispatch_encode_ms={:.3} dispatch_wait_ms={:.3} readback_ms={:.3} sampled_samples={} sample_decode_sampled_ms={:.3} weight_sampled_ms={:.3} tap_plan_sampled_ms={:.3} axis_weight_sampled_ms={:.3} push_sampled_ms={:.3} staged_bytes={}",
                profile::millis(timings.model_pack),
                profile::millis(timings.model_buffer),
                profile::millis(timings.grid_buffer),
                profile::millis(timings.replay),
                profile::millis(timings.append_total),
                profile::millis(timings.run_wrap),
                profile::millis(timings.dispatch_sample_buffer),
                profile::millis(timings.dispatch_params_buffer),
                profile::millis(timings.dispatch_encode),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.readback),
                timings.sampled_samples,
                profile::millis(timings.sample_decode_sampled),
                profile::millis(timings.weight_sampled),
                profile::millis(timings.tap_plan_sampled),
                profile::millis(timings.axis_weight_sampled),
                profile::millis(timings.push_sampled),
                timings.staged_bytes,
            );
        }
        Ok(accumulation)
    }

    fn grid_residual_refresh_row_runs(
        &self,
        gridder: &StandardGridder,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        use std::slice;

        use objc2_metal::{MTLBuffer, MTLResourceOptions};

        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest(
                "standard MFS Metal row-run residual grid is too large".to_string(),
            )
        })?;
        if residual_grid.shape() != [grid_width, grid_height] {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal row-run residual grid shape {:?} differs from gridder shape {:?}",
                residual_grid.shape(),
                [grid_width, grid_height]
            )));
        }

        let reweight_plan = weighting_plan.reweight_plan()?;
        let (weighting_mode, density, density_convention, briggs_f2) = match reweight_plan {
            StandardMfsStreamingReweightPlan::Natural => (0_u32, None, 0_u32, 0.0_f32),
            StandardMfsStreamingReweightPlan::Uniform {
                density,
                convention,
            } => (
                1_u32,
                Some(density),
                metal_density_convention_code(convention),
                0.0_f32,
            ),
            StandardMfsStreamingReweightPlan::Briggs {
                density,
                convention,
                f2,
                use_bandwidth_taper,
                fractional_bandwidth,
            } => {
                if use_bandwidth_taper {
                    return Err(ImagingError::Unsupported(format!(
                        "standard MFS residual backend 'metal-row-run' does not yet support BriggsBwTaper weighting (fractional_bandwidth={fractional_bandwidth})"
                    )));
                }
                (
                    2_u32,
                    Some(density),
                    metal_density_convention_code(convention),
                    f2,
                )
            }
        };

        let storage_options = MTLResourceOptions::StorageModeShared;
        let diagnostic_mode = MetalResidualRowRunDiagnosticMode::from_env()?;
        let mut timings = MetalResidualRowRunRefreshTimings::default();
        let model_pack_started = Instant::now();
        let mut model_re = Vec::<f32>::with_capacity(cell_count);
        let mut model_im = Vec::<f32>::with_capacity(cell_count);
        if let Some(model_grid) = model_grid {
            if model_grid.shape() != [grid_width, grid_height] {
                return Err(ImagingError::InvalidRequest(format!(
                    "standard MFS Metal row-run model grid shape {:?} differs from gridder shape {:?}",
                    model_grid.shape(),
                    [grid_width, grid_height]
                )));
            }
            for value in model_grid.as_slice_memory_order().ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run model grid must be contiguous".to_string(),
                )
            })? {
                model_re.push(value.re);
                model_im.push(value.im);
            }
        } else {
            model_re.resize(cell_count, 0.0);
            model_im.resize(cell_count, 0.0);
        }
        timings.model_pack += model_pack_started.elapsed();
        let model_buffer_started = Instant::now();
        let model_re_buffer = self.buffer_from_slice(&model_re, storage_options)?;
        let model_im_buffer = self.buffer_from_slice(&model_im, storage_options)?;
        timings.model_buffer += model_buffer_started.elapsed();

        let density_buffer_started = Instant::now();
        let density_dummy = [0.0_f32];
        let (density_values, density_width, density_height) = if let Some(density) = density {
            let shape = density.shape();
            (
                density.as_slice_memory_order().ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal row-run density grid must be contiguous".to_string(),
                    )
                })?,
                shape[0],
                shape[1],
            )
        } else {
            (density_dummy.as_slice(), 1usize, 1usize)
        };
        let density_buffer = self.buffer_from_slice(density_values, storage_options)?;
        timings.density_buffer += density_buffer_started.elapsed();

        let grid_buffer_started = Instant::now();
        let zero_grid = vec![0_u32; cell_count];
        let grid_re_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        let grid_im_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        timings.grid_buffer += grid_buffer_started.elapsed();

        let tap_weights_buffer =
            self.buffer_from_slice(gridder.normalized_tap_weights(), storage_options)?;
        let shared = MetalResidualRowRunSharedBuffers {
            tap_weights: &tap_weights_buffer,
            density: &density_buffer,
            model_re: &model_re_buffer,
            model_im: &model_im_buffer,
            grid_re: &grid_re_buffer,
            grid_im: &grid_im_buffer,
        };
        let [du_lambda, dv_lambda] = gridder.grid_spacing_lambda();
        let density_params = gridder.density_grid_coordinate_params();
        let chunk_lane_capacity = standard_mfs_metal_residual_chunk_samples();
        let mut chunk = MetalResidualRowRunChunk::default();
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut chunks_dispatched = 0usize;
        let prepare_started = Instant::now();

        let mut flush_chunk = |chunk: &mut MetalResidualRowRunChunk,
                               timings: &mut MetalResidualRowRunRefreshTimings,
                               accumulation: &StandardMfsTiledResidualAccumulation|
         -> Result<(), ImagingError> {
            if chunk.is_empty() {
                return Ok(());
            }
            let dispatch_timing = self.dispatch_residual_row_run_chunk(
                chunk,
                MetalResidualRowRunParams {
                    run_count: u32::try_from(chunk.runs.len()).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run chunk has too many runs".to_string(),
                        )
                    })?,
                    max_lane_count: u32::try_from(chunk.max_lane_count).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run chunk has too many lanes per run"
                                .to_string(),
                        )
                    })?,
                    grid_width: u32::try_from(grid_width).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run grid width exceeds u32".to_string(),
                        )
                    })?,
                    grid_height: u32::try_from(grid_height).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run grid height exceeds u32".to_string(),
                        )
                    })?,
                    oversampling: u32::try_from(gridder.oversampling()).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run oversampling exceeds u32".to_string(),
                        )
                    })?,
                    tap_weight_count: u32::try_from(gridder.normalized_tap_weights().len())
                        .map_err(|_| {
                            ImagingError::InvalidRequest(
                                "standard MFS Metal row-run tap table exceeds u32".to_string(),
                            )
                        })?,
                    weighting_mode,
                    density_convention,
                    density_width: u32::try_from(density_width).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run density width exceeds u32".to_string(),
                        )
                    })?,
                    density_height: u32::try_from(density_height).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal row-run density height exceeds u32".to_string(),
                        )
                    })?,
                    diagnostic_mode: diagnostic_mode.code(),
                    _pad0: 0,
                    du_lambda: du_lambda as f32,
                    dv_lambda: dv_lambda as f32,
                    density_center_x: density_params.center_x as f32,
                    density_center_y: density_params.center_y as f32,
                    density_u_scale: density_params.u_scale as f32,
                    density_v_scale: density_params.v_scale as f32,
                    briggs_f2,
                    _pad1: 0.0,
                },
                diagnostic_mode,
                &shared,
            )?;
            timings.dispatch_input_buffers += dispatch_timing.sample_buffer;
            timings.dispatch_params_buffer += dispatch_timing.params_buffer;
            timings.dispatch_encode += dispatch_timing.encode;
            timings.dispatch_wait += dispatch_timing.wait;
            timings.dispatch_gpu += dispatch_timing.gpu;
            timings.dispatch_kernel += dispatch_timing.kernel;
            timings.staged_bytes = timings.staged_bytes.saturating_add(chunk.staged_bytes());
            if diagnostic_mode.uses_diagnostic_pipeline() {
                timings.diagnostic_output_bytes = timings
                    .diagnostic_output_bytes
                    .saturating_add(chunk.lanes.len().saturating_mul(std::mem::size_of::<u32>()));
            }
            let candidate_tap_visits = (chunk.logical_lanes as u64)
                .saturating_mul(7)
                .saturating_mul(7);
            timings.candidate_tap_visits = timings
                .candidate_tap_visits
                .saturating_add(candidate_tap_visits);
            match diagnostic_mode {
                MetalResidualRowRunDiagnosticMode::Exact => {
                    timings.candidate_model_reads = timings
                        .candidate_model_reads
                        .saturating_add(candidate_tap_visits.saturating_mul(2));
                    timings.candidate_grid_atomic_adds = timings
                        .candidate_grid_atomic_adds
                        .saturating_add(candidate_tap_visits.saturating_mul(2));
                }
                MetalResidualRowRunDiagnosticMode::DegridOnly => {
                    timings.candidate_model_reads = timings
                        .candidate_model_reads
                        .saturating_add(candidate_tap_visits.saturating_mul(2));
                }
                MetalResidualRowRunDiagnosticMode::GridOnly => {
                    timings.candidate_grid_atomic_adds = timings
                        .candidate_grid_atomic_adds
                        .saturating_add(candidate_tap_visits.saturating_mul(2));
                }
                MetalResidualRowRunDiagnosticMode::SingleTap => {
                    timings.candidate_grid_atomic_adds = timings
                        .candidate_grid_atomic_adds
                        .saturating_add((chunk.logical_lanes as u64).saturating_mul(2));
                }
                MetalResidualRowRunDiagnosticMode::TapPlanOnly => {}
            }
            timings.runs = timings.runs.saturating_add(chunk.runs.len());
            timings.logical_lanes = timings.logical_lanes.saturating_add(chunk.logical_lanes);
            chunks_dispatched = chunks_dispatched.saturating_add(1);
            let _ = accumulation;
            chunk.clear();
            Ok(())
        };

        let replay_started = Instant::now();
        replay_routed_runs(&mut |routed_run| {
            if !chunk.is_empty()
                && chunk.logical_lanes.saturating_add(routed_run.len()) > chunk_lane_capacity
            {
                flush_chunk(&mut chunk, &mut timings, &accumulation)?;
            }
            let append_started = Instant::now();
            self.append_metal_residual_row_run(gridder, routed_run, &mut accumulation, &mut chunk)?;
            timings.append_total += append_started.elapsed();
            if chunk.logical_lanes >= chunk_lane_capacity {
                flush_chunk(&mut chunk, &mut timings, &accumulation)?;
            }
            Ok(())
        })?;
        timings.replay += replay_started.elapsed();
        flush_chunk(&mut chunk, &mut timings, &accumulation)?;

        let readback_started = Instant::now();
        let grid_re = unsafe {
            slice::from_raw_parts(grid_re_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let grid_im = unsafe {
            slice::from_raw_parts(grid_im_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        for ((cell, &re_bits), &im_bits) in residual_grid
            .as_slice_memory_order_mut()
            .expect("standard MFS residual grid should be contiguous")
            .iter_mut()
            .zip(grid_re)
            .zip(grid_im)
        {
            *cell = Complex64::new(
                f64::from(f32::from_bits(re_bits)),
                f64::from(f32::from_bits(im_bits)),
            );
        }
        timings.readback += readback_started.elapsed();
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_row_run_residual_refresh mode={} chunks={} chunk_lane_capacity={} runs={} logical_lanes={} prepare_plus_dispatch_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3}",
                diagnostic_mode.label(),
                chunks_dispatched,
                chunk_lane_capacity,
                timings.runs,
                timings.logical_lanes,
                profile::millis(prepare_started.elapsed()),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
            );
            eprintln!(
                "standard_mfs_metal_row_run_residual_refresh_detail model_pack_ms={:.3} model_buffer_ms={:.3} density_buffer_ms={:.3} grid_buffer_ms={:.3} replay_ms={:.3} append_total_ms={:.3} dispatch_input_buffers_ms={:.3} dispatch_params_buffer_ms={:.3} dispatch_encode_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3} staged_bytes={} diagnostic_output_bytes={} candidate_tap_visits={} candidate_model_reads={} candidate_grid_atomic_adds={} unsupported_runs={}",
                profile::millis(timings.model_pack),
                profile::millis(timings.model_buffer),
                profile::millis(timings.density_buffer),
                profile::millis(timings.grid_buffer),
                profile::millis(timings.replay),
                profile::millis(timings.append_total),
                profile::millis(timings.dispatch_input_buffers),
                profile::millis(timings.dispatch_params_buffer),
                profile::millis(timings.dispatch_encode),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
                timings.staged_bytes,
                timings.diagnostic_output_bytes,
                timings.candidate_tap_visits,
                timings.candidate_model_reads,
                timings.candidate_grid_atomic_adds,
                timings.unsupported_runs,
            );
        }
        Ok(accumulation)
    }

    fn grid_residual_refresh_row_runs_grouped(
        &self,
        gridder: &StandardGridder,
        replay_routed_runs: &mut MetalResidualRunReplay<'_>,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        model_grid: Option<&Array2<Complex32>>,
        residual_grid: &mut Array2<Complex64>,
        input_cache: Option<&mut StandardMfsMetalGroupedInputCache>,
    ) -> Result<StandardMfsTiledResidualAccumulation, ImagingError> {
        use std::slice;

        use objc2_metal::{MTLBuffer, MTLResourceOptions};

        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run residual grid is too large".to_string(),
            )
        })?;
        if residual_grid.shape() != [grid_width, grid_height] {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal grouped row-run residual grid shape {:?} differs from gridder shape {:?}",
                residual_grid.shape(),
                [grid_width, grid_height]
            )));
        }

        let reweight_plan = weighting_plan.reweight_plan()?;
        let (weighting_mode, density, density_convention, briggs_f2) = match reweight_plan {
            StandardMfsStreamingReweightPlan::Natural => (0_u32, None, 0_u32, 0.0_f32),
            StandardMfsStreamingReweightPlan::Uniform {
                density,
                convention,
            } => (
                1_u32,
                Some(density),
                metal_density_convention_code(convention),
                0.0_f32,
            ),
            StandardMfsStreamingReweightPlan::Briggs {
                density,
                convention,
                f2,
                use_bandwidth_taper,
                fractional_bandwidth,
            } => {
                if use_bandwidth_taper {
                    return Err(ImagingError::Unsupported(format!(
                        "standard MFS residual backend 'metal-row-run-grouped' does not yet support BriggsBwTaper weighting (fractional_bandwidth={fractional_bandwidth})"
                    )));
                }
                (
                    2_u32,
                    Some(density),
                    metal_density_convention_code(convention),
                    f2,
                )
            }
        };

        let storage_options = MTLResourceOptions::StorageModeShared;
        let group_tile_edge = standard_mfs_metal_group_tile_edge();
        let partition =
            MetalResidualGroupedTilePartition::new(grid_width, grid_height, group_tile_edge)?;
        let mut timings = MetalResidualRowRunRefreshTimings {
            group_tile_edge,
            ..Default::default()
        };

        let model_pack_started = Instant::now();
        let mut model_re = Vec::<f32>::with_capacity(cell_count);
        let mut model_im = Vec::<f32>::with_capacity(cell_count);
        if let Some(model_grid) = model_grid {
            if model_grid.shape() != [grid_width, grid_height] {
                return Err(ImagingError::InvalidRequest(format!(
                    "standard MFS Metal grouped row-run model grid shape {:?} differs from gridder shape {:?}",
                    model_grid.shape(),
                    [grid_width, grid_height]
                )));
            }
            for value in model_grid.as_slice_memory_order().ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run model grid must be contiguous".to_string(),
                )
            })? {
                model_re.push(value.re);
                model_im.push(value.im);
            }
        } else {
            model_re.resize(cell_count, 0.0);
            model_im.resize(cell_count, 0.0);
        }
        timings.model_pack += model_pack_started.elapsed();
        let model_buffer_started = Instant::now();
        let model_re_buffer = self.buffer_from_slice(&model_re, storage_options)?;
        let model_im_buffer = self.buffer_from_slice(&model_im, storage_options)?;
        timings.model_buffer += model_buffer_started.elapsed();

        let density_buffer_started = Instant::now();
        let density_dummy = [0.0_f32];
        let (density_values, density_width, density_height) = if let Some(density) = density {
            let shape = density.shape();
            (
                density.as_slice_memory_order().ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal grouped row-run density grid must be contiguous"
                            .to_string(),
                    )
                })?,
                shape[0],
                shape[1],
            )
        } else {
            (density_dummy.as_slice(), 1usize, 1usize)
        };
        let density_buffer = self.buffer_from_slice(density_values, storage_options)?;
        timings.density_buffer += density_buffer_started.elapsed();

        let grid_buffer_started = Instant::now();
        let zero_grid = vec![0_u32; cell_count];
        let grid_re_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        let grid_im_buffer = self.buffer_from_slice(&zero_grid, storage_options)?;
        timings.grid_buffer += grid_buffer_started.elapsed();

        let tap_weights_buffer =
            self.buffer_from_slice(gridder.normalized_tap_weights(), storage_options)?;
        let shared = MetalResidualRowRunSharedBuffers {
            tap_weights: &tap_weights_buffer,
            density: &density_buffer,
            model_re: &model_re_buffer,
            model_im: &model_im_buffer,
            grid_re: &grid_re_buffer,
            grid_im: &grid_im_buffer,
        };
        let [du_lambda, dv_lambda] = gridder.grid_spacing_lambda();
        let density_params = gridder.density_grid_coordinate_params();
        let chunk_lane_capacity = standard_mfs_metal_residual_chunk_samples();
        let cache_key = MetalResidualGroupedInputCacheKey {
            lane_layout_version: METAL_RESIDUAL_ROW_RUN_LANE_LAYOUT_VERSION,
            grid_width,
            grid_height,
            oversampling: gridder.oversampling(),
            tap_weight_count: gridder.normalized_tap_weights().len(),
            weighting_mode,
            density_convention,
            density_width,
            density_height,
            briggs_f2_bits: briggs_f2.to_bits(),
            group_tile_edge,
            group_tile_count: partition.tile_count(),
            chunk_lane_capacity,
            du_lambda_bits: (du_lambda as f32).to_bits(),
            dv_lambda_bits: (dv_lambda as f32).to_bits(),
            density_center_x_bits: (density_params.center_x as f32).to_bits(),
            density_center_y_bits: (density_params.center_y as f32).to_bits(),
            density_u_scale_bits: (density_params.u_scale as f32).to_bits(),
            density_v_scale_bits: (density_params.v_scale as f32).to_bits(),
        };
        let mut input_cache = input_cache;
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut chunks_dispatched = 0usize;
        let prepare_started = Instant::now();

        if let Some(cache) = input_cache.as_deref_mut() {
            if cache.matches(cache_key) {
                timings.input_cache_hit = true;
                timings.input_cache_chunks = cache.chunks.len();
                timings.input_cache_host_bytes = cache.host_bytes;
                accumulation = cache.accumulation;
                for cached in &cache.chunks {
                    let dispatch_timing = if cached.buffers.is_some() {
                        self.dispatch_cached_residual_row_run_grouped_chunk(cached, &shared)?
                    } else if let Some(host) = cached.host.as_ref() {
                        self.dispatch_residual_row_run_grouped_chunk(
                            host,
                            cached.params,
                            &shared,
                            MetalInputBufferCopyMode::NoCopy,
                        )?
                    } else {
                        return Err(ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run cache chunk has no input payload"
                                .to_string(),
                        ));
                    };
                    chunks_dispatched = chunks_dispatched.saturating_add(1);
                    record_metal_grouped_residual_dispatch(
                        chunks_dispatched,
                        cached.metrics,
                        dispatch_timing,
                        &mut timings,
                    );
                }
            }
        }

        if !timings.input_cache_hit {
            let cache_fill_enabled = input_cache.is_some();
            let mut cached_chunks = cache_fill_enabled.then(Vec::new);
            let mut chunk = MetalResidualGroupedRowRunChunk::new(partition.tile_count());

            let mut flush_chunk = |chunk: &mut MetalResidualGroupedRowRunChunk,
                                   timings: &mut MetalResidualRowRunRefreshTimings,
                                   cached_chunks: &mut Option<
                Vec<MetalResidualGroupedCachedChunk>,
            >|
             -> Result<(), ImagingError> {
                if chunk.is_empty() {
                    return Ok(());
                }
                let finalize_started = Instant::now();
                chunk.finalize_groups(&partition)?;
                timings.append_detail.group_finalize += finalize_started.elapsed();
                let params = MetalResidualRowRunParams {
                    run_count: u32::try_from(chunk.row_runs.runs.len()).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run chunk has too many runs"
                                .to_string(),
                        )
                    })?,
                    max_lane_count: u32::try_from(chunk.row_runs.max_lane_count).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run chunk has too many lanes per run"
                                .to_string(),
                        )
                    })?,
                    grid_width: u32::try_from(grid_width).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run grid width exceeds u32".to_string(),
                        )
                    })?,
                    grid_height: u32::try_from(grid_height).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run grid height exceeds u32"
                                .to_string(),
                        )
                    })?,
                    oversampling: u32::try_from(gridder.oversampling()).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run oversampling exceeds u32"
                                .to_string(),
                        )
                    })?,
                    tap_weight_count: u32::try_from(gridder.normalized_tap_weights().len())
                        .map_err(|_| {
                            ImagingError::InvalidRequest(
                                "standard MFS Metal grouped row-run tap table exceeds u32"
                                    .to_string(),
                            )
                        })?,
                    weighting_mode,
                    density_convention,
                    density_width: u32::try_from(density_width).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run density width exceeds u32"
                                .to_string(),
                        )
                    })?,
                    density_height: u32::try_from(density_height).map_err(|_| {
                        ImagingError::InvalidRequest(
                            "standard MFS Metal grouped row-run density height exceeds u32"
                                .to_string(),
                        )
                    })?,
                    diagnostic_mode: 0,
                    _pad0: 0,
                    du_lambda: du_lambda as f32,
                    dv_lambda: dv_lambda as f32,
                    density_center_x: density_params.center_x as f32,
                    density_center_y: density_params.center_y as f32,
                    density_u_scale: density_params.u_scale as f32,
                    density_v_scale: density_params.v_scale as f32,
                    briggs_f2,
                    _pad1: 0.0,
                };
                let dispatch_timing = self.dispatch_residual_row_run_grouped_chunk(
                    chunk,
                    params,
                    &shared,
                    MetalInputBufferCopyMode::Copy,
                )?;
                chunks_dispatched = chunks_dispatched.saturating_add(1);
                let metrics = MetalResidualGroupedChunkMetrics::from_chunk(chunk);
                record_metal_grouped_residual_dispatch(
                    chunks_dispatched,
                    metrics,
                    dispatch_timing,
                    timings,
                );
                if let Some(cached_chunks) = cached_chunks.as_mut() {
                    chunk.clear_group_scratch_after_finalize();
                    let finalized_chunk = std::mem::replace(
                        chunk,
                        MetalResidualGroupedRowRunChunk::new(partition.tile_count()),
                    );
                    cached_chunks.push(self.cached_grouped_chunk(
                        finalized_chunk,
                        params,
                        storage_options,
                    )?);
                } else {
                    chunk.clear();
                }
                Ok(())
            };

            let replay_started = Instant::now();
            replay_routed_runs(&mut |routed_run| {
                if !chunk.is_empty()
                    && chunk
                        .row_runs
                        .logical_lanes
                        .saturating_add(routed_run.len())
                        > chunk_lane_capacity
                {
                    flush_chunk(&mut chunk, &mut timings, &mut cached_chunks)?;
                }
                let append_started = Instant::now();
                let parts = MetalRowRunParts {
                    row: routed_run.row.as_ref(),
                    source_slot_range: routed_run.source_slot_range.clone(),
                    tap_centers: routed_run.tap_centers.as_ref(),
                    grid_width,
                    grid_height,
                    du_lambda,
                    dv_lambda,
                };
                self.append_metal_residual_grouped_row_run_profiled(
                    parts,
                    &partition,
                    &mut accumulation,
                    &mut chunk,
                    &mut timings.append_detail,
                )?;
                timings.append_total += append_started.elapsed();
                if chunk.row_runs.logical_lanes >= chunk_lane_capacity {
                    flush_chunk(&mut chunk, &mut timings, &mut cached_chunks)?;
                }
                Ok(())
            })?;
            timings.replay += replay_started.elapsed();
            flush_chunk(&mut chunk, &mut timings, &mut cached_chunks)?;
            if let (Some(cache), Some(cached_chunks)) = (input_cache, cached_chunks) {
                timings.input_cache_fill = true;
                timings.input_cache_chunks = cached_chunks.len();
                cache.replace(cache_key, cached_chunks, accumulation, None);
                timings.input_cache_host_bytes = cache.host_bytes;
            }
        }

        let readback_started = Instant::now();
        let grid_re = unsafe {
            slice::from_raw_parts(grid_re_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let grid_im = unsafe {
            slice::from_raw_parts(grid_im_buffer.contents().as_ptr().cast::<u32>(), cell_count)
        };
        for ((cell, &re_bits), &im_bits) in residual_grid
            .as_slice_memory_order_mut()
            .expect("standard MFS residual grid should be contiguous")
            .iter_mut()
            .zip(grid_re)
            .zip(grid_im)
        {
            *cell = Complex64::new(
                f64::from(f32::from_bits(re_bits)),
                f64::from(f32::from_bits(im_bits)),
            );
        }
        timings.readback += readback_started.elapsed();
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_row_run_grouped_residual_refresh chunks={} chunk_lane_capacity={} group_tile_edge={} runs={} logical_lanes={} group_descs={} lane_refs={} input_cache_hit={} input_cache_fill={} input_cache_chunks={} input_cache_host_bytes={} prepare_plus_dispatch_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3}",
                chunks_dispatched,
                chunk_lane_capacity,
                timings.group_tile_edge,
                timings.runs,
                timings.logical_lanes,
                timings.group_descs,
                timings.lane_refs,
                timings.input_cache_hit,
                timings.input_cache_fill,
                timings.input_cache_chunks,
                timings.input_cache_host_bytes,
                profile::millis(prepare_started.elapsed()),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
            );
            eprintln!(
                "standard_mfs_metal_row_run_grouped_residual_refresh_detail model_pack_ms={:.3} model_buffer_ms={:.3} density_buffer_ms={:.3} grid_buffer_ms={:.3} replay_ms={:.3} append_total_ms={:.3} dispatch_input_buffers_ms={:.3} dispatch_params_buffer_ms={:.3} dispatch_encode_ms={:.3} dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} readback_ms={:.3} staged_bytes={} candidate_tap_visits={} candidate_model_reads={} exact_candidate_grid_atomic_adds={} grouped_candidate_grid_atomic_adds={} grouped_candidate_scan_tests={} unsupported_runs={} input_cache_hit={} input_cache_fill={} input_cache_chunks={} input_cache_host_bytes={}",
                profile::millis(timings.model_pack),
                profile::millis(timings.model_buffer),
                profile::millis(timings.density_buffer),
                profile::millis(timings.grid_buffer),
                profile::millis(timings.replay),
                profile::millis(timings.append_total),
                profile::millis(timings.dispatch_input_buffers),
                profile::millis(timings.dispatch_params_buffer),
                profile::millis(timings.dispatch_encode),
                profile::millis(timings.dispatch_wait),
                profile::millis(timings.dispatch_gpu),
                profile::millis(timings.dispatch_kernel),
                profile::millis(timings.readback),
                timings.staged_bytes,
                timings.candidate_tap_visits,
                timings.candidate_model_reads,
                timings.candidate_grid_atomic_adds,
                timings.candidate_group_cell_atomic_adds,
                timings.candidate_group_scan_tests,
                timings.unsupported_runs,
                timings.input_cache_hit,
                timings.input_cache_fill,
                timings.input_cache_chunks,
                timings.input_cache_host_bytes,
            );
            eprintln!(
                "standard_mfs_metal_row_run_grouped_append_detail setup_ms={:.3} lane_push_ms={:.3} data_flag_copy_ms={:.3} run_desc_ms={:.3} group_assign_ms={:.3} group_finalize_ms={:.3}",
                profile::millis(timings.append_detail.setup),
                profile::millis(timings.append_detail.lane_push),
                profile::millis(timings.append_detail.data_flag_copy),
                profile::millis(timings.append_detail.run_desc),
                profile::millis(timings.append_detail.group_assign),
                profile::millis(timings.append_detail.group_finalize),
            );
        }
        Ok(accumulation)
    }

    fn begin_grouped_initial_dirty_state(
        &self,
        gridder: &StandardGridder,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<MetalInitialDirtyGroupedState, ImagingError> {
        use objc2_metal::MTLDevice;

        let [grid_width, grid_height] = gridder.grid_shape();
        let cell_count = grid_width.checked_mul(grid_height).ok_or_else(|| {
            ImagingError::InvalidRequest(
                "standard MFS Metal grouped initial dirty grid is too large".to_string(),
            )
        })?;
        let fill =
            self.begin_grouped_input_cache_fill_for_initial_dirty(gridder, weighting_plan)?;
        let storage_options = objc2_metal::MTLResourceOptions::StorageModeShared;
        let reweight_plan = weighting_plan.reweight_plan()?;
        let density_dummy = [0.0_f32];
        let density_values = match reweight_plan {
            StandardMfsStreamingReweightPlan::Natural => density_dummy.as_slice(),
            StandardMfsStreamingReweightPlan::Uniform { density, .. }
            | StandardMfsStreamingReweightPlan::Briggs { density, .. } => {
                density.as_slice_memory_order().ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal grouped initial dirty density grid must be contiguous"
                            .to_string(),
                    )
                })?
            }
        };
        let density = self.buffer_from_slice(density_values, storage_options)?;
        let tap_weights =
            self.buffer_from_slice(gridder.normalized_tap_weights(), storage_options)?;
        let grid_bytes = cell_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped initial dirty grid buffer is too large".to_string(),
                )
            })?;
        let psf_re = self
            .device
            .newBufferWithLength_options(grid_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate PSF real grid"
                        .to_string(),
                )
            })?;
        let psf_im = self
            .device
            .newBufferWithLength_options(grid_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate PSF imaginary grid"
                        .to_string(),
                )
            })?;
        let dirty_re = self
            .device
            .newBufferWithLength_options(grid_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate dirty real grid"
                        .to_string(),
                )
            })?;
        let dirty_im = self
            .device
            .newBufferWithLength_options(grid_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate dirty imaginary grid"
                        .to_string(),
                )
            })?;
        Ok(MetalInitialDirtyGroupedState {
            fill,
            density,
            tap_weights,
            psf_re,
            psf_im,
            dirty_re,
            dirty_im,
            pending: Vec::new(),
            storage_options,
            append_grouped_row_run: Duration::ZERO,
            append_detail: MetalGroupedAppendDetail::default(),
            dirty_accumulation: Duration::ZERO,
            chunk_finalize_dispatch: Duration::ZERO,
        })
    }

    fn append_grouped_initial_dirty_run(
        &self,
        state: &mut MetalInitialDirtyGroupedState,
        routed_run: &StandardMfsRoutedVisibilityRun,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<(), ImagingError> {
        if !state.fill.chunk.is_empty()
            && state
                .fill
                .chunk
                .row_runs
                .logical_lanes
                .saturating_add(routed_run.len())
                > state.fill.chunk_lane_capacity
        {
            self.finish_grouped_initial_dirty_chunk(state)?;
        }
        let append_started = Instant::now();
        let parts = MetalRowRunParts {
            row: routed_run.row.as_ref(),
            source_slot_range: routed_run.source_slot_range.clone(),
            tap_centers: routed_run.tap_centers.as_ref(),
            grid_width: state.fill.grid_width,
            grid_height: state.fill.grid_height,
            du_lambda: state.fill.du_lambda as f64,
            dv_lambda: state.fill.dv_lambda as f64,
        };
        self.append_metal_residual_grouped_row_run_profiled(
            parts,
            &state.fill.partition,
            &mut state.fill.accumulation,
            &mut state.fill.chunk,
            &mut state.append_detail,
        )?;
        state.append_grouped_row_run += append_started.elapsed();
        let _ = weighting_plan;
        if state.fill.chunk.row_runs.logical_lanes >= state.fill.chunk_lane_capacity {
            self.finish_grouped_initial_dirty_chunk(state)?;
        }
        Ok(())
    }

    fn finish_grouped_initial_dirty_chunk(
        &self,
        state: &mut MetalInitialDirtyGroupedState,
    ) -> Result<(), ImagingError> {
        if state.fill.chunk.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let finalize_started = Instant::now();
        state.fill.chunk.finalize_groups(&state.fill.partition)?;
        state.append_detail.group_finalize += finalize_started.elapsed();
        let params = grouped_row_run_params_from_fill(&state.fill)?;
        state.fill.chunk.clear_group_scratch_after_finalize();
        let finalized_chunk = std::mem::replace(
            &mut state.fill.chunk,
            MetalResidualGroupedRowRunChunk::new(state.fill.partition.tile_count()),
        );
        let metrics = MetalResidualGroupedChunkMetrics::from_chunk(&finalized_chunk);
        state.fill.chunks.push(MetalResidualGroupedCachedChunk {
            params,
            metrics,
            host: Some(finalized_chunk),
            buffers: None,
        });
        let cached = state
            .fill
            .chunks
            .last()
            .expect("chunk was just pushed into grouped initial dirty state");
        let pending = self.dispatch_initial_dirty_grouped_chunk_async(cached, state)?;
        state.pending.push(pending);
        state.chunk_finalize_dispatch += started.elapsed();
        Ok(())
    }

    fn dispatch_initial_dirty_grouped_chunk_async(
        &self,
        cached: &MetalResidualGroupedCachedChunk,
        state: &MetalInitialDirtyGroupedState,
    ) -> Result<MetalInitialDirtyGroupedPendingDispatch, ImagingError> {
        use std::slice;

        use objc2_metal::{
            MTLCommandBuffer, MTLCommandEncoder, MTLCommandQueue, MTLComputeCommandEncoder,
            MTLComputePipelineState, MTLDevice, MTLResourceOptions, MTLSize,
        };

        let Some(host) = cached.host.as_ref() else {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped initial dirty cache chunk has no host input payload"
                    .to_string(),
            ));
        };
        if host.is_empty() || host.group_descs.is_empty() {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped initial dirty cannot dispatch an empty grouped chunk"
                    .to_string(),
            ));
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let run = self.buffer_from_slice_no_copy(&host.row_runs.runs, storage_options)?;
        let lane = self.buffer_from_slice_no_copy(&host.row_runs.lanes, storage_options)?;
        let data = self.buffer_from_slice_no_copy(&host.row_runs.data, storage_options)?;
        let flag = self.buffer_from_slice_no_copy(&host.row_runs.flags, storage_options)?;
        let weight = self.buffer_from_slice_no_copy(&host.row_runs.weights, storage_options)?;
        let group_desc = self.buffer_from_slice_no_copy(&host.group_descs, storage_options)?;
        let lane_ref = self.buffer_from_slice_no_copy(&host.lane_refs, storage_options)?;
        let grouped_lane_bytes = host
            .row_runs
            .lanes
            .len()
            .checked_mul(std::mem::size_of::<MetalInitialDirtyGroupedLane>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped initial dirty lane buffer is too large".to_string(),
                )
            })?;
        let grouped_lane = self
            .device
            .newBufferWithLength_options(grouped_lane_bytes, state.storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate lane buffer"
                        .to_string(),
                )
            })?;
        let run_accum_bytes = host
            .row_runs
            .runs
            .len()
            .checked_mul(std::mem::size_of::<MetalInitialDirtyRunAccum>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped initial dirty run accumulation buffer is too large"
                        .to_string(),
                )
            })?;
        let run_accum = self
            .device
            .newBufferWithLength_options(run_accum_bytes, state.storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty could not allocate run accumulation buffer"
                        .to_string(),
                )
            })?;
        let params =
            self.buffer_from_slice(slice::from_ref(&cached.params), state.storage_options)?;
        let prepare_pipeline = self
            .initial_dirty_grouped_prepare_pipeline
            .as_ref()
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty prepare pipeline is not enabled"
                        .to_string(),
                )
            })?;
        let accumulate_pipeline = self
            .initial_dirty_grouped_accumulate_pipeline
            .as_ref()
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty accumulate pipeline is not enabled"
                        .to_string(),
                )
            })?;
        let run_accum_pipeline = self
            .initial_dirty_grouped_run_accum_pipeline
            .as_ref()
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS Metal grouped initial dirty run accumulation pipeline is not enabled"
                        .to_string(),
                )
            })?;
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS Metal grouped initial dirty could not create a command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS Metal grouped initial dirty could not create a compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(prepare_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&run), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&lane), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&data), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&flag), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&weight), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&state.density), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(&grouped_lane), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(&params), 0, 7);
            encoder.setBuffer_offset_atIndex(Some(&state.tap_weights), 0, 8);
        }
        let prepare_thread_width = prepare_pipeline.threadExecutionWidth().max(1);
        let prepare_max_threads = prepare_pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let prepare_group_width = prepare_thread_width
            .min(cached.params.max_lane_count as usize)
            .max(1);
        let prepare_group_height = (prepare_max_threads / prepare_group_width)
            .max(1)
            .min(cached.params.run_count as usize)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: cached.params.max_lane_count as usize,
                height: cached.params.run_count as usize,
                depth: 1,
            },
            MTLSize {
                width: prepare_group_width,
                height: prepare_group_height,
                depth: 1,
            },
        );
        encoder.setComputePipelineState(run_accum_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&run), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&lane), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&data), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&flag), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&weight), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&state.density), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(&run_accum), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(&params), 0, 7);
        }
        let run_accum_thread_width = run_accum_pipeline.threadExecutionWidth().max(1);
        let run_accum_max_threads = run_accum_pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let run_accum_group_width = run_accum_thread_width
            .min(cached.params.run_count as usize)
            .min(run_accum_max_threads)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: cached.params.run_count as usize,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: run_accum_group_width,
                height: 1,
                depth: 1,
            },
        );
        encoder.setComputePipelineState(accumulate_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&grouped_lane), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&group_desc), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&lane_ref), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&state.psf_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&state.psf_im), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&state.dirty_re), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(&state.dirty_im), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(&params), 0, 7);
            encoder.setBuffer_offset_atIndex(Some(&state.tap_weights), 0, 8);
        }
        let accumulate_width = cached.metrics.max_halo_cells.max(1);
        let accumulate_height = cached.metrics.group_descs.max(1);
        let accumulate_thread_width = accumulate_pipeline.threadExecutionWidth().max(1);
        let accumulate_max_threads = accumulate_pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let accumulate_group_width = accumulate_thread_width.min(accumulate_width).max(1);
        let accumulate_group_height = (accumulate_max_threads / accumulate_group_width)
            .max(1)
            .min(accumulate_height)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: accumulate_width,
                height: accumulate_height,
                depth: 1,
            },
            MTLSize {
                width: accumulate_group_width,
                height: accumulate_group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        Ok(MetalInitialDirtyGroupedPendingDispatch {
            command_buffer,
            _run: run,
            _lane: lane,
            _data: data,
            _flag: flag,
            _weight: weight,
            _group_desc: group_desc,
            _lane_ref: lane_ref,
            _grouped_lane: grouped_lane,
            _run_accum: run_accum,
            _params: params,
            metrics: cached.metrics,
        })
    }

    fn finish_grouped_initial_dirty_state(
        &self,
        mut state: MetalInitialDirtyGroupedState,
        cache: &mut StandardMfsMetalGroupedInputCache,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        self.finish_grouped_initial_dirty_chunk(&mut state)?;
        let (accumulation, metrics) =
            self.finish_grouped_initial_dirty_pending(&mut state, psf_grid, residual_grid)?;
        cache.replace(
            state.fill.key,
            state.fill.chunks,
            state.fill.accumulation,
            Some(accumulation),
        );
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_row_run_grouped_initial_dirty chunks={} runs={} logical_lanes={} group_descs={} input_cache_hit=false dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} input_cache_host_bytes={}",
                metrics.chunks,
                metrics.runs,
                metrics.logical_lanes,
                metrics.group_descs,
                profile::millis(metrics.wait),
                profile::millis(metrics.gpu),
                profile::millis(metrics.kernel),
                cache.host_bytes,
            );
            eprintln!(
                "standard_mfs_metal_row_run_grouped_initial_dirty_detail append_grouped_row_run_ms={:.3} dirty_accumulation_ms={:.3} chunk_finalize_dispatch_ms={:.3}",
                profile::millis(state.append_grouped_row_run),
                profile::millis(state.dirty_accumulation),
                profile::millis(state.chunk_finalize_dispatch),
            );
            eprintln!(
                "standard_mfs_metal_row_run_grouped_initial_dirty_append_detail setup_ms={:.3} lane_push_ms={:.3} data_flag_copy_ms={:.3} run_desc_ms={:.3} group_assign_ms={:.3} group_finalize_ms={:.3}",
                profile::millis(state.append_detail.setup),
                profile::millis(state.append_detail.lane_push),
                profile::millis(state.append_detail.data_flag_copy),
                profile::millis(state.append_detail.run_desc),
                profile::millis(state.append_detail.group_assign),
                profile::millis(state.append_detail.group_finalize),
            );
        }
        Ok(accumulation)
    }

    fn grid_initial_dirty_from_grouped_input_cache(
        &self,
        gridder: &StandardGridder,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        cache: &mut StandardMfsMetalGroupedInputCache,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<Option<StandardMfsDirtyAccumulation>, ImagingError> {
        let mut state = self.begin_grouped_initial_dirty_state(gridder, weighting_plan)?;
        if !cache.matches(state.fill.key) {
            return Ok(None);
        }
        if cache.chunks.iter().any(|chunk| chunk.host.is_none()) {
            return Ok(None);
        }
        let dispatch_started = Instant::now();
        for cached in &cache.chunks {
            let pending = self.dispatch_initial_dirty_grouped_chunk_async(cached, &state)?;
            state.pending.push(pending);
        }
        state.chunk_finalize_dispatch += dispatch_started.elapsed();
        let (accumulation, metrics) =
            self.finish_grouped_initial_dirty_pending(&mut state, psf_grid, residual_grid)?;
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_row_run_grouped_initial_dirty chunks={} runs={} logical_lanes={} group_descs={} input_cache_hit=true dispatch_wait_ms={:.3} dispatch_gpu_ms={:.3} dispatch_kernel_ms={:.3} input_cache_host_bytes={}",
                metrics.chunks,
                metrics.runs,
                metrics.logical_lanes,
                metrics.group_descs,
                profile::millis(metrics.wait),
                profile::millis(metrics.gpu),
                profile::millis(metrics.kernel),
                cache.host_bytes,
            );
            eprintln!(
                "standard_mfs_metal_row_run_grouped_initial_dirty_detail append_grouped_row_run_ms={:.3} dirty_accumulation_ms={:.3} chunk_finalize_dispatch_ms={:.3}",
                profile::millis(state.append_grouped_row_run),
                profile::millis(state.dirty_accumulation),
                profile::millis(state.chunk_finalize_dispatch),
            );
        }
        Ok(Some(accumulation))
    }

    fn finish_grouped_initial_dirty_pending(
        &self,
        state: &mut MetalInitialDirtyGroupedState,
        psf_grid: &mut Array2<Complex64>,
        residual_grid: &mut Array2<Complex64>,
    ) -> Result<
        (
            StandardMfsDirtyAccumulation,
            MetalInitialDirtyGroupedFinishMetrics,
        ),
        ImagingError,
    > {
        use std::slice;

        use objc2_metal::{MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus};

        let wait_started = Instant::now();
        let mut metrics = MetalInitialDirtyGroupedFinishMetrics::default();
        let mut dirty_accumulation = StandardMfsDirtyAccumulation::default();
        for pending in state.pending.drain(..) {
            pending.command_buffer.waitUntilCompleted();
            if pending.command_buffer.status() == MTLCommandBufferStatus::Error {
                let message = pending
                    .command_buffer
                    .error()
                    .map(|error| format!("{error:?}"))
                    .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
                return Err(ImagingError::Unsupported(format!(
                    "standard MFS Metal grouped initial dirty command failed: {message}"
                )));
            }
            let gpu_start = pending.command_buffer.GPUStartTime();
            let gpu_end = pending.command_buffer.GPUEndTime();
            if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
                metrics.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
            }
            let kernel_start = pending.command_buffer.kernelStartTime();
            let kernel_end = pending.command_buffer.kernelEndTime();
            if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
                metrics.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
            }
            metrics.chunks = metrics.chunks.saturating_add(1);
            metrics.runs = metrics.runs.saturating_add(pending.metrics.runs);
            metrics.logical_lanes = metrics
                .logical_lanes
                .saturating_add(pending.metrics.logical_lanes);
            metrics.group_descs = metrics
                .group_descs
                .saturating_add(pending.metrics.group_descs);
            let run_accum = unsafe {
                slice::from_raw_parts(
                    pending
                        ._run_accum
                        .contents()
                        .as_ptr()
                        .cast::<MetalInitialDirtyRunAccum>(),
                    pending.metrics.runs,
                )
            };
            for record in run_accum {
                let sumwt = f64::from(record.sumwt);
                dirty_accumulation.normalization_sumwt += sumwt;
                dirty_accumulation.reported_sumwt += sumwt;
                dirty_accumulation.gridded_samples = dirty_accumulation
                    .gridded_samples
                    .saturating_add(record.gridded as usize);
                dirty_accumulation.skipped_samples = dirty_accumulation
                    .skipped_samples
                    .saturating_add(record.skipped as usize);
                dirty_accumulation.max_abs_w_lambda = dirty_accumulation
                    .max_abs_w_lambda
                    .max(f64::from(record.max_abs_w_lambda));
            }
        }
        metrics.wait = wait_started.elapsed();
        let [grid_width, grid_height] = [state.fill.grid_width, state.fill.grid_height];
        let cell_count = grid_width.saturating_mul(grid_height);
        let psf_re = unsafe {
            slice::from_raw_parts(state.psf_re.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let psf_im = unsafe {
            slice::from_raw_parts(state.psf_im.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let dirty_re = unsafe {
            slice::from_raw_parts(state.dirty_re.contents().as_ptr().cast::<u32>(), cell_count)
        };
        let dirty_im = unsafe {
            slice::from_raw_parts(state.dirty_im.contents().as_ptr().cast::<u32>(), cell_count)
        };
        for (((psf_cell, &re_bits), &im_bits), ((dirty_cell, &dirty_re_bits), &dirty_im_bits)) in
            psf_grid
                .as_slice_memory_order_mut()
                .expect("standard MFS Metal grouped initial PSF grid should be contiguous")
                .iter_mut()
                .zip(psf_re)
                .zip(psf_im)
                .zip(
                    residual_grid
                        .as_slice_memory_order_mut()
                        .expect(
                            "standard MFS Metal grouped initial dirty grid should be contiguous",
                        )
                        .iter_mut()
                        .zip(dirty_re)
                        .zip(dirty_im),
                )
        {
            *psf_cell = Complex64::new(
                f64::from(f32::from_bits(re_bits)),
                f64::from(f32::from_bits(im_bits)),
            );
            *dirty_cell = Complex64::new(
                f64::from(f32::from_bits(dirty_re_bits)),
                f64::from(f32::from_bits(dirty_im_bits)),
            );
        }
        Ok((dirty_accumulation, metrics))
    }

    fn begin_grouped_input_cache_fill(
        &self,
        gridder: &StandardGridder,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsMetalGroupedInputCacheFill, ImagingError> {
        self.begin_grouped_input_cache_fill_with_dirty_accumulation(gridder, weighting_plan, false)
    }

    fn begin_grouped_input_cache_fill_for_initial_dirty(
        &self,
        gridder: &StandardGridder,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsMetalGroupedInputCacheFill, ImagingError> {
        self.begin_grouped_input_cache_fill_with_dirty_accumulation(gridder, weighting_plan, true)
    }

    fn begin_grouped_input_cache_fill_with_dirty_accumulation(
        &self,
        gridder: &StandardGridder,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        collect_dirty_accumulation: bool,
    ) -> Result<StandardMfsMetalGroupedInputCacheFill, ImagingError> {
        let [grid_width, grid_height] = gridder.grid_shape();
        let reweight_plan = weighting_plan.reweight_plan()?;
        let (weighting_mode, density_width, density_height, density_convention, briggs_f2) =
            match reweight_plan {
                StandardMfsStreamingReweightPlan::Natural => (0_u32, 1usize, 1usize, 0_u32, 0.0),
                StandardMfsStreamingReweightPlan::Uniform {
                    density,
                    convention,
                } => {
                    let shape = density.shape();
                    (
                        1_u32,
                        shape[0],
                        shape[1],
                        metal_density_convention_code(convention),
                        0.0,
                    )
                }
                StandardMfsStreamingReweightPlan::Briggs {
                    density,
                    convention,
                    f2,
                    use_bandwidth_taper,
                    fractional_bandwidth,
                } => {
                    if use_bandwidth_taper {
                        return Err(ImagingError::Unsupported(format!(
                            "standard MFS residual backend 'metal-row-run-grouped' does not yet support BriggsBwTaper weighting (fractional_bandwidth={fractional_bandwidth})"
                        )));
                    }
                    let shape = density.shape();
                    (
                        2_u32,
                        shape[0],
                        shape[1],
                        metal_density_convention_code(convention),
                        f2,
                    )
                }
            };
        let group_tile_edge = standard_mfs_metal_group_tile_edge();
        let partition =
            MetalResidualGroupedTilePartition::new(grid_width, grid_height, group_tile_edge)?;
        let [du_lambda, dv_lambda] = gridder.grid_spacing_lambda();
        let density_params = gridder.density_grid_coordinate_params();
        let chunk_lane_capacity = standard_mfs_metal_residual_chunk_samples();
        let oversampling = gridder.oversampling();
        let tap_weight_count = gridder.normalized_tap_weights().len();
        let key = MetalResidualGroupedInputCacheKey {
            lane_layout_version: METAL_RESIDUAL_ROW_RUN_LANE_LAYOUT_VERSION,
            grid_width,
            grid_height,
            oversampling,
            tap_weight_count,
            weighting_mode,
            density_convention,
            density_width,
            density_height,
            briggs_f2_bits: briggs_f2.to_bits(),
            group_tile_edge,
            group_tile_count: partition.tile_count(),
            chunk_lane_capacity,
            du_lambda_bits: (du_lambda as f32).to_bits(),
            dv_lambda_bits: (dv_lambda as f32).to_bits(),
            density_center_x_bits: (density_params.center_x as f32).to_bits(),
            density_center_y_bits: (density_params.center_y as f32).to_bits(),
            density_u_scale_bits: (density_params.u_scale as f32).to_bits(),
            density_v_scale_bits: (density_params.v_scale as f32).to_bits(),
        };
        Ok(StandardMfsMetalGroupedInputCacheFill {
            key,
            chunk: MetalResidualGroupedRowRunChunk::new(partition.tile_count()),
            partition,
            chunk_lane_capacity,
            grid_width,
            grid_height,
            oversampling,
            tap_weight_count,
            weighting_mode,
            density_convention,
            density_width,
            density_height,
            briggs_f2,
            du_lambda: du_lambda as f32,
            dv_lambda: dv_lambda as f32,
            density_center_x: density_params.center_x as f32,
            density_center_y: density_params.center_y as f32,
            density_u_scale: density_params.u_scale as f32,
            density_v_scale: density_params.v_scale as f32,
            chunks: Vec::new(),
            accumulation: StandardMfsTiledResidualAccumulation::default(),
            dirty_accumulation: StandardMfsDirtyAccumulation::default(),
            collect_dirty_accumulation,
            append_detail: MetalGroupedAppendDetail::default(),
        })
    }

    fn append_grouped_input_cache_run(
        &self,
        routed_run: &StandardMfsRoutedVisibilityRun,
        fill: &mut StandardMfsMetalGroupedInputCacheFill,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<(), ImagingError> {
        if !fill.chunk.is_empty()
            && fill
                .chunk
                .row_runs
                .logical_lanes
                .saturating_add(routed_run.len())
                > fill.chunk_lane_capacity
        {
            self.finish_grouped_input_cache_chunk(fill)?;
        }
        let parts = MetalRowRunParts {
            row: routed_run.row.as_ref(),
            source_slot_range: routed_run.source_slot_range.clone(),
            tap_centers: routed_run.tap_centers.as_ref(),
            grid_width: fill.grid_width,
            grid_height: fill.grid_height,
            du_lambda: fill.du_lambda as f64,
            dv_lambda: fill.dv_lambda as f64,
        };
        self.append_metal_residual_grouped_row_run_profiled(
            parts,
            &fill.partition,
            &mut fill.accumulation,
            &mut fill.chunk,
            &mut fill.append_detail,
        )?;
        if fill.collect_dirty_accumulation {
            let accumulation =
                Self::dirty_accumulation_from_routed_run(routed_run, weighting_plan)?;
            fill.dirty_accumulation.add(accumulation);
        }
        if fill.chunk.row_runs.logical_lanes >= fill.chunk_lane_capacity {
            self.finish_grouped_input_cache_chunk(fill)?;
        }
        Ok(())
    }

    fn dirty_accumulation_from_routed_run(
        routed_run: &StandardMfsRoutedVisibilityRun,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
    ) -> Result<StandardMfsDirtyAccumulation, ImagingError> {
        if routed_run.is_empty() {
            return Ok(StandardMfsDirtyAccumulation::default());
        }
        let row = routed_run.row.as_ref();
        let mut accumulation = StandardMfsDirtyAccumulation::default();
        if !row.gridable {
            accumulation.skipped_samples = accumulation
                .skipped_samples
                .saturating_add(routed_run.len());
            return Ok(accumulation);
        }
        if row.weight_spectrum.is_some() {
            return Err(ImagingError::Unsupported(
                "standard MFS initial dirty backend 'metal-row-run-grouped' does not yet support WEIGHT_SPECTRUM"
                    .to_string(),
            ));
        }
        let corr_count = row.data.shape().first().copied().unwrap_or(0);
        let local_channel_count = row.data.shape().get(1).copied().unwrap_or(0);
        if row.flag.shape() != [corr_count, local_channel_count] {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped initial dirty FLAG shape differs from DATA shape"
                    .to_string(),
            ));
        }
        if row.weight.len() < corr_count {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal grouped initial dirty WEIGHT has {} correlations but DATA has {corr_count}",
                row.weight.len()
            )));
        }
        let sumwt_factor = match row.polarization {
            StandardMfsVisibilityPolarization::Explicit { sumwt_factor, .. }
            | StandardMfsVisibilityPolarization::CollapsedPair { sumwt_factor, .. } => sumwt_factor,
        };
        if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
            accumulation.skipped_samples = accumulation
                .skipped_samples
                .saturating_add(routed_run.len());
            return Ok(accumulation);
        }
        for source_slot in routed_run.source_slot_range.clone() {
            let source_channel = *row.source_channel_indices.get(source_slot).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS grouped initial dirty source slot {source_slot} is out of bounds"
                ))
            })?;
            let local_channel = source_channel.checked_sub(row.channel_origin).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS grouped initial dirty source channel {source_channel} precedes loaded origin {}",
                    row.channel_origin
                ))
            })?;
            if local_channel >= local_channel_count {
                return Err(ImagingError::InvalidRequest(format!(
                    "standard MFS grouped initial dirty local channel {local_channel} exceeds row channel count {local_channel_count}"
                )));
            }
            let lambda_scale = *row.channel_lambda_scales.get(source_slot).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS grouped initial dirty lambda slot {source_slot} is out of bounds"
                ))
            })?;
            let u_lambda = row.uvw_m[0] * lambda_scale;
            let v_lambda = row.uvw_m[1] * lambda_scale;
            let w_lambda = row.uvw_m[2] * lambda_scale;
            accumulation.max_abs_w_lambda = accumulation.max_abs_w_lambda.max(w_lambda.abs());
            let natural_weight = match row.polarization {
                StandardMfsVisibilityPolarization::Explicit { corr_index, .. } => {
                    if *row.flag.get((corr_index, local_channel)).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS grouped initial dirty FLAG index [{corr_index}, {source_channel}] is out of bounds"
                        ))
                    })? {
                        accumulation.skipped_samples += 1;
                        continue;
                    }
                    *row.weight.get(corr_index).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS grouped initial dirty WEIGHT correlation {corr_index} is out of bounds"
                        ))
                    })?
                }
                StandardMfsVisibilityPolarization::CollapsedPair {
                    first_corr_index,
                    second_corr_index,
                    transform,
                    ..
                } => {
                    let first_flagged =
                        *row.flag
                            .get((first_corr_index, local_channel))
                            .ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS grouped initial dirty FLAG index [{first_corr_index}, {source_channel}] is out of bounds"
                                ))
                            })?;
                    let second_flagged =
                        *row.flag
                            .get((second_corr_index, local_channel))
                            .ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS grouped initial dirty FLAG index [{second_corr_index}, {source_channel}] is out of bounds"
                                ))
                            })?;
                    if first_flagged || second_flagged {
                        accumulation.skipped_samples += 1;
                        continue;
                    }
                    let first_visibility =
                        *row.data
                            .get((first_corr_index, local_channel))
                            .ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS grouped initial dirty DATA index [{first_corr_index}, {source_channel}] is out of bounds"
                                ))
                            })?;
                    let second_visibility =
                        *row.data
                            .get((second_corr_index, local_channel))
                            .ok_or_else(|| {
                                ImagingError::InvalidRequest(format!(
                                    "standard MFS grouped initial dirty DATA index [{second_corr_index}, {source_channel}] is out of bounds"
                                ))
                            })?;
                    let visibility = collapse_standard_mfs_pair_visibility(
                        first_visibility,
                        second_visibility,
                        transform,
                    );
                    if !(visibility.re.is_finite() && visibility.im.is_finite()) {
                        accumulation.skipped_samples += 1;
                        continue;
                    }
                    let first_weight = *row.weight.get(first_corr_index).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS grouped initial dirty WEIGHT correlation {first_corr_index} is out of bounds"
                        ))
                    })?;
                    let second_weight = *row.weight.get(second_corr_index).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS grouped initial dirty WEIGHT correlation {second_corr_index} is out of bounds"
                        ))
                    })?;
                    if !(first_weight.is_finite()
                        && first_weight > 0.0
                        && second_weight.is_finite()
                        && second_weight > 0.0)
                    {
                        accumulation.skipped_samples += 1;
                        continue;
                    }
                    0.5 * (first_weight + second_weight)
                }
            };
            if !(natural_weight.is_finite() && natural_weight > 0.0) {
                accumulation.skipped_samples += 1;
                continue;
            }
            let final_weight = weighting_plan.weight_sample(u_lambda, v_lambda, natural_weight)?;
            let grid_weight = final_weight * sumwt_factor;
            if !(grid_weight.is_finite() && grid_weight > 0.0) {
                accumulation.skipped_samples += 1;
                continue;
            }
            let grid_weight = f64::from(grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
        }
        Ok(accumulation)
    }

    fn finish_grouped_input_cache_fill(
        &self,
        mut fill: StandardMfsMetalGroupedInputCacheFill,
        cache: &mut StandardMfsMetalGroupedInputCache,
    ) -> Result<(), ImagingError> {
        self.finish_grouped_input_cache_chunk(&mut fill)?;
        let dirty_accumulation = fill
            .collect_dirty_accumulation
            .then_some(fill.dirty_accumulation);
        cache.replace(fill.key, fill.chunks, fill.accumulation, dirty_accumulation);
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_metal_grouped_input_cache_fill_append_detail setup_ms={:.3} lane_push_ms={:.3} data_flag_copy_ms={:.3} run_desc_ms={:.3} group_assign_ms={:.3} group_finalize_ms={:.3}",
                profile::millis(fill.append_detail.setup),
                profile::millis(fill.append_detail.lane_push),
                profile::millis(fill.append_detail.data_flag_copy),
                profile::millis(fill.append_detail.run_desc),
                profile::millis(fill.append_detail.group_assign),
                profile::millis(fill.append_detail.group_finalize),
            );
        }
        Ok(())
    }

    fn finish_grouped_input_cache_chunk(
        &self,
        fill: &mut StandardMfsMetalGroupedInputCacheFill,
    ) -> Result<(), ImagingError> {
        if fill.chunk.is_empty() {
            return Ok(());
        }
        let finalize_started = Instant::now();
        fill.chunk.finalize_groups(&fill.partition)?;
        fill.append_detail.group_finalize += finalize_started.elapsed();
        let params = MetalResidualRowRunParams {
            run_count: u32::try_from(fill.chunk.row_runs.runs.len()).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run chunk has too many runs".to_string(),
                )
            })?,
            max_lane_count: u32::try_from(fill.chunk.row_runs.max_lane_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run chunk has too many lanes per run"
                        .to_string(),
                )
            })?,
            grid_width: u32::try_from(fill.grid_width).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run grid width exceeds u32".to_string(),
                )
            })?,
            grid_height: u32::try_from(fill.grid_height).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run grid height exceeds u32".to_string(),
                )
            })?,
            oversampling: u32::try_from(fill.oversampling).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run oversampling exceeds u32".to_string(),
                )
            })?,
            tap_weight_count: u32::try_from(fill.tap_weight_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run tap table exceeds u32".to_string(),
                )
            })?,
            weighting_mode: fill.weighting_mode,
            density_convention: fill.density_convention,
            density_width: u32::try_from(fill.density_width).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run density width exceeds u32".to_string(),
                )
            })?,
            density_height: u32::try_from(fill.density_height).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run density height exceeds u32".to_string(),
                )
            })?,
            diagnostic_mode: 0,
            _pad0: 0,
            du_lambda: fill.du_lambda,
            dv_lambda: fill.dv_lambda,
            density_center_x: fill.density_center_x,
            density_center_y: fill.density_center_y,
            density_u_scale: fill.density_u_scale,
            density_v_scale: fill.density_v_scale,
            briggs_f2: fill.briggs_f2,
            _pad1: 0.0,
        };
        fill.chunk.clear_group_scratch_after_finalize();
        let finalized_chunk = std::mem::replace(
            &mut fill.chunk,
            MetalResidualGroupedRowRunChunk::new(fill.partition.tile_count()),
        );
        fill.chunks.push(self.cached_grouped_chunk(
            finalized_chunk,
            params,
            objc2_metal::MTLResourceOptions::StorageModeShared,
        )?);
        Ok(())
    }

    fn append_metal_residual_row_run(
        &self,
        gridder: &StandardGridder,
        routed_run: &StandardMfsRoutedVisibilityRun,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        chunk: &mut MetalResidualRowRunChunk,
    ) -> Result<(), ImagingError> {
        let [du_lambda, dv_lambda] = gridder.grid_spacing_lambda();
        let [grid_width, grid_height] = gridder.grid_shape();
        let parts = MetalRowRunParts {
            row: routed_run.row.as_ref(),
            source_slot_range: routed_run.source_slot_range.clone(),
            tap_centers: routed_run.tap_centers.as_ref(),
            grid_width,
            grid_height,
            du_lambda,
            dv_lambda,
        };
        self.append_metal_residual_row_run_parts(parts, accumulation, chunk, None)
    }

    fn append_metal_residual_row_run_parts(
        &self,
        parts: MetalRowRunParts<'_>,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        chunk: &mut MetalResidualRowRunChunk,
        mut append_detail: Option<&mut MetalGroupedAppendDetail>,
    ) -> Result<(), ImagingError> {
        let setup_started = Instant::now();
        let row = parts.row;
        let source_slot_range = parts.source_slot_range;
        let tap_centers = parts.tap_centers;
        let lane_count = source_slot_range
            .end
            .saturating_sub(source_slot_range.start);
        if lane_count == 0 {
            return Ok(());
        }
        if tap_centers.len() != lane_count {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal row-run has {} tap centers for {lane_count} lanes",
                tap_centers.len()
            )));
        }
        if row.weight_spectrum.is_some() {
            return Err(ImagingError::Unsupported(
                "standard MFS residual backend 'metal-row-run' does not yet support WEIGHT_SPECTRUM"
                    .to_string(),
            ));
        }
        if !row.gridable {
            accumulation.skipped_not_gridable =
                accumulation.skipped_not_gridable.saturating_add(lane_count);
            return Ok(());
        }
        let corr_count = row.data.shape().first().copied().unwrap_or(0);
        let local_channel_count = row.data.shape().get(1).copied().unwrap_or(0);
        if row.flag.shape() != [corr_count, local_channel_count] {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal row-run FLAG shape differs from DATA shape".to_string(),
            ));
        }
        if row.weight.len() < corr_count {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal row-run WEIGHT has {} correlations but DATA has {corr_count}",
                row.weight.len()
            )));
        }
        let source_slot_start = source_slot_range.start;
        let source_slot_end = source_slot_range.end;
        chunk.lanes.reserve(lane_count);
        chunk.data.reserve(lane_count.saturating_mul(corr_count));
        chunk.flags.reserve(lane_count.saturating_mul(corr_count));
        chunk.weights.reserve(corr_count);
        let contiguous_local_channels = if lane_count == 0 {
            Some(0..0)
        } else {
            let first_source_channel =
                *row.source_channel_indices
                    .get(source_slot_start)
                    .ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS Metal row-run source slot {source_slot_start} is out of bounds"
                        ))
                    })?;
            let first_local_channel =
                first_source_channel
                    .checked_sub(row.channel_origin)
                    .ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS Metal row-run source channel {first_source_channel} precedes loaded origin {}",
                            row.channel_origin
                        ))
                    })?;
            let contiguous_end = first_source_channel.saturating_add(lane_count - 1);
            let source_slots_are_contiguous = row
                .source_channel_indices
                .get(source_slot_end - 1)
                .copied()
                .is_some_and(|last_source_channel| last_source_channel == contiguous_end)
                || (source_slot_start..source_slot_end).all(|source_slot| {
                    row.source_channel_indices
                        .get(source_slot)
                        .copied()
                        .is_some_and(|source_channel| {
                            source_channel
                                == row
                                    .channel_origin
                                    .saturating_add(first_local_channel)
                                    .saturating_add(source_slot - source_slot_start)
                        })
                });
            if first_local_channel.saturating_add(lane_count) <= local_channel_count
                && source_slots_are_contiguous
            {
                Some(first_local_channel..first_local_channel + lane_count)
            } else {
                None
            }
        };
        let (polarization_mode, transform, corr0, corr1, sumwt_factor) = match row.polarization {
            StandardMfsVisibilityPolarization::Explicit {
                corr_index,
                sumwt_factor,
            } => (0_u32, 0_u32, corr_index, corr_index, sumwt_factor),
            StandardMfsVisibilityPolarization::CollapsedPair {
                first_corr_index,
                second_corr_index,
                transform,
                sumwt_factor,
            } => (
                1_u32,
                metal_pair_transform_code(transform),
                first_corr_index,
                second_corr_index,
                sumwt_factor,
            ),
        };
        if corr0 >= corr_count || corr1 >= corr_count {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal row-run polarization correlations [{corr0}, {corr1}] exceed DATA correlation count {corr_count}"
            )));
        }
        let lane_offset = chunk.lanes.len();
        let data_offset = chunk.data.len();
        let flag_offset = chunk.flags.len();
        let weight_offset = chunk.weights.len();
        if let Some(detail) = append_detail.as_deref_mut() {
            detail.setup += setup_started.elapsed();
        }
        let lane_push_started = Instant::now();
        for (lane_index, source_slot) in source_slot_range.clone().enumerate() {
            if contiguous_local_channels.is_none() {
                let source_channel =
                    *row.source_channel_indices.get(source_slot).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS Metal row-run source slot {source_slot} is out of bounds"
                        ))
                    })?;
                let local_channel = source_channel.checked_sub(row.channel_origin).ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "standard MFS Metal row-run source channel {source_channel} precedes loaded origin {}",
                        row.channel_origin
                    ))
                })?;
                if local_channel >= local_channel_count {
                    return Err(ImagingError::InvalidRequest(format!(
                        "standard MFS Metal row-run local channel {local_channel} exceeds row channel count {local_channel_count}"
                    )));
                }
            }
            let lambda_scale = *row.channel_lambda_scales.get(source_slot).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS Metal row-run lambda slot {source_slot} is out of bounds"
                ))
            })?;
            let center = tap_centers.get(lane_index).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS Metal row-run tap center {lane_index} is out of bounds"
                ))
            })?;
            let grid_x = (row.uvw_m[0] * lambda_scale / parts.du_lambda
                + parts.grid_width as f64 * 0.5) as f32;
            let grid_y = (-row.uvw_m[1] * lambda_scale / parts.dv_lambda
                + parts.grid_height as f64 * 0.5) as f32;
            if !(grid_x.is_finite() && grid_y.is_finite()) {
                return Err(ImagingError::InvalidRequest(
                    "standard MFS Metal row-run lane no longer maps to positive grid coordinates"
                        .to_string(),
                ));
            }
            chunk.lanes.push(MetalResidualRowRunLane {
                lambda_scale: lambda_scale as f32,
                center_x: center[0],
                center_y: center[1],
                _pad0: 0,
                grid_x,
                grid_y,
                _pad1: [0.0; 2],
            });
        }
        if let Some(detail) = append_detail.as_deref_mut() {
            detail.lane_push += lane_push_started.elapsed();
        }
        let data_flag_copy_started = Instant::now();
        if let (Some(local_channels), Some(data), Some(flags)) = (
            contiguous_local_channels
                .filter(|_| row.data.is_standard_layout() && row.flag.is_standard_layout()),
            row.data.as_slice(),
            row.flag.as_slice(),
        ) {
            // The Metal row-run ABI below is explicitly corr-major. A contiguous
            // ndarray slice is not enough here; non-standard layout would
            // scramble lanes under the row-major indexing below.
            for corr in 0..corr_count {
                let row_start = corr.checked_mul(local_channel_count).ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal row-run DATA row offset is too large".to_string(),
                    )
                })?;
                let start = row_start + local_channels.start;
                let end = row_start + local_channels.end;
                for visibility in &data[start..end] {
                    chunk.data.push(MetalComplex32 {
                        re: visibility.re,
                        im: visibility.im,
                    });
                }
                chunk
                    .flags
                    .extend(flags[start..end].iter().copied().map(u8::from));
            }
        } else {
            for corr in 0..corr_count {
                for source_slot in source_slot_range.clone() {
                    let source_channel = row.source_channel_indices[source_slot];
                    let local_channel = source_channel - row.channel_origin;
                    let visibility = *row.data.get((corr, local_channel)).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS Metal row-run DATA index [{corr}, {source_channel}] is out of bounds"
                        ))
                    })?;
                    let flag = *row.flag.get((corr, local_channel)).ok_or_else(|| {
                        ImagingError::InvalidRequest(format!(
                            "standard MFS Metal row-run FLAG index [{corr}, {source_channel}] is out of bounds"
                        ))
                    })?;
                    chunk.data.push(MetalComplex32 {
                        re: visibility.re,
                        im: visibility.im,
                    });
                    chunk.flags.push(u8::from(flag));
                }
            }
        }
        if let Some(detail) = append_detail.as_deref_mut() {
            detail.data_flag_copy += data_flag_copy_started.elapsed();
        }
        let run_desc_started = Instant::now();
        chunk
            .weights
            .extend(row.weight.iter().take(corr_count).copied());
        chunk.runs.push(MetalResidualRowRunDesc {
            u_m: row.uvw_m[0] as f32,
            v_m: row.uvw_m[1] as f32,
            sumwt_factor,
            w_m: row.uvw_m[2] as f32,
            lane_offset: u32::try_from(lane_offset).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run lane offset exceeds u32".to_string(),
                )
            })?,
            lane_count: u32::try_from(lane_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run lane count exceeds u32".to_string(),
                )
            })?,
            data_offset: u32::try_from(data_offset).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run data offset exceeds u32".to_string(),
                )
            })?,
            flag_offset: u32::try_from(flag_offset).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run flag offset exceeds u32".to_string(),
                )
            })?,
            weight_offset: u32::try_from(weight_offset).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run weight offset exceeds u32".to_string(),
                )
            })?,
            corr_count: u32::try_from(corr_count).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run correlation count exceeds u32".to_string(),
                )
            })?,
            polarization_mode,
            transform,
            corr0: u32::try_from(corr0).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run correlation index exceeds u32".to_string(),
                )
            })?,
            corr1: u32::try_from(corr1).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal row-run correlation index exceeds u32".to_string(),
                )
            })?,
            _pad1: [0; 2],
        });
        chunk.logical_lanes = chunk.logical_lanes.saturating_add(lane_count);
        chunk.max_lane_count = chunk.max_lane_count.max(lane_count);
        accumulation.valid_samples = accumulation.valid_samples.saturating_add(lane_count);
        accumulation.planned_samples = accumulation.planned_samples.saturating_add(lane_count);
        accumulation.gridded_residual_samples = accumulation
            .gridded_residual_samples
            .saturating_add(lane_count);
        if let Some(detail) = append_detail {
            detail.run_desc += run_desc_started.elapsed();
        }
        Ok(())
    }

    fn append_metal_residual_grouped_row_run_profiled(
        &self,
        parts: MetalRowRunParts<'_>,
        partition: &MetalResidualGroupedTilePartition,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        chunk: &mut MetalResidualGroupedRowRunChunk,
        append_detail: &mut MetalGroupedAppendDetail,
    ) -> Result<(), ImagingError> {
        self.append_metal_residual_grouped_row_run_parts(
            parts,
            partition,
            accumulation,
            chunk,
            Some(append_detail),
        )
    }

    fn append_metal_residual_grouped_row_run_parts(
        &self,
        parts: MetalRowRunParts<'_>,
        partition: &MetalResidualGroupedTilePartition,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        chunk: &mut MetalResidualGroupedRowRunChunk,
        mut append_detail: Option<&mut MetalGroupedAppendDetail>,
    ) -> Result<(), ImagingError> {
        let before_lanes = chunk.row_runs.lanes.len();
        let row_detail = append_detail.as_deref_mut();
        self.append_metal_residual_row_run_parts(
            parts.clone(),
            accumulation,
            &mut chunk.row_runs,
            row_detail,
        )?;
        let after_lanes = chunk.row_runs.lanes.len();
        if after_lanes == before_lanes {
            return Ok(());
        }
        let appended_lanes = after_lanes - before_lanes;
        chunk.lane_group_ids.reserve(appended_lanes);
        let lane_count = parts
            .source_slot_range
            .end
            .saturating_sub(parts.source_slot_range.start);
        if appended_lanes != lane_count {
            return Err(ImagingError::InvalidRequest(format!(
                "standard MFS Metal grouped row-run appended {appended_lanes} lanes for a {}-lane run",
                lane_count
            )));
        }
        let group_assign_started = Instant::now();
        for lane_index in 0..appended_lanes {
            let center = parts.tap_centers.get(lane_index).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS Metal grouped row-run tap center {lane_index} is out of bounds"
                ))
            })?;
            let Some(group_index) = partition.owner(center[0], center[1]) else {
                return Err(ImagingError::InvalidRequest(format!(
                    "standard MFS Metal grouped row-run tap center [{}, {}] has no owner",
                    center[0], center[1]
                )));
            };
            let group_id = u32::try_from(group_index).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run group index exceeds u32".to_string(),
                )
            })?;
            chunk.lane_group_ids.push(group_id);
            let count = chunk.group_counts.get_mut(group_index).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS Metal grouped row-run group index {group_index} is out of range"
                ))
            })?;
            *count = count.saturating_add(1);
        }
        if let Some(detail) = append_detail {
            detail.group_assign += group_assign_started.elapsed();
        }
        Ok(())
    }

    fn dispatch_residual_row_run_chunk(
        &self,
        chunk: &MetalResidualRowRunChunk,
        params: MetalResidualRowRunParams,
        diagnostic_mode: MetalResidualRowRunDiagnosticMode,
        shared: &MetalResidualRowRunSharedBuffers<'_>,
    ) -> Result<MetalResidualDispatchTiming, ImagingError> {
        use std::slice;

        use objc2_metal::{
            MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
            MTLComputeCommandEncoder, MTLComputePipelineState, MTLDevice, MTLResourceOptions,
            MTLSize,
        };

        let mut timing = MetalResidualDispatchTiming::default();
        if chunk.is_empty() {
            return Ok(timing);
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let input_buffers_started = Instant::now();
        let run_buffer = self.buffer_from_slice(&chunk.runs, storage_options)?;
        let lane_buffer = self.buffer_from_slice(&chunk.lanes, storage_options)?;
        let data_buffer = self.buffer_from_slice(&chunk.data, storage_options)?;
        let flag_buffer = self.buffer_from_slice(&chunk.flags, storage_options)?;
        let weight_buffer = self.buffer_from_slice(&chunk.weights, storage_options)?;
        let diagnostic_output_buffer = if diagnostic_mode.uses_diagnostic_pipeline() {
            let output_bytes = chunk
                .lanes
                .len()
                .max(1)
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal row-run diagnostic output is too large".to_string(),
                    )
                })?;
            Some(
                self.device
                    .newBufferWithLength_options(output_bytes, storage_options)
                    .ok_or_else(|| {
                        ImagingError::Unsupported(
                            "standard MFS backend 'metal-row-run' could not allocate diagnostic output buffer"
                                .to_string(),
                        )
                    })?,
            )
        } else {
            None
        };
        timing.sample_buffer += input_buffers_started.elapsed();
        let params_buffer_started = Instant::now();
        let params_buffer = self.buffer_from_slice(slice::from_ref(&params), storage_options)?;
        timing.params_buffer += params_buffer_started.elapsed();

        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run' could not create a residual command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run' could not create a residual compute encoder"
                    .to_string(),
            )
        })?;
        let pipeline = if diagnostic_mode.uses_diagnostic_pipeline() {
            &self.residual_row_run_diagnostic_pipeline
        } else {
            &self.residual_row_run_pipeline
        };
        encoder.setComputePipelineState(pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&run_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&lane_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&data_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&flag_buffer), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&weight_buffer), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(shared.density), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(shared.model_re), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(shared.model_im), 0, 7);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_re), 0, 8);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_im), 0, 9);
            encoder.setBuffer_offset_atIndex(Some(&params_buffer), 0, 10);
            encoder.setBuffer_offset_atIndex(Some(shared.tap_weights), 0, 11);
            if let Some(buffer) = diagnostic_output_buffer.as_ref() {
                encoder.setBuffer_offset_atIndex(Some(buffer), 0, 12);
            }
        }
        let thread_width = pipeline.threadExecutionWidth().max(1);
        let max_threads = pipeline.maxTotalThreadsPerThreadgroup().max(1);
        let group_width = thread_width.min(params.max_lane_count as usize).max(1);
        let group_height = (max_threads / group_width)
            .max(1)
            .min(params.run_count as usize)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: params.max_lane_count as usize,
                height: params.run_count as usize,
                depth: 1,
            },
            MTLSize {
                width: group_width,
                height: group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal-row-run' residual refresh command failed: {message}"
            )));
        }
        Ok(timing)
    }

    fn dispatch_residual_row_run_grouped_chunk(
        &self,
        chunk: &MetalResidualGroupedRowRunChunk,
        params: MetalResidualRowRunParams,
        shared: &MetalResidualRowRunSharedBuffers<'_>,
        input_copy_mode: MetalInputBufferCopyMode,
    ) -> Result<MetalResidualDispatchTiming, ImagingError> {
        use std::slice;

        use objc2_metal::{
            MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
            MTLComputeCommandEncoder, MTLComputePipelineState, MTLDevice, MTLResourceOptions,
            MTLSize,
        };

        let mut timing = MetalResidualDispatchTiming::default();
        if chunk.is_empty() || chunk.group_descs.is_empty() {
            return Ok(timing);
        }
        let storage_options = MTLResourceOptions::StorageModeShared;
        let input_buffers_started = Instant::now();
        let run_buffer = self.buffer_from_slice_with_mode(
            &chunk.row_runs.runs,
            storage_options,
            input_copy_mode,
        )?;
        let lane_buffer = self.buffer_from_slice_with_mode(
            &chunk.row_runs.lanes,
            storage_options,
            input_copy_mode,
        )?;
        let data_buffer = self.buffer_from_slice_with_mode(
            &chunk.row_runs.data,
            storage_options,
            input_copy_mode,
        )?;
        let flag_buffer = self.buffer_from_slice_with_mode(
            &chunk.row_runs.flags,
            storage_options,
            input_copy_mode,
        )?;
        let weight_buffer = self.buffer_from_slice_with_mode(
            &chunk.row_runs.weights,
            storage_options,
            input_copy_mode,
        )?;
        let group_desc_buffer =
            self.buffer_from_slice_with_mode(&chunk.group_descs, storage_options, input_copy_mode)?;
        let lane_ref_buffer =
            self.buffer_from_slice_with_mode(&chunk.lane_refs, storage_options, input_copy_mode)?;
        let grouped_lane_bytes = chunk
            .row_runs
            .lanes
            .len()
            .checked_mul(std::mem::size_of::<MetalResidualGroupedLane>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run output lane buffer is too large"
                        .to_string(),
                )
            })?;
        let grouped_lane_buffer = self
            .device
            .newBufferWithLength_options(grouped_lane_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run-grouped' could not allocate grouped lane buffer"
                        .to_string(),
                )
            })?;
        timing.sample_buffer += input_buffers_started.elapsed();
        let params_buffer_started = Instant::now();
        let params_buffer = self.buffer_from_slice(slice::from_ref(&params), storage_options)?;
        timing.params_buffer += params_buffer_started.elapsed();

        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run-grouped' could not create a residual command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run-grouped' could not create a residual compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.residual_row_run_grouped_prepare_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&run_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&lane_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&data_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&flag_buffer), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&weight_buffer), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(shared.density), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(shared.model_re), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(shared.model_im), 0, 7);
            encoder.setBuffer_offset_atIndex(Some(&grouped_lane_buffer), 0, 8);
            encoder.setBuffer_offset_atIndex(Some(&params_buffer), 0, 9);
            encoder.setBuffer_offset_atIndex(Some(shared.tap_weights), 0, 10);
        }
        let prepare_thread_width = self
            .residual_row_run_grouped_prepare_pipeline
            .threadExecutionWidth()
            .max(1);
        let prepare_max_threads = self
            .residual_row_run_grouped_prepare_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let prepare_group_width = prepare_thread_width
            .min(params.max_lane_count as usize)
            .max(1);
        let prepare_group_height = (prepare_max_threads / prepare_group_width)
            .max(1)
            .min(params.run_count as usize)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: params.max_lane_count as usize,
                height: params.run_count as usize,
                depth: 1,
            },
            MTLSize {
                width: prepare_group_width,
                height: prepare_group_height,
                depth: 1,
            },
        );

        encoder.setComputePipelineState(&self.residual_row_run_grouped_accumulate_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&grouped_lane_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&group_desc_buffer), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&lane_ref_buffer), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_im), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&params_buffer), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(shared.tap_weights), 0, 6);
        }
        let accumulate_width = chunk.max_halo_cells.max(1);
        let accumulate_height = chunk.group_descs.len().max(1);
        let accumulate_thread_width = self
            .residual_row_run_grouped_accumulate_pipeline
            .threadExecutionWidth()
            .max(1);
        let accumulate_max_threads = self
            .residual_row_run_grouped_accumulate_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let accumulate_group_width = accumulate_thread_width.min(accumulate_width).max(1);
        let accumulate_group_height = (accumulate_max_threads / accumulate_group_width)
            .max(1)
            .min(accumulate_height)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: accumulate_width,
                height: accumulate_height,
                depth: 1,
            },
            MTLSize {
                width: accumulate_group_width,
                height: accumulate_group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal-row-run-grouped' residual refresh command failed: {message}"
            )));
        }
        Ok(timing)
    }

    fn cached_grouped_chunk_buffers(
        &self,
        chunk: &MetalResidualGroupedRowRunChunk,
        params: MetalResidualRowRunParams,
        storage_options: objc2_metal::MTLResourceOptions,
    ) -> Result<MetalResidualGroupedCachedBuffers, ImagingError> {
        use objc2_metal::MTLDevice;

        let grouped_lane_bytes = chunk
            .row_runs
            .lanes
            .len()
            .checked_mul(std::mem::size_of::<MetalResidualGroupedLane>())
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal grouped row-run output lane buffer is too large"
                        .to_string(),
                )
            })?;
        let grouped_lane = self
            .device
            .newBufferWithLength_options(grouped_lane_bytes, storage_options)
            .ok_or_else(|| {
                ImagingError::Unsupported(
                    "standard MFS backend 'metal-row-run-grouped' could not allocate grouped lane buffer"
                        .to_string(),
                )
            })?;
        Ok(MetalResidualGroupedCachedBuffers {
            run: self.buffer_from_slice(&chunk.row_runs.runs, storage_options)?,
            lane: self.buffer_from_slice(&chunk.row_runs.lanes, storage_options)?,
            data: self.buffer_from_slice(&chunk.row_runs.data, storage_options)?,
            flag: self.buffer_from_slice(&chunk.row_runs.flags, storage_options)?,
            weight: self.buffer_from_slice(&chunk.row_runs.weights, storage_options)?,
            group_desc: self.buffer_from_slice(&chunk.group_descs, storage_options)?,
            lane_ref: self.buffer_from_slice(&chunk.lane_refs, storage_options)?,
            grouped_lane,
            params: self.buffer_from_slice(std::slice::from_ref(&params), storage_options)?,
        })
    }

    fn cached_grouped_chunk(
        &self,
        chunk: MetalResidualGroupedRowRunChunk,
        params: MetalResidualRowRunParams,
        storage_options: objc2_metal::MTLResourceOptions,
    ) -> Result<MetalResidualGroupedCachedChunk, ImagingError> {
        let metrics = MetalResidualGroupedChunkMetrics::from_chunk(&chunk);
        if standard_mfs_metal_resident_grouped_input_buffers_enabled() {
            let buffers = self.cached_grouped_chunk_buffers(&chunk, params, storage_options)?;
            Ok(MetalResidualGroupedCachedChunk {
                params,
                metrics,
                host: None,
                buffers: Some(buffers),
            })
        } else {
            Ok(MetalResidualGroupedCachedChunk {
                params,
                metrics,
                host: Some(chunk),
                buffers: None,
            })
        }
    }

    fn dispatch_cached_residual_row_run_grouped_chunk(
        &self,
        cached: &MetalResidualGroupedCachedChunk,
        shared: &MetalResidualRowRunSharedBuffers<'_>,
    ) -> Result<MetalResidualDispatchTiming, ImagingError> {
        use objc2_metal::{
            MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
            MTLComputeCommandEncoder, MTLComputePipelineState, MTLSize,
        };

        let mut timing = MetalResidualDispatchTiming::default();
        if cached.metrics.runs == 0 || cached.metrics.group_descs == 0 {
            return Ok(timing);
        }
        let Some(buffers) = cached.buffers.as_ref() else {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal grouped row-run cache chunk has no resident input buffers"
                    .to_string(),
            ));
        };
        let params = cached.params;
        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run-grouped' could not create a residual command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal-row-run-grouped' could not create a residual compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.residual_row_run_grouped_prepare_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&buffers.run), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&buffers.lane), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&buffers.data), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(&buffers.flag), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(&buffers.weight), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(shared.density), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(shared.model_re), 0, 6);
            encoder.setBuffer_offset_atIndex(Some(shared.model_im), 0, 7);
            encoder.setBuffer_offset_atIndex(Some(&buffers.grouped_lane), 0, 8);
            encoder.setBuffer_offset_atIndex(Some(&buffers.params), 0, 9);
            encoder.setBuffer_offset_atIndex(Some(shared.tap_weights), 0, 10);
        }
        let prepare_thread_width = self
            .residual_row_run_grouped_prepare_pipeline
            .threadExecutionWidth()
            .max(1);
        let prepare_max_threads = self
            .residual_row_run_grouped_prepare_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let prepare_group_width = prepare_thread_width
            .min(params.max_lane_count as usize)
            .max(1);
        let prepare_group_height = (prepare_max_threads / prepare_group_width)
            .max(1)
            .min(params.run_count as usize)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: params.max_lane_count as usize,
                height: params.run_count as usize,
                depth: 1,
            },
            MTLSize {
                width: prepare_group_width,
                height: prepare_group_height,
                depth: 1,
            },
        );

        encoder.setComputePipelineState(&self.residual_row_run_grouped_accumulate_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&buffers.grouped_lane), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(&buffers.group_desc), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(&buffers.lane_ref), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(shared.grid_im), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(&buffers.params), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(shared.tap_weights), 0, 6);
        }
        let accumulate_width = cached.metrics.max_halo_cells.max(1);
        let accumulate_height = cached.metrics.group_descs.max(1);
        let accumulate_thread_width = self
            .residual_row_run_grouped_accumulate_pipeline
            .threadExecutionWidth()
            .max(1);
        let accumulate_max_threads = self
            .residual_row_run_grouped_accumulate_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let accumulate_group_width = accumulate_thread_width.min(accumulate_width).max(1);
        let accumulate_group_height = (accumulate_max_threads / accumulate_group_width)
            .max(1)
            .min(accumulate_height)
            .max(1);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: accumulate_width,
                height: accumulate_height,
                depth: 1,
            },
            MTLSize {
                width: accumulate_group_width,
                height: accumulate_group_height,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        let gpu_start = command_buffer.GPUStartTime();
        let gpu_end = command_buffer.GPUEndTime();
        if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
            timing.gpu += Duration::from_secs_f64(gpu_end - gpu_start);
        }
        let kernel_start = command_buffer.kernelStartTime();
        let kernel_end = command_buffer.kernelEndTime();
        if kernel_start.is_finite() && kernel_end.is_finite() && kernel_end > kernel_start {
            timing.kernel += Duration::from_secs_f64(kernel_end - kernel_start);
        }
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal-row-run-grouped' residual refresh command failed: {message}"
            )));
        }
        Ok(timing)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_metal_residual_samples_from_routed_run(
        &self,
        gridder: &StandardGridder,
        routed_run: &StandardMfsRoutedVisibilityRun,
        weighting_plan: &StandardMfsStreamingWeightingPlan,
        accumulation: &mut StandardMfsTiledResidualAccumulation,
        chunk: &mut Vec<MetalResidualSample>,
        sample_stride: usize,
        timings: &mut MetalResidualRefreshTimings,
    ) -> Result<(), ImagingError> {
        if routed_run.is_empty() {
            return Ok(());
        }
        let run_wrap_started = Instant::now();
        let run = StandardMfsTileVisibilityRun::from_routed_visibility_run(
            routed_run,
            0..routed_run.len(),
            routed_run.first_input_seq,
        );
        timings.run_wrap += run_wrap_started.elapsed();
        accumulation.valid_samples = accumulation.valid_samples.saturating_add(run.len());
        accumulation.planned_samples = accumulation.planned_samples.saturating_add(run.len());
        for sample_index in 0..run.len() {
            let timed_sample = sample_stride > 0
                && accumulation
                    .gridded_residual_samples
                    .checked_rem(sample_stride)
                    .is_some_and(|remainder| remainder == 0);
            let sample_decode_started = timed_sample.then(Instant::now);
            let Some(sample) = run.routed_queue_sample_at(sample_index, false)? else {
                if let Some(started) = sample_decode_started {
                    timings.sample_decode_sampled += started.elapsed();
                    timings.sampled_samples = timings.sampled_samples.saturating_add(1);
                }
                accumulation.skipped_nonfinite_visibility =
                    accumulation.skipped_nonfinite_visibility.saturating_add(1);
                continue;
            };
            if let Some(started) = sample_decode_started {
                timings.sample_decode_sampled += started.elapsed();
            }
            let StandardMfsRoutedQueueVisibility::Finite(visibility) = sample.visibility else {
                accumulation.skipped_nonfinite_visibility =
                    accumulation.skipped_nonfinite_visibility.saturating_add(1);
                continue;
            };
            let weight_started = timed_sample.then(Instant::now);
            let Some(grid_weight) = sample.weighted_grid_weight(weighting_plan)? else {
                if let Some(started) = weight_started {
                    timings.weight_sampled += started.elapsed();
                    timings.sampled_samples = timings.sampled_samples.saturating_add(1);
                }
                accumulation.skipped_invalid_weight =
                    accumulation.skipped_invalid_weight.saturating_add(1);
                continue;
            };
            if let Some(started) = weight_started {
                timings.weight_sampled += started.elapsed();
            }
            let coordinate_started = timed_sample.then(Instant::now);
            let Some([grid_x, grid_y]) =
                gridder.positive_tap_grid_coordinates(sample.u_lambda, sample.v_lambda)
            else {
                if let Some(started) = coordinate_started {
                    timings.tap_plan_sampled += started.elapsed();
                    timings.sampled_samples = timings.sampled_samples.saturating_add(1);
                }
                accumulation.skipped_out_of_grid =
                    accumulation.skipped_out_of_grid.saturating_add(1);
                continue;
            };
            if let Some(started) = coordinate_started {
                timings.tap_plan_sampled += started.elapsed();
            }
            let push_started = timed_sample.then(Instant::now);
            chunk.push(MetalResidualSample {
                grid_x,
                grid_y,
                grid_weight,
                _pad0: 0.0,
                visibility_re: visibility.re,
                visibility_im: visibility.im,
                _pad1: [0.0; 2],
            });
            if let Some(started) = push_started {
                timings.push_sampled += started.elapsed();
                timings.sampled_samples = timings.sampled_samples.saturating_add(1);
            }
            accumulation.gridded_residual_samples =
                accumulation.gridded_residual_samples.saturating_add(1);
        }
        Ok(())
    }

    fn dispatch_residual_refresh_chunk(
        &self,
        samples: &[MetalResidualSample],
        grid_width: usize,
        grid_height: usize,
        buffers: MetalResidualDispatchBuffers<'_>,
    ) -> Result<MetalResidualDispatchTiming, ImagingError> {
        use std::{mem, ptr};

        use objc2_metal::{
            MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
            MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState, MTLSize,
        };

        let mut timing = MetalResidualDispatchTiming::default();
        let sample_count = u32::try_from(samples.len()).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS Metal residual chunk has too many samples".to_string(),
            )
        })?;
        if sample_count == 0 {
            return Ok(timing);
        }
        let params = MetalResidualParams {
            sample_count,
            grid_width: u32::try_from(grid_width).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal residual grid width exceeds u32".to_string(),
                )
            })?,
            grid_height: u32::try_from(grid_height).map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal residual grid height exceeds u32".to_string(),
                )
            })?,
            oversampling: buffers.oversampling,
            tap_weight_count: buffers.tap_weight_count,
            _pad0: [0; 3],
        };
        let sample_buffer_started = Instant::now();
        let sample_bytes = mem::size_of_val(samples);
        unsafe {
            ptr::copy_nonoverlapping(
                samples.as_ptr().cast::<u8>(),
                buffers.sample_buffer.contents().as_ptr().cast::<u8>(),
                sample_bytes,
            );
        }
        timing.sample_buffer += sample_buffer_started.elapsed();
        let params_buffer_started = Instant::now();
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(params).cast::<u8>(),
                buffers.params_buffer.contents().as_ptr().cast::<u8>(),
                mem::size_of::<MetalResidualParams>(),
            );
        }
        timing.params_buffer += params_buffer_started.elapsed();
        let encode_started = Instant::now();
        let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a residual command buffer"
                    .to_string(),
            )
        })?;
        let encoder = command_buffer.computeCommandEncoder().ok_or_else(|| {
            ImagingError::Unsupported(
                "standard MFS backend 'metal' could not create a residual compute encoder"
                    .to_string(),
            )
        })?;
        encoder.setComputePipelineState(&self.residual_pipeline);
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(buffers.sample_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(buffers.model_re), 0, 1);
            encoder.setBuffer_offset_atIndex(Some(buffers.model_im), 0, 2);
            encoder.setBuffer_offset_atIndex(Some(buffers.grid_re), 0, 3);
            encoder.setBuffer_offset_atIndex(Some(buffers.grid_im), 0, 4);
            encoder.setBuffer_offset_atIndex(Some(buffers.params_buffer), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(buffers.tap_weights), 0, 6);
        }
        let thread_count = samples.len();
        let thread_width = self.residual_pipeline.threadExecutionWidth().max(1);
        let max_threads = self
            .residual_pipeline
            .maxTotalThreadsPerThreadgroup()
            .max(1);
        let threads_per_group = thread_width.min(max_threads).min(thread_count);
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: thread_count,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: threads_per_group,
                height: 1,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        timing.encode += encode_started.elapsed();
        let wait_started = Instant::now();
        command_buffer.waitUntilCompleted();
        timing.wait += wait_started.elapsed();
        if command_buffer.status() == MTLCommandBufferStatus::Error {
            let message = command_buffer
                .error()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "unknown Metal command buffer error".to_string());
            return Err(ImagingError::Unsupported(format!(
                "standard MFS backend 'metal' residual refresh command failed: {message}"
            )));
        }
        Ok(timing)
    }

    fn buffer_from_slice<T>(
        &self,
        values: &[T],
        options: objc2_metal::MTLResourceOptions,
    ) -> Result<
        objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLBuffer>>,
        ImagingError,
    > {
        use std::{ffi::c_void, mem, ptr::NonNull};

        use objc2_metal::MTLDevice;

        let byte_len = mem::size_of_val(values);
        if byte_len == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal buffers must be non-empty".to_string(),
            ));
        }
        let pointer =
            NonNull::new(values.as_ptr().cast::<c_void>() as *mut c_void).ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal buffer pointer was null".to_string(),
                )
            })?;
        unsafe {
            self.device
                .newBufferWithBytes_length_options(pointer, byte_len, options)
                .ok_or_else(|| {
                    ImagingError::Unsupported(
                        "standard MFS backend 'metal' could not allocate an input buffer"
                            .to_string(),
                    )
                })
        }
    }

    fn buffer_from_slice_with_mode<T>(
        &self,
        values: &[T],
        options: objc2_metal::MTLResourceOptions,
        mode: MetalInputBufferCopyMode,
    ) -> Result<MetalBuffer, ImagingError> {
        match mode {
            MetalInputBufferCopyMode::Copy => self.buffer_from_slice(values, options),
            MetalInputBufferCopyMode::NoCopy => self.buffer_from_slice_no_copy(values, options),
        }
    }

    fn buffer_from_slice_no_copy<T>(
        &self,
        values: &[T],
        options: objc2_metal::MTLResourceOptions,
    ) -> Result<MetalBuffer, ImagingError> {
        use std::{ffi::c_void, mem, ptr::NonNull};

        use objc2_metal::MTLDevice;

        let byte_len = mem::size_of_val(values);
        if byte_len == 0 {
            return Err(ImagingError::InvalidRequest(
                "standard MFS Metal buffers must be non-empty".to_string(),
            ));
        }
        let pointer =
            NonNull::new(values.as_ptr().cast::<c_void>() as *mut c_void).ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "standard MFS Metal buffer pointer was null".to_string(),
                )
            })?;
        unsafe {
            self.device
                .newBufferWithBytesNoCopy_length_options_deallocator(
                    pointer, byte_len, options, None,
                )
                .ok_or_else(|| {
                    ImagingError::Unsupported(
                        "standard MFS backend 'metal' could not wrap an input buffer without copying"
                            .to_string(),
                    )
                })
        }
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn metal_error(
    context: &str,
    error: objc2::rc::Retained<objc2_foundation::NSError>,
) -> ImagingError {
    ImagingError::Unsupported(format!(
        "standard MFS backend 'metal' failed to {context}: {error:?}"
    ))
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn metal_density_convention_code(convention: DensityCellConvention) -> u32 {
    match convention {
        DensityCellConvention::VisImagingWeight => 0,
        DensityCellConvention::CubeBriggsWeightorDensity => 1,
        DensityCellConvention::CubeBriggsWeightorLookup => 2,
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn metal_pair_transform_code(transform: StandardMfsPairCollapseTransform) -> u32 {
    match transform {
        StandardMfsPairCollapseTransform::HalfSum => 0,
        StandardMfsPairCollapseTransform::HalfDifference => 1,
        StandardMfsPairCollapseTransform::PositiveHalfImagDifference => 2,
        StandardMfsPairCollapseTransform::NegativeHalfImagDifference => 3,
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn standard_mfs_metal_residual_chunk_samples() -> usize {
    const DEFAULT_CHUNK_SAMPLES: usize = 16_000_000;
    env::var("CASA_RS_STANDARD_MFS_METAL_CHUNK_SAMPLES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CHUNK_SAMPLES)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn standard_mfs_metal_residual_staging_sample_stride() -> usize {
    env::var("CASA_RS_STANDARD_MFS_METAL_STAGING_SAMPLE_STRIDE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn standard_mfs_metal_group_tile_edge() -> usize {
    const DEFAULT_GROUP_TILE_EDGE: usize = 1;
    env::var("CASA_RS_STANDARD_MFS_METAL_GROUP_TILE_EDGE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_GROUP_TILE_EDGE)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn standard_mfs_metal_resident_grouped_input_buffers_enabled() -> bool {
    env::var("CASA_RS_STANDARD_MFS_METAL_RESIDENT_GROUPED_INPUT_BUFFERS")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn mtmfs_metal_grouped_terms_enabled() -> bool {
    env::var("CASA_RS_MTMFS_METAL_GROUPED_TERMS")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[link(name = "CoreGraphics", kind = "framework")]
unsafe extern "C" {}

#[cfg(all(target_os = "macos", not(coverage)))]
const METAL_DIRTY_SHADER: &str = r#"
#include <metal_stdlib>
using namespace metal;

#define STANDARD_MFS_TAP_COUNT 7u
#define STANDARD_MFS_SUPPORT 3
#define STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY 1u

struct DirtySample {
    uint center_x;
    uint center_y;
    uint flags;
    uint _pad0;
    float grid_weight;
    float visibility_re;
    float visibility_im;
    float _pad1;
    float x_weights[STANDARD_MFS_TAP_COUNT];
    float y_weights[STANDARD_MFS_TAP_COUNT];
};

struct TileParams {
    uint sample_count;
    uint halo_x0;
    uint halo_y0;
    uint halo_width;
    uint halo_height;
    uint _pad0[3];
};

struct ResidualSample {
    float grid_x;
    float grid_y;
    float grid_weight;
    float _pad0;
    float visibility_re;
    float visibility_im;
    float _pad1[2];
};

struct ResidualParams {
    uint sample_count;
    uint grid_width;
    uint grid_height;
    uint oversampling;
    uint tap_weight_count;
    uint _pad0[3];
};

struct MtmfsSample {
    uint positive_center_x;
    uint positive_center_y;
    uint positive_x_weight_base;
    uint positive_y_weight_base;
    uint negative_center_x;
    uint negative_center_y;
    uint negative_x_weight_base;
    uint negative_y_weight_base;
    float weight;
    float sumwt_factor;
    float taylor_x;
    float _pad0;
    float visibility_re;
    float visibility_im;
    float _pad1[2];
};

struct MtmfsParams {
    uint sample_count;
    uint grid_width;
    uint grid_height;
    uint term_count;
    uint model_term_count;
    uint _pad0[3];
};

struct MtmfsResidualGroupedLane {
    uint positive_center_x;
    uint positive_center_y;
    uint positive_x_weight_base;
    uint positive_y_weight_base;
    uint negative_center_x;
    uint negative_center_y;
    uint negative_x_weight_base;
    uint negative_y_weight_base;
    float residual0_re;
    float residual0_im;
    float residual1_re;
    float residual1_im;
};

struct RowRunDesc {
    float u_m;
    float v_m;
    float sumwt_factor;
    float w_m;
    uint lane_offset;
    uint lane_count;
    uint data_offset;
    uint flag_offset;
    uint weight_offset;
    uint corr_count;
    uint polarization_mode;
    uint transform;
    uint corr0;
    uint corr1;
    uint _pad1[2];
};

struct RowRunLane {
    float lambda_scale;
    uint center_x;
    uint center_y;
    uint _pad0;
    float grid_x;
    float grid_y;
    float _pad1[2];
};

struct GroupedLane {
    uint center_x;
    uint center_y;
    uint x_weight_base;
    uint y_weight_base;
    float residual_re;
    float residual_im;
    float grid_weight;
    float _pad0;
};

struct InitialDirtyGroupedLane {
    uint center_x;
    uint center_y;
    uint x_weight_base;
    uint y_weight_base;
    float dirty_re;
    float dirty_im;
    float grid_weight;
    float dirty_valid;
};

struct InitialDirtyRunAccum {
    float sumwt;
    float max_abs_w_lambda;
    uint gridded;
    uint skipped;
};

struct GroupedTileDesc {
    uint ref_offset;
    uint ref_count;
    uint halo_x0;
    uint halo_y0;
    uint halo_width;
    uint halo_height;
    uint _pad0[2];
};

struct RowRunParams {
    uint run_count;
    uint max_lane_count;
    uint grid_width;
    uint grid_height;
    uint oversampling;
    uint tap_weight_count;
    uint weighting_mode;
    uint density_convention;
    uint density_width;
    uint density_height;
    uint diagnostic_mode;
    uint _pad0;
    float du_lambda;
    float dv_lambda;
    float density_center_x;
    float density_center_y;
    float density_u_scale;
    float density_v_scale;
    float briggs_f2;
    float _pad1;
};

static inline void atomic_add_float(device atomic_uint *address, float value) {
    uint old_bits = atomic_load_explicit(address, memory_order_relaxed);
    while (true) {
        float old_value = as_type<float>(old_bits);
        uint new_bits = as_type<uint>(old_value + value);
        if (atomic_compare_exchange_weak_explicit(
                address,
                &old_bits,
                new_bits,
                memory_order_relaxed,
                memory_order_relaxed)) {
            return;
        }
    }
}

static inline int round_half_away_from_zero(float value) {
    return value >= 0.0f ? int(floor(value + 0.5f)) : int(ceil(value - 0.5f));
}

static inline bool row_run_density_lookup(
    float u_lambda,
    float v_lambda,
    constant RowRunParams &params,
    device const float *density,
    thread float &cell_density
) {
    float x;
    float y;
    if (params.density_convention == 0u) {
        const float u = float(u_lambda);
        const float v = float(v_lambda);
        x = -u * params.density_u_scale + params.density_center_x;
        y = v * params.density_v_scale + params.density_center_y;
    } else {
        x = u_lambda * params.density_u_scale + params.density_center_x;
        y = -v_lambda * params.density_v_scale + params.density_center_y;
    }
    if (!isfinite(x) || !isfinite(y)) {
        return false;
    }
    const int anchor_x = params.density_convention == 0u ? int(x) : round_half_away_from_zero(x);
    const int anchor_y = params.density_convention == 0u ? int(y) : round_half_away_from_zero(y);
    if (anchor_x <= 0 || anchor_y <= 0 ||
        anchor_x >= int(params.density_width) ||
        anchor_y >= int(params.density_height)) {
        return false;
    }
    cell_density = density[uint(anchor_x) * params.density_height + uint(anchor_y)];
    return isfinite(cell_density) && cell_density > 0.0f;
}

static inline float mtmfs_taylor_power(float taylor_x, uint order) {
    float value = 1.0f;
    for (uint index = 0u; index < order; ++index) {
        value *= taylor_x;
    }
    return value;
}

static inline float2 mtmfs_degrid_term(
    device const float *model_re,
    device const float *model_im,
    device const float *tap_weights,
    constant MtmfsParams &params,
    const MtmfsSample sample,
    uint model_order
) {
    const int center_x = int(sample.positive_center_x);
    const int center_y = int(sample.positive_center_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    const uint cell_offset = model_order * params.grid_width * params.grid_height;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[sample.positive_x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[sample.positive_y_weight_base + dy];
            uint cell = cell_offset + uint(x) * params.grid_height + uint(y);
            predicted_re += model_re[cell] * tap_weight;
            predicted_im += model_im[cell] * tap_weight;
        }
    }
    return float2(predicted_re, predicted_im);
}

static inline void mtmfs_grid_one_side(
    device atomic_uint *grid_re,
    device atomic_uint *grid_im,
    device const float *tap_weights,
    constant MtmfsParams &params,
    uint term_order,
    uint center_x_u,
    uint center_y_u,
    uint x_weight_base,
    uint y_weight_base,
    float2 value
) {
    const int center_x = int(center_x_u);
    const int center_y = int(center_y_u);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    const uint cell_offset = term_order * params.grid_width * params.grid_height;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy];
            uint cell = cell_offset + uint(x) * params.grid_height + uint(y);
            atomic_add_float(&grid_re[cell], value.x * tap_weight);
            atomic_add_float(&grid_im[cell], value.y * tap_weight);
        }
    }
}

static inline float2 row_run_collapse_pair(float2 first, float2 second, uint transform) {
    if (transform == 1u) {
        return (first - second) * 0.5f;
    }
    if (transform == 2u) {
        float2 difference = first - second;
        return float2(-difference.y, difference.x) * 0.5f;
    }
    if (transform == 3u) {
        float2 difference = first - second;
        return float2(difference.y, -difference.x) * 0.5f;
    }
    return (first + second) * 0.5f;
}

kernel void initial_dirty_psf_row_run_grouped_prepare(
    device const RowRunDesc *runs [[buffer(0)]],
    device const RowRunLane *lanes [[buffer(1)]],
    device const float2 *data [[buffer(2)]],
    device const uchar *flags [[buffer(3)]],
    device const float *weights [[buffer(4)]],
    device const float *density [[buffer(5)]],
    device InitialDirtyGroupedLane *grouped_lanes [[buffer(6)]],
    constant RowRunParams &params [[buffer(7)]],
    device const float *tap_weights [[buffer(8)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint lane_index = gid.x;
    const uint run_index = gid.y;
    if (run_index >= params.run_count || lane_index >= params.max_lane_count) {
        return;
    }
    const RowRunDesc run = runs[run_index];
    if (lane_index >= run.lane_count) {
        return;
    }
    const RowRunLane lane = lanes[run.lane_offset + lane_index];
    const uint output_index = run.lane_offset + lane_index;
    InitialDirtyGroupedLane output;
    output.center_x = lane.center_x;
    output.center_y = lane.center_y;
    output.x_weight_base = 0u;
    output.y_weight_base = 0u;
    output.dirty_re = 0.0f;
    output.dirty_im = 0.0f;
    output.grid_weight = 0.0f;
    output.dirty_valid = 0.0f;
    grouped_lanes[output_index] = output;

    const float u_lambda = run.u_m * lane.lambda_scale;
    const float v_lambda = run.v_m * lane.lambda_scale;

    float natural_weight;
    float2 visibility = float2(0.0f, 0.0f);
    float dirty_valid = 0.0f;
    if (run.polarization_mode == 0u) {
        if (run.corr0 >= run.corr_count) {
            return;
        }
        const uint index = run.corr0 * run.lane_count + lane_index;
        if (flags[run.flag_offset + index] != 0) {
            return;
        }
        const float2 observed = data[run.data_offset + index];
        if (isfinite(observed.x) && isfinite(observed.y)) {
            visibility = observed;
            dirty_valid = 1.0f;
        }
        natural_weight = weights[run.weight_offset + run.corr0];
    } else {
        if (run.corr0 >= run.corr_count || run.corr1 >= run.corr_count) {
            return;
        }
        const uint first_index = run.corr0 * run.lane_count + lane_index;
        const uint second_index = run.corr1 * run.lane_count + lane_index;
        if (flags[run.flag_offset + first_index] != 0 ||
            flags[run.flag_offset + second_index] != 0) {
            return;
        }
        const float2 first_visibility = data[run.data_offset + first_index];
        const float2 second_visibility = data[run.data_offset + second_index];
        visibility = row_run_collapse_pair(first_visibility, second_visibility, run.transform);
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        dirty_valid = 1.0f;
        const float first_weight = weights[run.weight_offset + run.corr0];
        const float second_weight = weights[run.weight_offset + run.corr1];
        if (!(isfinite(first_weight) && first_weight > 0.0f &&
              isfinite(second_weight) && second_weight > 0.0f)) {
            return;
        }
        natural_weight = 0.5f * (first_weight + second_weight);
    }
    if (!(isfinite(natural_weight) && natural_weight > 0.0f &&
          isfinite(run.sumwt_factor) && run.sumwt_factor > 0.0f)) {
        return;
    }

    float final_weight = natural_weight;
    if (params.weighting_mode != 0u) {
        float cell_density = 0.0f;
        if (!row_run_density_lookup(u_lambda, v_lambda, params, density, cell_density)) {
            return;
        }
        if (params.weighting_mode == 1u) {
            final_weight = natural_weight / cell_density;
        } else {
            final_weight = natural_weight / (params.briggs_f2 * cell_density + 1.0f);
        }
    }
    const float grid_weight = final_weight * run.sumwt_factor;
    if (!(isfinite(grid_weight) && grid_weight > 0.0f)) {
        return;
    }

    const float grid_x = lane.grid_x;
    const float grid_y = lane.grid_y;
    if (!isfinite(grid_x) || !isfinite(grid_y)) {
        return;
    }
    const int center_x = int(lane.center_x);
    const int center_y = int(lane.center_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    if (start_x < 0 || start_y < 0 ||
        center_x + STANDARD_MFS_SUPPORT >= int(params.grid_width) ||
        center_y + STANDARD_MFS_SUPPORT >= int(params.grid_height)) {
        return;
    }
    const int offset_x = round_half_away_from_zero((float(center_x) - grid_x) * float(params.oversampling));
    const int offset_y = round_half_away_from_zero((float(center_y) - grid_y) * float(params.oversampling));
    const int half_oversampling = int(params.oversampling / 2u);
    const int x_weight_index = offset_x + half_oversampling;
    const int y_weight_index = offset_y + half_oversampling;
    if (x_weight_index < 0 || y_weight_index < 0 ||
        x_weight_index >= int(params.tap_weight_count) ||
        y_weight_index >= int(params.tap_weight_count)) {
        return;
    }

    output.x_weight_base = uint(x_weight_index) * STANDARD_MFS_TAP_COUNT;
    output.y_weight_base = uint(y_weight_index) * STANDARD_MFS_TAP_COUNT;
    output.dirty_re = visibility.x;
    output.dirty_im = visibility.y;
    output.grid_weight = grid_weight;
    output.dirty_valid = dirty_valid;
    grouped_lanes[output_index] = output;
}

kernel void initial_dirty_psf_row_run_grouped_accumulate_runs(
    device const RowRunDesc *runs [[buffer(0)]],
    device const RowRunLane *lanes [[buffer(1)]],
    device const float2 *data [[buffer(2)]],
    device const uchar *flags [[buffer(3)]],
    device const float *weights [[buffer(4)]],
    device const float *density [[buffer(5)]],
    device InitialDirtyRunAccum *run_accum [[buffer(6)]],
    constant RowRunParams &params [[buffer(7)]],
    uint run_index [[thread_position_in_grid]]
) {
    if (run_index >= params.run_count) {
        return;
    }
    const RowRunDesc run = runs[run_index];
    InitialDirtyRunAccum output;
    output.sumwt = 0.0f;
    output.max_abs_w_lambda = 0.0f;
    output.gridded = 0u;
    output.skipped = 0u;

    if (!(isfinite(run.sumwt_factor) && run.sumwt_factor > 0.0f)) {
        output.skipped = run.lane_count;
        run_accum[run_index] = output;
        return;
    }

    for (uint lane_index = 0u; lane_index < run.lane_count; ++lane_index) {
        const RowRunLane lane = lanes[run.lane_offset + lane_index];
        const float u_lambda = run.u_m * lane.lambda_scale;
        const float v_lambda = run.v_m * lane.lambda_scale;
        const float w_lambda = run.w_m * lane.lambda_scale;
        output.max_abs_w_lambda = max(output.max_abs_w_lambda, fabs(w_lambda));

        float natural_weight;
        if (run.polarization_mode == 0u) {
            if (run.corr0 >= run.corr_count) {
                output.skipped += 1u;
                continue;
            }
            const uint index = run.corr0 * run.lane_count + lane_index;
            if (flags[run.flag_offset + index] != 0) {
                output.skipped += 1u;
                continue;
            }
            natural_weight = weights[run.weight_offset + run.corr0];
        } else {
            if (run.corr0 >= run.corr_count || run.corr1 >= run.corr_count) {
                output.skipped += 1u;
                continue;
            }
            const uint first_index = run.corr0 * run.lane_count + lane_index;
            const uint second_index = run.corr1 * run.lane_count + lane_index;
            if (flags[run.flag_offset + first_index] != 0 ||
                flags[run.flag_offset + second_index] != 0) {
                output.skipped += 1u;
                continue;
            }
            const float2 first_visibility = data[run.data_offset + first_index];
            const float2 second_visibility = data[run.data_offset + second_index];
            const float2 visibility =
                row_run_collapse_pair(first_visibility, second_visibility, run.transform);
            if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
                output.skipped += 1u;
                continue;
            }
            const float first_weight = weights[run.weight_offset + run.corr0];
            const float second_weight = weights[run.weight_offset + run.corr1];
            if (!(isfinite(first_weight) && first_weight > 0.0f &&
                  isfinite(second_weight) && second_weight > 0.0f)) {
                output.skipped += 1u;
                continue;
            }
            natural_weight = 0.5f * (first_weight + second_weight);
        }

        if (!(isfinite(natural_weight) && natural_weight > 0.0f)) {
            output.skipped += 1u;
            continue;
        }

        float final_weight = natural_weight;
        if (params.weighting_mode != 0u) {
            float cell_density = 0.0f;
            if (!row_run_density_lookup(u_lambda, v_lambda, params, density, cell_density)) {
                output.skipped += 1u;
                continue;
            }
            if (params.weighting_mode == 1u) {
                final_weight = natural_weight / cell_density;
            } else {
                final_weight = natural_weight / (params.briggs_f2 * cell_density + 1.0f);
            }
        }
        const float grid_weight = final_weight * run.sumwt_factor;
        if (!(isfinite(grid_weight) && grid_weight > 0.0f)) {
            output.skipped += 1u;
            continue;
        }
        output.sumwt += grid_weight;
        output.gridded += 1u;
    }

    run_accum[run_index] = output;
}

kernel void initial_dirty_psf_row_run_grouped_accumulate(
    device const InitialDirtyGroupedLane *grouped_lanes [[buffer(0)]],
    device const GroupedTileDesc *group_descs [[buffer(1)]],
    device const uint *lane_refs [[buffer(2)]],
    device atomic_uint *psf_re [[buffer(3)]],
    device atomic_uint *psf_im [[buffer(4)]],
    device atomic_uint *dirty_re [[buffer(5)]],
    device atomic_uint *dirty_im [[buffer(6)]],
    constant RowRunParams &params [[buffer(7)]],
    device const float *tap_weights [[buffer(8)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const GroupedTileDesc desc = group_descs[gid.y];
    const uint cell_index = gid.x;
    const uint halo_cell_count = desc.halo_width * desc.halo_height;
    if (cell_index >= halo_cell_count) {
        return;
    }
    const uint local_x = cell_index / desc.halo_height;
    const uint local_y = cell_index - local_x * desc.halo_height;
    const int global_x = int(desc.halo_x0 + local_x);
    const int global_y = int(desc.halo_y0 + local_y);
    float psf_sum = 0.0f;
    float dirty_sum_re = 0.0f;
    float dirty_sum_im = 0.0f;
    for (uint ref_index = 0; ref_index < desc.ref_count; ++ref_index) {
        const uint lane_index = lane_refs[desc.ref_offset + ref_index];
        const InitialDirtyGroupedLane lane = grouped_lanes[lane_index];
        if (!(isfinite(lane.grid_weight) && lane.grid_weight > 0.0f)) {
            continue;
        }
        const int tap_x = global_x - (int(lane.center_x) - STANDARD_MFS_SUPPORT);
        const int tap_y = global_y - (int(lane.center_y) - STANDARD_MFS_SUPPORT);
        if (tap_x < 0 || tap_x >= int(STANDARD_MFS_TAP_COUNT) ||
            tap_y < 0 || tap_y >= int(STANDARD_MFS_TAP_COUNT)) {
            continue;
        }
        const float weighted_tap =
            tap_weights[lane.x_weight_base + uint(tap_x)] *
            tap_weights[lane.y_weight_base + uint(tap_y)] *
            lane.grid_weight;
        psf_sum += weighted_tap;
        if (lane.dirty_valid > 0.0f) {
            dirty_sum_re += lane.dirty_re * weighted_tap;
            dirty_sum_im += lane.dirty_im * weighted_tap;
        }
    }
    const uint cell = uint(global_x) * params.grid_height + uint(global_y);
    if (psf_sum != 0.0f) {
        atomic_add_float(&psf_re[cell], psf_sum);
    }
    if (dirty_sum_re != 0.0f || dirty_sum_im != 0.0f) {
        atomic_add_float(&dirty_re[cell], dirty_sum_re);
        atomic_add_float(&dirty_im[cell], dirty_sum_im);
    }
}

kernel void grid_dirty_tile_cell_owner(
    device const DirtySample *samples [[buffer(0)]],
    device float2 *psf_grid [[buffer(1)]],
    device float2 *residual_grid [[buffer(2)]],
    constant TileParams &params [[buffer(3)]],
    uint gid [[thread_position_in_grid]]
) {
    const uint cell_count = params.halo_width * params.halo_height;
    if (gid >= cell_count) {
        return;
    }
    const uint local_x = gid / params.halo_height;
    const uint local_y = gid - local_x * params.halo_height;
    const int global_x = int(params.halo_x0 + local_x);
    const int global_y = int(params.halo_y0 + local_y);
    float psf = 0.0f;
    float residual_re = 0.0f;
    float residual_im = 0.0f;
    for (uint index = 0; index < params.sample_count; ++index) {
        const DirtySample sample = samples[index];
        const int tap_x = global_x - (int(sample.center_x) - STANDARD_MFS_SUPPORT);
        const int tap_y = global_y - (int(sample.center_y) - STANDARD_MFS_SUPPORT);
        if (tap_x < 0 || tap_x >= int(STANDARD_MFS_TAP_COUNT) ||
            tap_y < 0 || tap_y >= int(STANDARD_MFS_TAP_COUNT)) {
            continue;
        }
        const float weight =
            sample.x_weights[tap_x] * sample.y_weights[tap_y] * sample.grid_weight;
        psf += weight;
        if ((sample.flags & STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY) != 0u) {
            residual_re += sample.visibility_re * weight;
            residual_im += sample.visibility_im * weight;
        }
    }
    psf_grid[gid] = float2(psf, 0.0f);
    residual_grid[gid] = float2(residual_re, residual_im);
}

kernel void residual_refresh_global_atomic_exact(
    device const ResidualSample *samples [[buffer(0)]],
    device const float *model_re [[buffer(1)]],
    device const float *model_im [[buffer(2)]],
    device atomic_uint *grid_re [[buffer(3)]],
    device atomic_uint *grid_im [[buffer(4)]],
    constant ResidualParams &params [[buffer(5)]],
    device const float *tap_weights [[buffer(6)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    const ResidualSample sample = samples[gid];
    const int center_x = round_half_away_from_zero(sample.grid_x);
    const int center_y = round_half_away_from_zero(sample.grid_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    if (start_x < 0 || start_y < 0 ||
        center_x + STANDARD_MFS_SUPPORT >= int(params.grid_width) ||
        center_y + STANDARD_MFS_SUPPORT >= int(params.grid_height)) {
        return;
    }
    const int offset_x = round_half_away_from_zero((float(center_x) - sample.grid_x) * float(params.oversampling));
    const int offset_y = round_half_away_from_zero((float(center_y) - sample.grid_y) * float(params.oversampling));
    const int half_oversampling = int(params.oversampling / 2u);
    const int x_weight_index = offset_x + half_oversampling;
    const int y_weight_index = offset_y + half_oversampling;
    if (x_weight_index < 0 || y_weight_index < 0 ||
        x_weight_index >= int(params.tap_weight_count) ||
        y_weight_index >= int(params.tap_weight_count)) {
        return;
    }
    const uint x_weight_base = uint(x_weight_index) * STANDARD_MFS_TAP_COUNT;
    const uint y_weight_base = uint(y_weight_index) * STANDARD_MFS_TAP_COUNT;
    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy];
            uint cell = uint(x) * params.grid_height + uint(y);
            predicted_re += model_re[cell] * tap_weight;
            predicted_im += model_im[cell] * tap_weight;
        }
    }

    const float residual_re = sample.visibility_re - predicted_re;
    const float residual_im = sample.visibility_im - predicted_im;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy] * sample.grid_weight;
            uint cell = uint(x) * params.grid_height + uint(y);
            atomic_add_float(&grid_re[cell], residual_re * tap_weight);
            atomic_add_float(&grid_im[cell], residual_im * tap_weight);
        }
    }
}

kernel void mtmfs_psf_terms_global_atomic(
    device const MtmfsSample *samples [[buffer(0)]],
    constant MtmfsParams &params [[buffer(1)]],
    device const float *tap_weights [[buffer(2)]],
    device atomic_uint *grid_re [[buffer(3)]],
    device atomic_uint *grid_im [[buffer(4)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint sample_index = gid.x;
    const uint term_order = gid.y;
    if (sample_index >= params.sample_count || term_order >= params.term_count) {
        return;
    }
    const MtmfsSample sample = samples[sample_index];
    const float factor = mtmfs_taylor_power(sample.taylor_x, term_order);
    const float psf_weight = sample.weight * factor;
    if (!(isfinite(psf_weight) && psf_weight != 0.0f)) {
        return;
    }
    const float2 value = float2(psf_weight, 0.0f);
    mtmfs_grid_one_side(
        grid_re, grid_im, tap_weights, params, term_order,
        sample.positive_center_x, sample.positive_center_y,
        sample.positive_x_weight_base, sample.positive_y_weight_base, value);
    mtmfs_grid_one_side(
        grid_re, grid_im, tap_weights, params, term_order,
        sample.negative_center_x, sample.negative_center_y,
        sample.negative_x_weight_base, sample.negative_y_weight_base, value);
}

kernel void mtmfs_residual_terms_global_atomic(
    device const MtmfsSample *samples [[buffer(0)]],
    constant MtmfsParams &params [[buffer(1)]],
    device const float *tap_weights [[buffer(2)]],
    device atomic_uint *grid_re [[buffer(3)]],
    device atomic_uint *grid_im [[buffer(4)]],
    device const float *model_re [[buffer(5)]],
    device const float *model_im [[buffer(6)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint sample_index = gid.x;
    const uint residual_order = gid.y;
    if (sample_index >= params.sample_count || residual_order >= params.term_count) {
        return;
    }
    const MtmfsSample sample = samples[sample_index];
    const float observed_factor = mtmfs_taylor_power(sample.taylor_x, residual_order);
    float2 predicted = float2(0.0f, 0.0f);
    for (uint model_order = 0u; model_order < params.model_term_count; ++model_order) {
        const float factor = mtmfs_taylor_power(sample.taylor_x, residual_order + model_order);
        const float2 model_visibility =
            mtmfs_degrid_term(model_re, model_im, tap_weights, params, sample, model_order);
        predicted += model_visibility * factor;
    }
    const float2 observed =
        float2(sample.visibility_re * observed_factor, sample.visibility_im * observed_factor);
    const float2 residual = (observed - predicted) * sample.weight;
    if (!isfinite(residual.x) || !isfinite(residual.y)) {
        return;
    }
    mtmfs_grid_one_side(
        grid_re, grid_im, tap_weights, params, residual_order,
        sample.positive_center_x, sample.positive_center_y,
        sample.positive_x_weight_base, sample.positive_y_weight_base, residual);
    mtmfs_grid_one_side(
        grid_re, grid_im, tap_weights, params, residual_order,
        sample.negative_center_x, sample.negative_center_y,
        sample.negative_x_weight_base, sample.negative_y_weight_base,
        float2(residual.x, -residual.y));
}

kernel void mtmfs_psf_terms_grouped_accumulate(
    device const MtmfsSample *samples [[buffer(0)]],
    device const GroupedTileDesc *group_descs [[buffer(1)]],
    device const uint *lane_refs [[buffer(2)]],
    constant MtmfsParams &params [[buffer(3)]],
    device const float *tap_weights [[buffer(4)]],
    device atomic_uint *grid_re [[buffer(5)]],
    device atomic_uint *grid_im [[buffer(6)]],
    uint3 gid [[thread_position_in_grid]]
) {
    const uint cell_index = gid.x;
    const uint group_index = gid.y;
    const uint term_order = gid.z;
    if (term_order >= params.term_count) {
        return;
    }
    const GroupedTileDesc desc = group_descs[group_index];
    const uint halo_cell_count = desc.halo_width * desc.halo_height;
    if (cell_index >= halo_cell_count) {
        return;
    }
    const uint local_x = cell_index / desc.halo_height;
    const uint local_y = cell_index - local_x * desc.halo_height;
    const int global_x = int(desc.halo_x0 + local_x);
    const int global_y = int(desc.halo_y0 + local_y);
    const bool exact_center_group =
        desc.halo_width == STANDARD_MFS_TAP_COUNT &&
        desc.halo_height == STANDARD_MFS_TAP_COUNT;
    float sum = 0.0f;
    for (uint ref_index = 0; ref_index < desc.ref_count; ++ref_index) {
        const uint encoded_ref = lane_refs[desc.ref_offset + ref_index];
        const uint sample_index = encoded_ref >> 1u;
        if (sample_index >= params.sample_count) {
            continue;
        }
        const uint side = encoded_ref & 1u;
        const MtmfsSample sample = samples[sample_index];
        const uint center_x = side == 0u ? sample.positive_center_x : sample.negative_center_x;
        const uint center_y = side == 0u ? sample.positive_center_y : sample.negative_center_y;
        int tap_x;
        int tap_y;
        if (exact_center_group) {
            tap_x = int(local_x);
            tap_y = int(local_y);
        } else {
            tap_x = global_x - (int(center_x) - STANDARD_MFS_SUPPORT);
            tap_y = global_y - (int(center_y) - STANDARD_MFS_SUPPORT);
            if (tap_x < 0 || tap_x >= int(STANDARD_MFS_TAP_COUNT) ||
                tap_y < 0 || tap_y >= int(STANDARD_MFS_TAP_COUNT)) {
                continue;
            }
        }
        const uint x_weight_base =
            side == 0u ? sample.positive_x_weight_base : sample.negative_x_weight_base;
        const uint y_weight_base =
            side == 0u ? sample.positive_y_weight_base : sample.negative_y_weight_base;
        const float factor = mtmfs_taylor_power(sample.taylor_x, term_order);
        const float weighted_tap =
            tap_weights[x_weight_base + uint(tap_x)] *
            tap_weights[y_weight_base + uint(tap_y)] *
            sample.weight * factor;
        if (isfinite(weighted_tap)) {
            sum += weighted_tap;
        }
    }
    if (sum == 0.0f) {
        return;
    }
    const uint cell =
        term_order * params.grid_width * params.grid_height +
        uint(global_x) * params.grid_height + uint(global_y);
    atomic_add_float(&grid_re[cell], sum);
    atomic_add_float(&grid_im[cell], 0.0f);
}

kernel void mtmfs_residual_terms_grouped_prepare_nterms2(
    device const MtmfsSample *samples [[buffer(0)]],
    constant MtmfsParams &params [[buffer(1)]],
    device const float *tap_weights [[buffer(2)]],
    device const float *model_re [[buffer(3)]],
    device const float *model_im [[buffer(4)]],
    device MtmfsResidualGroupedLane *grouped_lanes [[buffer(5)]],
    uint sample_index [[thread_position_in_grid]]
) {
    if (sample_index >= params.sample_count) {
        return;
    }
    const MtmfsSample sample = samples[sample_index];
    MtmfsResidualGroupedLane output;
    output.positive_center_x = sample.positive_center_x;
    output.positive_center_y = sample.positive_center_y;
    output.positive_x_weight_base = sample.positive_x_weight_base;
    output.positive_y_weight_base = sample.positive_y_weight_base;
    output.negative_center_x = sample.negative_center_x;
    output.negative_center_y = sample.negative_center_y;
    output.negative_x_weight_base = sample.negative_x_weight_base;
    output.negative_y_weight_base = sample.negative_y_weight_base;

    float2 model0 = float2(0.0f, 0.0f);
    float2 model1 = float2(0.0f, 0.0f);
    if (params.model_term_count > 0u) {
        model0 = mtmfs_degrid_term(model_re, model_im, tap_weights, params, sample, 0u);
    }
    if (params.model_term_count > 1u) {
        model1 = mtmfs_degrid_term(model_re, model_im, tap_weights, params, sample, 1u);
    }
    const float x = sample.taylor_x;
    const float x2 = x * x;
    const float2 observed = float2(sample.visibility_re, sample.visibility_im);
    const float2 residual0 = (observed - (model0 + model1 * x)) * sample.weight;
    const float2 residual1 = (observed * x - (model0 * x + model1 * x2)) * sample.weight;
    output.residual0_re = isfinite(residual0.x) ? residual0.x : 0.0f;
    output.residual0_im = isfinite(residual0.y) ? residual0.y : 0.0f;
    output.residual1_re = isfinite(residual1.x) ? residual1.x : 0.0f;
    output.residual1_im = isfinite(residual1.y) ? residual1.y : 0.0f;
    grouped_lanes[sample_index] = output;
}

kernel void mtmfs_residual_terms_grouped_accumulate_nterms2(
    device const MtmfsResidualGroupedLane *grouped_lanes [[buffer(0)]],
    device const GroupedTileDesc *group_descs [[buffer(1)]],
    device const uint *lane_refs [[buffer(2)]],
    constant MtmfsParams &params [[buffer(3)]],
    device const float *tap_weights [[buffer(4)]],
    device atomic_uint *grid_re [[buffer(5)]],
    device atomic_uint *grid_im [[buffer(6)]],
    uint3 gid [[thread_position_in_grid]]
) {
    const uint cell_index = gid.x;
    const uint group_index = gid.y;
    const uint residual_order = gid.z;
    if (residual_order >= 2u) {
        return;
    }
    const GroupedTileDesc desc = group_descs[group_index];
    const uint halo_cell_count = desc.halo_width * desc.halo_height;
    if (cell_index >= halo_cell_count) {
        return;
    }
    const uint local_x = cell_index / desc.halo_height;
    const uint local_y = cell_index - local_x * desc.halo_height;
    const int global_x = int(desc.halo_x0 + local_x);
    const int global_y = int(desc.halo_y0 + local_y);
    const bool exact_center_group =
        desc.halo_width == STANDARD_MFS_TAP_COUNT &&
        desc.halo_height == STANDARD_MFS_TAP_COUNT;
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint ref_index = 0; ref_index < desc.ref_count; ++ref_index) {
        const uint encoded_ref = lane_refs[desc.ref_offset + ref_index];
        const uint sample_index = encoded_ref >> 1u;
        if (sample_index >= params.sample_count) {
            continue;
        }
        const uint side = encoded_ref & 1u;
        const MtmfsResidualGroupedLane lane = grouped_lanes[sample_index];
        const uint center_x = side == 0u ? lane.positive_center_x : lane.negative_center_x;
        const uint center_y = side == 0u ? lane.positive_center_y : lane.negative_center_y;
        int tap_x;
        int tap_y;
        if (exact_center_group) {
            tap_x = int(local_x);
            tap_y = int(local_y);
        } else {
            tap_x = global_x - (int(center_x) - STANDARD_MFS_SUPPORT);
            tap_y = global_y - (int(center_y) - STANDARD_MFS_SUPPORT);
            if (tap_x < 0 || tap_x >= int(STANDARD_MFS_TAP_COUNT) ||
                tap_y < 0 || tap_y >= int(STANDARD_MFS_TAP_COUNT)) {
                continue;
            }
        }
        const uint x_weight_base =
            side == 0u ? lane.positive_x_weight_base : lane.negative_x_weight_base;
        const uint y_weight_base =
            side == 0u ? lane.positive_y_weight_base : lane.negative_y_weight_base;
        const float tap_weight =
            tap_weights[x_weight_base + uint(tap_x)] *
            tap_weights[y_weight_base + uint(tap_y)];
        const float residual_re = residual_order == 0u ? lane.residual0_re : lane.residual1_re;
        float residual_im = residual_order == 0u ? lane.residual0_im : lane.residual1_im;
        if (side != 0u) {
            residual_im = -residual_im;
        }
        sum_re += residual_re * tap_weight;
        sum_im += residual_im * tap_weight;
    }
    if (sum_re == 0.0f && sum_im == 0.0f) {
        return;
    }
    const uint cell =
        residual_order * params.grid_width * params.grid_height +
        uint(global_x) * params.grid_height + uint(global_y);
    atomic_add_float(&grid_re[cell], sum_re);
    atomic_add_float(&grid_im[cell], sum_im);
}

kernel void residual_refresh_row_run_global_atomic_exact(
    device const RowRunDesc *runs [[buffer(0)]],
    device const RowRunLane *lanes [[buffer(1)]],
    device const float2 *data [[buffer(2)]],
    device const uchar *flags [[buffer(3)]],
    device const float *weights [[buffer(4)]],
    device const float *density [[buffer(5)]],
    device const float *model_re [[buffer(6)]],
    device const float *model_im [[buffer(7)]],
    device atomic_uint *grid_re [[buffer(8)]],
    device atomic_uint *grid_im [[buffer(9)]],
    constant RowRunParams &params [[buffer(10)]],
    device const float *tap_weights [[buffer(11)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint lane_index = gid.x;
    const uint run_index = gid.y;
    if (run_index >= params.run_count || lane_index >= params.max_lane_count) {
        return;
    }
    const RowRunDesc run = runs[run_index];
    if (lane_index >= run.lane_count) {
        return;
    }
    const RowRunLane lane = lanes[run.lane_offset + lane_index];
    const float u_lambda = run.u_m * lane.lambda_scale;
    const float v_lambda = run.v_m * lane.lambda_scale;

    float natural_weight;
    float2 visibility;
    if (run.polarization_mode == 0u) {
        if (run.corr0 >= run.corr_count) {
            return;
        }
        const uint index = run.corr0 * run.lane_count + lane_index;
        if (flags[run.flag_offset + index] != 0) {
            return;
        }
        visibility = data[run.data_offset + index];
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        natural_weight = weights[run.weight_offset + run.corr0];
    } else {
        if (run.corr0 >= run.corr_count || run.corr1 >= run.corr_count) {
            return;
        }
        const uint first_index = run.corr0 * run.lane_count + lane_index;
        const uint second_index = run.corr1 * run.lane_count + lane_index;
        if (flags[run.flag_offset + first_index] != 0 ||
            flags[run.flag_offset + second_index] != 0) {
            return;
        }
        const float2 first_visibility = data[run.data_offset + first_index];
        const float2 second_visibility = data[run.data_offset + second_index];
        visibility = row_run_collapse_pair(first_visibility, second_visibility, run.transform);
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        const float first_weight = weights[run.weight_offset + run.corr0];
        const float second_weight = weights[run.weight_offset + run.corr1];
        if (!(isfinite(first_weight) && first_weight > 0.0f &&
              isfinite(second_weight) && second_weight > 0.0f)) {
            return;
        }
        natural_weight = 0.5f * (first_weight + second_weight);
    }
    if (!(isfinite(natural_weight) && natural_weight > 0.0f &&
          isfinite(run.sumwt_factor) && run.sumwt_factor > 0.0f)) {
        return;
    }

    float final_weight = natural_weight;
    if (params.weighting_mode != 0u) {
        float cell_density = 0.0f;
        if (!row_run_density_lookup(u_lambda, v_lambda, params, density, cell_density)) {
            return;
        }
        if (params.weighting_mode == 1u) {
            final_weight = natural_weight / cell_density;
        } else {
            final_weight = natural_weight / (params.briggs_f2 * cell_density + 1.0f);
        }
    }
    const float grid_weight = final_weight * run.sumwt_factor;
    if (!(isfinite(grid_weight) && grid_weight > 0.0f)) {
        return;
    }

    const float grid_x = lane.grid_x;
    const float grid_y = lane.grid_y;
    if (!isfinite(grid_x) || !isfinite(grid_y)) {
        return;
    }
    const int center_x = round_half_away_from_zero(grid_x);
    const int center_y = round_half_away_from_zero(grid_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    if (start_x < 0 || start_y < 0 ||
        center_x + STANDARD_MFS_SUPPORT >= int(params.grid_width) ||
        center_y + STANDARD_MFS_SUPPORT >= int(params.grid_height)) {
        return;
    }
    const int offset_x = round_half_away_from_zero((float(center_x) - grid_x) * float(params.oversampling));
    const int offset_y = round_half_away_from_zero((float(center_y) - grid_y) * float(params.oversampling));
    const int half_oversampling = int(params.oversampling / 2u);
    const int x_weight_index = offset_x + half_oversampling;
    const int y_weight_index = offset_y + half_oversampling;
    if (x_weight_index < 0 || y_weight_index < 0 ||
        x_weight_index >= int(params.tap_weight_count) ||
        y_weight_index >= int(params.tap_weight_count)) {
        return;
    }
    const uint x_weight_base = uint(x_weight_index) * STANDARD_MFS_TAP_COUNT;
    const uint y_weight_base = uint(y_weight_index) * STANDARD_MFS_TAP_COUNT;
    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy];
            uint cell = uint(x) * params.grid_height + uint(y);
            predicted_re += model_re[cell] * tap_weight;
            predicted_im += model_im[cell] * tap_weight;
        }
    }

    const float residual_re = visibility.x - predicted_re;
    const float residual_im = visibility.y - predicted_im;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy] * grid_weight;
            uint cell = uint(x) * params.grid_height + uint(y);
            atomic_add_float(&grid_re[cell], residual_re * tap_weight);
            atomic_add_float(&grid_im[cell], residual_im * tap_weight);
        }
    }
}

kernel void residual_refresh_row_run_grouped_prepare(
    device const RowRunDesc *runs [[buffer(0)]],
    device const RowRunLane *lanes [[buffer(1)]],
    device const float2 *data [[buffer(2)]],
    device const uchar *flags [[buffer(3)]],
    device const float *weights [[buffer(4)]],
    device const float *density [[buffer(5)]],
    device const float *model_re [[buffer(6)]],
    device const float *model_im [[buffer(7)]],
    device GroupedLane *grouped_lanes [[buffer(8)]],
    constant RowRunParams &params [[buffer(9)]],
    device const float *tap_weights [[buffer(10)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint lane_index = gid.x;
    const uint run_index = gid.y;
    if (run_index >= params.run_count || lane_index >= params.max_lane_count) {
        return;
    }
    const RowRunDesc run = runs[run_index];
    if (lane_index >= run.lane_count) {
        return;
    }
    const RowRunLane lane = lanes[run.lane_offset + lane_index];
    const uint output_index = run.lane_offset + lane_index;
    GroupedLane output;
    output.center_x = lane.center_x;
    output.center_y = lane.center_y;
    output.x_weight_base = 0u;
    output.y_weight_base = 0u;
    output.residual_re = 0.0f;
    output.residual_im = 0.0f;
    output.grid_weight = 0.0f;
    output._pad0 = 0.0f;
    grouped_lanes[output_index] = output;

    const bool psf_only_mode = params.diagnostic_mode == 5u;
    const bool dirty_only_mode = params.diagnostic_mode == 6u;
    const float u_lambda = run.u_m * lane.lambda_scale;
    const float v_lambda = run.v_m * lane.lambda_scale;

    float natural_weight;
    float2 visibility;
    if (run.polarization_mode == 0u) {
        if (run.corr0 >= run.corr_count) {
            return;
        }
        const uint index = run.corr0 * run.lane_count + lane_index;
        if (flags[run.flag_offset + index] != 0) {
            return;
        }
        if (psf_only_mode) {
            visibility = float2(1.0f, 0.0f);
        } else {
            visibility = data[run.data_offset + index];
            if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
                return;
            }
        }
        natural_weight = weights[run.weight_offset + run.corr0];
    } else {
        if (run.corr0 >= run.corr_count || run.corr1 >= run.corr_count) {
            return;
        }
        const uint first_index = run.corr0 * run.lane_count + lane_index;
        const uint second_index = run.corr1 * run.lane_count + lane_index;
        if (flags[run.flag_offset + first_index] != 0 ||
            flags[run.flag_offset + second_index] != 0) {
            return;
        }
        const float2 first_visibility = data[run.data_offset + first_index];
        const float2 second_visibility = data[run.data_offset + second_index];
        visibility = row_run_collapse_pair(first_visibility, second_visibility, run.transform);
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        if (psf_only_mode) {
            visibility = float2(1.0f, 0.0f);
        }
        const float first_weight = weights[run.weight_offset + run.corr0];
        const float second_weight = weights[run.weight_offset + run.corr1];
        if (!(isfinite(first_weight) && first_weight > 0.0f &&
              isfinite(second_weight) && second_weight > 0.0f)) {
            return;
        }
        natural_weight = 0.5f * (first_weight + second_weight);
    }
    if (!(isfinite(natural_weight) && natural_weight > 0.0f &&
          isfinite(run.sumwt_factor) && run.sumwt_factor > 0.0f)) {
        return;
    }

    float final_weight = natural_weight;
    if (params.weighting_mode != 0u) {
        float cell_density = 0.0f;
        if (!row_run_density_lookup(u_lambda, v_lambda, params, density, cell_density)) {
            return;
        }
        if (params.weighting_mode == 1u) {
            final_weight = natural_weight / cell_density;
        } else {
            final_weight = natural_weight / (params.briggs_f2 * cell_density + 1.0f);
        }
    }
    const float grid_weight = final_weight * run.sumwt_factor;
    if (!(isfinite(grid_weight) && grid_weight > 0.0f)) {
        return;
    }

    const float grid_x = lane.grid_x;
    const float grid_y = lane.grid_y;
    if (!isfinite(grid_x) || !isfinite(grid_y)) {
        return;
    }
    const int center_x = int(lane.center_x);
    const int center_y = int(lane.center_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    if (start_x < 0 || start_y < 0 ||
        center_x + STANDARD_MFS_SUPPORT >= int(params.grid_width) ||
        center_y + STANDARD_MFS_SUPPORT >= int(params.grid_height)) {
        return;
    }
    const int offset_x = round_half_away_from_zero((float(center_x) - grid_x) * float(params.oversampling));
    const int offset_y = round_half_away_from_zero((float(center_y) - grid_y) * float(params.oversampling));
    const int half_oversampling = int(params.oversampling / 2u);
    const int x_weight_index = offset_x + half_oversampling;
    const int y_weight_index = offset_y + half_oversampling;
    if (x_weight_index < 0 || y_weight_index < 0 ||
        x_weight_index >= int(params.tap_weight_count) ||
        y_weight_index >= int(params.tap_weight_count)) {
        return;
    }
    const uint x_weight_base = uint(x_weight_index) * STANDARD_MFS_TAP_COUNT;
    const uint y_weight_base = uint(y_weight_index) * STANDARD_MFS_TAP_COUNT;
    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    if (!psf_only_mode && !dirty_only_mode) {
        for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
            int x = start_x + int(dx);
            float wx = tap_weights[x_weight_base + dx];
            for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
                int y = start_y + int(dy);
                float tap_weight = wx * tap_weights[y_weight_base + dy];
                uint cell = uint(x) * params.grid_height + uint(y);
                predicted_re += model_re[cell] * tap_weight;
                predicted_im += model_im[cell] * tap_weight;
            }
        }
    }

    output.x_weight_base = x_weight_base;
    output.y_weight_base = y_weight_base;
    if (psf_only_mode) {
        output.residual_re = 1.0f;
        output.residual_im = 0.0f;
    } else {
        output.residual_re = visibility.x - predicted_re;
        output.residual_im = visibility.y - predicted_im;
    }
    output.grid_weight = grid_weight;
    grouped_lanes[output_index] = output;
}

kernel void residual_refresh_row_run_grouped_accumulate(
    device const GroupedLane *grouped_lanes [[buffer(0)]],
    device const GroupedTileDesc *group_descs [[buffer(1)]],
    device const uint *lane_refs [[buffer(2)]],
    device atomic_uint *grid_re [[buffer(3)]],
    device atomic_uint *grid_im [[buffer(4)]],
    constant RowRunParams &params [[buffer(5)]],
    device const float *tap_weights [[buffer(6)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const GroupedTileDesc desc = group_descs[gid.y];
    const uint cell_index = gid.x;
    const uint halo_cell_count = desc.halo_width * desc.halo_height;
    if (cell_index >= halo_cell_count) {
        return;
    }
    const uint local_x = cell_index / desc.halo_height;
    const uint local_y = cell_index - local_x * desc.halo_height;
    const int global_x = int(desc.halo_x0 + local_x);
    const int global_y = int(desc.halo_y0 + local_y);
    const bool exact_center_group =
        desc.halo_width == STANDARD_MFS_TAP_COUNT &&
        desc.halo_height == STANDARD_MFS_TAP_COUNT;
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint ref_index = 0; ref_index < desc.ref_count; ++ref_index) {
        const uint lane_index = lane_refs[desc.ref_offset + ref_index];
        const GroupedLane lane = grouped_lanes[lane_index];
        if (!(isfinite(lane.grid_weight) && lane.grid_weight > 0.0f)) {
            continue;
        }
        int tap_x;
        int tap_y;
        if (exact_center_group) {
            tap_x = int(local_x);
            tap_y = int(local_y);
        } else {
            tap_x = global_x - (int(lane.center_x) - STANDARD_MFS_SUPPORT);
            tap_y = global_y - (int(lane.center_y) - STANDARD_MFS_SUPPORT);
            if (tap_x < 0 || tap_x >= int(STANDARD_MFS_TAP_COUNT) ||
                tap_y < 0 || tap_y >= int(STANDARD_MFS_TAP_COUNT)) {
                continue;
            }
        }
        const float tap_weight =
            tap_weights[lane.x_weight_base + uint(tap_x)] *
            tap_weights[lane.y_weight_base + uint(tap_y)] *
            lane.grid_weight;
        sum_re += lane.residual_re * tap_weight;
        sum_im += lane.residual_im * tap_weight;
    }
    if (sum_re == 0.0f && sum_im == 0.0f) {
        return;
    }
    const uint cell = uint(global_x) * params.grid_height + uint(global_y);
    atomic_add_float(&grid_re[cell], sum_re);
    atomic_add_float(&grid_im[cell], sum_im);
}

kernel void residual_refresh_row_run_diagnostic(
    device const RowRunDesc *runs [[buffer(0)]],
    device const RowRunLane *lanes [[buffer(1)]],
    device const float2 *data [[buffer(2)]],
    device const uchar *flags [[buffer(3)]],
    device const float *weights [[buffer(4)]],
    device const float *density [[buffer(5)]],
    device const float *model_re [[buffer(6)]],
    device const float *model_im [[buffer(7)]],
    device atomic_uint *grid_re [[buffer(8)]],
    device atomic_uint *grid_im [[buffer(9)]],
    constant RowRunParams &params [[buffer(10)]],
    device const float *tap_weights [[buffer(11)]],
    device uint *diagnostic_output [[buffer(12)]],
    uint2 gid [[thread_position_in_grid]]
) {
    const uint lane_index = gid.x;
    const uint run_index = gid.y;
    if (run_index >= params.run_count || lane_index >= params.max_lane_count) {
        return;
    }
    const RowRunDesc run = runs[run_index];
    if (lane_index >= run.lane_count) {
        return;
    }
    const RowRunLane lane = lanes[run.lane_offset + lane_index];
    const uint output_index = run.lane_offset + lane_index;
    const float u_lambda = run.u_m * lane.lambda_scale;
    const float v_lambda = run.v_m * lane.lambda_scale;

    float natural_weight;
    float2 visibility;
    if (run.polarization_mode == 0u) {
        if (run.corr0 >= run.corr_count) {
            return;
        }
        const uint index = run.corr0 * run.lane_count + lane_index;
        if (flags[run.flag_offset + index] != 0) {
            return;
        }
        visibility = data[run.data_offset + index];
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        natural_weight = weights[run.weight_offset + run.corr0];
    } else {
        if (run.corr0 >= run.corr_count || run.corr1 >= run.corr_count) {
            return;
        }
        const uint first_index = run.corr0 * run.lane_count + lane_index;
        const uint second_index = run.corr1 * run.lane_count + lane_index;
        if (flags[run.flag_offset + first_index] != 0 ||
            flags[run.flag_offset + second_index] != 0) {
            return;
        }
        const float2 first_visibility = data[run.data_offset + first_index];
        const float2 second_visibility = data[run.data_offset + second_index];
        visibility = row_run_collapse_pair(first_visibility, second_visibility, run.transform);
        if (!isfinite(visibility.x) || !isfinite(visibility.y)) {
            return;
        }
        const float first_weight = weights[run.weight_offset + run.corr0];
        const float second_weight = weights[run.weight_offset + run.corr1];
        if (!(isfinite(first_weight) && first_weight > 0.0f &&
              isfinite(second_weight) && second_weight > 0.0f)) {
            return;
        }
        natural_weight = 0.5f * (first_weight + second_weight);
    }
    if (!(isfinite(natural_weight) && natural_weight > 0.0f &&
          isfinite(run.sumwt_factor) && run.sumwt_factor > 0.0f)) {
        return;
    }

    float final_weight = natural_weight;
    if (params.weighting_mode != 0u) {
        float cell_density = 0.0f;
        if (!row_run_density_lookup(u_lambda, v_lambda, params, density, cell_density)) {
            return;
        }
        if (params.weighting_mode == 1u) {
            final_weight = natural_weight / cell_density;
        } else {
            final_weight = natural_weight / (params.briggs_f2 * cell_density + 1.0f);
        }
    }
    const float grid_weight = final_weight * run.sumwt_factor;
    if (!(isfinite(grid_weight) && grid_weight > 0.0f)) {
        return;
    }

    const float grid_x = lane.grid_x;
    const float grid_y = lane.grid_y;
    if (!isfinite(grid_x) || !isfinite(grid_y)) {
        return;
    }
    const int center_x = round_half_away_from_zero(grid_x);
    const int center_y = round_half_away_from_zero(grid_y);
    const int start_x = center_x - STANDARD_MFS_SUPPORT;
    const int start_y = center_y - STANDARD_MFS_SUPPORT;
    if (start_x < 0 || start_y < 0 ||
        center_x + STANDARD_MFS_SUPPORT >= int(params.grid_width) ||
        center_y + STANDARD_MFS_SUPPORT >= int(params.grid_height)) {
        return;
    }
    const int offset_x = round_half_away_from_zero((float(center_x) - grid_x) * float(params.oversampling));
    const int offset_y = round_half_away_from_zero((float(center_y) - grid_y) * float(params.oversampling));
    const int half_oversampling = int(params.oversampling / 2u);
    const int x_weight_index = offset_x + half_oversampling;
    const int y_weight_index = offset_y + half_oversampling;
    if (x_weight_index < 0 || y_weight_index < 0 ||
        x_weight_index >= int(params.tap_weight_count) ||
        y_weight_index >= int(params.tap_weight_count)) {
        return;
    }
    const uint x_weight_base = uint(x_weight_index) * STANDARD_MFS_TAP_COUNT;
    const uint y_weight_base = uint(y_weight_index) * STANDARD_MFS_TAP_COUNT;

    if (params.diagnostic_mode == 4u) {
        diagnostic_output[output_index] = as_type<uint>(grid_x + grid_y + float(x_weight_base + y_weight_base));
        return;
    }

    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    if (params.diagnostic_mode == 1u) {
        for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
            int x = start_x + int(dx);
            float wx = tap_weights[x_weight_base + dx];
            for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
                int y = start_y + int(dy);
                float tap_weight = wx * tap_weights[y_weight_base + dy];
                uint cell = uint(x) * params.grid_height + uint(y);
                predicted_re += model_re[cell] * tap_weight;
                predicted_im += model_im[cell] * tap_weight;
            }
        }
        diagnostic_output[output_index] = as_type<uint>(predicted_re + predicted_im);
        return;
    }

    if (params.diagnostic_mode == 3u) {
        const uint center_cell = uint(center_x) * params.grid_height + uint(center_y);
        const float tap_weight =
            tap_weights[x_weight_base + STANDARD_MFS_SUPPORT] *
            tap_weights[y_weight_base + STANDARD_MFS_SUPPORT] *
            grid_weight;
        atomic_add_float(&grid_re[center_cell], visibility.x * tap_weight);
        atomic_add_float(&grid_im[center_cell], visibility.y * tap_weight);
        return;
    }

    const float residual_re = visibility.x;
    const float residual_im = visibility.y;
    for (uint dx = 0; dx < STANDARD_MFS_TAP_COUNT; dx++) {
        int x = start_x + int(dx);
        float wx = tap_weights[x_weight_base + dx];
        for (uint dy = 0; dy < STANDARD_MFS_TAP_COUNT; dy++) {
            int y = start_y + int(dy);
            float tap_weight = wx * tap_weights[y_weight_base + dy] * grid_weight;
            uint cell = uint(x) * params.grid_height + uint(y);
            atomic_add_float(&grid_re[cell], residual_re * tap_weight);
            atomic_add_float(&grid_im[cell], residual_im * tap_weight);
        }
    }
}
"#;

struct PsfTileBuffer {
    id: StandardMfsTileId,
    psf_grid: Array2<Complex64>,
}

struct PsfTileCache<'a, 'g> {
    partition: &'a StandardMfsFixedTilePartition,
    resident_limit: usize,
    global_psf_grid: &'g mut Array2<Complex64>,
    buffers: BTreeMap<StandardMfsTileId, PsfTileBuffer>,
    flushed_tiles: usize,
    evicted_tiles: usize,
}

impl<'a, 'g> PsfTileCache<'a, 'g> {
    fn new(
        partition: &'a StandardMfsFixedTilePartition,
        resident_limit: usize,
        global_psf_grid: &'g mut Array2<Complex64>,
    ) -> Self {
        Self {
            partition,
            resident_limit,
            global_psf_grid,
            buffers: BTreeMap::new(),
            flushed_tiles: 0,
            evicted_tiles: 0,
        }
    }

    fn acquire(&mut self, id: StandardMfsTileId) -> Result<&mut PsfTileBuffer, ImagingError> {
        if !self.buffers.contains_key(&id) {
            while self.buffers.len() >= self.resident_limit {
                self.flush_first();
                self.evicted_tiles += 1;
            }
            let tile = self.partition.tile(id).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile id {} is out of range",
                    id.index()
                ))
            })?;
            self.buffers.insert(
                id,
                PsfTileBuffer {
                    id,
                    psf_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
                },
            );
        }
        self.buffers.get_mut(&id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} was not resident after acquire",
                id.index()
            ))
        })
    }

    fn flush_first(&mut self) {
        if let Some((id, buffer)) = self.buffers.pop_first() {
            debug_assert_eq!(id, buffer.id);
            flush_psf_tile(self.partition, buffer, self.global_psf_grid);
            self.flushed_tiles += 1;
        }
    }

    fn flush_all(&mut self) -> usize {
        let before = self.flushed_tiles;
        while !self.buffers.is_empty() {
            self.flush_first();
        }
        self.flushed_tiles - before
    }

    fn flushed_tiles(&self) -> usize {
        self.flushed_tiles
    }

    fn evicted_tiles(&self) -> usize {
        self.evicted_tiles
    }
}

struct DirectPsfTileStore<'a> {
    partition: &'a StandardMfsFixedTilePartition,
    buffers: Vec<Mutex<Option<PsfTileBuffer>>>,
}

impl<'a> DirectPsfTileStore<'a> {
    fn new(partition: &'a StandardMfsFixedTilePartition) -> Self {
        Self {
            partition,
            buffers: (0..partition.tile_count())
                .map(|_| Mutex::new(None))
                .collect(),
        }
    }

    fn lock_tile(
        &self,
        id: StandardMfsTileId,
    ) -> Result<(MutexGuard<'_, Option<PsfTileBuffer>>, Duration), ImagingError> {
        let tile = self.partition.tile(id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                id.index()
            ))
        })?;
        let mut guard = self.buffers[id.index()].lock().map_err(|_| {
            ImagingError::InvalidRequest(format!(
                "standard MFS direct PSF tile {} lock was poisoned",
                id.index()
            ))
        })?;
        let alloc_started = profile::maybe_profile_now();
        if guard.is_none() {
            *guard = Some(PsfTileBuffer {
                id,
                psf_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
            });
            Ok((guard, profile::elapsed_since(alloc_started)))
        } else {
            Ok((guard, Duration::ZERO))
        }
    }

    fn flush_all(&self, global_psf_grid: &mut Array2<Complex64>) -> Result<usize, ImagingError> {
        let mut flushed_tiles = 0usize;
        for buffer in &self.buffers {
            let mut guard = buffer.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS direct PSF tile lock was poisoned during flush".to_string(),
                )
            })?;
            if let Some(buffer) = guard.take() {
                flush_psf_tile(self.partition, buffer, global_psf_grid);
                flushed_tiles += 1;
            }
        }
        Ok(flushed_tiles)
    }
}

struct DirtyTileBuffer {
    id: StandardMfsTileId,
    psf_grid: Array2<Complex64>,
    residual_grid: Array2<Complex64>,
}

struct DirtyTileCache<'a, 'g> {
    partition: &'a StandardMfsFixedTilePartition,
    resident_limit: usize,
    global_psf_grid: &'g mut Array2<Complex64>,
    global_residual_grid: &'g mut Array2<Complex64>,
    buffers: BTreeMap<StandardMfsTileId, DirtyTileBuffer>,
    flushed_tiles: usize,
    evicted_tiles: usize,
}

impl<'a, 'g> DirtyTileCache<'a, 'g> {
    fn new(
        partition: &'a StandardMfsFixedTilePartition,
        resident_limit: usize,
        global_psf_grid: &'g mut Array2<Complex64>,
        global_residual_grid: &'g mut Array2<Complex64>,
    ) -> Self {
        Self {
            partition,
            resident_limit,
            global_psf_grid,
            global_residual_grid,
            buffers: BTreeMap::new(),
            flushed_tiles: 0,
            evicted_tiles: 0,
        }
    }

    fn acquire(&mut self, id: StandardMfsTileId) -> Result<&mut DirtyTileBuffer, ImagingError> {
        if !self.buffers.contains_key(&id) {
            while self.buffers.len() >= self.resident_limit {
                self.flush_first();
                self.evicted_tiles += 1;
            }
            let tile = self.partition.tile(id).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile id {} is out of range",
                    id.index()
                ))
            })?;
            let shape = (tile.halo.width(), tile.halo.height());
            self.buffers.insert(
                id,
                DirtyTileBuffer {
                    id,
                    psf_grid: Array2::zeros(shape),
                    residual_grid: Array2::zeros(shape),
                },
            );
        }
        self.buffers.get_mut(&id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} was not resident after acquire",
                id.index()
            ))
        })
    }

    fn flush_first(&mut self) {
        if let Some((id, buffer)) = self.buffers.pop_first() {
            debug_assert_eq!(id, buffer.id);
            flush_dirty_tile(
                self.partition,
                buffer,
                self.global_psf_grid,
                self.global_residual_grid,
            );
            self.flushed_tiles += 1;
        }
    }

    fn flush_all(&mut self) -> usize {
        let before = self.flushed_tiles;
        while !self.buffers.is_empty() {
            self.flush_first();
        }
        self.flushed_tiles - before
    }

    fn flushed_tiles(&self) -> usize {
        self.flushed_tiles
    }

    fn evicted_tiles(&self) -> usize {
        self.evicted_tiles
    }
}

struct DirectDirtyTileStore<'a> {
    partition: &'a StandardMfsFixedTilePartition,
    buffers: Vec<Mutex<Option<DirtyTileBuffer>>>,
}

impl<'a> DirectDirtyTileStore<'a> {
    fn new(partition: &'a StandardMfsFixedTilePartition) -> Self {
        Self {
            partition,
            buffers: (0..partition.tile_count())
                .map(|_| Mutex::new(None))
                .collect(),
        }
    }

    fn lock_tile(
        &self,
        id: StandardMfsTileId,
    ) -> Result<(MutexGuard<'_, Option<DirtyTileBuffer>>, Duration), ImagingError> {
        let tile = self.partition.tile(id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                id.index()
            ))
        })?;
        let mut guard = self.buffers[id.index()].lock().map_err(|_| {
            ImagingError::InvalidRequest(format!(
                "standard MFS direct dirty tile {} lock was poisoned",
                id.index()
            ))
        })?;
        let alloc_started = profile::maybe_profile_now();
        if guard.is_none() {
            let shape = (tile.halo.width(), tile.halo.height());
            *guard = Some(DirtyTileBuffer {
                id,
                psf_grid: Array2::zeros(shape),
                residual_grid: Array2::zeros(shape),
            });
            Ok((guard, profile::elapsed_since(alloc_started)))
        } else {
            Ok((guard, Duration::ZERO))
        }
    }

    fn flush_all(
        &self,
        global_psf_grid: &mut Array2<Complex64>,
        global_residual_grid: &mut Array2<Complex64>,
    ) -> Result<usize, ImagingError> {
        let mut flushed_tiles = 0usize;
        for buffer in &self.buffers {
            let mut guard = buffer.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS direct dirty tile lock was poisoned during flush".to_string(),
                )
            })?;
            if let Some(buffer) = guard.take() {
                flush_dirty_tile(
                    self.partition,
                    buffer,
                    global_psf_grid,
                    global_residual_grid,
                );
                flushed_tiles += 1;
            }
        }
        Ok(flushed_tiles)
    }
}

struct ResidualTileBuffer {
    id: StandardMfsTileId,
    residual_grid: Array2<Complex64>,
}

struct ResidualTileCache<'a, 'g> {
    partition: &'a StandardMfsFixedTilePartition,
    resident_limit: usize,
    global_residual_grid: &'g mut Array2<Complex64>,
    buffers: BTreeMap<StandardMfsTileId, ResidualTileBuffer>,
    flushed_tiles: usize,
    evicted_tiles: usize,
}

impl<'a, 'g> ResidualTileCache<'a, 'g> {
    fn new(
        partition: &'a StandardMfsFixedTilePartition,
        resident_limit: usize,
        global_residual_grid: &'g mut Array2<Complex64>,
    ) -> Self {
        Self {
            partition,
            resident_limit,
            global_residual_grid,
            buffers: BTreeMap::new(),
            flushed_tiles: 0,
            evicted_tiles: 0,
        }
    }

    fn acquire(&mut self, id: StandardMfsTileId) -> Result<&mut ResidualTileBuffer, ImagingError> {
        if !self.buffers.contains_key(&id) {
            while self.buffers.len() >= self.resident_limit {
                self.flush_first();
                self.evicted_tiles += 1;
            }
            let tile = self.partition.tile(id).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "standard MFS tile id {} is out of range",
                    id.index()
                ))
            })?;
            self.buffers.insert(
                id,
                ResidualTileBuffer {
                    id,
                    residual_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
                },
            );
        }
        self.buffers.get_mut(&id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} was not resident after acquire",
                id.index()
            ))
        })
    }

    fn flush_first(&mut self) {
        if let Some((id, buffer)) = self.buffers.pop_first() {
            debug_assert_eq!(id, buffer.id);
            flush_residual_tile(self.partition, buffer, self.global_residual_grid);
            self.flushed_tiles += 1;
        }
    }

    fn flush_all(&mut self) -> usize {
        let before = self.flushed_tiles;
        while !self.buffers.is_empty() {
            self.flush_first();
        }
        self.flushed_tiles - before
    }

    fn flushed_tiles(&self) -> usize {
        self.flushed_tiles
    }

    fn evicted_tiles(&self) -> usize {
        self.evicted_tiles
    }
}

struct DirectResidualTileStore<'a> {
    partition: &'a StandardMfsFixedTilePartition,
    buffers: Vec<Mutex<Option<ResidualTileBuffer>>>,
}

impl<'a> DirectResidualTileStore<'a> {
    fn new(partition: &'a StandardMfsFixedTilePartition) -> Self {
        Self {
            partition,
            buffers: (0..partition.tile_count())
                .map(|_| Mutex::new(None))
                .collect(),
        }
    }

    fn lock_tile(
        &self,
        id: StandardMfsTileId,
    ) -> Result<(MutexGuard<'_, Option<ResidualTileBuffer>>, Duration), ImagingError> {
        let tile = self.partition.tile(id).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                id.index()
            ))
        })?;
        let mut guard = self.buffers[id.index()].lock().map_err(|_| {
            ImagingError::InvalidRequest(format!(
                "standard MFS direct residual tile {} lock was poisoned",
                id.index()
            ))
        })?;
        let alloc_started = profile::maybe_profile_now();
        if guard.is_none() {
            *guard = Some(ResidualTileBuffer {
                id,
                residual_grid: Array2::zeros((tile.halo.width(), tile.halo.height())),
            });
            Ok((guard, profile::elapsed_since(alloc_started)))
        } else {
            Ok((guard, Duration::ZERO))
        }
    }

    fn flush_all(
        &self,
        global_residual_grid: &mut Array2<Complex64>,
    ) -> Result<usize, ImagingError> {
        let mut flushed_tiles = 0usize;
        for buffer in &self.buffers {
            let mut guard = buffer.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS direct residual tile lock was poisoned during flush".to_string(),
                )
            })?;
            if let Some(buffer) = guard.take() {
                flush_residual_tile(self.partition, buffer, global_residual_grid);
                flushed_tiles += 1;
            }
        }
        Ok(flushed_tiles)
    }
}

fn tile_offset(tile: &StandardMfsFixedTile) -> [usize; 2] {
    [tile.halo.x0, tile.halo.y0]
}

fn flush_dirty_tile(
    partition: &StandardMfsFixedTilePartition,
    buffer: DirtyTileBuffer,
    global_psf_grid: &mut Array2<Complex64>,
    global_residual_grid: &mut Array2<Complex64>,
) {
    let tile = partition
        .tile(buffer.id)
        .expect("resident tile id should be in partition");
    add_tile_grid(tile, &buffer.psf_grid, global_psf_grid);
    add_tile_grid(tile, &buffer.residual_grid, global_residual_grid);
}

fn flush_psf_tile(
    partition: &StandardMfsFixedTilePartition,
    buffer: PsfTileBuffer,
    global_psf_grid: &mut Array2<Complex64>,
) {
    let tile = partition
        .tile(buffer.id)
        .expect("resident tile id should be in partition");
    add_tile_grid(tile, &buffer.psf_grid, global_psf_grid);
}

fn flush_residual_tile(
    partition: &StandardMfsFixedTilePartition,
    buffer: ResidualTileBuffer,
    global_residual_grid: &mut Array2<Complex64>,
) {
    let tile = partition
        .tile(buffer.id)
        .expect("resident tile id should be in partition");
    add_tile_grid(tile, &buffer.residual_grid, global_residual_grid);
}

fn add_tile_grid(
    tile: &StandardMfsFixedTile,
    tile_grid: &Array2<Complex64>,
    global_grid: &mut Array2<Complex64>,
) {
    for global_x in tile.halo.x0..tile.halo.x1 {
        let local_x = global_x - tile.halo.x0;
        for global_y in tile.halo.y0..tile.halo.y1 {
            let local_y = global_y - tile.halo.y0;
            global_grid[(global_x, global_y)] += tile_grid[(local_x, local_y)];
        }
    }
}

fn merge_dirty_tile_buffer_into_cache(
    cache: &mut DirtyTileCache<'_, '_>,
    buffer: DirtyTileBuffer,
) -> Result<(), ImagingError> {
    let target = cache.acquire(buffer.id)?;
    add_same_shape_grid(&buffer.psf_grid, &mut target.psf_grid);
    add_same_shape_grid(&buffer.residual_grid, &mut target.residual_grid);
    Ok(())
}

fn merge_psf_tile_buffer_into_cache(
    cache: &mut PsfTileCache<'_, '_>,
    buffer: PsfTileBuffer,
) -> Result<(), ImagingError> {
    let target = cache.acquire(buffer.id)?;
    add_same_shape_grid(&buffer.psf_grid, &mut target.psf_grid);
    Ok(())
}

fn merge_residual_tile_buffer_into_cache(
    cache: &mut ResidualTileCache<'_, '_>,
    buffer: ResidualTileBuffer,
) -> Result<(), ImagingError> {
    let target = cache.acquire(buffer.id)?;
    add_same_shape_grid(&buffer.residual_grid, &mut target.residual_grid);
    Ok(())
}

fn add_same_shape_grid(source: &Array2<Complex64>, target: &mut Array2<Complex64>) {
    debug_assert_eq!(source.raw_dim(), target.raw_dim());
    for (target_cell, source_cell) in target.iter_mut().zip(source.iter()) {
        *target_cell += *source_cell;
    }
}

fn tiled_scheduler_block_profile(
    input: StandardMfsTileSchedulerBlockInputs<'_>,
) -> StandardMfsTileSchedulerBlockProfile {
    StandardMfsTileSchedulerBlockProfile {
        requested_threads: standard_mfs_grid_threads(),
        actual_threads: input.worker_count,
        task_count: input.tasks.len(),
        sample_count: input
            .tasks
            .iter()
            .map(|task| task.sample_count)
            .sum::<usize>(),
        tap_visits: input
            .tasks
            .iter()
            .map(|task| task.estimated_tap_visits)
            .sum::<usize>(),
        largest_task_samples: input
            .tasks
            .iter()
            .map(|task| task.sample_count)
            .max()
            .unwrap_or(0),
        largest_task_tap_visits: input
            .tasks
            .iter()
            .map(|task| task.estimated_tap_visits)
            .max()
            .unwrap_or(0),
        bucket_bytes: input.buckets.estimated_bytes(),
        bucket_build: input.bucket_build,
        local_alloc_zero: input.task_timing.local_alloc_zero,
        worker_replan_grid: input.task_timing.worker_replan_grid,
        block_wall: input.block_wall,
        merge: input.merge,
        merged_tiles: input.merged_tiles,
        worker_profiles: input.worker_profiles,
    }
}

fn serial_tile_worker_profiles(
    tasks: &[StandardMfsTileTask],
    task_timing: StandardMfsTileTaskTiming,
    block_wall: Duration,
) -> Vec<StandardMfsTileWorkerProfile> {
    vec![StandardMfsTileWorkerProfile {
        task_count: tasks.len(),
        sample_count: tasks.iter().map(|task| task.sample_count).sum(),
        tap_visits: tasks.iter().map(|task| task.estimated_tap_visits).sum(),
        active: task_timing.active(),
        elapsed: block_wall,
    }]
}

fn log_tiled_scheduler_block(stage: &str, block: &StandardMfsTileSchedulerBlockProfile) {
    let worker_task_counts = block
        .worker_profiles
        .iter()
        .map(|worker| worker.task_count)
        .collect::<Vec<_>>();
    let worker_sample_counts = block
        .worker_profiles
        .iter()
        .map(|worker| worker.sample_count)
        .collect::<Vec<_>>();
    let worker_tap_visits = block
        .worker_profiles
        .iter()
        .map(|worker| worker.tap_visits)
        .collect::<Vec<_>>();
    let worker_tap_visits_per_s = block
        .worker_profiles
        .iter()
        .map(|worker| per_second_or_zero(worker.tap_visits, worker.active))
        .collect::<Vec<_>>();
    let worker_samples_per_s = block
        .worker_profiles
        .iter()
        .map(|worker| per_second_or_zero(worker.sample_count, worker.active))
        .collect::<Vec<_>>();
    let worker_active = block
        .worker_profiles
        .iter()
        .map(|worker| worker.active)
        .collect::<Vec<_>>();
    let worker_elapsed = block
        .worker_profiles
        .iter()
        .map(|worker| worker.elapsed)
        .collect::<Vec<_>>();
    let worker_active_total_ms = duration_total_ms(&worker_active);
    let worker_capacity_ms = profile::millis(block.block_wall) * block.actual_threads.max(1) as f64;
    let worker_utilization_pct = percent_or_zero(worker_active_total_ms, worker_capacity_ms);
    let worker_tail_idle_ms = (worker_capacity_ms - worker_active_total_ms).max(0.0);
    let block_tap_visits_per_s = per_second_or_zero(block.tap_visits, block.block_wall);
    let block_samples_per_s = per_second_or_zero(block.sample_count, block.block_wall);
    eprintln!(
        "standard_mfs_tile_scheduler_block stage={} requested_threads={} actual_threads={} max_live_row_blocks=1 task_count={} samples={} tap_visits={} largest_task_samples={} largest_task_tap_visits={} bucket_bytes={} bucket_build_ms={:.3} local_alloc_zero_ms={:.3} worker_replan_grid_ms={:.3} block_wall_ms={:.3} merge_ms={:.3} merged_tiles={} worker_task_count={} worker_samples={} worker_tap_visits={} worker_tap_visits_per_s={} worker_samples_per_s={} worker_active_total_ms={:.3} worker_active={} worker_elapsed={} worker_capacity_ms={:.3} worker_utilization_pct={:.3} worker_tail_idle_ms={:.3} block_tap_visits_per_s={:.3} block_samples_per_s={:.3} active_tile_wait_events=0 tasks_skipped_due_to_active_tile=0",
        stage,
        block.requested_threads,
        block.actual_threads,
        block.task_count,
        block.sample_count,
        block.tap_visits,
        block.largest_task_samples,
        block.largest_task_tap_visits,
        block.bucket_bytes,
        profile::millis(block.bucket_build),
        profile::millis(block.local_alloc_zero),
        profile::millis(block.worker_replan_grid),
        profile::millis(block.block_wall),
        profile::millis(block.merge),
        block.merged_tiles,
        stats_triplet(&worker_task_counts),
        stats_triplet(&worker_sample_counts),
        stats_triplet(&worker_tap_visits),
        f64_stats_triplet(&worker_tap_visits_per_s, "per_s"),
        f64_stats_triplet(&worker_samples_per_s, "per_s"),
        worker_active_total_ms,
        duration_stats_triplet(&worker_active),
        duration_stats_triplet(&worker_elapsed),
        worker_capacity_ms,
        worker_utilization_pct,
        worker_tail_idle_ms,
        block_tap_visits_per_s,
        block_samples_per_s,
    );
}

impl<'a> StandardMfsCpuExecutor<'a> {
    /// Build a CPU executor over weighted standard-gridder visibility batches.
    pub(crate) fn new(
        gridder: &'a StandardGridder,
        batches: &'a [VisibilityBatch],
    ) -> Result<Self, ImagingError> {
        Self::for_backend(StandardMfsBackend::Cpu, gridder, batches)
    }

    /// Build an executor for a requested internal backend.
    pub(crate) fn for_backend(
        backend: StandardMfsBackend,
        gridder: &'a StandardGridder,
        batches: &'a [VisibilityBatch],
    ) -> Result<Self, ImagingError> {
        match backend {
            StandardMfsBackend::Cpu => Ok(Self {
                gridder,
                plan: StandardMfsVisibilityPlan::new(gridder, batches),
                workspace: StandardMfsWorkspace::new(gridder),
            }),
            StandardMfsBackend::Metal => Err(unsupported_standard_mfs_backend("metal")),
            StandardMfsBackend::Reserved(name) => Err(unsupported_standard_mfs_backend(name)),
        }
    }

    /// Borrow the executor's gridder, visibility plan, and mutable workspace.
    pub(crate) fn parts_mut(
        &mut self,
    ) -> (
        &'a StandardGridder,
        &StandardMfsVisibilityPlan,
        &mut StandardMfsWorkspace,
    ) {
        (self.gridder, &self.plan, &mut self.workspace)
    }

    /// Return the prepared visibility plan.
    #[cfg(test)]
    pub(crate) fn plan(&self) -> &StandardMfsVisibilityPlan {
        &self.plan
    }
}

impl StandardMfsDirtyCpuExecutor {
    /// Build a dirty accumulator executor for the requested internal backend.
    pub(crate) fn for_backend(
        backend: StandardMfsBackend,
        geometry: ImageGeometry,
    ) -> Result<Self, ImagingError> {
        match backend {
            StandardMfsBackend::Cpu => {
                let gridder = StandardGridder::new(geometry)?;
                let workspace = StandardMfsWorkspace::new(&gridder);
                Ok(Self {
                    gridder,
                    workspace,
                    normalization_sumwt: 0.0,
                    reported_sumwt: 0.0,
                    gridded_samples: 0,
                    skipped_samples: 0,
                    max_abs_w_lambda: 0.0,
                })
            }
            StandardMfsBackend::Metal => Err(unsupported_standard_mfs_backend("metal")),
            StandardMfsBackend::Reserved(name) => Err(unsupported_standard_mfs_backend(name)),
        }
    }

    /// Build the current CPU dirty executor.
    pub(crate) fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Self::for_backend(StandardMfsBackend::Cpu, geometry)
    }

    /// Accumulate a borrowed row-block plan into the reusable dirty grids.
    pub(crate) fn accumulate_batches(
        &mut self,
        batches: &[VisibilityBatch],
    ) -> Result<(), ImagingError> {
        for batch in batches {
            batch.validate()?;
            self.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(self.max_abs_w_lambda, |max_abs_w_lambda, &w_lambda| {
                    max_abs_w_lambda.max(w_lambda.abs())
                });
        }

        maybe_probe_standard_mfs_tile_buckets(&self.gridder, batches)?;

        let gridder = &self.gridder;
        let (psf_grid, residual_grid) = self.workspace.dirty_grids_mut();
        let mut census = StandardMfsTapCensus::new("streaming_dirty_accumulate");
        for batch in batches {
            for sample_index in 0..batch.len() {
                if !batch.gridable[sample_index] {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::NotGridable);
                    }
                    continue;
                }
                let weight = batch.weight[sample_index];
                let sumwt_factor = batch.sumwt_factor[sample_index];
                if !(weight.is_finite()
                    && weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0)
                {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        if !(weight.is_finite() && weight > 0.0) {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidWeight);
                        } else {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidSumwt);
                        }
                    }
                    continue;
                }
                let Some(plan) = gridder
                    .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                else {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::OutOfGrid);
                    }
                    continue;
                };
                if let Some(census) = census.as_mut() {
                    census.observe_accepted(&plan);
                }
                let grid_weight = weight * sumwt_factor;
                let sumwt = f64::from(grid_weight);
                self.normalization_sumwt += sumwt;
                self.reported_sumwt += sumwt;
                self.gridded_samples += 1;

                let observed_visibility = batch.visibility[sample_index];
                if finite_visibility(observed_visibility) {
                    let residual = Complex64::new(
                        f64::from(observed_visibility.re) * sumwt,
                        f64::from(observed_visibility.im) * sumwt,
                    );
                    gridder.grid_sample_taps_real_complex_pair_planned_f64(
                        psf_grid,
                        sumwt,
                        residual_grid,
                        residual,
                        &plan,
                    );
                } else {
                    gridder.grid_sample_taps_real_planned_f64(psf_grid, &plan, sumwt);
                }
            }
        }
        if let Some(census) = census {
            census.log(std::mem::size_of::<StandardMfsPlannedSample>());
        }
        Ok(())
    }

    /// Return the reusable standard gridder.
    pub(crate) fn gridder(&self) -> &StandardGridder {
        &self.gridder
    }

    /// Return the accumulated dirty PSF and residual grids.
    pub(crate) fn dirty_grids(&self) -> (&Array2<Complex64>, &Array2<Complex64>) {
        self.workspace.dirty_grids()
    }

    /// Return the accumulated sample summary.
    pub(crate) fn accumulation(&self) -> StandardMfsDirtyAccumulation {
        StandardMfsDirtyAccumulation {
            normalization_sumwt: self.normalization_sumwt,
            reported_sumwt: self.reported_sumwt,
            gridded_samples: self.gridded_samples,
            skipped_samples: self.skipped_samples,
            max_abs_w_lambda: self.max_abs_w_lambda,
        }
    }
}

/// Compact planned view of weighted standard-gridder visibility samples.
pub(crate) struct StandardMfsVisibilityPlan {
    samples: Vec<StandardMfsPlannedSample>,
    normalization_sumwt: f64,
    reported_sumwt: f64,
    skipped_samples: usize,
}

impl StandardMfsVisibilityPlan {
    fn new(gridder: &StandardGridder, batches: &[VisibilityBatch]) -> Self {
        let plan_started = profile::maybe_profile_now();
        let sample_count = batches.iter().map(VisibilityBatch::len).sum();
        let mut samples = Vec::with_capacity(sample_count);
        let mut normalization_sumwt = 0.0f64;
        let mut reported_sumwt = 0.0f64;
        let mut skipped_samples = 0usize;
        let mut census = StandardMfsTapCensus::new("visibility_plan");

        for batch in batches {
            for sample_index in 0..batch.len() {
                if !batch.gridable[sample_index] {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::NotGridable);
                    }
                    continue;
                }
                let weight = batch.weight[sample_index];
                let sumwt_factor = batch.sumwt_factor[sample_index];
                if !(weight.is_finite()
                    && weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0)
                {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        if !(weight.is_finite() && weight > 0.0) {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidWeight);
                        } else {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidSumwt);
                        }
                    }
                    continue;
                }
                let Some(positive_taps) = gridder
                    .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                else {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::OutOfGrid);
                    }
                    continue;
                };
                if let Some(census) = census.as_mut() {
                    census.observe_accepted(&positive_taps);
                }
                let grid_weight = weight * sumwt_factor;
                let sumwt = f64::from(grid_weight);
                normalization_sumwt += sumwt;
                reported_sumwt += sumwt;
                samples.push(StandardMfsPlannedSample {
                    visibility: batch.visibility[sample_index],
                    grid_weight,
                    positive_taps,
                });
            }
        }
        if let Some(census) = census {
            census.log(std::mem::size_of::<StandardMfsPlannedSample>());
        }
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_executor_plan stage=build input_samples={} accepted_samples={} skipped_samples={} sample_plan_bytes={} build_ms={:.3}",
                sample_count,
                samples.len(),
                skipped_samples,
                samples
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StandardMfsPlannedSample>()),
                profile::millis(profile::elapsed_since(plan_started)),
            );
        }

        Self {
            samples,
            normalization_sumwt,
            reported_sumwt,
            skipped_samples,
        }
    }

    /// Return the planned samples accepted by the standard gridder.
    pub(crate) fn samples(&self) -> &[StandardMfsPlannedSample] {
        &self.samples
    }

    /// Return the PSF and residual normalization sum of accepted samples.
    pub(crate) fn normalization_sumwt(&self) -> f64 {
        self.normalization_sumwt
    }

    /// Return the CASA-style reported sumwt of accepted samples.
    pub(crate) fn reported_sumwt(&self) -> f64 {
        self.reported_sumwt
    }

    /// Return the number of samples accepted by the gridder plan.
    pub(crate) fn gridded_samples(&self) -> usize {
        self.samples.len()
    }

    /// Return the number of samples rejected while building the plan.
    pub(crate) fn skipped_samples(&self) -> usize {
        self.skipped_samples
    }

    /// Return approximate heap bytes owned by the compact sample plan.
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.samples
            .capacity()
            .saturating_mul(std::mem::size_of::<StandardMfsPlannedSample>())
    }
}

/// One planned standard MFS visibility sample.
pub(crate) struct StandardMfsPlannedSample {
    /// Weighted source visibility for this planned grid sample.
    pub(crate) visibility: Complex32,
    /// Product of imaging weight and sumwt factor used by standard MFS grids.
    pub(crate) grid_weight: f32,
    /// Precomputed positive-UV gridder taps for this `(u, v)` coordinate.
    pub(crate) positive_taps: PositiveTapSet,
}

impl StandardMfsPlannedSample {
    /// Return the grid weight in double precision for f64 grid accumulation.
    pub(crate) fn grid_weight_f64(&self) -> f64 {
        f64::from(self.grid_weight)
    }
}

/// Reusable f64 grid workspace for standard MFS CPU execution.
pub(crate) struct StandardMfsWorkspace {
    psf_grid: Array2<Complex64>,
    residual_grid: Array2<Complex64>,
}

impl StandardMfsWorkspace {
    fn new(gridder: &StandardGridder) -> Self {
        let [nx, ny] = gridder.grid_shape();
        Self {
            psf_grid: Array2::zeros((nx, ny)),
            residual_grid: Array2::zeros((nx, ny)),
        }
    }

    /// Clear and borrow the PSF grid.
    pub(crate) fn clear_psf_grid(&mut self) -> &mut Array2<Complex64> {
        self.psf_grid.fill(Complex64::new(0.0, 0.0));
        &mut self.psf_grid
    }

    /// Clear and borrow the residual grid.
    pub(crate) fn clear_residual_grid(&mut self) -> &mut Array2<Complex64> {
        self.residual_grid.fill(Complex64::new(0.0, 0.0));
        &mut self.residual_grid
    }

    /// Clear and borrow the PSF and residual grids for a combined dirty pass.
    pub(crate) fn clear_dirty_grids(&mut self) -> (&mut Array2<Complex64>, &mut Array2<Complex64>) {
        self.psf_grid.fill(Complex64::new(0.0, 0.0));
        self.residual_grid.fill(Complex64::new(0.0, 0.0));
        (&mut self.psf_grid, &mut self.residual_grid)
    }

    /// Borrow the PSF and residual grids without clearing them.
    pub(crate) fn dirty_grids_mut(&mut self) -> (&mut Array2<Complex64>, &mut Array2<Complex64>) {
        (&mut self.psf_grid, &mut self.residual_grid)
    }

    /// Borrow the PSF and residual grids without clearing them.
    pub(crate) fn dirty_grids(&self) -> (&Array2<Complex64>, &Array2<Complex64>) {
        (&self.psf_grid, &self.residual_grid)
    }
}

/// Return true when a scalar visibility can contribute to a residual grid.
pub(crate) fn finite_visibility(visibility: Complex32) -> bool {
    visibility.re.is_finite() && visibility.im.is_finite()
}

#[cfg(test)]
mod tests {
    #[cfg(all(target_os = "macos", not(coverage)))]
    use super::METAL_DIRTY_SHADER;
    use super::{
        STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY, STANDARD_MFS_TILE_FLAG_PSF_ONLY,
        StandardMfsBackend, StandardMfsBlockTileBuckets, StandardMfsCpuExecutor,
        StandardMfsDirtyCpuExecutor, StandardMfsFixedTilePartition,
        StandardMfsRowBlockSampleAccess, StandardMfsSampleRef, StandardMfsTileId,
        StandardMfsTiledCpuExecutor,
    };
    use crate::{
        ImageGeometry, StandardMfsExecutionConfig, StandardMfsMinorCycleBackend,
        StandardMfsPlannedWeightedSample, StandardMfsRoutedVisibilityRow,
        StandardMfsRoutedVisibilityRun, StandardMfsVisibilityPolarization, VisibilityBatch,
        gridder::StandardGridder,
    };
    use num_complex::{Complex32, Complex64};
    use std::{
        ffi::OsString,
        mem::size_of,
        sync::{Arc, Mutex},
        time::Duration,
    };

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = &self.previous {
                    std::env::set_var(self.key, previous);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    #[cfg(all(target_os = "macos", not(coverage)))]
    fn metal_row_run_density_lookup_matches_cpu_cube_briggs_axes() {
        assert!(
            METAL_DIRTY_SHADER.contains(
                "x = -u * params.density_u_scale + params.density_center_x;\n        y = v * params.density_v_scale + params.density_center_y;"
            ),
            "VisImagingWeight density convention should preserve the legacy Metal sign convention"
        );
        assert!(
            METAL_DIRTY_SHADER.contains(
                "x = u_lambda * params.density_u_scale + params.density_center_x;\n        y = -v_lambda * params.density_v_scale + params.density_center_y;"
            ),
            "Cube Briggs density convention must match StandardGridder::weight_density_cell_anchor"
        );
    }

    #[test]
    fn standard_mfs_plan_buckets_gridder_accepted_samples() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let batches = vec![VisibilityBatch {
            u_lambda: vec![0.0, 4.0, 8.0, 12.0],
            v_lambda: vec![0.0, 1.0, 2.0, 3.0],
            w_lambda: vec![0.0; 4],
            weight: vec![1.0, 2.0, 0.0, 3.0],
            sumwt_factor: vec![1.0, 2.0, 1.0, f32::NAN],
            gridable: vec![true, true, true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 4],
        }];

        let executor = StandardMfsCpuExecutor::new(&gridder, &batches).unwrap();
        let plan = executor.plan();

        assert_eq!(plan.gridded_samples(), 2);
        assert_eq!(plan.skipped_samples(), 2);
        assert!((plan.normalization_sumwt() - 5.0).abs() < 1.0e-6);
        assert!((plan.reported_sumwt() - 5.0).abs() < 1.0e-6);
        assert_eq!(plan.samples()[0].visibility, Complex32::new(1.0, 0.0));
        assert_eq!(plan.samples()[1].visibility, Complex32::new(1.0, 0.0));
    }

    #[test]
    fn reserved_standard_mfs_backend_fails_before_execution() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let batches = Vec::new();
        let error = match StandardMfsCpuExecutor::for_backend(
            StandardMfsBackend::Metal,
            &gridder,
            &batches,
        ) {
            Ok(_) => panic!("reserved backend unexpectedly built an executor"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'metal' is not implemented"),
            "{error}"
        );
    }

    #[test]
    fn fixed_tile_partition_uses_half_open_ownership_and_full_halo() {
        let partition = StandardMfsFixedTilePartition::new([32, 32], [16, 16], 3).unwrap();

        assert_eq!(partition.tile_count(), 4);
        assert_eq!(partition.owner(0, 0), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(15, 15), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(16, 0), Some(StandardMfsTileId(2)));
        assert_eq!(partition.owner(0, 16), Some(StandardMfsTileId(1)));
        assert_eq!(partition.owner(16, 16), Some(StandardMfsTileId(3)));
        assert_eq!(partition.owner(31, 31), Some(StandardMfsTileId(3)));
        assert_eq!(partition.owner(32, 0), None);

        let lower_left = partition.tile(StandardMfsTileId(0)).unwrap();
        assert_eq!(lower_left.interior.x0, 0);
        assert_eq!(lower_left.interior.x1, 16);
        assert_eq!(lower_left.halo.x0, 0);
        assert_eq!(lower_left.halo.x1, 19);
        assert_eq!(lower_left.halo.y0, 0);
        assert_eq!(lower_left.halo.y1, 19);

        let upper_right = partition.tile(StandardMfsTileId(3)).unwrap();
        assert_eq!(upper_right.interior.x0, 16);
        assert_eq!(upper_right.interior.x1, 32);
        assert_eq!(upper_right.halo.x0, 13);
        assert_eq!(upper_right.halo.x1, 32);
        assert_eq!(upper_right.halo.y0, 13);
        assert_eq!(upper_right.halo.y1, 32);
        assert_eq!(
            partition.resident_tile_bytes(StandardMfsTileId(3), 2),
            Some(19 * 19 * 2 * std::mem::size_of::<num_complex::Complex64>())
        );
    }

    #[test]
    fn fixed_tile_partition_rejects_invalid_geometry_and_bounds() {
        let error = StandardMfsFixedTilePartition::new([0, 32], [16, 16], 3).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("standard MFS tile grid shape must be non-empty"),
            "{error}"
        );

        let error = StandardMfsFixedTilePartition::new([32, 32], [0, 16], 3).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("standard MFS tile shape must be non-empty"),
            "{error}"
        );

        let error =
            StandardMfsFixedTilePartition::new_with_origin([32, 32], [16, 16], 3, [32, 0], "test")
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("standard MFS tile origin must be inside the grid"),
            "{error}"
        );

        let error = StandardMfsFixedTilePartition::new_with_axis_bounds(
            [32, 32],
            [16, 16],
            3,
            [0, 0],
            "test",
            vec![1, 16, 32],
            vec![0, 16, 32],
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("standard MFS x-tile bounds must span the full grid"),
            "{error}"
        );

        let error = StandardMfsFixedTilePartition::new_with_axis_bounds(
            [32, 32],
            [16, 16],
            3,
            [0, 0],
            "test",
            vec![0, 16, 32],
            vec![0, 16, 16, 32],
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("standard MFS y-tile bounds must be strictly increasing"),
            "{error}"
        );
    }

    #[test]
    fn fixed_tile_partition_supports_irregular_axis_bounds_and_missing_tiles() {
        let partition = StandardMfsFixedTilePartition::new_with_axis_bounds(
            [32, 32],
            [16, 16],
            2,
            [0, 0],
            "irregular",
            vec![0, 7, 32],
            vec![0, 11, 32],
        )
        .unwrap();

        assert_eq!(partition.tile_count(), 4);
        assert_eq!(partition.tile_shape(), [16, 16]);
        assert_eq!(partition.tile_origin(), [0, 0]);
        assert_eq!(partition.anchor_label(), "irregular");
        assert_eq!(partition.owner(6, 10), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(7, 10), Some(StandardMfsTileId(2)));
        assert_eq!(partition.owner(6, 11), Some(StandardMfsTileId(1)));
        assert_eq!(partition.owner(31, 31), Some(StandardMfsTileId(3)));
        assert_eq!(partition.owner(32, 31), None);
        assert!(partition.tile(StandardMfsTileId(4)).is_none());
        assert_eq!(partition.resident_tile_bytes(StandardMfsTileId(4), 2), None);

        let upper_right = partition.tile(StandardMfsTileId(3)).unwrap();
        assert_eq!(upper_right.interior.x0, 7);
        assert_eq!(upper_right.interior.y0, 11);
        assert_eq!(upper_right.halo.x0, 5);
        assert_eq!(upper_right.halo.y0, 9);
        assert_eq!(upper_right.halo.x1, 32);
        assert_eq!(upper_right.halo.y1, 32);
        assert_eq!(
            partition.resident_tile_bytes(StandardMfsTileId(3), 3),
            Some(27 * 23 * 3 * size_of::<Complex64>())
        );
    }

    #[test]
    fn fixed_tile_partition_offset_origin_bounds_cover_grid_once() {
        assert_eq!(super::tile_count_1d(0, 16, 1), 0);
        assert_eq!(super::tile_count_1d(65, 16, 0), 5);
        assert_eq!(super::tile_count_1d(65, 16, 1), 5);
        assert_eq!(super::tile_bounds_1d(0, 65, 16, 1), (0, 1));
        assert_eq!(super::tile_bounds_1d(1, 65, 16, 1), (1, 17));
        assert_eq!(super::tile_bounds_1d(4, 65, 16, 1), (49, 65));
        assert_eq!(
            super::tile_bounds_from_origin(65, 16, 1),
            vec![0, 1, 17, 33, 49, 65]
        );

        let partition =
            StandardMfsFixedTilePartition::new_with_origin([65, 65], [16, 16], 0, [1, 1], "one")
                .unwrap();
        assert_eq!(partition.tile_count(), 25);
        assert_eq!(partition.owner(0, 0), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(1, 1), Some(StandardMfsTileId(6)));
        assert_eq!(partition.owner(64, 64), Some(StandardMfsTileId(24)));
    }

    #[test]
    fn standard_mfs_tile_bucket_sample_rejects_invalid_tap_metadata() {
        let sample = super::StandardMfsTileBucketSample {
            sample_id: 0,
            center_x: 6,
            center_y: 6,
            kernel_u: 0,
            kernel_v: 0,
            support_id: 1,
            flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
            grid_weight: 1.0,
            tap_count: 49,
        };
        let error = sample.positive_taps().unwrap_err();
        assert!(
            error.to_string().contains("unsupported tap support id 1"),
            "{error}"
        );

        let sample = super::StandardMfsTileBucketSample {
            support_id: 0,
            center_x: 2,
            center_y: 6,
            ..sample
        };
        let error = sample.positive_taps().unwrap_err();
        assert!(
            error.to_string().contains("invalid x tap center"),
            "{error}"
        );

        let sample = super::StandardMfsTileBucketSample {
            center_x: 6,
            center_y: 2,
            ..sample
        };
        let error = sample.positive_taps().unwrap_err();
        assert!(
            error.to_string().contains("invalid y tap center"),
            "{error}"
        );
    }

    #[test]
    fn standard_mfs_environment_parsers_reject_empty_and_invalid_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _probe = EnvVarGuard::remove(super::STANDARD_MFS_TILE_BUCKET_PROBE_ENV);
        let _edge = EnvVarGuard::remove(super::STANDARD_MFS_TILE_EDGE_ENV);
        let _threads = EnvVarGuard::remove(super::STANDARD_MFS_GRID_THREADS_ENV);
        let _force = EnvVarGuard::remove(super::STANDARD_MFS_FORCE_TILED_ONE_WORKER_ENV);
        let _flush = EnvVarGuard::remove(super::STANDARD_MFS_TILE_FLUSH_ENV);
        let _inbox = EnvVarGuard::remove("CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN");

        assert!(!super::standard_mfs_tile_bucket_probe_enabled());
        assert_eq!(super::standard_mfs_tile_edge_with_config(Some(64)), 64);
        assert_eq!(super::standard_mfs_tile_edge_with_config(Some(0)), 256);
        assert_eq!(super::standard_mfs_grid_threads(), 1);
        assert!(!super::standard_mfs_force_tiled_one_worker());
        assert!(!super::standard_mfs_per_block_flush_enabled());
        assert_eq!(super::standard_mfs_tile_inbox_ready_sample_min(), 1024);

        let _probe = EnvVarGuard::set(super::STANDARD_MFS_TILE_BUCKET_PROBE_ENV, "off");
        let _edge = EnvVarGuard::set(super::STANDARD_MFS_TILE_EDGE_ENV, "not-a-number");
        let _threads = EnvVarGuard::set(super::STANDARD_MFS_GRID_THREADS_ENV, "0");
        let _force = EnvVarGuard::set(super::STANDARD_MFS_FORCE_TILED_ONE_WORKER_ENV, "no");
        let _flush = EnvVarGuard::set(super::STANDARD_MFS_TILE_FLUSH_ENV, "block");
        let _inbox = EnvVarGuard::set("CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN", "0");

        assert!(!super::standard_mfs_tile_bucket_probe_enabled());
        assert_eq!(super::standard_mfs_tile_edge_with_config(Some(64)), 64);
        assert_eq!(super::standard_mfs_grid_threads(), 1);
        assert!(!super::standard_mfs_force_tiled_one_worker());
        assert!(!super::standard_mfs_per_block_flush_enabled());
        assert_eq!(super::standard_mfs_tile_inbox_ready_sample_min(), 1024);
    }

    #[test]
    fn standard_mfs_environment_parsers_accept_explicit_overrides() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _probe = EnvVarGuard::set(super::STANDARD_MFS_TILE_BUCKET_PROBE_ENV, "yes");
        let _edge = EnvVarGuard::set(super::STANDARD_MFS_TILE_EDGE_ENV, "48");
        let _threads = EnvVarGuard::set(super::STANDARD_MFS_GRID_THREADS_ENV, "3");
        let _force = EnvVarGuard::set(super::STANDARD_MFS_FORCE_TILED_ONE_WORKER_ENV, "on");
        let _flush = EnvVarGuard::set(super::STANDARD_MFS_TILE_FLUSH_ENV, "per-block");
        let _inbox = EnvVarGuard::set("CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN", "7");

        assert!(super::standard_mfs_tile_bucket_probe_enabled());
        assert_eq!(super::standard_mfs_tile_edge_with_config(Some(64)), 48);
        assert_eq!(super::standard_mfs_grid_threads(), 3);
        assert!(super::standard_mfs_force_tiled_one_worker());
        assert!(super::standard_mfs_per_block_flush_enabled());
        assert_eq!(super::standard_mfs_tile_inbox_ready_sample_min(), 7);
    }

    #[test]
    fn block_tile_buckets_keep_only_compact_current_block_records() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let partition =
            StandardMfsFixedTilePartition::new(gridder.grid_shape(), [16, 16], 3).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let batch = VisibilityBatch {
            u_lambda: vec![-8.0 * du, 0.0, 4.0 * du, 8.0 * du],
            v_lambda: vec![8.0 * dv, 0.0, 4.0 * dv, -8.0 * dv],
            w_lambda: vec![0.0; 4],
            weight: vec![1.0, 2.0, 0.0, 3.0],
            sumwt_factor: vec![1.0, 2.0, 1.0, 4.0],
            gridable: vec![true; 4],
            visibility: vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(f32::NAN, 1.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, -3.0),
            ],
        };

        let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
            &gridder,
            &partition,
            std::slice::from_ref(&batch),
        )
        .unwrap();

        assert_eq!(buckets.accepted_samples(), 3);
        assert_eq!(buckets.skipped_samples(), 1);
        assert_eq!(buckets.samples().len(), 3);
        assert_eq!(
            buckets.nonempty_tiles(),
            &[StandardMfsTileId(0), StandardMfsTileId(3)]
        );
        let tile0 = buckets.tile_samples(StandardMfsTileId(0));
        assert_eq!(tile0.len(), 1);
        assert_eq!(tile0[0].sample_id, 0);
        assert_eq!((tile0[0].center_x, tile0[0].center_y), (8, 8));
        assert_eq!(tile0[0].flags, STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY);
        assert!(tile0[0].finite_visibility());
        assert!(!tile0[0].psf_only());
        assert_eq!(tile0[0].grid_weight, 1.0);
        assert_eq!(tile0[0].tap_count, 49);
        assert_eq!(
            tile0[0].positive_taps().unwrap(),
            gridder
                .plan_positive_taps(batch.u_lambda[0], batch.v_lambda[0])
                .unwrap()
        );

        let tile3 = buckets.tile_samples(StandardMfsTileId(3));
        assert_eq!(tile3.len(), 2);
        assert_eq!(tile3[0].sample_id, 1);
        assert_eq!((tile3[0].center_x, tile3[0].center_y), (16, 16));
        assert_eq!(tile3[0].flags, STANDARD_MFS_TILE_FLAG_PSF_ONLY);
        assert!(!tile3[0].finite_visibility());
        assert!(tile3[0].psf_only());
        assert_eq!(tile3[0].grid_weight, 4.0);
        assert_eq!(tile3[0].tap_count, 49);
        assert_eq!(
            tile3[0].positive_taps().unwrap(),
            gridder
                .plan_positive_taps(batch.u_lambda[1], batch.v_lambda[1])
                .unwrap()
        );
        assert_eq!(tile3[1].sample_id, 3);
        assert_eq!((tile3[1].center_x, tile3[1].center_y), (24, 24));
        assert_eq!(tile3[1].grid_weight, 12.0);
        assert_eq!(tile3[1].tap_count, 49);
        assert_eq!(
            tile3[1].positive_taps().unwrap(),
            gridder
                .plan_positive_taps(batch.u_lambda[3], batch.v_lambda[3])
                .unwrap()
        );

        assert!(
            std::mem::size_of::<super::StandardMfsTileBucketSample>()
                < std::mem::size_of::<super::StandardMfsPlannedSample>(),
            "bucket records should stay smaller than retained planned samples"
        );
        assert!(
            buckets.estimated_bytes() >= 4 * std::mem::size_of::<super::StandardMfsSampleRef>(),
            "row-block accounting should include the sample id table"
        );
    }

    #[test]
    fn block_tile_buckets_resolve_multi_batch_sample_ids() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let partition =
            StandardMfsFixedTilePartition::new(gridder.grid_shape(), [16, 16], 3).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let left = VisibilityBatch {
            u_lambda: vec![-8.0 * du, 0.0],
            v_lambda: vec![8.0 * dv, 0.0],
            w_lambda: vec![0.0, 1.0],
            weight: vec![1.0, 2.0],
            sumwt_factor: vec![1.0, 2.0],
            gridable: vec![true; 2],
            visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 1.0)],
        };
        let right = VisibilityBatch {
            u_lambda: vec![4.0 * du, 8.0 * du],
            v_lambda: vec![4.0 * dv, -8.0 * dv],
            w_lambda: vec![2.0, 3.0],
            weight: vec![0.0, 3.0],
            sumwt_factor: vec![1.0, 4.0],
            gridable: vec![true; 2],
            visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, -3.0)],
        };

        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(&gridder, &partition, &[left, right])
                .unwrap();

        assert_eq!(buckets.accepted_samples(), 3);
        assert_eq!(buckets.skipped_samples(), 1);
        let mut sample_ids = buckets
            .samples()
            .iter()
            .map(|sample| sample.sample_id)
            .collect::<Vec<_>>();
        sample_ids.sort_unstable();
        assert_eq!(sample_ids, vec![0, 1, 3]);
        assert_eq!(
            buckets.sample_ref(0).unwrap(),
            super::StandardMfsSampleRef {
                batch_index: 0,
                sample_index: 0
            }
        );
        assert_eq!(
            buckets.sample_ref(1).unwrap(),
            super::StandardMfsSampleRef {
                batch_index: 0,
                sample_index: 1
            }
        );
        assert_eq!(
            buckets.sample_ref(3).unwrap(),
            super::StandardMfsSampleRef {
                batch_index: 1,
                sample_index: 1
            }
        );
        assert!(buckets.sample_ref(4).is_err());
        assert!(
            buckets.estimated_bytes() >= 4 * std::mem::size_of::<super::StandardMfsSampleRef>()
        );
    }

    #[test]
    fn block_tile_buckets_keep_zero_task_blocks_unpublished() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let partition =
            StandardMfsFixedTilePartition::new(gridder.grid_shape(), [16, 16], 3).unwrap();
        let batch = VisibilityBatch {
            u_lambda: vec![0.0, 1.0],
            v_lambda: vec![0.0, 1.0],
            w_lambda: vec![0.0, 0.0],
            weight: vec![0.0, f32::NAN],
            sumwt_factor: vec![1.0, 1.0],
            gridable: vec![true; 2],
            visibility: vec![Complex32::new(1.0, 0.0); 2],
        };

        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(&gridder, &partition, &[batch]).unwrap();

        assert_eq!(buckets.accepted_samples(), 0);
        assert_eq!(buckets.skipped_samples(), 2);
        assert!(buckets.nonempty_tiles().is_empty());
        assert!(buckets.tile_tasks_descending().is_empty());
    }

    #[test]
    fn prepared_tile_row_block_accounts_owned_batch_storage_and_skips_empty_blocks() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let partition =
            StandardMfsFixedTilePartition::new(gridder.grid_shape(), [16, 16], 3).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let mut id_allocator = super::StandardMfsRowBlockIdAllocator::default();
        let left = VisibilityBatch {
            u_lambda: vec![-8.0 * du, 0.0],
            v_lambda: vec![0.0, 0.0],
            w_lambda: vec![0.0, 1.0],
            weight: vec![1.0, 2.0],
            sumwt_factor: vec![1.0, 2.0],
            gridable: vec![true; 2],
            visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 0.0)],
        };
        let right = VisibilityBatch {
            u_lambda: vec![8.0 * du],
            v_lambda: vec![0.0],
            w_lambda: vec![2.0],
            weight: vec![3.0],
            sumwt_factor: vec![4.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(2.0, -3.0)],
        };
        let batches = vec![left, right];
        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(&gridder, &partition, &batches).unwrap();

        let first_id = id_allocator.next();
        let second_id = id_allocator.next();
        assert!(first_id < second_id);
        let prepared =
            super::PreparedTileRowBlock::batch_backed(first_id, batches.clone(), buckets).unwrap();
        let prepared = prepared.expect("accepted samples should publish a prepared row block");

        assert_eq!(prepared.block_id, first_id);
        assert_eq!(prepared.storage.sample_count(), 3);
        assert_eq!(prepared.sample_ref(2).unwrap().batch_index, 1);
        assert_eq!(prepared.visibility(2).unwrap(), Complex32::new(2.0, -3.0));
        assert!(prepared.byte_ledger().storage_bytes > 0);
        assert!(prepared.byte_ledger().sample_ref_bytes >= 3 * size_of::<StandardMfsSampleRef>());
        assert!(
            prepared.byte_ledger().scalar_record_bytes
                >= size_of::<super::StandardMfsTaskScalarRecord>()
        );

        let empty = VisibilityBatch {
            u_lambda: vec![0.0],
            v_lambda: vec![0.0],
            w_lambda: vec![0.0],
            weight: vec![0.0],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        };
        let empty_buckets = StandardMfsBlockTileBuckets::build_for_dirty(
            &gridder,
            &partition,
            std::slice::from_ref(&empty),
        )
        .unwrap();
        assert!(
            super::PreparedTileRowBlock::batch_backed(second_id, vec![empty], empty_buckets)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn standard_mfs_sample_classifier_separates_psf_and_visibility_validity() {
        let batch = VisibilityBatch {
            u_lambda: vec![0.0, 0.0, f64::NAN],
            v_lambda: vec![0.0, 0.0, 0.0],
            w_lambda: vec![0.0, 0.0, 0.0],
            weight: vec![1.0, 1.0, 1.0],
            sumwt_factor: vec![1.0, 1.0, 1.0],
            gridable: vec![true, true, true],
            visibility: vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(f32::NAN, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        };

        let finite = super::classify_standard_mfs_sample(&batch, 0);
        assert!(finite.valid_for_density);
        assert!(finite.valid_for_psf);
        assert!(finite.valid_for_dirty_visibility);
        assert!(finite.valid_for_residual_visibility);

        let nonfinite = super::classify_standard_mfs_sample(&batch, 1);
        assert!(nonfinite.valid_for_density);
        assert!(nonfinite.valid_for_psf);
        assert!(!nonfinite.valid_for_dirty_visibility);
        assert!(!nonfinite.valid_for_residual_visibility);

        let invalid_geometry = super::classify_standard_mfs_sample(&batch, 2);
        assert!(!invalid_geometry.valid_for_density);
        assert!(!invalid_geometry.valid_for_psf);
        assert!(!invalid_geometry.valid_geometry);
    }

    #[test]
    fn standard_mfs_tile_sample_router_modes_preserve_stage_semantics() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let partition =
            StandardMfsFixedTilePartition::new(gridder.grid_shape(), [16, 16], 3).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let batch = VisibilityBatch {
            u_lambda: vec![0.0, du, 2.0 * du, 3.0 * du],
            v_lambda: vec![0.0; 4],
            w_lambda: vec![0.0; 4],
            weight: vec![1.0, 1.0, 1.0, 1.0],
            sumwt_factor: vec![1.0, 1.0, 1.0, 0.0],
            gridable: vec![true; 4],
            visibility: vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(f32::NAN, 0.0),
                Complex32::new(2.0, -1.0),
                Complex32::new(3.0, 0.0),
            ],
        };

        let psf_router = super::StandardMfsTileSampleRouter::new(
            &gridder,
            &partition,
            super::StandardMfsTileSampleRouteMode::PsfNoData,
        );
        let super::StandardMfsTileSampleRouteDecision::Enqueue(_, psf_sample) =
            psf_router.route_batch_sample(&batch, 0, 7).unwrap()
        else {
            panic!("expected PSF sample");
        };
        assert!(psf_sample.psf_only());
        assert_eq!(psf_sample.visibility, Complex32::new(0.0, 0.0));

        let dirty_router = super::StandardMfsTileSampleRouter::new(
            &gridder,
            &partition,
            super::StandardMfsTileSampleRouteMode::DirtyWithData,
        );
        let super::StandardMfsTileSampleRouteDecision::Enqueue(_, dirty_nonfinite) =
            dirty_router.route_batch_sample(&batch, 1, 8).unwrap()
        else {
            panic!("expected dirty PSF-only sample");
        };
        assert!(dirty_nonfinite.psf_only());
        assert!(dirty_nonfinite.visibility.re.is_nan());

        let residual_router = super::StandardMfsTileSampleRouter::new(
            &gridder,
            &partition,
            super::StandardMfsTileSampleRouteMode::ResidualWithData,
        );
        assert_eq!(
            residual_router.route_batch_sample(&batch, 1, 9).unwrap(),
            super::StandardMfsTileSampleRouteDecision::Skip(
                super::StandardMfsTileSampleRouteSkip::NonfiniteVisibility
            )
        );
        assert_eq!(
            residual_router.route_batch_sample(&batch, 3, 10).unwrap(),
            super::StandardMfsTileSampleRouteDecision::Skip(
                super::StandardMfsTileSampleRouteSkip::InvalidSumwt
            )
        );

        let density_router = super::StandardMfsTileSampleRouter::new(
            &gridder,
            &partition,
            super::StandardMfsTileSampleRouteMode::DensityNoData,
        );
        assert!(matches!(
            density_router.route_batch_sample(&batch, 2, 11).unwrap(),
            super::StandardMfsTileSampleRouteDecision::Density(_)
        ));
    }

    #[test]
    fn fixed_tile_partition_supports_center_boundary_origin() {
        let partition =
            StandardMfsFixedTilePartition::new_with_origin([65, 65], [16, 16], 3, [1, 1], "test")
                .unwrap();

        assert_eq!(partition.tile_origin(), [1, 1]);
        assert_eq!(partition.tile_count(), 25);
        assert_eq!(partition.owner(0, 0), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(1, 1), Some(StandardMfsTileId(6)));
        assert_eq!(partition.owner(16, 16), Some(StandardMfsTileId(6)));
        assert_eq!(partition.owner(17, 17), Some(StandardMfsTileId(12)));
        assert_eq!(partition.owner(64, 64), Some(StandardMfsTileId(24)));

        let first = partition.tile(StandardMfsTileId(0)).unwrap();
        assert_eq!(first.interior.x0, 0);
        assert_eq!(first.interior.x1, 1);
        assert_eq!(first.interior.y0, 0);
        assert_eq!(first.interior.y1, 1);

        let center = partition.tile(StandardMfsTileId(12)).unwrap();
        assert_eq!(center.interior.x0, 17);
        assert_eq!(center.interior.x1, 33);
        assert_eq!(center.interior.y0, 17);
        assert_eq!(center.interior.y1, 33);
    }

    #[test]
    fn fixed_tile_partition_origin_zero_has_no_empty_leading_tile() {
        let partition =
            StandardMfsFixedTilePartition::new_with_origin([64, 64], [16, 16], 3, [0, 0], "test")
                .unwrap();

        assert_eq!(partition.tile_count(), 16);
        assert_eq!(partition.owner(0, 0), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(15, 15), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(16, 0), Some(StandardMfsTileId(4)));
        assert_eq!(partition.owner(0, 16), Some(StandardMfsTileId(1)));

        let first = partition.tile(StandardMfsTileId(0)).unwrap();
        assert_eq!(first.interior.x0, 0);
        assert_eq!(first.interior.x1, 16);
        assert_eq!(first.interior.y0, 0);
        assert_eq!(first.interior.y1, 16);
    }

    #[test]
    fn center_boundary_partition_uses_gridder_tap_center() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let center = gridder.positive_tap_grid_center();
        let partition =
            StandardMfsFixedTilePartition::new_center_boundary(&gridder, [16, 16], 3).unwrap();

        assert_eq!(center, [16, 16]);
        assert_eq!(partition.tile_origin(), [0, 0]);
        assert_eq!(
            partition.owner(center[0], center[1]),
            Some(StandardMfsTileId(3))
        );
    }

    #[test]
    fn center_quadrant_partition_places_gridder_center_at_four_tile_intersection() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let center = gridder.positive_tap_grid_center();
        let partition = StandardMfsFixedTilePartition::new_center_quadrants(&gridder, 3).unwrap();

        assert_eq!(center, [16, 16]);
        assert_eq!(partition.anchor_label(), "center_quadrants");
        assert_eq!(partition.tile_origin(), center);
        assert_eq!(partition.tile_shape(), [16, 16]);
        assert_eq!(partition.tile_count(), 4);
        assert_eq!(partition.owner(15, 15), Some(StandardMfsTileId(0)));
        assert_eq!(partition.owner(15, 16), Some(StandardMfsTileId(1)));
        assert_eq!(partition.owner(16, 15), Some(StandardMfsTileId(2)));
        assert_eq!(partition.owner(16, 16), Some(StandardMfsTileId(3)));
        assert_eq!(partition.interior_cell_count(), 32 * 32);
        assert!(partition.halo_cell_count() > partition.interior_cell_count());
    }

    #[test]
    fn streaming_dirty_executor_accumulates_borrowed_row_blocks() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let left = VisibilityBatch {
            u_lambda: vec![0.0, 4.0],
            v_lambda: vec![0.0, 1.0],
            w_lambda: vec![3.0, -5.0],
            weight: vec![1.0, 2.0],
            sumwt_factor: vec![1.0, 2.0],
            gridable: vec![true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 2],
        };
        let right = VisibilityBatch {
            u_lambda: vec![8.0, 12.0],
            v_lambda: vec![2.0, 3.0],
            w_lambda: vec![7.0, 11.0],
            weight: vec![0.0, 3.0],
            sumwt_factor: vec![1.0, f32::NAN],
            gridable: vec![true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 2],
        };
        let mut executor = StandardMfsDirtyCpuExecutor::new(geometry).unwrap();

        executor.accumulate_batches(&[left]).unwrap();
        executor.accumulate_batches(&[right]).unwrap();
        let accumulation = executor.accumulation();

        assert_eq!(accumulation.gridded_samples, 2);
        assert_eq!(accumulation.skipped_samples, 2);
        assert!((accumulation.normalization_sumwt - 5.0).abs() < 1.0e-6);
        assert!((accumulation.reported_sumwt - 5.0).abs() < 1.0e-6);
        assert!((accumulation.max_abs_w_lambda - 11.0).abs() < 1.0e-6);
    }

    #[test]
    fn direct_resident_tiles_match_evicted_tile_dirty_and_residual_paths() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let batches = vec![
            VisibilityBatch {
                u_lambda: vec![-8.0 * du, 0.0],
                v_lambda: vec![8.0 * dv, 0.0],
                w_lambda: vec![0.0, 1.0],
                weight: vec![1.0, 2.0],
                sumwt_factor: vec![1.0, 2.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 1.0)],
            },
            VisibilityBatch {
                u_lambda: vec![4.0 * du, 8.0 * du],
                v_lambda: vec![4.0 * dv, -8.0 * dv],
                w_lambda: vec![-2.0, 3.0],
                weight: vec![0.0, 3.0],
                sumwt_factor: vec![1.0, 4.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, -3.0)],
            },
        ];
        let evicted = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(1),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: false,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let direct = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: false,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let shape = gridder.grid_shape();
        let mut evicted_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut evicted_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let evicted_accum = evicted
            .accumulate_dirty_grids(&batches, &mut evicted_psf, &mut evicted_dirty)
            .unwrap();
        let mut direct_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut direct_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_accum = direct
            .accumulate_dirty_grids(&batches, &mut direct_psf, &mut direct_dirty)
            .unwrap();

        assert_eq!(evicted_accum, direct_accum);
        assert_eq!(evicted_psf, direct_psf);
        assert_eq!(evicted_dirty, direct_dirty);

        let mut evicted_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let evicted_residual_accum = evicted
            .accumulate_residual_grid(&batches, None, &mut evicted_residual)
            .unwrap();
        let mut direct_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_residual_accum = direct
            .accumulate_residual_grid(&batches, None, &mut direct_residual)
            .unwrap();

        assert_eq!(evicted_residual_accum, direct_residual_accum);
        assert_eq!(evicted_residual, direct_residual);
    }

    fn test_tile_queue_sample(input_seq: u64) -> super::StandardMfsTileQueueSample {
        super::StandardMfsTileQueueSample {
            center_x: 16,
            center_y: 16,
            flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
            raw_weight: 1.0,
            sumwt_factor: 1.0,
            u_lambda: 0.0,
            v_lambda: 0.0,
            w_lambda: 0.0,
            visibility: Complex32::new(1.0, 0.0),
            input_seq,
        }
    }

    fn test_tile_visibility_run(
        first_input_seq: u64,
        sample_count: usize,
    ) -> super::StandardMfsTileVisibilityRun {
        let mut run =
            super::StandardMfsTileVisibilityRun::with_capacity(sample_count, first_input_seq);
        for offset in 0..sample_count {
            run.push_sample(test_tile_queue_sample(first_input_seq + offset as u64));
        }
        run
    }

    #[test]
    fn row_backed_visibility_runs_are_not_discarded_as_empty() {
        let row = Arc::new(StandardMfsRoutedVisibilityRow {
            uvw_m: [0.0, 0.0, 0.0],
            spw_id: 0,
            channel_origin: 0,
            source_channel_indices: Arc::from([0usize, 1usize]),
            channel_lambda_scales: Arc::from([1.0_f64, 1.1_f64]),
            data: ndarray::Array2::from_shape_vec(
                (1, 2),
                vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
            )
            .unwrap(),
            flag: ndarray::Array2::from_shape_vec((1, 2), vec![false, false]).unwrap(),
            weight: Arc::from([1.0_f32]),
            weight_spectrum: None,
            gridable: true,
            polarization: StandardMfsVisibilityPolarization::Explicit {
                corr_index: 0,
                sumwt_factor: 1.0,
            },
        });
        let routed = StandardMfsRoutedVisibilityRun {
            row,
            source_slot_range: 0..2,
            tap_centers: Arc::from([[16_u32, 16_u32], [17_u32, 16_u32]]),
            first_input_seq: 42,
        };
        let run =
            super::StandardMfsTileVisibilityRun::from_routed_visibility_run(&routed, 0..2, 42);
        assert_eq!(run.len(), 2);
        assert!(!run.is_empty());
        assert!(Arc::ptr_eq(&run.tap_centers, &routed.tap_centers));
        assert_eq!(run.tap_center_range, 0..2);

        let mut enqueued = Vec::<(StandardMfsTileId, usize, u64)>::new();
        {
            let mut enqueue = |tile_id, run: super::StandardMfsTileVisibilityRun| {
                enqueued.push((tile_id, run.len(), run.first_input_seq));
                Ok(())
            };
            let mut accumulator = super::StandardMfsTileRunAccumulator::new(&mut enqueue);
            accumulator.push_run(StandardMfsTileId(3), run).unwrap();
            accumulator.flush().unwrap();
        }

        assert_eq!(enqueued, vec![(StandardMfsTileId(3), 2, 42)]);
    }

    fn test_planned_weighted_sample(
        center_x: u32,
        center_y: u32,
    ) -> StandardMfsPlannedWeightedSample {
        StandardMfsPlannedWeightedSample {
            u_lambda: 0.0,
            v_lambda: 0.0,
            center_x,
            center_y,
            x_start: center_x.saturating_sub(3),
            y_start: center_y.saturating_sub(3),
            x_weight_index: 0,
            y_weight_index: 0,
            flags: StandardMfsPlannedWeightedSample::FINITE_VISIBILITY,
            tap_count: 49,
            grid_weight: 1.0,
            w_lambda: 0.0,
            visibility: Complex32::new(1.0, 0.0),
        }
    }

    #[test]
    fn planned_same_tile_samples_enqueue_as_one_visibility_run() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let executor = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: true,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let samples = vec![
            test_planned_weighted_sample(17, 17),
            test_planned_weighted_sample(18, 17),
            test_planned_weighted_sample(19, 17),
        ];
        let mut next_input_seq = 0u64;
        let mut enqueued = Vec::<(StandardMfsTileId, usize, u64)>::new();
        let accumulation = executor
            .enqueue_planned_dirty_samples_to_tile_inbox(
                &samples,
                false,
                &mut next_input_seq,
                &mut |tile_id, run| {
                    enqueued.push((tile_id, run.len(), run.first_input_seq));
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(enqueued.len(), 1);
        assert_eq!(enqueued[0].1, samples.len());
        assert_eq!(enqueued[0].2, 0);
        assert_eq!(next_input_seq, samples.len() as u64);
        assert_eq!(accumulation.skipped_samples, 0);
        assert_eq!(accumulation.max_abs_w_lambda, 0.0);
    }

    #[test]
    fn tile_inbox_scheduler_schedules_tiles_and_drains_all_runs() {
        let partition = StandardMfsFixedTilePartition::new([64, 64], [32, 32], 3).unwrap();
        let hot_tile = StandardMfsTileId(0);
        let cold_tile = StandardMfsTileId(3);
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_for_callback = std::sync::Arc::clone(&observed);
        let observability_callback: crate::StandardMfsObservabilityCallback =
            std::sync::Arc::new(move |event| {
                observed_for_callback.lock().unwrap().push(event);
            });
        let output = super::run_standard_mfs_tile_inbox_scheduler(
            &partition,
            2,
            "test",
            Some(&observability_callback),
            |enqueue| {
                enqueue(hot_tile, test_tile_visibility_run(0, 3))?;
                enqueue(hot_tile, test_tile_visibility_run(10, 4))?;
                enqueue(cold_tile, test_tile_visibility_run(10_000, 1))?;
                Ok(())
            },
            |_tile_id, samples| {
                Ok((
                    samples.len(),
                    super::StandardMfsTileTaskTiming {
                        local_alloc_zero: Duration::ZERO,
                        worker_replan_grid: Duration::from_nanos(samples.len() as u64 + 1),
                    },
                ))
            },
        )
        .unwrap();

        let processed_samples = output
            .task_outputs
            .iter()
            .map(|task| task.output)
            .sum::<usize>();
        assert_eq!(processed_samples, 8);
        assert_eq!(output.stats.enqueued_runs, 3);
        assert_eq!(output.stats.enqueued_samples, 8);
        assert_eq!(output.stats.current_queued_bytes, 0);
        assert!(output.stats.worker_drains <= output.stats.enqueued_runs);
        assert_eq!(output.stats.worker_runs, 3);
        assert!(output.stats.ready_heads_pushed >= 2);
        assert!(
            output
                .task_outputs
                .iter()
                .any(|task| task.tile_id == cold_tile)
        );
        let observed = observed.lock().unwrap();
        assert!(observed.len() >= 3);
        assert!(
            observed
                .iter()
                .any(|event| event.queues.iter().any(|queue| queue.confidence
                    == crate::StandardMfsQueueProgressConfidence::Measured
                    && queue.high_water_bytes.unwrap_or(0) > 0))
        );
    }

    #[test]
    fn tile_inbox_producer_pending_retries_fifo_after_try_lock_miss() {
        let shared = std::sync::Arc::new(super::StandardMfsTileInboxShared::new(
            1, 1, "test", 1, None,
        ));
        let tile_id = StandardMfsTileId(0);
        let held = shared.tiles[tile_id.index()].queue.lock().unwrap();
        let mut producer = super::StandardMfsTileInboxProducer::new(std::sync::Arc::clone(&shared));

        producer
            .enqueue_run(tile_id, test_tile_visibility_run(1, 1))
            .unwrap();
        producer
            .enqueue_run(tile_id, test_tile_visibility_run(2, 1))
            .unwrap();
        drop(held);
        producer.flush_pending_blocking().unwrap();

        let queue = shared.tiles[tile_id.index()].queue.lock().unwrap();
        let input_seq = queue
            .runs
            .iter()
            .map(|run| run.first_input_seq)
            .collect::<Vec<_>>();
        assert_eq!(input_seq, vec![1, 2]);
        drop(queue);
        let ready = shared.ready.0.lock().unwrap();
        assert_eq!(ready.stats.try_lock_misses, 2);
        assert_eq!(ready.stats.pending_runs, 0);
        assert_eq!(ready.stats.pending_bytes, 0);
        assert_eq!(ready.stats.enqueued_runs, 2);
    }

    #[test]
    fn tile_inbox_owned_replay_matches_direct_dirty_and_residual() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let batches = vec![
            VisibilityBatch {
                u_lambda: vec![-8.0 * du, 0.0],
                v_lambda: vec![8.0 * dv, 0.0],
                w_lambda: vec![0.0, 1.0],
                weight: vec![1.0, 2.0],
                sumwt_factor: vec![1.0, 2.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 1.0)],
            },
            VisibilityBatch {
                u_lambda: vec![4.0 * du, 8.0 * du],
                v_lambda: vec![4.0 * dv, -8.0 * dv],
                w_lambda: vec![-2.0, 3.0],
                weight: vec![0.0, 3.0],
                sumwt_factor: vec![1.0, 4.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, -3.0)],
            },
        ];
        let executor = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let shape = gridder.grid_shape();
        let mut direct_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut direct_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_accum = executor
            .accumulate_dirty_grids(&batches, &mut direct_psf, &mut direct_dirty)
            .unwrap();

        let mut inbox_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut inbox_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut dirty_replay = |consumer: &mut dyn FnMut(
            Vec<VisibilityBatch>,
        )
            -> Result<(), crate::ImagingError>|
         -> Result<(), crate::ImagingError> {
            for batch in batches.clone() {
                consumer(vec![batch])?;
            }
            Ok(())
        };
        let inbox_accum = executor
            .accumulate_dirty_grids_direct_owned_replay(
                &mut dirty_replay,
                &mut inbox_psf,
                &mut inbox_dirty,
                2,
            )
            .unwrap();

        assert_eq!(
            inbox_accum.normalization_sumwt,
            direct_accum.normalization_sumwt
        );
        assert_eq!(inbox_accum.reported_sumwt, direct_accum.reported_sumwt);
        assert_eq!(inbox_accum.gridded_samples, direct_accum.gridded_samples);
        assert_eq!(inbox_accum.max_abs_w_lambda, direct_accum.max_abs_w_lambda);
        assert_eq!(inbox_psf, direct_psf);
        assert_eq!(inbox_dirty, direct_dirty);

        let mut direct_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_residual_accum = executor
            .accumulate_residual_grid(&batches, None, &mut direct_residual)
            .unwrap();
        let mut inbox_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut residual_replay = |consumer: &mut dyn FnMut(
            Vec<VisibilityBatch>,
        )
            -> Result<(), crate::ImagingError>|
         -> Result<(), crate::ImagingError> {
            for batch in batches.clone() {
                consumer(vec![batch])?;
            }
            Ok(())
        };
        let inbox_residual_accum = executor
            .accumulate_residual_grid_direct_owned_replay(
                &mut residual_replay,
                None,
                &mut inbox_residual,
                2,
            )
            .unwrap();

        assert_eq!(
            inbox_residual_accum.gridded_residual_samples,
            direct_residual_accum.gridded_residual_samples
        );
        assert_eq!(
            inbox_residual_accum.skipped_nonfinite_visibility,
            direct_residual_accum.skipped_nonfinite_visibility
        );
        assert_eq!(inbox_residual, direct_residual);
    }

    #[test]
    fn tile_inbox_planned_replay_matches_direct_dirty_and_residual() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let batches = vec![
            VisibilityBatch {
                u_lambda: vec![-8.0 * du, 0.0],
                v_lambda: vec![8.0 * dv, 0.0],
                w_lambda: vec![0.0, 1.0],
                weight: vec![1.0, 2.0],
                sumwt_factor: vec![1.0, 2.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 1.0)],
            },
            VisibilityBatch {
                u_lambda: vec![4.0 * du, 8.0 * du],
                v_lambda: vec![4.0 * dv, -8.0 * dv],
                w_lambda: vec![-2.0, 3.0],
                weight: vec![0.0, 3.0],
                sumwt_factor: vec![1.0, 4.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, -3.0)],
            },
        ];
        let executor = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let planner = crate::StandardMfsPlannedSampleBuilder::new(geometry).unwrap();
        let planned_blocks = batches
            .iter()
            .map(|batch| {
                let mut planned = Vec::new();
                planner
                    .plan_visibility_batch_into(batch, &mut planned)
                    .unwrap();
                planned
            })
            .collect::<Vec<_>>();
        let planned_run_blocks = planned_blocks
            .iter()
            .map(|block| {
                let mut run_block = crate::StandardMfsPlannedWeightedSampleRunBlock::default();
                run_block.push_run_from_slice(block);
                run_block
            })
            .collect::<Vec<_>>();

        let shape = gridder.grid_shape();
        let mut direct_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut direct_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_accum = executor
            .accumulate_dirty_grids(&batches, &mut direct_psf, &mut direct_dirty)
            .unwrap();

        let mut inbox_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut inbox_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut dirty_replay = |consumer: &mut dyn FnMut(
            &[crate::StandardMfsPlannedWeightedSample],
        )
            -> Result<(), crate::ImagingError>|
         -> Result<(), crate::ImagingError> {
            for block in &planned_blocks {
                consumer(block)?;
            }
            Ok(())
        };
        let inbox_accum = executor
            .accumulate_dirty_grids_direct_planned_replay(
                &mut dirty_replay,
                &mut inbox_psf,
                &mut inbox_dirty,
            )
            .unwrap();

        assert_eq!(
            inbox_accum.normalization_sumwt,
            direct_accum.normalization_sumwt
        );
        assert_eq!(inbox_accum.reported_sumwt, direct_accum.reported_sumwt);
        assert_eq!(inbox_accum.gridded_samples, direct_accum.gridded_samples);
        assert_eq!(inbox_accum.max_abs_w_lambda, direct_accum.max_abs_w_lambda);
        assert_eq!(inbox_psf, direct_psf);
        assert_eq!(inbox_dirty, direct_dirty);

        let mut run_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut run_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut dirty_run_replay = |consumer: &mut dyn FnMut(
            &crate::StandardMfsPlannedWeightedSampleRunBlock,
        )
            -> Result<(), crate::ImagingError>|
         -> Result<(), crate::ImagingError> {
            for block in &planned_run_blocks {
                consumer(block)?;
            }
            Ok(())
        };
        let run_accum = executor
            .accumulate_dirty_grids_direct_planned_run_replay(
                &mut dirty_run_replay,
                &mut run_psf,
                &mut run_dirty,
            )
            .unwrap();

        assert_eq!(
            run_accum.normalization_sumwt,
            direct_accum.normalization_sumwt
        );
        assert_eq!(run_accum.reported_sumwt, direct_accum.reported_sumwt);
        assert_eq!(run_accum.gridded_samples, direct_accum.gridded_samples);
        assert_eq!(run_accum.max_abs_w_lambda, direct_accum.max_abs_w_lambda);
        assert_eq!(run_psf, direct_psf);
        assert_eq!(run_dirty, direct_dirty);

        let mut direct_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_residual_accum = executor
            .accumulate_residual_grid(&batches, None, &mut direct_residual)
            .unwrap();
        let mut inbox_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut residual_replay = |consumer: &mut dyn FnMut(
            &[crate::StandardMfsPlannedWeightedSample],
        )
            -> Result<(), crate::ImagingError>|
         -> Result<(), crate::ImagingError> {
            for block in &planned_blocks {
                consumer(block)?;
            }
            Ok(())
        };
        let inbox_residual_accum = executor
            .accumulate_residual_grid_direct_planned_replay(
                &mut residual_replay,
                None,
                &mut inbox_residual,
            )
            .unwrap();

        assert_eq!(
            inbox_residual_accum.gridded_residual_samples,
            direct_residual_accum.gridded_residual_samples
        );
        assert_eq!(
            inbox_residual_accum.skipped_nonfinite_visibility,
            direct_residual_accum.skipped_nonfinite_visibility
        );
        assert_eq!(inbox_residual, direct_residual);

        let mut run_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut residual_run_replay =
            |consumer: &mut dyn FnMut(
                &crate::StandardMfsPlannedWeightedSampleRunBlock,
            ) -> Result<(), crate::ImagingError>|
             -> Result<(), crate::ImagingError> {
                for block in &planned_run_blocks {
                    consumer(block)?;
                }
                Ok(())
            };
        let run_residual_accum = executor
            .accumulate_residual_grid_direct_planned_run_replay(
                &mut residual_run_replay,
                None,
                &mut run_residual,
            )
            .unwrap();

        assert_eq!(
            run_residual_accum.gridded_residual_samples,
            direct_residual_accum.gridded_residual_samples
        );
        assert_eq!(
            run_residual_accum.skipped_nonfinite_visibility,
            direct_residual_accum.skipped_nonfinite_visibility
        );
        assert_eq!(run_residual, direct_residual);
    }

    #[test]
    fn persistent_tile_scheduler_matches_direct_dirty_and_residual() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new_unpadded(geometry).unwrap();
        let du = gridder.grid_spacing_lambda()[0];
        let dv = gridder.grid_spacing_lambda()[1];
        let batches = vec![
            VisibilityBatch {
                u_lambda: vec![-8.0 * du, 0.0],
                v_lambda: vec![8.0 * dv, 0.0],
                w_lambda: vec![0.0, 1.0],
                weight: vec![1.0, 2.0],
                sumwt_factor: vec![1.0, 2.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(f32::NAN, 1.0)],
            },
            VisibilityBatch {
                u_lambda: vec![4.0 * du, 8.0 * du],
                v_lambda: vec![4.0 * dv, -8.0 * dv],
                w_lambda: vec![-2.0, 3.0],
                weight: vec![0.0, 3.0],
                sumwt_factor: vec![1.0, 4.0],
                gridable: vec![true; 2],
                visibility: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, -3.0)],
            },
        ];
        let executor = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                minor_cycle_backend: StandardMfsMinorCycleBackend::Cpu,
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
                metal_grouped_input_cache: false,
                materialized_sample_plan_max_samples: None,
                w_project_max_abs_w_lambda: None,
                progress_callback: None,
                observability_callback: None,
            },
        )
        .unwrap();
        let shape = gridder.grid_shape();
        let mut direct_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut direct_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_accum = executor
            .accumulate_dirty_grids(&batches, &mut direct_psf, &mut direct_dirty)
            .unwrap();

        let store = super::DirectDirtyTileStore::new(&executor.partition);
        let mut persistent_accum = super::StandardMfsDirtyAccumulation::default();
        let mut block_ids = super::StandardMfsRowBlockIdAllocator::default();
        let output = super::run_standard_mfs_persistent_tile_scheduler(
            &executor.partition,
            2,
            2,
            |publish| {
                for batch in batches.clone() {
                    persistent_accum.max_abs_w_lambda = batch
                        .w_lambda
                        .iter()
                        .fold(persistent_accum.max_abs_w_lambda, |max_value, value| {
                            max_value.max(value.abs())
                        });
                    let row_batches = vec![batch];
                    let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                        &gridder,
                        &executor.partition,
                        &row_batches,
                    )?;
                    persistent_accum.skipped_samples += buckets.skipped_samples();
                    if let Some(block) = super::PreparedTileRowBlock::batch_backed(
                        block_ids.next(),
                        row_batches,
                        buckets,
                    )? {
                        publish(block)?;
                    }
                }
                Ok(())
            },
            |block, task| {
                executor.grid_dirty_tile_task_direct(
                    block.storage.batches(),
                    &block.buckets,
                    task.tile_id,
                    &store,
                )
            },
        )
        .unwrap();
        for task_output in output.task_outputs {
            persistent_accum.add(task_output.output);
        }
        let mut persistent_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut persistent_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        store
            .flush_all(&mut persistent_psf, &mut persistent_dirty)
            .unwrap();

        assert_eq!(persistent_accum, direct_accum);
        assert_eq!(persistent_psf, direct_psf);
        assert_eq!(persistent_dirty, direct_dirty);

        let mut direct_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_residual_accum = executor
            .accumulate_residual_grid(&batches, None, &mut direct_residual)
            .unwrap();
        let residual_store = super::DirectResidualTileStore::new(&executor.partition);
        let mut persistent_residual_accum = super::StandardMfsTiledResidualAccumulation::default();
        let mut residual_block_ids = super::StandardMfsRowBlockIdAllocator::default();
        let residual_output = super::run_standard_mfs_persistent_tile_scheduler(
            &executor.partition,
            2,
            2,
            |publish| {
                for batch in batches.clone() {
                    let row_batches = vec![batch];
                    let (buckets, block_accumulation) =
                        StandardMfsBlockTileBuckets::build_for_residual_refresh(
                            &gridder,
                            &executor.partition,
                            &row_batches,
                        )?;
                    persistent_residual_accum.add_residual(block_accumulation);
                    if let Some(block) = super::PreparedTileRowBlock::batch_backed(
                        residual_block_ids.next(),
                        row_batches,
                        buckets,
                    )? {
                        publish(block)?;
                    }
                }
                Ok(())
            },
            |block, task| {
                executor.grid_residual_tile_task_direct(
                    block.storage.batches(),
                    &block.buckets,
                    None,
                    task.tile_id,
                    &residual_store,
                )
            },
        )
        .unwrap();
        for task_output in residual_output.task_outputs {
            persistent_residual_accum.gridded_residual_samples += task_output.output;
        }
        let mut persistent_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        residual_store.flush_all(&mut persistent_residual).unwrap();

        assert_eq!(persistent_residual_accum, direct_residual_accum);
        assert_eq!(persistent_residual, direct_residual);
    }

    #[test]
    fn reserved_streaming_dirty_backend_fails_before_workspace_creation() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let error =
            match StandardMfsDirtyCpuExecutor::for_backend(StandardMfsBackend::Metal, geometry) {
                Ok(_) => panic!("reserved backend unexpectedly built a dirty executor"),
                Err(error) => error,
            };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'metal' is not implemented"),
            "{error}"
        );
    }
}
