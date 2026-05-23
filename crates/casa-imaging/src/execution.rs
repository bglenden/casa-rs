// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal imaging execution plans and CPU workspaces.

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
    ImageGeometry, ImagingError, StandardMfsExecutionConfig, StandardMfsPlannedWeightedSample,
    StandardMfsPlannedWeightedSampleRunBlock, VisibilityBatch,
    gridder::{
        PositiveTapSet, STANDARD_GRIDDER_SUPPORT, STANDARD_GRIDDER_TAP_COUNT, StandardGridder,
        StandardMfsTapCensus, StandardMfsTapSkipReason, TapAxisSpan,
    },
    profile,
};

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

        let tiles_x = tile_count_1d(grid_shape[0], tile_shape[0], tile_origin[0]);
        let tiles_y = tile_count_1d(grid_shape[1], tile_shape[1], tile_origin[1]);
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
            let (interior_x0, interior_x1) =
                tile_bounds_1d(tile_x, grid_shape[0], tile_shape[0], tile_origin[0]);
            for tile_y in 0..tiles_y {
                let (interior_y0, interior_y1) =
                    tile_bounds_1d(tile_y, grid_shape[1], tile_shape[1], tile_origin[1]);
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
        let tile_x = owner_1d(
            center_x,
            self.grid_shape[0],
            self.tile_shape[0],
            self.tile_origin[0],
        )?;
        let tile_y = owner_1d(
            center_y,
            self.grid_shape[1],
            self.tile_shape[1],
            self.tile_origin[1],
        )?;
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

fn owner_1d(coord: usize, grid_len: usize, edge: usize, origin: usize) -> Option<usize> {
    if coord >= grid_len {
        return None;
    }
    if origin == 0 {
        Some(coord / edge)
    } else if coord < origin {
        Some(0)
    } else {
        Some(1 + (coord - origin) / edge)
    }
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
    fn finite_visibility(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY != 0
    }

    #[allow(dead_code)]
    fn psf_only(self) -> bool {
        self.flags & STANDARD_MFS_TILE_FLAG_PSF_ONLY != 0
    }

    #[allow(dead_code)]
    fn grid_weight(self) -> f32 {
        self.raw_weight * self.sumwt_factor
    }

    fn queue_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(STANDARD_MFS_QUEUE_SAMPLE_SLOP_BYTES)
    }

    fn estimated_work(self) -> usize {
        usize::from(self.tap_count())
    }

    fn tap_count(self) -> u8 {
        STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT) as u8
    }

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

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsVisibilityRow {
    uvw_m: [f64; 3],
    spw_id: usize,
    channel_hz: Arc<[f64]>,
    data: Array2<Complex32>,
    flag: Array2<bool>,
    weight: Arc<[f32]>,
    weight_spectrum: Option<Array2<f32>>,
    gridable: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
struct StandardMfsTileVisibilityRun {
    row: Option<Arc<StandardMfsVisibilityRow>>,
    channel_range: Range<usize>,
    selected_correlations: Arc<[usize]>,
    tap_centers: Arc<[[u32; 2]]>,
    first_input_seq: u64,
    center_x: Vec<u32>,
    center_y: Vec<u32>,
    flags: Vec<u16>,
    raw_weight: Vec<f32>,
    sumwt_factor: Vec<f32>,
    u_lambda: Vec<f64>,
    v_lambda: Vec<f64>,
    w_lambda: Vec<f64>,
    visibility: Vec<Complex32>,
    bytes: usize,
    estimated_work: usize,
}

type StandardMfsTileQueueChunk = StandardMfsTileVisibilityRun;

impl StandardMfsTileVisibilityRun {
    fn empty() -> Self {
        Self::with_capacity(0, u64::MAX)
    }

    fn with_capacity(capacity: usize, first_input_seq: u64) -> Self {
        Self {
            row: None,
            channel_range: 0..0,
            selected_correlations: Arc::from([]),
            tap_centers: Arc::from([]),
            first_input_seq,
            center_x: Vec::with_capacity(capacity),
            center_y: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
            raw_weight: Vec::with_capacity(capacity),
            sumwt_factor: Vec::with_capacity(capacity),
            u_lambda: Vec::with_capacity(capacity),
            v_lambda: Vec::with_capacity(capacity),
            w_lambda: Vec::with_capacity(capacity),
            visibility: Vec::with_capacity(capacity),
            bytes: 0,
            estimated_work: 0,
        }
    }

    fn push_sample(&mut self, sample: StandardMfsTileQueueSample) {
        if self.is_empty() {
            self.first_input_seq = sample.input_seq;
        }
        self.center_x.push(sample.center_x);
        self.center_y.push(sample.center_y);
        self.flags.push(sample.flags);
        self.raw_weight.push(sample.raw_weight);
        self.sumwt_factor.push(sample.sumwt_factor);
        self.u_lambda.push(sample.u_lambda);
        self.v_lambda.push(sample.v_lambda);
        self.w_lambda.push(sample.w_lambda);
        self.visibility.push(sample.visibility);
        self.bytes = self
            .bytes
            .saturating_add(StandardMfsTileQueueSample::queue_bytes());
        self.estimated_work = self.estimated_work.saturating_add(sample.estimated_work());
    }

    fn append_run(&mut self, mut run: StandardMfsTileVisibilityRun) {
        if run.is_empty() {
            return;
        }
        if self.is_empty() {
            self.first_input_seq = run.first_input_seq;
        }
        self.center_x.append(&mut run.center_x);
        self.center_y.append(&mut run.center_y);
        self.flags.append(&mut run.flags);
        self.raw_weight.append(&mut run.raw_weight);
        self.sumwt_factor.append(&mut run.sumwt_factor);
        self.u_lambda.append(&mut run.u_lambda);
        self.v_lambda.append(&mut run.v_lambda);
        self.w_lambda.append(&mut run.w_lambda);
        self.visibility.append(&mut run.visibility);
        self.bytes = self.bytes.saturating_add(run.bytes);
        self.estimated_work = self.estimated_work.saturating_add(run.estimated_work);
    }

    fn queue_bytes(&self) -> usize {
        self.bytes.saturating_add(STANDARD_MFS_QUEUE_RUN_SLOP_BYTES)
    }

    fn len(&self) -> usize {
        self.visibility.len()
    }

    fn is_empty(&self) -> bool {
        self.visibility.is_empty()
    }

    fn finite_visibility_at(&self, sample_index: usize) -> bool {
        self.flags[sample_index] & STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY != 0
    }

    fn psf_only_at(&self, sample_index: usize) -> bool {
        self.flags[sample_index] & STANDARD_MFS_TILE_FLAG_PSF_ONLY != 0
    }

    fn grid_weight_at(&self, sample_index: usize) -> f32 {
        self.raw_weight[sample_index] * self.sumwt_factor[sample_index]
    }

    fn visibility_at(&self, sample_index: usize) -> Complex32 {
        self.visibility[sample_index]
    }

    fn positive_taps_at(
        &self,
        sample_index: usize,
        gridder: &StandardGridder,
    ) -> Result<PositiveTapSet, ImagingError> {
        let sample = StandardMfsTileQueueSample {
            center_x: self.center_x[sample_index],
            center_y: self.center_y[sample_index],
            flags: self.flags[sample_index],
            raw_weight: self.raw_weight[sample_index],
            sumwt_factor: self.sumwt_factor[sample_index],
            u_lambda: self.u_lambda[sample_index],
            v_lambda: self.v_lambda[sample_index],
            w_lambda: self.w_lambda[sample_index],
            visibility: self.visibility[sample_index],
            input_seq: 0,
        };
        sample.positive_taps(gridder)
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
        }
    }

    fn ready_head(&self, tile_id: StandardMfsTileId) -> Option<StandardMfsTileInboxReadyHead> {
        self.runs.front().map(|run| StandardMfsTileInboxReadyHead {
            tile_id,
            generation: self.generation,
            first_input_seq: run.first_input_seq,
            estimated_work: self.queued_work_estimate,
        })
    }
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
    output: T,
    timing: StandardMfsTileTaskTiming,
}

#[allow(dead_code)]
struct StandardMfsTileInboxSchedulerOutput<T> {
    task_outputs: Vec<StandardMfsTileInboxTaskOutput<T>>,
    worker_profiles: Vec<StandardMfsTileWorkerProfile>,
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

#[allow(dead_code)]
struct StandardMfsTileInboxShared {
    tiles: Vec<Arc<StandardMfsTileInboxRuntime>>,
    ready: Arc<(Mutex<StandardMfsTileInboxReadyState>, Condvar)>,
    ready_sample_min: usize,
}

impl StandardMfsTileInboxShared {
    fn new(tile_count: usize, ready_sample_min: usize) -> Self {
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
        }
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
        while let Some(run) = runs.pop_front() {
            let run_bytes = run.queue_bytes();
            stats.runs += 1;
            stats.samples += run.len();
            stats.bytes = stats.bytes.saturating_add(run_bytes);
            stats.estimated_work = stats.estimated_work.saturating_add(run.estimated_work);
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
            queue.ready_head(tile_id)
        } else {
            None
        };
        stats
    }

    fn record_published_runs(
        &self,
        published: StandardMfsTileInboxPublishStats,
    ) -> Result<(), ImagingError> {
        if published.runs == 0 {
            return Ok(());
        }
        let (lock, cvar) = &*self.ready;
        let mut ready = lock.lock().map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS tile inbox scheduler lock was poisoned".to_string(),
            )
        })?;
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
            cvar.notify_one();
        } else if published.runs > 0 {
            ready.stats.ready_deferred_runs += published.runs;
            ready.stats.ready_deferred_samples += published.samples;
        }
        Ok(())
    }

    fn try_enqueue_runs(
        &self,
        tile_id: StandardMfsTileId,
        runs: &mut VecDeque<StandardMfsTileVisibilityRun>,
    ) -> Result<bool, ImagingError> {
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
                drop(queue);
                self.record_published_runs(published)?;
                Ok(true)
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let (lock, _) = &*self.ready;
                let mut ready = lock.lock().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                    )
                })?;
                ready.stats.try_lock_misses += 1;
                Ok(false)
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
    ) -> Result<(), ImagingError> {
        if runs.is_empty() {
            return Ok(());
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
        self.record_published_runs(published)
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
            let mut samples = StandardMfsTileQueueChunk::empty();
            let mut run_count = 0usize;
            let mut bytes = 0usize;
            while let Some(run) = queue.runs.pop_front() {
                run_count += 1;
                bytes = bytes.saturating_add(run.queue_bytes());
                samples.append_run(run);
            }
            let estimated_work = samples.estimated_work;
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

            return Ok(Some(StandardMfsDrainedTileWork {
                tile_id: head.tile_id,
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
                if let Some(head) = queue.ready_head(tile.tile_id) {
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

    fn finish_work(&self, tile_id: StandardMfsTileId, bytes: usize) -> Result<(), ImagingError> {
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
            queue.active = false;
            if !queue.runs.is_empty()
                && !queue.ready_enqueued
                && self.queue_is_ready_for_workers(&queue)
            {
                queue.ready_enqueued = true;
                queue.generation = queue.generation.saturating_add(1);
                queue.ready_head(tile_id)
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
}

impl StandardMfsTileInboxProducer {
    fn new(shared: Arc<StandardMfsTileInboxShared>) -> Self {
        let pending = (0..shared.tiles.len()).map(|_| VecDeque::new()).collect();
        Self { shared, pending }
    }

    fn enqueue_run(
        &mut self,
        tile_id: StandardMfsTileId,
        run: StandardMfsTileVisibilityRun,
    ) -> Result<(), ImagingError> {
        let run_bytes = run.queue_bytes();
        let mut runs = VecDeque::new();
        let pending = self.pending.get_mut(tile_id.index()).ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "standard MFS tile id {} is out of range",
                tile_id.index()
            ))
        })?;
        let old_pending_runs = pending.len();
        let old_pending_bytes = pending.iter().map(|run| run.queue_bytes()).sum::<usize>();
        runs.append(pending);
        runs.push_back(run);
        if self.shared.try_enqueue_runs(tile_id, &mut runs)? {
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
            self.shared
                .enqueue_runs_blocking(StandardMfsTileId(tile_index as u32), &mut runs)?;
            let (lock, _) = &*self.shared.ready;
            let mut ready = lock.lock().map_err(|_| {
                ImagingError::InvalidRequest(
                    "standard MFS tile inbox scheduler lock was poisoned".to_string(),
                )
            })?;
            ready.stats.pending_runs = ready.stats.pending_runs.saturating_sub(pending_runs);
            ready.stats.pending_bytes = ready.stats.pending_bytes.saturating_sub(pending_bytes);
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
                    drop(work.samples);
                    match execution {
                        Ok((output, timing)) => {
                            worker_profile.record_task(task, timing);
                            shared.finish_work(task.tile_id, bytes)?;
                            outputs.push(StandardMfsTileInboxTaskOutput {
                                tile_id: task.tile_id,
                                output,
                                timing,
                            });
                        }
                        Err(error) => {
                            shared.finish_work(task.tile_id, bytes)?;
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
            let mut producer = StandardMfsTileInboxProducer::new(shared_for_producer);
            let result = produce_runs(&mut |tile_id, run| producer.enqueue_run(tile_id, run));
            if result.is_ok() {
                producer.flush_pending_blocking()
            } else {
                result
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
        producer_active: ready.stats.producer_active,
        producer_worker_overlap: ready.stats.producer_worker_overlap,
        worker_active_union: ready.stats.worker_active_union,
        neither_active: ready.stats.neither_active,
    };
    drop(ready);
    all_outputs.sort_by_key(|output| output.tile_id);
    Ok(StandardMfsTileInboxSchedulerOutput {
        task_outputs: all_outputs,
        worker_profiles: all_worker_profiles,
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
    eprintln!(
        "standard_mfs_tile_inbox_scheduler_summary stage={} requested_threads={} actual_threads={} tile_shape={}x{} tile_anchor={} tile_origin={}x{} tile_count={} inbox_worker_count={} ready_sample_min={} enqueued_runs={} enqueued_samples={} enqueued_bytes={} queued_bytes_high_water={} ready_deferred_runs={} ready_deferred_samples={} pending_runs={} pending_bytes={} pending_bytes_high_water={} try_lock_misses={} ready_heads_pushed={} worker_drains={} worker_runs={} worker_samples={} worker_tap_visits={} avg_runs_per_drain={:.3} avg_samples_per_run={:.3} producer_active_ms={:.3} worker_active_union_ms={:.3} producer_worker_overlap_ms={:.3} producer_only_ms={:.3} worker_only_ms={:.3} neither_active_ms={:.3} producer_preprocess_total_ms={:.3} worker_task_count={} worker_samples_by_worker={} worker_tap_visits_by_worker={} worker_active_total_ms={:.3} worker_active={} worker_capacity_ms={:.3} worker_utilization_pct={:.3} worker_tail_idle_ms={:.3} active_tile_skips={} stale_heap_entries={} wait_with_queued_bytes_events={} task_outputs={} stage_total_ms={:.3}",
        inputs.stage,
        inputs.requested_threads,
        inputs.output.worker_profiles.len(),
        inputs.partition.tile_shape()[0],
        inputs.partition.tile_shape()[1],
        inputs.partition.anchor_label(),
        inputs.partition.tile_origin()[0],
        inputs.partition.tile_origin()[1],
        inputs.partition.tile_count(),
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
        inputs.output.stats.active_tile_skips,
        inputs.output.stats.stale_heap_entries,
        inputs.output.stats.wait_with_queued_bytes_events,
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
            standard_mfs_tile_partition_for_gridder_with_config(gridder, execution_config)?;
        let resident_tile_limit = standard_mfs_tile_resident_limit(
            &partition,
            execution_config.fixed_tile_resident_bytes,
        );
        Ok(Self {
            gridder,
            partition,
            resident_tile_limit,
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
                run_accumulator.push_sample(
                    tile_id,
                    StandardMfsTileQueueSample {
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
                    },
                )?;
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            return Ok(accumulation);
        }

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
        for sample_index in 0..samples.len() {
            let taps = samples.positive_taps_at(sample_index, self.gridder)?;
            let grid_weight = samples.grid_weight_at(sample_index);
            if !(grid_weight.is_finite() && grid_weight > 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "standard MFS tile inbox dirty sample has invalid queued weight".to_string(),
                ));
            }
            let grid_weight = f64::from(grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            if samples.finite_visibility_at(sample_index) {
                let visibility = samples.visibility_at(sample_index);
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
                debug_assert!(samples.psf_only_at(sample_index));
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
        for sample_index in 0..samples.len() {
            let taps = samples.positive_taps_at(sample_index, self.gridder)?;
            let grid_weight = samples.grid_weight_at(sample_index);
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
        for sample_index in 0..samples.len() {
            let taps = samples.positive_taps_at(sample_index, self.gridder)?;
            let residual_weight = samples.grid_weight_at(sample_index);
            if !(residual_weight.is_finite() && residual_weight > 0.0) {
                return Err(ImagingError::InvalidRequest(
                    "standard MFS tile inbox residual sample has invalid queued weight".to_string(),
                ));
            }
            let residual_weight = f64::from(residual_weight);
            if let Some(model_grid) = model_grid {
                self.gridder
                    .degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
                        model_grid,
                        &mut buffer.residual_grid,
                        &taps,
                        samples.visibility_at(sample_index),
                        residual_weight,
                        offset,
                    );
            } else {
                let visibility = samples.visibility_at(sample_index);
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
        Ok((
            gridded_samples,
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
            for &sample in samples {
                accumulation.valid_samples += 1;
                accumulation.planned_samples += 1;
                run_accumulator.push_sample(
                    tile_id,
                    StandardMfsTileQueueSample {
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
                    },
                )?;
                *next_input_seq = (*next_input_seq).saturating_add(1);
            }
            return Ok(accumulation);
        }

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

#[cfg(target_os = "macos")]
pub(crate) struct StandardMfsMetalExecutor<'a> {
    gridder: &'a StandardGridder,
    partition: StandardMfsFixedTilePartition,
    backend: MetalDirtyBackend,
}

#[cfg(target_os = "macos")]
const _: () = {
    assert!(STANDARD_GRIDDER_SUPPORT == 3);
    assert!(STANDARD_GRIDDER_TAP_COUNT == 7);
};

#[cfg(target_os = "macos")]
impl<'a> StandardMfsMetalExecutor<'a> {
    pub(crate) fn new_with_resident_bytes(
        gridder: &'a StandardGridder,
        _resident_bytes: Option<usize>,
    ) -> Result<Self, ImagingError> {
        let partition = standard_mfs_tile_partition_for_gridder(gridder)?;
        Ok(Self {
            gridder,
            partition,
            backend: MetalDirtyBackend::new()?,
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetalComplex32 {
    re: f32,
    im: f32,
}

#[cfg(target_os = "macos")]
struct MetalDirtyBackend {
    device: objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLDevice>>,
    queue: objc2::rc::Retained<objc2::runtime::ProtocolObject<dyn objc2_metal::MTLCommandQueue>>,
    pipeline: objc2::rc::Retained<
        objc2::runtime::ProtocolObject<dyn objc2_metal::MTLComputePipelineState>,
    >,
}

#[cfg(target_os = "macos")]
impl MetalDirtyBackend {
    fn new() -> Result<Self, ImagingError> {
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
        let _ = MTLResourceOptions::StorageModeShared;
        Ok(Self {
            device,
            queue,
            pipeline,
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
}

#[cfg(target_os = "macos")]
fn metal_error(
    context: &str,
    error: objc2::rc::Retained<objc2_foundation::NSError>,
) -> ImagingError {
    ImagingError::Unsupported(format!(
        "standard MFS backend 'metal' failed to {context}: {error:?}"
    ))
}

#[cfg(target_os = "macos")]
#[link(name = "CoreGraphics", kind = "framework")]
unsafe extern "C" {}

#[cfg(target_os = "macos")]
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
    use super::{
        STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY, STANDARD_MFS_TILE_FLAG_PSF_ONLY,
        StandardMfsBackend, StandardMfsBlockTileBuckets, StandardMfsCpuExecutor,
        StandardMfsDirtyCpuExecutor, StandardMfsFixedTilePartition,
        StandardMfsRowBlockSampleAccess, StandardMfsSampleRef, StandardMfsTileId,
        StandardMfsTiledCpuExecutor,
    };
    use crate::{
        ImageGeometry, StandardMfsExecutionConfig, StandardMfsPlannedWeightedSample,
        VisibilityBatch, gridder::StandardGridder,
    };
    use num_complex::{Complex32, Complex64};
    use std::{mem::size_of, time::Duration};

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
    fn direct_resident_tiles_match_scratch_tile_dirty_and_residual_paths() {
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
        let scratch = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                fixed_tile_resident_bytes: Some(1),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: false,
            },
        )
        .unwrap();
        let direct = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: false,
            },
        )
        .unwrap();
        let shape = gridder.grid_shape();
        let mut scratch_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut scratch_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let scratch_accum = scratch
            .accumulate_dirty_grids(&batches, &mut scratch_psf, &mut scratch_dirty)
            .unwrap();
        let mut direct_psf = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let mut direct_dirty = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_accum = direct
            .accumulate_dirty_grids(&batches, &mut direct_psf, &mut direct_dirty)
            .unwrap();

        assert_eq!(scratch_accum, direct_accum);
        assert_eq!(scratch_psf, direct_psf);
        assert_eq!(scratch_dirty, direct_dirty);

        let mut scratch_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let scratch_residual_accum = scratch
            .accumulate_residual_grid(&batches, None, &mut scratch_residual)
            .unwrap();
        let mut direct_residual = ndarray::Array2::<Complex64>::zeros((shape[0], shape[1]));
        let direct_residual_accum = direct
            .accumulate_residual_grid(&batches, None, &mut direct_residual)
            .unwrap();

        assert_eq!(scratch_residual_accum, direct_residual_accum);
        assert_eq!(scratch_residual, direct_residual);
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

    fn test_planned_weighted_sample(
        center_x: u32,
        center_y: u32,
    ) -> StandardMfsPlannedWeightedSample {
        StandardMfsPlannedWeightedSample {
            u_lambda: 0.0,
            v_lambda: 0.0,
            center_x,
            center_y,
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
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
                fixed_tile_use_planned_run_blocks: true,
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
        let output = super::run_standard_mfs_tile_inbox_scheduler(
            &partition,
            2,
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
    }

    #[test]
    fn tile_inbox_producer_pending_retries_fifo_after_try_lock_miss() {
        let shared = std::sync::Arc::new(super::StandardMfsTileInboxShared::new(1, 1));
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
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
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
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
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
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 2,
                fixed_tile_use_planned_run_blocks: false,
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
