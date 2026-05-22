// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal imaging execution plans and CPU workspaces.

use std::{
    collections::BTreeMap,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    ImageGeometry, ImagingError, StandardMfsExecutionConfig, VisibilityBatch,
    gridder::{
        PositiveTapSet, STANDARD_GRIDDER_SUPPORT, STANDARD_GRIDDER_TAP_COUNT, StandardGridder,
        StandardMfsTapCensus, StandardMfsTapSkipReason,
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
#[derive(Debug, Clone, Copy, PartialEq)]
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

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct StandardMfsTileBucketSample {
    pub(crate) sample_index: u32,
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StandardMfsBlockTileBuckets {
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

        let tile_count = partition.tile_count();
        let mut counts = vec![0usize; tile_count];
        let mut accepted_samples = 0usize;
        let mut skipped_samples = 0usize;

        for batch in batches {
            for sample_index in 0..batch.len() {
                let Some((tile_id, _center, _flags, _grid_weight, _tap_count)) =
                    plan_dirty_tile_sample(gridder, partition, batch, sample_index)
                else {
                    skipped_samples += 1;
                    continue;
                };
                counts[tile_id.index()] += 1;
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
                sample_index: 0,
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
                let Some((tile_id, center, flags, grid_weight, tap_count)) =
                    plan_dirty_tile_sample(gridder, partition, batch, sample_index)
                else {
                    flat_sample_index += 1;
                    continue;
                };
                let output_index = fill_offsets[tile_id.index()];
                fill_offsets[tile_id.index()] += 1;
                samples[output_index] = StandardMfsTileBucketSample {
                    sample_index: flat_sample_index as u32,
                    center_x: center[0] as u32,
                    center_y: center[1] as u32,
                    kernel_u: 0,
                    kernel_v: 0,
                    support_id: 0,
                    flags,
                    grid_weight,
                    tap_count,
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
        batch: &VisibilityBatch,
    ) -> Result<(Self, StandardMfsTiledResidualAccumulation), ImagingError> {
        batch.validate()?;
        if batch.len() > u32::MAX as usize {
            return Err(ImagingError::InvalidRequest(
                "standard MFS residual tile bucket block has too many samples".to_string(),
            ));
        }
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let mut per_tile = vec![Vec::<StandardMfsTileBucketSample>::new(); partition.tile_count()];
        for sample_index in 0..batch.len() {
            let weight = batch.weight[sample_index];
            let observed_visibility = batch.visibility[sample_index];
            if !batch.gridable[sample_index] {
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
            let Some(taps) = gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
            else {
                accumulation.skipped_out_of_grid += 1;
                continue;
            };
            accumulation.planned_samples += 1;
            let sumwt_factor = batch.sumwt_factor[sample_index];
            if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                accumulation.skipped_invalid_sumwt += 1;
                continue;
            }
            let residual_weight = weight * sumwt_factor;
            if !(residual_weight.is_finite() && residual_weight > 0.0) {
                accumulation.skipped_invalid_sumwt += 1;
                continue;
            }
            let center = taps.center();
            let Some(tile_id) = partition.owner(center[0], center[1]) else {
                accumulation.skipped_out_of_grid += 1;
                continue;
            };
            per_tile[tile_id.index()].push(StandardMfsTileBucketSample {
                sample_index: sample_index as u32,
                center_x: center[0] as u32,
                center_y: center[1] as u32,
                kernel_u: 0,
                kernel_v: 0,
                support_id: 0,
                flags: STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY,
                grid_weight: residual_weight,
                tap_count: STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT)
                    as u8,
            });
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

    pub(crate) fn accepted_samples(&self) -> usize {
        self.accepted_samples
    }

    pub(crate) fn skipped_samples(&self) -> usize {
        self.skipped_samples
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
        self.samples
            .capacity()
            .saturating_mul(std::mem::size_of::<StandardMfsTileBucketSample>())
            .saturating_add(
                self.tile_offsets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.nonempty_tiles
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StandardMfsTileId>()),
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StandardMfsTileTask {
    pub(crate) tile_id: StandardMfsTileId,
    pub(crate) sample_count: usize,
    pub(crate) estimated_tap_visits: usize,
}

fn plan_dirty_tile_sample(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batch: &VisibilityBatch,
    sample_index: usize,
) -> Option<(StandardMfsTileId, [usize; 2], u16, f32, u8)> {
    if !batch.gridable[sample_index] {
        return None;
    }
    let weight = batch.weight[sample_index];
    let sumwt_factor = batch.sumwt_factor[sample_index];
    if !(weight.is_finite() && weight > 0.0 && sumwt_factor.is_finite() && sumwt_factor > 0.0) {
        return None;
    }
    let taps =
        gridder.plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])?;
    let center = taps.center();
    let tile_id = partition.owner(center[0], center[1])?;
    let grid_weight = weight * sumwt_factor;
    if !(grid_weight.is_finite() && grid_weight > 0.0) {
        return None;
    }
    let flags = if finite_visibility(batch.visibility[sample_index]) {
        STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY
    } else {
        STANDARD_MFS_TILE_FLAG_PSF_ONLY
    };
    Some((
        tile_id,
        center,
        flags,
        grid_weight,
        STANDARD_GRIDDER_TAP_COUNT.saturating_mul(STANDARD_GRIDDER_TAP_COUNT) as u8,
    ))
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
            let Some((tile_id, sample_center, _flags, _grid_weight, _tap_count)) =
                plan_dirty_tile_sample(gridder, partition, batch, sample_index)
            else {
                continue;
            };
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
    for dx in -2..=2 {
        for dy in -2..=2 {
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
}

#[derive(Clone, Copy, Debug, Default)]
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
    flush_duration: Duration,
    tile_flush_count: usize,
    tile_eviction_count: usize,
    started_at: Instant,
}

impl StandardMfsTileSchedulerStageProfile {
    fn new(
        stage: &'static str,
        partition: &StandardMfsFixedTilePartition,
        resident_tile_limit: usize,
    ) -> Self {
        Self {
            stage,
            tile_count: partition.tile_count(),
            tile_shape: partition.tile_shape(),
            tile_origin: partition.tile_origin(),
            tile_anchor: partition.anchor_label(),
            resident_tile_limit,
            blocks: Vec::new(),
            flush_duration: Duration::ZERO,
            tile_flush_count: 0,
            tile_eviction_count: 0,
            started_at: Instant::now(),
        }
    }

    fn record(&mut self, block: StandardMfsTileSchedulerBlockProfile) {
        if profile::standard_mfs_profile_block_detail_enabled() {
            log_tiled_scheduler_block(self.stage, block);
        }
        self.blocks.push(block);
    }

    fn add_flush_duration(&mut self, duration: Duration) {
        self.flush_duration += duration;
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
        eprintln!(
            "standard_mfs_tile_scheduler_summary stage={} requested_threads={} actual_threads={} tile_shape={}x{} tile_anchor={} tile_origin={}x{} tile_count={} resident_tile_limit={} max_live_row_blocks=1 block_count={} task_count={} samples_total={} tap_visits_total={} task_samples={} task_tap_visits={} largest_task_samples={} largest_task_tap_visits={} bucket_bytes_total={} bucket_bytes_max={} bucket_build_total_ms={:.3} bucket_build={} local_alloc_zero_total_ms={:.3} local_alloc_zero={} worker_replan_grid_total_ms={:.3} worker_replan_grid={} block_wall_total_ms={:.3} block_wall={} merge_total_ms={:.3} merge={} tile_flush_ms={:.3} tile_flush_count={} tile_eviction_count={} merged_tiles={} active_tile_wait_events=0 tasks_skipped_due_to_active_tile=0 stage_total_ms={:.3}",
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
            bucket_bytes_total,
            bucket_bytes_max,
            duration_total_ms(&bucket_build),
            duration_stats_triplet(&bucket_build),
            duration_total_ms(&local_alloc_zero),
            duration_stats_triplet(&local_alloc_zero),
            duration_total_ms(&worker_replan_grid),
            duration_stats_triplet(&worker_replan_grid),
            duration_total_ms(&block_wall),
            duration_stats_triplet(&block_wall),
            duration_total_ms(&merge),
            duration_stats_triplet(&merge),
            profile::millis(self.flush_duration),
            self.tile_flush_count,
            self.tile_eviction_count,
            self.blocks
                .iter()
                .map(|block| block.merged_tiles)
                .sum::<usize>(),
            profile::millis(self.started_at.elapsed()),
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

        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                std::slice::from_ref(batch),
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            let block_profile = self.accumulate_dirty_block(
                batch,
                &buckets,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
            if standard_mfs_per_block_flush_enabled() {
                let flush_started = Instant::now();
                cache.flush_all();
                scheduler_profile.add_flush_duration(flush_started.elapsed());
            }
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

        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                std::slice::from_ref(batch),
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            let block_profile = self.accumulate_psf_block(
                batch,
                &buckets,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
            if standard_mfs_per_block_flush_enabled() {
                let flush_started = Instant::now();
                cache.flush_all();
                scheduler_profile.add_flush_duration(flush_started.elapsed());
            }
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
        batch: &VisibilityBatch,
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
                    self.grid_dirty_tile_task(batch, buckets, task.tile_id)?;
                let merge_started = Instant::now();
                merge_dirty_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.add(task_accumulation);
                task_timing.add(timing);
                merged_count += 1;
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: merge_duration,
                    merged_tiles: merged_count,
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
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
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
                        worker_outputs.push(self.grid_dirty_tile_task(
                            batch,
                            buckets,
                            task.tile_id,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled dirty worker panicked".to_string(),
                    )
                })??);
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
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: merge_duration,
                merged_tiles: merged_count,
            },
        ))
    }

    fn accumulate_dirty_grids_direct(
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
        let store = DirectDirtyTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "dirty",
            &self.partition,
            self.resident_tile_limit,
        );

        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                std::slice::from_ref(batch),
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            let block_profile = self.accumulate_dirty_block_direct(
                batch,
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

    fn accumulate_psf_grid_direct(
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
        let store = DirectPsfTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "psf",
            &self.partition,
            self.resident_tile_limit,
        );

        for batch in batches {
            batch.validate()?;
            accumulation.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(accumulation.max_abs_w_lambda, |max_value, value| {
                    max_value.max(value.abs())
                });
            let bucket_started = Instant::now();
            let buckets = StandardMfsBlockTileBuckets::build_for_dirty(
                self.gridder,
                &self.partition,
                std::slice::from_ref(batch),
            )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.skipped_samples += buckets.skipped_samples();
            let block_profile = self.accumulate_psf_block_direct(
                batch,
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

    fn grid_dirty_tile_task(
        &self,
        batch: &VisibilityBatch,
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
        for sample in buckets.tile_samples(tile_id) {
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled dirty bucket lost its tap plan".to_string(),
                    )
                })?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            if sample.finite_visibility() {
                let observed_visibility = batch.visibility[sample_index];
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
        batch: &VisibilityBatch,
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
                    self.grid_dirty_tile_task_direct(batch, buckets, task.tile_id, store)?;
                accumulation.add(task_accumulation);
                task_timing.add(timing);
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let mut worker_outputs =
                        Vec::<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        worker_outputs.push(self.grid_dirty_tile_task_direct(
                            batch,
                            buckets,
                            task.tile_id,
                            store,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled dirty worker panicked".to_string(),
                    )
                })??);
            }
            Ok::<_, ImagingError>(())
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (task_accumulation, timing) in outputs.into_iter().flatten() {
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
            },
        ))
    }

    fn grid_dirty_tile_task_direct(
        &self,
        batch: &VisibilityBatch,
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
        for sample in buckets.tile_samples(tile_id) {
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled dirty bucket lost its tap plan".to_string(),
                    )
                })?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let grid_weight = f64::from(sample.grid_weight);
            accumulation.normalization_sumwt += grid_weight;
            accumulation.reported_sumwt += grid_weight;
            accumulation.gridded_samples += 1;
            if sample.finite_visibility() {
                let observed_visibility = batch.visibility[sample_index];
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

    fn accumulate_psf_block(
        &self,
        batch: &VisibilityBatch,
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
                    self.grid_psf_tile_task(batch, buckets, task.tile_id)?;
                let merge_started = Instant::now();
                merge_psf_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.add(task_accumulation);
                task_timing.add(timing);
                merged_count += 1;
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: merge_duration,
                    merged_tiles: merged_count,
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
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
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
                        worker_outputs.push(self.grid_psf_tile_task(
                            batch,
                            buckets,
                            task.tile_id,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled PSF worker panicked".to_string(),
                    )
                })??);
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
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: merge_duration,
                merged_tiles: merged_count,
            },
        ))
    }

    fn grid_psf_tile_task(
        &self,
        batch: &VisibilityBatch,
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
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled PSF bucket lost its tap plan".to_string(),
                    )
                })?;
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
        batch: &VisibilityBatch,
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
                    self.grid_psf_tile_task_direct(batch, buckets, task.tile_id, store)?;
                accumulation.add(task_accumulation);
                task_timing.add(timing);
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let mut worker_outputs =
                        Vec::<(StandardMfsDirtyAccumulation, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        worker_outputs.push(self.grid_psf_tile_task_direct(
                            batch,
                            buckets,
                            task.tile_id,
                            store,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled PSF worker panicked".to_string(),
                    )
                })??);
            }
            Ok::<_, ImagingError>(())
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (task_accumulation, timing) in outputs.into_iter().flatten() {
            accumulation.add(task_accumulation);
            task_timing.add(timing);
        }
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
            },
        ))
    }

    fn grid_psf_tile_task_direct(
        &self,
        batch: &VisibilityBatch,
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
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled PSF bucket lost its tap plan".to_string(),
                    )
                })?;
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

        for batch in batches {
            batch.validate()?;
            let bucket_started = Instant::now();
            let (buckets, block_accumulation) =
                StandardMfsBlockTileBuckets::build_for_residual_refresh(
                    self.gridder,
                    &self.partition,
                    batch,
                )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.add_residual(block_accumulation);
            let block_profile = self.accumulate_residual_block(
                batch,
                &buckets,
                model_grid,
                &mut cache,
                &mut accumulation,
                bucket_build,
            )?;
            scheduler_profile.record(block_profile);
            if standard_mfs_per_block_flush_enabled() {
                let flush_started = Instant::now();
                cache.flush_all();
                scheduler_profile.add_flush_duration(flush_started.elapsed());
            }
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
        batch: &VisibilityBatch,
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
                    self.grid_residual_tile_task(batch, buckets, model_grid, task.tile_id)?;
                let merge_started = Instant::now();
                merge_residual_tile_buffer_into_cache(cache, buffer)?;
                merge_duration += merge_started.elapsed();
                accumulation.gridded_residual_samples += gridded_samples;
                task_timing.add(timing);
                merged_count += 1;
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: merge_duration,
                    merged_tiles: merged_count,
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        let mut outputs = Vec::<Vec<(ResidualTileBuffer, usize, StandardMfsTileTaskTiming)>>::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let mut worker_outputs =
                        Vec::<(ResidualTileBuffer, usize, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        worker_outputs.push(self.grid_residual_tile_task(
                            batch,
                            buckets,
                            model_grid,
                            task.tile_id,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled residual worker panicked".to_string(),
                    )
                })??);
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
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: merge_duration,
                merged_tiles: merged_count,
            },
        ))
    }

    fn grid_residual_tile_task(
        &self,
        batch: &VisibilityBatch,
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
        for sample in buckets.tile_samples(tile_id) {
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS tiled residual bucket lost its tap plan".to_string(),
                    )
                })?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let observed_visibility = batch.visibility[sample_index];
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
        let mut accumulation = StandardMfsTiledResidualAccumulation::default();
        let store = DirectResidualTileStore::new(&self.partition);
        let mut scheduler_profile = StandardMfsTileSchedulerStageProfile::new(
            "residual",
            &self.partition,
            self.resident_tile_limit,
        );

        for batch in batches {
            batch.validate()?;
            let bucket_started = Instant::now();
            let (buckets, block_accumulation) =
                StandardMfsBlockTileBuckets::build_for_residual_refresh(
                    self.gridder,
                    &self.partition,
                    batch,
                )?;
            let bucket_build = bucket_started.elapsed();
            accumulation.add_residual(block_accumulation);
            let block_profile = self.accumulate_residual_block_direct(
                batch,
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

    fn accumulate_residual_block_direct(
        &self,
        batch: &VisibilityBatch,
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
                    batch,
                    buckets,
                    model_grid,
                    task.tile_id,
                    store,
                )?;
                accumulation.gridded_residual_samples += gridded_samples;
                task_timing.add(timing);
            }
            return Ok(tiled_scheduler_block_profile(
                StandardMfsTileSchedulerBlockInputs {
                    worker_count,
                    tasks: &tasks,
                    buckets,
                    bucket_build,
                    task_timing,
                    block_wall: started.elapsed(),
                    merge: Duration::ZERO,
                    merged_tiles: tasks.len(),
                },
            ));
        }

        let next_task = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                handles.push(scope.spawn(|| {
                    let mut worker_outputs = Vec::<(usize, StandardMfsTileTaskTiming)>::new();
                    loop {
                        let task_index = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        worker_outputs.push(self.grid_residual_tile_task_direct(
                            batch,
                            buckets,
                            model_grid,
                            task.tile_id,
                            store,
                        )?);
                    }
                    Ok::<_, ImagingError>(worker_outputs)
                }));
            }
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled residual worker panicked".to_string(),
                    )
                })??);
            }
            Ok::<_, ImagingError>(())
        })?;

        let mut task_timing = StandardMfsTileTaskTiming::default();
        for (gridded_samples, timing) in outputs.into_iter().flatten() {
            accumulation.gridded_residual_samples += gridded_samples;
            task_timing.add(timing);
        }
        Ok(tiled_scheduler_block_profile(
            StandardMfsTileSchedulerBlockInputs {
                worker_count,
                tasks: &tasks,
                buckets,
                bucket_build,
                task_timing,
                block_wall: started.elapsed(),
                merge: Duration::ZERO,
                merged_tiles: tasks.len(),
            },
        ))
    }

    fn grid_residual_tile_task_direct(
        &self,
        batch: &VisibilityBatch,
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
        for sample in buckets.tile_samples(tile_id) {
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS direct tiled residual bucket lost its tap plan".to_string(),
                    )
                })?;
            debug_assert_eq!(
                taps.center(),
                [sample.center_x as usize, sample.center_y as usize]
            );
            let observed_visibility = batch.visibility[sample_index];
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
            let sample_index = sample.sample_index as usize;
            let taps = self
                .gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                .ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "standard MFS Metal dirty bucket lost its tap plan".to_string(),
                    )
                })?;
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
    }
}

fn log_tiled_scheduler_block(stage: &str, block: StandardMfsTileSchedulerBlockProfile) {
    eprintln!(
        "standard_mfs_tile_scheduler_block stage={} requested_threads={} actual_threads={} max_live_row_blocks=1 task_count={} samples={} tap_visits={} largest_task_samples={} largest_task_tap_visits={} bucket_bytes={} bucket_build_ms={:.3} local_alloc_zero_ms={:.3} worker_replan_grid_ms={:.3} block_wall_ms={:.3} merge_ms={:.3} merged_tiles={} active_tile_wait_events=0 tasks_skipped_due_to_active_tile=0",
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
        StandardMfsDirtyCpuExecutor, StandardMfsFixedTilePartition, StandardMfsTileId,
        StandardMfsTiledCpuExecutor,
    };
    use crate::{
        ImageGeometry, StandardMfsExecutionConfig, VisibilityBatch, gridder::StandardGridder,
    };
    use num_complex::{Complex32, Complex64};

    #[test]
    fn standard_mfs_plan_buckets_gridder_accepted_samples() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
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

        let buckets =
            StandardMfsBlockTileBuckets::build_for_dirty(&gridder, &partition, &[batch]).unwrap();

        assert_eq!(buckets.accepted_samples(), 3);
        assert_eq!(buckets.skipped_samples(), 1);
        assert_eq!(buckets.samples().len(), 3);
        assert_eq!(
            buckets.nonempty_tiles(),
            &[StandardMfsTileId(0), StandardMfsTileId(3)]
        );
        let tile0 = buckets.tile_samples(StandardMfsTileId(0));
        assert_eq!(tile0.len(), 1);
        assert_eq!(tile0[0].sample_index, 0);
        assert_eq!((tile0[0].center_x, tile0[0].center_y), (8, 8));
        assert_eq!(tile0[0].flags, STANDARD_MFS_TILE_FLAG_FINITE_VISIBILITY);
        assert!(tile0[0].finite_visibility());
        assert!(!tile0[0].psf_only());
        assert_eq!(tile0[0].grid_weight, 1.0);
        assert_eq!(tile0[0].tap_count, 49);

        let tile3 = buckets.tile_samples(StandardMfsTileId(3));
        assert_eq!(tile3.len(), 2);
        assert_eq!(tile3[0].sample_index, 1);
        assert_eq!((tile3[0].center_x, tile3[0].center_y), (16, 16));
        assert_eq!(tile3[0].flags, STANDARD_MFS_TILE_FLAG_PSF_ONLY);
        assert!(!tile3[0].finite_visibility());
        assert!(tile3[0].psf_only());
        assert_eq!(tile3[0].grid_weight, 4.0);
        assert_eq!(tile3[0].tap_count, 49);
        assert_eq!(tile3[1].sample_index, 3);
        assert_eq!((tile3[1].center_x, tile3[1].center_y), (24, 24));
        assert_eq!(tile3[1].grid_weight, 12.0);
        assert_eq!(tile3[1].tap_count, 49);

        assert!(
            buckets.estimated_bytes() < 3 * std::mem::size_of::<super::StandardMfsPlannedSample>(),
            "bucket records should stay smaller than retained planned samples"
        );
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
        let batches = vec![VisibilityBatch {
            u_lambda: vec![-8.0 * du, 0.0, 4.0 * du, 8.0 * du],
            v_lambda: vec![8.0 * dv, 0.0, 4.0 * dv, -8.0 * dv],
            w_lambda: vec![0.0, 1.0, -2.0, 3.0],
            weight: vec![1.0, 2.0, 0.0, 3.0],
            sumwt_factor: vec![1.0, 2.0, 1.0, 4.0],
            gridable: vec![true; 4],
            visibility: vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(f32::NAN, 1.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, -3.0),
            ],
        }];
        let scratch = StandardMfsTiledCpuExecutor::new_with_execution_config(
            &gridder,
            StandardMfsExecutionConfig {
                fixed_tile_resident_bytes: Some(1),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
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
