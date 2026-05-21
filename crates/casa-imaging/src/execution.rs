// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal imaging execution plans and CPU workspaces.

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    ImageGeometry, ImagingError, VisibilityBatch,
    gridder::{PositiveTapSet, StandardGridder, StandardMfsTapCensus, StandardMfsTapSkipReason},
    profile,
};

/// Internal backend selection for standard MFS execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StandardMfsBackend {
    /// Synchronous CPU execution using the native Rust gridder.
    Cpu,
    /// Reserved marker for future backends that must fail before execution.
    #[allow(dead_code)]
    Reserved(&'static str),
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

const STANDARD_MFS_TILE_BUCKET_PROBE_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_BUCKET_PROBE";
const STANDARD_MFS_TILE_EDGE_ENV: &str = "CASA_RS_STANDARD_MFS_TILE_EDGE";
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
    tiles_y: usize,
    tiles: Vec<StandardMfsFixedTile>,
}

impl StandardMfsFixedTilePartition {
    pub(crate) fn new(
        grid_shape: [usize; 2],
        tile_shape: [usize; 2],
        halo: usize,
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

        let tiles_x = grid_shape[0].div_ceil(tile_shape[0]);
        let tiles_y = grid_shape[1].div_ceil(tile_shape[1]);
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
            let interior_x0 = tile_x * tile_shape[0];
            let interior_x1 = (interior_x0 + tile_shape[0]).min(grid_shape[0]);
            for tile_y in 0..tiles_y {
                let interior_y0 = tile_y * tile_shape[1];
                let interior_y1 = (interior_y0 + tile_shape[1]).min(grid_shape[1]);
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

    pub(crate) fn owner(&self, center_x: usize, center_y: usize) -> Option<StandardMfsTileId> {
        if center_x >= self.grid_shape[0] || center_y >= self.grid_shape[1] {
            return None;
        }
        let tile_x = center_x / self.tile_shape[0];
        let tile_y = center_y / self.tile_shape[1];
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
                let Some((tile_id, _center, _flags, _grid_weight)) =
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
            };
            accepted_samples
        ];

        let mut flat_sample_index = 0usize;
        for batch in batches {
            for sample_index in 0..batch.len() {
                let Some((tile_id, center, flags, grid_weight)) =
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

fn plan_dirty_tile_sample(
    gridder: &StandardGridder,
    partition: &StandardMfsFixedTilePartition,
    batch: &VisibilityBatch,
    sample_index: usize,
) -> Option<(StandardMfsTileId, [usize; 2], u16, f32)> {
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
    Some((tile_id, center, flags, grid_weight))
}

fn maybe_probe_standard_mfs_tile_buckets(
    gridder: &StandardGridder,
    batches: &[VisibilityBatch],
) -> Result<(), ImagingError> {
    if !standard_mfs_tile_bucket_probe_enabled() {
        return Ok(());
    }

    let grid_shape = gridder.grid_shape();
    let tile_edge = standard_mfs_tile_edge().min(grid_shape[0].max(grid_shape[1]));
    let tile_shape = [tile_edge, tile_edge];
    let halo = gridder.positive_tap_halo();
    let partition = StandardMfsFixedTilePartition::new(grid_shape, tile_shape, halo)?;
    let buckets = StandardMfsBlockTileBuckets::build_for_dirty(gridder, &partition, batches)?;

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

    let mut max_bucket_samples = 0usize;
    let mut resident_bytes_if_all_nonempty = 0usize;
    let mut interior_cells_if_all_nonempty = 0usize;
    for &tile_id in buckets.nonempty_tiles() {
        max_bucket_samples = max_bucket_samples.max(buckets.tile_samples(tile_id).len());
        resident_bytes_if_all_nonempty = resident_bytes_if_all_nonempty
            .saturating_add(partition.resident_tile_bytes(tile_id, 2).unwrap_or(0));
        if let Some(tile) = partition.tile(tile_id) {
            debug_assert_eq!(tile.id, tile_id);
            interior_cells_if_all_nonempty = interior_cells_if_all_nonempty
                .saturating_add(tile.interior.width().saturating_mul(tile.interior.height()));
        }
    }

    eprintln!(
        "standard_mfs_tile_bucket_probe \
         grid_shape={}x{} \
         tile_shape={}x{} \
         halo={} \
         tiles={} \
         accepted_samples={} \
         skipped_samples={} \
         finite_visibility_samples={} \
         psf_only_samples={} \
         bucket_bytes={} \
         nonempty_tiles={} \
         max_bucket_samples={} \
         interior_cells_if_all_nonempty={} \
         resident_bytes_if_all_nonempty={}",
        grid_shape[0],
        grid_shape[1],
        tile_shape[0],
        tile_shape[1],
        halo,
        partition.tile_count(),
        buckets.accepted_samples(),
        buckets.skipped_samples(),
        finite_visibility_samples,
        psf_only_samples,
        buckets.estimated_bytes(),
        buckets.nonempty_tiles().len(),
        max_bucket_samples,
        interior_cells_if_all_nonempty,
        resident_bytes_if_all_nonempty
    );

    Ok(())
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

fn standard_mfs_tile_edge() -> usize {
    std::env::var(STANDARD_MFS_TILE_EDGE_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|edge| *edge > 0)
        .unwrap_or(DEFAULT_STANDARD_MFS_TILE_EDGE)
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
            StandardMfsBackend::Reserved(name) => Err(ImagingError::Unsupported(format!(
                "standard MFS backend '{name}' is not implemented"
            ))),
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
            StandardMfsBackend::Reserved(name) => Err(ImagingError::Unsupported(format!(
                "standard MFS backend '{name}' is not implemented"
            ))),
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
    };
    use crate::{ImageGeometry, VisibilityBatch, gridder::StandardGridder};
    use num_complex::Complex32;

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
            StandardMfsBackend::Reserved("gpu"),
            &gridder,
            &batches,
        ) {
            Ok(_) => panic!("reserved backend unexpectedly built an executor"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'gpu' is not implemented"),
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

        let tile3 = buckets.tile_samples(StandardMfsTileId(3));
        assert_eq!(tile3.len(), 2);
        assert_eq!(tile3[0].sample_index, 1);
        assert_eq!((tile3[0].center_x, tile3[0].center_y), (16, 16));
        assert_eq!(tile3[0].flags, STANDARD_MFS_TILE_FLAG_PSF_ONLY);
        assert!(!tile3[0].finite_visibility());
        assert!(tile3[0].psf_only());
        assert_eq!(tile3[0].grid_weight, 4.0);
        assert_eq!(tile3[1].sample_index, 3);
        assert_eq!((tile3[1].center_x, tile3[1].center_y), (24, 24));
        assert_eq!(tile3[1].grid_weight, 12.0);

        assert!(
            buckets.estimated_bytes() < 3 * std::mem::size_of::<super::StandardMfsPlannedSample>(),
            "bucket records should stay smaller than retained planned samples"
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
    fn reserved_streaming_dirty_backend_fails_before_workspace_creation() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let error = match StandardMfsDirtyCpuExecutor::for_backend(
            StandardMfsBackend::Reserved("gpu"),
            geometry,
        ) {
            Ok(_) => panic!("reserved backend unexpectedly built a dirty executor"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'gpu' is not implemented"),
            "{error}"
        );
    }
}
