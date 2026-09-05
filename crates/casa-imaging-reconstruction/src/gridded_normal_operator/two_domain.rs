// SPDX-License-Identifier: LGPL-3.0-or-later

//! Production two-domain execution for bounded gridded-normal replay.

use std::{cmp::Reverse, mem::size_of, ops::Range, sync::Mutex};

use ndarray::Array2;
use num_complex::Complex64;

use super::*;
use crate::spectral_operator::GriddedNormalLocalContribution;

fn planned_vec<T>(capacity: usize) -> Result<Vec<T>, SpectralOperatorError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    if values.capacity() != capacity {
        return Err(SpectralOperatorError::ResidencyOverflow);
    }
    Ok(values)
}

fn planned_prediction_lane(
    group_capacity: usize,
    record_layout: GriddedNormalRecordLayout,
) -> Result<RwLock<GriddedNormalPredictionLane>, SpectralOperatorError> {
    let prediction_width = record_layout.prediction_width();
    let value_capacity = group_capacity
        .checked_mul(prediction_width)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let (model_scratch, moment_scratch) = match record_layout {
        GriddedNormalRecordLayout::Scalar
        | GriddedNormalRecordLayout::ChannelLocal { .. }
        | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
        | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
        | GriddedNormalRecordLayout::Joint { .. } => (planned_vec(0)?, planned_vec(0)?),
        GriddedNormalRecordLayout::Taylor(_) => {
            let mut model_scratch = planned_vec(prediction_width)?;
            model_scratch.resize(prediction_width, Complex64::default());
            let mut moment_scratch = planned_vec(record_layout.normal_moments())?;
            moment_scratch.resize(record_layout.normal_moments(), 0.0);
            (model_scratch, moment_scratch)
        }
    };
    Ok(RwLock::new(GriddedNormalPredictionLane {
        groups: 0..0,
        values: planned_vec(value_capacity)?,
        model_scratch,
        moment_scratch,
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GriddedNormalTileKey {
    x: i32,
    y: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct GriddedNormalTileGeometry {
    key: GriddedNormalTileKey,
    origin: [usize; 2],
    shape: [usize; 2],
    support: usize,
}

impl GriddedNormalTileGeometry {
    fn new(
        grid_shape: [usize; 2],
        key: GriddedNormalTileKey,
        support: usize,
    ) -> Result<Self, SpectralOperatorError> {
        let axis = |extent: usize, tile: i32| -> Result<(usize, usize), SpectralOperatorError> {
            let center =
                i64::try_from(extent / 2).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            let raw_start = center + i64::from(tile) * GRIDDED_NORMAL_TILE_EDGE as i64;
            let raw_end = raw_start + GRIDDED_NORMAL_TILE_EDGE as i64;
            let extent =
                i64::try_from(extent).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            let core_start = usize::try_from(raw_start.clamp(0, extent))
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            let core_end = usize::try_from(raw_end.clamp(0, extent))
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            if core_start >= core_end {
                return Err(SpectralOperatorError::UnsupportedGeometry);
            }
            Ok((core_start, core_end))
        };
        let (x0, x1) = axis(grid_shape[0], key.x)?;
        let (y0, y1) = axis(grid_shape[1], key.y)?;
        let origin = [x0.saturating_sub(support), y0.saturating_sub(support)];
        let end = [
            x1.saturating_add(support).min(grid_shape[0]),
            y1.saturating_add(support).min(grid_shape[1]),
        ];
        Ok(Self {
            key,
            origin,
            shape: [end[0] - origin[0], end[1] - origin[1]],
            support,
        })
    }

    pub(super) fn cell_count(self) -> Option<usize> {
        self.shape[0].checked_mul(self.shape[1])
    }

    pub(super) fn translated_taps(
        self,
        taps: SampleTaps,
    ) -> Result<SampleTaps, SpectralOperatorError> {
        let translated = SampleTaps {
            x: TapSpan {
                start: taps
                    .x
                    .start
                    .checked_sub(self.origin[0])
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                weight_index: taps.x.weight_index,
            },
            y: TapSpan {
                start: taps
                    .y
                    .start
                    .checked_sub(self.origin[1])
                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                weight_index: taps.y.weight_index,
            },
        };
        if translated
            .x
            .start
            .checked_add(2 * self.support)
            .is_none_or(|end| end >= self.shape[0])
            || translated
                .y
                .start
                .checked_add(2 * self.support)
                .is_none_or(|end| end >= self.shape[1])
        {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        Ok(translated)
    }

    #[cfg(test)]
    pub(super) const fn origin(self) -> [usize; 2] {
        self.origin
    }

    #[cfg(test)]
    pub(super) const fn shape(self) -> [usize; 2] {
        self.shape
    }
}

pub(super) struct GriddedNormalTileCatalog {
    grid_shape: [usize; 2],
    minimum_key: GriddedNormalTileKey,
    tiles_y: usize,
    pub(super) geometries: Vec<GriddedNormalTileGeometry>,
    support: usize,
}

pub(super) struct GriddedNormalDomainTileCatalogs {
    catalogs: Vec<GriddedNormalTileCatalog>,
    offsets: Vec<usize>,
}

impl GriddedNormalDomainTileCatalogs {
    #[cfg(test)]
    pub(super) fn new(
        grid_shapes: impl IntoIterator<Item = [usize; 2]>,
        support: usize,
    ) -> Result<Self, SpectralOperatorError> {
        Self::new_for_projection(grid_shapes, support, false)
    }

    pub(super) fn new_for_projection(
        grid_shapes: impl IntoIterator<Item = [usize; 2]>,
        support: usize,
        aw_projection: bool,
    ) -> Result<Self, SpectralOperatorError> {
        let catalogs = grid_shapes
            .into_iter()
            .map(|shape| {
                GriddedNormalTileCatalog::new_for_projection(shape, support, aw_projection)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if catalogs.is_empty() {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let mut offsets: Vec<usize> = Vec::with_capacity(catalogs.len() + 1);
        offsets.push(0);
        for catalog in &catalogs {
            offsets.push(
                offsets
                    .last()
                    .copied()
                    .and_then(|offset| offset.checked_add(catalog.geometries.len()))
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?,
            );
        }
        Ok(Self { catalogs, offsets })
    }

    pub(super) fn domain_count(&self) -> usize {
        self.catalogs.len()
    }

    pub(super) fn tile_count(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0)
    }

    pub(super) fn grid_shape(&self, domain_ordinal: usize) -> Option<[usize; 2]> {
        self.catalogs
            .get(domain_ordinal)
            .map(|catalog| catalog.grid_shape)
    }

    pub(super) fn tile_ordinal(
        &self,
        domain_ordinal: usize,
        taps: SampleTaps,
    ) -> Result<usize, SpectralOperatorError> {
        let catalog = self
            .catalogs
            .get(domain_ordinal)
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
        self.offsets[domain_ordinal]
            .checked_add(catalog.tile_ordinal(taps)?)
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn tile_ordinal_center(
        &self,
        domain_ordinal: usize,
        center: [usize; 2],
    ) -> Result<usize, SpectralOperatorError> {
        let catalog = self
            .catalogs
            .get(domain_ordinal)
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
        self.offsets[domain_ordinal]
            .checked_add(catalog.tile_ordinal_center(center)?)
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn geometry(
        &self,
        global_tile_ordinal: usize,
    ) -> Result<(usize, GriddedNormalTileGeometry), SpectralOperatorError> {
        let domain_ordinal = self
            .offsets
            .windows(2)
            .position(|window| window[0] <= global_tile_ordinal && global_tile_ordinal < window[1])
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
        let local = global_tile_ordinal - self.offsets[domain_ordinal];
        self.catalogs[domain_ordinal]
            .geometries
            .get(local)
            .copied()
            .map(|geometry| (domain_ordinal, geometry))
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)
    }

    pub(super) fn accumulators(
        &self,
        core_depth: usize,
    ) -> Result<Vec<Mutex<GriddedNormalTileAccumulator>>, SpectralOperatorError> {
        let count = self
            .tile_count()
            .checked_add(GRIDDED_NORMAL_HOT_TILE_DUPLICATES)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let mut accumulators = Vec::new();
        accumulators
            .try_reserve_exact(count)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        for catalog in &self.catalogs {
            for geometry in &catalog.geometries {
                accumulators.push(Mutex::new(GriddedNormalTileAccumulator::new(
                    geometry.shape,
                    core_depth,
                )));
            }
        }
        let maximum_shape = self
            .catalogs
            .iter()
            .flat_map(|catalog| &catalog.geometries)
            .max_by_key(|geometry| geometry.cell_count())
            .map(|geometry| geometry.shape)
            .ok_or(SpectralOperatorError::UnsupportedGeometry)?;
        for _ in 0..GRIDDED_NORMAL_HOT_TILE_DUPLICATES {
            accumulators.push(Mutex::new(GriddedNormalTileAccumulator::new(
                maximum_shape,
                core_depth,
            )));
        }
        if accumulators.len() != count || accumulators.capacity() != count {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(accumulators)
    }
}

pub(super) fn domain_execution_residency(
    grid_shapes: impl IntoIterator<Item = [usize; 2]>,
    coefficient_terms: usize,
    support: usize,
) -> Result<GriddedNormalExecutionResidency, SpectralOperatorError> {
    domain_execution_residency_for_projection(grid_shapes, coefficient_terms, support, false)
}

pub(super) fn domain_execution_residency_for_projection(
    grid_shapes: impl IntoIterator<Item = [usize; 2]>,
    coefficient_terms: usize,
    support: usize,
    aw_projection: bool,
) -> Result<GriddedNormalExecutionResidency, SpectralOperatorError> {
    if coefficient_terms == 0 {
        return Err(SpectralOperatorError::InvalidSlab);
    }
    let catalogs =
        GriddedNormalDomainTileCatalogs::new_for_projection(grid_shapes, support, aw_projection)?;
    let tile_halo_cells = catalogs
        .catalogs
        .iter()
        .flat_map(|catalog| &catalog.geometries)
        .try_fold(0_usize, |total, geometry| {
            geometry
                .cell_count()
                .and_then(|cells| total.checked_add(cells))
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })?;
    let duplicate_cells = catalogs
        .catalogs
        .iter()
        .map(GriddedNormalTileCatalog::maximum_cell_count)
        .max()
        .unwrap_or(0)
        .checked_mul(GRIDDED_NORMAL_HOT_TILE_DUPLICATES)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let tile_accumulator_complex_values = tile_halo_cells
        .checked_add(duplicate_cells)
        .and_then(|cells| cells.checked_mul(coefficient_terms))
        .and_then(|values| values.checked_mul(2))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let merge_complex_values = catalogs
        .catalogs
        .iter()
        .try_fold(0_usize, |total, catalog| {
            catalog.grid_shape[0]
                .checked_mul(catalog.grid_shape[1])
                .and_then(|cells| total.checked_add(cells))
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })?
        .checked_mul(coefficient_terms)
        .and_then(|values| values.checked_mul(2))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let peak_complex_values = tile_accumulator_complex_values
        .checked_add(merge_complex_values)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let tile_count = catalogs.tile_count();
    let accumulator_count = tile_count
        .checked_add(GRIDDED_NORMAL_HOT_TILE_DUPLICATES)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let catalog_bytes = catalogs
        .catalogs
        .capacity()
        .checked_mul(size_of::<GriddedNormalTileCatalog>())
        .and_then(|bytes| {
            catalogs
                .offsets
                .capacity()
                .checked_mul(size_of::<usize>())
                .and_then(|offsets| bytes.checked_add(offsets))
        })
        .and_then(|bytes| {
            catalogs.catalogs.iter().try_fold(bytes, |total, catalog| {
                catalog
                    .geometries
                    .capacity()
                    .checked_mul(size_of::<GriddedNormalTileGeometry>())
                    .and_then(|geometry| total.checked_add(geometry))
            })
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let routing_bytes = tile_count
        .checked_mul(size_of::<u32>() * 2)
        .and_then(|bytes| {
            tile_count
                .checked_add(1)
                .and_then(|count| count.checked_mul(size_of::<u32>()))
                .and_then(|offsets| bytes.checked_add(offsets))
        })
        .and_then(|bytes| {
            accumulator_count
                .checked_mul(size_of::<GriddedNormalTileTask>())
                .and_then(|tasks| bytes.checked_add(tasks))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let accumulator_bytes = accumulator_count
        .checked_mul(size_of::<Mutex<GriddedNormalTileAccumulator>>())
        .and_then(|bytes| {
            accumulator_count
                .checked_mul(coefficient_terms)
                .and_then(|planes| planes.checked_mul(size_of::<Array2<Complex64>>() * 2))
                .and_then(|planes| bytes.checked_add(planes))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let domain_count = catalogs.domain_count();
    let merge_descriptor_bytes = domain_count
        .checked_mul(size_of::<Vec<Array2<Complex64>>>() * 2)
        .and_then(|bytes| {
            domain_count
                .checked_mul(coefficient_terms)
                .and_then(|planes| planes.checked_mul(size_of::<Array2<Complex64>>() * 2))
                .and_then(|planes| bytes.checked_add(planes))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let metadata_bytes = catalog_bytes
        .checked_add(routing_bytes)
        .and_then(|bytes| bytes.checked_add(accumulator_bytes))
        .and_then(|bytes| bytes.checked_add(merge_descriptor_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(GriddedNormalExecutionResidency {
        tile_accumulator_complex_values,
        merge_complex_values,
        peak_complex_values,
        metadata_bytes,
    })
}

impl GriddedNormalTileCatalog {
    #[cfg(test)]
    pub(super) fn new(
        grid_shape: [usize; 2],
        support: usize,
    ) -> Result<Self, SpectralOperatorError> {
        Self::new_for_projection(grid_shape, support, false)
    }

    fn new_for_projection(
        grid_shape: [usize; 2],
        support: usize,
        aw_projection: bool,
    ) -> Result<Self, SpectralOperatorError> {
        let (minimum_key, maximum_key) =
            Self::key_bounds_for_projection(grid_shape, support, aw_projection)?;
        let tiles_x = usize::try_from(i64::from(maximum_key.x) - i64::from(minimum_key.x) + 1)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        let tiles_y = usize::try_from(i64::from(maximum_key.y) - i64::from(minimum_key.y) + 1)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        let tile_count = tiles_x
            .checked_mul(tiles_y)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let mut geometries = Vec::new();
        geometries
            .try_reserve_exact(tile_count)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        for x in minimum_key.x..=maximum_key.x {
            for y in minimum_key.y..=maximum_key.y {
                geometries.push(GriddedNormalTileGeometry::new(
                    grid_shape,
                    GriddedNormalTileKey { x, y },
                    support,
                )?);
            }
        }
        if geometries.capacity() != tile_count || geometries.len() != tile_count {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(Self {
            grid_shape,
            minimum_key,
            tiles_y,
            geometries,
            support,
        })
    }

    fn key_bounds_for_projection(
        grid_shape: [usize; 2],
        support: usize,
        aw_projection: bool,
    ) -> Result<(GriddedNormalTileKey, GriddedNormalTileKey), SpectralOperatorError> {
        if grid_shape
            .into_iter()
            .any(|extent| extent <= support.saturating_mul(2))
        {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let axis = |coordinate: usize, extent: usize| -> Result<i32, SpectralOperatorError> {
            let coordinate =
                i64::try_from(coordinate).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            let center =
                i64::try_from(extent / 2).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            i32::try_from((coordinate - center).div_euclid(GRIDDED_NORMAL_TILE_EDGE as i64))
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)
        };
        Ok((
            GriddedNormalTileKey {
                x: axis(if aw_projection { 0 } else { support }, grid_shape[0])?,
                y: axis(if aw_projection { 0 } else { support }, grid_shape[1])?,
            },
            GriddedNormalTileKey {
                x: axis(
                    if aw_projection {
                        grid_shape[0] - 1
                    } else {
                        grid_shape[0] - support - 1
                    },
                    grid_shape[0],
                )?,
                y: axis(
                    if aw_projection {
                        grid_shape[1] - 1
                    } else {
                        grid_shape[1] - support - 1
                    },
                    grid_shape[1],
                )?,
            },
        ))
    }

    fn tile_ordinal(&self, taps: SampleTaps) -> Result<usize, SpectralOperatorError> {
        let center = [
            taps.x
                .start
                .checked_add(self.support)
                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
            taps.y
                .start
                .checked_add(self.support)
                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
        ];
        self.tile_ordinal_center(center)
    }

    fn tile_ordinal_center(&self, center: [usize; 2]) -> Result<usize, SpectralOperatorError> {
        if center[0] >= self.grid_shape[0] || center[1] >= self.grid_shape[1] {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let axis = |coordinate: usize, extent: usize| -> Result<i32, SpectralOperatorError> {
            let coordinate =
                i64::try_from(coordinate).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            let anchor =
                i64::try_from(extent / 2).map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
            i32::try_from((coordinate - anchor).div_euclid(GRIDDED_NORMAL_TILE_EDGE as i64))
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)
        };
        let key = GriddedNormalTileKey {
            x: axis(center[0], self.grid_shape[0])?,
            y: axis(center[1], self.grid_shape[1])?,
        };
        let x = usize::try_from(i64::from(key.x) - i64::from(self.minimum_key.x))
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
        let y = usize::try_from(i64::from(key.y) - i64::from(self.minimum_key.y))
            .map_err(|_| SpectralOperatorError::InvalidGriddedRecord)?;
        let ordinal = x
            .checked_mul(self.tiles_y)
            .and_then(|base| base.checked_add(y))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if self
            .geometries
            .get(ordinal)
            .is_none_or(|geometry| geometry.key != key)
        {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        Ok(ordinal)
    }

    pub(super) fn maximum_cell_count(&self) -> usize {
        self.geometries
            .iter()
            .filter_map(|geometry| geometry.cell_count())
            .max()
            .unwrap_or(0)
    }
}

pub(super) struct GriddedNormalTileAccumulator {
    pub(super) grids: Vec<Array2<Complex64>>,
    pub(super) compensations: Vec<Array2<Complex64>>,
}

impl GriddedNormalTileAccumulator {
    fn new(shape: [usize; 2], core_depth: usize) -> Self {
        let shape = (shape[0], shape[1]);
        let planes = || (0..core_depth).map(|_| Array2::zeros(shape)).collect();
        Self {
            grids: planes(),
            compensations: planes(),
        }
    }

    fn clear(&mut self) {
        for grid in &mut self.grids {
            grid.fill(Complex64::default());
        }
        for compensation in &mut self.compensations {
            compensation.fill(Complex64::default());
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct GriddedNormalGroupSpan {
    frame_ordinal: u32,
    records: Range<u32>,
    prediction_lane: u8,
    prediction_index: u32,
}

impl GriddedNormalGroupSpan {
    fn record_count(&self) -> usize {
        usize::try_from(self.records.end - self.records.start).expect("group record count fits")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct GriddedNormalClassification {
    tile_ordinal: u32,
    group_ordinal: u32,
    frame_ordinal: u32,
    record_ordinal: u32,
    tap_count: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct GriddedNormalRoute {
    group_ordinal: u32,
    frame_ordinal: u32,
    record_ordinal: u32,
    tap_count: u32,
}

#[derive(Clone, Debug, Default)]
pub(super) struct GriddedNormalTileTask {
    tile_ordinal: u32,
    shard: u8,
    lane: u8,
    accumulator_ordinal: u32,
    routes: Range<u32>,
}

impl GriddedNormalTileTask {
    fn record_count(&self) -> usize {
        usize::try_from(self.routes.end - self.routes.start).expect("task record count fits")
    }
}

struct GriddedNormalPredictionLane {
    groups: Range<usize>,
    values: Vec<Complex64>,
    model_scratch: Vec<Complex64>,
    moment_scratch: Vec<f64>,
}

pub(super) struct PreparedGriddedNormalTwoDomainWindow {
    record_layout: GriddedNormalRecordLayout,
    record_bytes: usize,
    prediction_width: usize,
    frame_capacities: Box<[usize]>,
    frame_sequences: Vec<u64>,
    frame_record_counts: Vec<u32>,
    groups: Vec<GriddedNormalGroupSpan>,
    predictions: [RwLock<GriddedNormalPredictionLane>; GRIDDED_NORMAL_LANE_COUNT],
    classifications: Vec<GriddedNormalClassification>,
    routes: Vec<GriddedNormalRoute>,
    tile_counts: Vec<u32>,
    tile_cursors: Vec<u32>,
    tile_offsets: Vec<u32>,
    tasks: Vec<GriddedNormalTileTask>,
    lane_record_counts: [u64; GRIDDED_NORMAL_LANE_COUNT],
    lane_tap_visit_counts: [u64; GRIDDED_NORMAL_LANE_COUNT],
    pub(super) active_frames: usize,
    first_sequence: Option<u64>,
    record_count: u64,
    prediction_groups: u64,
}

impl PreparedGriddedNormalTwoDomainWindow {
    #[cfg(test)]
    pub(super) fn with_record_capacities(
        record_capacities: &[usize],
        tile_count: usize,
        record_layout: GriddedNormalRecordLayout,
    ) -> Result<Self, SpectralOperatorError> {
        Self::with_projection_record_capacities(record_capacities, tile_count, record_layout, false)
    }

    pub(super) fn with_projection_record_capacities(
        record_capacities: &[usize],
        tile_count: usize,
        record_layout: GriddedNormalRecordLayout,
        aw_projection: bool,
    ) -> Result<Self, SpectralOperatorError> {
        if record_capacities.is_empty() || tile_count == 0 {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let record_bytes = record_bytes(record_layout, aw_projection)?;
        let prediction_width = record_layout.prediction_width();
        if record_bytes == 0 || prediction_width == 0 {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let record_capacity = record_capacities.iter().try_fold(0_usize, |total, value| {
            total
                .checked_add(*value)
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })?;
        let prediction_capacity = record_capacity
            .div_ceil(GRIDDED_NORMAL_LANE_COUNT)
            .checked_add(1)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let frame_count = record_capacities.len();
        let mut tile_counts = planned_vec::<u32>(tile_count)?;
        let mut tile_cursors = planned_vec::<u32>(tile_count)?;
        let mut tile_offsets = planned_vec::<u32>(tile_count + 1)?;
        tile_counts.resize(tile_count, 0);
        tile_cursors.resize(tile_count, 0);
        tile_offsets.resize(tile_count + 1, 0);
        let predictions = [
            planned_prediction_lane(prediction_capacity, record_layout)?,
            planned_prediction_lane(prediction_capacity, record_layout)?,
            planned_prediction_lane(prediction_capacity, record_layout)?,
            planned_prediction_lane(prediction_capacity, record_layout)?,
        ];
        Ok(Self {
            record_layout,
            record_bytes,
            prediction_width,
            frame_capacities: record_capacities.into(),
            frame_sequences: planned_vec(frame_count)?,
            frame_record_counts: planned_vec(frame_count)?,
            groups: planned_vec(record_capacity)?,
            predictions,
            classifications: planned_vec(record_capacity)?,
            routes: planned_vec(record_capacity)?,
            tile_counts,
            tile_cursors,
            tile_offsets,
            tasks: planned_vec(tile_count + GRIDDED_NORMAL_HOT_TILE_DUPLICATES)?,
            lane_record_counts: [0; GRIDDED_NORMAL_LANE_COUNT],
            lane_tap_visit_counts: [0; GRIDDED_NORMAL_LANE_COUNT],
            active_frames: 0,
            first_sequence: None,
            record_count: 0,
            prediction_groups: 0,
        })
    }

    fn prepare<'a, I>(
        &mut self,
        first_sequence: u64,
        descriptors: &[BlockDescriptor],
        frames: I,
        catalogs: &GriddedNormalDomainTileCatalogs,
        operators: &[SpectralSlabOperator],
        output_channels: usize,
    ) -> Result<(), SpectralOperatorError>
    where
        I: IntoIterator<Item = (u64, &'a [u8])>,
    {
        if self.active_frames != 0 {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.frame_sequences.clear();
        self.frame_record_counts.clear();
        self.groups.clear();
        self.classifications.clear();
        self.routes.clear();
        self.tasks.clear();
        self.tile_counts.fill(0);
        self.tile_cursors.fill(0);
        self.tile_offsets.fill(0);
        self.lane_record_counts.fill(0);
        self.lane_tap_visit_counts.fill(0);

        let prepared = (|| {
            for (frame_ordinal, (sequence, encoded)) in frames.into_iter().enumerate() {
                let frame_ordinal_u64 = u64::try_from(frame_ordinal)
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                let expected = first_sequence
                    .checked_add(frame_ordinal_u64)
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                if sequence != expected {
                    return Err(SpectralOperatorError::BlockSequence);
                }
                let descriptor = descriptors
                    .get(
                        usize::try_from(sequence)
                            .map_err(|_| SpectralOperatorError::GriddedRecordMismatch)?,
                    )
                    .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
                validate_encoded_block(descriptor, encoded, self.record_bytes)?;
                let record_count = encoded.len() / self.record_bytes;
                if record_count
                    > *self
                        .frame_capacities
                        .get(frame_ordinal)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?
                {
                    return Err(SpectralOperatorError::ResidencyOverflow);
                }
                let frame_ordinal_u32 = u32::try_from(frame_ordinal)
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                match self.record_layout {
                    GriddedNormalRecordLayout::Scalar
                    | GriddedNormalRecordLayout::ChannelLocal { .. }
                    | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                    | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
                    | GriddedNormalRecordLayout::Joint { .. } => {
                        let mut group_start = 0usize;
                        for (record_ordinal, bytes) in
                            encoded.chunks_exact(self.record_bytes).enumerate()
                        {
                            let record = decode_domain_record(bytes, catalogs, output_channels)?;
                            let group_ordinal = u32::try_from(self.groups.len())
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                            let (tile_ordinal, tap_count) = if let Some(aw) = record.aw {
                                let (center, tap_count) = operators
                                    .get(record.chart_ordinal)
                                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?
                                    .aw_gridded_grid_footprint(aw)?;
                                (
                                    catalogs.tile_ordinal_center(record.chart_ordinal, center)?,
                                    tap_count,
                                )
                            } else {
                                (
                                    catalogs.tile_ordinal(record.chart_ordinal, record.taps)?,
                                    usize::try_from(GRIDDED_NORMAL_TAPS_PER_RECORD)
                                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                )
                            };
                            self.classifications.push(GriddedNormalClassification {
                                tile_ordinal: u32::try_from(tile_ordinal)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                group_ordinal,
                                frame_ordinal: frame_ordinal_u32,
                                record_ordinal: u32::try_from(record_ordinal)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                tap_count: u32::try_from(tap_count)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                            });
                            self.tile_counts[tile_ordinal] = self.tile_counts[tile_ordinal]
                                .checked_add(1)
                                .ok_or(SpectralOperatorError::CoverageOverflow)?;
                            if record.group_end {
                                self.groups.push(GriddedNormalGroupSpan {
                                    frame_ordinal: frame_ordinal_u32,
                                    records: u32::try_from(group_start)
                                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                        ..u32::try_from(record_ordinal + 1)
                                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                    prediction_lane: 0,
                                    prediction_index: 0,
                                });
                                group_start = record_ordinal + 1;
                            }
                        }
                        if group_start != record_count {
                            return Err(SpectralOperatorError::InvalidGriddedRecord);
                        }
                    }
                    GriddedNormalRecordLayout::Taylor(plan) => {
                        for (record_ordinal, bytes) in
                            encoded.chunks_exact(self.record_bytes).enumerate()
                        {
                            let record = decode_taylor_record(
                                bytes,
                                catalogs
                                    .grid_shape(0)
                                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                                plan.normal_moment_count(),
                            )?;
                            let group_ordinal = u32::try_from(self.groups.len())
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                            let tile_ordinal = catalogs.tile_ordinal(0, record.taps)?;
                            self.classifications.push(GriddedNormalClassification {
                                tile_ordinal: u32::try_from(tile_ordinal)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                group_ordinal,
                                frame_ordinal: frame_ordinal_u32,
                                record_ordinal: u32::try_from(record_ordinal)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                tap_count: u32::try_from(GRIDDED_NORMAL_TAPS_PER_RECORD)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                            });
                            self.tile_counts[tile_ordinal] = self.tile_counts[tile_ordinal]
                                .checked_add(1)
                                .ok_or(SpectralOperatorError::CoverageOverflow)?;
                            self.groups.push(GriddedNormalGroupSpan {
                                frame_ordinal: frame_ordinal_u32,
                                records: u32::try_from(record_ordinal)
                                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                    ..u32::try_from(record_ordinal + 1)
                                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                prediction_lane: 0,
                                prediction_index: 0,
                            });
                        }
                    }
                }
                self.frame_sequences.push(sequence);
                self.frame_record_counts.push(
                    u32::try_from(record_count)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                );
            }
            if self.frame_sequences.is_empty() {
                return Err(SpectralOperatorError::IncompleteCoverage);
            }
            self.prepare_prediction_lanes()?;
            self.prepare_tile_routes()?;
            self.active_frames = self.frame_sequences.len();
            self.first_sequence = Some(first_sequence);
            self.record_count = u64::try_from(self.routes.len())
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            self.prediction_groups = self
                .prediction_groups
                .checked_add(
                    u64::try_from(self.groups.len())
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                )
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            Ok(())
        })();
        if prepared.is_err() {
            self.reset_active()?;
        }
        prepared
    }

    fn prepare_prediction_lanes(&mut self) -> Result<(), SpectralOperatorError> {
        let total_records: usize = self
            .groups
            .iter()
            .map(GriddedNormalGroupSpan::record_count)
            .sum();
        let mut starts = [0usize; GRIDDED_NORMAL_LANE_COUNT + 1];
        starts[GRIDDED_NORMAL_LANE_COUNT] = self.groups.len();
        let mut group = 0usize;
        let mut records = 0usize;
        for (lane, start) in starts
            .iter_mut()
            .enumerate()
            .take(GRIDDED_NORMAL_LANE_COUNT)
            .skip(1)
        {
            let target = total_records
                .saturating_mul(lane)
                .div_ceil(GRIDDED_NORMAL_LANE_COUNT);
            while group < self.groups.len() && records < target {
                records = records
                    .checked_add(self.groups[group].record_count())
                    .ok_or(SpectralOperatorError::CoverageOverflow)?;
                group += 1;
            }
            *start = group;
        }
        for lane in 0..GRIDDED_NORMAL_LANE_COUNT {
            let range = starts[lane]..starts[lane + 1];
            let owner = self.predictions[lane]
                .get_mut()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
            let value_count = range
                .len()
                .checked_mul(self.prediction_width)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            if value_count > owner.values.capacity() {
                return Err(SpectralOperatorError::ResidencyOverflow);
            }
            owner.groups = range.clone();
            owner.values.resize(value_count, Complex64::default());
            for (local, group) in self.groups[range].iter_mut().enumerate() {
                group.prediction_lane =
                    u8::try_from(lane).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                group.prediction_index =
                    u32::try_from(local).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            }
        }
        Ok(())
    }

    fn prepare_tile_routes(&mut self) -> Result<(), SpectralOperatorError> {
        for tile in 0..self.tile_counts.len() {
            self.tile_offsets[tile + 1] = self.tile_offsets[tile]
                .checked_add(self.tile_counts[tile])
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            self.tile_cursors[tile] = self.tile_offsets[tile];
        }
        let record_count = self.classifications.len();
        if usize::try_from(self.tile_offsets[self.tile_counts.len()])
            .map_err(|_| SpectralOperatorError::CoverageOverflow)?
            != record_count
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.routes
            .resize(record_count, GriddedNormalRoute::default());
        for classification in &self.classifications {
            let tile = usize::try_from(classification.tile_ordinal)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let route = usize::try_from(self.tile_cursors[tile])
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            self.routes[route] = GriddedNormalRoute {
                group_ordinal: classification.group_ordinal,
                frame_ordinal: classification.frame_ordinal,
                record_ordinal: classification.record_ordinal,
                tap_count: classification.tap_count,
            };
            self.tile_cursors[tile] = self.tile_cursors[tile]
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
        }
        self.classifications.clear();

        let target = record_count.div_ceil(GRIDDED_NORMAL_LANE_COUNT).max(1);
        let mut duplicate = 0usize;
        for tile in 0..self.tile_counts.len() {
            let start = usize::try_from(self.tile_offsets[tile])
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let end = usize::try_from(self.tile_offsets[tile + 1])
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let count = end - start;
            if count == 0 {
                continue;
            }
            let shards = count.div_ceil(target).clamp(1, GRIDDED_NORMAL_LANE_COUNT);
            let chunk = count.div_ceil(shards);
            for (shard, route_start) in (start..end).step_by(chunk).enumerate() {
                let route_end = route_start.saturating_add(chunk).min(end);
                let accumulator = if shard == 0 {
                    tile
                } else {
                    let accumulator = self
                        .tile_counts
                        .len()
                        .checked_add(duplicate)
                        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                    duplicate += 1;
                    accumulator
                };
                self.tasks.push(GriddedNormalTileTask {
                    tile_ordinal: u32::try_from(tile)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    shard: u8::try_from(shard)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    lane: 0,
                    accumulator_ordinal: u32::try_from(accumulator)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                    routes: u32::try_from(route_start)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                        ..u32::try_from(route_end)
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                });
            }
        }
        if duplicate > GRIDDED_NORMAL_HOT_TILE_DUPLICATES {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        self.tasks.sort_unstable_by_key(|task| {
            (Reverse(task.record_count()), task.tile_ordinal, task.shard)
        });
        let mut lane_record_loads = [0_u64; GRIDDED_NORMAL_LANE_COUNT];
        let mut lane_tap_loads = [0_u64; GRIDDED_NORMAL_LANE_COUNT];
        for task in &mut self.tasks {
            let lane = lane_tap_loads
                .iter()
                .enumerate()
                .min_by_key(|(lane, load)| (**load, lane_record_loads[*lane], *lane))
                .map(|(lane, _)| lane)
                .expect("four logical lanes");
            let records = u64::try_from(task.record_count())
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let route_start = usize::try_from(task.routes.start)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let route_end = usize::try_from(task.routes.end)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let taps = self.routes[route_start..route_end]
                .iter()
                .try_fold(0_u64, |total, route| {
                    total.checked_add(u64::from(route.tap_count))
                })
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            lane_record_loads[lane] = lane_record_loads[lane]
                .checked_add(records)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            lane_tap_loads[lane] = lane_tap_loads[lane]
                .checked_add(taps)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            task.lane = u8::try_from(lane).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
        }
        self.tasks
            .sort_unstable_by_key(|task| (task.tile_ordinal, task.shard));
        self.lane_record_counts = lane_record_loads;
        self.lane_tap_visit_counts = lane_tap_loads;
        Ok(())
    }

    fn reset_active(&mut self) -> Result<(), SpectralOperatorError> {
        for owner in &mut self.predictions {
            let owner = owner
                .get_mut()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
            owner.groups = 0..0;
            owner.values.clear();
        }
        self.frame_sequences.clear();
        self.frame_record_counts.clear();
        self.groups.clear();
        self.classifications.clear();
        self.routes.clear();
        self.tasks.clear();
        self.tile_counts.fill(0);
        self.tile_cursors.fill(0);
        self.tile_offsets.fill(0);
        self.lane_record_counts.fill(0);
        self.lane_tap_visit_counts.fill(0);
        self.active_frames = 0;
        self.first_sequence = None;
        self.record_count = 0;
        Ok(())
    }

    fn with_published_predictions<T>(
        &self,
        operation: impl FnOnce(
            &[std::sync::RwLockReadGuard<'_, GriddedNormalPredictionLane>;
                 GRIDDED_NORMAL_LANE_COUNT],
        ) -> Result<T, SpectralOperatorError>,
    ) -> Result<T, SpectralOperatorError> {
        let predictions = [
            self.predictions[0]
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?,
            self.predictions[1]
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?,
            self.predictions[2]
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?,
            self.predictions[3]
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?,
        ];
        operation(&predictions)
    }

    pub(super) fn route_capacity_bytes(&self) -> Result<u64, SpectralOperatorError> {
        let capacity_bytes = |capacity: usize, value_bytes: usize| {
            capacity
                .checked_mul(value_bytes)
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        };
        let mut bytes = capacity_bytes(self.frame_capacities.len(), size_of::<usize>())?
            .checked_add(capacity_bytes(
                self.frame_sequences.capacity(),
                size_of::<u64>(),
            )?)
            .and_then(|bytes| {
                bytes.checked_add(
                    capacity_bytes(self.frame_record_counts.capacity(), size_of::<u32>()).ok()?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    capacity_bytes(self.groups.capacity(), size_of::<GriddedNormalGroupSpan>())
                        .ok()?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    capacity_bytes(
                        self.classifications.capacity(),
                        size_of::<GriddedNormalClassification>(),
                    )
                    .ok()?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    capacity_bytes(self.routes.capacity(), size_of::<GriddedNormalRoute>()).ok()?,
                )
            })
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        for owner in &self.predictions {
            let owner = owner
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
            bytes = bytes
                .checked_add(capacity_bytes(
                    owner.values.capacity(),
                    size_of::<Complex64>(),
                )?)
                .and_then(|bytes| {
                    bytes.checked_add(
                        capacity_bytes(owner.model_scratch.capacity(), size_of::<Complex64>())
                            .ok()?,
                    )
                })
                .and_then(|bytes| {
                    bytes.checked_add(
                        capacity_bytes(owner.moment_scratch.capacity(), size_of::<f64>()).ok()?,
                    )
                })
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        }
        u64::try_from(bytes).map_err(|_| SpectralOperatorError::ResidencyOverflow)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GriddedNormalWorkKind {
    Prediction,
    Accumulation,
}

/// One worker-count-independent lane in the ordered prediction/accumulation plan.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalWork {
    first_block_sequence: u64,
    frame_count: u64,
    window_record_count: u64,
    lane: usize,
    kind: GriddedNormalWorkKind,
    routed_record_count: u64,
    tap_visit_count: u64,
    shared_route_capacity_bytes: u64,
}

impl GriddedNormalWork {
    /// Return the runtime phase: complete-group prediction precedes accumulation.
    #[must_use]
    pub const fn phase(self) -> u8 {
        match self.kind {
            GriddedNormalWorkKind::Prediction => 0,
            GriddedNormalWorkKind::Accumulation => 1,
        }
    }

    /// Return a stable key across worker counts.
    #[must_use]
    pub const fn partition_key(self) -> u64 {
        self.phase() as u64 * GRIDDED_NORMAL_LANE_COUNT as u64 + self.lane as u64
    }

    /// Return the exclusive logical owner region.
    #[must_use]
    pub const fn region(self) -> u64 {
        self.partition_key()
    }

    /// Return records consumed in this domain lane.
    #[must_use]
    pub const fn routed_record_count(self) -> u64 {
        self.routed_record_count
    }

    /// Return convolutional visits; prediction lanes perform no accumulation taps.
    #[must_use]
    pub const fn tap_visit_count(self) -> u64 {
        self.tap_visit_count
    }

    /// Return shared reusable planning capacity exactly once per window.
    #[must_use]
    pub const fn shared_route_capacity_bytes(self) -> u64 {
        self.shared_route_capacity_bytes
    }
}

/// Fixed-size proof that one two-domain lane completed.
#[doc(hidden)]
#[derive(Debug)]
pub struct GriddedNormalPartial {
    work: GriddedNormalWork,
}

impl GriddedNormalOperatorApply {
    /// Apply one borrowed frame through the same eight logical works used by runtime.
    pub fn apply_encoded_block(
        &mut self,
        sequence: u64,
        encoded: &[u8],
    ) -> Result<(), SpectralOperatorError> {
        let partition_count =
            self.two_domain_window_partition_count(std::iter::once((sequence, encoded)))?;
        for ordinal in 0..partition_count {
            let work = self.two_domain_window_partition(sequence, 1, ordinal)?;
            let partial = self.execute_two_domain_window(
                |frame| (frame == 0).then_some((sequence, encoded)),
                work,
            )?;
            self.commit_two_domain(partial)?;
        }
        Ok(())
    }

    /// Prepare one ordered frame window and return four prediction plus four grid lanes.
    pub fn two_domain_window_partition_count<'a, I>(
        &self,
        frames: I,
    ) -> Result<usize, SpectralOperatorError>
    where
        I: IntoIterator<Item = (u64, &'a [u8])>,
    {
        if self.next_partition_commit != 0 {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let mut prepared = self
            .two_domain
            .write()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        prepared.prepare(
            self.next_block_sequence,
            &self.program.manifest.descriptors,
            frames,
            &self.tile_catalogs,
            &self.operators,
            self.program.output_plane_count()?,
        )?;
        for task in &prepared.tasks {
            self.tile_accumulators
                .get(
                    usize::try_from(task.accumulator_ordinal)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                )
                .ok_or(SpectralOperatorError::ResidencyOverflow)?
                .lock()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?
                .clear();
        }
        Ok(GRIDDED_NORMAL_PARTITION_COUNT)
    }

    /// Return one of the eight stable two-domain works for the active window.
    pub fn two_domain_window_partition(
        &self,
        first_sequence: u64,
        frame_count: usize,
        local_ordinal: usize,
    ) -> Result<GriddedNormalWork, SpectralOperatorError> {
        if local_ordinal >= GRIDDED_NORMAL_PARTITION_COUNT
            || local_ordinal < self.next_partition_commit
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let prepared = self
            .two_domain
            .read()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if prepared.first_sequence != Some(first_sequence)
            || prepared.active_frames != frame_count
            || first_sequence != self.next_block_sequence
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let (kind, lane, records) = if local_ordinal < GRIDDED_NORMAL_LANE_COUNT {
            let lane = local_ordinal;
            let owner = prepared.predictions[lane]
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
            let records = prepared.groups[owner.groups.clone()]
                .iter()
                .try_fold(0_u64, |total, group| {
                    total.checked_add(u64::try_from(group.record_count()).ok()?)
                })
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            (GriddedNormalWorkKind::Prediction, lane, records)
        } else {
            let lane = local_ordinal - GRIDDED_NORMAL_LANE_COUNT;
            (
                GriddedNormalWorkKind::Accumulation,
                lane,
                prepared.lane_record_counts[lane],
            )
        };
        let tap_visit_count = match kind {
            GriddedNormalWorkKind::Prediction => 0,
            GriddedNormalWorkKind::Accumulation => prepared.lane_tap_visit_counts[lane],
        };
        Ok(GriddedNormalWork {
            first_block_sequence: first_sequence,
            frame_count: u64::try_from(frame_count)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
            window_record_count: prepared.record_count,
            lane,
            kind,
            routed_record_count: records,
            tap_visit_count,
            shared_route_capacity_bytes: if local_ordinal == 0 {
                prepared.route_capacity_bytes()?
            } else {
                0
            },
        })
    }

    /// Execute one prepared lane against borrowed immutable artifact frames.
    pub fn execute_two_domain_window<'a, F>(
        &self,
        frame_at: F,
        work: GriddedNormalWork,
    ) -> Result<GriddedNormalPartial, SpectralOperatorError>
    where
        F: Fn(usize) -> Option<(u64, &'a [u8])>,
    {
        if work.first_block_sequence != self.next_block_sequence
            || work.lane >= GRIDDED_NORMAL_LANE_COUNT
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let prepared = self
            .two_domain
            .read()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
        if prepared.first_sequence != Some(work.first_block_sequence)
            || u64::try_from(prepared.active_frames)
                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                != work.frame_count
            || prepared.record_count != work.window_record_count
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        match work.kind {
            GriddedNormalWorkKind::Prediction => {
                let mut owner = prepared.predictions[work.lane]
                    .write()
                    .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
                let group_range = owner.groups.clone();
                match prepared.record_layout {
                    GriddedNormalRecordLayout::Scalar
                    | GriddedNormalRecordLayout::ChannelLocal { .. }
                    | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                    | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
                    | GriddedNormalRecordLayout::Joint { .. } => {
                        for (local, group) in prepared.groups[group_range].iter().enumerate() {
                            let frame_ordinal = usize::try_from(group.frame_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                            let (sequence, encoded) = frame_at(frame_ordinal)
                                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                            if prepared.frame_sequences.get(frame_ordinal) != Some(&sequence) {
                                return Err(SpectralOperatorError::BlockSequence);
                            }
                            let start = usize::try_from(group.records.start)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                .checked_mul(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let end = usize::try_from(group.records.end)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                .checked_mul(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let mut prediction = Complex64::default();
                            for bytes in encoded
                                .get(start..end)
                                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?
                                .chunks_exact(prepared.record_bytes)
                            {
                                let record = decode_domain_record(
                                    bytes,
                                    &self.tile_catalogs,
                                    self.program.output_plane_count()?,
                                )?;
                                let polarizations =
                                    self.program.manifest.specification.polarization_count();
                                let output_channel = record.output_channel / polarizations;
                                let polarization = record.output_channel % polarizations;
                                let operator = self
                                    .operators
                                    .get(record.chart_ordinal)
                                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
                                prediction += if let Some(aw) = record.aw {
                                    operator.predict_gridded_normal_aw_polarization(
                                        output_channel,
                                        polarization,
                                        aw,
                                        record.forward_scale,
                                    )?
                                } else {
                                    operator.predict_gridded_normal_polarization(
                                        output_channel,
                                        polarization,
                                        record.taps,
                                        record.forward_scale,
                                    )?
                                };
                            }
                            if !prediction.re.is_finite() || !prediction.im.is_finite() {
                                return Err(SpectralOperatorError::GeneratedNonfinite);
                            }
                            owner.values[local] = prediction;
                        }
                    }
                    GriddedNormalRecordLayout::Taylor(plan) => {
                        for (local, group) in prepared.groups[group_range].iter().enumerate() {
                            if group.record_count() != 1 {
                                return Err(SpectralOperatorError::InvalidGriddedRecord);
                            }
                            let frame_ordinal = usize::try_from(group.frame_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                            let (sequence, encoded) = frame_at(frame_ordinal)
                                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                            if prepared.frame_sequences.get(frame_ordinal) != Some(&sequence) {
                                return Err(SpectralOperatorError::BlockSequence);
                            }
                            let start = usize::try_from(group.records.start)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                .checked_mul(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let end = start
                                .checked_add(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let record = decode_taylor_record(
                                encoded
                                    .get(start..end)
                                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                                self.tile_catalogs
                                    .grid_shape(0)
                                    .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                                plan.normal_moment_count(),
                            )?;
                            let value_start = local
                                .checked_mul(prepared.prediction_width)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let value_end = value_start
                                .checked_add(prepared.prediction_width)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let GriddedNormalPredictionLane {
                                values,
                                model_scratch,
                                moment_scratch,
                                ..
                            } = &mut *owner;
                            record.fill_moments(moment_scratch)?;
                            self.operators[0].predict_gridded_block_normal(
                                record.taps,
                                moment_scratch,
                                model_scratch,
                                values
                                    .get_mut(value_start..value_end)
                                    .ok_or(SpectralOperatorError::IncompleteCoverage)?,
                            )?;
                        }
                    }
                }
            }
            GriddedNormalWorkKind::Accumulation => {
                prepared.with_published_predictions(|predictions| {
                    for task in prepared
                        .tasks
                        .iter()
                        .filter(|task| usize::from(task.lane) == work.lane)
                    {
                        let (domain_ordinal, geometry) = self.tile_catalogs.geometry(
                            usize::try_from(task.tile_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                        )?;
                        let mut accumulator =
                            self.tile_accumulators[usize::try_from(task.accumulator_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?]
                            .lock()
                            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
                        let route_start = usize::try_from(task.routes.start)
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                        let route_end = usize::try_from(task.routes.end)
                            .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                        for route in &prepared.routes[route_start..route_end] {
                            let frame_ordinal = usize::try_from(route.frame_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?;
                            let (sequence, encoded) = frame_at(frame_ordinal)
                                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                            if prepared.frame_sequences.get(frame_ordinal) != Some(&sequence) {
                                return Err(SpectralOperatorError::BlockSequence);
                            }
                            let start = usize::try_from(route.record_ordinal)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                .checked_mul(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let end = start
                                .checked_add(prepared.record_bytes)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let record_bytes = encoded
                                .get(start..end)
                                .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
                            let group = prepared
                                .groups
                                .get(
                                    usize::try_from(route.group_ordinal)
                                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                                )
                                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                            let prediction_start = usize::try_from(group.prediction_index)
                                .map_err(|_| SpectralOperatorError::CoverageOverflow)?
                                .checked_mul(prepared.prediction_width)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let prediction_end = prediction_start
                                .checked_add(prepared.prediction_width)
                                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                            let predicted = predictions[usize::from(group.prediction_lane)]
                                .values
                                .get(prediction_start..prediction_end)
                                .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                            let GriddedNormalTileAccumulator {
                                grids,
                                compensations,
                            } = &mut *accumulator;
                            match prepared.record_layout {
                                GriddedNormalRecordLayout::Scalar
                                | GriddedNormalRecordLayout::ChannelLocal { .. }
                                | GriddedNormalRecordLayout::TaylorWithCoordinates(_)
                                | GriddedNormalRecordLayout::TaylorViaChannelMajor { .. }
                                | GriddedNormalRecordLayout::Joint { .. } => {
                                    let record = decode_domain_record(
                                        record_bytes,
                                        &self.tile_catalogs,
                                        self.program.output_plane_count()?,
                                    )?;
                                    if record.chart_ordinal != domain_ordinal {
                                        return Err(SpectralOperatorError::GriddedRecordMismatch);
                                    }
                                    let predicted = predicted
                                        .first()
                                        .copied()
                                        .ok_or(SpectralOperatorError::IncompleteCoverage)?;
                                    let polarizations =
                                        self.program.manifest.specification.polarization_count();
                                    if let Some(aw) = record.aw {
                                        self.operators[domain_ordinal]
                                            .grid_gridded_normal_local_aw_polarization(
                                                grids,
                                                compensations,
                                                geometry.origin,
                                                record.output_channel / polarizations,
                                                record.output_channel % polarizations,
                                                predicted,
                                                record.forward_scale.conj(),
                                                record.imaging_weight,
                                                aw,
                                            )?;
                                    } else {
                                        self.operators[domain_ordinal]
                                            .grid_gridded_normal_local_polarization(
                                                grids,
                                                compensations,
                                                GriddedNormalLocalContribution::new(
                                                    geometry.translated_taps(record.taps)?,
                                                    record.output_channel / polarizations,
                                                    record.output_channel % polarizations,
                                                    predicted,
                                                    record.forward_scale.conj()
                                                        * record.imaging_weight,
                                                ),
                                            )?;
                                    }
                                }
                                GriddedNormalRecordLayout::Taylor(plan) => {
                                    let record = decode_taylor_record(
                                        record_bytes,
                                        self.tile_catalogs
                                            .grid_shape(0)
                                            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?,
                                        plan.normal_moment_count(),
                                    )?;
                                    self.operators[0].grid_gridded_block_normal_local(
                                        grids,
                                        compensations,
                                        geometry.translated_taps(record.taps)?,
                                        predicted,
                                    )?;
                                }
                            }
                        }
                    }
                    Ok(())
                })?;
            }
        }
        Ok(GriddedNormalPartial { work })
    }

    /// Commit one lane in canonical phase/lane order and merge the completed window.
    pub fn commit_two_domain(
        &mut self,
        partial: GriddedNormalPartial,
    ) -> Result<(), SpectralOperatorError> {
        let work = partial.work;
        if work.first_block_sequence != self.next_block_sequence
            || work.partition_key() as usize != self.next_partition_commit
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.next_partition_commit += 1;
        if self.next_partition_commit == GRIDDED_NORMAL_LANE_COUNT {
            if let Some(trace) = self.science_prediction_trace.as_mut() {
                let prepared = self
                    .two_domain
                    .read()
                    .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
                prepared.with_published_predictions(|predictions| {
                    for prediction in predictions {
                        for value in &prediction.values {
                            trace.push_complex(*value);
                        }
                    }
                    Ok(())
                })?;
            }
        }
        if self.next_partition_commit != GRIDDED_NORMAL_PARTITION_COUNT {
            return Ok(());
        }
        {
            let prepared = self
                .two_domain
                .read()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
            for task in &prepared.tasks {
                let (domain_ordinal, geometry) = self.tile_catalogs.geometry(
                    usize::try_from(task.tile_ordinal)
                        .map_err(|_| SpectralOperatorError::CoverageOverflow)?,
                )?;
                let accumulator = self.tile_accumulators[usize::try_from(task.accumulator_ordinal)
                    .map_err(|_| SpectralOperatorError::CoverageOverflow)?]
                .lock()
                .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?;
                for plane in 0..self.normal_grids[domain_ordinal].len() {
                    for local_x in 0..geometry.shape[0] {
                        for local_y in 0..geometry.shape[1] {
                            let value = accumulator.grids[plane][(local_x, local_y)];
                            if value == Complex64::default() {
                                continue;
                            }
                            let target =
                                (geometry.origin[0] + local_x, geometry.origin[1] + local_y);
                            let cell = &mut self.normal_grids[domain_ordinal][plane][target];
                            let compensation =
                                &mut self.normal_compensations[domain_ordinal][plane][target];
                            let contribution =
                                value - accumulator.compensations[plane][(local_x, local_y)];
                            let updated = *cell + contribution;
                            *compensation = (updated - *cell) - contribution;
                            *cell = updated;
                        }
                    }
                }
            }
        }
        let applied_records = self
            .applied_records
            .checked_add(work.window_record_count)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        let next_block_sequence = self
            .next_block_sequence
            .checked_add(work.frame_count)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        self.two_domain
            .write()
            .map_err(|_| SpectralOperatorError::GriddedSectorPoisoned)?
            .reset_active()?;
        self.applied_records = applied_records;
        self.next_block_sequence = next_block_sequence;
        self.next_partition_commit = 0;
        Ok(())
    }

    pub(super) fn two_domain_routing_measurements(&self) -> GriddedNormalRoutingMeasurements {
        let prepared = self
            .two_domain
            .read()
            .expect("gridded-normal measurements require an unpoisoned owner");
        let active_frames = u64::try_from(prepared.active_frames)
            .expect("planned gridded-normal frame count fits u64");
        let routed_frames = self
            .next_block_sequence
            .checked_add(active_frames)
            .expect("validated gridded-normal frame coverage fits u64");
        let routed_records = self
            .program
            .manifest
            .descriptors
            .iter()
            .take(usize::try_from(routed_frames).expect("frame count fits usize"))
            .map(|descriptor| descriptor.record_count)
            .sum();
        GriddedNormalRoutingMeasurements {
            frames_routed: routed_frames,
            encoded_records: routed_records,
            routed_record_memberships: routed_records,
            prediction_groups: prepared.prediction_groups,
            degrid_records: routed_records,
            grid_records: self.applied_records,
            sector_rescans: 0,
            peak_physical_route_capacity_bytes: if routed_frames == 0 {
                0
            } else {
                prepared
                    .route_capacity_bytes()
                    .expect("planned route capacity remains representable")
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Condvar, Mutex as TestMutex, mpsc},
        time::Duration,
    };

    use super::*;

    fn taps(x: usize, y: usize) -> SampleTaps {
        SampleTaps {
            x: TapSpan {
                start: x,
                weight_index: 50,
            },
            y: TapSpan {
                start: y,
                weight_index: 50,
            },
        }
    }

    #[test]
    fn central_one_hot_tile_splits_into_four_balanced_canonical_shards() {
        let catalog = GriddedNormalTileCatalog::new([128, 128], SUPPORT).unwrap();
        let mut prepared = PreparedGriddedNormalTwoDomainWindow::with_record_capacities(
            &[100],
            catalog.geometries.len(),
            GriddedNormalRecordLayout::Scalar,
        )
        .unwrap();
        let tile = catalog.tile_ordinal(taps(61, 61)).unwrap();
        prepared
            .classifications
            .extend((0..100).map(|record| GriddedNormalClassification {
                tile_ordinal: tile as u32,
                group_ordinal: record,
                frame_ordinal: 0,
                record_ordinal: record,
                tap_count: GRIDDED_NORMAL_TAPS_PER_RECORD as u32,
            }));
        prepared.tile_counts[tile] = 100;
        prepared.prepare_tile_routes().unwrap();
        assert_eq!(prepared.tasks.len(), 4);
        assert_eq!(prepared.lane_record_counts, [25, 25, 25, 25]);
        assert_eq!(
            prepared.lane_tap_visit_counts,
            [25 * GRIDDED_NORMAL_TAPS_PER_RECORD; GRIDDED_NORMAL_LANE_COUNT]
        );
        assert_eq!(
            prepared
                .tasks
                .iter()
                .map(|task| task.shard)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            prepared.tasks.iter().filter(|task| task.shard != 0).count(),
            GRIDDED_NORMAL_HOT_TILE_DUPLICATES
        );
    }

    #[test]
    fn every_valid_support_three_center_maps_inside_its_tile_halo() {
        let shape = [130, 126];
        let catalog = GriddedNormalTileCatalog::new(shape, SUPPORT).unwrap();
        for x in SUPPORT..shape[0] - SUPPORT {
            for y in SUPPORT..shape[1] - SUPPORT {
                let sample = taps(x - SUPPORT, y - SUPPORT);
                let tile = catalog.tile_ordinal(sample).unwrap();
                catalog.geometries[tile].translated_taps(sample).unwrap();
            }
        }
    }

    #[test]
    fn planned_route_capacity_matches_exact_reusable_vector_capacities() {
        let shape = [128, 128];
        let catalog = GriddedNormalTileCatalog::new(shape, SUPPORT).unwrap();
        let prepared = PreparedGriddedNormalTwoDomainWindow::with_record_capacities(
            &[11, 7, 5],
            catalog.geometries.len(),
            GriddedNormalRecordLayout::Scalar,
        )
        .unwrap();
        assert_eq!(
            prepared.route_capacity_bytes().unwrap(),
            gridded_normal_route_capacity_bytes(23, 3, 1).unwrap()
        );
    }

    #[test]
    fn taylor_prediction_lanes_reuse_planned_flat_values_and_scratch() {
        let catalog = GriddedNormalTileCatalog::new([128, 128], SUPPORT).unwrap();
        let plan = crate::block_normal::BlockNormalPlan::taylor(1.0, 3).unwrap();
        let record_count = 11;
        let mut prepared = PreparedGriddedNormalTwoDomainWindow::with_record_capacities(
            &[record_count],
            catalog.geometries.len(),
            GriddedNormalRecordLayout::Taylor(plan),
        )
        .unwrap();
        prepared
            .groups
            .extend((0..record_count).map(|record| GriddedNormalGroupSpan {
                frame_ordinal: 0,
                records: record as u32..record as u32 + 1,
                prediction_lane: 0,
                prediction_index: 0,
            }));

        prepared.prepare_prediction_lanes().unwrap();

        let group_capacity = record_count.div_ceil(GRIDDED_NORMAL_LANE_COUNT) + 1;
        assert_eq!(
            prepared
                .predictions
                .iter()
                .map(|owner| owner.read().unwrap().values.len())
                .sum::<usize>(),
            record_count * plan.coefficient_term_count()
        );
        for owner in &prepared.predictions {
            let owner = owner.read().unwrap();
            assert_eq!(
                owner.values.capacity(),
                group_capacity * plan.coefficient_term_count()
            );
            assert_eq!(
                owner.model_scratch.capacity(),
                plan.coefficient_term_count()
            );
            assert_eq!(owner.moment_scratch.capacity(), plan.normal_moment_count());
        }
        assert_eq!(
            prepared.route_capacity_bytes().unwrap(),
            gridded_normal_route_capacity_bytes(record_count, 1, plan.coefficient_term_count())
                .unwrap()
        );
    }

    #[test]
    fn execution_metadata_projection_matches_all_retained_heap_descriptors() {
        for (support, aw_projection) in [(SUPPORT, false), (50, true)] {
            let shape = [128, 128];
            let depth = 3;
            let catalogs = GriddedNormalDomainTileCatalogs::new_for_projection(
                [shape],
                support,
                aw_projection,
            )
            .unwrap();
            let prepared = PreparedGriddedNormalTwoDomainWindow::with_projection_record_capacities(
                &[1],
                catalogs.tile_count(),
                GriddedNormalRecordLayout::Scalar,
                aw_projection,
            )
            .unwrap();
            let accumulators = catalogs.accumulators(depth).unwrap();
            let plane_shape = (shape[0], shape[1]);
            let domain_planes = || {
                vec![
                    (0..depth)
                        .map(|_| Array2::<Complex64>::zeros(plane_shape))
                        .collect::<Vec<_>>(),
                ]
            };
            let normal_grids = domain_planes();
            let normal_compensations = domain_planes();

            let tile_plane_descriptors = accumulators
                .iter()
                .map(|accumulator| {
                    let accumulator = accumulator.lock().unwrap();
                    (accumulator.grids.capacity() + accumulator.compensations.capacity())
                        * size_of::<Array2<Complex64>>()
                })
                .sum::<usize>();
            let catalog_metadata_bytes = catalogs.catalogs.capacity()
                * size_of::<GriddedNormalTileCatalog>()
                + catalogs.offsets.capacity() * size_of::<usize>()
                + catalogs
                    .catalogs
                    .iter()
                    .map(|catalog| {
                        catalog.geometries.capacity() * size_of::<GriddedNormalTileGeometry>()
                    })
                    .sum::<usize>();
            let merge_descriptor_bytes = (normal_grids.capacity()
                + normal_compensations.capacity())
                * size_of::<Vec<Array2<Complex64>>>()
                + normal_grids
                    .iter()
                    .chain(&normal_compensations)
                    .map(|planes| planes.capacity() * size_of::<Array2<Complex64>>())
                    .sum::<usize>();
            let actual_metadata_bytes = catalog_metadata_bytes
                + prepared.tile_counts.capacity() * size_of::<u32>()
                + prepared.tile_cursors.capacity() * size_of::<u32>()
                + prepared.tile_offsets.capacity() * size_of::<u32>()
                + prepared.tasks.capacity() * size_of::<GriddedNormalTileTask>()
                + accumulators.capacity() * size_of::<Mutex<GriddedNormalTileAccumulator>>()
                + tile_plane_descriptors
                + merge_descriptor_bytes;
            assert_eq!(
                domain_execution_residency_for_projection([shape], depth, support, aw_projection)
                    .unwrap()
                    .metadata_bytes(),
                actual_metadata_bytes
            );
        }
    }

    #[test]
    fn t51_aw_catalog_reserves_and_routes_every_edge_tile() {
        for (shape, support, expected_tiles) in [
            ([65, 65], 32, 9),
            ([65, 97], 32, 12),
            ([4096, 4096], 50, 16_384),
        ] {
            let catalog = GriddedNormalTileCatalog::new_for_projection(shape, support, true)
                .expect("AW catalog with support spanning tiles");
            assert_eq!(catalog.geometries.len(), expected_tiles);
            assert_eq!(catalog.geometries.capacity(), expected_tiles);
            let corners = [
                [0, 0],
                [0, shape[1] - 1],
                [shape[0] - 1, 0],
                [shape[0] - 1, shape[1] - 1],
            ];
            let mut corner_tiles = std::collections::BTreeSet::new();
            for center in corners {
                let ordinal = catalog.tile_ordinal_center(center).expect("AW edge tile");
                let geometry = catalog.geometries[ordinal];
                for (axis, coordinate) in center.into_iter().enumerate() {
                    assert!(coordinate >= geometry.origin[axis]);
                    assert!(coordinate < geometry.origin[axis] + geometry.shape[axis]);
                }
                corner_tiles.insert(ordinal);
            }
            assert_eq!(corner_tiles.len(), 4);
        }
    }

    #[test]
    fn prediction_lane_capacity_covers_every_adversarial_complete_group_composition() {
        let shape = [128, 128];
        let catalog = GriddedNormalTileCatalog::new(shape, SUPPORT).unwrap();
        for records in 1_usize..=10 {
            let capacity = records.div_ceil(GRIDDED_NORMAL_LANE_COUNT) + 1;
            for boundaries in 0_usize..(1_usize << records.saturating_sub(1)) {
                let mut prepared = PreparedGriddedNormalTwoDomainWindow::with_record_capacities(
                    &[records],
                    catalog.geometries.len(),
                    GriddedNormalRecordLayout::Scalar,
                )
                .unwrap();
                let mut start = 0usize;
                for record in 1..=records {
                    if record == records || boundaries & (1 << (record - 1)) != 0 {
                        prepared.groups.push(GriddedNormalGroupSpan {
                            frame_ordinal: 0,
                            records: start as u32..record as u32,
                            prediction_lane: 0,
                            prediction_index: 0,
                        });
                        start = record;
                    }
                }
                prepared.prepare_prediction_lanes().unwrap();
                for owner in &prepared.predictions {
                    let owner = owner.read().unwrap();
                    assert!(owner.values.len() <= capacity);
                    assert_eq!(owner.values.capacity(), capacity);
                }
                assert_eq!(
                    prepared
                        .predictions
                        .iter()
                        .map(|owner| owner.read().unwrap().values.len())
                        .sum::<usize>(),
                    prepared.groups.len(),
                    "every complete group is assigned exactly once"
                );
            }
        }
    }

    #[test]
    fn published_predictions_support_four_concurrent_accumulation_readers() {
        let catalog = GriddedNormalTileCatalog::new([128, 128], SUPPORT).unwrap();
        let mut prepared = PreparedGriddedNormalTwoDomainWindow::with_record_capacities(
            &[GRIDDED_NORMAL_LANE_COUNT],
            catalog.geometries.len(),
            GriddedNormalRecordLayout::Scalar,
        )
        .unwrap();
        prepared
            .groups
            .extend(
                (0..GRIDDED_NORMAL_LANE_COUNT).map(|record| GriddedNormalGroupSpan {
                    frame_ordinal: 0,
                    records: record as u32..record as u32 + 1,
                    prediction_lane: 0,
                    prediction_index: 0,
                }),
            );
        prepared.prepare_prediction_lanes().unwrap();
        for (lane, owner) in prepared.predictions.iter().enumerate() {
            owner.write().unwrap().values[0] = Complex64::new((lane + 1) as f64, 0.0);
        }

        let release = Arc::new((TestMutex::new(false), Condvar::new()));
        let (entered_sender, entered_receiver) = mpsc::channel();
        std::thread::scope(|scope| {
            for _ in 0..GRIDDED_NORMAL_LANE_COUNT {
                let prepared = &prepared;
                let release = Arc::clone(&release);
                let entered_sender = entered_sender.clone();
                scope.spawn(move || {
                    prepared
                        .with_published_predictions(|predictions| {
                            assert_eq!(
                                predictions
                                    .iter()
                                    .map(|lane| lane.values[0].re)
                                    .sum::<f64>(),
                                10.0
                            );
                            entered_sender.send(()).unwrap();
                            let (released, changed) = &*release;
                            let mut released = released.lock().unwrap();
                            while !*released {
                                released = changed.wait(released).unwrap();
                            }
                            Ok(())
                        })
                        .unwrap();
                });
            }

            let mut concurrent_readers = 0;
            while concurrent_readers < GRIDDED_NORMAL_LANE_COUNT
                && entered_receiver
                    .recv_timeout(Duration::from_secs(1))
                    .is_ok()
            {
                concurrent_readers += 1;
            }
            let (released, changed) = &*release;
            *released.lock().unwrap() = true;
            changed.notify_all();
            assert_eq!(concurrent_readers, GRIDDED_NORMAL_LANE_COUNT);
        });
    }
}
