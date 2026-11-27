// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Default)]
pub(super) struct RowGeometryCache {
    pub(super) elevations: HashMap<RowGeometryKey, f64>,
    pub(super) projected_offsets: HashMap<ProjectedOffsetKey, [f64; 3]>,
    pub(super) materialized_grids: HashMap<MaterializedGridKey, Arc<CalibrationGrid>>,
    pub(super) bandpass_power: bandpass::PowerCache,
    pub(super) ordered_gains: ordered_gain::Cache,
    pub(super) calibration_lookups: HashMap<CalibrationLookupKey, Option<fields::GridAtTime>>,
    pub(super) transient_lookups: HashMap<CalibrationLookupKey, Option<fields::GridAtTime>>,
    pub(super) transient_frequency_grids: HashMap<MaterializedGridKey, Arc<CalibrationGrid>>,
    pub(super) transient_time: Option<u64>,
    pub(super) additive_matrices: HashMap<(usize, i32), additive::CachedMatrix>,
    pub(super) last_elevation_context: Option<RowGeometryContextKey>,
    pub(super) last_elevations: Vec<Option<f64>>,
    pub(super) last_projected_context: Option<RowGeometryContextKey>,
    pub(super) last_projected_offsets: Vec<Option<LastProjectedOffset>>,
    pub(super) last_fast_term_context: Option<RowGeometryContextKey>,
    pub(super) last_fast_antpos_delays: Vec<Vec<Option<FastAntposDelay>>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RowGeometryKey {
    pub(super) time_bits: u64,
    pub(super) field_id: i32,
    pub(super) antenna_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RowGeometryContextKey {
    pub(super) time_bits: u64,
    pub(super) field_id: i32,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LastProjectedOffset {
    pub(super) offset_bits: [u64; 3],
    pub(super) uvw: [f64; 3],
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FastAntposDelay {
    pub(super) offset_bits: [u64; 3],
    pub(super) delay_ns: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct ProjectedOffsetKey {
    pub(super) row: RowGeometryKey,
    pub(super) offset_bits: [u64; 3],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct MaterializedGridKey {
    pub(super) row: RowGeometryKey,
    pub(super) kind: u8,
    pub(super) data_spw_id: i32,
    pub(super) grid_id: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CalibrationLookupKey {
    pub(super) table_index: usize,
    pub(super) field_id: i32,
    pub(super) spw_id: i32,
    pub(super) antenna_id: i32,
    pub(super) time_bits: u64,
    pub(super) interp: u8,
    pub(super) observation_id: i32,
    pub(super) scan_number: i32,
    pub(super) data_spw_id: i32,
}

impl RowGeometryCache {
    pub(super) fn elevation(
        &mut self,
        engine: &MsCalEngine,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
    ) -> MsResult<f64> {
        let key = RowGeometryKey::new(time_seconds, field_id, antenna_id);
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            if self.last_elevation_context == Some(context) {
                if let Some(Some(elevation)) = self.last_elevations.get(antenna_index) {
                    return Ok(*elevation);
                }
            } else {
                self.last_elevation_context = Some(context);
                self.last_elevations.clear();
            }
            if let Some(elevation) = self.elevations.get(&key).copied() {
                self.remember_last_elevation(antenna_index, elevation);
                return Ok(elevation);
            }
        } else if let Some(elevation) = self.elevations.get(&key) {
            return Ok(*elevation);
        }
        let (_az, elevation) = engine.azel(
            time_seconds,
            usize::try_from(field_id).unwrap_or(usize::MAX),
            usize::try_from(antenna_id).unwrap_or(usize::MAX),
        )?;
        self.elevations.insert(key, elevation);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            self.remember_last_elevation(antenna_index, elevation);
        }
        Ok(elevation)
    }

    pub(super) fn project_itrf_offset_to_uvw(
        &mut self,
        engine: &MsCalEngine,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
        offset_m: [f64; 3],
    ) -> MsResult<[f64; 3]> {
        let offset_bits = [
            offset_m[0].to_bits(),
            offset_m[1].to_bits(),
            offset_m[2].to_bits(),
        ];
        let key = ProjectedOffsetKey {
            row: RowGeometryKey::new(time_seconds, field_id, antenna_id),
            offset_bits,
        };
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            if self.last_projected_context == Some(context) {
                if let Some(Some(projected)) = self.last_projected_offsets.get(antenna_index)
                    && projected.offset_bits == offset_bits
                {
                    return Ok(projected.uvw);
                }
            } else {
                self.last_projected_context = Some(context);
                self.last_projected_offsets.clear();
            }
            if let Some(uvw) = self.projected_offsets.get(&key).copied() {
                self.remember_last_projected_offset(antenna_index, offset_bits, uvw);
                return Ok(uvw);
            }
        } else if let Some(uvw) = self.projected_offsets.get(&key) {
            return Ok(*uvw);
        }
        let uvw = engine.project_itrf_offset_to_uvw(
            time_seconds,
            usize::try_from(field_id).unwrap_or(usize::MAX),
            usize::try_from(antenna_id).unwrap_or(usize::MAX),
            offset_m,
        )?;
        self.projected_offsets.insert(key, uvw);
        if let Ok(antenna_index) = usize::try_from(antenna_id) {
            self.remember_last_projected_offset(antenna_index, offset_bits, uvw);
        }
        Ok(uvw)
    }

    fn remember_last_elevation(&mut self, antenna_index: usize, elevation: f64) {
        if self.last_elevations.len() <= antenna_index {
            self.last_elevations.resize(antenna_index + 1, None);
        }
        self.last_elevations[antenna_index] = Some(elevation);
    }

    fn remember_last_projected_offset(
        &mut self,
        antenna_index: usize,
        offset_bits: [u64; 3],
        uvw: [f64; 3],
    ) {
        if self.last_projected_offsets.len() <= antenna_index {
            self.last_projected_offsets.resize(antenna_index + 1, None);
        }
        self.last_projected_offsets[antenna_index] = Some(LastProjectedOffset { offset_bits, uvw });
    }

    pub(super) fn ensure_fast_term_context(&mut self, time_seconds: f64, field_id: i32) {
        let context = RowGeometryContextKey::new(time_seconds, field_id);
        if self.last_fast_term_context != Some(context) {
            self.last_fast_term_context = Some(context);
            self.last_fast_antpos_delays.clear();
        }
    }

    pub(super) fn cached_fast_antpos_delay(
        &mut self,
        table_index: usize,
        antenna_index: usize,
        offset_bits: [u64; 3],
    ) -> Option<f64> {
        self.last_fast_antpos_delays
            .get(table_index)
            .and_then(|values| values.get(antenna_index))
            .and_then(|value| *value)
            .and_then(|value| (value.offset_bits == offset_bits).then_some(value.delay_ns))
    }

    pub(super) fn remember_fast_antpos_delay(
        &mut self,
        table_index: usize,
        antenna_index: usize,
        offset_bits: [u64; 3],
        delay_ns: f64,
    ) {
        let values = resize_nested_vec(
            &mut self.last_fast_antpos_delays,
            table_index,
            antenna_index,
        );
        values[antenna_index] = Some(FastAntposDelay {
            offset_bits,
            delay_ns,
        });
    }

    pub(super) fn materialized_grid(
        &self,
        key: MaterializedGridKey,
        transient: bool,
    ) -> Option<Arc<CalibrationGrid>> {
        if transient {
            &self.transient_frequency_grids
        } else {
            &self.materialized_grids
        }
        .get(&key)
        .cloned()
    }

    pub(super) fn insert_materialized_grid(
        &mut self,
        key: MaterializedGridKey,
        grid: Arc<CalibrationGrid>,
        transient: bool,
    ) {
        if transient {
            &mut self.transient_frequency_grids
        } else {
            &mut self.materialized_grids
        }
        .insert(key, grid);
    }

    pub(super) fn calibration_lookup<F>(
        &mut self,
        key: CalibrationLookupKey,
        transient_time: Option<u64>,
        load: F,
    ) -> Result<Option<fields::GridAtTime>, ApplyExecutionError>
    where
        F: FnOnce() -> Result<Option<fields::GridAtTime>, ApplyExecutionError>,
    {
        let lookups = if let Some(time) = transient_time {
            if self.transient_time != Some(time) {
                self.transient_frequency_grids.clear();
                self.transient_lookups.clear();
                self.transient_time = Some(time);
            }
            &mut self.transient_lookups
        } else {
            &mut self.calibration_lookups
        };
        if let Some(grid) = lookups.get(&key) {
            return Ok(grid.clone());
        }
        let grid = load()?;
        lookups.insert(key, grid.clone());
        Ok(grid)
    }
}

impl RowGeometryKey {
    pub(super) fn new(time_seconds: f64, field_id: i32, antenna_id: i32) -> Self {
        Self {
            time_bits: time_seconds.to_bits(),
            field_id,
            antenna_id,
        }
    }
}

impl RowGeometryContextKey {
    fn new(time_seconds: f64, field_id: i32) -> Self {
        Self {
            time_bits: time_seconds.to_bits(),
            field_id,
        }
    }
}

pub(super) fn resize_nested_vec<T: Clone>(
    values: &mut Vec<Vec<Option<T>>>,
    table_index: usize,
    antenna_index: usize,
) -> &mut Vec<Option<T>> {
    if values.len() <= table_index {
        values.resize_with(table_index + 1, Vec::new);
    }
    let values = &mut values[table_index];
    if values.len() <= antenna_index {
        values.resize(antenna_index + 1, None);
    }
    values
}

pub(super) fn materialized_grid_key(
    grid: &Arc<CalibrationGrid>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    data_spw_id: i32,
) -> Option<MaterializedGridKey> {
    let kind = match grid.as_ref() {
        CalibrationGrid::Antpos(_) => 1,
        CalibrationGrid::GainCurve(_) => 2,
        CalibrationGrid::Opacity(_) | CalibrationGrid::SpectralOpacity(_) => 3,
        _ => return None,
    };
    Some(MaterializedGridKey {
        row: RowGeometryKey::new(time_seconds, field_id, antenna_id),
        kind,
        data_spw_id,
        grid_id: Arc::as_ptr(grid) as usize,
    })
}

pub(super) fn calibration_lookup_key(
    table_index: usize,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    interp: ApplyInterpolationMode,
) -> CalibrationLookupKey {
    CalibrationLookupKey {
        table_index,
        field_id,
        spw_id,
        antenna_id,
        time_bits: time_seconds.to_bits(),
        observation_id: -1,
        scan_number: -1,
        data_spw_id: -1,
        interp: match interp {
            ApplyInterpolationMode::Nearest => 1,
            ApplyInterpolationMode::Linear => 2,
            ApplyInterpolationMode::NearestLinear => 3,
            ApplyInterpolationMode::Casa { .. } => 4,
            ApplyInterpolationMode::Invalid => 0,
        },
    }
}
