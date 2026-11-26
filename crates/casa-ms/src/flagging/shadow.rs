// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Arc;

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MEpoch, MPosition, MeasFrame, MeasuresProvider, UvwFrame};
use casa_core::{ArrayValue, ScalarValue};

use crate::derived::engine::MsCalEngine;
use crate::selection::syntax::ChannelSelection;
use crate::{MeasurementSet, MsReadPlan, MsResult, MsSelectionIoBudget, open_measures_runtime};

use super::clip_time::{ClipTimedRow, integration_blocks, read_clip_times, sort_iterator_rows};
use super::geometry::{apply_geometric_flags, phase_center};
use super::{ChangeSet, CorrelationSelection, FlaggingError};

/// A dish used for flagging shadows. Additional dishes do not become MS rows.
#[derive(Clone, Copy, Debug)]
pub struct ShadowAntenna {
    /// ITRF position in meters.
    pub position_m: [f64; 3],
    /// Dish diameter in meters.
    pub diameter_m: f64,
}

/// Controls CASA-style flagging of baselines that include a shadowed dish.
#[derive(Clone, Debug)]
pub struct ShadowOptions {
    /// Allowed overlap of the projected dish diameters, in meters.
    pub tolerance_m: f64,
    /// Dishes outside the MS antenna table that can cast shadows.
    pub additional: Vec<ShadowAntenna>,
    /// Whether the parsed task configuration admits a shadow agent.
    pub enabled: bool,
}

impl Default for ShadowOptions {
    fn default() -> Self {
        Self {
            tolerance_m: 0.0,
            additional: Vec::new(),
            enabled: true,
        }
    }
}

pub(super) fn apply(
    ms: &mut MeasurementSet,
    selected: &[usize],
    options: &ShadowOptions,
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    path: &str,
) -> Result<ChangeSet, FlaggingError> {
    if !options.enabled {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    if selected.is_empty() {
        return Ok(ChangeSet::default());
    }
    let affected = shadowed_rows(ms, selected, options, path)?;
    apply_geometric_flags(ms, &affected, channels, correlations)
}

struct Baseline {
    centroid: f64,
    first: usize,
    second: usize,
    uvw_m: [f64; 3],
}

fn shadowed_rows(
    ms: &MeasurementSet,
    selected: &[usize],
    options: &ShadowOptions,
    path: &str,
) -> Result<Vec<usize>, FlaggingError> {
    let fail = |reason: String| FlaggingError::MutateFlags {
        path: path.to_owned(),
        reason,
    };
    let antenna_table = ms.antenna().map_err(|error| fail(error.to_string()))?;
    let antenna_count = antenna_table.row_count();
    let antennas = (0..antenna_count)
        .map(|row| {
            Ok(ShadowAntenna {
                position_m: antenna_table.position(row)?,
                diameter_m: antenna_table.dish_diameter(row)?,
            })
        })
        .collect::<MsResult<Vec<_>>>()
        .map_err(|error| fail(error.to_string()))?;
    let antennas: Vec<_> = antennas
        .into_iter()
        .chain(options.additional.iter().copied())
        .collect();
    let mut ordered = read_clip_times(ms, selected, path)?;
    sort_iterator_rows(&mut ordered);
    let bytes_per_row = size_of::<Baseline>()
        + size_of::<usize>()
        + size_of::<Option<ScalarValue>>()
        + size_of::<Option<ArrayValue>>()
        + size_of::<[f64; 3]>();
    let budget = MsSelectionIoBudget::from_system_memory(2, bytes_per_row, None)
        .map_err(|error| fail(error.to_string()))?;
    let plan = MsReadPlan::new(ordered.len(), budget).map_err(|error| fail(error.to_string()))?;
    let batch_rows = plan.rows_per_block.max(1);
    let mut affected = Vec::new();
    let mut engine = None;
    let mut measures = None;
    let mut field = None;
    let mut field_start = 0.0;
    let mut center = None;
    let mut time = None;
    let mut shadowed = vec![false; antennas.len()];
    for block in integration_blocks(&ordered, batch_rows) {
        // Physical I/O is bounded even when one logical integration is large.
        let mut baselines = Vec::with_capacity(block.len());
        for rows in block.chunks(batch_rows) {
            baselines.extend(read_baselines(ms, rows, antenna_count, fail)?);
        }
        let mut unread = baselines.as_slice();
        for integration in block.chunk_by(ClipTimedRow::shares_integration) {
            let next_field = usize::try_from(integration[0].field())
                .map_err(|_| fail(format!("negative FIELD_ID at row {}", integration[0].row)))?;
            if field != Some(next_field) {
                field = Some(next_field);
                field_start = integration[0].time();
                center = None;
            }
            let (baselines, remaining) = unread.split_at(integration.len());
            unread = remaining;
            for (index, baseline) in baselines.iter().enumerate() {
                if time != Some(baseline.centroid) {
                    time = Some(baseline.centroid);
                    let rows = baselines[index..]
                        .iter()
                        .take_while(|candidate| candidate.centroid <= baseline.centroid);
                    shadowed = shadowed_antennas(&antennas, rows, options.tolerance_m, || {
                        if engine.is_none() {
                            // CASA's visibility-buffer phase center also
                            // accepts legacy geocentric apparent ephemerides.
                            engine = Some(MsCalEngine::for_calibration(ms)?);
                            measures = Some(open_measures_runtime()?);
                        }
                        if center.is_none() {
                            center = Some(phase_center(
                                ms,
                                engine.as_ref().expect("shadow measures are initialized"),
                                next_field,
                                field_start,
                            )?);
                        }
                        let direction = center.as_ref().expect("the field has a phase center");
                        project_positions(
                            &antennas,
                            direction,
                            baseline.centroid,
                            Arc::clone(measures.as_ref().expect("shadow measures are initialized")),
                        )
                    })
                    .map_err(|error| fail(error.to_string()))?;
                }
                if shadowed[baseline.first] || shadowed[baseline.second] {
                    affected.push(integration[index].row);
                }
            }
        }
    }
    affected.sort_unstable();
    Ok(affected)
}

fn read_baselines(
    ms: &MeasurementSet,
    rows: &[ClipTimedRow],
    antenna_count: usize,
    fail: impl Fn(String) -> FlaggingError,
) -> Result<Vec<Baseline>, FlaggingError> {
    let indices: Vec<_> = rows.iter().map(|row| row.row).collect();
    let table = ms.main_table();
    let times = table
        .column_accessor("TIME_CENTROID")
        .and_then(|column| column.scalar_cells_owned_for_rows(&indices))
        .map_err(|error| fail(error.to_string()))?;
    let uvws = table
        .column_accessor("UVW")
        .and_then(|column| column.array_cells_owned_uncached(&indices))
        .map_err(|error| fail(error.to_string()))?;
    rows.iter()
        .zip(times)
        .zip(uvws)
        .map(|((row, time), uvw)| {
            let antenna = |id: i32| {
                usize::try_from(id)
                    .ok()
                    .filter(|&index| index < antenna_count)
                    .ok_or_else(|| fail(format!("invalid antenna {id} at MAIN row {}", row.row)))
            };
            let [first, second] = row.antennas();
            let centroid = match time {
                Some(ScalarValue::Float64(seconds)) if seconds.is_finite() => seconds,
                _ => {
                    return Err(fail(format!(
                        "TIME_CENTROID row {} must be a finite Double",
                        row.row
                    )));
                }
            };
            let uvw_m = match uvw {
                Some(ArrayValue::Float64(array)) if array.shape() == [3] => {
                    [array[[0]], array[[1]], array[[2]]]
                }
                _ => {
                    return Err(fail(format!(
                        "UVW row {} must have three Double values",
                        row.row
                    )));
                }
            };
            Ok(Baseline {
                centroid,
                first: antenna(first)?,
                second: antenna(second)?,
                uvw_m,
            })
        })
        .collect()
}

fn shadowed_antennas<'a>(
    antennas: &[ShadowAntenna],
    observed: impl IntoIterator<Item = &'a Baseline>,
    tolerance: f64,
    project: impl FnOnce() -> MsResult<Vec<[f64; 3]>>,
) -> MsResult<Vec<bool>> {
    let mut shadowed = vec![false; antennas.len()];
    let mut present = BTreeSet::new();
    let mut check = |first: usize, second: usize, [u, v, w]: [f64; 3]| {
        let separation = (u * u + v * v).sqrt();
        let limit = (antennas[first].diameter_m + antennas[second].diameter_m) / 2.0 - tolerance;
        if separation < limit {
            shadowed[if w < 0.0 { first } else { second }] = true;
        }
    };
    for row in observed {
        if row.first != row.second {
            present.insert((row.first.min(row.second), row.first.max(row.second)));
            check(row.first, row.second, row.uvw_m);
        }
    }
    let count = antennas.len();
    if present.len() != count * count.saturating_sub(1) / 2 {
        let positions = project()?;
        for (first, position) in positions.iter().enumerate() {
            for (second, other) in positions.iter().enumerate().skip(first + 1) {
                if !present.contains(&(first, second)) {
                    let baseline = std::array::from_fn(|axis| other[axis] - position[axis]);
                    check(first, second, baseline);
                }
            }
        }
    }
    Ok(shadowed)
}

fn project_positions(
    antennas: &[ShadowAntenna],
    center: &MDirection,
    centroid: f64,
    measures: Arc<dyn MeasuresProvider>,
) -> MsResult<Vec<[f64; 3]>> {
    let origin = antennas[0].position_m;
    let position = MPosition::new_itrf(origin[0], origin[1], origin[2]);
    let epoch = MEpoch::from_mjd_seconds(centroid, EpochRef::UT1);
    let direction = if center.refer() == DirectionRef::J2000 {
        center.clone()
    } else {
        let frame = MeasFrame::new()
            .with_position(position.clone())
            .with_epoch(epoch.clone())
            .with_measures(measures.clone());
        center.convert_to(DirectionRef::J2000, &frame)?
    };
    let projection = UvwFrame::new(direction.as_angles().into(), epoch, &position, measures)?;
    Ok(antennas
        .iter()
        .map(|antenna| {
            let offset = std::array::from_fn(|axis| origin[axis] - antenna.position_m[axis]);
            projection.baseline_uvw(offset)
        })
        .collect())
}
