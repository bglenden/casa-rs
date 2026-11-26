// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_core::measures::direction::{DirectionRef, MDirection};

use crate::MeasurementSet;
use crate::derived::engine::MsCalEngine;
use crate::selection::syntax::ChannelSelection;

use super::clip_time::{read_clip_times, sort_iterator_rows};
use super::geometry::{apply_geometric_flags, phase_center};
use super::{ChangeSet, CorrelationSelection, FlaggingError};

pub(super) fn apply(
    ms: &mut MeasurementSet,
    selected: &[usize],
    limits: [f64; 2],
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    path: &str,
) -> Result<ChangeSet, FlaggingError> {
    let outside = rows_outside_limits(ms, selected, limits, path)?;
    apply_geometric_flags(ms, &outside, channels, correlations)
}

fn rows_outside_limits(
    ms: &MeasurementSet,
    selected: &[usize],
    [lower, upper]: [f64; 2],
    path: &str,
) -> Result<Vec<usize>, FlaggingError> {
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let fail = |reason: String| FlaggingError::MutateFlags {
        path: path.to_owned(),
        reason,
    };
    let engine = MsCalEngine::new(ms)
        .and_then(|engine| engine.with_field_ephemerides(ms))
        .map_err(|error| fail(error.to_string()))?;
    let mut ordered = read_clip_times(ms, selected, path)?;
    sort_iterator_rows(&mut ordered);
    let mut center: Option<(usize, MDirection)> = None;
    let mut cached_time = None;
    let mut elevations = BTreeMap::<usize, f64>::new();
    let mut outside = Vec::new();
    for row in ordered {
        let field = usize::try_from(row.field())
            .map_err(|_| fail(format!("negative FIELD_ID at row {}", row.row)))?;
        let time = row.time();
        // The visibility iterator refreshes the phase center on a field
        // change using its first selected timestamp. Every antenna frame
        // still uses the actual TIME of its visibility row.
        if center
            .as_ref()
            .is_none_or(|(previous, _)| *previous != field)
        {
            let direction =
                phase_center(ms, &engine, field, time).map_err(|error| fail(error.to_string()))?;
            center = Some((field, direction));
            elevations.clear();
        }
        if cached_time != Some(time) {
            cached_time = Some(time);
            elevations.clear();
        }
        let (_, direction) = center.as_ref().expect("current field has a phase center");
        let mut elevation = |antenna| -> Result<f64, FlaggingError> {
            let antenna = usize::try_from(antenna)
                .map_err(|_| fail(format!("negative antenna ID at row {}", row.row)))?;
            if let Some(&value) = elevations.get(&antenna) {
                return Ok(value);
            }
            let frame = engine
                .antenna_frame(time, antenna)
                .map_err(|error| fail(error.to_string()))?;
            let angle = direction
                .convert_to(DirectionRef::AZEL, &frame)
                .map_err(|error| fail(error.to_string()))?;
            let degrees = angle.latitude_rad().to_degrees();
            elevations.insert(antenna, degrees);
            Ok(degrees)
        };
        let [first, second] = row.antennas();
        let left = elevation(first)?;
        let right = elevation(second)?;
        if left < lower || right < lower || left > upper || right > upper {
            outside.push(row.row);
        }
    }
    outside.sort_unstable();
    Ok(outside)
}
