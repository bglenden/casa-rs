// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select integrations in the central region of a double-circle scan.

use std::collections::BTreeMap;

use casa_core::measures::direction::DirectionRef;
use casa_ms::MeasurementSet;
use casa_ms::derived::engine::{MsCalEngine, raw_field_reference_direction};

use super::{error_text, normalization};
use crate::sd_solver::edges::Integration;
use crate::sd_solver::map_geometry::{MapTargets, SourceRow, Target};

type FeedKey = (i32, i32, i32);

pub(super) fn select<'a>(
    ms: &MeasurementSet,
    samples: &'a [Integration],
    requested_radius: Option<f64>,
) -> Result<Vec<&'a Integration>, String> {
    let spectral = ms.spectral_window().map_err(error_text)?;
    let antennas = ms.antenna().map_err(error_text)?;
    let channels = (0..spectral.row_count())
        .map(|window| spectral.num_chan(window).map_err(error_text))
        .collect::<Result<Vec<_>, _>>()?;
    let mut feeds = BTreeMap::<_, Vec<_>>::new();
    // Four-channel WVR integrations can pass a science feed's time ranges,
    // but never establish a central region on their own.
    for row in samples.iter().filter(|row| channels[row.window] != 4) {
        feeds
            .entry((key(row), row.description))
            .or_default()
            .push(row);
    }
    if feeds.is_empty() {
        return Ok(Vec::new());
    }
    let engine = MsCalEngine::new(ms)
        .and_then(|engine| engine.with_field_ephemerides(ms))
        .map_err(error_text)?;
    let mut targets = MapTargets::read(ms)?;
    let mut ranges = BTreeMap::<FeedKey, Vec<[f64; 2]>>::new();
    for ((key, _), rows) in feeds {
        if ranges.contains_key(&key) {
            continue;
        }
        let first = rows[0];
        let antenna = first.antenna as usize;
        let field = first.field as usize;
        let radius = normalization::radius(
            requested_radius,
            &spectral.chan_freq(first.window).map_err(error_text)?,
            antennas.dish_diameter(antenna).map_err(error_text)?,
        )?;
        let target = targets.field(field, SourceRow::Optional)?;
        let fixed = if matches!(target, Target::Fixed) {
            let frame = engine
                .antenna_frame(first.time, antenna)
                .map_err(error_text)?;
            let center = raw_field_reference_direction(ms, field)
                .map_err(error_text)?
                .convert_to(DirectionRef::J2000, &frame)
                .map_err(error_text)?;
            Some(center.as_angles())
        } else {
            None
        };
        let mut requested = rows
            .iter()
            .map(|row| (row.antenna, row.time))
            .collect::<Vec<_>>();
        // The CASA pointing calculator sorts before interpolation. The
        // filter uses those positions with the group's original times.
        requested.sort_by(|left, right| left.1.total_cmp(&right.1));
        let directions = ms
            .pointing()
            .and_then(|p| p.scan_directions(&requested, &engine))
            .map_err(error_text)?;
        let mut accepted = Vec::<[f64; 2]>::new();
        for ((row, &(_, pointing_time)), direction) in
            rows.into_iter().zip(&requested).zip(directions)
        {
            let position = direction.as_angles();
            let offset = if let Some(center) = fixed {
                sin_offset(position, center)
            } else if let Some((longitude, latitude)) =
                target.direction(&engine, pointing_time, antenna, field)?
            {
                [
                    (position.0 - longitude)
                        .sin()
                        .atan2((position.0 - longitude).cos()),
                    position.1 - latitude,
                ]
            } else {
                unreachable!("fixed fields have a reference pointing")
            };
            if offset[0].hypot(offset[1]) <= radius {
                let start = row.time - row.interval * 0.5;
                let end = row.time + row.interval * 0.5;
                accepted.push([start, end]);
            }
        }
        if !accepted.is_empty() {
            accepted.sort_by(|left, right| left[0].total_cmp(&right[0]));
            let mut combined = Vec::<[f64; 2]>::new();
            for [start, end] in accepted {
                if let Some(last) = combined.last_mut()
                    && start <= last[1]
                {
                    last[1] = last[1].max(end);
                } else {
                    combined.push([start, end]);
                }
            }
            ranges.insert(key, combined);
        }
    }
    if ranges.is_empty() {
        return Err("No integration matches the double-circle central region.".into());
    }
    Ok(samples
        .iter()
        .filter(|row| {
            ranges.get(&key(row)).is_some_and(|ranges| {
                let index = ranges.partition_point(|range| range[1] < row.time);
                ranges.get(index).is_some_and(|range| range[0] <= row.time)
            })
        })
        .collect())
}

fn key(sample: &Integration) -> FeedKey {
    (sample.field, sample.antenna, sample.feed)
}

fn sin_offset((longitude, latitude): (f64, f64), (lon0, lat0): (f64, f64)) -> [f64; 2] {
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let (sin_lat0, cos_lat0) = lat0.sin_cos();
    let (sin_delta, cos_delta) = (longitude - lon0).sin_cos();
    // OrthographicProjector converts x pixels with dy, after using
    // dx = dy / cos(latitude_0) for the world-to-pixel projection.
    [
        cos_lat * sin_delta * cos_lat0,
        sin_lat * cos_lat0 - cos_lat * sin_lat0 * cos_delta,
    ]
}
