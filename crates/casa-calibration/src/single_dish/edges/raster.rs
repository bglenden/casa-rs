// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select the head and tail of each time-separated raster strip.

use casa_ms::MeasurementSet;

use super::{Integration, error_text};

pub(super) fn edge_windows(
    ms: &MeasurementSet,
    rows: &[Integration],
    windows: &[usize],
    channels: &[i32],
    fraction: f64,
    noff: i64,
) -> Result<Vec<[f64; 2]>, String> {
    let states = ms.state().map_err(error_text)?;
    let on_source = (0..states.row_count())
        .map(|row| {
            states
                .string(row, "OBS_MODE")
                .map(|mode| mode.starts_with("OBSERVE_TARGET#ON_SOURCE"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let mut nominal = vec![None; channels.len()];
    for (description, &window) in windows.iter().enumerate() {
        let slot = nominal
            .get_mut(window)
            .ok_or("Invalid spectral window ID.")?;
        *slot = Some(description as i32);
    }
    let reference_description = nominal
        .into_iter()
        .zip(channels)
        .filter(|&(_, &count)| count != 4)
        .filter_map(|(description, _)| description)
        .find(|&description| {
            rows.iter().any(|row| {
                row.antenna == 0
                    && row.other_antenna == 0
                    && row.description == description
                    && on_source.get(row.state as usize) == Some(&true)
            })
        })
        .ok_or("No on-source raster reference window for antenna 0.")?;
    let mut scan = rows
        .iter()
        .filter(|row| {
            row.antenna == 0
                && row.other_antenna == 0
                && row.feed == 0
                && row.other_feed == 0
                && row.description == reference_description
        })
        .collect::<Vec<_>>();
    if scan.len() < 2 {
        return Err("Not enough integrations in raster reference window.".into());
    }
    scan.sort_by(|left, right| left.time.total_cmp(&right.time));
    let differences = scan.windows(2).map(|pair| pair[1].time - pair[0].time);
    let mut sorted = differences.clone().collect::<Vec<_>>();
    sorted.sort_unstable_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    let lower = sorted[(sorted.len() - 1) / 2];
    // casacore averages the middle pair only on the small-array path.
    let median = if sorted.len() <= 100 {
        (lower + sorted[middle]) / 2.
    } else {
        lower
    };
    let half_interval = scan
        .iter()
        .map(|row| row.interval)
        .reduce(f64::min)
        .unwrap()
        / 2.;
    let mut first = 0;
    let mut result = Vec::new();
    for last in differences
        .enumerate()
        .filter_map(|(index, difference)| (difference > 5. * median).then_some(index + 1))
        .chain(std::iter::once(scan.len()))
    {
        let count = edge_count(last - first, fraction, noff)?;
        result.extend([
            [
                scan[first].time - half_interval,
                scan[first + count - 1].time + half_interval,
            ],
            [
                scan[last - count].time - half_interval,
                scan[last - 1].time + half_interval,
            ],
        ]);
        first = last;
    }
    Ok(result)
}

fn edge_count(integrations: usize, fraction: f64, noff: i64) -> Result<usize, String> {
    let count = if noff > 0 {
        usize::try_from(noff).unwrap_or(usize::MAX)
    } else if fraction > 0. {
        ((integrations as f32 * fraction as f32) as usize).max(1)
    } else {
        (((integrations as f64 + 1.).sqrt() - 1.) as usize).max(1)
    };
    if count.saturating_mul(2) >= integrations {
        return Err(format!(
            "Too many edge points ({count} at each end of {integrations} points)."
        ));
    }
    Ok(count)
}
