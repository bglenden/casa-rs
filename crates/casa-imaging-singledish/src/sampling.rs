// SPDX-License-Identifier: LGPL-3.0-or-later
//! The raster sampling of a total-power map, as CASA estimates it from the
//! pointings (`SingleDishBeamUtil::getPointingSamplingRaster`, behind
//! `im.pointingsampling(pattern='raster')`), and the sampling `tsdimaging`
//! then takes for the theoretical beam (`set_beam_size`).
//!
//! The pointings are one antenna's, or several antennas' one after
//! another, each in time order. A pointing whose time equals the one
//! before it (to 10⁻¹⁴ relative) is dropped. Then:
//! - along the scan, the interval is the median distance between
//!   consecutive pointings, the longitude difference scaled by the cosine
//!   of the map's central latitude (floored at 10⁻⁸ rad);
//! - the scan's position angle is the arctangent of the median slope;
//! - the scan rows end where the time step exceeds five times the median
//!   positive step, or goes back (a new antenna), and at the end;
//! - across the scan, the interval is the median distance between
//!   consecutive rows' median positions, projected on the direction
//!   perpendicular to the scan.
//!
//! The medians are casacore's: an even count of at most 100 takes the
//! mean of the middle two, and a larger even count the lower one.
//!
//! One departure (`docs/CASA (C++) bugs.md`): CASA projects the rows'
//! longitude differences without the cosine of the latitude, so a map
//! scanned in latitude has its row spacing overestimated by `1/cos δ`;
//! casa-rs scales them as it scales the along-scan steps.

/// A pointing direction at a time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pointing {
    /// The time, seconds.
    pub time_s: f64,
    /// Longitude and latitude in the image's frame, rad.
    pub direction_rad: [f64; 2],
}

/// The sampling CASA estimates for a raster scan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RasterSampling {
    /// The interval along the scan and between the scan's rows, rad; 0
    /// where there is none to measure (a single pointing, or no row gap).
    pub interval_rad: [f64; 2],
    /// The scan's position angle, rad.
    pub position_angle_rad: f64,
}

/// `getPointingSamplingRaster` over `pointings`, each antenna's in time
/// order; `None` for no pointings.
#[must_use]
pub fn raster_sampling(pointings: &[Pointing]) -> Option<RasterSampling> {
    let first = pointings.first()?;
    let mut longitudes: Vec<f64> = pointings.iter().map(|p| p.direction_rad[0]).collect();
    unwrap_longitudes(&mut longitudes);
    // Unique time stamps: a pointing differing from the previous one.
    let mut unique = vec![0];
    let mut current = first.time_s;
    for (index, pointing) in pointings.iter().enumerate().skip(1) {
        if (pointing.time_s - current).abs() > 1.0e-14 * current {
            unique.push(index);
        }
        current = pointing.time_s;
    }
    if unique.len() == 1 {
        return Some(RasterSampling {
            interval_rad: [0.0; 2],
            position_angle_rad: 0.0,
        });
    }
    let times: Vec<f64> = unique.iter().map(|&i| pointings[i].time_s).collect();
    let longitude: Vec<f64> = unique.iter().map(|&i| longitudes[i]).collect();
    let latitude: Vec<f64> = unique
        .iter()
        .map(|&i| pointings[i].direction_rad[1])
        .collect();
    let (minimum, maximum) = latitude
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &v| {
            (lo.min(v), hi.max(v))
        });
    let scale = (0.5 * (minimum + maximum)).cos();
    let steps: Vec<[f64; 2]> = (0..longitude.len() - 1)
        .map(|i| {
            let along = (longitude[i + 1] - longitude[i]) * scale;
            [
                if along.abs() > 1.0e-8 { along } else { 1.0e-8 },
                latitude[i + 1] - latitude[i],
            ]
        })
        .collect();
    let along = median(steps.iter().map(|[x, y]| x * x + y * y).collect()).sqrt();
    let slope = median(steps.iter().map(|[x, y]| y / x).collect()).atan();
    let position_angle_rad = if slope.is_finite() {
        slope
    } else {
        0.5 * std::f64::consts::PI
    };
    let across = row_spacing(&times, (&longitude, &latitude), scale, position_angle_rad);
    Some(RasterSampling {
        interval_rad: [along, across],
        position_angle_rad,
    })
}

/// The interval between the scan's rows, 0 when no row gap is found.
fn row_spacing(
    times: &[f64],
    (longitude, latitude): (&[f64], &[f64]),
    scale: f64,
    position_angle_rad: f64,
) -> f64 {
    let steps: Vec<f64> = times.windows(2).map(|t| t[1] - t[0]).collect();
    let positive: Vec<f64> = steps.iter().copied().filter(|&step| step > 0.0).collect();
    if positive.is_empty() {
        return 0.0;
    }
    let threshold = 5.0 * median(positive);
    let mut ends = Vec::new();
    let mut antenna_ends = 0;
    for (index, &step) in steps.iter().enumerate() {
        if step > threshold {
            ends.push(index);
        } else if step < 0.0 || index == steps.len() - 1 {
            ends.push(index);
            antenna_ends += 1;
        }
    }
    if ends.len() == antenna_ends {
        return 0.0;
    }
    let normal = [
        (position_angle_rad + 0.5 * std::f64::consts::PI).cos(),
        (position_angle_rad + 0.5 * std::f64::consts::PI).sin(),
    ];
    let mut start = 0;
    let rows: Vec<[f64; 2]> = ends
        .iter()
        .map(|&end| {
            let row = [
                median(longitude[start..=end].to_vec()),
                median(latitude[start..=end].to_vec()),
            ];
            start = end + 1;
            row
        })
        .collect();
    median(
        rows.windows(2)
            .map(|pair| {
                let longitude = (pair[0][0] - pair[1][0]) * scale;
                let latitude = pair[0][1] - pair[1][1];
                (longitude * normal[0] + latitude * normal[1]).abs()
            })
            .collect(),
    )
}

/// `getMapPointings`: longitudes split by the periodic boundary (their
/// sample standard deviation above 2π/3) are made continuous, across ±π
/// when their mean is within π/2 of 0, or across 0 when it is within π/2
/// of π.
fn unwrap_longitudes(longitudes: &mut [f64]) {
    if longitudes.len() < 2 {
        return;
    }
    let count = longitudes.len() as f64;
    let mean = longitudes.iter().sum::<f64>() / count;
    let variance = longitudes.iter().map(|l| (l - mean).powi(2)).sum::<f64>() / (count - 1.0);
    if variance.sqrt() <= 2.0 / 3.0 * std::f64::consts::PI {
        return;
    }
    let pi = std::f64::consts::PI;
    if mean.abs() < 0.5 * pi {
        longitudes
            .iter_mut()
            .filter(|l| **l < 0.0)
            .for_each(|l| *l += 2.0 * pi);
    } else if (mean - pi).abs() < 0.5 * pi {
        longitudes
            .iter_mut()
            .filter(|l| **l < pi)
            .for_each(|l| *l += 2.0 * pi);
    }
}

/// casacore's `median`: the middle value, and for an even count the mean
/// of the middle two when there are at most 100, else the lower one. An
/// empty list has median 0.
pub(crate) fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let count = values.len();
    let middle = (count - 1) / 2;
    let (_, &mut lower, above) = values.select_nth_unstable_by(middle, f64::total_cmp);
    if count.is_multiple_of(2) && count <= 100 {
        let upper = above.iter().copied().fold(f64::INFINITY, f64::min);
        0.5 * (lower + upper)
    } else {
        lower
    }
}

/// The sampling `tsdimaging` gives the theoretical beam (`set_beam_size`),
/// from a raster's intervals in arcsec: both when they differ by more than
/// 1%, else one; the valid one alone when the other is not (along the
/// scan below 2.2 mas, across it below 1 mas, or not finite); `None` when
/// neither is valid, and CASA sets no beam.
///
/// One departure (`docs/CASA (C++) bugs.md`): CASA sets no beam when only
/// the along-scan interval is invalid, though its log says it uses the
/// other; casa-rs uses the other.
#[must_use]
pub fn beam_sampling(interval_arcsec: [f64; 2]) -> Option<Vec<f64>> {
    let [along, across] = interval_arcsec;
    let along_valid = along.is_finite() && along.abs() >= 2.2e-3;
    let across_valid = across.is_finite() && across.abs() >= 1.0e-3;
    match (along_valid, across_valid) {
        (false, false) => None,
        (true, false) => Some(vec![along]),
        (false, true) => Some(vec![across]),
        (true, true) if (along - across).abs() <= 0.01 * along.abs() => Some(vec![along]),
        (true, true) => Some(vec![along, across]),
    }
}

#[cfg(test)]
mod tests;
