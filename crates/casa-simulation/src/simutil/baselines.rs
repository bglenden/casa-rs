// SPDX-License-Identifier: LGPL-3.0-or-later
//! An array's baselines and the resolution they give: `simutil.itrf2loc`,
//! `baselineLengths` and `approxBeam`.

use crate::readantenna::AntennaConfigFile;
use crate::readantenna::geodesy;

use super::SimutilError;

/// `simutil.itrf2loc(x, y, z, cx, cy, cz)`: positions relative to `centre`,
/// as `[east, north, up]` metres in the plane tangent to the WGS84
/// ellipsoid at the centre's latitude and longitude. (CASA returns the
/// three as `lat, lon, el`.) The centre's latitude and longitude come from
/// casa-rs's `xyz2long`, which corrects CASA's (see
/// [`crate::readantenna::geodesy`]); the rotation is CASA's.
pub fn itrf_to_local(
    positions: &[[f64; 3]],
    centre: [f64; 3],
) -> Result<Vec<[f64; 3]>, SimutilError> {
    let wgs84 = geodesy::datum("WGS84").expect("WGS84 is one of casa-rs's datums");
    let (longitude, latitude, _) = geodesy::xyz_to_geodetic(centre[0], centre[1], centre[2], wgs84)
        .ok_or_else(|| SimutilError::Arithmetic(format!("no WGS84 position for {centre:?}")))?;
    let (sin_lon, cos_lon) = longitude.sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    Ok(positions
        .iter()
        .map(|position| {
            let [x, y, z] = [
                position[0] - centre[0],
                position[1] - centre[1],
                position[2] - centre[2],
            ];
            [
                (-sin_lon * x) + (cos_lon * y),
                (-sin_lat * cos_lon * x) - (sin_lat * sin_lon * y) + cos_lat * z,
                (cos_lat * cos_lon * x) + (cos_lat * sin_lon * y) + sin_lat * z,
            ]
        })
        .collect())
}

/// `simutil.baselineLengths(configfile)`: every antenna pair's distance,
/// metres, pair `(i, j)` for `i < j` in file order. CASA puts the antennas
/// in local coordinates about their mean position first, which leaves the
/// distances as they are.
pub fn baseline_lengths(config: &AntennaConfigFile) -> Result<Vec<f64>, SimutilError> {
    let count = config.x.len();
    let positions: Vec<[f64; 3]> = (0..count)
        .map(|i| [config.x[i], config.y[i], config.z[i]])
        .collect();
    let mean = |axis: usize| positions.iter().map(|p| p[axis]).sum::<f64>() / count as f64;
    let local = itrf_to_local(&positions, [mean(0), mean(1), mean(2)])?;
    let mut lengths = Vec::with_capacity(count * count.saturating_sub(1) / 2);
    for i in 0..count {
        for j in i + 1..count {
            let [x, y, z] = [
                local[i][0] - local[j][0],
                local[i][1] - local[j][1],
                local[i][2] - local[j][2],
            ];
            lengths.push((x * x + y * y + z * z).sqrt());
        }
    }
    Ok(lengths)
}

/// `simutil.approxBeam(configfile, freq)`: λ over the 90th-percentile
/// baseline, arcsec, at `frequency_ghz`. The percentile is scipy's
/// `scoreatpercentile`: the sorted lengths interpolated linearly at
/// `0.9 × (n − 1)`.
pub fn approximate_beam(
    config: &AntennaConfigFile,
    frequency_ghz: f64,
) -> Result<f64, SimutilError> {
    if !(frequency_ghz > 0.0 && frequency_ghz.is_finite()) {
        return Err(SimutilError::Frequency(frequency_ghz));
    }
    let mut lengths = baseline_lengths(config)?;
    if lengths.is_empty() {
        return Err(SimutilError::Arithmetic(
            "an array of one antenna has no baselines".to_string(),
        ));
    }
    lengths.sort_by(f64::total_cmp);
    let index = 90.0 / 100.0 * (lengths.len() - 1) as f64;
    let fraction = index % 1.0;
    let below = lengths[index as usize];
    let ninety = if fraction == 0.0 {
        below
    } else {
        below + (lengths[index as usize + 1] - below) * fraction
    };
    Ok(0.2997924 / frequency_ghz / ninety * 3600.0 * 180.0 / std::f64::consts::PI)
}
