// SPDX-License-Identifier: LGPL-3.0-or-later
//! A dataset's `pointing_xds`, as XRADIO's converter writes it from MSv2
//! `POINTING`: `POINTING_BEAM[time_pointing, antenna_name,
//! local_sky_dir_label (, n_polynomial)]`, NaN where an antenna has no
//! sample at a time. MSv4 states no interval per sample, and its polynomial
//! terms are in the time since the sample, so an MSv2 row's polynomial is
//! re-expressed about its own time.

use std::sync::Arc;

use casa_observation::{AntennaEntry, AntennaId, ObservationError, PointingSeries};
use casa_types::measures::{EpochView, MjdHighPrec};
use ndarray::ArrayView1;
use serde_json::json;
use zarrs::array::ArraySubset;
use zarrs::filesystem::FilesystemStore;

use super::{ArraySpec, codecs, coordinate, create_array, create_group, storage, strings};
use crate::observation_io::Msv4Codec;
use crate::observation_io::msv4::metadata::{UNIX_EPOCH_MJD, time_attributes};
use crate::observation_io::msv4::pointing::POINTING_FRAMES;

/// Pointing times per chunk of `POINTING_BEAM`.
const CHUNK_TIMES: u64 = 4096;

/// The series' samples of `antennas` (each with its catalogue id), with
/// times in `scale` from `origin`, as `path`; nothing when no antenna has a
/// sample.
pub(super) fn write_pointing(
    store: &Arc<FilesystemStore>,
    path: &str,
    antennas: &[(AntennaId, &AntennaEntry)],
    series: &PointingSeries,
    scale: casa_types::measures::EpochRef,
    origin: MjdHighPrec,
) -> Result<(), ObservationError> {
    let columns: Vec<_> = antennas
        .iter()
        .filter_map(|(id, entry)| series.antennas.get(id).map(|samples| (*entry, samples)))
        .filter(|(_, samples)| !samples.times_s().is_empty())
        .collect();
    if columns.is_empty() {
        return Ok(());
    }
    let frame = POINTING_FRAMES
        .iter()
        .find(|(_, frame)| *frame == series.frame)
        .map(|(name, _)| *name)
        .ok_or(ObservationError::Unsupported {
            what: "writing MSv4 pointing in a frame other than J2000, ICRS or AZELGEO",
        })?;
    let mut times: Vec<f64> = columns
        .iter()
        .flat_map(|(_, samples)| samples.times_s().iter().copied())
        .collect();
    times.sort_by(f64::total_cmp);
    times.dedup_by(|next, kept| next.to_bits() == kept.to_bits());
    let terms = series.terms;
    let width = 2 * terms;
    let mut values = vec![f64::NAN; times.len() * columns.len() * width];
    for (column, (_, samples)) in columns.iter().enumerate() {
        for (sample, time) in samples.times_s().iter().enumerate() {
            let row = times.partition_point(|known| known < time);
            let start = (row * columns.len() + column) * width;
            if !values[start].is_nan() {
                return Err(ObservationError::Unsupported {
                    what: "pointing samples of one antenna at one time",
                });
            }
            let coefficients = &samples.coefficients()[sample * width..(sample + 1) * width];
            values[start..start + width].copy_from_slice(coefficients);
            for axis in 0..2 {
                shift(
                    &mut values[start + axis * terms..start + (axis + 1) * terms],
                    time - samples.origins_s()[sample],
                );
            }
        }
    }
    create_group(store, path, json!({"type": "pointing"}))?;
    let view = EpochView::new(scale, origin, ArrayView1::from(&times[..]));
    let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
    let unix_times: Vec<f64> = (0..times.len())
        .map(|time| view.seconds_since(time, unix))
        .collect();
    coordinate(
        store,
        &format!("{path}/time_pointing"),
        json!("float64"),
        json!("NaN"),
        "time_pointing",
        &unix_times,
        serde_json::Value::Object(time_attributes(scale)?),
    )?;
    let names: Vec<String> = columns
        .iter()
        .map(|(entry, _)| entry.name.clone())
        .collect();
    strings(
        store,
        &format!("{path}/antenna_name"),
        &[names.len() as u64],
        &["antenna_name"],
        &names,
    )?;
    // XRADIO labels a pointing direction's axes az and alt in every frame.
    strings(
        store,
        &format!("{path}/local_sky_dir_label"),
        &[2],
        &["local_sky_dir_label"],
        &["az", "alt"].map(String::from),
    )?;
    let mut shape = vec![times.len() as u64, names.len() as u64, 2];
    let mut dimensions = vec!["time_pointing", "antenna_name", "local_sky_dir_label"];
    if terms > 1 {
        shape.push(terms as u64);
        dimensions.push("n_polynomial");
    }
    let mut chunks = shape.clone();
    chunks[0] = shape[0].clamp(1, CHUNK_TIMES);
    let array = create_array(
        store,
        &format!("{path}/POINTING_BEAM"),
        ArraySpec {
            shape: &shape,
            chunks: &chunks,
            data_type: json!("float64"),
            codecs: codecs(Msv4Codec::Zstd, 8),
            fill: json!("NaN"),
            dimensions: &dimensions,
            attributes: json!({"type": "sky_coord", "units": "rad", "frame": frame}),
        },
    )?;
    array
        .store_array_subset(&ArraySubset::new_with_shape(shape), &values)
        .map_err(storage)
}

/// Re-express `coefficients` (increasing powers of the time since an
/// origin) about a point `delta` later: Taylor's shift by repeated
/// synthetic division. One term is unchanged.
fn shift(coefficients: &mut [f64], delta: f64) {
    let terms = coefficients.len();
    if terms < 2 || delta == 0.0 {
        return;
    }
    for start in 0..terms - 1 {
        for term in (start..terms - 1).rev() {
            coefficients[term] += delta * coefficients[term + 1];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::shift;

    /// A polynomial shifted to a later origin takes the same values.
    #[test]
    fn a_shifted_polynomial_keeps_its_values() {
        let original = [0.5, -2.0, 3.0, 0.25];
        let delta = 1.5;
        let mut shifted = original;
        shift(&mut shifted, delta);
        let at = |coefficients: &[f64], x: f64| {
            coefficients
                .iter()
                .rev()
                .fold(0.0, |value, term| value * x + term)
        };
        for x in [-1.0, 0.0, 0.75, 2.0] {
            let expected = at(&original, x + delta);
            assert!((at(&shifted, x) - expected).abs() <= 1e-12 * expected.abs().max(1.0));
        }
    }
}
