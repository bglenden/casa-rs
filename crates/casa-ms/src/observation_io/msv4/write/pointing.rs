// SPDX-License-Identifier: LGPL-3.0-or-later
//! A dataset's `pointing_xds`, as XRADIO's converter writes it from MSv2
//! `POINTING`: `POINTING_BEAM[time_pointing, antenna_name,
//! local_sky_dir_label]`, NaN where an antenna has no sample at a time. Each
//! sample's direction is its polynomial evaluated at its own time: XRADIO
//! keeps one term (`n_polynomial` of one) and defines no origin for more, so
//! the converter reports further terms as a loss. MSv4 states no interval
//! per sample.

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

/// Pointing times per chunk of `POINTING_BEAM`, and per write.
const CHUNK_TIMES: usize = 4096;

/// The series' samples of `antennas` (each with its catalogue id), with
/// times in `scale` from `origin`, as `path`; nothing when no antenna has a
/// sample. Of several samples of one antenna at one time the first is kept
/// (the converter reports the others as a loss). Written a chunk of times at
/// a time, so the values held are one chunk's.
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
    create_group(store, path, json!({"type": "pointing"}))?;
    let view = EpochView::new(scale, origin, ArrayView1::from(&times[..]));
    let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
    let unix_times: Vec<f64> = (0..times.len())
        .map(|time| view.seconds_since(time, unix))
        .collect();
    // XRADIO's schema types the axis by its name.
    let mut time = time_attributes(scale)?;
    time.insert("type".into(), "time_pointing".into());
    coordinate(
        store,
        &format!("{path}/time_pointing"),
        json!("float64"),
        json!("NaN"),
        "time_pointing",
        &unix_times,
        serde_json::Value::Object(time),
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
    let shape = [times.len() as u64, names.len() as u64, 2];
    let chunks = [times.len().clamp(1, CHUNK_TIMES) as u64, shape[1], 2];
    let array = create_array(
        store,
        &format!("{path}/POINTING_BEAM"),
        ArraySpec {
            shape: &shape,
            chunks: &chunks,
            data_type: json!("float64"),
            codecs: codecs(Msv4Codec::Zstd, 8),
            fill: json!("NaN"),
            dimensions: &["time_pointing", "antenna_name", "local_sky_dir_label"],
            attributes: json!({"type": "sky_coord", "units": "rad", "frame": frame}),
        },
    )?;
    // Each antenna's next sample, as the chunks advance through the times.
    let mut next = vec![0_usize; columns.len()];
    let mut values = Vec::with_capacity(CHUNK_TIMES * columns.len() * 2);
    for (block, block_times) in times.chunks(CHUNK_TIMES).enumerate() {
        values.clear();
        values.resize(block_times.len() * columns.len() * 2, f64::NAN);
        for (column, (_, samples)) in columns.iter().enumerate() {
            let own = samples.times_s();
            for (row, time) in block_times.iter().enumerate() {
                if next[column] < own.len() && own[next[column]].to_bits() == time.to_bits() {
                    let start = (row * columns.len() + column) * 2;
                    values[start..start + 2]
                        .copy_from_slice(&samples.direction_at(next[column], *time));
                    // Later samples at the same time are not held.
                    while next[column] < own.len() && own[next[column]].to_bits() == time.to_bits()
                    {
                        next[column] += 1;
                    }
                }
            }
        }
        let first = (block * CHUNK_TIMES) as u64;
        let subset = ArraySubset::new_with_ranges(&[
            first..first + block_times.len() as u64,
            0..shape[1],
            0..2,
        ]);
        array
            .store_array_subset(&subset, &values)
            .map_err(storage)?;
    }
    Ok(())
}
