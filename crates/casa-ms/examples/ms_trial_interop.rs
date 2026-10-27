// SPDX-License-Identifier: LGPL-3.0-or-later
//! The Rust side of the MSv2/MSv4 trial's interoperability check
//! (`docs/measurement-set/trial/README.md`); `tools/ms-trial/check_interop.py`
//! is the XRADIO side.
//!
//! ```text
//! cargo run -p casa-ms --features msv4 --example ms_trial_interop -- write MS OUT.ps.zarr
//! cargo run -p casa-ms --features msv4 --example ms_trial_interop -- dump STORE.ps.zarr OUT_DIR
//! ```
//!
//! `write` reads the MSv2 through the trial adapter's batches and creates a
//! new MSv4 processing set with the trial writer, applying XRADIO's
//! converter conventions where the formats name things differently.
//! `dump` reads an MSv4 store through the trial adapter and writes every
//! array the batch delivers as raw little-endian files with a JSON
//! manifest, for the oracle to compare with XRADIO's own load.

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::{
    Msv4Codec, Msv4Observation, Msv4ProcessingSetWriter, Msv4StorageOptions, OpenOptions,
};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, Observation, PartitionId, ReadEvent,
    Roles, TimeSelection, WeightRequest,
};
use serde_json::json;

type Error = Box<dyn std::error::Error>;

fn request(batch_bytes: usize) -> AccessRequest {
    AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..8,
        times: TimeSelection::All,
        weights: WeightRequest::Stored,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes,
    }
}

/// `request` budgeted for `times` whole times per batch.
fn per_times(observation: &dyn Observation, times: usize) -> Result<AccessRequest, Error> {
    let estimate = observation.estimate(&request(usize::MAX))?;
    let partition = &estimate.partitions[0];
    Ok(request(partition.held_bytes_per_time() * times))
}

/// Convert the MSv2 at `ms` through batches into a new MSv4 at `out`.
///
/// The declaration is the source's own catalogue with XRADIO's converter
/// naming applied as an explicit policy (antennas `NAME_STATION`, fields
/// and spectral windows `NAME_ID`), and the grid of one traversal. The
/// UVW frame is the catalogue's effective one, which the MSv2 adapter
/// resolved at opening: no relabelling happens here.
fn write(ms: &Path, out: &Path) -> Result<(), Error> {
    let observation = casa_ms::observation_io::open(ms, &OpenOptions::default())?;
    let mut catalogue = observation.catalogue().as_ref().clone();
    for antenna in &mut catalogue.antennas {
        antenna.name = format!("{}_{}", antenna.name, antenna.station);
    }
    for (index, field) in catalogue.fields.iter_mut().enumerate() {
        field.name = format!("{}_{index}", field.name);
    }
    for (index, partition) in catalogue.partitions.iter_mut().enumerate() {
        partition.spectral_window = format!("{}_{index}", partition.spectral_window);
    }
    let grid = observation.grid(PartitionId(0), "base")?;
    let options = Msv4StorageOptions {
        dataset_name: "ms_trial_0".to_string(),
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-24T00:00:00.000000+00:00".to_string(),
        chunks: [5, 10, 4, 4],
        codec: Msv4Codec::BloscLz4,
    };
    let set = Msv4ProcessingSetWriter::create(out)?;
    let mut writer = set.dataset(&catalogue, PartitionId(0), &["base"], &grid, &options)?;
    // Four times per batch, so batches straddle the five-time chunks and
    // the writer holds their partial chunks.
    let mut traversal = observation.bind_read(&per_times(&*observation, 4)?)?;
    let mut buffers = BatchBuffers::default();
    while let Some(event) = traversal.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        writer.write_batch(&batch)?;
    }
    let counters = writer.finish()?;
    let published = set.finish()?;
    println!(
        "{}",
        json!({
            "published": published.file_name().map(|name| name.to_string_lossy().into_owned()),
            "region_writes": counters.region_writes,
            "elements": counters.elements,
            "packed_elements": counters.packed_elements,
            "held_elements": counters.held_elements,
            "partial_chunks": counters.partial_chunks,
        })
    );
    Ok(())
}

fn raw<T: bytemuck_free::Bytes>(out: &Path, name: &str, values: &[T]) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(values.len() * T::SIZE);
    for value in values {
        value.extend(&mut bytes);
    }
    fs::write(out.join(format!("{name}.bin")), bytes)?;
    Ok(())
}

/// Little-endian encodings, written out so the dump needs no extra crate.
mod bytemuck_free {
    pub trait Bytes {
        const SIZE: usize;
        fn extend(&self, bytes: &mut Vec<u8>);
    }
    impl Bytes for f64 {
        const SIZE: usize = 8;
        fn extend(&self, bytes: &mut Vec<u8>) {
            bytes.extend_from_slice(&self.to_le_bytes());
        }
    }
    impl Bytes for f32 {
        const SIZE: usize = 4;
        fn extend(&self, bytes: &mut Vec<u8>) {
            bytes.extend_from_slice(&self.to_le_bytes());
        }
    }
    impl Bytes for bool {
        const SIZE: usize = 1;
        fn extend(&self, bytes: &mut Vec<u8>) {
            bytes.push(u8::from(*self));
        }
    }
    impl Bytes for num_complex::Complex32 {
        const SIZE: usize = 8;
        fn extend(&self, bytes: &mut Vec<u8>) {
            bytes.extend_from_slice(&self.re.to_le_bytes());
            bytes.extend_from_slice(&self.im.to_le_bytes());
        }
    }
}

fn dump(store: &Path, out: &Path) -> Result<(), Error> {
    fs::create_dir_all(out)?;
    let observation = Msv4Observation::open(store)?;
    let catalogue = observation.catalogue();
    let partition = &catalogue.partitions[0];
    let mut traversal = observation.bind_read(&request(usize::MAX))?;
    let estimate = format!("{:?}", traversal.estimate);
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers)? else {
        return Err("no batch".into());
    };
    let (times, baselines) = (batch.times(), batch.samples_per_time());
    let (channels, correlations) = (batch.channel_count(), batch.correlations.len());
    let group = batch.group;
    let values = group.values.ok_or("values")?;
    let weights = group.weights.ok_or("weights")?;
    raw(
        out,
        "VISIBILITY",
        values.as_standard_layout().as_slice().ok_or("layout")?,
    )?;
    raw(
        out,
        "FLAG",
        group
            .flags
            .ok_or("flags")?
            .as_standard_layout()
            .as_slice()
            .ok_or("layout")?,
    )?;
    raw(
        out,
        "WEIGHT",
        weights.as_standard_layout().as_slice().ok_or("layout")?,
    )?;
    raw(
        out,
        "UVW",
        group
            .uvw
            .ok_or("uvw")?
            .metres()
            .as_standard_layout()
            .as_slice()
            .ok_or("layout")?,
    )?;
    raw(
        out,
        "time",
        batch.time.offsets_s().as_slice().ok_or("layout")?,
    )?;
    let centroid = batch.timing.centroid.ok_or("centroid")?;
    raw(
        out,
        "TIME_CENTROID",
        centroid.offsets_s().as_slice().ok_or("layout")?,
    )?;
    let exposure: Vec<f64> = (0..batch.samples())
        .map(|sample| batch.exposure_s(sample).ok_or("exposure"))
        .collect::<Result<_, _>>()?;
    raw(out, "EFFECTIVE_INTEGRATION_TIME", &exposure)?;
    // The represented instants, as the epoch views give them: two-part MJD
    // in the delivered scale, for the oracle to compare with astropy's
    // reading of XRADIO's attributes.
    let instants = |view: casa_types::measures::EpochView<'_>| {
        let epochs: Vec<_> = (0..view.len())
            .map(|index| view.get(index).value())
            .collect();
        (
            epochs.iter().map(|epoch| epoch.day()).collect::<Vec<_>>(),
            epochs.iter().map(|epoch| epoch.frac()).collect::<Vec<_>>(),
        )
    };
    let (time_day, time_frac) = instants(batch.time);
    let (centroid_day, centroid_frac) = instants(centroid);
    raw(
        out,
        "frequency",
        batch.frequency.hz().as_slice().ok_or("layout")?,
    )?;
    let name =
        |antenna: casa_observation::AntennaId| catalogue.antennas[antenna.0 as usize].name.clone();
    let observation_info = catalogue.observation(partition.observation);
    let manifest = json!({
        "shape": [times, baselines, channels, correlations],
        "arrays": {
            "VISIBILITY": {"dtype": "<c8", "shape": [times, baselines, channels, correlations]},
            "FLAG": {"dtype": "|b1", "shape": [times, baselines, channels, correlations]},
            "WEIGHT": {"dtype": "<f4", "shape": [times, baselines, channels, correlations]},
            "UVW": {"dtype": "<f8", "shape": [times, baselines, 3]},
            "time": {"dtype": "<f8", "shape": [times]},
            "TIME_CENTROID": {"dtype": "<f8", "shape": [times, baselines]},
            "EFFECTIVE_INTEGRATION_TIME": {"dtype": "<f8", "shape": [times, baselines]},
            "frequency": {"dtype": "<f8", "shape": [channels]},
        },
        "time_origin_mjd": batch.time.origin().day() + batch.time.origin().frac(),
        "time_scale": format!("{:?}", batch.time.reference()),
        "time_instant_mjd": {"day": time_day, "frac": time_frac},
        "centroid_instant_mjd": {"day": centroid_day, "frac": centroid_frac},
        "interval_s": batch.interval_s(0),
        "scan_name": (0..times).map(|t| batch.scan(t * baselines).map(|scan| catalogue.scans[scan.0 as usize].name.clone())).collect::<Vec<_>>(),
        "frequency_frame": format!("{:?}", batch.frequency.reference()),
        "uvw_frame": format!("{:?}", group.uvw.ok_or("uvw")?.reference()),
        "units": format!("{:?}", group.entry.units),
        "data_group": group.entry.name,
        "polarization": batch.correlations.iter().map(|c| c.label()).collect::<Vec<_>>(),
        "baseline_antenna1_name": batch.baselines.iter().map(|b| name(b.antenna1)).collect::<Vec<_>>(),
        "baseline_antenna2_name": batch.baselines.iter().map(|b| name(b.antenna2)).collect::<Vec<_>>(),
        "field_name": batch.field.iter().map(|f| catalogue.fields[f.0 as usize].name.clone()).collect::<Vec<_>>(),
        "field_phase_centre": catalogue.fields.iter().map(|f| {
            let (longitude, latitude) = f.phase_centre.as_angles();
            json!({"name": f.name, "rad": [longitude, latitude], "frame": format!("{:?}", f.phase_centre.refer())})
        }).collect::<Vec<_>>(),
        "spectral_window": partition.spectral_window,
        "channel_width_hz": partition.channel_widths_hz,
        "intents": partition.intents,
        "observation": {"telescope": observation_info.telescope, "project": observation_info.project, "observers": observation_info.observers},
        "antennas": catalogue.antennas.iter().map(|a| json!({
            "name": a.name, "station": a.station, "position_m": a.position.values(), "mount": a.mount,
            "dish_diameter_m": a.dish_diameter.value(), "receptors": a.receptors,
        })).collect::<Vec<_>>(),
        "estimate": estimate,
        "counters": format!("{:?}", traversal.counters()),
    });
    fs::write(
        out.join("manifest.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;
    println!("{}", out.display());
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [verb, input, output] if verb == "write" => write(Path::new(input), &PathBuf::from(output)),
        [verb, input, output] if verb == "dump" => dump(Path::new(input), &PathBuf::from(output)),
        _ => Err("usage: ms_trial_interop write MS OUT.ps.zarr | dump STORE OUT_DIR".into()),
    }
}
