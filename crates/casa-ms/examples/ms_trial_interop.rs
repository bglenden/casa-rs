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

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use casa_ms::observation_io::{
    Msv2Observation, Msv4Codec, Msv4DatasetSpec, Msv4Observation, Msv4Writer,
};
use casa_observation::{
    AccessRequest, BatchBuffers, Closure, FieldId, Grouping, Layout, OrderedBaseline, PartitionId,
    Precision, ReadEvent, Roles, SampleTopology, VisibilityCursor, WeightView,
};
use casa_types::measures::MjdHighPrec;
use serde_json::json;

type Error = Box<dyn std::error::Error>;

fn request(max_samples: usize) -> AccessRequest {
    AccessRequest {
        partition: PartitionId(0),
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..8,
        layout: Layout::SampleMajor,
        precision: Precision::Native,
        grouping: Grouping::None,
        closure: Closure::AtDelivery,
        max_samples,
    }
}

/// The scans and intents the trial batch does not carry, read from MAIN
/// `SCAN_NUMBER` and `STATE::OBS_MODE` directly.
fn scans(path: &Path, times: &[f64]) -> Result<(Vec<String>, Vec<String>), Error> {
    let ms = MeasurementSet::open(path)?;
    let main = ms.main_table();
    let mut per_time = vec![String::new(); times.len()];
    for row in 0..main.row_count() {
        let time = casa_ms::columns::time_columns::TimeColumn::new(main).get_mjd_seconds(row)?;
        let scan = casa_ms::columns::main_ids::scan_number(main).get(row)?;
        if let Some(index) = times.iter().position(|known| *known == time) {
            per_time[index] = scan.to_string();
        }
    }
    let state = ms.state()?;
    let intents = (0..state.row_count())
        .map(|row| state.string(row, "OBS_MODE"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok((per_time, intents.into_iter().collect()))
}

fn write(ms: &Path, out: &Path) -> Result<(), Error> {
    let observation = Msv2Observation::open(ms)?;
    let catalogue = observation.catalogue();
    let partition = &catalogue.partitions[0];
    // The axes, from one traversal of the batch coordinates.
    let mut times = Vec::<f64>::new();
    let mut baselines = Vec::<OrderedBaseline>::new();
    let mut fields = Vec::<FieldId>::new();
    let mut cursor = observation.bind_read(&request(1_000))?;
    let mut buffers = BatchBuffers::default();
    while let Some(event) = cursor.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            if times.last() != Some(&coordinate.time_offset_s) {
                times.push(coordinate.time_offset_s);
                fields.push(coordinate.field);
            }
            if !baselines.contains(&coordinate.baseline) {
                baselines.push(coordinate.baseline);
            }
        }
    }
    let (scan_per_time, scan_intents) = scans(ms, &times)?;
    // XRADIO's converter conventions: antenna NAME_STATION, field and
    // spectral window NAME_ID, Unix-second times.
    let mut antennas = catalogue.antennas.clone();
    for antenna in &mut antennas {
        antenna.name = format!("{}_{}", antenna.name, antenna.station);
    }
    let mut field_entries = catalogue.fields.clone();
    for (index, field) in field_entries.iter_mut().enumerate() {
        field.name = format!("{}_{index}", field.name);
    }
    let unix = MjdHighPrec::new(40_587.0, 0.0);
    let spec = Msv4DatasetSpec {
        name: "ms_trial_0".to_string(),
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-24T00:00:00.000000+00:00".to_string(),
        telescope: catalogue.telescope.clone(),
        observers: catalogue.observers.clone(),
        project: catalogue.project.clone(),
        antennas,
        fields: field_entries,
        baselines,
        times_unix_s: times
            .iter()
            .map(|mjd_seconds| mjd_seconds - unix.day() * 86_400.0)
            .collect(),
        time_scale: casa_types::measures::EpochRef::UTC,
        integration_time_s: 10.0,
        field_per_time: fields,
        scan_per_time,
        scan_intents,
        spectral_window: format!("{}_0", partition.spectral_window),
        frequencies_hz: partition.frequencies_hz.clone(),
        channel_width_hz: partition.channel_widths_hz[0],
        frequency_frame: partition.frequency_frame,
        correlations: partition.correlations.clone(),
        units: partition.data_groups[0].units.clone(),
        // XRADIO relabels MSv2's ITRF UVW as the phase centre's frame.
        uvw_frame: catalogue.fields[0].phase_centre.refer(),
        chunks: [5, 10, 4, 4],
        codec: Msv4Codec::BloscLz4,
    };
    let mut writer = Msv4Writer::create(out, &spec)?;
    // Four times per batch, so batches straddle the five-time chunks.
    let mut cursor = observation.bind_read(&request(40))?;
    while let Some(event) = cursor.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        writer.write_batch(&batch)?;
    }
    let counters = writer.counters();
    let published = writer.finish()?;
    println!(
        "{}",
        json!({
            "published": published.file_name().map(|name| name.to_string_lossy().into_owned()),
            "region_writes": counters.region_writes,
            "elements": counters.elements,
            "packed_elements": counters.packed_elements,
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
    let mut cursor = observation.bind_read(&request(usize::MAX))?;
    let realization = format!("{:?}", cursor.realization());
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers)? else {
        return Err("no batch".into());
    };
    let SampleTopology::TimeBaseline(topology) = batch.topology else {
        return Err("an MSv4 batch is factored".into());
    };
    let (times, baselines) = (topology.time.len(), topology.baselines.len());
    let (channels, correlations) = (batch.channel_count(), batch.correlations.len());
    let group = batch.group;
    let values = group
        .values
        .and_then(|v| v.as_complex32())
        .ok_or("values")?;
    let Some(WeightView::Spectral(weights)) = group.weights else {
        return Err("MSv4 weights are spectral".into());
    };
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
        topology.time.offsets_s().as_slice().ok_or("layout")?,
    )?;
    raw(
        out,
        "frequency",
        batch.frequency.hz().as_slice().ok_or("layout")?,
    )?;
    let name =
        |antenna: casa_observation::AntennaId| catalogue.antennas[antenna.0 as usize].name.clone();
    let manifest = json!({
        "shape": [times, baselines, channels, correlations],
        "arrays": {
            "VISIBILITY": {"dtype": "<c8", "shape": [times, baselines, channels, correlations]},
            "FLAG": {"dtype": "|b1", "shape": [times, baselines, channels, correlations]},
            "WEIGHT": {"dtype": "<f4", "shape": [times, baselines, channels, correlations]},
            "UVW": {"dtype": "<f8", "shape": [times, baselines, 3]},
            "time": {"dtype": "<f8", "shape": [times]},
            "frequency": {"dtype": "<f8", "shape": [channels]},
        },
        "time_origin_mjd": topology.time.origin().day() + topology.time.origin().frac(),
        "time_scale": format!("{:?}", topology.time.reference()),
        "frequency_frame": format!("{:?}", batch.frequency.reference()),
        "uvw_frame": format!("{:?}", group.uvw.ok_or("uvw")?.reference()),
        "units": format!("{:?}", group.units),
        "data_group": group.name,
        "polarization": batch.correlations.iter().map(|c| c.label()).collect::<Vec<_>>(),
        "baseline_antenna1_name": topology.baselines.iter().map(|b| name(b.antenna1)).collect::<Vec<_>>(),
        "baseline_antenna2_name": topology.baselines.iter().map(|b| name(b.antenna2)).collect::<Vec<_>>(),
        "field_name": topology.field.iter().map(|f| catalogue.fields[*f as usize].name.clone()).collect::<Vec<_>>(),
        "field_phase_centre": catalogue.fields.iter().map(|f| {
            let (longitude, latitude) = f.phase_centre.as_angles();
            json!({"name": f.name, "rad": [longitude, latitude], "frame": format!("{:?}", f.phase_centre.refer())})
        }).collect::<Vec<_>>(),
        "spectral_window": partition.spectral_window,
        "channel_width_hz": partition.channel_widths_hz,
        "antennas": catalogue.antennas.iter().map(|a| json!({
            "name": a.name, "station": a.station, "position_m": a.position_m, "mount": a.mount,
            "dish_diameter_m": a.dish_diameter_m, "receptors": a.receptors,
        })).collect::<Vec<_>>(),
        "realization": realization,
        "counters": format!("{:?}", cursor.counters()),
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
