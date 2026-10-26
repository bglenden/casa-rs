// SPDX-License-Identifier: LGPL-3.0-or-later
//! Either store converted to a new MSv4 processing set: one dataset per
//! partition, named and laid out as XRADIO's converter names and lays out
//! an MSv2.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use casa_observation::{Catalogue, Observation, ObservationError, PartitionId, Roles};

use super::{ConversionOptions, ConversionReport, lockstep, losses};
use crate::observation_io::{
    Msv4ProcessingSetWriter, Msv4StorageOptions, Msv4WriteCounters, OpenOptions, open,
};

/// What converting one partition did.
struct Converted {
    batches: usize,
    samples: usize,
    counters: Msv4WriteCounters,
}

/// Convert the store at `source` (MSv2 or MSv4) into a new MSv4
/// processing set at `destination`.
///
/// Every partition becomes one dataset, `<source stem>_<partition>`, with
/// the selected data groups ([`ConversionOptions::data_groups`]); an MSv2's
/// antennas, fields and spectral windows are renamed as XRADIO's converter
/// renames them (`NAME_STATION`, `NAME_ID`, `NAME_ID`), so the two
/// converters' stores agree. Partitions are converted
/// [`ConversionOptions::workers`] at a time; nothing appears at
/// `destination` until every dataset is complete.
///
/// # Errors
///
/// [`ObservationError::Unsupported`] naming the first loss not accepted,
/// before anything is written; any error opening or reading the source or
/// writing the destination, which then is not created.
pub fn to_msv4(
    source: &Path,
    destination: &Path,
    options: &ConversionOptions,
) -> Result<ConversionReport, ObservationError> {
    let observation = open(source, &OpenOptions::default())?;
    let catalogue = observation.catalogue();
    let groups = losses::selected_groups(catalogue, options.data_groups.as_deref())?;
    let source_is_msv2 = !source.join("zarr.json").is_file();
    let found = losses::inventory(source, catalogue, &groups)?;
    losses::refuse_unaccepted(&found, &options.accept)?;
    let named = if source_is_msv2 {
        xradio_names(catalogue)
    } else {
        catalogue.as_ref().clone()
    };
    let stem = source.file_stem().map_or_else(
        || "dataset".to_string(),
        |stem| stem.to_string_lossy().into_owned(),
    );
    let set = Msv4ProcessingSetWriter::create(destination)?;
    let ids = catalogue.partition_ids();
    let next = AtomicUsize::new(0);
    let results = Mutex::new(Vec::with_capacity(ids.len()));
    std::thread::scope(|scope| {
        for _ in 0..options.workers.clamp(1, ids.len().max(1)) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(id) = ids.get(index) else {
                        break;
                    };
                    let result = partition(
                        observation.as_ref(),
                        &set,
                        &named,
                        *id,
                        &groups,
                        &format!("{stem}_{}", id.0),
                        options,
                    );
                    let failed = result.is_err();
                    results
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(result);
                    if failed {
                        // Stop handing out partitions; the others finish
                        // what they hold.
                        next.store(ids.len(), Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let mut report = ConversionReport {
        losses: found,
        ..ConversionReport::default()
    };
    for result in results
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        let converted = result?;
        report.partitions += 1;
        report.batches += converted.batches;
        report.samples += converted.samples;
        report.elements += converted.counters.elements;
        report.held_elements += converted.counters.held_elements;
    }
    set.finish()?;
    Ok(report)
}

/// `catalogue` with XRADIO's names: antennas `NAME_STATION`, fields
/// `NAME_ID`, spectral windows `NAME_ID`, as its converter writes an MSv2's.
fn xradio_names(catalogue: &Catalogue) -> Catalogue {
    let mut named = catalogue.clone();
    for antenna in &mut named.antennas {
        antenna.name = format!("{}_{}", antenna.name, antenna.station);
    }
    for (index, field) in named.fields.iter_mut().enumerate() {
        field.name = format!("{}_{index}", field.name);
    }
    for (index, partition) in named.partitions.iter_mut().enumerate() {
        partition.spectral_window = format!("{}_{index}", partition.spectral_window);
    }
    named
}

/// Convert partition `id` into dataset `name` of `set`: one traversal per
/// selected group in lockstep, each batch of the same whole times.
fn partition(
    observation: &dyn Observation,
    set: &Msv4ProcessingSetWriter,
    named: &Catalogue,
    id: PartitionId,
    groups: &[String],
    name: &str,
    options: &ConversionOptions,
) -> Result<Converted, ObservationError> {
    let grid = observation.grid(id, &groups[0])?;
    let entry = observation.catalogue().partition(id);
    let extents = [
        grid.times_s.len(),
        grid.baselines.len(),
        entry.frequencies_hz.len(),
        entry.correlations.len(),
    ];
    let chunks: [u64; 4] = std::array::from_fn(|axis| {
        let extent = extents[axis].max(1) as u64;
        match options.chunks[axis] {
            0 => extent,
            chunk => chunk.min(extent),
        }
    });
    let storage = Msv4StorageOptions {
        dataset_name: name.to_string(),
        creator: options.creator.clone(),
        creation_date: options.creation_date.clone(),
        chunks,
        codec: options.codec,
    };
    let names: Vec<&str> = groups.iter().map(String::as_str).collect();
    let mut writer = set.dataset(named, id, &names, &grid, &storage)?;
    let budget = (options.batch_bytes, Some(writer.chunk_times()));
    let read = lockstep::read(observation, id, groups, Roles::ALL, budget, |batches| {
        writer.write_groups(batches)
    })?;
    Ok(Converted {
        batches: read.batches,
        samples: read.samples,
        counters: writer.finish()?,
    })
}
