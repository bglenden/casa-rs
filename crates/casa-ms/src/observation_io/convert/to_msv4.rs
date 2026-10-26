// SPDX-License-Identifier: LGPL-3.0-or-later
//! Either store converted to a new MSv4 processing set: one dataset per
//! partition, named and laid out as XRADIO's converter names and lays out
//! an MSv2.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use casa_observation::{
    AccessRequest, BatchBuffers, Catalogue, CorrelationRequest, Grouping, Observation,
    ObservationError, PartitionId, ReadEvent, Roles,
};

use super::{ConversionOptions, ConversionReport, losses};
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
    let mut requests: Vec<AccessRequest> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| AccessRequest {
            partitions: vec![id],
            data_group: group.clone(),
            roles: if index == 0 { Roles::ALL } else { VISIBILITIES },
            channels: 0..entry.frequencies_hz.len(),
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        })
        .collect();
    let per_time = requests
        .iter()
        .map(|request| Ok(observation.estimate(request)?.partitions[0].held_bytes_per_time()))
        .collect::<Result<Vec<_>, ObservationError>>()?;
    let times = times_per_batch(
        options.batch_bytes,
        per_time.iter().sum(),
        writer.chunk_times(),
        extents[0],
    );
    for (request, bytes) in requests.iter_mut().zip(&per_time) {
        request.batch_bytes = bytes * times;
    }
    let mut traversals = requests
        .iter()
        .map(|request| observation.bind_read(request))
        .collect::<Result<Vec<_>, _>>()?;
    let mut buffers: Vec<BatchBuffers> =
        (0..groups.len()).map(|_| BatchBuffers::default()).collect();
    let (mut batches, mut samples) = (0, 0);
    loop {
        let mut lockstep = Vec::with_capacity(groups.len());
        let mut ended = 0;
        for (traversal, buffer) in traversals.iter_mut().zip(buffers.iter_mut()) {
            match traversal.next(buffer)? {
                Some(ReadEvent::Batch(batch)) => lockstep.push(batch),
                Some(ReadEvent::GroupEnd(_)) => {
                    return Err(super::super::update::request_error(
                        "a conversion traversal delivered a group end",
                    ));
                }
                None => ended += 1,
            }
        }
        if ended == groups.len() {
            break;
        }
        if ended > 0 {
            return Err(super::super::update::request_error(
                "the data groups' traversals ended apart",
            ));
        }
        batches += 1;
        samples += lockstep[0].samples();
        writer.write_groups(&lockstep)?;
    }
    Ok(Converted {
        batches,
        samples,
        counters: writer.finish()?,
    })
}

/// A further group's roles: its visibilities, beside the first group's
/// flags, weights, UVW and time auxiliaries.
const VISIBILITIES: Roles = Roles {
    values: true,
    flags: false,
    weights: false,
    uvw: false,
    time_centroid: false,
    interval: false,
    exposure: false,
    scan: false,
};

/// Whole times per batch for `budget` bytes of `bytes_per_time` each: a
/// multiple of the chunk's `chunk` times where that fits, so that no chunk
/// is held, and never more than the dataset's `total`.
fn times_per_batch(budget: usize, bytes_per_time: usize, chunk: usize, total: usize) -> usize {
    let fit = (budget / bytes_per_time.max(1)).max(1);
    let aligned = if fit >= chunk { fit - fit % chunk } else { fit };
    aligned.min(total.max(1))
}

#[cfg(test)]
mod tests {
    use super::times_per_batch;

    #[test]
    fn batches_hold_whole_chunks_of_times_where_the_budget_allows() {
        assert_eq!(times_per_batch(1000, 10, 5, 100), 100, "the dataset fits");
        assert_eq!(times_per_batch(470, 10, 5, 100), 45, "whole chunks");
        assert_eq!(times_per_batch(30, 10, 5, 100), 3, "less than a chunk");
        assert_eq!(times_per_batch(5, 10, 5, 100), 1, "at least one time");
    }
}
