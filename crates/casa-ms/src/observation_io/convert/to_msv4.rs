// SPDX-License-Identifier: LGPL-3.0-or-later
//! Either store converted to a new MSv4 processing set: one dataset per
//! partition, named and laid out as XRADIO's converter names and lays out
//! an MSv2.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use casa_observation::{Catalogue, Observation, ObservationError, PartitionId, Roles};

use super::{ConversionOptions, ConversionReport, lockstep, losses};
use crate::observation_io::update::storage;
use crate::observation_io::{
    Msv2Observation, Msv4Observation, Msv4ProcessingSetWriter, Msv4StorageOptions,
    Msv4WriteCounters, OpenOptions,
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
/// Every partition becomes one dataset with the selected data groups
/// ([`ConversionOptions::data_groups`]) and the antennas its baselines name
/// (others are [`Loss::UnusedAntennas`](super::Loss::UnusedAntennas)).
/// An MSv4's datasets keep their names. An MSv2's are named as XRADIO's
/// converter names them: the file name without `.ms`, then the partition's
/// number in the order the partitions' first MAIN rows appear, padded with
/// zeros to the width of the largest. Its antennas (for the telescopes
/// XRADIO renames, by the first observation's telescope), fields, sources
/// and spectral windows are renamed as that converter renames them
/// (`NAME_STATION`, `NAME_FIELD_ID`, `NAME_SOURCE_ID`,
/// `NAME_SPECTRAL_WINDOW_ID`), so the two converters' stores agree; a
/// source XRADIO finds no row for in one spectral window, which it names
/// `Unknown` there, is still named here. Two partitions XRADIO would merge,
/// rows of different feeds, are two datasets here. Partitions are
/// converted [`ConversionOptions::workers`] at a time; nothing appears at
/// `destination` until every dataset is complete.
///
/// # Errors
///
/// [`ObservationError::InvalidRequest`] naming the first loss not accepted,
/// and [`ObservationError::Unsupported`] for fields that follow an
/// ephemeris or whose direction is a polynomial in time, both before
/// anything is written; any error opening or
/// reading the source or writing the destination, which then is not
/// created.
pub fn to_msv4(
    source: &Path,
    destination: &Path,
    options: &ConversionOptions,
) -> Result<ConversionReport, ObservationError> {
    let Named {
        observation,
        catalogue: named,
        datasets,
    } = open_named(source)?;
    let catalogue = observation.catalogue();
    let groups = losses::selected_groups(catalogue, options.data_groups.as_deref())?;
    let mut found = losses::inventory(source, catalogue, &groups)?;
    found.extend(losses::unused_antennas(observation.as_ref(), &groups[0])?);
    found.sort();
    losses::refuse_unaccepted(&found, &options.accept)?;
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
                        &datasets[id.0 as usize],
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

/// A source opened for conversion, with what its datasets are named.
struct Named {
    observation: Box<dyn Observation>,
    /// The catalogue the datasets are declared with.
    catalogue: Catalogue,
    /// Each partition's dataset name.
    datasets: Vec<String>,
}

/// Open `source`, with the catalogue its datasets are declared with and
/// each partition's dataset name: an MSv4's own, or an MSv2's with
/// XRADIO's names.
fn open_named(source: &Path) -> Result<Named, ObservationError> {
    let options = OpenOptions::default();
    if source.join("zarr.json").is_file() {
        let observation = Msv4Observation::open_with(source, &options)?;
        let catalogue = observation.catalogue().as_ref().clone();
        let datasets = catalogue
            .partition_ids()
            .into_iter()
            .map(|id| observation.dataset_name(id).to_string())
            .collect();
        return Ok(Named {
            observation: Box::new(observation),
            catalogue,
            datasets,
        });
    }
    let observation = Msv2Observation::open_with(source, &options)?;
    let ids = observation.catalogue().partition_ids();
    let windows: Vec<u32> = ids
        .iter()
        .map(|id| observation.spectral_window_id(*id))
        .collect();
    let first_rows: Vec<Option<u32>> = ids.iter().map(|id| observation.first_row(*id)).collect();
    let fields = observation.measurement_set().field().map_err(storage)?;
    let sources = (0..observation.catalogue().fields.len())
        .map(|row| fields.source_id(row).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    let named = xradio_names(observation.catalogue(), &windows, &sources);
    let datasets = dataset_names(source, &first_rows);
    Ok(Named {
        observation: Box::new(observation),
        catalogue: named,
        datasets,
    })
}

/// Each partition's dataset name, as XRADIO's converter names an MSv2's:
/// the file name without `.ms` (of the path made absolute, so `.` and a
/// trailing `/` still name the MeasurementSet), then the partition's
/// number in the order of `first_rows`, the partitions' first MAIN rows,
/// padded with zeros to the width of the largest.
fn dataset_names(source: &Path, first_rows: &[Option<u32>]) -> Vec<String> {
    let path = std::path::absolute(source).unwrap_or_else(|_| source.to_path_buf());
    let stem = path.file_name().map_or_else(
        || "dataset".to_string(),
        |name| name.to_string_lossy().replace(".ms", ""),
    );
    let mut order: Vec<usize> = (0..first_rows.len()).collect();
    order.sort_by_key(|index| (first_rows[*index].unwrap_or(u32::MAX), *index));
    let width = first_rows.len().saturating_sub(1).to_string().len();
    let mut names = vec![String::new(); first_rows.len()];
    for (number, index) in order.into_iter().enumerate() {
        names[index] = format!("{stem}_{number:0width$}");
    }
    names
}

/// Telescopes whose antennas XRADIO's converter names `NAME_STATION`;
/// others keep their names.
const STATION_NAMED: [&str; 4] = ["ALMA", "VLA", "NOEMA", "EVLA"];

/// `catalogue` with XRADIO's names, as its converter names an MSv2's:
/// antennas `NAME_STATION` for the telescopes it names so
/// ([`STATION_NAMED`]), fields `NAME_ID`, sources `NAME_SOURCEID` by each
/// field's `SOURCE_ID` (`sources`), and spectral windows `NAME_ID`
/// (`spw_ID` unnamed) by the `SPECTRAL_WINDOW` row of each partition
/// (`windows`).
fn xradio_names(catalogue: &Catalogue, windows: &[u32], sources: &[i32]) -> Catalogue {
    let mut named = catalogue.clone();
    let telescope = catalogue
        .observations
        .first()
        .map_or("", |observation| observation.telescope.as_str());
    if STATION_NAMED.contains(&telescope) {
        for antenna in &mut named.antennas {
            antenna.name = format!("{}_{}", antenna.name, antenna.station);
        }
    }
    for ((index, field), source) in named.fields.iter_mut().enumerate().zip(sources) {
        field.name = format!("{}_{index}", field.name);
        if let Some(name) = &mut field.source_name {
            *name = format!("{name}_{source}");
        }
    }
    for (partition, window) in named.partitions.iter_mut().zip(windows) {
        partition.spectral_window = match partition.spectral_window.as_str() {
            "" | "none" => format!("spw_{window}"),
            name => format!("{name}_{window}"),
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datasets_are_numbered_by_first_row_and_padded_to_the_largest() {
        let rows = [Some(10), Some(0), None];
        assert_eq!(
            dataset_names(Path::new("data/pilot.ms"), &rows),
            ["pilot_1", "pilot_0", "pilot_2"]
        );
        let eleven: Vec<Option<u32>> = (0..11).rev().map(Some).collect();
        let names = dataset_names(Path::new("pilot.ms/"), &eleven);
        assert_eq!(names[10], "pilot_00");
        assert_eq!(names[0], "pilot_10");
        let here = dataset_names(Path::new("."), &[Some(0)]);
        assert!(
            !here[0].starts_with("dataset") && !here[0].starts_with('_'),
            "{here:?}"
        );
    }
}
