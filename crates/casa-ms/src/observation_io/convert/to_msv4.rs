// SPDX-License-Identifier: LGPL-3.0-or-later
//! Either store converted to a new MSv4 processing set: one dataset per
//! partition, named and laid out as XRADIO's converter names and lays out
//! an MSv2.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use casa_observation::{
    Catalogue, FieldEphemeris, Observation, ObservationError, PartitionId, Roles,
};

use super::pointing_plan::{self, PointingPlan};
use super::{ConversionOptions, ConversionReport, Loss, lockstep, losses};
use crate::observation_io::msv4::{STATION_NAMED, check_fields, kept_samples, unix_times};
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
/// (others are [`Loss::UnusedAntennas`]).
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
/// rows of different feeds, are two datasets here, and rows with
/// `STATE_ID` -1 have no intents here where XRADIO gives them the last
/// `STATE` row's; the datasets numbered after such a difference can hold
/// other data than XRADIO's of the same name. A dataset whose fields
/// follow an ephemeris holds it as XRADIO's `field_and_source_ephemeris`,
/// the samples on `time_ephemeris` from the last before its first
/// time to the first two minutes past its last (the samples no dataset
/// holds are a reported loss), with the phase centre at each time as XRADIO
/// states it. Partitions are
/// converted [`ConversionOptions::workers`] at a time; nothing appears at
/// `destination` until every dataset is complete.
///
/// # Errors
///
/// All before anything is written: [`ObservationError::InvalidRequest`]
/// naming the first loss not accepted, or an ephemeris that does not cover
/// a dataset's times; [`ObservationError::Unsupported`] for fields whose
/// direction is a polynomial in time, a dataset whose fields follow
/// different ephemerides (or only some one), or radial velocities that are
/// not geocentric. Otherwise, any error opening or reading the source or
/// writing the destination, which then is not created.
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
    let mut found = losses::inventory(source, catalogue, &groups, losses::Destination::Msv4)?;
    let (pointing, lost) = pointing_plan::plan(
        source,
        observation.as_ref(),
        losses::Destination::Msv4,
        options.batch_bytes,
    )?;
    found.extend(lost);
    found.extend(losses::unused_antennas(catalogue));
    found.extend(preflight_fields(observation.as_ref(), &groups[0])?);
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
                        &Destination {
                            set: &set,
                            named: &named,
                            pointing: &pointing,
                        },
                        observation.as_ref(),
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

/// Refuse, before anything is written, what a dataset's field dataset
/// cannot hold ([`check_fields`]), and report the samples of each ephemeris
/// the partitions' fields follow that no dataset holds, one loss an
/// ephemeris: a dataset holds those from the last before its first time to
/// the first that reaches past its last time's TDB ([`kept_samples`]),
/// much as XRADIO's converter keeps those either side of a dataset's times.
/// Only the samples the source states count: an extrapolated one is not
/// written, and reading the destination extrapolates it again.
fn preflight_fields(
    observation: &dyn Observation,
    group: &str,
) -> Result<Vec<Loss>, ObservationError> {
    let catalogue = observation.catalogue();
    let mut followed: Vec<(&Arc<FieldEphemeris>, Vec<bool>)> = Vec::new();
    for id in catalogue.partition_ids() {
        let grid = observation.grid(id, group)?;
        let times = unix_times(&grid);
        let entry = catalogue.partition(id);
        if let Some((_, data_group)) = entry.data_group(group) {
            check_fields(catalogue, &grid, &times, data_group.uvw_frame.effective)?;
        }
        for field in &entry.fields {
            let Some(ephemeris) = catalogue.fields[field.0 as usize].ephemeris.as_ref() else {
                continue;
            };
            let index = followed
                .iter()
                .position(|(known, _)| Arc::ptr_eq(known, ephemeris))
                .unwrap_or_else(|| {
                    let stated = ephemeris.times_mjd().len() - ephemeris.extrapolated();
                    followed.push((ephemeris, vec![false; stated]));
                    followed.len() - 1
                });
            for held in &mut followed[index].1[kept_samples(ephemeris, &times)] {
                *held = true;
            }
        }
    }
    Ok(followed
        .into_iter()
        .filter_map(|(ephemeris, held)| {
            let lost = held.iter().filter(|held| !**held).count();
            (lost > 0).then(|| {
                Loss::Table(format!(
                    "the ephemeris of {}: samples no dataset keeps ({lost}; a dataset keeps \
                     those from the last before its first time to the first 120 s or more past \
                     its last)",
                    ephemeris.name()
                ))
            })
        })
        .collect())
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
/// the file name without `.ms`, then the partition's number in the order
/// of `first_rows`, the partitions' first MAIN rows, padded with zeros to
/// the width of the largest. The path is made absolute, and resolved when
/// that leaves no file name, so `.`, `..` and a trailing `/` name the
/// MeasurementSet they stand for, where XRADIO's converter names `.`'s
/// datasets `_0`, `..`'s `.._0` and `/`'s `_0` (here `dataset_0`, `/`
/// having no name).
fn dataset_names(source: &Path, first_rows: &[Option<u32>]) -> Vec<String> {
    let mut path = std::path::absolute(source).unwrap_or_else(|_| source.to_path_buf());
    if path.file_name().is_none() {
        path = std::fs::canonicalize(source).unwrap_or(path);
    }
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

/// What every partition is written into: the processing set, the
/// catalogue its datasets are declared with, and the pointing carried.
struct Destination<'a> {
    set: &'a Msv4ProcessingSetWriter,
    named: &'a Catalogue,
    pointing: &'a PointingPlan,
}

/// Convert partition `id` into dataset `name` of the destination: its
/// pointing, then one traversal per selected group in lockstep, each batch
/// of the same whole times.
fn partition(
    destination: &Destination<'_>,
    observation: &dyn Observation,
    id: PartitionId,
    groups: &[String],
    name: &str,
    options: &ConversionOptions,
) -> Result<Converted, ObservationError> {
    let Destination {
        set,
        named,
        pointing,
    } = *destination;
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
    // The pointing of the antennas the partition's baselines name, as
    // planned; dropped before the batches are read.
    if let Some(read) = pointing.read_for(id) {
        writer.write_pointing(named, &observation.pointing(read)?)?;
    }
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
        for path in [".", ".."] {
            let named = dataset_names(Path::new(path), &[Some(0)]);
            assert!(
                !named[0].starts_with("dataset") && !named[0].starts_with('_'),
                "{path}: {named:?}"
            );
        }
    }
}
