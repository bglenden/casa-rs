// SPDX-License-Identifier: LGPL-3.0-or-later
//! Describe the observation a row producer has planned. The producer keeps
//! its native column metadata and supplies the grid directly, without
//! serializing MAIN or reopening a temporary MeasurementSet.

use casa_observation::{
    Catalogue, ObservationError, ObservationId, PartitionEntry, ScanEntry, TimeBaselineGrid,
    WindowId,
};

use super::{Msv2Observation, catalogue, data_groups, invalid, partition_receptors, storage};
use crate::{
    MeasurementSet,
    observation_io::{STATION_NAMED, distinct},
};

/// A producer's partition, whose subtables already state the observing
/// session, spectral window, polarization and feeds.
pub struct Msv2CreationPartition<'a> {
    /// Row of OBSERVATION.
    pub observation: ObservationId,
    /// Row of DATA_DESCRIPTION.
    pub data_description: usize,
    /// Feed IDs of the two antennas.
    pub feeds: [u32; 2],
    /// Coordinates to be produced.
    pub grid: &'a TimeBaselineGrid,
}

/// Build a catalogue from an unsaved producer's metadata and planned
/// grids. No MAIN rows or payload are read.
///
/// # Errors
/// If the column metadata or partition references are invalid.
pub fn catalogue_for_creation(
    ms: &MeasurementSet,
    scans: Vec<ScanEntry>,
    partitions: &[Msv2CreationPartition<'_>],
) -> Result<Catalogue, ObservationError> {
    let (mut output, uvw) = Msv2Observation::subtable_catalogue(ms)?;
    output.scans = scans;
    let setups = catalogue::spectral_setups(ms, false)?;
    let feeds = catalogue::read_feeds(ms)?;
    let fields = ms.field().map_err(storage)?;
    let groups = data_groups(ms.main_table(), false, uvw);
    let sources = (0..output.fields.len())
        .map(|row| fields.source_id(row).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    for spec in partitions {
        let setup = setups
            .get(spec.data_description)
            .ok_or_else(|| invalid("data description outside the creator's catalogue"))?;
        let window = setup.spectral_window_id;
        let grid = spec.grid;
        let receptors = partition_receptors(
            &feeds,
            &grid.baselines,
            spec.feeds.map(|feed| feed as i32),
            window as i32,
        )?;
        output.partitions.push(PartitionEntry {
            spectral_window: setup.spectral_window.clone(),
            window: WindowId(window),
            frequencies_hz: setup.frequencies_hz.clone(),
            frequency_frame: setup.frequency_frame,
            channel_widths_hz: setup.channel_widths_hz.clone(),
            correlations: setup.correlations.clone(),
            intents: Vec::new(),
            observation: spec.observation,
            feeds: Some(spec.feeds),
            fields: distinct(&grid.field_per_time),
            scans: distinct(&grid.scan_per_time),
            baselines: grid.baselines.clone(),
            time_scale: grid.time_scale,
            time_origin: grid.time_origin,
            data_groups: groups.clone(),
            receptors,
            states: Vec::new(),
            processor: None,
        });
    }
    let windows: Vec<_> = output
        .partitions
        .iter()
        .map(|entry| entry.window.0)
        .collect();
    name_msv4_entities(&mut output, &windows, &sources);
    Ok(output)
}

/// Apply XRADIO's names to `catalogue`, as its converter names an MSv2's:
/// antennas `NAME_STATION` for the telescopes it names so
/// ([`STATION_NAMED`]), fields `NAME_ID`, sources `NAME_SOURCEID` by each
/// field's `SOURCE_ID` (`sources`), and spectral windows `NAME_ID`
/// (`spw_ID` unnamed) by the `SPECTRAL_WINDOW` row of each partition
/// (`windows`).
pub fn name_msv4_entities(catalogue: &mut Catalogue, windows: &[u32], sources: &[i32]) {
    let telescope = catalogue
        .observations
        .first()
        .map_or("", |observation| observation.telescope.as_str());
    if STATION_NAMED.contains(&telescope) {
        for antenna in &mut catalogue.antennas {
            antenna.name = format!("{}_{}", antenna.name, antenna.station);
        }
    }
    for ((index, field), source) in catalogue.fields.iter_mut().enumerate().zip(sources) {
        field.name = format!("{}_{index}", field.name);
        if let Some(name) = &mut field.source_name {
            *name = format!("{name}_{source}");
        }
    }
    for (partition, window) in catalogue.partitions.iter_mut().zip(windows) {
        partition.spectral_window = match partition.spectral_window.as_str() {
            "" | "none" => format!("spw_{window}"),
            name => format!("{name}_{window}"),
        };
    }
}
