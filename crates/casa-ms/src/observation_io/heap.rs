// SPDX-License-Identifier: LGPL-3.0-or-later
//! The heap a catalogue and its entities hold: every string and array at
//! its capacity, a map's entries twice (for its nodes' spare slots) and
//! each ephemeris once. Each entity is taken apart whole, so a member added
//! to one is counted here or the build fails. Openings bound what they will
//! hold with these before they build it, and creation plans charge their
//! catalogues by them.

use casa_observation::{
    AntennaEntry, AntennaId, Catalogue, DataGroupEntry, FieldEntry, ObservationEntry,
    PartitionEntry, ProcessorEntry, Receptors, ScanEntry, SourceEntry, SourceRecord, StateEntry,
    VisibilityUnits, WindowEntry,
};
use casa_types::quanta::Quantity;

/// The heap `catalogue` holds ([`Catalogue::ephemeris_bytes`] for its
/// ephemerides).
pub(crate) fn catalogue_bytes(catalogue: &Catalogue) -> usize {
    let Catalogue {
        antennas,
        fields,
        scans,
        observations,
        partitions,
        windows,
        sources,
        states,
        processors,
    } = catalogue;
    catalogue.ephemeris_bytes()
        + entries(antennas, antenna_bytes)
        + entries(fields, field_bytes)
        + entries(scans, |scan| {
            let ScanEntry {
                name,
                number: _,
                intents,
            } = scan;
            name.capacity() + strings(intents)
        })
        + entries(observations, observation_bytes)
        + entries(partitions, partition_bytes)
        + entries(windows, window_bytes)
        + entries(sources, |source| {
            let SourceEntry { name, records } = source;
            name.capacity() + entries(records, record_bytes)
        })
        + entries(states, |state| {
            let StateEntry {
                obs_mode,
                cal_k: _,
                load_k: _,
                signal: _,
                reference: _,
                sub_scan: _,
                flagged: _,
            } = state;
            obs_mode.capacity()
        })
        + entries(processors, |processor| {
            let ProcessorEntry {
                kind,
                sub_type,
                type_id: _,
                mode_id: _,
                pass_id: _,
                flagged: _,
            } = processor;
            optional(kind) + optional(sub_type)
        })
}

/// A vector's slots, at its capacity, and what each entry holds beyond.
pub(crate) fn entries<T>(values: &Vec<T>, held: impl Fn(&T) -> usize) -> usize {
    values.capacity() * size_of::<T>() + values.iter().map(held).sum::<usize>()
}

/// The heap a vector of plain values holds.
pub(crate) fn plain<T>(values: &Vec<T>) -> usize {
    values.capacity() * size_of::<T>()
}

pub(crate) fn strings(values: &Vec<String>) -> usize {
    entries(values, String::capacity)
}

fn optional(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}

/// A quantity's heap: its unit's name.
fn quantity(value: &Quantity) -> usize {
    value.unit().name().len()
}

fn antenna_bytes(antenna: &AntennaEntry) -> usize {
    let AntennaEntry {
        name,
        station,
        position: _,
        mount,
        dish_diameter,
        flagged: _,
    } = antenna;
    name.capacity() + station.capacity() + mount.capacity() + quantity(dish_diameter)
}

fn field_bytes(field: &FieldEntry) -> usize {
    // The ephemeris, which fields may share, is counted once for the
    // catalogue.
    let FieldEntry {
        name,
        phase_centre: _,
        reference_epoch: _,
        source_name,
        source: _,
        code,
        delay_centre: _,
        reference_centre: _,
        flagged: _,
        ephemeris: _,
    } = field;
    name.capacity() + optional(source_name) + optional(code)
}

fn observation_bytes(observation: &ObservationEntry) -> usize {
    let ObservationEntry {
        telescope,
        observers,
        project,
        execution_block,
        session_reference,
        scheduling_block,
        time_range: _,
        release_date: _,
        flagged: _,
    } = observation;
    telescope.capacity()
        + strings(observers)
        + project.capacity()
        + optional(execution_block)
        + optional(session_reference)
        + optional(scheduling_block)
}

fn window_bytes(window: &WindowEntry) -> usize {
    let WindowEntry {
        name,
        reference_frequency: _,
        total_bandwidth_hz: _,
        if_chain: _,
        net_sideband: _,
        frequency_group: _,
        frequency_group_name,
        effective_bandwidths_hz,
        resolutions_hz,
        flagged: _,
    } = window;
    name.capacity()
        + optional(frequency_group_name)
        + effective_bandwidths_hz.as_ref().map_or(0, plain)
        + resolutions_hz.as_ref().map_or(0, plain)
}

fn record_bytes(record: &SourceRecord) -> usize {
    let SourceRecord {
        window: _,
        time: _,
        interval_s: _,
        code,
        calibration_group: _,
        direction: _,
        proper_motion_rad_s: _,
        position: _,
        pulsar_id: _,
        num_lines: _,
        transitions,
        rest_frequencies,
        systemic_velocities,
    } = record;
    optional(code)
        + transitions.as_ref().map_or(0, strings)
        + rest_frequencies.as_ref().map_or(0, plain)
        + systemic_velocities.as_ref().map_or(0, plain)
}

/// The heap a partition entry holds.
pub(crate) fn partition_bytes(partition: &PartitionEntry) -> usize {
    let PartitionEntry {
        spectral_window,
        window: _,
        frequencies_hz,
        frequency_frame: _,
        channel_widths_hz,
        correlations,
        intents,
        observation: _,
        feeds: _,
        fields,
        scans,
        baselines,
        time_scale: _,
        time_origin: _,
        data_groups,
        receptors,
        states,
        processor: _,
    } = partition;
    spectral_window.capacity()
        + plain(frequencies_hz)
        + plain(channel_widths_hz)
        + plain(correlations)
        + strings(intents)
        + plain(fields)
        + plain(scans)
        + plain(baselines)
        + entries(data_groups, group_bytes)
        + casa_types::memory::btree_node_bytes::<AntennaId, Receptors>(receptors.len())
        + receptors.values().map(receptors_bytes).sum::<usize>()
        + plain(states)
}

/// The heap one antenna's receptor values own, excluding their map's nodes.
pub(crate) fn receptors_bytes(receptors: &Receptors) -> usize {
    let Receptors { types, angles } = receptors;
    strings(types) + entries(angles, quantity)
}

/// The heap a data group's entry holds.
pub(crate) fn group_bytes(group: &DataGroupEntry) -> usize {
    let DataGroupEntry {
        name,
        visibilities,
        flags,
        weights,
        uvw,
        units,
        uvw_frame: _,
    } = group;
    name.capacity()
        + visibilities.capacity()
        + flags.capacity()
        + weights.capacity()
        + uvw.capacity()
        + units_bytes(units)
}

pub(crate) fn units_bytes(units: &VisibilityUnits) -> usize {
    match units {
        VisibilityUnits::Declared(units) => units.capacity(),
        _ => 0,
    }
}
