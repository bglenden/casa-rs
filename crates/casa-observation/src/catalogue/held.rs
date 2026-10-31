// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a catalogue holds on the heap ([`Catalogue::held_bytes`]): each
//! entry vector and, within each entry, its vectors and strings, by
//! capacity. A store's opening charges it with what else the store holds.

use std::collections::BTreeMap;

use casa_types::quanta::Quantity;

use super::{
    AntennaEntry, AntennaId, Catalogue, DataGroupEntry, FieldEntry, ObservationEntry,
    PartitionEntry, ProcessorEntry, Receptors, ScanEntry, SourceEntry, SourceRecord, StateEntry,
    WindowEntry,
};

impl Catalogue {
    /// Bytes the catalogue holds on the heap: its entry vectors, each
    /// entry's own vectors and strings, by capacity, and the fields'
    /// ephemerides ([`Self::ephemeris_bytes`]). Receptor-map nodes are
    /// bounded at minimum occupancy, including a small map's root;
    /// keys' and values' own buffers are counted by capacity.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        [
            items(&self.antennas, antenna),
            items(&self.fields, field),
            self.ephemeris_bytes(),
            items(&self.scans, scan),
            items(&self.observations, observation),
            items(&self.partitions, partition),
            items(&self.windows, window),
            items(&self.sources, source),
            items(&self.states, |state: &StateEntry| state.obs_mode.capacity()),
            items(&self.processors, processor),
        ]
        .into_iter()
        .sum()
    }
}

/// A vector's buffer and what each of its items holds beyond itself.
fn items<T>(items: &Vec<T>, held: impl Fn(&T) -> usize) -> usize {
    items.capacity() * size_of::<T>() + items.iter().map(held).sum::<usize>()
}

fn strings(strings: &Vec<String>) -> usize {
    items(strings, String::capacity)
}

fn optional(text: Option<&String>) -> usize {
    text.map_or(0, String::capacity)
}

fn quantity(quantity: &Quantity) -> usize {
    quantity.unit().name().len()
}

fn antenna(entry: &AntennaEntry) -> usize {
    entry.name.capacity()
        + entry.station.capacity()
        + entry.mount.capacity()
        + quantity(&entry.dish_diameter)
}

fn field(entry: &FieldEntry) -> usize {
    entry.name.capacity() + optional(entry.source_name.as_ref()) + optional(entry.code.as_ref())
}

fn scan(entry: &ScanEntry) -> usize {
    entry.name.capacity() + strings(&entry.intents)
}

fn observation(entry: &ObservationEntry) -> usize {
    entry.telescope.capacity()
        + strings(&entry.observers)
        + entry.project.capacity()
        + optional(entry.execution_block.as_ref())
        + optional(entry.session_reference.as_ref())
        + optional(entry.scheduling_block.as_ref())
}

fn receptors(map: &BTreeMap<AntennaId, Receptors>) -> usize {
    casa_types::memory::btree_node_bytes::<AntennaId, Receptors>(map.len())
        + map
            .values()
            .map(|receptors| strings(&receptors.types) + items(&receptors.angles, quantity))
            .sum::<usize>()
}

fn data_group(entry: &DataGroupEntry) -> usize {
    entry.name.capacity()
        + entry.visibilities.capacity()
        + entry.flags.capacity()
        + entry.weights.capacity()
        + entry.uvw.capacity()
}

fn partition(entry: &PartitionEntry) -> usize {
    entry.spectral_window.capacity()
        + items(&entry.frequencies_hz, |_| 0)
        + items(&entry.channel_widths_hz, |_| 0)
        + items(&entry.correlations, |_| 0)
        + strings(&entry.intents)
        + items(&entry.fields, |_| 0)
        + items(&entry.scans, |_| 0)
        + items(&entry.baselines, |_| 0)
        + items(&entry.data_groups, data_group)
        + receptors(&entry.receptors)
        + items(&entry.states, |_| 0)
}

fn window(entry: &WindowEntry) -> usize {
    entry.name.capacity()
        + optional(entry.frequency_group_name.as_ref())
        + entry
            .effective_bandwidths_hz
            .as_ref()
            .map_or(0, |values| items(values, |_| 0))
        + entry
            .resolutions_hz
            .as_ref()
            .map_or(0, |values| items(values, |_| 0))
}

fn source(entry: &SourceEntry) -> usize {
    entry.name.capacity() + items(&entry.records, record)
}

fn record(record: &SourceRecord) -> usize {
    optional(record.code.as_ref())
        + record.transitions.as_ref().map_or(0, strings)
        + record
            .rest_frequencies
            .as_ref()
            .map_or(0, |values| items(values, |_| 0))
        + record
            .systemic_velocities
            .as_ref()
            .map_or(0, |values| items(values, |_| 0))
}

fn processor(entry: &ProcessorEntry) -> usize {
    optional(entry.kind.as_ref()) + optional(entry.sub_type.as_ref())
}
