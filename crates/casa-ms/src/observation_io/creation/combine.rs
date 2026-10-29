// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet several inputs make, decided from their catalogues:
//! the catalogues merged (`casa_observation::merge`), the scans offset
//! ([`super::scans`]), and the partitions combined by the key the MSv2
//! reader partitions rows by (data description, `OBS_MODE`, observation,
//! feeds), each input partition's rows placed in one output partition
//! ([`Placement`]).
//!
//! An output partition holds the union of its inputs' baselines, fields,
//! scans, states and receptors, every window's channels its first
//! contributor's (later inputs matched them within the frequency
//! tolerance), and every selected data group: a missing `corrected` is
//! written from `base` and a missing `model` as CASA's initial model
//! (`VisSetUtil::initScrCols`), each a defaulted loss. Refused here, naming
//! the inputs: a remapped baseline whose first antenna follows its second,
//! an antenna whose receptors differ between inputs, processors that differ
//! within an output partition, one column's units that differ, and UVW in
//! different frames.

use std::collections::{BTreeMap, BTreeSet};

use casa_observation::{
    Catalogue, DataGroupEntry, MergeTolerance, ObservationError, OrderedBaseline, PartitionEntry,
    PartitionId, ScanEntry, ScanId, UvwFrame, VisibilityUnits, merge,
};
use casa_types::measures::MjdHighPrec;

use super::Loss;
use super::inputs::Input;
use super::msv2_rows::{ColumnSource, Placement, RowMap};
use super::scans::{self, Incoming, Session};
use crate::observation_io::update::request_error;

/// What several inputs make.
pub(super) struct Combined {
    /// The output's catalogue: the merged entities, the output's scans,
    /// and its partitions in the order they were combined.
    pub(super) catalogue: Catalogue,
    /// Each input's row map.
    pub(super) rows: Vec<RowMap>,
    /// Which output partitions need a state of intents.
    pub(super) intents: Vec<bool>,
    /// Each output partition's `OBS_MODE`, by which the MSv2 reader keys
    /// it.
    pub(super) modes: Vec<String>,
    /// Each output partition's input partitions, as (input, partition).
    pub(super) contributors: Vec<Vec<(usize, PartitionId)>>,
    /// Each data column's units, which every input shares.
    pub(super) units: Vec<VisibilityUnits>,
    /// What the combination writes that an input did not state.
    pub(super) losses: Vec<Loss>,
    /// The merge's notices.
    pub(super) notices: Vec<String>,
}

/// The data column of a selected group, as the MSv2 reader names them.
pub(super) fn column_name(group: &str) -> &'static str {
    match group {
        "corrected" => "CORRECTED_DATA",
        "model" => "MODEL_DATA",
        _ => "DATA",
    }
}

/// Combine `inputs` into one MeasurementSet of the data groups `groups`
/// (the first `base`), merging their entities within `tolerance`.
///
/// # Errors
///
/// As the merge, and what the module refuses.
pub(super) fn combine(
    inputs: &[Input],
    groups: &[String],
    tolerance: &MergeTolerance,
) -> Result<Combined, ObservationError> {
    let catalogues: Vec<&Catalogue> = inputs
        .iter()
        .map(|input| input.observation.catalogue().as_ref())
        .collect();
    let merged = merge(&catalogues, tolerance)?;
    let offsets = scan_offsets(inputs, &catalogues, &merged)?;
    let mut building = Building::new(groups, inputs.len());
    for (index, catalogue) in catalogues.iter().enumerate() {
        let map = &merged.inputs[index];
        let mut row = RowMap {
            antennas: map.antennas.iter().map(|id| id.0 as i32).collect(),
            fields: map.fields.iter().map(|id| id.0 as i32).collect(),
            scans: catalogue
                .scans
                .iter()
                .map(|scan| scan.number.unwrap_or(0))
                .collect(),
            partitions: Vec::with_capacity(catalogue.partitions.len()),
            weight_scale: inputs[index].weight_scale,
        };
        for id in catalogue.partition_ids() {
            let placement = building.place((index, id), (catalogue, map), &offsets[index], &row)?;
            row.partitions.push(placement);
        }
        building.rows.push(row);
    }
    building.finish(merged)
}

/// Each input's scan offset by its own observing session, as CASA's
/// `concat` offsets them; the first input's are 0.
fn scan_offsets(
    inputs: &[Input],
    catalogues: &[&Catalogue],
    merged: &casa_observation::Merged,
) -> Result<Vec<Vec<i32>>, ObservationError> {
    let mut destination: Vec<Session> = Vec::new();
    let mut offsets = Vec::with_capacity(inputs.len());
    for (index, (input, catalogue)) in inputs.iter().zip(catalogues).enumerate() {
        let map = &merged.inputs[index];
        // Each own session's smallest and largest scan.
        let mut own: Vec<Option<[i32; 2]>> = vec![None; catalogue.observations.len()];
        for entry in &catalogue.partitions {
            for scan in &entry.scans {
                let number = catalogue.scans[scan.0 as usize].number.ok_or_else(|| {
                    request_error("a scan without a casacore number cannot be offset")
                })?;
                let range = &mut own[entry.observation.0 as usize];
                *range = Some(range.map_or([number, number], |[low, high]| {
                    [low.min(number), high.max(number)]
                }));
            }
        }
        let offset = if index == 0 {
            vec![0; own.len()]
        } else {
            let smallest = own
                .iter()
                .flatten()
                .map(|range| range[0])
                .min()
                .unwrap_or(0);
            let incoming: Vec<Incoming> = (0..own.len())
                .map(|session| Incoming {
                    own: session as i32,
                    written: map.observations[session].0 as i32,
                    counterpart: map.counterparts[session],
                })
                .collect();
            scans::offsets(&destination, &incoming, smallest)
        };
        for session in &input.appearance {
            let session = session.0 as usize;
            let Some([low, high]) = own[session] else {
                continue;
            };
            let written = map.observations[session].0 as i32;
            let range = [low + offset[session], high + offset[session]];
            match destination.iter_mut().find(|known| known.id == written) {
                Some(known) => {
                    known.scans = [known.scans[0].min(range[0]), known.scans[1].max(range[1])];
                }
                None => destination.push(Session {
                    id: written,
                    scans: range,
                }),
            }
        }
        offsets.push(offset);
    }
    Ok(offsets)
}

/// The key the MSv2 reader would read an input partition's rows back by,
/// in the output's identities.
#[derive(Clone, Debug, PartialEq)]
struct Key {
    window: u32,
    correlations: Vec<casa_types::Correlation>,
    mode: String,
    observation: u32,
    feeds: Option<[u32; 2]>,
}

/// The output being built.
struct Building<'a> {
    groups: &'a [String],
    keys: Vec<Key>,
    partitions: Vec<PartitionEntry>,
    contributors: Vec<Vec<(usize, PartitionId)>>,
    /// Each output partition's output scan numbers.
    scans: Vec<BTreeSet<i32>>,
    /// Each output scan number's intents.
    scan_intents: BTreeMap<i32, BTreeSet<String>>,
    intents: Vec<bool>,
    /// Each merged window's channels: its first contributor's frequencies,
    /// widths and frame.
    channels: BTreeMap<
        u32,
        (
            Vec<f64>,
            Vec<f64>,
            casa_types::measures::frequency::FrequencyRef,
        ),
    >,
    units: Vec<Option<VisibilityUnits>>,
    uvw_frame: Option<UvwFrame>,
    rows: Vec<RowMap>,
    losses: BTreeSet<Loss>,
}

impl<'a> Building<'a> {
    fn new(groups: &'a [String], inputs: usize) -> Self {
        Self {
            groups,
            keys: Vec::new(),
            partitions: Vec::new(),
            contributors: Vec::new(),
            scans: Vec::new(),
            scan_intents: BTreeMap::new(),
            intents: Vec::new(),
            channels: BTreeMap::new(),
            units: vec![None; groups.len()],
            uvw_frame: None,
            rows: Vec::with_capacity(inputs),
            losses: BTreeSet::new(),
        }
    }

    /// Place input `index`'s partition `id` (of `catalogue`, merged by
    /// `map`), its scans offset by `offsets` (by session), its identities
    /// those of `row`.
    fn place(
        &mut self,
        (index, id): (usize, PartitionId),
        (catalogue, map): (&Catalogue, &casa_observation::InputMap),
        offsets: &[i32],
        row: &RowMap,
    ) -> Result<Placement, ObservationError> {
        let entry = catalogue.partition(id);
        let mode = match entry.states.first() {
            Some(state) => catalogue.states[state.0 as usize].obs_mode.clone(),
            None => entry.intents.join(","),
        };
        let key = Key {
            window: map.windows[entry.window.0 as usize].0,
            correlations: entry.correlations.clone(),
            mode,
            observation: map.observations[entry.observation.0 as usize].0,
            feeds: entry.feeds,
        };
        let processor = entry.processor.map_or(-1, |processor| {
            map.processors[processor.0 as usize].0 as i32
        });
        let output = match self.keys.iter().position(|known| *known == key) {
            Some(output) => {
                let known = self.partitions[output].processor.map_or(-1, |p| p.0 as i32);
                if known != processor {
                    return Err(ObservationError::Unsupported {
                        what: "combining partitions of one key with different processors",
                    });
                }
                output
            }
            None => self.open(key, entry, processor),
        };
        let state = match entry.states.as_slice() {
            [] if entry.intents.is_empty() => Some(-1),
            [] => {
                self.intents[output] = true;
                None
            }
            [first, rest @ ..] => {
                if !rest.is_empty() {
                    self.losses.insert(Loss::Column(
                        "STATE_ID of rows using several states in one partition (each is \
                         written with the first)"
                            .to_string(),
                    ));
                }
                Some(map.states[first.0 as usize].0 as i32)
            }
        };
        let scan_offset = offsets[entry.observation.0 as usize];
        self.join(output, (index, id), (catalogue, map), (row, scan_offset))?;
        let (groups, columns) = self.sources(entry, index)?;
        Ok(Placement {
            output,
            state,
            processor,
            scan_offset,
            groups,
            columns,
        })
    }

    /// A new output partition of `key`, from its first contributor `entry`.
    fn open(&mut self, key: Key, entry: &PartitionEntry, processor: i32) -> usize {
        let channels = self.channels.entry(key.window).or_insert_with(|| {
            (
                entry.frequencies_hz.clone(),
                entry.channel_widths_hz.clone(),
                entry.frequency_frame,
            )
        });
        let partition = PartitionEntry {
            spectral_window: entry.spectral_window.clone(),
            window: casa_observation::WindowId(key.window),
            frequencies_hz: channels.0.clone(),
            frequency_frame: channels.2,
            channel_widths_hz: channels.1.clone(),
            correlations: key.correlations.clone(),
            intents: entry.intents.clone(),
            observation: casa_observation::ObservationId(key.observation),
            feeds: key.feeds,
            fields: Vec::new(),
            scans: Vec::new(),
            baselines: Vec::new(),
            time_scale: entry.time_scale,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            data_groups: Vec::new(),
            receptors: BTreeMap::new(),
            states: Vec::new(),
            processor: u32::try_from(processor)
                .ok()
                .map(casa_observation::ProcessorId),
        };
        self.keys.push(key);
        self.partitions.push(partition);
        self.contributors.push(Vec::new());
        self.scans.push(BTreeSet::new());
        self.intents.push(false);
        self.partitions.len() - 1
    }

    /// Add input `index`'s partition `id` to output partition `output`:
    /// its fields, scans, baselines, receptors and states, in the output's
    /// identities.
    fn join(
        &mut self,
        output: usize,
        (index, id): (usize, PartitionId),
        (catalogue, map): (&Catalogue, &casa_observation::InputMap),
        (row, scan_offset): (&RowMap, i32),
    ) -> Result<(), ObservationError> {
        let entry = catalogue.partition(id);
        self.contributors[output].push((index, id));
        for scan in &entry.scans {
            let number = row.scans[scan.0 as usize] + scan_offset;
            self.scans[output].insert(number);
            self.scan_intents
                .entry(number)
                .or_default()
                .extend(entry.intents.iter().cloned());
        }
        let partition = &mut self.partitions[output];
        for field in &entry.fields {
            partition.fields.push(map.fields[field.0 as usize]);
        }
        for baseline in &entry.baselines {
            let [antenna1, antenna2] = [baseline.antenna1, baseline.antenna2]
                .map(|antenna| map.antennas[antenna.0 as usize]);
            if antenna1 > antenna2 {
                return Err(ObservationError::Unsupported {
                    what: "combining a baseline whose antennas the merge puts in the other order",
                });
            }
            partition
                .baselines
                .push(OrderedBaseline { antenna1, antenna2 });
        }
        for (antenna, receptors) in &entry.receptors {
            let antenna = map.antennas[antenna.0 as usize];
            match partition.receptors.get(&antenna) {
                Some(known) if known != receptors => {
                    return Err(ObservationError::Unsupported {
                        what: "combining an antenna whose receptors differ between inputs",
                    });
                }
                Some(_) => {}
                None => {
                    partition.receptors.insert(antenna, receptors.clone());
                }
            }
        }
        for state in &entry.states {
            partition.states.push(map.states[state.0 as usize]);
        }
        partition.fields.sort_unstable();
        partition.fields.dedup();
        partition.baselines.sort_unstable();
        partition.baselines.dedup();
        partition.states.sort_unstable();
        partition.states.dedup();
        Ok(())
    }

    /// The groups an input partition reads and each data column's source,
    /// its units and UVW frame checked against the other inputs'.
    fn sources(
        &mut self,
        entry: &PartitionEntry,
        index: usize,
    ) -> Result<(Vec<String>, Vec<ColumnSource>), ObservationError> {
        let mut read: Vec<String> = Vec::new();
        let mut columns = Vec::with_capacity(self.groups.len());
        for (column, group) in self.groups.iter().enumerate() {
            let source = match (entry.data_group(group), group.as_str()) {
                (Some(_), _) => Some(group.as_str()),
                (None, "corrected") if entry.data_group("base").is_some() => {
                    self.losses.insert(Loss::Defaulted(
                        "CORRECTED_DATA of an input without it (its DATA is written)".to_string(),
                    ));
                    Some("base")
                }
                (None, "model") => {
                    self.losses.insert(Loss::Defaulted(
                        "MODEL_DATA of an input without it (CASA's initial model is written)"
                            .to_string(),
                    ));
                    None
                }
                (None, _) => {
                    return Err(request_error(format!(
                        "partition {} of input {index} has no data group {group}",
                        entry.spectral_window
                    )));
                }
            };
            let Some(source) = source else {
                columns.push(ColumnSource::InitialModel);
                continue;
            };
            let (_, group_entry) = entry.data_group(source).expect("a group found above");
            match &self.units[column] {
                Some(known) if *known != group_entry.units => {
                    return Err(ObservationError::Unsupported {
                        what: "writing one MSv2 column of data groups with different units",
                    });
                }
                Some(_) => {}
                None => self.units[column] = Some(group_entry.units.clone()),
            }
            let position = read
                .iter()
                .position(|known| known == source)
                .unwrap_or_else(|| {
                    read.push(source.to_string());
                    read.len() - 1
                });
            columns.push(ColumnSource::Group(position));
        }
        let first = read
            .first()
            .and_then(|group| entry.data_group(group))
            .map(|(_, group)| group.uvw_frame)
            .ok_or_else(|| request_error("a partition reads no data group"))?;
        match &self.uvw_frame {
            Some(known) if known.effective != first.effective => {
                return Err(ObservationError::Unsupported {
                    what: "combining UVW in different frames",
                });
            }
            Some(_) => {}
            None => self.uvw_frame = Some(first),
        }
        Ok((read, columns))
    }

    /// The combined output: its scans numbered, each output partition's
    /// data groups the selected ones.
    fn finish(mut self, merged: casa_observation::Merged) -> Result<Combined, ObservationError> {
        let numbers: Vec<i32> = self.scan_intents.keys().copied().collect();
        let scans: Vec<ScanEntry> = self
            .scan_intents
            .iter()
            .map(|(number, intents)| ScanEntry {
                name: number.to_string(),
                number: Some(*number),
                intents: intents.iter().cloned().collect(),
            })
            .collect();
        let uvw_frame = self.uvw_frame.ok_or_else(|| {
            request_error("a MeasurementSet is created from at least one partition")
        })?;
        let units: Vec<VisibilityUnits> = self
            .units
            .iter()
            .map(|units| units.clone().unwrap_or(VisibilityUnits::Undeclared))
            .collect();
        for (partition, own) in self.partitions.iter_mut().zip(&self.scans) {
            partition.scans = own
                .iter()
                .map(
                    |number| ScanId(numbers.binary_search(number).expect("a numbered scan") as u32),
                )
                .collect();
            partition.data_groups = self
                .groups
                .iter()
                .zip(&units)
                .map(|(group, units)| DataGroupEntry {
                    name: group.clone(),
                    visibilities: column_name(group).to_string(),
                    flags: "FLAG".to_string(),
                    weights: "WEIGHT".to_string(),
                    uvw: "UVW".to_string(),
                    units: units.clone(),
                    uvw_frame,
                })
                .collect();
        }
        let catalogue = Catalogue {
            antennas: merged.antennas,
            fields: merged.fields,
            scans,
            observations: merged.observations,
            partitions: self.partitions,
            windows: merged.windows,
            sources: merged.sources,
            states: merged.states,
            processors: merged.processors,
        };
        Ok(Combined {
            catalogue,
            rows: self.rows,
            intents: self.intents,
            modes: self.keys.into_iter().map(|key| key.mode).collect(),
            contributors: self.contributors,
            units,
            losses: self.losses.into_iter().collect(),
            notices: merged.notices,
        })
    }
}
