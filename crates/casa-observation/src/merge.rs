// SPDX-License-Identifier: LGPL-3.0-or-later
//! Merging the catalogues of several observations into one, as CASA's
//! `concat` (casacore `MSConcat::concatenate`) merges MeasurementSets'
//! subtables ([`merge`]).
//!
//! The first catalogue's entities are taken as they are. Each later one is
//! appended in CASA's order (states, sources, windows, antennas, fields,
//! observing sessions, processors), each entity matched against the merged
//! entities so far by casacore's predicate and in its search order, so the
//! representative of a chain of tolerance matches is the one CASA picks;
//! an entity without a match is appended. Observing sessions and
//! processors are instead appended and then made distinct over the whole
//! table, as CASA does, each mapped to the first equivalent; sources are
//! merged as [`sources`] describes.
//!
//! A predicate reads only what both entities state: an entity lacking a
//! member its predicate needs (an MSv4 window's IF chain, a flag no store
//! states) is not matched, but appended, and the merge reports it as a
//! notice ([`Merged::notices`]). Values the predicates compare exactly
//! (names, codes, modes) must be equal; an unstated one is equal to
//! nothing.

mod sources;
#[cfg(test)]
mod tests;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;

use crate::{
    AntennaEntry, AntennaId, Catalogue, FieldEntry, FieldId, ObservationEntry, ObservationError,
    ObservationId, ProcessorEntry, ProcessorId, SourceEntry, SourceId, StateEntry, StateId,
    WindowEntry, WindowId,
};

/// One milliarcsecond in radians: CASA `concat`'s default `dirtol`.
const MILLIARCSECOND: f64 = std::f64::consts::PI / (180.0 * 3600.0 * 1000.0);

/// How close two entities must be to be one: casacore's antenna position
/// tolerance and CASA `concat`'s `dirtol`, `freqtol` and `respectname`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MergeTolerance {
    /// Each component of an antenna's position (casacore: 1 m).
    pub position_m: f64,
    /// The separation of each of a field's directions, strictly below
    /// (`dirtol`, 1 mas by default).
    pub direction_rad: f64,
    /// A window's reference frequency and each channel's (`freqtol`, 1 Hz
    /// by default).
    pub frequency_hz: f64,
    /// Whether fields must also share their names (`respectname`).
    pub respect_names: bool,
}

impl Default for MergeTolerance {
    fn default() -> Self {
        Self {
            position_m: 1.0,
            direction_rad: MILLIARCSECOND,
            frequency_hz: 1.0,
            respect_names: false,
        }
    }
}

/// Where one input's entities are in the merged catalogue, each by the
/// input's own identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputMap {
    /// Each antenna's.
    pub antennas: Vec<AntennaId>,
    /// Each window's.
    pub windows: Vec<WindowId>,
    /// Each field's.
    pub fields: Vec<FieldId>,
    /// Each logical source's; `None` for one the merge removed, which no
    /// field names.
    pub sources: Vec<Option<SourceId>>,
    /// Each state's; empty for an input without states, whose rows keep
    /// none.
    pub states: Vec<StateId>,
    /// Each processor's.
    pub processors: Vec<ProcessorId>,
    /// Each observing session's.
    pub observations: Vec<ObservationId>,
    /// Whether each observing session matched one already merged: CASA's
    /// counterpart, which its scan numbering continues.
    pub counterparts: Vec<bool>,
}

/// The merged entities and each input's map into them.
#[derive(Clone, Debug, Default)]
pub struct Merged {
    /// The antennas.
    pub antennas: Vec<AntennaEntry>,
    /// The windows.
    pub windows: Vec<WindowEntry>,
    /// The fields, each naming its merged source.
    pub fields: Vec<FieldEntry>,
    /// The logical sources.
    pub sources: Vec<SourceEntry>,
    /// The states.
    pub states: Vec<StateEntry>,
    /// The processors.
    pub processors: Vec<ProcessorEntry>,
    /// The observing sessions.
    pub observations: Vec<ObservationEntry>,
    /// Each input's map, in input order.
    pub inputs: Vec<InputMap>,
    /// Entities appended because a predicate lacked what it needs, each
    /// said once.
    pub notices: Vec<String>,
}

impl Merged {
    fn notice(&mut self, notice: String) {
        if !self.notices.contains(&notice) {
            self.notices.push(notice);
        }
    }
}

/// A window's channels: the frequencies and frame of the first partition
/// that uses it, `None` for a window no partition uses.
type Channels = Option<(Vec<f64>, FrequencyRef)>;

/// The channels of each of `catalogue`'s windows.
fn window_channels(catalogue: &Catalogue) -> Vec<Channels> {
    (0..catalogue.windows.len())
        .map(|index| {
            catalogue
                .partitions
                .iter()
                .find(|entry| entry.window.0 as usize == index)
                .map(|entry| (entry.frequencies_hz.clone(), entry.frequency_frame))
        })
        .collect()
}

/// Merge `catalogues` (at least one) as CASA's `concat` merges their
/// MeasurementSets, within `tolerance`.
///
/// # Errors
///
/// [`ObservationError::Unsupported`] for what CASA's predicates cannot
/// match or this merge does not: antenna positions in different frames
/// (casacore refuses them) and a window whose channels match another's
/// only reversed. (A catalogue holds no field in a solar-system frame, which
/// CASA matches by frame alone: no store's adapter represents one.)
pub fn merge(
    catalogues: &[&Catalogue],
    tolerance: &MergeTolerance,
) -> Result<Merged, ObservationError> {
    let mut merged = Merged::default();
    let mut channels: Vec<Channels> = Vec::new();
    for (index, catalogue) in catalogues.iter().enumerate() {
        let own_channels = window_channels(catalogue);
        if index == 0 {
            let map = first(&mut merged, catalogue);
            merged.inputs.push(map);
            channels = own_channels;
            continue;
        }
        let mut map = InputMap {
            states: states(&mut merged, catalogue),
            ..InputMap::default()
        };
        map.windows = windows(
            &mut merged,
            (&mut channels, catalogue, &own_channels),
            tolerance,
        )?;
        map.antennas = antennas(&mut merged, catalogue, tolerance)?;
        let before = merged.fields.len();
        let (fields, matched) = fields(&mut merged, catalogue, tolerance);
        map.fields = fields;
        merged.inputs.push(map);
        sources::append(&mut merged, catalogue, (before, matched))?;
        observations(&mut merged, catalogue);
        processors(&mut merged, catalogue);
    }
    Ok(merged)
}

/// The first input's entities, taken as they are.
fn first(merged: &mut Merged, catalogue: &Catalogue) -> InputMap {
    merged.antennas.clone_from(&catalogue.antennas);
    merged.windows.clone_from(&catalogue.windows);
    merged.fields.clone_from(&catalogue.fields);
    merged.sources.clone_from(&catalogue.sources);
    merged.states.clone_from(&catalogue.states);
    merged.processors.clone_from(&catalogue.processors);
    merged.observations.clone_from(&catalogue.observations);
    let ids = |count: usize| 0..count as u32;
    InputMap {
        antennas: ids(catalogue.antennas.len()).map(AntennaId).collect(),
        windows: ids(catalogue.windows.len()).map(WindowId).collect(),
        fields: ids(catalogue.fields.len()).map(FieldId).collect(),
        sources: ids(catalogue.sources.len())
            .map(|id| Some(SourceId(id)))
            .collect(),
        states: ids(catalogue.states.len()).map(StateId).collect(),
        processors: ids(catalogue.processors.len()).map(ProcessorId).collect(),
        observations: ids(catalogue.observations.len())
            .map(ObservationId)
            .collect(),
        counterparts: vec![false; catalogue.observations.len()],
    }
}

/// Whether `values` are all stated: the evidence a predicate needs.
macro_rules! stated {
    ($($value:expr),+ $(,)?) => {
        true $(&& $value.is_some())+
    };
}

/// Each incoming state matched (casacore `MSStateColumns::matchState`,
/// from the last state back) or appended. An input without states has no
/// map: its rows keep none, where CASA drops every state of the result
/// (recorded in `docs/CASA (C++) bugs.md`).
fn states(merged: &mut Merged, catalogue: &Catalogue) -> Vec<StateId> {
    const KELVIN: f64 = 1.0;
    let mut map = Vec::with_capacity(catalogue.states.len());
    for state in &catalogue.states {
        let evident = stated!(
            state.cal_k,
            state.load_k,
            state.signal,
            state.reference,
            state.sub_scan
        );
        let matches = |known: &StateEntry| {
            known.flagged == Some(false)
                && stated!(known.cal_k, known.load_k)
                && (known.cal_k.unwrap_or(f64::NAN) - state.cal_k.unwrap_or(f64::NAN)).abs()
                    < KELVIN
                && (known.load_k.unwrap_or(f64::NAN) - state.load_k.unwrap_or(f64::NAN)).abs()
                    < KELVIN
                && known.obs_mode == state.obs_mode
                && stated!(known.reference, known.signal, known.sub_scan)
                && known.reference == state.reference
                && known.signal == state.signal
                && known.sub_scan == state.sub_scan
        };
        let found = if evident {
            merged.states.iter().rposition(matches)
        } else {
            merged.notice(format!(
                "state {} is appended: it does not state what matching needs",
                state.obs_mode
            ));
            None
        };
        map.push(StateId(found.unwrap_or_else(|| {
            merged.states.push(state.clone());
            merged.states.len() - 1
        }) as u32));
    }
    map
}

/// Each incoming window matched (casacore `MSSpWindowColumns::matchSpw`) or
/// appended: of the merged windows in order, those unflagged with the same
/// channel count and IF chain, a total bandwidth within a quarter of the
/// incoming one and a reference frequency in the same frame within the
/// tolerance, the first whose every channel is within it (the first
/// outright for one channel). `channels` are the merged windows', and
/// `own` the incoming catalogue's.
///
/// The incoming windows are visited as `MSConcat::copySpwAndPol` visits
/// them, which matters because matching within the tolerance is not
/// transitive: those its partitions use, in the order they are first used
/// (an MSv2's partitions are ordered by data description first, so this is
/// CASA's DATA_DESCRIPTION order), then the others in row order. A window
/// of a DATA_DESCRIPTION row no MAIN row uses is among the others, which
/// CASA would visit in that row's place.
fn windows(
    merged: &mut Merged,
    (channels, catalogue, own): (&mut Vec<Channels>, &Catalogue, &[Channels]),
    tolerance: &MergeTolerance,
) -> Result<Vec<WindowId>, ObservationError> {
    let count = catalogue.windows.len();
    let mut seen = vec![false; count];
    let mut order = Vec::with_capacity(count);
    let used = catalogue
        .partitions
        .iter()
        .map(|entry| entry.window.0 as usize);
    for window in used.chain(0..count) {
        if window < count && !seen[window] {
            seen[window] = true;
            order.push(window);
        }
    }
    let mut map = vec![WindowId(0); count];
    for index in order {
        let (window, incoming) = (&catalogue.windows[index], &own[index]);
        let evident = incoming.is_some()
            && stated!(
                window.if_chain,
                window.total_bandwidth_hz,
                window.reference_frequency
            );
        let found = if evident {
            window_match(merged, channels, (window, incoming.as_ref()), tolerance)?
        } else {
            merged.notice(format!(
                "window {} is appended: it does not state what matching needs",
                window.name
            ));
            None
        };
        map[index] = WindowId(found.unwrap_or_else(|| {
            merged.windows.push(window.clone());
            channels.push(incoming.clone());
            merged.windows.len() - 1
        }) as u32);
    }
    Ok(map)
}

/// The merged window `window` (with `incoming` channels) matches, if any.
fn window_match(
    merged: &Merged,
    channels: &[Channels],
    (window, incoming): (&WindowEntry, Option<&(Vec<f64>, FrequencyRef)>),
    tolerance: &MergeTolerance,
) -> Result<Option<usize>, ObservationError> {
    let (Some((frequencies, _)), Some(bandwidth), Some(reference)) = (
        incoming,
        window.total_bandwidth_hz,
        window.reference_frequency.as_ref(),
    ) else {
        return Ok(None);
    };
    let near = |a: f64, b: f64| (a - b).abs() <= tolerance.frequency_hz;
    let candidates =
        merged
            .windows
            .iter()
            .zip(channels)
            .enumerate()
            .filter(|(_, (known, known_channels))| {
                let Some((known_frequencies, _)) = known_channels else {
                    return false;
                };
                known.flagged == Some(false)
                    && known_frequencies.len() == frequencies.len()
                    && known.if_chain.is_some()
                    && known.if_chain == window.if_chain
                    && known
                        .total_bandwidth_hz
                        .is_some_and(|known| (known - bandwidth).abs() <= bandwidth.abs() / 4.0)
                    && known.reference_frequency.as_ref().is_some_and(|known| {
                        known.refer() == reference.refer() && near(known.hz(), reference.hz())
                    })
            });
    for (index, (_, known_channels)) in candidates {
        let Some((known, _)) = known_channels else {
            continue;
        };
        if frequencies.len() == 1 || known.iter().zip(frequencies).all(|(a, b)| near(*a, *b)) {
            return Ok(Some(index));
        }
        if known
            .iter()
            .zip(frequencies.iter().rev())
            .all(|(a, b)| near(*a, *b))
        {
            return Err(ObservationError::Unsupported {
                what: "merging a window whose channels match another's only reversed",
            });
        }
    }
    Ok(None)
}

/// Each incoming antenna matched (casacore
/// `MSAntennaColumns::matchAntennaAndStation`, from the last antenna back)
/// or appended: unflagged, of the same name, of the same station unless the
/// incoming one states none, each position component within the
/// tolerance.
fn antennas(
    merged: &mut Merged,
    catalogue: &Catalogue,
    tolerance: &MergeTolerance,
) -> Result<Vec<AntennaId>, ObservationError> {
    let mut map = Vec::with_capacity(catalogue.antennas.len());
    for antenna in &catalogue.antennas {
        if merged
            .antennas
            .first()
            .is_some_and(|known| known.position.refer() != antenna.position.refer())
        {
            return Err(ObservationError::Unsupported {
                what: "merging antennas whose positions are in different frames",
            });
        }
        let position = antenna.position.values();
        let found = merged.antennas.iter().rposition(|known| {
            let known_position = known.position.values();
            known.flagged == Some(false)
                && (antenna.station.is_empty() || known.station == antenna.station)
                && known.name == antenna.name
                && known_position
                    .iter()
                    .zip(&position)
                    .all(|(a, b)| (a - b).abs() <= tolerance.position_m)
        });
        map.push(AntennaId(found.unwrap_or_else(|| {
            merged.antennas.push(antenna.clone());
            merged.antennas.len() - 1
        }) as u32));
    }
    Ok(map)
}

/// The great-circle separation of two directions, as casacore computes it
/// (`MVDirection::separation`).
pub(crate) fn separation(a: &MDirection, b: &MDirection) -> f64 {
    let (a, b) = (a.cosines(), b.cosines());
    let chord = ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2) + (a[2] - b[2]).powi(2)).sqrt();
    2.0 * (chord / 2.0).min(1.0).asin()
}

/// Each incoming field matched (casacore `MSFieldColumns::matchDirection`
/// as `MSConcat::copyField` calls it) or appended, and whether any matched:
/// the last merged field of its name is tried first, then every field from
/// the last back, for one unflagged whose reference, delay and phase
/// directions are each in the frame of its reference direction and
/// strictly within the tolerance of the incoming ones; with names
/// respected, a match of another name is not used. An appended field's
/// source is the incoming one's until [`sources::append`] maps it.
fn fields(
    merged: &mut Merged,
    catalogue: &Catalogue,
    tolerance: &MergeTolerance,
) -> (Vec<FieldId>, bool) {
    let mut map = Vec::with_capacity(catalogue.fields.len());
    let mut matched = false;
    for field in &catalogue.fields {
        let found = match (&field.delay_centre, &field.reference_centre) {
            (Some(delay), Some(reference)) => {
                field_match(merged, field, (delay, reference), tolerance)
            }
            _ => {
                merged.notice(format!(
                    "field {} is appended: it does not state what matching needs",
                    field.name
                ));
                None
            }
        };
        matched |= found.is_some();
        map.push(FieldId(found.unwrap_or_else(|| {
            merged.fields.push(field.clone());
            merged.fields.len() - 1
        }) as u32));
    }
    (map, matched)
}

/// The merged field `field` matches, if any.
fn field_match(
    merged: &Merged,
    field: &FieldEntry,
    (delay, reference): (&MDirection, &MDirection),
    tolerance: &MergeTolerance,
) -> Option<usize> {
    let matches = |known: &FieldEntry| {
        let (Some(known_delay), Some(known_reference)) =
            (&known.delay_centre, &known.reference_centre)
        else {
            return false;
        };
        let frame: DirectionRef = known_reference.refer();
        known.flagged == Some(false)
            && [reference, delay, &field.phase_centre]
                .iter()
                .all(|direction| direction.refer() == frame)
            && separation(known_reference, reference) < tolerance.direction_rad
            && separation(known_delay, delay) < tolerance.direction_rad
            && separation(&known.phase_centre, &field.phase_centre) < tolerance.direction_rad
    };
    let named = merged
        .fields
        .iter()
        .rposition(|known| known.name == field.name);
    let found = named
        .filter(|index| matches(&merged.fields[*index]))
        .or_else(|| merged.fields.iter().rposition(matches))?;
    (!tolerance.respect_names || merged.fields[found].name == field.name).then_some(found)
}

/// Whether two observing sessions are one (`MSConcat::obsRowsEquivalent`):
/// their flags, observers, projects, release dates, telescopes and extents
/// equal, each stated.
fn same_session(a: &ObservationEntry, b: &ObservationEntry) -> bool {
    let epoch = |a: &casa_types::measures::MEpoch, b: &casa_types::measures::MEpoch| {
        a.refer() == b.refer() && a.value().as_mjd() == b.value().as_mjd()
    };
    stated!(a.flagged, a.release_date, a.time_range)
        && a.flagged == b.flagged
        && a.observers == b.observers
        && a.project == b.project
        && a.telescope == b.telescope
        && b.release_date
            .as_ref()
            .zip(a.release_date.as_ref())
            .is_some_and(|(a, b)| epoch(a, b))
        && b.time_range
            .as_ref()
            .zip(a.time_range.as_ref())
            .is_some_and(|(a, b)| epoch(&a[0], &b[0]) && epoch(&a[1], &b[1]))
}

/// Whether two processors are one (`MSConcat::procRowsEquivalent`): their
/// flags, modes, types, type IDs and subtypes equal and stated, and their
/// pass IDs equal (an optional column).
fn same_processor(a: &ProcessorEntry, b: &ProcessorEntry) -> bool {
    stated!(a.flagged, a.mode_id, a.kind, a.type_id, a.sub_type)
        && a.flagged == b.flagged
        && a.mode_id == b.mode_id
        && a.kind == b.kind
        && a.type_id == b.type_id
        && a.sub_type == b.sub_type
        && a.pass_id == b.pass_id
}

/// `entries` with each one equal (`same`) to an earlier one removed, and
/// where each old position went: the first equivalent's position among
/// those kept. CASA maps a duplicate to the last earlier equivalent and
/// does not close the gaps the removals leave in its observation IDs
/// (recorded in `docs/CASA (C++) bugs.md`); with an exact predicate the
/// first equivalent is the same entity.
fn distinct<T>(entries: Vec<T>, same: impl Fn(&T, &T) -> bool) -> (Vec<T>, Vec<usize>) {
    let mut kept: Vec<T> = Vec::with_capacity(entries.len());
    let mut moved = Vec::with_capacity(entries.len());
    for entry in entries {
        match kept.iter().position(|known| same(known, &entry)) {
            Some(index) => moved.push(index),
            None => {
                moved.push(kept.len());
                kept.push(entry);
            }
        }
    }
    (kept, moved)
}

/// The incoming observing sessions appended, then every session made
/// distinct (`MSConcat::copyObservation` with redundant IDs removed), the
/// maps of the inputs before remapped, and the counterparts noted.
fn observations(merged: &mut Merged, catalogue: &Catalogue) {
    let existing = merged.observations.len();
    let mut entries = std::mem::take(&mut merged.observations);
    entries.extend(catalogue.observations.iter().cloned());
    let (kept, moved) = distinct(entries, same_session);
    merged.observations = kept;
    let to = |index: usize| ObservationId(moved[index] as u32);
    let (input, earlier) = merged.inputs.split_last_mut().expect("this input's map");
    for map in earlier {
        for id in &mut map.observations {
            *id = to(id.0 as usize);
        }
    }
    input.observations = (0..catalogue.observations.len())
        .map(|index| to(existing + index))
        .collect();
    // The earlier sessions keep the first positions, so an incoming one
    // with a counterpart went below the last of them.
    let earlier = moved[..existing].iter().max().map_or(0, |last| last + 1);
    input.counterparts = (0..catalogue.observations.len())
        .map(|index| moved[existing + index] < earlier)
        .collect();
}

/// The incoming processors appended, then every processor made distinct
/// (`MSConcat::copyProcessor` with redundant IDs removed), the maps of
/// the inputs before remapped.
fn processors(merged: &mut Merged, catalogue: &Catalogue) {
    let existing = merged.processors.len();
    let mut entries = std::mem::take(&mut merged.processors);
    entries.extend(catalogue.processors.iter().cloned());
    let (kept, moved) = distinct(entries, same_processor);
    merged.processors = kept;
    let to = |index: usize| ProcessorId(moved[index] as u32);
    let (input, earlier) = merged.inputs.split_last_mut().expect("this input's map");
    for map in earlier {
        for id in &mut map.processors {
            *id = to(id.0 as usize);
        }
    }
    input.processors = (0..catalogue.processors.len())
        .map(|index| to(existing + index))
        .collect();
}
