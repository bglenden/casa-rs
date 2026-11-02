// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue's descriptive metadata (#937): spectral windows, sources,
//! states and processors, beside the identities the samples carry.
//!
//! Every member a store may leave unstated is an `Option`, and `None` is
//! never a value: a flag is `Option<bool>`, so an unstated flag is not
//! `false`; a string is `Option<String>`, so an unstated one is not empty.
//! An adapter validates shapes and units at opening and refuses a store
//! whose metadata it cannot read as stated, naming the column.

use casa_types::measures::MEpoch;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::MFrequency;
use casa_types::measures::position::MPosition;
use casa_types::measures::radial_velocity::RadialVelocityRef;

/// A spectral window of one opened observation: an index into
/// [`Catalogue::windows`](super::Catalogue::windows). Windows have their own
/// identity because sources and partitions both name them, and a window no
/// partition uses (an MSv2 `SPECTRAL_WINDOW` row without rows in MAIN) is
/// still part of what the store states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowId(pub u32);

/// One spectral window: what defines it besides the channel frequencies,
/// widths and frame its partitions carry (MSv2 `SPECTRAL_WINDOW`; MSv4 the
/// frequency coordinate's attributes).
#[derive(Clone, Debug, Default)]
pub struct WindowEntry {
    /// The window's name as stored (MSv2 `NAME`; MSv4
    /// `spectral_window_name`).
    pub name: String,
    /// The reference frequency in its stored frame (MSv2 `REF_FREQUENCY`
    /// with its `MEASINFO`; MSv4 `reference_frequency`). A simulator writes
    /// the sky model's reference value here, which need not be any
    /// channel's frequency.
    pub reference_frequency: Option<MFrequency>,
    /// `TOTAL_BANDWIDTH`, Hz.
    pub total_bandwidth_hz: Option<f64>,
    /// `IF_CONV_CHAIN`.
    pub if_chain: Option<i32>,
    /// `NET_SIDEBAND`.
    pub net_sideband: Option<i32>,
    /// `FREQ_GROUP`.
    pub frequency_group: Option<i32>,
    /// `FREQ_GROUP_NAME`.
    pub frequency_group_name: Option<String>,
    /// `EFFECTIVE_BW` of each channel, Hz.
    pub effective_bandwidths_hz: Option<Vec<f64>>,
    /// `RESOLUTION` of each channel, Hz.
    pub resolutions_hz: Option<Vec<f64>>,
    /// `FLAG_ROW`.
    pub flagged: Option<bool>,
}

impl WindowEntry {
    /// Whether `other` can describe the same window: every member both
    /// state is equal (bit for bit), and a member either leaves unstated
    /// matches anything, so that an MSv2 and its XRADIO conversion, which
    /// keeps fewer members, still bind. Names, which converters rewrite,
    /// take no part. Concatenation's matching is stricter: it needs the
    /// members its predicate compares.
    #[must_use]
    pub fn compatible(&self, other: &Self) -> bool {
        fn agree<T>(a: Option<&T>, b: Option<&T>, same: impl Fn(&T, &T) -> bool) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => same(a, b),
                _ => true,
            }
        }
        let bits = |a: &f64, b: &f64| a.to_bits() == b.to_bits();
        let all_bits = |a: &Vec<f64>, b: &Vec<f64>| {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| bits(a, b))
        };
        agree(
            self.reference_frequency.as_ref(),
            other.reference_frequency.as_ref(),
            |a, b| a.refer() == b.refer() && bits(&a.hz(), &b.hz()),
        ) && agree(
            self.total_bandwidth_hz.as_ref(),
            other.total_bandwidth_hz.as_ref(),
            bits,
        ) && agree(
            self.if_chain.as_ref(),
            other.if_chain.as_ref(),
            PartialEq::eq,
        ) && agree(
            self.net_sideband.as_ref(),
            other.net_sideband.as_ref(),
            PartialEq::eq,
        ) && agree(
            self.frequency_group.as_ref(),
            other.frequency_group.as_ref(),
            PartialEq::eq,
        ) && agree(
            self.frequency_group_name.as_ref(),
            other.frequency_group_name.as_ref(),
            PartialEq::eq,
        ) && agree(
            self.effective_bandwidths_hz.as_ref(),
            other.effective_bandwidths_hz.as_ref(),
            all_bits,
        ) && agree(
            self.resolutions_hz.as_ref(),
            other.resolutions_hz.as_ref(),
            all_bits,
        ) && agree(self.flagged.as_ref(), other.flagged.as_ref(), PartialEq::eq)
    }
}

/// A logical source of one opened observation: an index into
/// [`Catalogue::sources`](super::Catalogue::sources). It is a source (an
/// MSv2 `SOURCE_ID`; an MSv4 field's source), never one `SOURCE` row: one
/// source has a record per spectral window and validity interval. Two
/// sources that share a name stay two sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub u32);

/// The spectral window a source record states its lines for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceWindow {
    /// Every window (MSv2 `SPECTRAL_WINDOW_ID` −1).
    All,
    /// One window.
    Window(WindowId),
}

/// A radial velocity as the store states it: its value, and its frame when
/// the store names one (MSv2 `SYSVEL` carries a `MEASINFO`; XRADIO's
/// `LINE_SYSTEMIC_VELOCITY` is a quantity in m/s without one). No frame is
/// invented for a value stated without one.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Velocity {
    /// The velocity, m/s.
    pub value_m_s: f64,
    /// Its frame, when stated.
    pub frame: Option<RadialVelocityRef>,
}

/// One record of a source: what the store states of it for one window and,
/// when stated, one validity interval (MSv2: one `SOURCE` row).
#[derive(Clone, Debug)]
pub struct SourceRecord {
    /// The window the record's lines are for.
    pub window: SourceWindow,
    /// The centre of the record's validity interval (`TIME`).
    pub time: Option<MEpoch>,
    /// The validity interval, s (`INTERVAL`).
    pub interval_s: Option<f64>,
    /// `CODE`.
    pub code: Option<String>,
    /// `CALIBRATION_GROUP`.
    pub calibration_group: Option<i32>,
    /// The source's direction (MSv2 `DIRECTION`; MSv4 `SOURCE_DIRECTION`).
    pub direction: Option<MDirection>,
    /// `PROPER_MOTION`, rad/s in longitude and latitude.
    pub proper_motion_rad_s: Option<[f64; 2]>,
    /// `POSITION`, for a source at a finite distance.
    pub position: Option<MPosition>,
    /// `PULSAR_ID`.
    pub pulsar_id: Option<i32>,
    /// `NUM_LINES`, as stored; each line array below, when present, has
    /// this many elements.
    pub num_lines: u32,
    /// `TRANSITION` of each line.
    pub transitions: Option<Vec<String>>,
    /// `REST_FREQUENCY` of each line (MSv4 `LINE_REST_FREQUENCY`).
    pub rest_frequencies: Option<Vec<MFrequency>>,
    /// `SYSVEL` of each line (MSv4 `LINE_SYSTEMIC_VELOCITY`).
    pub systemic_velocities: Option<Vec<Velocity>>,
}

/// One logical source and its records, in stored order.
#[derive(Clone, Debug)]
pub struct SourceEntry {
    /// The source's name as stored.
    pub name: String,
    /// Its records.
    pub records: Vec<SourceRecord>,
}

/// A state of one opened observation: an index into
/// [`Catalogue::states`](super::Catalogue::states).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateId(pub u32);

/// One state: an MSv2 `STATE` row. MSv4 states intents only, and has none.
#[derive(Clone, Debug)]
pub struct StateEntry {
    /// `OBS_MODE`, whole: a comma-separated list of intents.
    pub obs_mode: String,
    /// `CAL`, K.
    pub cal_k: Option<f64>,
    /// `LOAD`, K.
    pub load_k: Option<f64>,
    /// `SIG`.
    pub signal: Option<bool>,
    /// `REF`.
    pub reference: Option<bool>,
    /// `SUB_SCAN`.
    pub sub_scan: Option<i32>,
    /// `FLAG_ROW`. CASA's intent selection never selects the rows of a
    /// flagged state ([`Catalogue::partitions_with_intent`](super::Catalogue::partitions_with_intent)).
    pub flagged: Option<bool>,
}

/// A processor of one opened observation: an index into
/// [`Catalogue::processors`](super::Catalogue::processors).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessorId(pub u32);

/// One processor (MSv2 `PROCESSOR` row; MSv4 `processor_info`, which states
/// the type and subtype only).
#[derive(Clone, Debug, Default)]
pub struct ProcessorEntry {
    /// `TYPE` (`CORRELATOR`, `SPECTROMETER`, ...).
    pub kind: Option<String>,
    /// `SUB_TYPE`.
    pub sub_type: Option<String>,
    /// `TYPE_ID`.
    pub type_id: Option<i32>,
    /// `MODE_ID`.
    pub mode_id: Option<i32>,
    /// `PASS_ID`.
    pub pass_id: Option<i32>,
    /// `FLAG_ROW`.
    pub flagged: Option<bool>,
}

impl super::Catalogue {
    /// Spectral window `id`.
    ///
    /// # Panics
    ///
    /// If `id` was not issued by this catalogue.
    #[must_use]
    pub fn window(&self, id: WindowId) -> &WindowEntry {
        &self.windows[id.0 as usize]
    }

    /// The records of `field`'s source for `window`: that window's own,
    /// then those for every window, each in stored order. CASA's default
    /// rest frequency is the first record's first line (`tsdimaging`; the
    /// imager's cube rest frequency); a caller that needs a time chooses
    /// among the records by their validity. Empty when the field names no
    /// source, or its source states nothing for the window.
    #[must_use]
    pub fn source_records(&self, field: super::FieldId, window: WindowId) -> Vec<&SourceRecord> {
        let Some(source) = self
            .fields
            .get(field.0 as usize)
            .and_then(|field| field.source)
            .and_then(|source| self.sources.get(source.0 as usize))
        else {
            return Vec::new();
        };
        let in_window = |wanted: SourceWindow| {
            source
                .records
                .iter()
                .filter(move |record| record.window == wanted)
        };
        in_window(SourceWindow::Window(window))
            .chain(in_window(SourceWindow::All))
            .collect()
    }

    /// The partitions CASA's selection of `intent` reads
    /// (`MSStateIndex::matchStateObsMode`): those of a state whose stored
    /// `OBS_MODE`, split at commas and not trimmed, has an element equal to
    /// it, without the rows of flagged states (so `"A, B"` holds `" B"`, not
    /// `"B"`). A partition whose states are all flagged is not selected. A
    /// partition without states (MSv4) is selected by its stated intents
    /// alone. Rows without a state (`STATE_ID` −1) have no intents and are
    /// never selected.
    ///
    /// # Errors
    ///
    /// [`IntentError::MixedStateFlags`] for a partition whose rows use
    /// flagged and unflagged states: a batch has no state to separate
    /// them. Reading it without intent selection still reads every row.
    pub fn partitions_with_intent(
        &self,
        intent: &str,
    ) -> Result<Vec<super::PartitionId>, IntentError> {
        let mut selected = Vec::new();
        for (index, partition) in self.partitions.iter().enumerate() {
            let matched = if partition.states.is_empty() {
                partition.intents.iter().any(|known| known == intent)
            } else {
                partition.states.iter().any(|state| {
                    self.states[state.0 as usize]
                        .obs_mode
                        .split(',')
                        .any(|element| element == intent)
                })
            };
            if !matched {
                continue;
            }
            let id = super::PartitionId(u32::try_from(index).expect("few partitions"));
            let flags: Vec<bool> = partition
                .states
                .iter()
                .map(|state| self.states[state.0 as usize].flagged == Some(true))
                .collect();
            match (
                flags.iter().any(|flagged| *flagged),
                flags.iter().all(|flagged| *flagged),
            ) {
                (false, _) => selected.push(id),
                (true, true) => {}
                (true, false) => {
                    return Err(IntentError::MixedStateFlags {
                        partition: id,
                        intent: intent.to_string(),
                    });
                }
            }
        }
        Ok(selected)
    }
}

/// Why intent selection refused ([`Catalogue::partitions_with_intent`](super::Catalogue::partitions_with_intent)).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IntentError {
    /// The partition's rows use flagged and unflagged states of the
    /// intent's `OBS_MODE`. CASA's selection keeps only the unflagged
    /// rows, and a batch carries no state to tell them apart.
    #[error(
        "partition {} holds rows of flagged and unflagged states with intent {intent}; \
         selecting by intent cannot separate them",
        partition.0
    )]
    MixedStateFlags {
        /// The partition.
        partition: super::PartitionId,
        /// The intent asked for.
        intent: String,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use casa_types::Correlation;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use casa_types::measures::{EpochRef, MjdHighPrec};

    use super::*;
    use crate::{
        BindingTolerance, Catalogue, FieldEntry, FieldId, ObservationEntry, ObservationId,
        PartitionEntry, PartitionId,
    };

    fn record(window: SourceWindow, rest_hz: f64) -> SourceRecord {
        SourceRecord {
            window,
            time: None,
            interval_s: None,
            code: None,
            calibration_group: None,
            direction: None,
            proper_motion_rad_s: None,
            position: None,
            pulsar_id: None,
            num_lines: 1,
            transitions: None,
            rest_frequencies: Some(vec![MFrequency::new(rest_hz, FrequencyRef::LSRK)]),
            systemic_velocities: None,
        }
    }

    fn field(source: Option<u32>) -> FieldEntry {
        FieldEntry {
            name: "f".to_string(),
            phase_centre: MDirection::from_angles(0.0, 0.0, DirectionRef::J2000),
            reference_epoch: None,
            source_name: None,
            source: source.map(SourceId),
            code: None,
            delay_centre: None,
            reference_centre: None,
            flagged: None,
            ephemeris: None,
        }
    }

    fn partition(intents: &[&str], states: &[u32]) -> PartitionEntry {
        PartitionEntry {
            spectral_window: "spw".to_string(),
            window: WindowId(0),
            frequencies_hz: vec![1.0e9],
            frequency_frame: FrequencyRef::LSRK,
            channel_widths_hz: vec![1.0e6],
            correlations: vec![Correlation::XX],
            intents: intents.iter().map(|intent| (*intent).to_string()).collect(),
            observation: ObservationId(0),
            feeds: None,
            fields: vec![FieldId(0)],
            scans: Vec::new(),
            baselines: Vec::new(),
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            data_groups: Vec::new(),
            receptors: BTreeMap::new(),
            states: states.iter().copied().map(StateId).collect(),
            processor: None,
        }
    }

    fn state(obs_mode: &str, flagged: Option<bool>) -> StateEntry {
        StateEntry {
            obs_mode: obs_mode.to_string(),
            cal_k: None,
            load_k: None,
            signal: None,
            reference: None,
            sub_scan: None,
            flagged,
        }
    }

    /// A field's records for a window are that window's own, then those for
    /// every window, in stored order; a field without a source, or a source
    /// silent on the window, has none.
    #[test]
    fn source_records_take_the_windows_own_then_every_windows() {
        let catalogue = Catalogue {
            fields: vec![field(Some(0)), field(None), field(Some(1))],
            sources: vec![
                SourceEntry {
                    name: "M51".to_string(),
                    records: vec![
                        record(SourceWindow::All, 1.0e9),
                        record(SourceWindow::Window(WindowId(1)), 2.0e9),
                        record(SourceWindow::Window(WindowId(0)), 3.0e9),
                    ],
                },
                SourceEntry {
                    name: "M51".to_string(),
                    records: vec![record(SourceWindow::Window(WindowId(0)), 4.0e9)],
                },
            ],
            ..Catalogue::default()
        };
        let rest = |field: u32, window: u32| -> Vec<f64> {
            catalogue
                .source_records(FieldId(field), WindowId(window))
                .iter()
                .map(|record| record.rest_frequencies.as_ref().expect("lines")[0].hz())
                .collect()
        };
        assert_eq!(rest(0, 1), [2.0e9, 1.0e9]);
        assert_eq!(rest(0, 0), [3.0e9, 1.0e9]);
        assert_eq!(rest(0, 2), [1.0e9], "only the record for every window");
        assert!(rest(1, 0).is_empty(), "no source");
        assert_eq!(rest(2, 0), [4.0e9], "the other source of one name");
        assert!(rest(2, 1).is_empty(), "nothing for that window");
    }

    /// Intent selection matches one OBS_MODE element exactly and leaves out
    /// partitions whose states are all flagged; a partition mixing flagged
    /// and unflagged states is refused, and one without states (MSv4) is
    /// chosen by its intents.
    #[test]
    fn intent_selection_leaves_out_flagged_states_and_refuses_a_mixture() {
        let mut catalogue = Catalogue {
            states: vec![
                state("OBSERVE_TARGET#ON_SOURCE", Some(false)),
                state("OBSERVE_TARGET#ON_SOURCE", Some(true)),
                state("CALIBRATE_PHASE#ON_SOURCE", None),
            ],
            partitions: vec![
                partition(&["OBSERVE_TARGET#ON_SOURCE"], &[0]),
                partition(&["OBSERVE_TARGET#ON_SOURCE"], &[1]),
                partition(&["CALIBRATE_PHASE#ON_SOURCE"], &[2]),
                partition(&["OBSERVE_TARGET#ON_SOURCE"], &[]),
                partition(&[], &[]),
            ],
            ..Catalogue::default()
        };
        assert_eq!(
            catalogue.partitions_with_intent("OBSERVE_TARGET#ON_SOURCE"),
            Ok(vec![PartitionId(0), PartitionId(3)])
        );
        assert_eq!(
            catalogue.partitions_with_intent("CALIBRATE_PHASE#ON_SOURCE"),
            Ok(vec![PartitionId(2)])
        );
        assert_eq!(
            catalogue.partitions_with_intent("OBSERVE_TARGET"),
            Ok(Vec::new()),
            "an element matches whole"
        );
        // A state's stored OBS_MODE is matched element by element, untrimmed,
        // as CASA's literal matcher does, whatever the intents say.
        catalogue.states[2].obs_mode = "CALIBRATE_PHASE#ON_SOURCE, CALIBRATE_BANDPASS".to_string();
        catalogue.partitions[2].intents = vec![
            "CALIBRATE_PHASE#ON_SOURCE".to_string(),
            "CALIBRATE_BANDPASS".to_string(),
        ];
        assert_eq!(
            catalogue.partitions_with_intent("CALIBRATE_BANDPASS"),
            Ok(Vec::new()),
            "\" CALIBRATE_BANDPASS\" is not \"CALIBRATE_BANDPASS\""
        );
        assert_eq!(
            catalogue.partitions_with_intent(" CALIBRATE_BANDPASS"),
            Ok(vec![PartitionId(2)])
        );
        assert_eq!(
            catalogue.partitions_with_intent("CALIBRATE_PHASE#ON_SOURCE"),
            Ok(vec![PartitionId(2)])
        );
        catalogue.partitions[0].states = vec![StateId(0), StateId(1)];
        assert_eq!(
            catalogue.partitions_with_intent("OBSERVE_TARGET#ON_SOURCE"),
            Err(IntentError::MixedStateFlags {
                partition: PartitionId(0),
                intent: "OBSERVE_TARGET#ON_SOURCE".to_string(),
            })
        );
    }

    /// Window metadata one store leaves unstated does not keep two
    /// partitions from binding; members both state must agree.
    #[test]
    fn windows_bind_when_their_stated_members_agree() {
        let full = WindowEntry {
            name: "spw".to_string(),
            reference_frequency: Some(MFrequency::new(1.0e9, FrequencyRef::LSRK)),
            total_bandwidth_hz: Some(1.0e6),
            if_chain: Some(0),
            flagged: Some(false),
            ..WindowEntry::default()
        };
        let sparse = WindowEntry {
            name: "renamed".to_string(),
            reference_frequency: Some(MFrequency::new(1.0e9, FrequencyRef::LSRK)),
            ..WindowEntry::default()
        };
        let other = WindowEntry {
            reference_frequency: Some(MFrequency::new(1.1e9, FrequencyRef::LSRK)),
            ..sparse.clone()
        };
        assert!(full.compatible(&sparse) && sparse.compatible(&full));
        assert!(!full.compatible(&other));
        let catalogue = |window: WindowEntry| Catalogue {
            fields: vec![field(None)],
            observations: vec![ObservationEntry::default()],
            partitions: vec![partition(&[], &[])],
            windows: vec![window],
            ..Catalogue::default()
        };
        let tolerance = BindingTolerance::EXACT;
        assert!(
            catalogue(full.clone())
                .bind_to(&catalogue(sparse), &tolerance)
                .is_ok()
        );
        assert!(
            catalogue(full)
                .bind_to(&catalogue(other), &tolerance)
                .is_err()
        );
    }
}
