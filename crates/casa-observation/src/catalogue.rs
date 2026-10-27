// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue of an opened observation: its antennas, fields, scans,
//! observations and partitions, with identities scoped to that opening.
//!
//! Every per-partition constant a batch needs (channel frequencies and
//! their frame, correlation products, the baseline table, the time system,
//! the data groups with their units and UVW frame) lives here once; a batch
//! links to it by [`PartitionId`] and copies none of it.

use std::collections::BTreeMap;

use casa_types::Correlation;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::position::MPosition;
use casa_types::measures::{EpochRef, MEpoch, MjdHighPrec};
use casa_types::quanta::Quantity;

use crate::ObservationError;

/// An antenna of one opened observation.
///
/// The number is an index into [`Catalogue::antennas`], valid only for the
/// observation that issued it. It is not a casacore `ANTENNA` row number or
/// an MSv4 antenna name, though a backend may happen to number antennas the
/// way its store does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AntennaId(pub u32);

/// A field of one opened observation: an index into [`Catalogue::fields`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(pub u32);

/// A scan of one opened observation: an index into [`Catalogue::scans`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScanId(pub u32);

/// An observation (an observing session) of one opened store: an index into
/// [`Catalogue::observations`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationId(pub u32);

/// A partition of one opened observation: an index into
/// [`Catalogue::partitions`]. A partition is one store unit (an MSv4
/// dataset; the MSv2 rows that one such dataset would hold), described by
/// its content ([`PartitionEntry`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u32);

/// An ordered antenna pair. The order is the stored one: swapping it
/// conjugates the visibility, negates UVW and swaps the cross hands, so no
/// layer reorders it in passing. Feeds are a partition key
/// ([`PartitionEntry::feeds`]), not part of the baseline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedBaseline {
    /// First antenna (MSv2 `ANTENNA1`, MSv4 `baseline_antenna1_name`).
    pub antenna1: AntennaId,
    /// Second antenna.
    pub antenna2: AntennaId,
}

impl OrderedBaseline {
    /// Whether both antennas are the same: an autocorrelation.
    #[must_use]
    pub fn is_auto(self) -> bool {
        self.antenna1 == self.antenna2
    }
}

/// What a data group's visibilities are measured in.
///
/// Jy only where the store says so; anything else is kept as the store
/// spells it and never assumed to be a calibrated flux density.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum VisibilityUnits {
    /// Janskys.
    Jansky,
    /// A unit the store names but this contract does not interpret, kept
    /// verbatim (XRADIO's converter writes `"unkown"`).
    Declared(String),
    /// The store declares none (MSv2 `DATA` has no unit keyword).
    #[default]
    Undeclared,
}

/// One antenna of the catalogue, in native Measures and Quanta types: the
/// adapter checks the stored units and frame once, and no consumer handles
/// raw coordinates.
#[derive(Clone, Debug)]
pub struct AntennaEntry {
    /// Antenna name as stored (`ANTENNA::NAME`; in MSv4 the
    /// `antenna_name` coordinate, which XRADIO forms as `NAME_STATION`).
    pub name: String,
    /// Station or pad name.
    pub station: String,
    /// Position (geocentric ITRF in both trial stores).
    pub position: MPosition,
    /// Mount type as stored (`ALT-AZ`).
    pub mount: String,
    /// Dish diameter.
    pub dish_diameter: Quantity,
}

/// An antenna's receptors in one partition's spectral window: MSv2 `FEED`
/// rows of the window (or of every window) and the partition's feed, MSv4
/// the dataset's `antenna_xds`.
#[derive(Clone, Debug, PartialEq)]
pub struct Receptors {
    /// Polarization types in receptor order (`X`, `Y`).
    pub types: Vec<String>,
    /// Receptor angles, in the same order.
    pub angles: Vec<Quantity>,
}

/// One field of the catalogue.
#[derive(Clone, Debug)]
pub struct FieldEntry {
    /// Field name as stored. XRADIO appends the MSv2 field number
    /// (`trial_0`); the catalogue keeps what the store holds.
    pub name: String,
    /// The phase-tracking centre, in its stored frame. Fields of one
    /// opening share the frame of their data groups' UVW
    /// ([`DataGroupEntry::uvw_frame`]).
    pub phase_centre: MDirection,
    /// The epoch at which [`Self::phase_centre`] is stated, which a frame
    /// that moves with time (apparent, topocentric) needs to be converted:
    /// MSv2 `FIELD.TIME`. `None` when the store states none, as MSv4, whose
    /// phase centres are in celestial frames.
    pub reference_epoch: Option<MEpoch>,
    /// The source observed, when the store names one.
    pub source_name: Option<String>,
}

/// One scan of the catalogue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanEntry {
    /// The scan's name as stored: MSv4 `scan_name`; for MSv2 the decimal
    /// `SCAN_NUMBER`, which is how XRADIO names it.
    pub name: String,
    /// The casacore scan number: MSv2 `SCAN_NUMBER`, or an MSv4 scan name
    /// that is a decimal integer (XRADIO's convention); `None` otherwise.
    pub number: Option<i32>,
    /// Scan intents (MSv2 `STATE::OBS_MODE` of the scan's rows; MSv4
    /// `scan_name.scan_intents`).
    pub intents: Vec<String>,
}

/// One observing session (MSv2 `OBSERVATION` row; MSv4 `observation_info`).
///
/// The archive identifiers are MSv4's; an MSv2 holds them only where an
/// importer recorded them, and the adapter reads them where XRADIO's
/// converter does. A session lacking one is not distinguished by it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObservationEntry {
    /// Telescope name (`TELESCOPE_NAME`; MSv4 `antenna_xds.telescope_name`).
    pub telescope: String,
    /// Observers (`OBSERVER`).
    pub observers: Vec<String>,
    /// Project code (`PROJECT`; MSv4 `project_UID`).
    pub project: String,
    /// The execution block's archive UID (MSv4 `execution_block_UID`; in
    /// MSv2 the `ExecBlock` UID of `OBSERVATION::SCHEDULE`).
    pub execution_block: Option<String>,
    /// The observing session's reference (MSv4 `session_reference_UID`;
    /// MSv2 keeps it only in the `ASDM_EXECBLOCK` table, which the trial
    /// adapter does not read).
    pub session_reference: Option<String>,
    /// The scheduling block's archive UID (MSv4 `scheduling_block_UID`; in
    /// MSv2 the `SchedulingBlock` UID of `OBSERVATION::SCHEDULE`).
    pub scheduling_block: Option<String>,
}

impl ObservationEntry {
    /// Whether `other` can be the same session: the same telescope, project
    /// and observers, and the same archive identifier wherever both state
    /// one.
    #[must_use]
    pub fn same_session(&self, other: &Self) -> bool {
        let agree = |a: &Option<String>, b: &Option<String>| match (a, b) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        };
        self.telescope == other.telescope
            && self.project == other.project
            && self.observers == other.observers
            && agree(&self.execution_block, &other.execution_block)
            && agree(&self.session_reference, &other.session_reference)
            && agree(&self.scheduling_block, &other.scheduling_block)
    }
}

/// The frame a data group's UVW are in, and the label the store wrote.
///
/// The batch's [`UvwView`](casa_types::measures::UvwView) carries only the
/// effective frame: an adapter resolves the store's convention once, at
/// opening, and refuses a store whose label it cannot resolve. For MSv2
/// that convention is CASA's: `UVW` is computed in the phase centre's frame
/// whatever its `MEASINFO` says (simulators and fillers commonly label it
/// `ITRF`, the casacore default; `fixvis` notes the label is usually wrong,
/// and XRADIO's converter relabels it the same way). MSv4 states the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UvwFrame {
    /// The frame the UVW values are in.
    pub effective: DirectionRef,
    /// The frame the store's label names, when it names one.
    pub stored_label: Option<DirectionRef>,
}

/// One named data group of a partition: which stored variables hold its
/// visibilities, flags, weights and UVW, and their units and frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataGroupEntry {
    /// The group's name (`base` in MSv4; the trial names MSv2's `DATA`
    /// association `base` too).
    pub name: String,
    /// The visibility variable or column.
    pub visibilities: String,
    /// The flag variable or column.
    pub flags: String,
    /// The weight variable or column.
    pub weights: String,
    /// The UVW variable or column.
    pub uvw: String,
    /// The visibilities' units.
    pub units: VisibilityUnits,
    /// The UVW frame.
    pub uvw_frame: UvwFrame,
}

/// One partition: a store unit and the content that defines it.
///
/// The content keys (spectral window, correlation products, intents,
/// observation, feed pair) and the fields and scans the partition's
/// samples visit are what two stores of one observation agree on; the MSv2
/// adapter partitions rows the way XRADIO's converter does by default
/// (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID` × `EPHEMERIS_ID`, and the
/// feed pair, which MSv4 has no axis for), so the same observation opens as
/// the same partition set from either store. Cross-store matching
/// ([`Catalogue::bind_to`]) compares content, never list positions.
#[derive(Clone, Debug)]
pub struct PartitionEntry {
    /// The spectral window's name as stored.
    pub spectral_window: String,
    /// Channel centres in hertz.
    pub frequencies_hz: Vec<f64>,
    /// Their spectral frame.
    pub frequency_frame: FrequencyRef,
    /// Signed channel widths in hertz.
    pub channel_widths_hz: Vec<f64>,
    /// The correlation products, in stored order.
    pub correlations: Vec<Correlation>,
    /// The scan intents of every sample (MSv2 `STATE::OBS_MODE`, MSv4
    /// `scan_name.scan_intents`), sorted.
    pub intents: Vec<String>,
    /// The observing session.
    pub observation: ObservationId,
    /// The feed pair (MSv2 `FEED1`, `FEED2`) of every sample; `None` for a
    /// store without a feed axis (MSv4).
    pub feeds: Option<[u32; 2]>,
    /// The fields the partition's samples are phased to, over every data
    /// group, sorted.
    pub fields: Vec<FieldId>,
    /// The scans its samples belong to, sorted; empty for a store that
    /// names none (an MSv4 dataset without `scan_name`).
    pub scans: Vec<ScanId>,
    /// The baseline table: sample `t × baselines.len() + b` of every batch
    /// is baseline `b`.
    pub baselines: Vec<OrderedBaseline>,
    /// Time scale of the partition's times and time centroids.
    pub time_scale: EpochRef,
    /// Origin the partition's time offsets count from (MJD 0 for MSv2,
    /// the Unix epoch for MSv4 `format: unix`).
    pub time_origin: MjdHighPrec,
    /// The partition's data groups.
    pub data_groups: Vec<DataGroupEntry>,
    /// The receptors of each antenna the baselines name, in this
    /// partition's spectral window; an antenna the store states none for
    /// is absent.
    pub receptors: BTreeMap<AntennaId, Receptors>,
}

impl PartitionEntry {
    /// Data group `name`, with its index in [`Self::data_groups`].
    #[must_use]
    pub fn data_group(&self, name: &str) -> Option<(usize, &DataGroupEntry)> {
        self.data_groups
            .iter()
            .enumerate()
            .find(|(_, group)| group.name == name)
    }

    /// Where `correlation` is along this partition's correlation axis.
    #[must_use]
    pub fn correlation_index(&self, correlation: Correlation) -> Option<usize> {
        self.correlations.iter().position(|c| *c == correlation)
    }

    /// Whether `other` has the same spectral and product content: the same
    /// channel frequencies and widths (bit for bit) in the same frame, the
    /// same correlation products in the same order and the same intents.
    /// Names, which converters rewrite, take no part; the keys that name
    /// other entities (observation, fields, scans) and the feed pair are
    /// compared by [`Catalogue::bind_to`] through the binding.
    #[must_use]
    pub fn same_content(&self, other: &Self) -> bool {
        let bitwise = |a: &[f64], b: &[f64]| {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.to_bits() == b.to_bits())
        };
        self.frequency_frame == other.frequency_frame
            && bitwise(&self.frequencies_hz, &other.frequencies_hz)
            && bitwise(&self.channel_widths_hz, &other.channel_widths_hz)
            && self.correlations == other.correlations
            && self.intents == other.intents
    }
}

/// What an opened observation holds, without its payload.
#[derive(Clone, Debug, Default)]
pub struct Catalogue {
    /// Antennas; [`AntennaId`] indexes them.
    pub antennas: Vec<AntennaEntry>,
    /// Fields; [`FieldId`] indexes them.
    pub fields: Vec<FieldEntry>,
    /// Scans; [`ScanId`] indexes them.
    pub scans: Vec<ScanEntry>,
    /// Observing sessions; [`ObservationId`] indexes them.
    pub observations: Vec<ObservationEntry>,
    /// Partitions; [`PartitionId`] indexes them.
    pub partitions: Vec<PartitionEntry>,
}

/// How far two catalogues' antennas and fields may differ and still bind
/// ([`Catalogue::bind_to`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BindingTolerance {
    /// Largest difference in any antenna position component, in metres.
    pub position_m: f64,
    /// Largest difference in a phase centre's longitude or latitude, in
    /// radians.
    pub direction_rad: f64,
}

impl BindingTolerance {
    /// Bit-for-bit agreement.
    pub const EXACT: Self = Self {
        position_m: 0.0,
        direction_rad: 0.0,
    };
}

/// One catalogue's entities as another catalogue numbers them: what
/// applying a calibration solved on one store to another needs
/// (`CalibrationBinding` in the design). Index `i` of each list is entity
/// `i` of the catalogue [`Catalogue::bind_to`] was called on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Each antenna's identity in the other catalogue.
    pub antennas: Vec<AntennaId>,
    /// Each field's identity in the other catalogue.
    pub fields: Vec<FieldId>,
    /// Each scan's identity in the other catalogue.
    pub scans: Vec<ScanId>,
    /// Each observing session's identity in the other catalogue.
    pub observations: Vec<ObservationId>,
    /// Each partition's identity in the other catalogue.
    pub partitions: Vec<PartitionId>,
}

impl Catalogue {
    /// Partition `id`.
    ///
    /// # Panics
    ///
    /// If `id` was not issued by this catalogue.
    #[must_use]
    pub fn partition(&self, id: PartitionId) -> &PartitionEntry {
        &self.partitions[id.0 as usize]
    }

    /// Observing session `id`.
    ///
    /// # Panics
    ///
    /// If `id` was not issued by this catalogue.
    #[must_use]
    pub fn observation(&self, id: ObservationId) -> &ObservationEntry {
        &self.observations[id.0 as usize]
    }

    /// Data group `name` of partition `id`, if it has one.
    #[must_use]
    pub fn data_group(&self, id: PartitionId, name: &str) -> Option<&DataGroupEntry> {
        self.partitions
            .get(id.0 as usize)?
            .data_group(name)
            .map(|(_, group)| group)
    }

    /// Every partition.
    #[must_use]
    pub fn partition_ids(&self) -> Vec<PartitionId> {
        (0..self.partitions.len())
            .map(|index| PartitionId(u32::try_from(index).expect("few partitions")))
            .collect()
    }

    /// Bind this catalogue's entities to `other`'s by content: antennas by
    /// station and position within `tolerance`, fields by phase centre
    /// (frame and angles within `tolerance`), scans by casacore number (by
    /// name when a scan has none), observing sessions by
    /// [`ObservationEntry::same_session`], and partitions by
    /// [`PartitionEntry::same_content`] together with the bound session, the
    /// bound sets of fields and of scans (scans when both stores name
    /// them) and the feed pair (when both stores have one). Names and list
    /// positions decide nothing: converters rename antennas and fields.
    ///
    /// The binding is one to one: a key one store lacks (MSv4 has no feed
    /// axis; an MSv2 may hold no archive identifiers) does not distinguish,
    /// but two entities this catalogue keeps apart never bind to one entity
    /// of `other`.
    ///
    /// # Errors
    ///
    /// When an entity of this catalogue matches none of `other`'s, or more
    /// than one, or two of them match the same one: an ambiguity is
    /// refused, never resolved by position.
    pub fn bind_to(
        &self,
        other: &Self,
        tolerance: &BindingTolerance,
    ) -> Result<Binding, ObservationError> {
        let within = |a: f64, b: f64, tolerance: f64| (a - b).abs() <= tolerance;
        let antennas = bind_each("antenna", &self.antennas, &other.antennas, |a, b| {
            a.station == b.station
                && a.position.refer() == b.position.refer()
                && a.position
                    .values()
                    .iter()
                    .zip(b.position.values())
                    .all(|(x, y)| within(*x, y, tolerance.position_m))
        })?;
        let fields = bind_each("field", &self.fields, &other.fields, |a, b| {
            let (al, ab) = a.phase_centre.as_angles();
            let (bl, bb) = b.phase_centre.as_angles();
            a.phase_centre.refer() == b.phase_centre.refer()
                && within(al, bl, tolerance.direction_rad)
                && within(ab, bb, tolerance.direction_rad)
        })?;
        let scans = bind_each("scan", &self.scans, &other.scans, |a, b| match a.number {
            Some(number) => b.number == Some(number),
            None => b.number.is_none() && a.name == b.name,
        })?;
        let observations = bind_each(
            "observation",
            &self.observations,
            &other.observations,
            ObservationEntry::same_session,
        )?;
        let partitions = bind_each("partition", &self.partitions, &other.partitions, |a, b| {
            a.same_content(b)
                && observations.get(a.observation.0 as usize) == Some(&b.observation.0)
                && (a.feeds.is_none() || b.feeds.is_none() || a.feeds == b.feeds)
                && same_set(&a.fields, &b.fields, |field| field.0, &fields)
                && (a.scans.is_empty()
                    || b.scans.is_empty()
                    || same_set(&a.scans, &b.scans, |scan| scan.0, &scans))
        })?;
        Ok(Binding {
            antennas: antennas.into_iter().map(AntennaId).collect(),
            fields: fields.into_iter().map(FieldId).collect(),
            scans: scans.into_iter().map(ScanId).collect(),
            observations: observations.into_iter().map(ObservationId).collect(),
            partitions: partitions.into_iter().map(PartitionId).collect(),
        })
    }
}

/// Whether identities `ours`, renumbered through `binding`, are the sorted
/// set `theirs`.
fn same_set<T: Copy>(ours: &[T], theirs: &[T], id: fn(T) -> u32, binding: &[u32]) -> bool {
    let renumbered: Option<Vec<u32>> = ours
        .iter()
        .map(|ours| binding.get(id(*ours) as usize).copied())
        .collect();
    renumbered.is_some_and(|mut renumbered| {
        renumbered.sort_unstable();
        renumbered
            .into_iter()
            .eq(theirs.iter().map(|theirs| id(*theirs)))
    })
}

/// The index in `others` of the one entry `matches` each of `entries`, no
/// two of them the same.
fn bind_each<T>(
    what: &str,
    entries: &[T],
    others: &[T],
    matches: impl Fn(&T, &T) -> bool,
) -> Result<Vec<u32>, ObservationError> {
    let bound: Vec<u32> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let mut found = others
                .iter()
                .enumerate()
                .filter(|(_, other)| matches(entry, other))
                .map(|(index, _)| index);
            match (found.next(), found.next()) {
                (Some(one), None) => Ok(u32::try_from(one).expect("few entries")),
                (None, _) => Err(ObservationError::InvalidRequest {
                    reason: format!("{what} {index} matches nothing in the other catalogue"),
                }),
                (Some(_), Some(_)) => Err(ObservationError::InvalidRequest {
                    reason: format!("{what} {index} matches more than one in the other catalogue"),
                }),
            }
        })
        .collect::<Result<_, _>>()?;
    let mut first = std::collections::BTreeMap::new();
    for (index, other) in bound.iter().enumerate() {
        if let Some(earlier) = first.insert(*other, index) {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "{what}s {earlier} and {index} both match {what} {other} of the other \
                     catalogue, which does not keep them apart"
                ),
            });
        }
    }
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(project: &str, execution_block: Option<&str>) -> ObservationEntry {
        ObservationEntry {
            telescope: "VLA".to_string(),
            observers: vec!["someone".to_string()],
            project: project.to_string(),
            execution_block: execution_block.map(str::to_string),
            ..ObservationEntry::default()
        }
    }

    fn partition(observation: u32, fields: &[u32], scans: &[u32]) -> PartitionEntry {
        PartitionEntry {
            spectral_window: "spw".to_string(),
            frequencies_hz: vec![1.0e9, 1.1e9],
            frequency_frame: FrequencyRef::LSRK,
            channel_widths_hz: vec![1.0e8; 2],
            correlations: vec![Correlation::XX, Correlation::YY],
            intents: vec!["TARGET".to_string()],
            observation: ObservationId(observation),
            feeds: None,
            fields: fields.iter().copied().map(FieldId).collect(),
            scans: scans.iter().copied().map(ScanId).collect(),
            baselines: Vec::new(),
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            data_groups: Vec::new(),
            receptors: BTreeMap::new(),
        }
    }

    fn field(longitude: f64) -> FieldEntry {
        FieldEntry {
            name: format!("f{longitude}"),
            phase_centre: MDirection::from_angles(longitude, 0.0, DirectionRef::J2000),
            reference_epoch: None,
            source_name: None,
        }
    }

    fn scan(number: i32) -> ScanEntry {
        ScanEntry {
            name: number.to_string(),
            number: Some(number),
            intents: Vec::new(),
        }
    }

    /// Two fields, two scans, `observations` and `partitions`.
    fn observed(observations: Vec<ObservationEntry>, partitions: Vec<PartitionEntry>) -> Catalogue {
        Catalogue {
            fields: vec![field(0.0), field(0.1)],
            scans: vec![scan(1), scan(2)],
            observations,
            partitions,
            ..Catalogue::default()
        }
    }

    fn binds(a: &Catalogue, b: &Catalogue) -> Result<Vec<PartitionId>, ObservationError> {
        a.bind_to(b, &BindingTolerance::EXACT)
            .map(|binding| binding.partitions)
    }

    /// Equal spectral setups are told apart by everything else a partition
    /// is keyed by: its session, the fields and scans it visits, its feed
    /// pair where both stores have one, and its channel widths. A key one
    /// store lacks does not distinguish, and two entities never bind to one.
    #[test]
    fn partitions_bind_by_session_fields_scans_feeds_and_widths() {
        let one = observed(vec![session("p", None)], vec![partition(0, &[0], &[0])]);
        assert_eq!(binds(&one, &one).expect("itself"), [PartitionId(0)]);

        // Same spectrum, another session.
        let other_project = observed(vec![session("q", None)], vec![partition(0, &[0], &[0])]);
        assert!(binds(&one, &other_project).is_err(), "another project");
        let two = observed(
            vec![session("p", Some("uid://A")), session("p", Some("uid://B"))],
            vec![partition(0, &[0], &[0]), partition(1, &[0], &[0])],
        );
        assert_eq!(
            binds(&two, &two).expect("sessions apart"),
            [PartitionId(0), PartitionId(1)]
        );
        let only_b = observed(
            vec![session("p", Some("uid://B"))],
            vec![partition(0, &[0], &[0])],
        );
        assert_eq!(
            binds(&only_b, &two).expect("a subset binds to its own session"),
            [PartitionId(1)]
        );
        assert!(
            binds(&two, &only_b).is_err(),
            "session A has no counterpart"
        );
        assert!(
            binds(&two, &one).is_err(),
            "two sessions told apart by their UIDs would collapse onto one"
        );
        assert_eq!(
            binds(&one, &only_b).expect("a UID one store lacks does not distinguish"),
            [PartitionId(0)]
        );

        // Same session and spectrum, split by field and by scan.
        let by_field = observed(
            vec![session("p", None)],
            vec![partition(0, &[0], &[0, 1]), partition(0, &[1], &[0, 1])],
        );
        let both_fields = observed(
            vec![session("p", None)],
            vec![partition(0, &[0, 1], &[0, 1])],
        );
        assert!(binds(&by_field, &both_fields).is_err(), "field split");
        assert!(binds(&both_fields, &by_field).is_err(), "field split");
        let field_one = observed(vec![session("p", None)], vec![partition(0, &[1], &[0, 1])]);
        assert_eq!(
            binds(&field_one, &by_field).expect("asymmetric subset"),
            [PartitionId(1)]
        );
        let by_scan = observed(
            vec![session("p", None)],
            vec![partition(0, &[0], &[0]), partition(0, &[0], &[1])],
        );
        assert_eq!(
            binds(&by_scan, &by_scan).expect("scans apart"),
            [PartitionId(0), PartitionId(1)]
        );
        let unscanned = observed(vec![session("p", None)], vec![partition(0, &[0], &[])]);
        assert!(
            binds(&by_scan, &unscanned).is_err(),
            "two scan partitions would collapse onto one without scans"
        );

        // Feeds distinguish only where both stores have them.
        let feeds = |pair: Option<[u32; 2]>| {
            let mut entry = partition(0, &[0], &[0]);
            entry.feeds = pair;
            entry
        };
        let feed_split = observed(
            vec![session("p", None)],
            vec![feeds(Some([0, 0])), feeds(Some([1, 1]))],
        );
        assert_eq!(
            binds(&feed_split, &feed_split).expect("feeds apart"),
            [PartitionId(0), PartitionId(1)]
        );
        assert!(
            binds(&feed_split, &one).is_err(),
            "two feed pairs would collapse onto a store without feeds"
        );
        let feed_one = observed(vec![session("p", None)], vec![feeds(Some([1, 1]))]);
        assert_eq!(
            binds(&feed_one, &feed_split).expect("feed"),
            [PartitionId(1)]
        );
        assert_eq!(
            binds(&feed_one, &one).expect("no feed axis"),
            [PartitionId(0)]
        );

        // Channel widths, bit for bit.
        let mut narrow = observed(vec![session("p", None)], vec![partition(0, &[0], &[0])]);
        narrow.partitions[0].channel_widths_hz[1] = 0.9e8;
        assert!(binds(&narrow, &one).is_err(), "another channel width");
    }

    fn antenna(station: &str, x: f64) -> AntennaEntry {
        AntennaEntry {
            name: station.to_string(),
            station: station.to_string(),
            position: MPosition::new_itrf(x, 0.0, 0.0),
            mount: "ALT-AZ".to_string(),
            dish_diameter: Quantity::new(25.0, "m").expect("quantity"),
        }
    }

    #[test]
    fn binding_matches_content_and_refuses_ambiguity() {
        let mut a = Catalogue {
            antennas: vec![antenna("W1", 1.0), antenna("W2", 2.0)],
            ..Catalogue::default()
        };
        let mut b = Catalogue {
            antennas: vec![antenna("W2", 2.0), antenna("W1", 1.0 + 1e-4)],
            ..Catalogue::default()
        };
        b.antennas[0].name = "renamed".to_string();
        let tolerance = BindingTolerance {
            position_m: 1e-3,
            direction_rad: 0.0,
        };
        let binding = a.bind_to(&b, &tolerance).expect("binds");
        assert_eq!(binding.antennas, vec![AntennaId(1), AntennaId(0)]);
        assert!(
            a.bind_to(&b, &BindingTolerance::EXACT).is_err(),
            "1e-4 m off"
        );
        b.antennas.push(antenna("W1", 1.0));
        assert!(
            a.bind_to(&b, &tolerance).is_err(),
            "two W1 within tolerance"
        );
        a.antennas.push(antenna("W9", 9.0));
        assert!(a.bind_to(&b, &tolerance).is_err(), "W9 matches nothing");
    }
}
