// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's catalogue: subtables read once at opening, and the
//! measure and unit keywords checked once so that every native view the
//! batches carry has the reference and unit its type claims.

use std::collections::{BTreeMap, BTreeSet};

use casa_observation::{
    AntennaEntry, Correlation, FieldEntry, ObservationEntry, ObservationError, Receptors,
    ScanEntry, ScanId, UvwFrame, VisibilityUnits,
};
use casa_tables::Table;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::position::MPosition;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::quanta::Quantity;
use casa_types::{ArrayValue, ScalarValue, Value};

use super::{invalid, storage};
use crate::columns::direction_columns::DirectionColumn;
use crate::derived::engine::resolve_direction_reference_selected;
use crate::{MeasurementSet, SubtableId};

/// A column's `MEASINFO` record, if it has one.
fn measinfo<'t>(table: &'t Table, column: &str) -> Option<&'t casa_types::RecordValue> {
    match table.column_keywords(column)?.get("MEASINFO")? {
        Value::Record(record) => Some(record),
        _ => None,
    }
}

/// A column's fixed `MEASINFO.Ref`, if it names one. A variable reference
/// (`VarRefCol`) is refused: the trial's views carry one reference per
/// batch.
pub(crate) fn measure_reference(
    table: &Table,
    column: &str,
) -> Result<Option<String>, ObservationError> {
    let Some(record) = measinfo(table, column) else {
        return Ok(None);
    };
    if record.get("VarRefCol").is_some() {
        return Err(ObservationError::Unsupported {
            what: "an MSv2 measure column with a variable reference (VarRefCol)",
        });
    }
    Ok(match record.get("Ref") {
        Some(Value::Scalar(ScalarValue::String(reference))) => Some(reference.clone()),
        _ => None,
    })
}

/// Require a column's `QuantumUnits`, when present, to be `expected` in
/// every position. An absent keyword is the MeasurementSet definition's
/// unit, which is `expected`.
pub(crate) fn require_units(
    table: &Table,
    column: &str,
    expected: &str,
) -> Result<(), ObservationError> {
    let Some(units) = quantum_units(table, column) else {
        return Ok(());
    };
    if units.iter().all(|unit| unit == expected) {
        Ok(())
    } else {
        Err(invalid(format!(
            "{column} is in {units:?}; the trial adapter reads only {expected} and does not convert"
        )))
    }
}

fn quantum_units(table: &Table, column: &str) -> Option<Vec<String>> {
    match table.column_keywords(column)?.get("QuantumUnits")? {
        Value::Array(ArrayValue::String(units)) => Some(units.iter().cloned().collect()),
        Value::Scalar(ScalarValue::String(unit)) => Some(vec![unit.clone()]),
        _ => None,
    }
}

/// What visibility column `column`'s `QuantumUnits` declares: janskys
/// only when it says `Jy`.
pub(super) fn data_units(table: &Table, column: &str) -> VisibilityUnits {
    match quantum_units(table, column).as_deref() {
        None | Some([]) => VisibilityUnits::Undeclared,
        Some([unit, ..]) if unit == "Jy" => VisibilityUnits::Jansky,
        Some([unit, ..]) => VisibilityUnits::Declared(unit.clone()),
    }
}

/// The time scale of MAIN `TIME` and `TIME_CENTROID`, in seconds. The
/// MeasurementSet definition's default reference is UTC; another fixed
/// reference is kept, a variable one refused, and the two columns must
/// agree.
pub(super) fn time_scale(main: &Table) -> Result<EpochRef, ObservationError> {
    let mut scales = Vec::new();
    for column in ["TIME", "TIME_CENTROID"] {
        require_units(main, column, "s")?;
        let scale = match measure_reference(main, column)? {
            Some(name) => name
                .parse::<EpochRef>()
                .map_err(|_| invalid(format!("unknown {column} reference {name}")))?,
            None => EpochRef::UTC,
        };
        scales.push(scale);
    }
    for column in ["INTERVAL", "EXPOSURE"] {
        require_units(main, column, "s")?;
    }
    if scales[0] == scales[1] {
        Ok(scales[0])
    } else {
        Err(ObservationError::Unsupported {
            what: "MSv2 TIME and TIME_CENTROID in different time scales",
        })
    }
}

/// The `FIELD` subtable as the catalogue needs it.
pub(super) struct Fields {
    pub(super) entries: Vec<FieldEntry>,
    /// Each field's `EPHEMERIS_ID`, when the optional column is present.
    pub(super) ephemerides: Vec<Option<i32>>,
    /// The UVW convention the phase centres' frame resolves.
    pub(super) uvw_frame: UvwFrame,
}

/// Each `FIELD` row's `PHASE_DIR` frame: fixed for the column, or
/// variable per row (`VarRefCol`, integer or string codes, as ALMA's
/// `PhaseDir_Ref`).
pub(crate) fn phase_frames(table: &Table) -> Result<Vec<DirectionRef>, ObservationError> {
    require_units(table, "PHASE_DIR", "rad")?;
    (0..table.row_count())
        .map(|row| {
            resolve_direction_reference_selected(table, "FIELD", "PHASE_DIR", row).map_err(storage)
        })
        .collect()
}

/// The fields, their ephemerides, and the UVW convention.
pub(super) fn fields_and_uvw(ms: &MeasurementSet) -> Result<Fields, ObservationError> {
    let field = ms.field().map_err(storage)?;
    let field_table = ms
        .subtable(SubtableId::Field)
        .ok_or_else(|| invalid("no FIELD subtable"))?;
    // The fields share one frame: a data group has one UVW frame, and MSv4
    // one phase-centre frame. Without fields it is J2000, the definition's
    // default.
    let frames = phase_frames(field_table)?;
    let phase_frame = frames.first().copied().unwrap_or(DirectionRef::J2000);
    if frames.iter().any(|frame| *frame != phase_frame) {
        return Err(ObservationError::Unsupported {
            what: "MSv2 fields whose phase centres are in different frames",
        });
    }
    require_units(field_table, "TIME", "s")?;
    let epoch_scale = match measure_reference(field_table, "TIME")? {
        Some(name) => name
            .parse::<EpochRef>()
            .map_err(|_| invalid(format!("unknown FIELD TIME reference {name}")))?,
        None => EpochRef::UTC,
    };
    let phase_dir = DirectionColumn::phase_dir(field_table);
    let fields = (0..field.row_count())
        .map(|row| {
            let (longitude, latitude) = phase_dir.get_direction(row).map_err(storage)?.as_angles();
            let source = field.source_id(row).map_err(storage)?;
            let time_s = field.time(row).map_err(storage)?;
            Ok(FieldEntry {
                name: field.name(row).map_err(storage)?,
                phase_centre: MDirection::from_angles(longitude, latitude, frames[row]),
                reference_epoch: Some(MEpoch::from_mjd(time_s / 86_400.0, epoch_scale)),
                source_name: source_name(ms, source)?,
            })
        })
        .collect::<Result<_, ObservationError>>()?;
    let ephemerides = (0..field.row_count())
        .map(|row| field.ephemeris_id(row).map_err(storage))
        .collect::<Result<_, ObservationError>>()?;
    let main = ms.main_table();
    require_units(main, "UVW", "m")?;
    let stored_label = measure_reference(main, "UVW")?
        .map(|name| {
            name.parse::<DirectionRef>()
                .map_err(|_| invalid(format!("unknown UVW reference {name}")))
        })
        .transpose()?;
    // CASA's convention: UVW are in the phase centre's frame. An ITRF label
    // (casacore's default, which simulators and fillers write) is the known
    // mislabelling; any other label must name the phase centres' frame.
    match stored_label {
        None | Some(DirectionRef::ITRF) => {}
        Some(label) if label == phase_frame => {}
        Some(_) => {
            return Err(ObservationError::Unsupported {
                what: "MSv2 UVW labelled with a celestial frame other than the phase centres'",
            });
        }
    }
    Ok(Fields {
        entries: fields,
        ephemerides,
        uvw_frame: UvwFrame {
            effective: phase_frame,
            stored_label,
        },
    })
}

/// Every `OBSERVATION` row. A MeasurementSet without one gets a single
/// empty entry, which every row's `OBSERVATION_ID` 0 then names.
pub(super) fn read_observations(
    ms: &MeasurementSet,
) -> Result<Vec<ObservationEntry>, ObservationError> {
    let observation = ms.observation().map_err(storage)?;
    if observation.row_count() == 0 {
        return Ok(vec![ObservationEntry::default()]);
    }
    (0..observation.row_count())
        .map(|row| {
            let schedule: Vec<String> = match observation
                .optional_array(row, "SCHEDULE")
                .map_err(storage)?
            {
                Some(ArrayValue::String(lines)) => lines.iter().cloned().collect(),
                _ => Vec::new(),
            };
            let uid = |line: usize, label: &str| {
                schedule
                    .get(line)
                    .filter(|_| schedule.len() == 2)
                    .and_then(|line| scheduled_uid(line, label))
            };
            Ok(ObservationEntry {
                telescope: observation.string(row, "TELESCOPE_NAME").map_err(storage)?,
                observers: vec![observation.string(row, "OBSERVER").map_err(storage)?],
                project: observation.string(row, "PROJECT").map_err(storage)?,
                execution_block: uid(1, "ExecBlock"),
                session_reference: None,
                scheduling_block: uid(0, "SchedulingBlock"),
            })
        })
        .collect()
}

/// The UID after `label` in a `SCHEDULE` line, as XRADIO's converter reads
/// one `importasdm` writes (`[SchedulingBlock uid://A001/X3571/X122,
/// ExecBlock uid://A002/X1003af4/X75a3]`, one entry per line): the word
/// characters, `/` and `:` that follow `label` and a space.
fn scheduled_uid(line: &str, label: &str) -> Option<String> {
    let start = line.find(&format!("{label} "))? + label.len() + 1;
    let uid: String = line[start..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || matches!(c, '_' | '/' | ':'))
        .collect();
    (!uid.is_empty()).then_some(uid)
}

/// `STATE::OBS_MODE` of every state row: the intents a `STATE_ID` names.
pub(super) fn read_state_modes(ms: &MeasurementSet) -> Result<Vec<String>, ObservationError> {
    match ms.state() {
        Ok(state) => (0..state.row_count())
            .map(|row| state.string(row, "OBS_MODE").map_err(storage))
            .collect(),
        Err(_) => Ok(Vec::new()),
    }
}

/// The intents an `OBS_MODE` string lists, sorted and deduplicated.
pub(super) fn intents(mode: &str) -> Vec<String> {
    let set: BTreeSet<String> = mode
        .split(',')
        .map(str::trim)
        .filter(|intent| !intent.is_empty())
        .map(str::to_string)
        .collect();
    set.into_iter().collect()
}

/// Every `ANTENNA` row.
pub(super) fn read_antennas(ms: &MeasurementSet) -> Result<Vec<AntennaEntry>, ObservationError> {
    let antenna = ms.antenna().map_err(storage)?;
    let antenna_table = ms
        .subtable(SubtableId::Antenna)
        .ok_or_else(|| invalid("no ANTENNA subtable"))?;
    require_units(antenna_table, "POSITION", "m")?;
    require_units(antenna_table, "DISH_DIAMETER", "m")?;
    match measure_reference(antenna_table, "POSITION")?.as_deref() {
        None | Some("ITRF") => {}
        Some(_) => {
            return Err(ObservationError::Unsupported {
                what: "MSv2 antenna positions in other than ITRF",
            });
        }
    }
    (0..antenna.row_count())
        .map(|row| {
            let [x, y, z] = antenna.position(row).map_err(storage)?;
            Ok(AntennaEntry {
                name: antenna.name(row).map_err(storage)?,
                station: antenna.station(row).map_err(storage)?,
                position: MPosition::new_itrf(x, y, z),
                mount: antenna.mount(row).map_err(storage)?,
                dish_diameter: metres(antenna.dish_diameter(row).map_err(storage)?),
            })
        })
        .collect()
}

/// Every `FEED` row's receptors, by (antenna, feed, spectral window), the
/// window -1 standing for every window.
pub(super) struct Feeds {
    rows: BTreeMap<(i32, i32, i32), Vec<Receptors>>,
    /// Some row names a window: casacore's `MSIter` then reads only each
    /// window's own rows, and XRADIO's converter likewise.
    window_dependent: bool,
}

/// The `FEED` rows.
pub(super) fn read_feeds(ms: &MeasurementSet) -> Result<Feeds, ObservationError> {
    let feed = ms.feed().map_err(storage)?;
    if let Some(feed_table) = ms.subtable(SubtableId::Feed) {
        require_units(feed_table, "RECEPTOR_ANGLE", "rad")?;
    }
    let mut rows = BTreeMap::<(i32, i32, i32), Vec<Receptors>>::new();
    for row in 0..feed.row_count() {
        let key = (
            feed.i32(row, "ANTENNA_ID").map_err(storage)?,
            feed.i32(row, "FEED_ID").map_err(storage)?,
            feed.i32(row, "SPECTRAL_WINDOW_ID").map_err(storage)?,
        );
        let types = match feed.array(row, "POLARIZATION_TYPE").map_err(storage)? {
            ArrayValue::String(types) => types.iter().cloned().collect(),
            _ => return Err(invalid("FEED POLARIZATION_TYPE is not a string array")),
        };
        let angles = match feed.array(row, "RECEPTOR_ANGLE").map_err(storage)? {
            ArrayValue::Float64(angles) => angles.iter().map(|angle| radians(*angle)).collect(),
            _ => return Err(invalid("FEED RECEPTOR_ANGLE is not a Float64 array")),
        };
        rows.entry(key)
            .or_default()
            .push(Receptors { types, angles });
    }
    let window_dependent = rows.keys().any(|(_, _, window)| *window != -1);
    Ok(Feeds {
        rows,
        window_dependent,
    })
}

impl Feeds {
    /// Antenna `antenna`'s receptors through feed `feed` in spectral window
    /// `window`, as casacore's `MSIter::setFeedInfo` and XRADIO's converter
    /// select them: the window's own rows when any row names a window, else
    /// the rows for every window (-1). A partition's receptors are one set
    /// (MSv4 holds no time dependence), so rows of one antenna, feed and
    /// window that differ, `FEED` stating receptors per time, are refused;
    /// casacore warns and takes the last, XRADIO's converter refuses. `None`
    /// without a row.
    pub(super) fn receptors(
        &self,
        antenna: i32,
        feed: i32,
        window: i32,
    ) -> Result<Option<Receptors>, ObservationError> {
        let window = if self.window_dependent { window } else { -1 };
        let Some((first, others)) = self
            .rows
            .get(&(antenna, feed, window))
            .and_then(|rows| rows.split_first())
        else {
            return Ok(None);
        };
        if others.iter().any(|other| other != first) {
            return Err(ObservationError::Unsupported {
                what: "MSv2 FEED rows of one antenna, feed and window that differ (receptors \
                       that vary in time)",
            });
        }
        Ok(Some(first.clone()))
    }
}

pub(crate) fn metres(value: f64) -> Quantity {
    Quantity::new(value, "m").expect("metres parse")
}

pub(crate) fn radians(value: f64) -> Quantity {
    Quantity::new(value, "rad").expect("radians parse")
}

/// The `SOURCE` name of source `id`, if the MeasurementSet has one.
fn source_name(ms: &MeasurementSet, id: i32) -> Result<Option<String>, ObservationError> {
    if id < 0 {
        return Ok(None);
    }
    let Ok(source) = ms.source() else {
        return Ok(None);
    };
    for row in 0..source.row_count() {
        if source.i32(row, "SOURCE_ID").map_err(storage)? == id {
            return source.string(row, "NAME").map(Some).map_err(storage);
        }
    }
    Ok(None)
}

/// The scans: MAIN's distinct `SCAN_NUMBER`s in ascending order, named by
/// their decimal number as XRADIO names them, with the `STATE::OBS_MODE`
/// intents of their rows' states (`states_by_number`, each scan's
/// `STATE_ID`s; `modes`, each `STATE` row's `OBS_MODE`); and the scan of
/// each `SCAN_NUMBER`.
pub(super) fn scans(
    states_by_number: BTreeMap<i32, BTreeSet<i32>>,
    modes: &[String],
) -> (Vec<ScanEntry>, BTreeMap<i32, ScanId>) {
    let index: BTreeMap<i32, ScanId> = states_by_number
        .keys()
        .enumerate()
        .map(|(index, number)| (*number, ScanId(index as u32)))
        .collect();
    let scans = states_by_number
        .into_iter()
        .map(|(number, states)| {
            let intents: BTreeSet<String> = states
                .into_iter()
                .filter_map(|state| {
                    usize::try_from(state)
                        .ok()
                        .and_then(|state| modes.get(state))
                })
                .flat_map(|mode| intents(mode))
                .collect();
            ScanEntry {
                name: number.to_string(),
                number: Some(number),
                intents: intents.into_iter().collect(),
            }
        })
        .collect();
    (scans, index)
}

/// The spectral window and correlation products of one data description.
pub(super) struct SpectralSetup {
    /// The `SPECTRAL_WINDOW` row.
    pub(super) spectral_window_id: u32,
    pub(super) spectral_window: String,
    pub(super) frequencies_hz: Vec<f64>,
    pub(super) frequency_frame: FrequencyRef,
    pub(super) channel_widths_hz: Vec<f64>,
    pub(super) correlations: Vec<Correlation>,
}

/// Every `DATA_DESCRIPTION` row's spectral setup.
pub(super) fn spectral_setups(ms: &MeasurementSet) -> Result<Vec<SpectralSetup>, ObservationError> {
    let descriptions = ms.data_description().map_err(storage)?;
    let windows = ms.spectral_window().map_err(storage)?;
    if let Some(window_table) = ms.subtable(SubtableId::SpectralWindow) {
        require_units(window_table, "CHAN_FREQ", "Hz")?;
        require_units(window_table, "CHAN_WIDTH", "Hz")?;
    }
    let polarizations = ms.polarization().map_err(storage)?;
    (0..descriptions.row_count())
        .map(|row| {
            let window = usize::try_from(descriptions.spectral_window_id(row).map_err(storage)?)
                .map_err(|_| invalid("negative SPECTRAL_WINDOW_ID"))?;
            let polarization = usize::try_from(descriptions.polarization_id(row).map_err(storage)?)
                .map_err(|_| invalid("negative POLARIZATION_ID"))?;
            let frame_code = windows.meas_freq_ref(window).map_err(storage)?;
            let frequency_frame = FrequencyRef::from_casacore_code(frame_code)
                .ok_or_else(|| invalid(format!("unknown MEAS_FREQ_REF {frame_code}")))?;
            let correlations = polarizations
                .corr_type(polarization)
                .map_err(storage)?
                .into_iter()
                .map(|code| {
                    Correlation::from_casacore_code(code)
                        .ok_or_else(|| invalid(format!("unknown CORR_TYPE {code}")))
                })
                .collect::<Result<_, _>>()?;
            Ok(SpectralSetup {
                spectral_window_id: window as u32,
                spectral_window: windows.name(window).map_err(storage)?,
                frequencies_hz: windows.chan_freq(window).map_err(storage)?,
                frequency_frame,
                channel_widths_hz: windows.chan_width(window).map_err(storage)?,
                correlations,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::scheduled_uid;

    #[test]
    fn schedule_lines_name_their_uids_as_xradio_reads_them() {
        let lines = [
            "[SchedulingBlock uid://A001/X3571/X122,",
            "ExecBlock uid://A002/X1003af4/X75a3]",
        ];
        assert_eq!(
            scheduled_uid(lines[0], "SchedulingBlock").as_deref(),
            Some("uid://A001/X3571/X122")
        );
        assert_eq!(
            scheduled_uid(lines[1], "ExecBlock").as_deref(),
            Some("uid://A002/X1003af4/X75a3")
        );
        assert_eq!(scheduled_uid("default", "ExecBlock"), None);
        assert_eq!(scheduled_uid("ExecBlock ", "ExecBlock"), None);
    }
}
