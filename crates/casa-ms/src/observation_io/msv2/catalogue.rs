// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's catalogue: subtables read once at opening, and the
//! measure and unit keywords checked once so that every native view the
//! batches carry has the reference and unit its type claims.

use std::collections::{BTreeMap, BTreeSet};

use casa_observation::{
    AntennaEntry, Correlation, FieldEntry, ObservationEntry, ObservationError, ScanEntry, ScanId,
    UvwFrame, VisibilityUnits,
};
use casa_tables::Table;
use casa_types::measures::EpochRef;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::position::MPosition;
use casa_types::quanta::Quantity;
use casa_types::{ArrayValue, ScalarValue, Value};

use super::{invalid, storage};
use crate::columns::direction_columns::DirectionColumn;
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
pub(super) fn measure_reference(
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
pub(super) fn require_units(
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

/// What `DATA`'s `QuantumUnits` declares: janskys only when it says `Jy`.
pub(super) fn data_units(table: &Table) -> VisibilityUnits {
    match quantum_units(table, "DATA").as_deref() {
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

/// The fields, their ephemerides, and the UVW convention.
pub(super) fn fields_and_uvw(ms: &MeasurementSet) -> Result<Fields, ObservationError> {
    let field = ms.field().map_err(storage)?;
    let field_table = ms
        .subtable(SubtableId::Field)
        .ok_or_else(|| invalid("no FIELD subtable"))?;
    require_units(field_table, "PHASE_DIR", "rad")?;
    let phase_frame = measure_reference(field_table, "PHASE_DIR")?
        .map(|name| {
            name.parse::<DirectionRef>()
                .map_err(|_| invalid(format!("unknown PHASE_DIR reference {name}")))
        })
        .transpose()?
        .unwrap_or(DirectionRef::J2000);
    let phase_dir = DirectionColumn::phase_dir(field_table);
    let fields = (0..field.row_count())
        .map(|row| {
            let (longitude, latitude) = phase_dir.get_direction(row).map_err(storage)?.as_angles();
            let source = field.source_id(row).map_err(storage)?;
            Ok(FieldEntry {
                name: field.name(row).map_err(storage)?,
                phase_centre: MDirection::from_angles(longitude, latitude, phase_frame),
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
            Ok(ObservationEntry {
                telescope: observation.string(row, "TELESCOPE_NAME").map_err(storage)?,
                observers: vec![observation.string(row, "OBSERVER").map_err(storage)?],
                project: observation.string(row, "PROJECT").map_err(storage)?,
            })
        })
        .collect()
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

/// Every `ANTENNA` row, with its first `FEED` row's receptors.
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
    let feed = ms.feed().map_err(storage)?;
    if let Some(feed_table) = ms.subtable(SubtableId::Feed) {
        require_units(feed_table, "RECEPTOR_ANGLE", "rad")?;
    }
    let mut receptors = vec![(Vec::new(), Vec::new()); antenna.row_count()];
    for row in (0..feed.row_count()).rev() {
        let id = usize::try_from(feed.i32(row, "ANTENNA_ID").map_err(storage)?)
            .map_err(|_| invalid("negative FEED ANTENNA_ID"))?;
        let Some(slot) = receptors.get_mut(id) else {
            continue;
        };
        let types = match feed.array(row, "POLARIZATION_TYPE").map_err(storage)? {
            ArrayValue::String(types) => types.iter().cloned().collect(),
            _ => return Err(invalid("FEED POLARIZATION_TYPE is not a string array")),
        };
        let angles = match feed.array(row, "RECEPTOR_ANGLE").map_err(storage)? {
            ArrayValue::Float64(angles) => angles.iter().map(|angle| radians(*angle)).collect(),
            _ => return Err(invalid("FEED RECEPTOR_ANGLE is not a Float64 array")),
        };
        *slot = (types, angles);
    }
    (0..antenna.row_count())
        .zip(receptors)
        .map(|(row, (receptors, receptor_angles))| {
            let [x, y, z] = antenna.position(row).map_err(storage)?;
            Ok(AntennaEntry {
                name: antenna.name(row).map_err(storage)?,
                station: antenna.station(row).map_err(storage)?,
                position: MPosition::new_itrf(x, y, z),
                mount: antenna.mount(row).map_err(storage)?,
                dish_diameter: metres(antenna.dish_diameter(row).map_err(storage)?),
                receptors,
                receptor_angles,
            })
        })
        .collect()
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
/// intents of their rows; and the scan of each `SCAN_NUMBER`.
pub(super) fn scans(
    numbers: &[i32],
    states: &[i32],
    modes: &[String],
) -> (Vec<ScanEntry>, BTreeMap<i32, ScanId>) {
    let mut by_number = BTreeMap::<i32, BTreeSet<String>>::new();
    for (&number, &state) in numbers.iter().zip(states) {
        let entry = by_number.entry(number).or_default();
        if let Some(mode) = usize::try_from(state)
            .ok()
            .and_then(|state| modes.get(state))
        {
            entry.extend(intents(mode));
        }
    }
    let index: BTreeMap<i32, ScanId> = by_number
        .keys()
        .enumerate()
        .map(|(index, number)| (*number, ScanId(index as u32)))
        .collect();
    let scans = by_number
        .into_iter()
        .map(|(number, intents)| ScanEntry {
            name: number.to_string(),
            number: Some(number),
            intents: intents.into_iter().collect(),
        })
        .collect();
    (scans, index)
}

/// The spectral window and correlation products of one data description.
pub(super) struct SpectralSetup {
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
                        .ok_or_else(|| invalid(format!("unsupported CORR_TYPE {code}")))
                })
                .collect::<Result<_, _>>()?;
            Ok(SpectralSetup {
                spectral_window: windows.name(window).map_err(storage)?,
                frequencies_hz: windows.chan_freq(window).map_err(storage)?,
                frequency_frame,
                channel_widths_hz: windows.chan_width(window).map_err(storage)?,
                correlations,
            })
        })
        .collect()
}
