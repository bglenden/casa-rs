// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's descriptive metadata (#937): every spectral window,
//! the logical sources with their records, the states and the processors,
//! read once at opening. A column the table lacks, or a cell it leaves
//! undefined, is `None`; a unit or measure reference outside the table's
//! definition refuses the store, naming the column, and so does a defined
//! cell of another shape than its definition's (a window's per-channel
//! arrays of `NUM_CHAN` values, SOURCE's fixed vectors of their length,
//! each on one axis). A SOURCE line array whose length is not the row's
//! `NUM_LINES` is dropped with a warning, since such a table is readable
//! otherwise.

use std::collections::BTreeMap;

use casa_observation::{
    ObservationError, ProcessorEntry, SourceEntry, SourceId, SourceRecord, SourceWindow,
    StateEntry, Velocity, WindowEntry, WindowId,
};
use casa_tables::Table;
use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position::MPosition;
use casa_types::measures::radial_velocity::RadialVelocityRef;
use casa_types::measures::{EpochRef, MEpoch};

use super::catalogue::{measure_reference, require_units};
use super::{invalid, storage};
use crate::subtables::{
    get_i32, get_string, optional_array, optional_bool, optional_f64, optional_i32, optional_string,
};
use crate::{MeasurementSet, SubtableId};

/// The `f64` values of `value`, a `column` cell.
pub(super) fn floats(
    value: Option<&ArrayValue>,
    column: &str,
) -> Result<Option<Vec<f64>>, ObservationError> {
    match value {
        None => Ok(None),
        Some(ArrayValue::Float64(values)) => Ok(Some(values.iter().copied().collect())),
        Some(_) => Err(invalid(format!("{column} is not a Float64 array"))),
    }
}

/// The `f64` values of `value`, a `column` cell of one axis.
fn vector(value: Option<&ArrayValue>, column: &str) -> Result<Option<Vec<f64>>, ObservationError> {
    match value {
        Some(ArrayValue::Float64(values)) if values.ndim() != 1 => Err(invalid(format!(
            "{column} has shape {:?}, not one axis",
            values.shape()
        ))),
        value => floats(value, column),
    }
}

/// `value`, a `column` cell, as exactly `N` values on one axis.
fn fixed<const N: usize>(
    value: Option<&ArrayValue>,
    column: &str,
) -> Result<Option<[f64; N]>, ObservationError> {
    vector(value, column)?
        .map(|values| {
            <[f64; N]>::try_from(values.as_slice())
                .map_err(|_| invalid(format!("{column} holds {} values, not {N}", values.len())))
        })
        .transpose()
}

/// `value`, a `column` cell of a window of `channels` channels, as one
/// value per channel.
fn per_channel(
    value: Option<&ArrayValue>,
    column: &str,
    channels: i32,
) -> Result<Option<Vec<f64>>, ObservationError> {
    vector(value, column)?
        .map(|values| {
            if i32::try_from(values.len()) == Ok(channels) {
                Ok(values)
            } else {
                Err(invalid(format!(
                    "{column} holds {} values for a window of NUM_CHAN {channels}",
                    values.len()
                )))
            }
        })
        .transpose()
}

/// The time scale of a table's `column` of epochs, in seconds: its fixed
/// `MEASINFO.Ref`, or UTC, the definition's default.
pub(super) fn epoch_scale(table: &Table, column: &str) -> Result<EpochRef, ObservationError> {
    require_units(table, column, "s")?;
    match measure_reference(table, column)? {
        Some(name) => name
            .parse()
            .map_err(|_| invalid(format!("unknown {column} reference {name}"))),
        None => Ok(EpochRef::UTC),
    }
}

/// A column's fixed measure reference parsed as `R`, or `default` without
/// one.
fn reference<R: std::str::FromStr>(
    table: &Table,
    column: &str,
    default: Option<R>,
) -> Result<Option<R>, ObservationError> {
    match measure_reference(table, column)? {
        Some(name) => name
            .parse()
            .map(Some)
            .map_err(|_| invalid(format!("unknown {column} reference {name}"))),
        None => Ok(default),
    }
}

/// Every `SPECTRAL_WINDOW` row, used by MAIN or not.
pub(super) fn read_windows(ms: &MeasurementSet) -> Result<Vec<WindowEntry>, ObservationError> {
    let Some(table) = ms.subtable(SubtableId::SpectralWindow) else {
        return Ok(Vec::new());
    };
    for column in [
        "REF_FREQUENCY",
        "TOTAL_BANDWIDTH",
        "EFFECTIVE_BW",
        "RESOLUTION",
    ] {
        require_units(table, column, "Hz")?;
    }
    (0..table.row_count())
        .map(|row| {
            let code = get_i32(table, row, "MEAS_FREQ_REF").map_err(storage)?;
            let frame = FrequencyRef::from_casacore_code(code)
                .ok_or_else(|| invalid(format!("unknown MEAS_FREQ_REF {code}")))?;
            let channels = get_i32(table, row, "NUM_CHAN").map_err(storage)?;
            Ok(WindowEntry {
                name: get_string(table, row, "NAME").map_err(storage)?,
                reference_frequency: optional_f64(table, row, "REF_FREQUENCY")
                    .map_err(storage)?
                    .map(|hz| MFrequency::new(hz, frame)),
                total_bandwidth_hz: optional_f64(table, row, "TOTAL_BANDWIDTH").map_err(storage)?,
                if_chain: optional_i32(table, row, "IF_CONV_CHAIN").map_err(storage)?,
                net_sideband: optional_i32(table, row, "NET_SIDEBAND").map_err(storage)?,
                frequency_group: optional_i32(table, row, "FREQ_GROUP").map_err(storage)?,
                frequency_group_name: optional_string(table, row, "FREQ_GROUP_NAME")
                    .map_err(storage)?,
                effective_bandwidths_hz: per_channel(
                    optional_array(table, row, "EFFECTIVE_BW").map_err(storage)?,
                    "EFFECTIVE_BW",
                    channels,
                )?,
                resolutions_hz: per_channel(
                    optional_array(table, row, "RESOLUTION").map_err(storage)?,
                    "RESOLUTION",
                    channels,
                )?,
                flagged: optional_bool(table, row, "FLAG_ROW").map_err(storage)?,
            })
        })
        .collect()
}

/// The logical sources and each stored `SOURCE_ID`'s identity.
#[derive(Default)]
pub(super) struct Sources {
    pub(super) entries: Vec<SourceEntry>,
    pub(super) ids: BTreeMap<i32, SourceId>,
}

/// The frames of `SOURCE`'s measure columns.
struct SourceFrames {
    time: EpochRef,
    direction: DirectionRef,
    rest: FrequencyRef,
    velocity: Option<RadialVelocityRef>,
}

impl SourceFrames {
    fn of(table: &Table) -> Result<Self, ObservationError> {
        for (column, unit) in [
            ("INTERVAL", "s"),
            ("DIRECTION", "rad"),
            ("PROPER_MOTION", "rad/s"),
            ("POSITION", "m"),
            ("REST_FREQUENCY", "Hz"),
            ("SYSVEL", "m/s"),
        ] {
            require_units(table, column, unit)?;
        }
        if let Some(name) = measure_reference(table, "POSITION")?
            && name != "ITRF"
        {
            return Err(ObservationError::Unsupported {
                what: "MSv2 SOURCE positions in other than ITRF",
            });
        }
        Ok(Self {
            time: epoch_scale(table, "TIME")?,
            direction: reference(table, "DIRECTION", Some(DirectionRef::J2000))?
                .expect("a default"),
            rest: reference(table, "REST_FREQUENCY", Some(FrequencyRef::LSRK))?.expect("a default"),
            velocity: reference(table, "SYSVEL", None)?,
        })
    }
}

/// Every `SOURCE` row, as records of the logical sources their
/// `SOURCE_ID`s name, in stored order; `windows` is how many spectral
/// windows the store has. A record naming a window `SPECTRAL_WINDOW` does
/// not hold is skipped with a warning.
pub(super) fn read_sources(
    ms: &MeasurementSet,
    windows: usize,
) -> Result<Sources, ObservationError> {
    let Some(table) = ms.subtable(SubtableId::Source) else {
        return Ok(Sources::default());
    };
    let frames = SourceFrames::of(table)?;
    let mut sources = Sources::default();
    for row in 0..table.row_count() {
        let window = get_i32(table, row, "SPECTRAL_WINDOW_ID").map_err(storage)?;
        let window = match usize::try_from(window) {
            Err(_) => SourceWindow::All,
            Ok(window) if window < windows => SourceWindow::Window(WindowId(window as u32)),
            Ok(_) => {
                tracing::warn!(
                    "SOURCE row {row} names spectral window {window}, which SPECTRAL_WINDOW does \
                     not hold; the record is not read"
                );
                continue;
            }
        };
        let record = source_record(table, row, window, &frames)?;
        let stored = get_i32(table, row, "SOURCE_ID").map_err(storage)?;
        let next = SourceId(u32::try_from(sources.entries.len()).expect("few sources"));
        let id = *sources.ids.entry(stored).or_insert(next);
        if id == next {
            sources.entries.push(SourceEntry {
                name: get_string(table, row, "NAME").map_err(storage)?,
                records: Vec::new(),
            });
        }
        sources.entries[id.0 as usize].records.push(record);
    }
    Ok(sources)
}

/// One `SOURCE` row's record for `window`.
fn source_record(
    table: &Table,
    row: usize,
    window: SourceWindow,
    frames: &SourceFrames,
) -> Result<SourceRecord, ObservationError> {
    let array = |column: &str| optional_array(table, row, column).map_err(storage);
    let num_lines = optional_i32(table, row, "NUM_LINES")
        .map_err(storage)?
        .map_or(0, |lines| u32::try_from(lines).unwrap_or(0));
    let lines = |column: &str, length: usize| {
        let matches = length == num_lines as usize;
        if !matches {
            tracing::warn!(
                "SOURCE row {row} holds {length} {column} values for {num_lines} lines; they are \
                 not read"
            );
        }
        matches
    };
    let transitions = match array("TRANSITION")? {
        None => None,
        Some(ArrayValue::String(values)) => Some(values.iter().cloned().collect::<Vec<_>>()),
        Some(_) => return Err(invalid("SOURCE TRANSITION is not a String array")),
    }
    .filter(|values| lines("TRANSITION", values.len()));
    let rest_frequencies = floats(array("REST_FREQUENCY")?, "REST_FREQUENCY")?
        .filter(|values| lines("REST_FREQUENCY", values.len()))
        .map(|values| {
            values
                .into_iter()
                .map(|hz| MFrequency::new(hz, frames.rest))
                .collect()
        });
    let systemic_velocities = floats(array("SYSVEL")?, "SYSVEL")?
        .filter(|values| lines("SYSVEL", values.len()))
        .map(|values| {
            values
                .into_iter()
                .map(|value_m_s| Velocity {
                    value_m_s,
                    frame: frames.velocity,
                })
                .collect()
        });
    Ok(SourceRecord {
        window,
        time: optional_f64(table, row, "TIME")
            .map_err(storage)?
            .map(|seconds| MEpoch::from_mjd_seconds(seconds, frames.time)),
        interval_s: optional_f64(table, row, "INTERVAL").map_err(storage)?,
        code: optional_string(table, row, "CODE").map_err(storage)?,
        calibration_group: optional_i32(table, row, "CALIBRATION_GROUP").map_err(storage)?,
        direction: fixed::<2>(array("DIRECTION")?, "DIRECTION")?.map(|[longitude, latitude]| {
            MDirection::from_angles(longitude, latitude, frames.direction)
        }),
        proper_motion_rad_s: fixed::<2>(array("PROPER_MOTION")?, "PROPER_MOTION")?,
        position: fixed::<3>(array("POSITION")?, "POSITION")?
            .map(|[x, y, z]| MPosition::new_itrf(x, y, z)),
        pulsar_id: optional_i32(table, row, "PULSAR_ID").map_err(storage)?,
        num_lines,
        transitions,
        rest_frequencies,
        systemic_velocities,
    })
}

/// Every `STATE` row; none without the table.
pub(super) fn read_states(ms: &MeasurementSet) -> Result<Vec<StateEntry>, ObservationError> {
    let Some(table) = ms.subtable(SubtableId::State) else {
        return Ok(Vec::new());
    };
    for column in ["CAL", "LOAD"] {
        require_units(table, column, "K")?;
    }
    (0..table.row_count())
        .map(|row| {
            Ok(StateEntry {
                obs_mode: get_string(table, row, "OBS_MODE").map_err(storage)?,
                cal_k: optional_f64(table, row, "CAL").map_err(storage)?,
                load_k: optional_f64(table, row, "LOAD").map_err(storage)?,
                signal: optional_bool(table, row, "SIG").map_err(storage)?,
                reference: optional_bool(table, row, "REF").map_err(storage)?,
                sub_scan: optional_i32(table, row, "SUB_SCAN").map_err(storage)?,
                flagged: optional_bool(table, row, "FLAG_ROW").map_err(storage)?,
            })
        })
        .collect()
}

/// Every `PROCESSOR` row; none without the table.
pub(super) fn read_processors(
    ms: &MeasurementSet,
) -> Result<Vec<ProcessorEntry>, ObservationError> {
    let Some(table) = ms.subtable(SubtableId::Processor) else {
        return Ok(Vec::new());
    };
    (0..table.row_count())
        .map(|row| {
            Ok(ProcessorEntry {
                kind: optional_string(table, row, "TYPE").map_err(storage)?,
                sub_type: optional_string(table, row, "SUB_TYPE").map_err(storage)?,
                type_id: optional_i32(table, row, "TYPE_ID").map_err(storage)?,
                mode_id: optional_i32(table, row, "MODE_ID").map_err(storage)?,
                pass_id: optional_i32(table, row, "PASS_ID").map_err(storage)?,
                flagged: optional_bool(table, row, "FLAG_ROW").map_err(storage)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use casa_tables::ColumnSchema;
    use casa_tables::table_measures::{MeasureType, TableMeasDesc};
    use casa_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    use super::*;
    use crate::MeasurementSetBuilder;
    use crate::schema::optional_columns;

    fn values(values: &[f64]) -> Value {
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![values.len()], values.to_vec()).expect("shape"),
        ))
    }

    /// An MS whose SOURCE has the line columns, rest frequencies stated
    /// barycentric and velocities LSRK.
    fn with_lines() -> MeasurementSet {
        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_optional_subtable(SubtableId::Source),
        )
        .expect("ms");
        let table = ms.subtable_mut(SubtableId::Source).expect("SOURCE");
        for name in ["REST_FREQUENCY", "SYSVEL", "TRANSITION"] {
            let definition = optional_columns(SubtableId::Source)
                .iter()
                .find(|definition| definition.name == name)
                .expect("an optional SOURCE column");
            table
                .add_column(
                    ColumnSchema::array_variable(name, definition.data_type, None),
                    None,
                )
                .expect("column");
        }
        TableMeasDesc::new_fixed("REST_FREQUENCY", MeasureType::Frequency, "BARY")
            .write(table)
            .expect("REST_FREQUENCY MEASINFO");
        TableMeasDesc::new_fixed("SYSVEL", MeasureType::RadialVelocity, "LSRK")
            .write(table)
            .expect("SYSVEL MEASINFO");
        ms
    }

    /// A SOURCE row of `id` for `window` stating `lines` lines, with the
    /// rest frequencies and velocities of `stated` when given.
    fn source(
        ms: &mut MeasurementSet,
        (id, window, name): (i32, i32, &str),
        lines: i32,
        stated: Option<(&[f64], &[f64])>,
    ) {
        let int = |value| Value::Scalar(ScalarValue::Int32(value));
        let mut fields = vec![
            RecordField::new("CALIBRATION_GROUP", int(0)),
            RecordField::new("CODE", Value::Scalar(ScalarValue::String("OBJ".into()))),
            RecordField::new("DIRECTION", values(&[1.0, 0.5])),
            RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(1e30))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.into()))),
            RecordField::new("NUM_LINES", int(lines)),
            RecordField::new("PROPER_MOTION", values(&[0.0, 0.0])),
            RecordField::new("SOURCE_ID", int(id)),
            RecordField::new("SPECTRAL_WINDOW_ID", int(window)),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(4.8e9))),
        ];
        if let Some((rest, velocities)) = stated {
            fields.push(RecordField::new("REST_FREQUENCY", values(rest)));
            fields.push(RecordField::new("SYSVEL", values(velocities)));
        }
        ms.subtable_mut(SubtableId::Source)
            .expect("SOURCE")
            .add_row(RecordValue::new(fields))
            .expect("row");
    }

    /// A logical source holds every record of its `SOURCE_ID`, each for its
    /// own window or for all of them, in stored order; two sources of one
    /// name stay two. A record for a window the store lacks is not read,
    /// and a line array of another length than `NUM_LINES` is dropped.
    #[test]
    fn sources_are_logical_with_their_records_by_window() {
        let mut ms = with_lines();
        source(&mut ms, (0, 0, "M51"), 1, Some((&[330.0e9], &[10.0])));
        source(&mut ms, (0, 1, "M51"), 1, Some((&[331.0e9], &[12.0])));
        source(&mut ms, (0, -1, "M51"), 0, None);
        source(&mut ms, (7, 0, "M51"), 1, Some((&[1.0e9], &[0.0])));
        source(&mut ms, (3, 5, "stray"), 0, None);
        source(&mut ms, (4, 0, "short"), 2, Some((&[2.0e9], &[0.0])));
        let sources = read_sources(&ms, 2).expect("sources");
        assert_eq!(
            sources.ids,
            BTreeMap::from([(0, SourceId(0)), (7, SourceId(1)), (4, SourceId(2))])
        );
        let m51 = &sources.entries[0];
        assert_eq!(m51.name, "M51");
        let windows: Vec<SourceWindow> = m51.records.iter().map(|record| record.window).collect();
        assert_eq!(
            windows,
            [
                SourceWindow::Window(WindowId(0)),
                SourceWindow::Window(WindowId(1)),
                SourceWindow::All
            ]
        );
        let line = m51.records[1].rest_frequencies.as_ref().expect("lines")[0].clone();
        assert_eq!((line.hz(), line.refer()), (331.0e9, FrequencyRef::BARY));
        let velocity = m51.records[1].systemic_velocities.as_ref().expect("lines")[0];
        assert_eq!(
            (velocity.value_m_s, velocity.frame),
            (12.0, Some(RadialVelocityRef::LSRK))
        );
        assert_eq!(m51.records[2].num_lines, 0);
        assert!(m51.records[2].rest_frequencies.is_none(), "undefined cells");
        assert_eq!(m51.records[0].code.as_deref(), Some("OBJ"));
        assert_eq!(sources.entries[1].name, "M51", "another source of one name");
        let short = &sources.entries[2].records[0];
        assert_eq!(short.num_lines, 2);
        assert!(short.rest_frequencies.is_none() && short.systemic_velocities.is_none());
    }

    /// A SOURCE vector of fixed length is refused on another axis count,
    /// whatever its element count.
    #[test]
    fn a_source_vector_of_two_axes_is_refused() {
        let mut ms = with_lines();
        let table = ms.subtable_mut(SubtableId::Source).expect("SOURCE");
        table
            .add_column(
                ColumnSchema::array_variable("POSITION", casa_types::PrimitiveType::Float64, None),
                None,
            )
            .expect("POSITION");
        source(&mut ms, (0, 0, "M51"), 0, None);
        ms.subtable_mut(SubtableId::Source)
            .expect("SOURCE")
            .row_accessor_mut()
            .set_cell(
                0,
                "POSITION",
                Value::Array(ArrayValue::Float64(ArrayD::zeros(vec![3, 1]))),
            )
            .expect("a position of two axes");
        let refused = read_sources(&ms, 1).err().map(|error| error.to_string());
        assert!(
            refused
                .as_deref()
                .is_some_and(|reason| reason.contains("POSITION") && reason.contains("one axis")),
            "{refused:?}"
        );
    }

    /// Without a SOURCE table there are no sources.
    #[test]
    fn a_store_without_source_has_no_sources() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("ms");
        let sources = read_sources(&ms, 1).expect("sources");
        assert!(sources.entries.is_empty() && sources.ids.is_empty());
    }
}
