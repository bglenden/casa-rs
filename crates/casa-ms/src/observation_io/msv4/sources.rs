// SPDX-License-Identifier: LGPL-3.0-or-later
//! A `field_and_source` dataset's sources (#976): each field's source
//! direction and spectral lines, as XRADIO's `FieldSourceXds` states them
//! for the dataset's window, read into the catalogue's logical sources.
//!
//! XRADIO's converter fills them from the MSv2 `SOURCE` rows of the
//! partition's window (`SPECTRAL_WINDOW_ID` equal to it, not −1), the first
//! row of each source: `DIRECTION` as `SOURCE_DIRECTION`, `REST_FREQUENCY`
//! and `SYSVEL` as `LINE_REST_FREQUENCY` and `LINE_SYSTEMIC_VELOCITY`,
//! `TRANSITION` as the line names, each line variable padded with `NaN`
//! (and each name with an empty string) to the most lines a field's source
//! has. The schema names the names' coordinate `line_names`; XRADIO's
//! converter writes it `line_name`, which is read first.
//! A record is therefore for one window, at no time, and states nothing
//! else; its velocities are quantities in m/s without a frame, and none is
//! invented. A source is one name (XRADIO's `NAME_SOURCEID`), however many
//! datasets state it.
//!
//! The ephemeris form (`field_and_source_ephemeris`) states its lines per
//! time; they are not read, and its fields name no source.

use std::sync::Arc;

use casa_observation::{
    ObservationError, SourceEntry, SourceId, SourceRecord, SourceWindow, Velocity, WindowId,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::MFrequency;
use serde_json::{Map, Value};
use zarrs::filesystem::FilesystemStore;

use super::metadata::{
    direction_frame, frequency_frame, has_node, invalid, numbers, require_units, strings,
};

/// The record each of the `rows` fields of the `field_and_source` dataset
/// at `path` states of its source for `window`: `None` for a field whose
/// source the dataset states nothing of.
///
/// # Errors
///
/// A variable whose shape is not one value (two for a direction) per field
/// and line, whose units are not the schema's, or whose frame is unknown.
pub(super) fn records(
    store: &Arc<FilesystemStore>,
    path: &str,
    rows: usize,
    window: WindowId,
) -> Result<Vec<Option<SourceRecord>>, ObservationError> {
    let optional = |name: &str| -> Result<Option<(Vec<f64>, Map<String, Value>)>, _> {
        let variable = format!("{path}/{name}");
        if has_node(store, &variable)? {
            numbers::<f64>(store, &variable).map(Some)
        } else {
            Ok(None)
        }
    };
    let lines = if has_node(store, &format!("{path}/line_label"))? {
        strings(store, &format!("{path}/line_label"))?.len()
    } else {
        0
    };
    let shaped = |name: &str, values: &[f64], per_row: usize| {
        if values.len() == rows * per_row {
            Ok(())
        } else {
            Err(invalid(format!(
                "{path}/{name} holds {} values for {rows} fields of {per_row}",
                values.len()
            )))
        }
    };
    let directions = optional("SOURCE_DIRECTION")?
        .map(|(values, attributes)| {
            shaped("SOURCE_DIRECTION", &values, 2)?;
            require_units(&attributes, "rad", "SOURCE_DIRECTION")?;
            Ok::<_, ObservationError>((values, direction_frame(&attributes, "SOURCE_DIRECTION")?))
        })
        .transpose()?;
    let rest = optional("LINE_REST_FREQUENCY")?
        .map(|(values, attributes)| {
            shaped("LINE_REST_FREQUENCY", &values, lines)?;
            Ok::<_, ObservationError>((
                values,
                frequency_frame(&attributes, "LINE_REST_FREQUENCY")?,
            ))
        })
        .transpose()?;
    let velocities = optional("LINE_SYSTEMIC_VELOCITY")?
        .map(|(values, attributes)| {
            shaped("LINE_SYSTEMIC_VELOCITY", &values, lines)?;
            require_units(&attributes, "m/s", "LINE_SYSTEMIC_VELOCITY")?;
            Ok::<_, ObservationError>(values)
        })
        .transpose()?;
    let mut named = None;
    for coordinate in ["line_name", "line_names"] {
        if named.is_none() && has_node(store, &format!("{path}/{coordinate}"))? {
            named = Some(coordinate);
        }
    }
    let names = if let Some(coordinate) = named {
        let names = strings(store, &format!("{path}/{coordinate}"))?;
        if names.len() != rows * lines {
            return Err(invalid(format!(
                "{path}/{coordinate} holds {} names for {rows} fields of {lines} lines",
                names.len()
            )));
        }
        Some(names)
    } else {
        None
    };
    Ok((0..rows)
        .map(|row| {
            let line = |values: &[f64], index: usize| values[row * lines + index];
            // A line is stated when any of its variables is: XRADIO pads a
            // source's lines to the dataset's most with NaN and "".
            let stated = (0..lines)
                .rev()
                .find(|index| {
                    rest.as_ref()
                        .is_some_and(|(values, _)| !line(values, *index).is_nan())
                        || velocities
                            .as_ref()
                            .is_some_and(|values| !line(values, *index).is_nan())
                        || names
                            .as_ref()
                            .is_some_and(|names| !names[row * lines + index].is_empty())
                })
                .map_or(0, |last| last + 1);
            let direction = directions
                .as_ref()
                .map(|(values, frame)| (values[2 * row], values[2 * row + 1], *frame))
                .filter(|(longitude, latitude, _)| !longitude.is_nan() && !latitude.is_nan())
                .map(|(longitude, latitude, frame)| {
                    MDirection::from_angles(longitude, latitude, frame)
                });
            if direction.is_none() && stated == 0 {
                return None;
            }
            let has_lines = stated > 0;
            Some(SourceRecord {
                window: SourceWindow::Window(window),
                time: None,
                interval_s: None,
                code: None,
                calibration_group: None,
                direction,
                proper_motion_rad_s: None,
                position: None,
                pulsar_id: None,
                num_lines: u32::try_from(stated).expect("few lines"),
                transitions: names
                    .as_ref()
                    .filter(|_| has_lines)
                    .map(|names| names[row * lines..row * lines + stated].to_vec()),
                rest_frequencies: rest.as_ref().filter(|_| has_lines).map(|(values, frame)| {
                    (0..stated)
                        .map(|index| MFrequency::new(line(values, index), *frame))
                        .collect()
                }),
                systemic_velocities: velocities.as_ref().filter(|_| has_lines).map(|values| {
                    (0..stated)
                        .map(|index| Velocity {
                            value_m_s: line(values, index),
                            frame: None,
                        })
                        .collect()
                }),
            })
        })
        .collect())
}

/// The source named `name` in `sources`, added if new, with `record` added
/// to its records unless it holds the same already (each dataset of one
/// window states its sources again).
pub(super) fn add(
    sources: &mut Vec<SourceEntry>,
    name: &str,
    record: Option<SourceRecord>,
) -> SourceId {
    let index = sources
        .iter()
        .position(|source| source.name == name)
        .unwrap_or_else(|| {
            sources.push(SourceEntry {
                name: name.to_string(),
                records: Vec::new(),
            });
            sources.len() - 1
        });
    if let Some(record) = record {
        let records = &mut sources[index].records;
        if !records.iter().any(|known| same_record(known, &record)) {
            records.push(record);
        }
    }
    SourceId(u32::try_from(index).expect("few sources"))
}

/// Whether two records of an MSv4 state the same: their window, direction
/// and lines, bit for bit (what [`records`] reads).
fn same_record(a: &SourceRecord, b: &SourceRecord) -> bool {
    let direction = |record: &SourceRecord| {
        record.direction.as_ref().map(|direction| {
            let (longitude, latitude) = direction.as_angles();
            (longitude.to_bits(), latitude.to_bits(), direction.refer())
        })
    };
    let rest = |record: &SourceRecord| {
        record.rest_frequencies.as_ref().map(|lines| {
            lines
                .iter()
                .map(|line| (line.hz().to_bits(), line.refer()))
                .collect::<Vec<_>>()
        })
    };
    let velocities = |record: &SourceRecord| {
        record.systemic_velocities.as_ref().map(|lines| {
            lines
                .iter()
                .map(|line| (line.value_m_s.to_bits(), line.frame))
                .collect::<Vec<_>>()
        })
    };
    a.window == b.window
        && a.num_lines == b.num_lines
        && a.transitions == b.transitions
        && direction(a) == direction(b)
        && rest(a) == rest(b)
        && velocities(a) == velocities(b)
}
