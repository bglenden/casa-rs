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
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
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
    let stated = Stated::read(store, path, rows)?;
    Ok((0..rows).map(|row| stated.record(row, window)).collect())
}

/// What a `field_and_source` dataset states of its fields' sources, each
/// variable flattened field by field (by line within a field).
struct Stated {
    lines: usize,
    directions: Option<(Vec<f64>, DirectionRef)>,
    rest: Option<(Vec<f64>, FrequencyRef)>,
    velocities: Option<Vec<f64>>,
    names: Option<Vec<String>>,
}

impl Stated {
    /// The source variables of the dataset at `path`, of `rows` fields.
    fn read(
        store: &Arc<FilesystemStore>,
        path: &str,
        rows: usize,
    ) -> Result<Self, ObservationError> {
        let lines = if has_node(store, &format!("{path}/line_label"))? {
            strings(store, &format!("{path}/line_label"))?.len()
        } else {
            0
        };
        let variable = |name: &str, per_row: usize| {
            let variable = format!("{path}/{name}");
            if !has_node(store, &variable)? {
                return Ok(None);
            }
            let (values, attributes) = numbers::<f64>(store, &variable)?;
            if values.len() != rows * per_row {
                return Err(invalid(format!(
                    "{variable} holds {} values for {rows} fields of {per_row}",
                    values.len()
                )));
            }
            Ok(Some((values, attributes)))
        };
        let directions = variable("SOURCE_DIRECTION", 2)?
            .map(|(values, attributes)| {
                require_units(&attributes, "rad", "SOURCE_DIRECTION")?;
                Ok::<_, ObservationError>((
                    values,
                    direction_frame(&attributes, "SOURCE_DIRECTION")?,
                ))
            })
            .transpose()?;
        let rest = variable("LINE_REST_FREQUENCY", lines)?
            .map(|(values, attributes)| {
                Ok::<_, ObservationError>((
                    values,
                    frequency_frame(&attributes, "LINE_REST_FREQUENCY")?,
                ))
            })
            .transpose()?;
        let velocities = variable("LINE_SYSTEMIC_VELOCITY", lines)?
            .map(|(values, attributes)| {
                require_units(&attributes, "m/s", "LINE_SYSTEMIC_VELOCITY")?;
                Ok::<_, ObservationError>(values)
            })
            .transpose()?;
        let mut names = None;
        for coordinate in ["line_name", "line_names"] {
            let coordinate = format!("{path}/{coordinate}");
            if names.is_none() && has_node(store, &coordinate)? {
                let stated = strings(store, &coordinate)?;
                if stated.len() != rows * lines {
                    return Err(invalid(format!(
                        "{coordinate} holds {} names for {rows} fields of {lines} lines",
                        stated.len()
                    )));
                }
                names = Some(stated);
            }
        }
        Ok(Self {
            lines,
            directions,
            rest,
            velocities,
            names,
        })
    }

    /// How many of field `row`'s lines are stated: a line is when any of
    /// its variables is, since XRADIO pads a source's lines to the
    /// dataset's most with `NaN` and `""`.
    fn stated_lines(&self, row: usize) -> usize {
        let at = |index: usize| row * self.lines + index;
        (0..self.lines)
            .rev()
            .find(|index| {
                self.rest
                    .as_ref()
                    .is_some_and(|(values, _)| !values[at(*index)].is_nan())
                    || self
                        .velocities
                        .as_ref()
                        .is_some_and(|values| !values[at(*index)].is_nan())
                    || self
                        .names
                        .as_ref()
                        .is_some_and(|names| !names[at(*index)].is_empty())
            })
            .map_or(0, |last| last + 1)
    }

    /// Field `row`'s record for `window`, or `None` when nothing of its
    /// source is stated.
    fn record(&self, row: usize, window: WindowId) -> Option<SourceRecord> {
        let stated = self.stated_lines(row);
        let direction = self
            .directions
            .as_ref()
            .map(|(values, frame)| (values[2 * row], values[2 * row + 1], *frame))
            .filter(|(longitude, latitude, _)| !longitude.is_nan() && !latitude.is_nan())
            .map(|(longitude, latitude, frame)| {
                MDirection::from_angles(longitude, latitude, frame)
            });
        if direction.is_none() && stated == 0 {
            return None;
        }
        let lines = row * self.lines..row * self.lines + stated;
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
            transitions: self
                .names
                .as_ref()
                .filter(|_| has_lines)
                .map(|names| names[lines.clone()].to_vec()),
            rest_frequencies: self
                .rest
                .as_ref()
                .filter(|_| has_lines)
                .map(|(values, frame)| {
                    values[lines.clone()]
                        .iter()
                        .map(|hz| MFrequency::new(*hz, *frame))
                        .collect()
                }),
            systemic_velocities: self
                .velocities
                .as_ref()
                .filter(|_| has_lines)
                .map(|values| {
                    values[lines.clone()]
                        .iter()
                        .map(|value| Velocity {
                            value_m_s: *value,
                            frame: None,
                        })
                        .collect()
                }),
        })
    }
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
