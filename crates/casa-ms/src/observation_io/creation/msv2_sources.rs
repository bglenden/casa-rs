// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `SOURCE` rows of an MSv2 a plan creates.
//!
//! A catalogue that states its sources has each logical source's records
//! written in stored order under one `SOURCE_ID` (its catalogue index),
//! each in its window (`SPECTRAL_WINDOW_ID` -1 for every window), with
//! `TRANSITION`, `REST_FREQUENCY`, `SYSVEL`, `POSITION` and `PULSAR_ID`
//! when some record states them; a record's unstated line array is an
//! undefined cell. A catalogue that states none (an MSv4's, until #976) has
//! one row per source its fields name and per window, at the first such
//! field's direction, as CASA's fillers write them and XRADIO's converter
//! looks them up, which is noted as defaulted.

use casa_observation::{Catalogue, FieldEntry, ObservationError, SourceRecord, SourceWindow};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_tables::{ColumnSchema, Table};
use casa_types::RecordField;
use casa_types::measures::MeasFrame;
use casa_types::measures::direction::DirectionRef;

use super::Loss;
use super::msv2_identity::{field_time, utc_seconds};
use super::msv2_tables::Notes;
use crate::MeasurementSet;
use crate::observation_io::update::{request_error, storage};
use crate::schema::{self, SubtableId, optional_columns};
use crate::subtable_rows::{f, f64_array, i, row_from_defs, s, string_array, subtable_mut};

/// Seconds per day, for MSv2 `TIME` (MJD seconds).
const SECONDS_PER_DAY: f64 = 86_400.0;

/// The sources a catalogue that states none has its fields name: each
/// name once, at its first field's direction ([`source_direction`]).
pub(super) struct NamedSources(Vec<(String, [f64; 2])>);

/// The sources `catalogue`'s fields name, when it states no sources of
/// its own, each at its first field's direction at that field's time
/// ([`field_time`] from `times`).
///
/// # Errors
///
/// A field epoch in another scale than UTC, or a body's direction that
/// cannot be evaluated.
pub(super) fn named(
    catalogue: &Catalogue,
    times: (f64, &[Option<f64>]),
) -> Result<NamedSources, ObservationError> {
    let mut named: Vec<(String, [f64; 2])> = Vec::new();
    if catalogue.sources.is_empty() {
        for (index, field) in catalogue.fields.iter().enumerate() {
            if let Some(name) = &field.source_name
                && !named.iter().any(|known| known.0 == *name)
            {
                let (time, _) = field_time(field, index, times)?;
                named.push((name.clone(), source_direction(field, time)?));
            }
        }
    }
    Ok(NamedSources(named))
}

/// A source's `DIRECTION`, from its first field (at `field_time`, MJD
/// seconds): a fixed field's phase centre, or the body a moving field
/// follows at that time (clamped to its samples), in J2000 as the column
/// states it. The source is the body; its fields are offsets from it.
fn source_direction(field: &FieldEntry, field_time: f64) -> Result<[f64; 2], ObservationError> {
    let Some(ephemeris) = field.ephemeris.as_deref() else {
        let (longitude, latitude) = field.phase_centre.as_angles();
        return Ok([longitude, latitude]);
    };
    let times = ephemeris.times_mjd();
    let mjd = (field_time / SECONDS_PER_DAY).clamp(times[0], times[times.len() - 1]);
    let body = crate::ephemeris::reference_direction(ephemeris, mjd, [0.0, 0.0])
        .and_then(|body| {
            body.convert_to(DirectionRef::J2000, &MeasFrame::new())
                .map_err(Into::into)
        })
        .map_err(storage)?;
    let (longitude, latitude) = body.as_angles();
    Ok([longitude, latitude])
}

/// The `SOURCE_ID` of `field`: its logical source's, or in a catalogue
/// that states no sources the source its name names; -1 for none.
pub(super) fn field_source(catalogue: &Catalogue, field: &FieldEntry, named: &NamedSources) -> i32 {
    if let Some(source) = field.source {
        return source.0 as i32;
    }
    if !catalogue.sources.is_empty() {
        return -1;
    }
    field
        .source_name
        .as_ref()
        .and_then(|name| named.0.iter().position(|known| known.0 == *name))
        .map_or(-1, |id| id as i32)
}

/// The `SOURCE` rows of `catalogue`, each window's by its row in
/// `window_rows` (`None` for a window not written, whose records are a
/// loss), over data spanning `time_range` (MJD seconds).
pub(super) fn write(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    (named, window_rows): (&NamedSources, &[Option<i32>]),
    time_range: [f64; 2],
    notes: &mut Notes,
) -> Result<(), ObservationError> {
    if catalogue.sources.is_empty() {
        return from_fields(ms, named, window_rows, time_range, notes);
    }
    let table = subtable_mut(ms, SubtableId::Source).map_err(storage)?;
    let columns = Columns::add(table, catalogue)?;
    let mut dropped = 0;
    for (id, source) in catalogue.sources.iter().enumerate() {
        for record in &source.records {
            let window = match record.window {
                SourceWindow::All => -1,
                SourceWindow::Window(window) => {
                    match window_rows.get(window.0 as usize).copied().flatten() {
                        Some(row) => row,
                        None => {
                            dropped += 1;
                            continue;
                        }
                    }
                }
            };
            let mut row = row_from_defs(
                schema::source::REQUIRED_COLUMNS,
                &required((id as i32, &source.name, window), record, time_range, notes)?,
            );
            columns.optional(&mut row, record, notes)?;
            table.add_row(row).map_err(storage)?;
        }
    }
    if dropped > 0 {
        notes.lost(Loss::Table(format!(
            "SOURCE records of spectral windows not written ({dropped})"
        )));
    }
    columns.describe(table)
}

/// A record's required columns: its source's `id` and `name`, the row of
/// its `window`, and its own members or MSv2's defaults.
fn required(
    (id, name, window): (i32, &str, i32),
    record: &SourceRecord,
    time_range: [f64; 2],
    notes: &mut Notes,
) -> Result<Vec<(&'static str, casa_types::Value)>, ObservationError> {
    let time = record.time.as_ref().map(utc_seconds).transpose()?;
    let direction = record.direction.as_ref().map(|direction| {
        let (longitude, latitude) = direction.as_angles();
        [longitude, latitude]
    });
    Ok(vec![
        (
            "CALIBRATION_GROUP",
            i(notes.or(record.calibration_group, "SOURCE CALIBRATION_GROUP", || -1)),
        ),
        (
            "CODE",
            s(&notes.or(record.code.clone(), "SOURCE CODE", String::new)),
        ),
        (
            "DIRECTION",
            f64_array(
                &notes.or(direction, "SOURCE DIRECTION", || [0.0, 0.0]),
                vec![2],
            ),
        ),
        (
            "INTERVAL",
            f(notes.or(record.interval_s, "SOURCE INTERVAL", || {
                time_range[1] - time_range[0]
            })),
        ),
        ("NAME", s(name)),
        (
            "NUM_LINES",
            i(i32::try_from(record.num_lines).map_err(|_| request_error("too many lines"))?),
        ),
        (
            "PROPER_MOTION",
            f64_array(
                &notes.or(record.proper_motion_rad_s, "SOURCE PROPER_MOTION", || {
                    [0.0, 0.0]
                }),
                vec![2],
            ),
        ),
        ("SOURCE_ID", i(id)),
        ("SPECTRAL_WINDOW_ID", i(window)),
        (
            "TIME",
            f(notes.or(time, "SOURCE TIME", || {
                0.5 * (time_range[0] + time_range[1])
            })),
        ),
    ])
}

/// The optional `SOURCE` columns some record of the catalogue states, and
/// the one frame each measure column's records share.
struct Columns {
    direction: DirectionRef,
    transitions: bool,
    rest: Option<String>,
    /// `SYSVEL`, and its frame when stated.
    velocities: Option<Option<String>>,
    positions: bool,
    pulsars: bool,
}

impl Columns {
    /// Add to `table` the optional columns `catalogue`'s records state,
    /// refusing a measure column whose records are in several frames.
    fn add(table: &mut Table, catalogue: &Catalogue) -> Result<Self, ObservationError> {
        let records: Vec<&SourceRecord> = catalogue
            .sources
            .iter()
            .flat_map(|source| &source.records)
            .collect();
        let direction = one_frame(
            records
                .iter()
                .filter_map(|record| record.direction.as_ref().map(|d| d.refer())),
            "writing MSv2 source directions in several frames",
        )?
        .unwrap_or(DirectionRef::J2000);
        let rest = one_frame(
            records
                .iter()
                .filter_map(|record| record.rest_frequencies.as_ref())
                .flatten()
                .map(|frequency| frequency.refer().as_str().to_string()),
            "writing MSv2 rest frequencies in several frames",
        )?;
        let any_rest = records
            .iter()
            .any(|record| record.rest_frequencies.is_some());
        let velocity_frame = one_frame(
            records
                .iter()
                .filter_map(|record| record.systemic_velocities.as_ref())
                .flatten()
                .map(|velocity| velocity.frame.map(|frame| frame.as_str().to_string())),
            "writing MSv2 systemic velocities in several frames, or some unstated",
        )?;
        let any_velocity = records
            .iter()
            .any(|record| record.systemic_velocities.is_some());
        let columns = Self {
            direction,
            transitions: records.iter().any(|record| record.transitions.is_some()),
            rest: rest.or_else(|| any_rest.then(|| "LSRK".to_string())),
            velocities: any_velocity.then(|| velocity_frame.flatten()),
            positions: records.iter().any(|record| record.position.is_some()),
            pulsars: records.iter().any(|record| record.pulsar_id.is_some()),
        };
        // In casacore's order (MSSourceEnums), as CASA's writers add them.
        for (name, wanted) in [
            ("POSITION", columns.positions),
            ("PULSAR_ID", columns.pulsars),
            ("REST_FREQUENCY", columns.rest.is_some()),
            ("SYSVEL", columns.velocities.is_some()),
            ("TRANSITION", columns.transitions),
        ] {
            if !wanted {
                continue;
            }
            let definition = optional_columns(SubtableId::Source)
                .iter()
                .find(|definition| definition.name == name)
                .expect("an optional SOURCE column");
            // A scalar or fixed-shape column is added with the default every
            // cell then has until its row is written: MSv2's.
            let (schema, default) = match name {
                "PULSAR_ID" => (
                    ColumnSchema::scalar(name, definition.data_type),
                    Some(i(-1)),
                ),
                "POSITION" => (
                    ColumnSchema::array_fixed(name, definition.data_type, vec![3]),
                    Some(f64_array(&[0.0; 3], vec![3])),
                ),
                _ => (
                    ColumnSchema::array_variable(name, definition.data_type, Some(1)),
                    None,
                ),
            };
            table.add_column(schema, default).map_err(storage)?;
        }
        Ok(columns)
    }

    /// Add `record`'s optional cells to `row`: a stated line array or
    /// position, and its pulsar, MSv2's default when the column is written
    /// but the record states none.
    fn optional(
        &self,
        row: &mut casa_types::RecordValue,
        record: &SourceRecord,
        notes: &mut Notes,
    ) -> Result<(), ObservationError> {
        let lines = record.num_lines as usize;
        let length = |count: usize, column: &str| {
            if count == lines {
                Ok(())
            } else {
                Err(request_error(format!(
                    "a SOURCE record of {lines} lines states {count} in {column}"
                )))
            }
        };
        if let Some(transitions) = &record.transitions {
            length(transitions.len(), "TRANSITION")?;
            let names: Vec<&str> = transitions.iter().map(String::as_str).collect();
            row.push(RecordField::new(
                "TRANSITION",
                string_array(&names, vec![lines]),
            ));
        }
        if let Some(frequencies) = &record.rest_frequencies {
            length(frequencies.len(), "REST_FREQUENCY")?;
            let hz: Vec<f64> = frequencies.iter().map(|frequency| frequency.hz()).collect();
            row.push(RecordField::new(
                "REST_FREQUENCY",
                f64_array(&hz, vec![lines]),
            ));
        }
        if let Some(velocities) = &record.systemic_velocities {
            length(velocities.len(), "SYSVEL")?;
            let values: Vec<f64> = velocities
                .iter()
                .map(|velocity| velocity.value_m_s)
                .collect();
            row.push(RecordField::new("SYSVEL", f64_array(&values, vec![lines])));
        }
        if self.positions {
            let position = record.position.as_ref().map(|position| position.as_itrf());
            let position = notes.or(position, "SOURCE POSITION", || [0.0; 3]);
            row.push(RecordField::new("POSITION", f64_array(&position, vec![3])));
        }
        if self.pulsars {
            let pulsar = notes.or(record.pulsar_id, "SOURCE PULSAR_ID", || -1);
            row.push(RecordField::new("PULSAR_ID", i(pulsar)));
        }
        Ok(())
    }

    /// Write the measure and unit descriptions of the columns written.
    fn describe(&self, table: &mut Table) -> Result<(), ObservationError> {
        TableMeasDesc::new_fixed("DIRECTION", MeasureType::Direction, self.direction.as_str())
            .write(table)
            .map_err(storage)?;
        if let Some(frame) = &self.rest {
            TableMeasDesc::new_fixed("REST_FREQUENCY", MeasureType::Frequency, frame)
                .write(table)
                .map_err(storage)?;
            TableQuantumDesc::with_unit("REST_FREQUENCY", "Hz")
                .write(table)
                .map_err(storage)?;
        }
        if let Some(frame) = &self.velocities {
            if let Some(frame) = frame {
                TableMeasDesc::new_fixed("SYSVEL", MeasureType::RadialVelocity, frame)
                    .write(table)
                    .map_err(storage)?;
            }
            TableQuantumDesc::with_unit("SYSVEL", "m/s")
                .write(table)
                .map_err(storage)?;
        }
        if self.positions {
            TableMeasDesc::new_fixed("POSITION", MeasureType::Position, "ITRF")
                .write(table)
                .map_err(storage)?;
            TableQuantumDesc::with_unit("POSITION", "m")
                .write(table)
                .map_err(storage)?;
        }
        Ok(())
    }
}

/// The one value of `frames`, `None` when there is none; several are
/// refused as `what`.
fn one_frame<T: PartialEq>(
    frames: impl Iterator<Item = T>,
    what: &'static str,
) -> Result<Option<T>, ObservationError> {
    let mut one = None;
    for frame in frames {
        match &one {
            None => one = Some(frame),
            Some(known) if *known == frame => {}
            Some(_) => return Err(ObservationError::Unsupported { what }),
        }
    }
    Ok(one)
}

/// One `SOURCE` row per source the fields name and per window written, at
/// the first such field's direction: what a catalogue without sources says
/// of them.
fn from_fields(
    ms: &mut MeasurementSet,
    named: &NamedSources,
    window_rows: &[Option<i32>],
    time_range: [f64; 2],
    notes: &mut Notes,
) -> Result<(), ObservationError> {
    if named.0.is_empty() {
        return Ok(());
    }
    notes.defaulted("SOURCE (rows written from the fields' source names)");
    let table = subtable_mut(ms, SubtableId::Source).map_err(storage)?;
    for (id, (name, direction)) in named.0.iter().enumerate() {
        for window in window_rows.iter().flatten() {
            let row = row_from_defs(
                schema::source::REQUIRED_COLUMNS,
                &[
                    ("CALIBRATION_GROUP", i(-1)),
                    ("CODE", s("")),
                    ("DIRECTION", f64_array(direction, vec![2])),
                    ("INTERVAL", f(time_range[1] - time_range[0])),
                    ("NAME", s(name)),
                    ("NUM_LINES", i(0)),
                    ("PROPER_MOTION", f64_array(&[0.0, 0.0], vec![2])),
                    ("SOURCE_ID", i(id as i32)),
                    ("SPECTRAL_WINDOW_ID", i(*window)),
                    ("TIME", f(0.5 * (time_range[0] + time_range[1]))),
                ],
            );
            table.add_row(row).map_err(storage)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use casa_observation::{FieldEntry, FieldEphemeris};
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::radial_velocity::RadialVelocityRef;

    use super::{SECONDS_PER_DAY, source_direction};

    /// A moving field's source is the body it follows at the field's time
    /// (clamped to the samples), not its offset; a fixed field's is its
    /// phase centre.
    #[test]
    fn a_moving_fields_source_is_its_body_at_the_fields_time() {
        let times = vec![60_000.0, 60_000.01, 60_000.02];
        let directions = vec![[1.0, 0.2], [1.01, 0.21], [1.02, 0.22]];
        let ephemeris = FieldEphemeris::new(
            "comet",
            times.clone(),
            directions.clone(),
            DirectionRef::J2000,
            vec![3.8e8; 3],
            (vec![10.0; 3], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris");
        let offset = MDirection::from_angles(1.0e-5, -5.0e-6, DirectionRef::J2000);
        let field = FieldEntry {
            name: "comet".to_string(),
            phase_centre: offset.clone(),
            reference_epoch: None,
            source_name: Some("comet".to_string()),
            source: None,
            code: None,
            delay_centre: None,
            reference_centre: None,
            flagged: None,
            ephemeris: Some(Arc::new(ephemeris)),
        };
        let close = |found: [f64; 2], expected: [f64; 2]| {
            assert!(
                (found[0] - expected[0]).abs() < 1.0e-12
                    && (found[1] - expected[1]).abs() < 1.0e-12,
                "{found:?} is not {expected:?}"
            );
        };
        close(
            source_direction(&field, times[1] * SECONDS_PER_DAY).expect("a direction"),
            directions[1],
        );
        close(
            source_direction(&field, (times[2] + 1.0) * SECONDS_PER_DAY).expect("clamped"),
            directions[2],
        );
        let fixed = FieldEntry {
            ephemeris: None,
            ..field
        };
        let (longitude, latitude) = offset.as_angles();
        close(
            source_direction(&fixed, 0.0).expect("a direction"),
            [longitude, latitude],
        );
    }
}
