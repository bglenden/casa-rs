// SPDX-License-Identifier: LGPL-3.0-or-later
//! MeasurementSet metadata shared by single-dish imports.

use casa_core::{RecordValue, ScalarValue, Value};
use casa_measures::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{Table, table_quantum::TableQuantumDesc};

use super::{Direction, Field, ImportHeader, Source};
use casa_ms::schema::{self, SubtableId};
use casa_ms::subtable_rows::{b, f, f64_array, i, row_from_defs, s, string_array, subtable_mut};
use casa_ms::{ColumnKind, column_schema};
use casa_ms::{MeasurementSet, MeasurementSetBuilder, MsError, MsResult, OptionalMainColumn};

pub(super) fn create(
    header: &ImportHeader,
    column: OptionalMainColumn,
    fields: &[Field],
    sources: &[Source],
) -> MsResult<MeasurementSet> {
    let builder = SubtableId::ALL_OPTIONAL.iter().fold(
        MeasurementSetBuilder::new().with_main_column(column),
        |builder, &id| builder.with_optional_subtable(id),
    );
    let mut ms = MeasurementSet::create_memory(builder)?;
    let mut data_keywords = RecordValue::default();
    data_keywords.upsert("UNIT", s(&header.data_unit));
    ms.main_table_mut()
        .set_column_keywords(column.name(), data_keywords);
    add_fixed_tables(&mut ms, header)?;
    add_windows(&mut ms, header)?;
    add_sources(&mut ms, sources)?;
    add_fields(&mut ms, fields)?;
    for (id, columns) in [
        (
            SubtableId::Field,
            &["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"][..],
        ),
        (SubtableId::Pointing, &["DIRECTION", "TARGET"][..]),
        (SubtableId::Source, &["DIRECTION"][..]),
    ] {
        for column in columns {
            TableMeasDesc::new_fixed(column, MeasureType::Direction, header.direction_frame)
                .write(subtable_mut(&mut ms, id)?)?;
        }
    }
    add_optional(
        subtable_mut(&mut ms, SubtableId::SysCal)?,
        SubtableId::SysCal,
        &["TCAL", "TCAL_SPECTRUM", "TSYS", "TSYS_SPECTRUM"],
    )?;
    add_optional(
        subtable_mut(&mut ms, SubtableId::Weather)?,
        SubtableId::Weather,
        &[
            "PRESSURE",
            "REL_HUMIDITY",
            "TEMPERATURE",
            "WIND_DIRECTION",
            "WIND_SPEED",
        ],
    )?;
    Ok(ms)
}

fn add_fixed_tables(ms: &mut MeasurementSet, header: &ImportHeader) -> MsResult<()> {
    for antenna in &header.antenna {
        insert(
            ms,
            SubtableId::Antenna,
            &[
                ("DISH_DIAMETER", f(antenna.diameter)),
                ("NAME", s(&antenna.name)),
                ("STATION", s(&antenna.station)),
                ("TYPE", s("GROUND-BASED")),
                ("MOUNT", s("ALT-AZ")),
                ("POSITION", f64_array(&antenna.position, vec![3])),
            ],
        )?;
    }
    let mut observation = row_from_defs(
        schema::observation::REQUIRED_COLUMNS,
        &[
            ("TIME_RANGE", f64_array(&header.range, vec![2])),
            ("OBSERVER", s(&header.observer)),
            ("PROJECT", s(&header.project)),
            ("TELESCOPE_NAME", s(&header.telescope)),
        ],
    );
    observation = without(observation, &["LOG", "SCHEDULE"]);
    subtable_mut(ms, SubtableId::Observation)?.add_row(observation)?;
    insert(
        ms,
        SubtableId::Processor,
        &[("MODE_ID", i(-1)), ("TYPE_ID", i(-1))],
    )?;
    Ok(())
}

fn add_windows(ms: &mut MeasurementSet, header: &ImportHeader) -> MsResult<()> {
    let table = subtable_mut(ms, SubtableId::SpectralWindow)?;
    for (&index, window) in &header.windows {
        if index < 0 || window.channels == 0 {
            return Err(MsError::InvalidInput(format!(
                "Invalid imported spectral window {index}"
            )));
        }
        while table.row_count() < index as usize {
            table.add_row(row_from_defs(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[("NUM_CHAN", i(1))],
            ))?;
        }
        let center = window.reference_frequency - window.reference_channel * window.increment;
        let mut frequencies = Vec::with_capacity(window.channels);
        let mut frequency = center;
        for _ in 0..window.channels {
            frequencies.push(frequency);
            frequency += window.increment;
        }
        let bandwidth = window.increment.abs();
        let channels = vec![window.channels];
        let spacing = f64_array(&vec![window.increment; window.channels], channels.clone());
        let resolution = f64_array(&vec![bandwidth; window.channels], channels.clone());
        let row = row_from_defs(
            schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("MEAS_FREQ_REF", i(window.frame)),
                ("NUM_CHAN", i(window.channels as i32)),
                ("REF_FREQUENCY", f(center)),
                ("CHAN_FREQ", f64_array(&frequencies, channels)),
                ("CHAN_WIDTH", spacing),
                ("EFFECTIVE_BW", resolution.clone()),
                ("RESOLUTION", resolution),
                ("TOTAL_BANDWIDTH", f(window.channels as f64 * bandwidth)),
                ("NET_SIDEBAND", i(i32::from(window.increment < 0.0))),
            ],
        );
        table.add_row(row)?;
    }
    Ok(())
}

fn add_fields(ms: &mut MeasurementSet, fields: &[Field]) -> MsResult<()> {
    for field in fields {
        debug_assert_eq!(ms.field()?.row_count(), field.id as usize);
        let (degree, direction) = direction_cell(field.direction);
        insert(
            ms,
            SubtableId::Field,
            &[
                ("TIME", f(field.time)),
                ("NAME", s(&field.name)),
                ("SOURCE_ID", i(field.source_id)),
                ("NUM_POLY", i(degree)),
                ("DELAY_DIR", direction.clone()),
                ("PHASE_DIR", direction.clone()),
                ("REFERENCE_DIR", direction),
            ],
        )?;
    }
    Ok(())
}

fn add_sources(ms: &mut MeasurementSet, sources: &[Source]) -> MsResult<()> {
    let table = subtable_mut(ms, SubtableId::Source)?;
    add_optional(
        table,
        SubtableId::Source,
        &["REST_FREQUENCY", "SYSVEL", "TRANSITION"],
    )?;
    for source in sources {
        let mut row = row_from_defs(
            schema::source::REQUIRED_COLUMNS,
            &[
                ("TIME", f(source.time)),
                ("INTERVAL", f(source.interval)),
                ("NAME", s(&source.name)),
                ("SOURCE_ID", i(source.id)),
                ("SPECTRAL_WINDOW_ID", i(source.window)),
                ("NUM_LINES", i(source.rest_frequencies.len() as i32)),
                ("CALIBRATION_GROUP", i(-1)),
                ("DIRECTION", f64_array(&source.direction, vec![2])),
                ("PROPER_MOTION", f64_array(&source.proper_motion, vec![2])),
            ],
        );
        if !source.rest_frequencies.is_empty() {
            let lines = source.rest_frequencies.len();
            row.upsert(
                "REST_FREQUENCY",
                f64_array(&source.rest_frequencies, vec![lines]),
            );
            row.upsert(
                "SYSVEL",
                f64_array(&vec![source.velocity; lines], vec![lines]),
            );
        }
        if !source.transitions.is_empty() {
            let names = source
                .transitions
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            row.upsert("TRANSITION", string_array(&names, vec![names.len()]));
        }
        table.add_row(row)?;
    }
    Ok(())
}

fn add_optional(table: &mut Table, id: SubtableId, names: &[&str]) -> MsResult<()> {
    for &name in names {
        let definition = schema::optional_columns(id)
            .iter()
            .find(|column| column.name == name)
            .expect("a defined optional MeasurementSet column");
        let default = match definition.column_kind {
            ColumnKind::Scalar => Some(Value::Scalar(ScalarValue::Float32(0.))),
            _ => None,
        };
        table.add_column(column_schema(definition), default)?;
        if let Some(measure) = definition.measure_type {
            let frame = if name == "REST_FREQUENCY" {
                "LSRK"
            } else {
                definition.measure_ref
            };
            TableMeasDesc::new_fixed(name, measure, frame).write(table)?;
        }
        if !definition.unit.is_empty() {
            TableQuantumDesc::with_unit(name, definition.unit).write(table)?;
        }
    }
    Ok(())
}

pub(super) fn insert(
    ms: &mut MeasurementSet,
    id: SubtableId,
    cells: &[(&str, Value)],
) -> MsResult<i32> {
    let table = subtable_mut(ms, id)?;
    let row = table.row_count() as i32;
    table.add_row(row_from_defs(schema::required_columns(id), cells))?;
    Ok(row)
}

pub(super) fn without(row: RecordValue, names: &[&str]) -> RecordValue {
    RecordValue::new(
        row.into_fields()
            .into_iter()
            .filter(|field| !names.contains(&field.name.as_str()))
            .collect(),
    )
}

pub(super) fn direction_cell(direction: Direction) -> (i32, Value) {
    let [lon, lat] = direction.position;
    let [dlon, dlat] = direction.rate;
    if direction.rate == [0.0; 2] {
        (0, f64_array(&[lon, lat], vec![2, 1]))
    } else {
        (1, f64_array(&[lon, dlon, lat, dlat], vec![2, 2]))
    }
}

pub(super) fn state_row(ms: &mut MeasurementSet, intent: &str, subscan: i32) -> MsResult<i32> {
    // The filler matches the entire state string against its on-source label.
    let signal = intent == "OBSERVE_TARGET#ON_SOURCE";
    insert(
        ms,
        SubtableId::State,
        &[
            ("OBS_MODE", s(intent)),
            ("SUB_SCAN", i(subscan)),
            ("SIG", b(signal)),
            ("REF", b(!signal)),
        ],
    )
}
