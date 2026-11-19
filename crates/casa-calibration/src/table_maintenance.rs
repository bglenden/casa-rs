// SPDX-License-Identifier: LGPL-3.0-or-later
//! Empty NewCalTable construction and pre-4.1 observation metadata repair.

use std::path::Path;

use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, derived::engine::MsCalEngine, schema::SubtableId,
};
use casa_tables::{ColumnSchema, Table, TableOptions, TableWriteLock};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::CalibrationOutputMode;
use crate::constants::{COL_OBSERVATION_ID, TABLE_INFO_TYPE};
use crate::paths::overlap;
use crate::solution_edit::{error_text, stage_calibration_table_edit};
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, SingleChannelFrequency,
    make_windows_single_channel, new_calibration_table_schema,
};

/// Create an empty calibration table against an open MeasurementSet.
///
/// CASA selects a complex parameter column only for `partype == "Complex"`.
/// All other strings select the real column. The caltype is stored verbatim.
/// Existing calibration tables can be replaced when the new table is complete.
///
/// # Errors
/// Returns a description of table construction, metadata or publication errors.
pub fn create_calibration_table(
    measurement_set: &MeasurementSet,
    destination: &Path,
    partype: &str,
    caltype: &str,
    single_channel: bool,
) -> Result<(), String> {
    let writer = prepare_empty_calibration_table(
        measurement_set,
        destination,
        partype,
        caltype,
        single_channel.then_some(SingleChannelFrequency::MiddleChannel),
    )?;
    let _destination_lock = if destination.exists() {
        Some(TableWriteLock::acquire_to_reread(destination, 1).map_err(error_text)?)
    } else {
        None
    };
    writer.save_main().map_err(error_text)
}

pub(crate) fn prepare_empty_calibration_table<'a>(
    measurement_set: &'a MeasurementSet,
    destination: &Path,
    partype: &str,
    caltype: &str,
    single_channel: Option<SingleChannelFrequency>,
) -> Result<CalibrationTableWriter<'a>, String> {
    if destination.as_os_str().is_empty() {
        return Err("an output calibration table is required".into());
    }
    if let Some(source) = measurement_set.path()
        && overlap(source, destination).map_err(error_text)?
    {
        return Err("the MeasurementSet and output calibration table overlap".into());
    }
    let complex = partype == "Complex";
    let writer = CalibrationTableWriter::with_mode(
        measurement_set,
        CalibrationTableDescriptor {
            output: destination,
            schema: new_calibration_table_schema(complex),
            subtype: caltype,
            parameter_type: Some(if complex { "Complex" } else { "Float" }),
            measurement_set_name: measurement_set
                .path()
                .map_or_else(|| "none".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        CalibrationOutputMode::Replace,
    )
    .map_err(error_text)?;
    let mut template =
        MeasurementSet::create_memory(MeasurementSetBuilder::new()).map_err(error_text)?;
    for id in [
        SubtableId::Observation,
        SubtableId::Antenna,
        SubtableId::Field,
        SubtableId::SpectralWindow,
        SubtableId::History,
    ] {
        let table = template.subtable_mut(id).expect("required MS subtable");
        if id != SubtableId::History {
            let source = measurement_set
                .subtable(id)
                .ok_or_else(|| format!("missing {} subtable", id.name()))?;
            copy_standard_rows(source, table)?;
            if id == SubtableId::Field {
                copy_moving_field_directions(measurement_set, table)?;
            }
        }
        table
            .save(TableOptions::new(writer.output_path().join(id.name())))
            .map_err(error_text)?;
    }
    if let Some(frequency) = single_channel {
        make_windows_single_channel(&writer.output_path().join("SPECTRAL_WINDOW"), frequency)
            .map_err(error_text)?;
    }
    Ok(writer)
}

fn copy_standard_rows(source: &Table, target: &mut Table) -> Result<(), String> {
    let columns = target
        .schema()
        .expect("required MS subtable schema")
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    for (row, undefined) in source
        .rows()
        .map_err(error_text)?
        .iter()
        .zip(source.undefined_cells().map_err(error_text)?)
    {
        let values = columns
            .iter()
            .filter(|column| !undefined.contains(column.as_str()))
            .filter_map(|column| {
                row.get(column)
                    .cloned()
                    .map(|value| RecordField::new(column, value))
            })
            .collect();
        target
            .add_row(RecordValue::new(values))
            .map_err(error_text)?;
    }
    Ok(())
}

fn copy_moving_field_directions(ms: &MeasurementSet, target: &mut Table) -> Result<(), String> {
    let field = ms.field().map_err(error_text)?;
    let mut engine = None;
    for row in 0..field.row_count() {
        if !field
            .ephemeris_id(row)
            .map_err(error_text)?
            .is_some_and(|id| id >= 0)
        {
            continue;
        }
        let engine = match &mut engine {
            Some(engine) => engine,
            empty => empty.insert(MsCalEngine::for_calibration(ms).map_err(error_text)?),
        };
        let ArrayValue::Float64(offset) = field.reference_dir(row).map_err(error_text)? else {
            return Err("invalid moving field REFERENCE_DIR".into());
        };
        let offset = [offset.get([0, 0]), offset.get([1, 0])].map(|coord| coord.copied());
        let [Some(longitude), Some(latitude)] = offset else {
            return Err("invalid moving field REFERENCE_DIR dimensions".into());
        };
        let time = field.time(row).map_err(error_text)?;
        let direction = engine
            .moving_field_reference_direction_with_offset(row, time, Some([longitude, latitude]))
            .map_err(error_text)?
            .ok_or("moving field reference direction is missing")?;
        let (longitude, latitude) = direction.as_angles();
        let cell = Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![longitude, latitude])
                .expect("a direction has two coordinates"),
        ));
        for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
            target
                .cell_accessor_mut(row, column)
                .and_then(|mut accessor| accessor.set(cell.clone()))
                .map_err(error_text)?;
        }
    }
    Ok(())
}

/// Add the pre-4.1 OBSERVATION metadata while retaining existing solution rows.
///
/// # Errors
/// Returns a description of read, schema, or publication errors.
pub fn update_calibration_table(path: &Path) -> Result<(), String> {
    let table = Table::open(TableOptions::new(path)).map_err(error_text)?;
    if table.info().table_type != TABLE_INFO_TYPE {
        return Err("input is not a calibration table".into());
    }
    if has_observation_column(&table) && table.keywords().get("OBSERVATION").is_some() {
        return Ok(());
    }
    drop(table);
    stage_calibration_table_edit(path, path, |table, stage| {
        if !has_observation_column(table) {
            table
                .add_column(
                    ColumnSchema::scalar(COL_OBSERVATION_ID, PrimitiveType::Int32),
                    Some(Value::Scalar(ScalarValue::Int32(0))),
                )
                .map_err(error_text)?;
        }
        if table.keywords().get("OBSERVATION").is_none() {
            let observation = stage.join("OBSERVATION");
            if observation.exists() {
                return Err("the table already contains an unlinked OBSERVATION subtable".into());
            }
            write_generic_observation(&observation)?;
            table.keywords_mut().upsert(
                "OBSERVATION",
                Value::table_ref(casa_tables::strip_directory(&observation, stage)),
            );
        }
        table.save(TableOptions::new(stage)).map_err(error_text)
    })
}

fn has_observation_column(table: &Table) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(COL_OBSERVATION_ID))
}

fn write_generic_observation(path: &Path) -> Result<(), String> {
    let mut template =
        MeasurementSet::create_memory(MeasurementSetBuilder::new()).map_err(error_text)?;
    let table = template
        .subtable_mut(SubtableId::Observation)
        .expect("required OBSERVATION subtable");
    let mut row = RecordValue::new(vec![
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(0.0))),
        RecordField::new(
            "SCHEDULE_TYPE",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new(
            "TIME_RANGE",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]), vec![0.0, 7_609_161_600.0])
                    .expect("a pair of observation limits"),
            )),
        ),
    ]);
    for name in ["OBSERVER", "PROJECT", "TELESCOPE_NAME"] {
        row.upsert(name, Value::Scalar(ScalarValue::String("unknown".into())));
    }
    table.add_row(row).map_err(error_text)?;
    table.save(TableOptions::new(path)).map_err(error_text)
}
