// SPDX-License-Identifier: LGPL-3.0-or-later
//! Empty NewCalTable construction and pre-4.1 observation metadata repair.

use std::path::Path;

use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, derived::engine::MsCalEngine, schema::SubtableId,
};
use casa_tables::{
    ColumnAddition, ColumnSchema, ManagerAddition, Table, TableOptions, TableWriteLock,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::CalibrationOutputMode;
use crate::constants::{COL_OBSERVATION_ID, TABLE_INFO_TYPE};
use crate::paths::{normalized, overlap, subtable_owner};
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
    if overlaps_measurement_set(measurement_set, destination)? {
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
    write_standard_calibration_subtables(measurement_set, writer.output_path(), single_channel)?;
    Ok(writer)
}

pub(crate) fn write_standard_calibration_subtables(
    measurement_set: &MeasurementSet,
    staging: &Path,
    single_channel: Option<SingleChannelFrequency>,
) -> Result<(), String> {
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
            .save(TableOptions::new(staging.join(id.name())))
            .map_err(error_text)?;
    }
    if let Some(frequency) = single_channel {
        make_windows_single_channel(&staging.join("SPECTRAL_WINDOW"), frequency)
            .map_err(error_text)?;
    }
    Ok(())
}

/// Return whether `destination` lives inside or contains the MeasurementSet tree.
pub fn overlaps_measurement_set(ms: &MeasurementSet, destination: &Path) -> Result<bool, String> {
    let Some(source) = ms.path() else {
        return Ok(false);
    };
    let source = normalized(source).map_err(error_text)?;
    let destination = normalized(destination).map_err(error_text)?;
    if source.starts_with(&destination) {
        return Ok(true);
    }
    // CASA can publish an independent calibration table inside the MS.
    // Its storage files and subtables remain reserved to the owning table.
    if let Ok(relative) = destination.strip_prefix(&source)
        && let Some(first) = relative.iter().next()
        && (first.to_string_lossy().starts_with("table.")
            || SubtableId::ALL_REQUIRED
                .iter()
                .chain(SubtableId::ALL_OPTIONAL)
                .any(|subtable| first == subtable.name()))
    {
        return Ok(true);
    }
    let main = ms.main_table();
    let owner = subtable_owner(main, &source).map_err(error_text)?;
    for link in main
        .keywords()
        .fields()
        .iter()
        .filter_map(|field| field.value.as_table_ref())
    {
        if overlap(&destination, &casa_tables::add_directory(link, &owner)).map_err(error_text)? {
            return Ok(true);
        }
    }
    Ok(false)
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
        let longitude = if longitude > std::f64::consts::PI {
            longitude - std::f64::consts::TAU
        } else {
            longitude
        };
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
    if !path.exists() {
        return Err(format!("Table {} does not exist", path.display()));
    }
    let table = Table::open(TableOptions::new(path)).map_err(error_text)?;
    if has_observation_column(&table) && table.keywords().get("OBSERVATION").is_some() {
        return Ok(());
    }
    if table.info().table_type != TABLE_INFO_TYPE {
        return Err("input is not a calibration table".into());
    }
    drop(table);
    stage_calibration_table_edit(path, path, |table, stage| {
        link_observation_subtable(table, stage)?;
        table.save_keywords_atomically(stage).map_err(error_text)?;
        if !has_observation_column(table) {
            // A separate manager supplies the new zero-filled column while
            // preserving every existing solution and flag storage file.
            table
                .add_columns_in_place(
                    vec![ColumnAddition {
                        schema: ColumnSchema::scalar(COL_OBSERVATION_ID, PrimitiveType::Int32),
                        default: Some(Value::Scalar(ScalarValue::Int32(0))),
                        keywords: RecordValue::default(),
                        manager_type: None,
                        manager_group: None,
                    }],
                    Some(ManagerAddition {
                        kind: "StandardStMan".into(),
                        name: String::new(),
                        spec: RecordValue::default(),
                    }),
                )
                .map_err(error_text)?;
        }
        Ok(())
    })
}

pub(crate) fn repair_observation_metadata(table: &mut Table, stage: &Path) -> Result<(), String> {
    if !has_observation_column(table) {
        table
            .add_column(
                ColumnSchema::scalar(COL_OBSERVATION_ID, PrimitiveType::Int32),
                Some(Value::Scalar(ScalarValue::Int32(0))),
            )
            .map_err(error_text)?;
    }
    link_observation_subtable(table, stage)
}

fn link_observation_subtable(table: &mut Table, stage: &Path) -> Result<(), String> {
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
    Ok(())
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
