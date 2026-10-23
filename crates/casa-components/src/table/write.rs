// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing a component list as CASA's `createTable` and `writeTable` do.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_tables::{ColumnBinding, DataManagerKind, Table, TableError, TableInfo, TableOptions};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use super::measures::casacore_angles;
use super::spectral_record::spectral_record;
use super::{
    DIRECTION_ERROR, DIRECTION_ERROR_UNITS, DIRECTION_FRAME, FLUX, FLUX_ERROR, FLUX_POLARISATION,
    FLUX_UNIT, FREQUENCY_ERROR, FREQUENCY_ERROR_UNITS, FREQUENCY_FRAME, LABEL, OPTIONAL_PARAMETERS,
    OPTIONAL_PARAMETERS_TILE, REFERENCE_DIRECTION, REFERENCE_FREQUENCY, SHAPE, SHAPE_ERROR,
    SHAPE_PARAMETERS, SPECTRAL_ERROR, SPECTRAL_PARAMETERS, SPECTRAL_RECORD, SPECTRUM_SHAPE,
    TABLE_INFO_README, TABLE_INFO_TYPE, component_list_schema,
};
use crate::component::SkyComponent;
use crate::component_type::ShapeType;
use crate::error::{ComponentError, ComponentListError};
use crate::list::ComponentList;

/// Write `list` as a new component-list table at `path`.
pub(crate) fn write_component_list(
    list: &ComponentList,
    path: &Path,
) -> Result<(), ComponentListError> {
    if path.exists() {
        return Err(ComponentListError::Exists(path.to_path_buf()));
    }
    let table = build_table(list, path)?;
    let staging = prepare_staging(path)?;
    let saved = save(&table, &staging, has_limb_darkened_disk(list));
    if let Err(source) = saved {
        let _ = fs::remove_dir_all(&staging);
        return Err(ComponentListError::Table {
            path: path.to_path_buf(),
            source,
        });
    }
    publish(&staging, path)
}

/// CASA describes `Optional_Parameters` once a limb-darkened disk has been
/// added to the list.
fn has_limb_darkened_disk(list: &ComponentList) -> bool {
    list.iter()
        .any(|component| component.shape().shape_type() == ShapeType::LimbDarkenedDisk)
}

/// The in-memory table: `createTable`'s description, keywords and table
/// info, and one row per component as `writeTable` fills it.
fn build_table(list: &ComponentList, path: &Path) -> Result<Table, ComponentListError> {
    let table_error = |source| ComponentListError::Table {
        path: path.to_path_buf(),
        source,
    };
    let optional_parameters = has_limb_darkened_disk(list);
    let mut table = Table::with_schema(component_list_schema(optional_parameters));
    table.set_info(TableInfo {
        table_type: TABLE_INFO_TYPE.to_string(),
        sub_type: String::new(),
        readme: TABLE_INFO_README
            .iter()
            .map(|line| (*line).to_string())
            .collect(),
    });
    write_measure_keywords(&mut table).map_err(table_error)?;
    for (index, component) in list.iter().enumerate() {
        let row = component_row(component, optional_parameters).map_err(|source| {
            ComponentListError::Unwritable {
                path: path.to_path_buf(),
                index,
                source,
            }
        })?;
        table.add_row(row).map_err(table_error)?;
    }
    Ok(table)
}

/// The measure and quantum keywords `createTable` writes, in its order:
/// casacore's `TableMeasDesc::write` puts `QuantumUnits` before `MEASINFO`,
/// and the error columns name their units columns (`VariableUnits`).
fn write_measure_keywords(table: &mut Table) -> Result<(), TableError> {
    TableQuantumDesc::with_units(REFERENCE_DIRECTION, &["rad", "rad"]).write(table)?;
    TableMeasDesc::new_variable_string(
        REFERENCE_DIRECTION,
        MeasureType::Direction,
        DIRECTION_FRAME,
    )
    .write(table)?;
    TableQuantumDesc::with_variable_units(DIRECTION_ERROR, DIRECTION_ERROR_UNITS).write(table)?;
    TableQuantumDesc::with_units(REFERENCE_FREQUENCY, &["Hz"]).write(table)?;
    TableMeasDesc::new_variable_string(
        REFERENCE_FREQUENCY,
        MeasureType::Frequency,
        FREQUENCY_FRAME,
    )
    .write(table)?;
    TableQuantumDesc::with_variable_units(FREQUENCY_ERROR, FREQUENCY_ERROR_UNITS).write(table)
}

/// One row, as `writeTable` fills it for `component`.
fn component_row(
    component: &SkyComponent,
    optional_parameters: bool,
) -> Result<RecordValue, ComponentError> {
    let flux = component.flux();
    let shape = component.shape();
    let direction_error = shape.direction_error();
    let spectrum = component.spectrum();
    let frequency = spectrum.reference_frequency();
    let frequency_error = spectrum.reference_frequency_error();
    let mut fields = vec![
        field(
            FLUX,
            Value::Array(ArrayValue::from_complex64_vec(flux.values().to_vec())),
        ),
        field(FLUX_UNIT, string(flux.unit().name())),
        field(FLUX_POLARISATION, string(flux.polarisation().casa_name())),
        field(
            FLUX_ERROR,
            Value::Array(ArrayValue::from_complex64_vec(flux.errors().to_vec())),
        ),
        field(SHAPE, string(shape.shape_type().casa_name())),
        field(
            REFERENCE_DIRECTION,
            reals(casacore_angles(shape.reference_direction()).to_vec()),
        ),
        field(
            DIRECTION_FRAME,
            string(shape.reference_direction().refer().as_str()),
        ),
        // `writeTable` puts the latitude error first.
        field(
            DIRECTION_ERROR,
            reals(vec![
                direction_error.latitude().value(),
                direction_error.longitude().value(),
            ]),
        ),
        field(
            DIRECTION_ERROR_UNITS,
            Value::Array(ArrayValue::from_string_vec(vec![
                direction_error.latitude().unit().name().to_string(),
                direction_error.longitude().unit().name().to_string(),
            ])),
        ),
        field(SHAPE_PARAMETERS, reals(shape.parameters())),
        field(SHAPE_ERROR, reals(shape.errors())),
        field(SPECTRUM_SHAPE, string(spectrum.spectral_type().casa_name())),
        field(REFERENCE_FREQUENCY, real(frequency.hz())),
        field(FREQUENCY_FRAME, string(frequency.refer().as_str())),
        field(FREQUENCY_ERROR, real(frequency_error.value())),
        field(FREQUENCY_ERROR_UNITS, string(frequency_error.unit().name())),
        field(SPECTRAL_PARAMETERS, reals(spectrum.parameters())),
        field(SPECTRAL_ERROR, reals(spectrum.errors())),
        field(LABEL, string(component.label())),
    ];
    // CASA fills Optional_Parameters only for limb-darkened disks, leaving the
    // other rows' cells undefined. An empty vector is left undefined too: CASA
    // reads a defined cell's first value as the exponent.
    if optional_parameters
        && shape.shape_type() == ShapeType::LimbDarkenedDisk
        && !component.optional_parameters().is_empty()
    {
        fields.push(field(
            OPTIONAL_PARAMETERS,
            reals(component.optional_parameters().to_vec()),
        ));
    }
    fields.push(field(
        SPECTRAL_RECORD,
        Value::Record(spectral_record(spectrum)?),
    ));
    Ok(RecordValue::new(fields))
}

/// Save into `staging`: every column in one `StandardStMan`, as CASA's
/// default binding, and `Optional_Parameters` in a `TiledCellStMan` with
/// one-element tiles, as `createTable` binds it.
fn save(table: &Table, staging: &Path, optional_parameters: bool) -> Result<(), TableError> {
    let mut bindings = HashMap::new();
    if optional_parameters {
        bindings.insert(
            OPTIONAL_PARAMETERS.to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::TiledCellStMan,
                tile_shape: Some(OPTIONAL_PARAMETERS_TILE.to_vec()),
            },
        );
    }
    table.save_with_bindings(
        TableOptions::new(staging).with_data_manager(DataManagerKind::StandardStMan),
        &bindings,
    )
}

/// Reserve a staging directory beside `path` (ADR-0008: a new table is built
/// out of sight and published complete).
fn prepare_staging(path: &Path) -> Result<PathBuf, ComponentListError> {
    let publish_error = |reason: String| ComponentListError::Publish {
        path: path.to_path_buf(),
        reason,
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| publish_error("the output path must name a table".to_string()))?
        .to_string_lossy();
    let staging = parent.join(format!(".{name}.casa-rs-incomplete-{}", std::process::id()));
    fs::create_dir(&staging).map_err(|error| {
        publish_error(format!(
            "cannot reserve staging path {} (an interrupted or concurrent write may exist): {error}",
            staging.display()
        ))
    })?;
    Ok(staging)
}

/// Move the finished table into place, refusing a destination that
/// appeared meanwhile.
fn publish(staging: &Path, path: &Path) -> Result<(), ComponentListError> {
    if path.exists() {
        let _ = fs::remove_dir_all(staging);
        return Err(ComponentListError::Exists(path.to_path_buf()));
    }
    fs::rename(staging, path).map_err(|error| {
        let _ = fs::remove_dir_all(staging);
        ComponentListError::Publish {
            path: path.to_path_buf(),
            reason: format!("cannot publish {}: {error}", staging.display()),
        }
    })
}

fn field(name: &str, value: Value) -> RecordField {
    RecordField::new(name, value)
}

fn string(text: &str) -> Value {
    Value::Scalar(ScalarValue::String(text.to_string()))
}

fn real(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn reals(values: Vec<f64>) -> Value {
    Value::Array(ArrayValue::from_f64_vec(values))
}
