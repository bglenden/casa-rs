// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reading a component list as `ComponentList::readTable` does.

use std::path::Path;

use casa_tables::table_measures::MeasureType;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use num_complex::Complex64;

use super::measures::{MeasureColumn, array_f64, cell, cell_error, quantity, quantum_units};
use super::spectral_record::apply_spectral_record;
use super::{
    DIRECTION_ERROR, FLUX, FLUX_ERROR, FLUX_POLARISATION, FLUX_UNIT, FREQUENCY_ERROR, LABEL,
    OPTIONAL_PARAMETERS, REFERENCE_DIRECTION, REFERENCE_FREQUENCY, SHAPE, SHAPE_ERROR,
    SHAPE_PARAMETERS, SPECTRAL_ERROR, SPECTRAL_PARAMETERS, SPECTRAL_RECORD, SPECTRUM_SHAPE,
};
use crate::component::SkyComponent;
use crate::component_type::{Polarisation, ShapeType, SpectralType};
use crate::error::{ComponentError, ComponentListError};
use crate::flux::Flux;
use crate::list::ComponentList;
use crate::shape::{ComponentShape, DirectionError};
use crate::spectrum::{SpectralKind, SpectralModel};

/// The columns `readTable` attaches unconditionally; a table without one is
/// not a component list.
const REQUIRED_COLUMNS: [&str; 10] = [
    FLUX,
    FLUX_UNIT,
    FLUX_POLARISATION,
    SHAPE,
    REFERENCE_DIRECTION,
    SHAPE_PARAMETERS,
    SPECTRUM_SHAPE,
    REFERENCE_FREQUENCY,
    SPECTRAL_PARAMETERS,
    LABEL,
];

/// Bytes a row of a component list holds while it is read, beside its
/// columns' bytes on disk: casa-tables' loaded row and the component formed
/// from it. Reading the lists casa-components writes holds 9.5 to 9.8 KB a
/// row, for 128 to 8,192 rows (the simulator's counting law, #841).
const READ_ROW_BYTES: u64 = 10 * 1024;

/// Read the component list at `path`.
pub(crate) fn read_component_list(path: &Path) -> Result<ComponentList, ComponentListError> {
    let table = open(path)?;
    let reader = ListReader::attach(&table, path)?;
    (0..table.row_count())
        .map(|row| reader.component(row))
        .collect()
}

/// The most reading the list at `path` holds: [`READ_ROW_BYTES`] a row and
/// the table's files.
pub(crate) fn read_bytes(path: &Path) -> Result<u64, ComponentListError> {
    let rows = Table::open(TableOptions::new(path))
        .map_err(|source| ComponentListError::Table {
            path: path.to_path_buf(),
            source,
        })?
        .row_count() as u64;
    let on_disk: u64 = std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok()?.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|metadata| metadata.len())
        .sum();
    Ok(rows * READ_ROW_BYTES + on_disk)
}

/// Open the table read-only, holding casacore's read lock while it is read
/// (CASA opens it with the default `AutoLocking`).
fn open(path: &Path) -> Result<Table, ComponentListError> {
    let table_error = |source| ComponentListError::Table {
        path: path.to_path_buf(),
        source,
    };
    #[cfg(unix)]
    {
        use casa_tables::{LockMode, LockOptions};
        Table::open_with_lock(
            TableOptions::new(path),
            LockOptions::new(LockMode::AutoLocking),
        )
        .map_err(table_error)
    }
    #[cfg(not(unix))]
    {
        Table::open(TableOptions::new(path)).map_err(table_error)
    }
}

/// The optional columns: old component lists lack the error columns and
/// `Spectral_Record`, and only lists with a limb-darkened disk have
/// `Optional_Parameters`.
struct OptionalColumns {
    flux_error: bool,
    direction_error: bool,
    shape_error: bool,
    frequency_error: bool,
    spectral_error: bool,
    spectral_record: bool,
    optional_parameters: bool,
}

struct ListReader<'a> {
    table: &'a Table,
    path: &'a Path,
    direction: MeasureColumn,
    frequency: MeasureColumn,
    optional: OptionalColumns,
}

impl<'a> ListReader<'a> {
    fn attach(table: &'a Table, path: &'a Path) -> Result<Self, ComponentListError> {
        let has = |column: &str| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(column))
        };
        if let Some(column) = REQUIRED_COLUMNS.into_iter().find(|column| !has(column)) {
            return Err(ComponentListError::MissingColumn {
                path: path.to_path_buf(),
                column,
            });
        }
        Ok(Self {
            table,
            path,
            direction: MeasureColumn::attach(
                table,
                path,
                REFERENCE_DIRECTION,
                MeasureType::Direction,
            )?,
            frequency: MeasureColumn::attach(
                table,
                path,
                REFERENCE_FREQUENCY,
                MeasureType::Frequency,
            )?,
            optional: OptionalColumns {
                flux_error: has(FLUX_ERROR),
                direction_error: has(DIRECTION_ERROR),
                shape_error: has(SHAPE_ERROR),
                frequency_error: has(FREQUENCY_ERROR),
                spectral_error: has(SPECTRAL_ERROR),
                spectral_record: has(SPECTRAL_RECORD),
                optional_parameters: has(OPTIONAL_PARAMETERS),
            },
        })
    }

    /// The component in `row`, built in `readTable`'s order.
    fn component(&self, row: usize) -> Result<SkyComponent, ComponentListError> {
        let flux = self.flux(row)?;
        let shape = self.shape(row)?;
        let spectrum = self.spectrum(row)?;
        let mut component =
            SkyComponent::new(flux, shape, spectrum).with_label(self.string(row, LABEL)?);
        if self.optional.optional_parameters && self.defined(row, OPTIONAL_PARAMETERS)? {
            component.set_optional_parameters(self.reals(row, OPTIONAL_PARAMETERS)?);
        }
        Ok(component)
    }

    fn flux(&self, row: usize) -> Result<Flux, ComponentListError> {
        let values = self.complex4(row, FLUX)?;
        let unit = self.string(row, FLUX_UNIT)?;
        let polarisation = Polarisation::parse_casa(&self.string(row, FLUX_POLARISATION)?)
            .map_err(|source| self.refused(row, source))?;
        let mut flux =
            Flux::new(values, &unit, polarisation).map_err(|source| self.refused(row, source))?;
        if self.optional.flux_error {
            flux.set_errors(self.complex4(row, FLUX_ERROR)?);
        }
        Ok(flux)
    }

    fn shape(&self, row: usize) -> Result<ComponentShape, ComponentListError> {
        let shape_type = ShapeType::parse_casa(&self.string(row, SHAPE)?)
            .map_err(|source| self.refused(row, source))?;
        let direction = self.direction.direction(self.table, self.path, row)?;
        let parameters = self.reals(row, SHAPE_PARAMETERS)?;
        let errors = if self.optional.shape_error {
            Some(self.reals(row, SHAPE_ERROR)?)
        } else {
            None
        };
        let mut shape =
            ComponentShape::from_parameters(shape_type, direction, &parameters, errors.as_deref())
                .map_err(|source| self.refused(row, source))?;
        if self.optional.direction_error {
            shape.set_direction_error(self.direction_error(row)?);
        }
        Ok(shape)
    }

    /// `Direction_Error` holds `[latitude, longitude]`, each in its own unit.
    fn direction_error(&self, row: usize) -> Result<DirectionError, ComponentListError> {
        let values = self.reals(row, DIRECTION_ERROR)?;
        let [latitude, longitude] = <[f64; 2]>::try_from(values.as_slice()).map_err(|_| {
            cell_error(
                self.path,
                row,
                DIRECTION_ERROR,
                format!("expected 2 values, got {}", values.len()),
            )
        })?;
        let units = quantum_units(self.table, self.path, row, DIRECTION_ERROR, 2)?;
        DirectionError::new(
            quantity(self.path, row, DIRECTION_ERROR, latitude, &units[0])?,
            quantity(self.path, row, DIRECTION_ERROR, longitude, &units[1])?,
        )
        .map_err(|source| self.refused(row, source))
    }

    /// The spectrum: the record first, then the reference frequency, its
    /// error, the parameters and their errors from their columns.
    fn spectrum(&self, row: usize) -> Result<SpectralModel, ComponentListError> {
        let spectral_type = SpectralType::parse_casa(&self.string(row, SPECTRUM_SHAPE)?)
            .map_err(|source| self.refused(row, source))?;
        let mut kind = SpectralKind::casa_default(spectral_type);
        if self.optional.spectral_record && self.defined(row, SPECTRAL_RECORD)? {
            let record = self
                .table
                .record_cell(row, SPECTRAL_RECORD)
                .map_err(|source| ComponentListError::Table {
                    path: self.path.to_path_buf(),
                    source,
                })?;
            apply_spectral_record(&mut kind, &record)
                .map_err(|source| self.refused(row, source))?;
        }
        let frequency = self.frequency.frequency(self.table, self.path, row)?;
        let mut spectrum = SpectralModel::new(frequency, kind);
        if self.optional.frequency_error {
            let value = self.real(row, FREQUENCY_ERROR)?;
            let unit = quantum_units(self.table, self.path, row, FREQUENCY_ERROR, 1)?;
            spectrum
                .set_reference_frequency_error(quantity(
                    self.path,
                    row,
                    FREQUENCY_ERROR,
                    value,
                    &unit[0],
                )?)
                .map_err(|source| self.refused(row, source))?;
        }
        let parameters = self.reals(row, SPECTRAL_PARAMETERS)?;
        spectrum
            .set_parameters(&parameters)
            .map_err(|source| self.refused(row, source))?;
        if self.optional.spectral_error {
            let errors = self.reals(row, SPECTRAL_ERROR)?;
            // `readTable` reads the errors into the parameters' vector, which
            // `ArrayColumn::get` resizes only when it is empty.
            if !parameters.is_empty() && errors.len() != parameters.len() {
                return Err(cell_error(
                    self.path,
                    row,
                    SPECTRAL_ERROR,
                    format!(
                        "{} errors for {} spectral parameters",
                        errors.len(),
                        parameters.len()
                    ),
                ));
            }
            spectrum
                .set_errors(&errors)
                .map_err(|source| self.refused(row, source))?;
        }
        Ok(spectrum)
    }

    fn refused(&self, row: usize, source: ComponentError) -> ComponentListError {
        ComponentListError::Component {
            path: self.path.to_path_buf(),
            row,
            source,
        }
    }

    fn defined(&self, row: usize, column: &str) -> Result<bool, ComponentListError> {
        self.table
            .cell_accessor(row, column)
            .and_then(|cell| cell.is_defined())
            .map_err(|source| ComponentListError::Table {
                path: self.path.to_path_buf(),
                source,
            })
    }

    fn string(&self, row: usize, column: &str) -> Result<String, ComponentListError> {
        match cell(self.table, self.path, row, column)? {
            Value::Scalar(ScalarValue::String(text)) => Ok(text.clone()),
            _ => Err(cell_error(
                self.path,
                row,
                column,
                "expected a string".to_string(),
            )),
        }
    }

    fn real(&self, row: usize, column: &str) -> Result<f64, ComponentListError> {
        match cell(self.table, self.path, row, column)? {
            Value::Scalar(ScalarValue::Float64(value)) => Ok(*value),
            Value::Scalar(ScalarValue::Float32(value)) => Ok(f64::from(*value)),
            _ => Err(cell_error(
                self.path,
                row,
                column,
                "expected a real number".to_string(),
            )),
        }
    }

    fn reals(&self, row: usize, column: &str) -> Result<Vec<f64>, ComponentListError> {
        match cell(self.table, self.path, row, column)? {
            Value::Array(array) => array_f64(array),
            _ => None,
        }
        .ok_or_else(|| cell_error(self.path, row, column, "expected a real array".to_string()))
    }

    fn complex4(&self, row: usize, column: &str) -> Result<[Complex64; 4], ComponentListError> {
        let values: Vec<Complex64> = match cell(self.table, self.path, row, column)? {
            Value::Array(ArrayValue::Complex64(values)) => values.iter().copied().collect(),
            Value::Array(ArrayValue::Complex32(values)) => values
                .iter()
                .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im)))
                .collect(),
            _ => {
                return Err(cell_error(
                    self.path,
                    row,
                    column,
                    "expected a complex array".to_string(),
                ));
            }
        };
        <[Complex64; 4]>::try_from(values.as_slice()).map_err(|_| {
            cell_error(
                self.path,
                row,
                column,
                format!("expected 4 values, got {}", values.len()),
            )
        })
    }
}
