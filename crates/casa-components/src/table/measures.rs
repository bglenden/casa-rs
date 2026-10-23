// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measure and quantum columns of a component list, read as casacore's
//! `ScalarMeasColumn` and `ScalarQuantColumn`/`ArrayQuantColumn` read them.
//!
//! casa-tables' measure accessors read array cells only and ignore
//! `QuantumUnits`, while a component list's `Reference_Frequency` is a scalar
//! `Double` column. These readers honour what casacore honours: the
//! `QuantumUnits` of the value column (default the measure's own units), and a
//! reference that is fixed (`MEASINFO.Ref`) or read per row from a string or
//! integer column (`MEASINFO.VarRefCol`).

use std::path::Path;

use casa_tables::Table;
use casa_tables::table_measures::{MeasRefDesc, MeasureType, TableMeasDesc};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::error::ComponentListError;

/// Where a measure column's reference frame comes from.
#[derive(Debug, Clone)]
enum Reference {
    Fixed(String),
    StringColumn(String),
    IntColumn {
        column: String,
        types: Vec<String>,
        codes: Vec<i32>,
    },
}

/// A scalar measure column: one direction or frequency per row.
#[derive(Debug, Clone)]
pub(super) struct MeasureColumn {
    column: &'static str,
    measure: MeasureType,
    /// Factors taking each stored value to radians or hertz.
    to_si: Vec<f64>,
    reference: Reference,
}

impl MeasureColumn {
    /// Attach to `column`, which must carry `MEASINFO` of type `measure`.
    pub(super) fn attach(
        table: &Table,
        path: &Path,
        column: &'static str,
        measure: MeasureType,
    ) -> Result<Self, ComponentListError> {
        let keyword_error = |reason: String| ComponentListError::ColumnKeywords {
            path: path.to_path_buf(),
            column: column.to_string(),
            reason,
        };
        let desc = TableMeasDesc::reconstruct(table, column)
            .ok_or_else(|| keyword_error("no MEASINFO measure description".to_string()))?;
        if desc.measure_type() != measure {
            return Err(keyword_error(format!(
                "MEASINFO describes a {} measure, not a {}",
                desc.measure_type().as_str(),
                measure.as_str()
            )));
        }
        if desc.offset_desc().is_some() {
            return Err(keyword_error(
                "measure offsets are not used by component lists and are not supported".to_string(),
            ));
        }
        let reference = match desc.ref_desc() {
            MeasRefDesc::Fixed { refer } => Reference::Fixed(refer.clone()),
            MeasRefDesc::VariableString { ref_column } => {
                Reference::StringColumn(ref_column.clone())
            }
            MeasRefDesc::VariableInt {
                ref_column,
                tab_ref_types,
                tab_ref_codes,
            } => Reference::IntColumn {
                column: ref_column.clone(),
                types: tab_ref_types.clone(),
                codes: tab_ref_codes.clone(),
            },
        };
        let si = match measure {
            MeasureType::Direction => "rad",
            _ => "Hz",
        };
        let count = measure.values_per_measure();
        let units = TableQuantumDesc::reconstruct(table, column)
            .filter(|quantum| !quantum.is_unit_variable())
            .map_or_else(
                || measure.default_units(),
                |quantum| quantum.units().to_vec(),
            );
        let to_si = conversion_factors(&units, count, si).map_err(keyword_error)?;
        Ok(Self {
            column,
            measure,
            to_si,
            reference,
        })
    }

    /// The direction at `row`.
    pub(super) fn direction(
        &self,
        table: &Table,
        path: &Path,
        row: usize,
    ) -> Result<MDirection, ComponentListError> {
        let values = self.values(table, path, row)?;
        let reference = self.reference(table, path, row)?;
        let refer = reference
            .parse::<DirectionRef>()
            .map_err(|_| cell_error(path, row, self.column, unsupported_frame(&reference)))?;
        Ok(MDirection::from_angles(values[0], values[1], refer))
    }

    /// The frequency at `row`.
    pub(super) fn frequency(
        &self,
        table: &Table,
        path: &Path,
        row: usize,
    ) -> Result<MFrequency, ComponentListError> {
        let values = self.values(table, path, row)?;
        let reference = self.reference(table, path, row)?;
        let refer = reference
            .parse::<FrequencyRef>()
            .map_err(|_| cell_error(path, row, self.column, unsupported_frame(&reference)))?;
        Ok(MFrequency::new(values[0], refer))
    }

    /// The stored values at `row`, in radians or hertz. A one-value measure
    /// may sit in a scalar column, as `ScalarMeasColumn` allows.
    fn values(
        &self,
        table: &Table,
        path: &Path,
        row: usize,
    ) -> Result<Vec<f64>, ComponentListError> {
        let raw = match cell(table, path, row, self.column)? {
            Value::Scalar(scalar) => vec![scalar_f64(scalar).ok_or_else(|| {
                cell_error(path, row, self.column, "expected a real number".to_string())
            })?],
            Value::Array(array) => array_f64(array).ok_or_else(|| {
                cell_error(path, row, self.column, "expected a real array".to_string())
            })?,
            _ => {
                return Err(cell_error(
                    path,
                    row,
                    self.column,
                    "expected a number or array".to_string(),
                ));
            }
        };
        let count = self.measure.values_per_measure();
        if raw.len() != count {
            return Err(cell_error(
                path,
                row,
                self.column,
                format!("expected {count} values, got {}", raw.len()),
            ));
        }
        Ok(raw
            .iter()
            .zip(&self.to_si)
            .map(|(value, factor)| value * factor)
            .collect())
    }

    fn reference(
        &self,
        table: &Table,
        path: &Path,
        row: usize,
    ) -> Result<String, ComponentListError> {
        match &self.reference {
            Reference::Fixed(refer) => Ok(refer.clone()),
            Reference::StringColumn(column) => match cell(table, path, row, column)? {
                Value::Scalar(ScalarValue::String(refer)) => Ok(refer.clone()),
                _ => Err(cell_error(
                    path,
                    row,
                    column,
                    "expected a frame name".to_string(),
                )),
            },
            Reference::IntColumn {
                column,
                types,
                codes,
            } => {
                let code = match cell(table, path, row, column)? {
                    Value::Scalar(scalar) => scalar_i32(scalar),
                    _ => None,
                }
                .ok_or_else(|| {
                    cell_error(path, row, column, "expected a frame code".to_string())
                })?;
                if let Some(position) = codes.iter().position(|candidate| *candidate == code) {
                    return types.get(position).cloned().ok_or_else(|| {
                        cell_error(path, row, column, format!("frame code {code} has no name"))
                    });
                }
                let name = match self.measure {
                    MeasureType::Direction => {
                        DirectionRef::from_casacore_code(code).map(|refer| refer.as_str())
                    }
                    _ => FrequencyRef::from_casacore_code(code).map(|refer| refer.as_str()),
                };
                name.map(str::to_string).ok_or_else(|| {
                    cell_error(path, row, column, format!("unknown frame code {code}"))
                })
            }
        }
    }
}

/// The units of a quantum column's cell at `row`, `count` of them: from its
/// `VariableUnits` column (per element when that column holds arrays, per row
/// when it holds strings) or its fixed `QuantumUnits`.
pub(super) fn quantum_units(
    table: &Table,
    path: &Path,
    row: usize,
    column: &str,
    count: usize,
) -> Result<Vec<String>, ComponentListError> {
    let desc = TableQuantumDesc::reconstruct(table, column).ok_or_else(|| {
        ComponentListError::ColumnKeywords {
            path: path.to_path_buf(),
            column: column.to_string(),
            reason: "no QuantumUnits or VariableUnits keyword".to_string(),
        }
    })?;
    let units = match desc.unit_column_name() {
        Some(units_column) => match cell(table, path, row, units_column)? {
            Value::Scalar(ScalarValue::String(unit)) => vec![unit.clone(); count],
            Value::Array(ArrayValue::String(units)) => units.iter().cloned().collect(),
            _ => {
                return Err(cell_error(
                    path,
                    row,
                    units_column,
                    "expected unit names".to_string(),
                ));
            }
        },
        None => desc.units().to_vec(),
    };
    match units.len() {
        length if length == count => Ok(units),
        1 => Ok(vec![units[0].clone(); count]),
        length => Err(cell_error(
            path,
            row,
            column,
            format!("expected {count} units, got {length}"),
        )),
    }
}

/// A quantity from a value and a unit name.
pub(super) fn quantity(
    path: &Path,
    row: usize,
    column: &str,
    value: f64,
    unit: &str,
) -> Result<Quantity, ComponentListError> {
    Quantity::new(value, unit).map_err(|error| {
        cell_error(
            path,
            row,
            column,
            format!("cannot parse unit {unit:?}: {error}"),
        )
    })
}

/// A direction's longitude and latitude as casacore stores them
/// (`MVDirection::get`): `atan2(y, x)` in (−π, π] and `asin(z)`, from the
/// direction cosines.
pub(super) fn casacore_angles(direction: &MDirection) -> [f64; 2] {
    let [x, y, z] = direction.cosines();
    let longitude = if x != 0.0 || y != 0.0 {
        y.atan2(x)
    } else {
        0.0
    };
    [longitude, z.asin()]
}

/// The cell at `row` of `column`.
pub(super) fn cell<'a>(
    table: &'a Table,
    path: &Path,
    row: usize,
    column: &str,
) -> Result<&'a Value, ComponentListError> {
    table
        .cell_accessor(row, column)
        .and_then(|cell| cell.value())
        .map_err(|source| ComponentListError::Table {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| cell_error(path, row, column, "the cell is undefined".to_string()))
}

pub(super) fn cell_error(
    path: &Path,
    row: usize,
    column: &str,
    reason: String,
) -> ComponentListError {
    ComponentListError::Cell {
        path: path.to_path_buf(),
        row,
        column: column.to_string(),
        reason,
    }
}

pub(super) fn array_f64(array: &ArrayValue) -> Option<Vec<f64>> {
    match array {
        ArrayValue::Float64(values) => Some(values.iter().copied().collect()),
        ArrayValue::Float32(values) => Some(values.iter().map(|value| f64::from(*value)).collect()),
        ArrayValue::Int32(values) => Some(values.iter().map(|value| f64::from(*value)).collect()),
        _ => None,
    }
}

fn scalar_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float64(value) => Some(*value),
        ScalarValue::Float32(value) => Some(f64::from(*value)),
        ScalarValue::Int32(value) => Some(f64::from(*value)),
        _ => None,
    }
}

fn scalar_i32(scalar: &ScalarValue) -> Option<i32> {
    match scalar {
        ScalarValue::Int32(value) => Some(*value),
        ScalarValue::UInt32(value) => i32::try_from(*value).ok(),
        ScalarValue::Int16(value) => Some(i32::from(*value)),
        _ => None,
    }
}

fn unsupported_frame(reference: &str) -> String {
    format!("the frame {reference:?} is not one casa-types supports")
}

/// Factors converting each of `count` stored values in `units` to `si`.
fn conversion_factors(units: &[String], count: usize, si: &str) -> Result<Vec<f64>, String> {
    let target = Unit::new(si).expect("SI units parse");
    let units: Vec<&String> = match units.len() {
        length if length == count => units.iter().collect(),
        1 => vec![&units[0]; count],
        length => {
            return Err(format!(
                "QuantumUnits holds {length} units for {count} values"
            ));
        }
    };
    units
        .into_iter()
        .map(|unit| {
            let parsed = Unit::new(unit).map_err(|error| format!("unit {unit:?}: {error}"))?;
            if !parsed.conformant(&target) {
                return Err(format!("unit {unit:?} is not convertible to {si}"));
            }
            Ok(parsed.val().factor / target.val().factor)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_angles_follow_casacore_atan2_convention() {
        // RA 20h is stored as -60 degrees, as casacore's MVDirection::get gives.
        let direction = MDirection::from_angles(300.0_f64.to_radians(), -0.5, DirectionRef::J2000);
        let [longitude, latitude] = casacore_angles(&direction);
        assert!(
            (longitude + 60.0_f64.to_radians()).abs() < 1e-15,
            "{longitude}"
        );
        assert!((latitude + 0.5).abs() < 1e-15);
        let pole = MDirection::from_cosines([0.0, 0.0, 1.0], DirectionRef::J2000);
        assert_eq!(casacore_angles(&pole), [0.0, std::f64::consts::FRAC_PI_2]);
    }

    #[test]
    fn conversion_factors_honour_quantum_units() {
        let factors =
            conversion_factors(&["deg".to_string(), "rad".to_string()], 2, "rad").expect("angles");
        assert!((factors[0] - 1.0_f64.to_radians()).abs() < 1e-18);
        assert_eq!(factors[1], 1.0);
        assert_eq!(
            conversion_factors(&["GHz".to_string()], 1, "Hz"),
            Ok(vec![1.0e9])
        );
        assert!(conversion_factors(&["m".to_string()], 1, "Hz").is_err());
    }
}
