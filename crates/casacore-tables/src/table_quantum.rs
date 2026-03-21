// SPDX-License-Identifier: LGPL-3.0-or-later
//! Quantum column support: unit-tagged table columns.
//!
//! This module provides the Rust counterparts of C++ `TableQuantumDesc`,
//! `ScalarQuantColumn`, and `ArrayQuantColumn` from the casacore
//! `measures/TableMeasures` subsystem.
//!
//! A "quantum column" is a regular numeric column whose values carry physical
//! units. The unit metadata is stored in the column's keyword record as either:
//!
//! - **`QuantumUnits`** — a `Vector<String>` of fixed unit name(s), or
//! - **`VariableUnits`** — the name of a companion `String` column that holds
//!   per-row (scalar) or per-element (array) unit strings.
//!
//! # Examples
//!
//! ```rust
//! use casacore_tables::{Table, TableSchema, ColumnSchema, table_quantum::TableQuantumDesc};
//! use casacore_types::*;
//!
//! // Create a table with a quantum column
//! let schema = TableSchema::new(vec![
//!     ColumnSchema::scalar("flux", PrimitiveType::Float64),
//! ]).unwrap();
//! let mut table = Table::with_schema(schema);
//!
//! // Attach fixed units
//! let desc = TableQuantumDesc::with_unit("flux", "Jy");
//! desc.write(&mut table).unwrap();
//! assert!(TableQuantumDesc::has_quanta(&table, "flux"));
//! ```

use crate::schema::ColumnType;
use crate::table::{Table, TableError};
use casacore_types::quanta::{Quantity, Unit};
use casacore_types::{ArrayValue, ScalarValue, Value};

// ─── Keyword names (matching C++ casacore) ──────────────────────────────────

const QUANTUM_UNITS_KW: &str = "QuantumUnits";
const VARIABLE_UNITS_KW: &str = "VariableUnits";

// ─── TableQuantumDesc ───────────────────────────────────────────────────────

/// Descriptor for a quantum column's unit metadata.
///
/// Corresponds to C++ `casa::TableQuantumDesc`.
///
/// A `TableQuantumDesc` records whether a column uses **fixed units**
/// (stored in the column's keyword record) or **variable units** (stored in
/// a companion `String` column). It can read, write, and detect quantum
/// metadata on table columns.
#[derive(Debug, Clone)]
pub struct TableQuantumDesc {
    /// Name of the data column.
    column_name: String,
    /// Fixed unit names (non-empty when `units_column_name` is `None`).
    units: Vec<String>,
    /// Name of the companion units column (variable-unit mode).
    units_column_name: Option<String>,
}

impl TableQuantumDesc {
    /// Creates a descriptor with a single fixed unit.
    ///
    /// All cells in the column share the same unit string.
    ///
    /// # Examples
    ///
    /// ```
    /// use casacore_tables::table_quantum::TableQuantumDesc;
    ///
    /// let desc = TableQuantumDesc::with_unit("flux", "Jy");
    /// assert!(!desc.is_unit_variable());
    /// assert_eq!(desc.units(), &["Jy"]);
    /// ```
    pub fn with_unit(column: &str, unit: &str) -> Self {
        Self {
            column_name: column.to_owned(),
            units: vec![unit.to_owned()],
            units_column_name: None,
        }
    }

    /// Creates a descriptor with multiple fixed units.
    ///
    /// Typically used for array columns where each axis has a different unit
    /// (e.g. `["MHz", "GHz"]`).
    pub fn with_units(column: &str, units: &[&str]) -> Self {
        Self {
            column_name: column.to_owned(),
            units: units.iter().map(|u| (*u).to_owned()).collect(),
            units_column_name: None,
        }
    }

    /// Creates a descriptor with variable units stored in a companion column.
    ///
    /// The companion column must be a `String` column. If it is a scalar
    /// column, units vary per row; if an array column, units vary per element.
    pub fn with_variable_units(column: &str, units_column: &str) -> Self {
        Self {
            column_name: column.to_owned(),
            units: Vec::new(),
            units_column_name: Some(units_column.to_owned()),
        }
    }

    /// Returns `true` if the column `column` in `table` has quantum keywords.
    ///
    /// Checks for the presence of either `QuantumUnits` or `VariableUnits`
    /// in the column's keyword record.
    pub fn has_quanta(table: &Table, column: &str) -> bool {
        if let Some(kw) = table.column_keywords(column) {
            kw.get(QUANTUM_UNITS_KW).is_some() || kw.get(VARIABLE_UNITS_KW).is_some()
        } else {
            false
        }
    }

    /// Reconstructs a descriptor from a column's persisted keywords.
    ///
    /// Returns `None` if the column has no quantum keywords.
    pub fn reconstruct(table: &Table, column: &str) -> Option<Self> {
        let kw = table.column_keywords(column)?;

        // Check for variable units first.
        if let Some(Value::Scalar(ScalarValue::String(units_col))) = kw.get(VARIABLE_UNITS_KW) {
            return Some(Self {
                column_name: column.to_owned(),
                units: Vec::new(),
                units_column_name: Some(units_col.clone()),
            });
        }

        // Check for fixed units.
        if let Some(Value::Array(ArrayValue::String(arr))) = kw.get(QUANTUM_UNITS_KW) {
            let units: Vec<String> = arr.iter().cloned().collect();
            return Some(Self {
                column_name: column.to_owned(),
                units,
                units_column_name: None,
            });
        }

        None
    }

    /// Writes the quantum keywords to the column's keyword record.
    ///
    /// For fixed units, writes `QuantumUnits` as a `Vector<String>`.
    /// For variable units, writes `VariableUnits` as a `String`.
    pub fn write(&self, table: &mut Table) -> Result<(), TableError> {
        let mut kw = table
            .column_keywords(&self.column_name)
            .cloned()
            .unwrap_or_default();

        if self.is_unit_variable() {
            let col_name = self.units_column_name.as_ref().unwrap();
            kw.upsert(
                VARIABLE_UNITS_KW,
                Value::Scalar(ScalarValue::String(col_name.clone())),
            );
            // Remove QuantumUnits if it existed.
            kw.remove(QUANTUM_UNITS_KW);
        } else {
            kw.upsert(
                QUANTUM_UNITS_KW,
                Value::Array(ArrayValue::from_string_vec(self.units.clone())),
            );
            // Remove VariableUnits if it existed.
            kw.remove(VARIABLE_UNITS_KW);
        }

        table.set_column_keywords(&self.column_name, kw);
        Ok(())
    }

    /// Returns the data column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Returns `true` if units vary per row or per element.
    pub fn is_unit_variable(&self) -> bool {
        self.units_column_name.is_some()
    }

    /// Returns the companion units column name, if variable.
    pub fn unit_column_name(&self) -> Option<&str> {
        self.units_column_name.as_deref()
    }

    /// Returns the fixed unit name(s). Empty if variable.
    pub fn units(&self) -> &[String] {
        &self.units
    }
}

// ─── ScalarQuantColumn (read-only) ─────────────────────────────────────────

/// Read-only accessor for a scalar column with quantum (unit) metadata.
///
/// Corresponds to C++ `casa::ScalarQuantColumn<Double>`.
///
/// The unit is resolved once at construction time. Each [`get`](Self::get)
/// call returns a [`Quantity`] (value + unit) with no additional parsing or
/// locking overhead.
pub struct ScalarQuantColumn<'a> {
    table: &'a Table,
    column_name: String,
    desc: TableQuantumDesc,
    /// Cached unit for fixed-unit columns; `None` for variable-unit.
    fixed_unit: Option<Unit>,
    /// Optional on-read conversion target.
    convert_unit: Option<Unit>,
}

impl<'a> ScalarQuantColumn<'a> {
    /// Attaches to a scalar quantum column.
    ///
    /// Returns an error if the column has no quantum keywords.
    pub fn new(table: &'a Table, column: &str) -> Result<Self, TableError> {
        let desc = TableQuantumDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no quantum keywords"))
        })?;
        let fixed_unit = if !desc.is_unit_variable() && !desc.units.is_empty() {
            Some(Unit::new(&desc.units[0]).map_err(|e| {
                TableError::Storage(format!("invalid unit '{}': {e}", &desc.units[0]))
            })?)
        } else {
            None
        };
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
            fixed_unit,
            convert_unit: None,
        })
    }

    /// Attaches to a scalar quantum column with on-read conversion to `unit`.
    ///
    /// Every [`get`](Self::get) call converts the stored value to the target
    /// unit automatically.
    pub fn with_unit(table: &'a Table, column: &str, unit: &str) -> Result<Self, TableError> {
        let mut col = Self::new(table, column)?;
        col.convert_unit = Some(
            Unit::new(unit)
                .map_err(|e| TableError::Storage(format!("invalid target unit '{unit}': {e}")))?,
        );
        Ok(col)
    }

    /// Returns `true` if units vary per row.
    pub fn is_unit_variable(&self) -> bool {
        self.desc.is_unit_variable()
    }

    /// Returns the fixed unit name(s). Empty if variable.
    pub fn units(&self) -> &[String] {
        self.desc.units()
    }

    /// Reads the quantity at `row`.
    ///
    /// For fixed-unit columns, the unit is the one stored in the keyword.
    /// For variable-unit columns, the unit is read from the companion column.
    /// If a conversion unit was set at construction, the value is converted.
    pub fn get(&self, row: usize) -> Result<Quantity, TableError> {
        let scalar = self.table.get_scalar_cell(row, &self.column_name)?;
        let value = scalar_to_f64(scalar, row, &self.column_name)?;

        let unit = if let Some(ref u) = self.fixed_unit {
            u.clone()
        } else {
            self.read_variable_unit(row)?
        };

        let q = Quantity::with_unit(value, unit);

        if let Some(ref target) = self.convert_unit {
            q.get_value_in(target)
                .map(|v| Quantity::with_unit(v, target.clone()))
                .map_err(|e| TableError::Storage(format!("unit conversion failed: {e}")))
        } else {
            Ok(q)
        }
    }

    fn read_variable_unit(&self, row: usize) -> Result<Unit, TableError> {
        let units_col = self.desc.unit_column_name().unwrap();
        let scalar = self.table.get_scalar_cell(row, units_col)?;
        match scalar {
            ScalarValue::String(s) => Unit::new(s)
                .map_err(|e| TableError::Storage(format!("invalid unit '{s}' at row {row}: {e}"))),
            _ => Err(TableError::Storage(format!(
                "units column '{units_col}' at row {row}: expected String"
            ))),
        }
    }
}

// ─── ScalarQuantColumnMut (write) ──────────────────────────────────────────

/// Mutable accessor for writing quantum values to a scalar column.
///
/// Corresponds to the write path of C++ `casa::ScalarQuantColumn<Double>`.
pub struct ScalarQuantColumnMut<'a> {
    table: &'a mut Table,
    column_name: String,
    desc: TableQuantumDesc,
}

impl<'a> ScalarQuantColumnMut<'a> {
    /// Attaches to a scalar quantum column for writing.
    pub fn new(table: &'a mut Table, column: &str) -> Result<Self, TableError> {
        let desc = TableQuantumDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no quantum keywords"))
        })?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
        })
    }

    /// Writes a quantity at `row`.
    ///
    /// For fixed-unit columns, the quantity's value is converted to the
    /// column's unit before writing. For variable-unit columns, both the
    /// value and unit string are written to their respective columns.
    pub fn put(&mut self, row: usize, q: &Quantity) -> Result<(), TableError> {
        if self.desc.is_unit_variable() {
            // Write value as-is.
            self.table.set_cell(
                row,
                &self.column_name,
                Value::Scalar(ScalarValue::Float64(q.value())),
            )?;
            // Write unit string.
            let units_col = self.desc.unit_column_name().unwrap().to_owned();
            self.table.set_cell(
                row,
                &units_col,
                Value::Scalar(ScalarValue::String(q.unit().name().to_owned())),
            )?;
        } else {
            // Convert to column's fixed unit.
            let col_unit_str = &self.desc.units[0];
            let col_unit = Unit::new(col_unit_str).map_err(|e| {
                TableError::Storage(format!("invalid column unit '{col_unit_str}': {e}"))
            })?;
            let converted = q
                .get_value_in(&col_unit)
                .map_err(|e| TableError::Storage(format!("unit conversion failed: {e}")))?;
            self.table.set_cell(
                row,
                &self.column_name,
                Value::Scalar(ScalarValue::Float64(converted)),
            )?;
        }
        Ok(())
    }
}

// ─── ArrayQuantColumn (read-only) ──────────────────────────────────────────

/// Read-only accessor for an array column with quantum (unit) metadata.
///
/// Corresponds to C++ `casa::ArrayQuantColumn<Double>`.
///
/// Supports three unit modes:
/// - **Fixed**: all elements share the same unit(s) from the keyword.
/// - **Variable per-element**: each array element has its own unit, stored in
///   an array `String` column with the same shape.
/// - **Variable per-row**: each row has a single unit, stored in a scalar
///   `String` column.
pub struct ArrayQuantColumn<'a> {
    table: &'a Table,
    column_name: String,
    desc: TableQuantumDesc,
    fixed_units: Vec<Unit>,
    convert_unit: Option<Unit>,
}

impl<'a> ArrayQuantColumn<'a> {
    /// Attaches to an array quantum column.
    pub fn new(table: &'a Table, column: &str) -> Result<Self, TableError> {
        let desc = TableQuantumDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no quantum keywords"))
        })?;
        let fixed_units = desc
            .units
            .iter()
            .map(|u| {
                Unit::new(u).map_err(|e| TableError::Storage(format!("invalid unit '{u}': {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
            fixed_units,
            convert_unit: None,
        })
    }

    /// Attaches with on-read conversion to `unit`.
    pub fn with_unit(table: &'a Table, column: &str, unit: &str) -> Result<Self, TableError> {
        let mut col = Self::new(table, column)?;
        col.convert_unit = Some(
            Unit::new(unit)
                .map_err(|e| TableError::Storage(format!("invalid target unit '{unit}': {e}")))?,
        );
        Ok(col)
    }

    /// Returns `true` if units vary per row or per element.
    pub fn is_unit_variable(&self) -> bool {
        self.desc.is_unit_variable()
    }

    /// Returns the fixed unit name(s). Empty if variable.
    pub fn units(&self) -> &[String] {
        self.desc.units()
    }

    /// Reads all quantities at `row`, returned as a `Vec<Quantity>`.
    ///
    /// The array is flattened to a vector of quantities in row-major order.
    pub fn get(&self, row: usize) -> Result<Vec<Quantity>, TableError> {
        let arr = self.table.get_array_cell(row, &self.column_name)?;
        let values = array_to_f64_vec(arr, row, &self.column_name)?;

        if self.desc.is_unit_variable() {
            self.get_variable(row, &values)
        } else {
            self.get_fixed(&values)
        }
    }

    fn get_fixed(&self, values: &[f64]) -> Result<Vec<Quantity>, TableError> {
        let units = if !self.fixed_units.is_empty() {
            &self.fixed_units
        } else {
            return Ok(values.iter().map(|&v| Quantity::dimensionless(v)).collect());
        };
        let unit_count = units.len();

        let mut result = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let q = Quantity::with_unit(v, units[i % unit_count].clone());
            if let Some(ref target) = self.convert_unit {
                let converted = q
                    .get_value_in(target)
                    .map_err(|e| TableError::Storage(format!("unit conversion failed: {e}")))?;
                result.push(Quantity::with_unit(converted, target.clone()));
            } else {
                result.push(q);
            }
        }
        Ok(result)
    }

    fn get_variable(&self, row: usize, values: &[f64]) -> Result<Vec<Quantity>, TableError> {
        let units_col = self.desc.unit_column_name().unwrap();

        // Try array units column first (per-element), then scalar (per-row).
        let cell = self.table.cell(row, units_col)?;
        match cell {
            Some(Value::Array(ArrayValue::String(arr))) => {
                let unit_strs: Vec<&str> = arr.iter().map(|s| s.as_str()).collect();
                if unit_strs.len() != values.len() {
                    return Err(TableError::Storage(format!(
                        "unit array length {} != value array length {} at row {row}",
                        unit_strs.len(),
                        values.len()
                    )));
                }
                let mut result = Vec::with_capacity(values.len());
                for (&v, unit_str) in values.iter().zip(unit_strs) {
                    let unit = Unit::new(unit_str).map_err(|e| {
                        TableError::Storage(format!("invalid unit '{unit_str}' at row {row}: {e}"))
                    })?;
                    let q = Quantity::with_unit(v, unit);
                    if let Some(ref target) = self.convert_unit {
                        let converted = q.get_value_in(target).map_err(|e| {
                            TableError::Storage(format!("unit conversion failed: {e}"))
                        })?;
                        result.push(Quantity::with_unit(converted, target.clone()));
                    } else {
                        result.push(q);
                    }
                }
                Ok(result)
            }
            Some(Value::Scalar(ScalarValue::String(unit_str))) => {
                let unit = Unit::new(unit_str).map_err(|e| {
                    TableError::Storage(format!("invalid unit '{unit_str}' at row {row}: {e}"))
                })?;
                let mut result = Vec::with_capacity(values.len());
                for &v in values {
                    let q = Quantity::with_unit(v, unit.clone());
                    if let Some(ref target) = self.convert_unit {
                        let converted = q.get_value_in(target).map_err(|e| {
                            TableError::Storage(format!("unit conversion failed: {e}"))
                        })?;
                        result.push(Quantity::with_unit(converted, target.clone()));
                    } else {
                        result.push(q);
                    }
                }
                Ok(result)
            }
            _ => Err(TableError::Storage(format!(
                "units column '{units_col}' has no String value at row {row}"
            ))),
        }
    }
}

// ─── ArrayQuantColumnMut (write) ───────────────────────────────────────────

/// Mutable accessor for writing quantum arrays to an array column.
///
/// Corresponds to the write path of C++ `casa::ArrayQuantColumn<Double>`.
pub struct ArrayQuantColumnMut<'a> {
    table: &'a mut Table,
    column_name: String,
    desc: TableQuantumDesc,
    /// Whether the units companion column is scalar (per-row) vs array (per-element).
    /// Determined once at construction from the column descriptor, matching C++
    /// `ArrayQuantColumn::init` which inspects the column desc, not cell contents.
    units_col_is_scalar: Option<bool>,
}

impl<'a> ArrayQuantColumnMut<'a> {
    /// Attaches to an array quantum column for writing.
    pub fn new(table: &'a mut Table, column: &str) -> Result<Self, TableError> {
        let desc = TableQuantumDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no quantum keywords"))
        })?;
        let units_col_is_scalar = if let Some(units_col_name) = desc.unit_column_name() {
            table
                .schema()
                .and_then(|s| s.column(units_col_name))
                .map(|cs| matches!(cs.column_type(), ColumnType::Scalar))
        } else {
            None
        };
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
            units_col_is_scalar,
        })
    }

    /// Writes an array of quantities at `row`.
    ///
    /// For fixed-unit columns, values are converted to the column's unit.
    /// For variable-unit columns, each quantity's unit string is written to
    /// the companion column.
    pub fn put(&mut self, row: usize, quanta: &[Quantity]) -> Result<(), TableError> {
        if self.desc.is_unit_variable() {
            let values: Vec<f64> = quanta.iter().map(|q| q.value()).collect();
            let units: Vec<String> = quanta.iter().map(|q| q.unit().name().to_owned()).collect();

            self.table.set_cell(
                row,
                &self.column_name,
                Value::Array(ArrayValue::from_f64_vec(values)),
            )?;

            let units_col = self.desc.unit_column_name().unwrap().to_owned();
            // Determine if units column is scalar or array from schema,
            // matching C++ ArrayQuantColumn::init which inspects the column
            // descriptor once at construction (not per-cell contents).
            let is_scalar = self.units_col_is_scalar.unwrap_or(true);

            if is_scalar && quanta.len() == 1 {
                self.table.set_cell(
                    row,
                    &units_col,
                    Value::Scalar(ScalarValue::String(units[0].clone())),
                )?;
            } else {
                // Check if the existing cell at this position is scalar
                // (per-row mode: all elements share one unit).
                // If so, write the first unit as the per-row unit.
                if is_scalar {
                    self.table.set_cell(
                        row,
                        &units_col,
                        Value::Scalar(ScalarValue::String(units[0].clone())),
                    )?;
                } else {
                    self.table.set_cell(
                        row,
                        &units_col,
                        Value::Array(ArrayValue::from_string_vec(units)),
                    )?;
                }
            }
        } else {
            let col_units = self
                .desc
                .units
                .iter()
                .map(|unit| {
                    Unit::new(unit).map_err(|e| {
                        TableError::Storage(format!("invalid column unit '{unit}': {e}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if col_units.is_empty() {
                self.table.set_cell(
                    row,
                    &self.column_name,
                    Value::Array(ArrayValue::from_f64_vec(
                        quanta.iter().map(|q| q.value()).collect(),
                    )),
                )?;
                return Ok(());
            }
            let unit_count = col_units.len();

            let values: Result<Vec<f64>, _> = quanta
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    q.get_value_in(&col_units[i % unit_count])
                        .map_err(|e| TableError::Storage(format!("unit conversion failed: {e}")))
                })
                .collect();

            self.table.set_cell(
                row,
                &self.column_name,
                Value::Array(ArrayValue::from_f64_vec(values?)),
            )?;
        }
        Ok(())
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn scalar_to_f64(sv: &ScalarValue, row: usize, column: &str) -> Result<f64, TableError> {
    match sv {
        ScalarValue::Float64(v) => Ok(*v),
        ScalarValue::Float32(v) => Ok(*v as f64),
        ScalarValue::Int32(v) => Ok(*v as f64),
        ScalarValue::Int64(v) => Ok(*v as f64),
        _ => Err(TableError::Storage(format!(
            "column '{column}' at row {row}: expected numeric scalar"
        ))),
    }
}

fn array_to_f64_vec(av: &ArrayValue, row: usize, column: &str) -> Result<Vec<f64>, TableError> {
    match av {
        ArrayValue::Float64(arr) => Ok(arr.iter().copied().collect()),
        ArrayValue::Float32(arr) => Ok(arr.iter().map(|&v| v as f64).collect()),
        ArrayValue::Int32(arr) => Ok(arr.iter().map(|&v| v as f64).collect()),
        ArrayValue::Int64(arr) => Ok(arr.iter().map(|&v| v as f64).collect()),
        _ => Err(TableError::Storage(format!(
            "column '{column}' at row {row}: expected numeric array"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, TableSchema};
    use casacore_types::PrimitiveType;

    /// Verifies that `ArrayQuantColumnMut::put` writes array units (not scalar)
    /// on an empty row when the schema says the units column is an array column.
    #[test]
    fn array_quant_put_writes_array_units_on_empty_row() {
        let schema = TableSchema::new(vec![
            ColumnSchema::array_variable("DATA", PrimitiveType::Float64, Some(1)),
            ColumnSchema::array_variable("DATA_UNITS", PrimitiveType::String, Some(1)),
        ])
        .unwrap();
        let mut table = crate::Table::with_schema(schema);

        TableQuantumDesc::with_variable_units("DATA", "DATA_UNITS")
            .write(&mut table)
            .unwrap();

        // Add an empty row
        table
            .add_row(casacore_types::RecordValue::default())
            .unwrap();

        // Write quanta to the empty row
        let quanta = vec![
            Quantity::with_unit(1.0, Unit::new("Hz").unwrap()),
            Quantity::with_unit(2.0, Unit::new("kHz").unwrap()),
        ];
        {
            let mut col = ArrayQuantColumnMut::new(&mut table, "DATA").unwrap();
            col.put(0, &quanta).unwrap();
        }

        // Read back: the units column should be an array, not a scalar
        let cell = table.cell(0, "DATA_UNITS");
        assert!(
            matches!(cell, Ok(Some(Value::Array(ArrayValue::String(_))))),
            "expected array units but got: {cell:?}"
        );
        if let Ok(Some(Value::Array(ArrayValue::String(arr)))) = cell {
            let units: Vec<&str> = arr.iter().map(|s| s.as_str()).collect();
            assert_eq!(units, vec!["Hz", "kHz"]);
        }
    }

    #[test]
    fn array_quant_fixed_multiple_units_roundtrip() {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            PrimitiveType::Float64,
            vec![4],
        )])
        .unwrap();
        let mut table = crate::Table::with_schema(schema);

        TableQuantumDesc::with_units("DATA", &["MHz", "GHz"])
            .write(&mut table)
            .unwrap();

        table
            .add_row(casacore_types::RecordValue::new(vec![
                casacore_types::RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::from_f64_vec(vec![0.0; 4])),
                ),
            ]))
            .unwrap();

        let quanta = vec![
            Quantity::with_unit(1000.0, Unit::new("kHz").unwrap()),
            Quantity::with_unit(2.0, Unit::new("GHz").unwrap()),
            Quantity::with_unit(3.0, Unit::new("MHz").unwrap()),
            Quantity::with_unit(4_000_000.0, Unit::new("kHz").unwrap()),
        ];
        {
            let mut col = ArrayQuantColumnMut::new(&mut table, "DATA").unwrap();
            col.put(0, &quanta).unwrap();
        }

        let col = ArrayQuantColumn::new(&table, "DATA").unwrap();
        let got = col.get(0).unwrap();
        let got_units: Vec<&str> = got.iter().map(|q| q.unit().name()).collect();
        let got_values: Vec<f64> = got.iter().map(|q| q.value()).collect();

        assert_eq!(got_units, vec!["MHz", "GHz", "MHz", "GHz"]);
        assert_eq!(got_values, vec![1.0, 2.0, 3.0, 4.0]);
    }
}
