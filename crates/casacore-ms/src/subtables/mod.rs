// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for MS subtables.
//!
//! Each subtable wrapper holds a reference to the underlying [`Table`] and
//! provides typed accessor methods for the columns defined in the
//! corresponding schema module.
//!
//! Immutable wrappers (`Ms*<'a>`) provide read access; mutable wrappers
//! (`Ms*Mut<'a>`) additionally support write operations.

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue, Value};

use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::schema::SubtableId;

macro_rules! define_generic_subtable {
    (
        read = $read_ty:ident,
        write = $write_ty:ident,
        id = $id:path,
        required = $required:path,
        optional = $optional:path,
        table_name = $table_name:literal,
        read_doc = $read_doc:literal,
        write_doc = $write_doc:literal
    ) => {
        #[doc = $read_doc]
        pub struct $read_ty<'a> {
            table: &'a casacore_tables::Table,
        }

        impl<'a> $read_ty<'a> {
            /// Wrap an existing table as this MS subtable.
            pub fn new(table: &'a casacore_tables::Table) -> Self {
                Self { table }
            }

            /// Number of rows in the subtable.
            pub fn row_count(&self) -> usize {
                self.table.row_count()
            }

            /// Access the underlying table.
            pub fn table(&self) -> &casacore_tables::Table {
                self.table
            }

            /// Read an `i32` scalar column by name.
            pub fn i32(&self, row: usize, col: &str) -> crate::MsResult<i32> {
                crate::subtables::get_i32(self.table, row, col)
            }

            /// Read an optional `i32` scalar column by name.
            pub fn optional_i32(&self, row: usize, col: &str) -> crate::MsResult<Option<i32>> {
                crate::subtables::optional_i32(self.table, row, col)
            }

            /// Read an `f64` scalar column by name.
            pub fn f64(&self, row: usize, col: &str) -> crate::MsResult<f64> {
                crate::subtables::get_f64(self.table, row, col)
            }

            /// Read an optional `f64` scalar column by name.
            pub fn optional_f64(&self, row: usize, col: &str) -> crate::MsResult<Option<f64>> {
                crate::subtables::optional_f64(self.table, row, col)
            }

            /// Read a `bool` scalar column by name.
            pub fn bool(&self, row: usize, col: &str) -> crate::MsResult<bool> {
                crate::subtables::get_bool(self.table, row, col)
            }

            /// Read an optional `bool` scalar column by name.
            pub fn optional_bool(&self, row: usize, col: &str) -> crate::MsResult<Option<bool>> {
                crate::subtables::optional_bool(self.table, row, col)
            }

            /// Read a `String` scalar column by name.
            pub fn string(&self, row: usize, col: &str) -> crate::MsResult<String> {
                crate::subtables::get_string(self.table, row, col)
            }

            /// Read an optional `String` scalar column by name.
            pub fn optional_string(
                &self,
                row: usize,
                col: &str,
            ) -> crate::MsResult<Option<String>> {
                crate::subtables::optional_string(self.table, row, col)
            }

            /// Read an array column by name.
            pub fn array(
                &self,
                row: usize,
                col: &str,
            ) -> crate::MsResult<&casacore_types::ArrayValue> {
                crate::subtables::get_array(self.table, row, col)
            }

            /// Read an optional array column by name.
            pub fn optional_array(
                &self,
                row: usize,
                col: &str,
            ) -> crate::MsResult<Option<&casacore_types::ArrayValue>> {
                crate::subtables::optional_array(self.table, row, col)
            }
        }

        impl crate::subtables::SubTable for $read_ty<'_> {
            fn id() -> crate::schema::SubtableId {
                $id
            }
            fn required_columns() -> &'static [crate::column_def::ColumnDef] {
                $required
            }
            fn optional_columns() -> &'static [crate::column_def::ColumnDef] {
                $optional
            }
            fn table(&self) -> &casacore_tables::Table {
                self.table
            }
        }

        #[doc = $write_doc]
        pub struct $write_ty<'a> {
            table: &'a mut casacore_tables::Table,
        }

        impl<'a> $write_ty<'a> {
            /// Wrap an existing mutable table as this MS subtable.
            pub fn new(table: &'a mut casacore_tables::Table) -> Self {
                Self { table }
            }

            /// Number of rows in the subtable.
            pub fn row_count(&self) -> usize {
                self.table.row_count()
            }

            /// View the table through the read-only wrapper.
            pub fn as_ref(&self) -> $read_ty<'_> {
                $read_ty { table: self.table }
            }

            /// Access the underlying mutable table.
            pub fn table_mut(&mut self) -> &mut casacore_tables::Table {
                self.table
            }

            /// Set an `i32` scalar column by name.
            pub fn set_i32(&mut self, row: usize, col: &str, value: i32) -> crate::MsResult<()> {
                crate::subtables::set_scalar(
                    self.table,
                    row,
                    col,
                    casacore_types::ScalarValue::Int32(value),
                )
            }

            /// Set an `f64` scalar column by name.
            pub fn set_f64(&mut self, row: usize, col: &str, value: f64) -> crate::MsResult<()> {
                crate::subtables::set_scalar(
                    self.table,
                    row,
                    col,
                    casacore_types::ScalarValue::Float64(value),
                )
            }

            /// Set a `bool` scalar column by name.
            pub fn set_bool(&mut self, row: usize, col: &str, value: bool) -> crate::MsResult<()> {
                crate::subtables::set_scalar(
                    self.table,
                    row,
                    col,
                    casacore_types::ScalarValue::Bool(value),
                )
            }

            /// Set a `String` scalar column by name.
            pub fn set_string(
                &mut self,
                row: usize,
                col: &str,
                value: impl Into<String>,
            ) -> crate::MsResult<()> {
                crate::subtables::set_scalar(
                    self.table,
                    row,
                    col,
                    casacore_types::ScalarValue::String(value.into()),
                )
            }

            /// Set an array column by name.
            pub fn set_array(
                &mut self,
                row: usize,
                col: &str,
                value: casacore_types::ArrayValue,
            ) -> crate::MsResult<()> {
                crate::subtables::set_array(self.table, row, col, value)
            }
        }

        impl crate::subtables::SubTable for $write_ty<'_> {
            fn id() -> crate::schema::SubtableId {
                $id
            }
            fn required_columns() -> &'static [crate::column_def::ColumnDef] {
                $required
            }
            fn optional_columns() -> &'static [crate::column_def::ColumnDef] {
                $optional
            }
            fn table(&self) -> &casacore_tables::Table {
                self.table
            }
        }
    };
}

pub(crate) use define_generic_subtable;

pub mod antenna;
pub mod data_description;
pub mod doppler;
pub mod feed;
pub mod field;
pub mod flag_cmd;
pub mod freq_offset;
pub mod history;
pub mod observation;
pub mod pointing;
pub mod polarization;
pub mod processor;
pub mod source;
pub mod spectral_window;
pub mod state;
pub mod syscal;
pub mod weather;

pub use antenna::{MsAntenna, MsAntennaMut};
pub use data_description::{MsDataDescription, MsDataDescriptionMut};
pub use doppler::{MsDoppler, MsDopplerMut};
pub use feed::{MsFeed, MsFeedMut};
pub use field::{MsField, MsFieldMut};
pub use flag_cmd::{MsFlagCmd, MsFlagCmdMut};
pub use freq_offset::{MsFreqOffset, MsFreqOffsetMut};
pub use history::{MsHistory, MsHistoryMut};
pub use observation::{MsObservation, MsObservationMut};
pub use pointing::{MsPointing, MsPointingMut};
pub use polarization::{MsPolarization, MsPolarizationMut};
pub use processor::{MsProcessor, MsProcessorMut};
pub use source::{MsSource, MsSourceMut};
pub use spectral_window::{MsSpectralWindow, MsSpectralWindowMut};
pub use state::{MsState, MsStateMut};
pub use syscal::{MsSysCal, MsSysCalMut};
pub use weather::{MsWeather, MsWeatherMut};

/// Trait implemented by all typed subtable wrappers.
///
/// Provides access to the subtable identity, schema metadata, and the
/// underlying table. This is the Rust equivalent of C++ `MSTable<T>`.
pub trait SubTable {
    /// The subtable identity.
    fn id() -> SubtableId;
    /// Required column definitions for this subtable.
    fn required_columns() -> &'static [ColumnDef];
    /// Optional column definitions for this subtable.
    fn optional_columns() -> &'static [ColumnDef];
    /// Reference to the underlying table.
    fn table(&self) -> &Table;
}

// ---- Helper functions used by subtable wrapper macros ----

/// Extract an `i32` from a scalar cell.
pub(crate) fn get_i32(table: &Table, row: usize, col: &str) -> MsResult<i32> {
    match table.get_scalar_cell(row, col)? {
        ScalarValue::Int32(v) => Ok(*v),
        other => Err(MsError::ColumnTypeMismatch {
            column: col.to_string(),
            table: "subtable".to_string(),
            expected: "Int32".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Extract an `f64` from a scalar cell.
pub(crate) fn get_f64(table: &Table, row: usize, col: &str) -> MsResult<f64> {
    match table.get_scalar_cell(row, col)? {
        ScalarValue::Float64(v) => Ok(*v),
        other => Err(MsError::ColumnTypeMismatch {
            column: col.to_string(),
            table: "subtable".to_string(),
            expected: "Float64".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Extract a `bool` from a scalar cell.
pub(crate) fn get_bool(table: &Table, row: usize, col: &str) -> MsResult<bool> {
    match table.get_scalar_cell(row, col)? {
        ScalarValue::Bool(v) => Ok(*v),
        other => Err(MsError::ColumnTypeMismatch {
            column: col.to_string(),
            table: "subtable".to_string(),
            expected: "Bool".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Extract a `&str` from a scalar cell.
pub(crate) fn get_string(table: &Table, row: usize, col: &str) -> MsResult<String> {
    match table.get_scalar_cell(row, col)? {
        ScalarValue::String(v) => Ok(v.clone()),
        other => Err(MsError::ColumnTypeMismatch {
            column: col.to_string(),
            table: "subtable".to_string(),
            expected: "String".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Extract an array cell reference.
pub(crate) fn get_array<'a>(table: &'a Table, row: usize, col: &str) -> MsResult<&'a ArrayValue> {
    Ok(table.get_array_cell(row, col)?)
}

/// Extract an optional `i32` from a scalar cell.
pub(crate) fn optional_i32(table: &Table, row: usize, col: &str) -> MsResult<Option<i32>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(get_i32(table, row, col)?))
}

/// Extract an optional `f64` from a scalar cell.
pub(crate) fn optional_f64(table: &Table, row: usize, col: &str) -> MsResult<Option<f64>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(get_f64(table, row, col)?))
}

/// Extract an optional `bool` from a scalar cell.
pub(crate) fn optional_bool(table: &Table, row: usize, col: &str) -> MsResult<Option<bool>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(get_bool(table, row, col)?))
}

/// Extract an optional `String` from a scalar cell.
pub(crate) fn optional_string(table: &Table, row: usize, col: &str) -> MsResult<Option<String>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(get_string(table, row, col)?))
}

/// Extract an optional array cell reference.
pub(crate) fn optional_array<'a>(
    table: &'a Table,
    row: usize,
    col: &str,
) -> MsResult<Option<&'a ArrayValue>> {
    if !has_column(table, col) {
        return Ok(None);
    }
    Ok(Some(get_array(table, row, col)?))
}

/// Check if a column exists in the table's schema.
pub(crate) fn has_column(table: &Table, col: &str) -> bool {
    table.schema().is_some_and(|s| s.contains_column(col))
}

/// Set a scalar cell.
pub(crate) fn set_scalar(
    table: &mut Table,
    row: usize,
    col: &str,
    value: ScalarValue,
) -> MsResult<()> {
    table.set_cell(row, col, Value::Scalar(value))?;
    Ok(())
}

/// Set an array cell.
pub(crate) fn set_array(
    table: &mut Table,
    row: usize,
    col: &str,
    value: ArrayValue,
) -> MsResult<()> {
    table.set_cell(row, col, Value::Array(value))?;
    Ok(())
}
