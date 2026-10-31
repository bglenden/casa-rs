// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's structural writes, as CASA 6.7.6.14's `tb` does them
//! (`tools/casa-compat/table_tool_oracle.py`, scenario `rows_columns`):
//! `renamecol` and `removecols` change `table.dat` in place and delete the
//! files of a storage manager left with no column, whatever the table's
//! layout (`Table::rename_column_in_place`, `Table::remove_columns_in_place`);
//! `addrows` and `removerows` write the table in full as it is laid out,
//! which casa-tables can do for one `StManAipsIO` group only
//! (`Table::single_stman_aipsio_options`). A table of other storage managers
//! refuses them with `NotImplementedError`, and never has a row loaded before
//! the refusal (#1028 names the library's missing in-place row changes).
//!
//! Departures from CASA 6.7.6.14, recorded in `docs/CASA (C++) bugs.md`:
//! `addrows` of a negative count removes rows there (the count wraps); here
//! it is refused.

use casa_tables::{ArrayShapeContract, ColumnSchema, ColumnType, Table, TableOptions};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::keyword_value::Incoming;
use super::new_column::parse;
use super::state::{Fail, State};
use super::write::{Persist, Target};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// A refusal in casacore's `Invalid Table operation` wording.
fn invalid(message: String) -> Fail {
    Fail::Runtime(format!("Invalid Table operation: {message}"))
}

/// The options that write `table` in full as it is laid out, or the refusal
/// of a layout casa-tables cannot rewrite without changing it.
pub(super) fn whole_table(table: &Table, what: &str) -> Result<TableOptions, Fail> {
    table.single_stman_aipsio_options()?.ok_or_else(|| {
        Fail::Unsupported(format!(
            "table.{what} of a table that is not one StManAipsIO group (without a manager name \
             of its own) is not yet implemented \
             in casa-rs: casa-tables cannot change the rows or columns of other storage \
             managers in place (#1028)"
        ))
    })
}

/// What a column holds in a row nobody has written: zero, false or the
/// empty string for a scalar, zeros for a fixed-shape array, an empty record
/// for a record column; no cell for an array of variable shape.
pub(super) fn default_row(table: &Table) -> RecordValue {
    let Some(schema) = table.schema() else {
        return RecordValue::default();
    };
    let fields = schema
        .columns()
        .iter()
        .filter_map(|column| {
            default_value(column).map(|value| RecordField::new(column.name(), value))
        })
        .collect();
    RecordValue::new(fields)
}

/// What `column` holds in a row nobody has written (see [`default_row`]).
fn default_value(column: &ColumnSchema) -> Option<Value> {
    match (column.column_type(), column.data_type()) {
        (ColumnType::Scalar, Some(ty)) => Some(Value::Scalar(default_scalar(ty))),
        (ColumnType::Array(ArrayShapeContract::Fixed { shape }), Some(ty)) => {
            Some(Value::Array(default_array(ty, shape)))
        }
        (ColumnType::Record, _) => Some(Value::Record(RecordValue::default())),
        _ => None,
    }
}

fn default_scalar(ty: PrimitiveType) -> ScalarValue {
    match ty {
        PrimitiveType::Bool => ScalarValue::Bool(false),
        PrimitiveType::UInt8 => ScalarValue::UInt8(0),
        PrimitiveType::UInt16 => ScalarValue::UInt16(0),
        PrimitiveType::UInt32 => ScalarValue::UInt32(0),
        PrimitiveType::Int16 => ScalarValue::Int16(0),
        PrimitiveType::Int32 => ScalarValue::Int32(0),
        PrimitiveType::Int64 => ScalarValue::Int64(0),
        PrimitiveType::Float32 => ScalarValue::Float32(0.0),
        PrimitiveType::Float64 => ScalarValue::Float64(0.0),
        PrimitiveType::Complex32 => ScalarValue::Complex32(Default::default()),
        PrimitiveType::Complex64 => ScalarValue::Complex64(Default::default()),
        PrimitiveType::String => ScalarValue::String(String::new()),
    }
}

fn default_array(ty: PrimitiveType, shape: &[usize]) -> ArrayValue {
    fn zeros<T: Clone + Default>(shape: &[usize]) -> ArrayD<T> {
        ArrayD::from_elem(IxDyn(shape), T::default())
    }
    match ty {
        PrimitiveType::Bool => ArrayValue::Bool(zeros(shape)),
        PrimitiveType::UInt8 => ArrayValue::UInt8(zeros(shape)),
        PrimitiveType::UInt16 => ArrayValue::UInt16(zeros(shape)),
        PrimitiveType::UInt32 => ArrayValue::UInt32(zeros(shape)),
        PrimitiveType::Int16 => ArrayValue::Int16(zeros(shape)),
        PrimitiveType::Int32 => ArrayValue::Int32(zeros(shape)),
        PrimitiveType::Int64 => ArrayValue::Int64(zeros(shape)),
        PrimitiveType::Float32 => ArrayValue::Float32(zeros(shape)),
        PrimitiveType::Float64 => ArrayValue::Float64(zeros(shape)),
        PrimitiveType::Complex32 => ArrayValue::Complex32(zeros(shape)),
        PrimitiveType::Complex64 => ArrayValue::Complex64(zeros(shape)),
        PrimitiveType::String => ArrayValue::String(zeros(shape)),
    }
}

impl State {
    /// The open table's path as CASA words it in a message, or empty.
    fn table_path(&self) -> String {
        self.open
            .as_ref()
            .map(|open| open.name.display().to_string())
            .unwrap_or_default()
    }

    /// `renamecol(oldname, newname)`.
    pub(super) fn renamecol(&mut self, old: String, new: String, log: &mut Vec<LogLine>) -> Reply {
        let path = self.table_path();
        let (old_memory, new_memory) = (old.clone(), new.clone());
        let result = self.write_with(
            Target::Structure("Table::renameColumn"),
            log,
            move |table| {
                let schema = table.schema();
                if schema.and_then(|schema| schema.column(&old)).is_none() {
                    return Err(invalid(format!(
                        "Table::renameColumn; column {old} does not exist in table {path}"
                    )));
                }
                if schema.and_then(|schema| schema.column(&new)).is_some() {
                    return Err(invalid(format!(
                        "Table::renameColumn; new column {new} already exists in table {path}"
                    )));
                }
                if table.is_memory() {
                    table.rename_column(&old, &new)?;
                } else {
                    table.rename_column_in_place(&old, &new)?;
                }
                Ok(Persist::Done)
            },
        );
        if matches!(&result, Ok(CasaValue::Bool(true)))
            && let Some(open) = self.open.as_mut()
            && open.memory_added_columns.remove(&old_memory)
        {
            open.memory_added_columns.insert(new_memory);
        }
        result
    }

    /// `removecols(columnames)`: all or none of the columns go.
    pub(super) fn removecols(&mut self, columns: Vec<String>, log: &mut Vec<LogLine>) -> Reply {
        let path = self.table_path();
        let result = self.write_with(
            Target::Structure("Table::removeColumn"),
            log,
            move |table| {
                let schema = table.schema();
                if let Some(missing) = columns
                    .iter()
                    .find(|name| schema.and_then(|schema| schema.column(name)).is_none())
                {
                    return Err(invalid(format!(
                        "Table::removeColumn - column {missing} does not exist in table {path}"
                    )));
                }
                let mut distinct: Vec<&str> = Vec::with_capacity(columns.len());
                for name in &columns {
                    if !distinct.contains(&name.as_str()) {
                        distinct.push(name);
                    }
                }
                if distinct.is_empty() {
                    return Ok(Persist::Nothing);
                }
                // One StManAipsIO group keeps every column in one file, so a
                // column cannot be taken out of it in place: the table is
                // written again without it.
                if table.is_memory() {
                    for name in &distinct {
                        table.remove_column(name)?;
                    }
                    return Ok(Persist::Done);
                }
                match table.single_stman_aipsio_options()? {
                    Some(options) => {
                        for name in &distinct {
                            table.remove_column(name)?;
                        }
                        table.save(options)?;
                    }
                    None => table.remove_columns_in_place(&distinct)?,
                }
                Ok(Persist::Done)
            },
        );
        if let Some(open) = self.open.as_mut()
            && open.table.is_memory()
        {
            let schema = open.table.schema();
            open.memory_added_columns
                .retain(|name| schema.and_then(|schema| schema.column(name)).is_some());
        }
        result
    }

    /// `addcols(desc, dminfo)`: columns of default values (a cell of an array
    /// of variable shape undefined), all or none of them added (CASA adds the
    /// ones before a column that fails, which casa-rs does not follow:
    /// `docs/CASA (C++) bugs.md`). A column goes into the table's one storage
    /// manager; naming another, or a `dminfo`, is not implemented.
    pub(super) fn addcols(
        &mut self,
        description: Vec<(String, Incoming)>,
        with_dminfo: bool,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let before = self
            .open
            .as_ref()
            .filter(|open| open.table.is_memory())
            .map(|open| {
                open.table
                    .schema()
                    .into_iter()
                    .flat_map(|schema| schema.columns())
                    .map(|column| column.name().to_string())
                    .collect::<std::collections::HashSet<_>>()
            });
        let result = self.write_with(Target::Structure("Table::addColumn"), log, move |table| {
            // The description is a record, whose fields casatools sorts by
            // name: the columns are added in that order.
            let mut description = description;
            description.sort_by(|a, b| a.0.cmp(&b.0));
            let columns: Vec<_> = description
                .iter()
                .map(|(name, column)| parse(name, column))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();
            let schema = table.schema();
            if let Some(existing) = columns.iter().find(|column| {
                schema.is_some_and(|schema| schema.column(column.schema.name()).is_some())
            }) {
                return Err(Fail::Runtime(format!(
                    "Invalid description of column {}: column already exists",
                    existing.schema.name()
                )));
            }
            if columns.is_empty() {
                return Ok(Persist::Nothing);
            }
            if with_dminfo || columns.iter().any(|column| column.names_manager) {
                return Err(Fail::Unsupported(
                    "table.addcols with a storage manager of its own is not yet implemented in \
                     casa-rs: a column goes into the table's one StManAipsIO group (#1028)"
                        .to_string(),
                ));
            }
            let options = if table.is_memory() {
                None
            } else {
                Some(whole_table(table, "addcols")?)
            };
            for column in columns {
                let name = column.schema.name().to_string();
                let default = default_value(&column.schema);
                table.add_column(column.schema, default)?;
                if let Some(keywords) = column.keywords {
                    table.set_column_keywords(&name, keywords);
                }
            }
            if let Some(options) = options {
                table.save(options)?;
                // Refresh the data-manager description after a full save.
                table.resync()?;
            }
            Ok(Persist::Done)
        });
        if let (Some(before), Some(open)) = (before, self.open.as_mut()) {
            for column in open
                .table
                .schema()
                .into_iter()
                .flat_map(|schema| schema.columns())
            {
                if !before.contains(column.name()) {
                    open.memory_added_columns.insert(column.name().to_string());
                }
            }
        }
        result
    }

    /// `addrows(nrow)`: rows of default values, a cell of an array of
    /// variable shape left undefined.
    pub(super) fn addrows(&mut self, count: i64, log: &mut Vec<LogLine>) -> Reply {
        self.write_with(Target::Structure("Table::addRow"), log, move |table| {
            let count = usize::try_from(count).map_err(|_| {
                Fail::Runtime(
                    "TableProxy::addRow: the number of rows must not be negative".to_string(),
                )
            })?;
            if count == 0 {
                return Ok(Persist::Nothing);
            }
            let options = if table.is_memory() {
                None
            } else {
                Some(whole_table(table, "addrows")?)
            };
            let row = default_row(table);
            for _ in 0..count {
                table.add_row(row.clone())?;
            }
            if let Some(options) = options {
                table.save(options)?;
            }
            Ok(Persist::Done)
        })
    }

    /// `removerows(rownrs)`, as `Table::removeRow(Vector)` does it: the row
    /// numbers are taken from the highest down, a repeated number removing
    /// the row that has moved into its place, each against the table as it
    /// then is. A number beyond the table refuses the call, naming the
    /// highest. (casacore removes the rows it got to before it fails on a
    /// repeated number past the end; here none is removed.)
    pub(super) fn removerows(&mut self, rows: Vec<i64>, log: &mut Vec<LogLine>) -> Reply {
        let path = self.table_path();
        self.write_with(Target::Structure("Table::rowmoveRow"), log, move |table| {
            if rows.is_empty() {
                return Ok(Persist::Nothing);
            }
            // casacore refuses at once when a manager cannot remove rows.
            if table.data_manager_info().iter().any(|manager| {
                manager.dm_type == "IncrementalStMan" || manager.dm_type.starts_with("Tiled")
            }) {
                return Err(invalid(format!(
                    "Rows cannot be removed from table {path}; its storage managers do not \
                     support it"
                )));
            }
            // A negative number is the huge unsigned one casacore sees.
            let mut numbers: Vec<u64> = rows.into_iter().map(|row| row as u64).collect();
            numbers.sort_unstable_by(|a, b| b.cmp(a));
            let nrows = table.row_count();
            let mut left: Vec<usize> = (0..nrows).collect();
            for number in numbers {
                if number >= left.len() as u64 {
                    return Err(invalid(format!(
                        "removeRow: rownr {number} too high in table {path} (#rows={})",
                        left.len()
                    )));
                }
                left.remove(number as usize);
            }
            let mut kept = left.into_iter().peekable();
            let removed: Vec<usize> = (0..nrows)
                .filter(|row| {
                    let stays = kept.peek() == Some(row);
                    if stays {
                        kept.next();
                    }
                    !stays
                })
                .collect();
            let options = if table.is_memory() {
                None
            } else {
                Some(whole_table(table, "removerows")?)
            };
            table.remove_rows(&removed)?;
            if let Some(options) = options {
                table.save(options)?;
            }
            Ok(Persist::Done)
        })
    }
}
