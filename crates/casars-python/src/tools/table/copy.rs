// SPDX-License-Identifier: LGPL-3.0-or-later
//! `copy` and `copyrows`, as CASA 6.7.6.14's `tb` does them
//! (`tools/casa-compat/table_tool_oracle.py`, scenarios `copies` and
//! `copyrows`).
//!
//! `copy` is casacore's `TableProxy::copy`. Unless values, no rows or a
//! `dminfo` are asked for, the table's directory is copied as it is (`deep`
//! alone changes nothing, as in casacore); otherwise the table is written
//! again in full, which casa-tables can do for one `StManAipsIO` group only
//! (`Table::single_stman_aipsio_options`), the other layouts refusing with
//! `NotImplementedError` before anything is written. The result is a tool
//! open read-only on the copy under the default locking.
//!
//! `copyrows` writes the rows into the output table as `addrows` does, a
//! table of one `StManAipsIO` group in full; it takes the columns both
//! tables have, by name, and leaves the others as they are.
//!
//! Not implemented, and refusing with `NotImplementedError`: `memorytable`,
//! a `dminfo`, and a copy that would have to copy subtables.

use std::fs;
use std::path::Path;

use casa_tables::{ColumnSchema, ColumnType, EndianFormat, Table, TableSchema};
use casa_types::{ArrayValue, ScalarValue, Value};

use super::housekeeping::has_subtables;
use super::state::{Fail, LockRequest, Open, State, no_table_specified};
use super::write::{Persist, Target};
use super::write_structure::{default_row, whole_table};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// The arguments of `copy`.
pub(crate) struct CopyRequest {
    pub(crate) new_name: String,
    pub(crate) value_copy: bool,
    pub(crate) has_dminfo: bool,
    pub(crate) endian: String,
    pub(crate) memory: bool,
    pub(crate) no_rows: bool,
}

/// `makeEndianFormat`: the byte order a name asks for.
fn endian_format(name: &str) -> Result<EndianFormat, Fail> {
    match name.to_ascii_lowercase().as_str() {
        "big" => Ok(EndianFormat::BigEndian),
        "little" => Ok(EndianFormat::LittleEndian),
        "aipsrc" | "local" => Ok(EndianFormat::LocalEndian),
        _ => Err(Fail::Runtime(format!(
            "endian format '{name}' is incorrect;  must be aipsrc, local, big, or little"
        ))),
    }
}

/// `directory` and everything in it, copied to `to`.
fn copy_directory(directory: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else if kind.is_symlink() {
            copy_link(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// The symbolic link `link` made again at `to` (a platform without links
/// copies what it points at).
fn copy_link(link: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(fs::read_link(link)?, to)
    }
    #[cfg(not(unix))]
    {
        fs::copy(link, to).map(|_| ())
    }
}

/// `prepareCopyRename`: a table already at `target` goes. casacore removes
/// any directory there; casa-rs removes a table (a directory with a
/// `table.dat`) or an empty directory, and refuses anything else, so that a
/// slip of the name does not delete a directory of other files.
fn clear_target(target: &Path) -> Result<(), Fail> {
    let refusal = || {
        Fail::Runtime(format!(
            "Table {} already exists (and is not a true table directory)",
            target.display()
        ))
    };
    match fs::symlink_metadata(target) {
        Ok(meta) if meta.is_dir() => {
            let empty = fs::read_dir(target).map_err(io_failure)?.next().is_none();
            if empty || target.join("table.dat").is_file() {
                fs::remove_dir_all(target).map_err(io_failure)
            } else {
                Err(refusal())
            }
        }
        Ok(_) => Err(refusal()),
        Err(_) => Ok(()),
    }
}

fn io_failure(error: std::io::Error) -> Fail {
    Fail::Runtime(error.to_string())
}

/// Whether two paths name the same directory.
fn same_directory(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// The cells of the common columns that are to be copied, row by row; a
/// cell the input leaves undefined is `None`.
struct Rows {
    columns: Vec<String>,
    /// `cells[row][column]`.
    cells: Vec<Vec<Option<Value>>>,
}

/// Whether two columns hold the same kind of cell (scalar, array or record)
/// of the same data type, as a copy of one into the other needs.
fn same_kind(a: &ColumnSchema, b: &ColumnSchema) -> bool {
    std::mem::discriminant(a.column_type()) == std::mem::discriminant(b.column_type())
        && a.data_type() == b.data_type()
}

/// The columns `source` shares with the output table (`target`), in the
/// output's order; a shared column of another kind is refused.
fn shared_columns(source: &TableSchema, target: &TableSchema) -> Result<Vec<String>, Fail> {
    let mut shared = Vec::new();
    for wanted in target.columns() {
        let Some(column) = source.column(wanted.name()) else {
            continue;
        };
        if !same_kind(column, wanted) {
            return Err(Fail::Runtime(format!(
                "TableCopy::copyRows: column {} has another type in the output table",
                wanted.name()
            )));
        }
        shared.push(wanted.name().to_string());
    }
    Ok(shared)
}

/// The cells of `rows` in the `shared` columns of `source`.
fn read_rows(source: &Table, shared: &[String], rows: &[usize]) -> Result<Rows, Fail> {
    let Some(schema) = source.schema() else {
        return Ok(Rows {
            columns: Vec::new(),
            cells: Vec::new(),
        });
    };
    let mut columns = Vec::new();
    let mut by_column: Vec<Vec<Option<Value>>> = Vec::new();
    for name in shared {
        let Some(column) = schema.column(name) else {
            continue;
        };
        let accessor = source.column_accessor(name)?;
        let cells: Vec<Option<Value>> = match column.column_type() {
            ColumnType::Record => rows
                .iter()
                .map(|&row| {
                    accessor
                        .record_cell(row)
                        .map(|record| Some(Value::Record(record)))
                })
                .collect::<Result<_, _>>()?,
            ColumnType::Scalar => accessor
                .scalar_cells_owned_for_rows(rows)?
                .into_iter()
                .map(|cell: Option<ScalarValue>| cell.map(Value::Scalar))
                .collect(),
            _ => accessor
                .array_cells_owned(rows)?
                .into_iter()
                .map(|cell: Option<ArrayValue>| cell.map(Value::Array))
                .collect(),
        };
        columns.push(name.to_string());
        by_column.push(cells);
    }
    let cells = (0..rows.len())
        .map(|row| by_column.iter().map(|cells| cells[row].clone()).collect())
        .collect();
    Ok(Rows { columns, cells })
}

/// Puts `rows` into `table` from `start`, adding the rows it does not have.
fn put_rows(table: &mut Table, start: usize, rows: Rows) -> Result<(), Fail> {
    if rows.columns.is_empty() {
        return Ok(());
    }
    let wanted = start + rows.cells.len();
    let default = default_row(table);
    while table.row_count() < wanted {
        table.add_row(default.clone())?;
    }
    for (offset, cells) in rows.cells.into_iter().enumerate() {
        for (column, cell) in rows.columns.iter().zip(cells) {
            if let Some(value) = cell {
                table
                    .cell_accessor_mut(start + offset, column)?
                    .set(value)?;
            }
        }
    }
    Ok(())
}

/// The change `copyrows` makes to the output table: `rows` put from `start`,
/// the table written again in full as it is laid out.
fn put_in_full(rows: Rows, start: usize) -> impl FnOnce(&mut Table) -> Result<Persist, Fail> {
    move |table| {
        if rows.columns.is_empty() {
            return Ok(Persist::Nothing);
        }
        let options = whole_table(table, "copyrows")?;
        put_rows(table, start, rows)?;
        table.save(options)?;
        Ok(Persist::Done)
    }
}

/// The input rows `copyrows` copies, and the output row they go to. CASA
/// takes the input rows first (a start beyond them is `no such row`), then
/// checks the output row: a negative number is the end of the table, and a
/// number beyond it is refused.
fn gather(
    source: &Table,
    target: &Table,
    start_in: i64,
    start_out: i64,
    count: i64,
) -> Result<(usize, Rows), Fail> {
    let rows = super::read::rows(
        source.row_count(),
        start_in,
        count,
        1,
        "TableProxy::TableProxy::copyRows: no such row",
    )?;
    let nrows = target.row_count();
    let start = usize::try_from(start_out).unwrap_or(nrows);
    if start > nrows {
        return Err(Fail::Runtime(
            "TableProxy::copyRows: start output row too high".to_string(),
        ));
    }
    let shared = match (source.schema(), target.schema()) {
        (Some(source), Some(target)) => shared_columns(source, target)?,
        _ => Vec::new(),
    };
    // A table whose rows casa-tables cannot add in place refuses before a
    // cell is read.
    if !shared.is_empty() {
        whole_table(target, "copyrows")?;
    }
    Ok((start, read_rows(source, &shared, &rows)?))
}

impl State {
    /// `copyrows(outtable, startrowin, startrowout, nrow)`.
    pub(super) fn copyrows(
        &mut self,
        out: &str,
        start_in: i64,
        start_out: i64,
        count: i64,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        if self.open.is_none() {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        }
        if out.is_empty() {
            return Err(Fail::Runtime(
                "TableUtil.cc: Failed AlwaysAssert !names.empty()".to_string(),
            ));
        }
        let out_path = Path::new(out);
        if !out_path.exists() {
            return Err(Fail::Runtime(format!("Table {out} does not exist")));
        }
        self.synchronize()?;
        let source = self.open.as_ref().expect("checked above");
        let target = Target::Structure("TableCopy::copyRows");
        if same_directory(out_path, &source.name) {
            // The rows are taken before any is added.
            let (start, rows) = gather(&source.table, &source.table, start_in, start_out, count)?;
            return self.write_with(target, log, put_in_full(rows, start));
        }
        // CASA opens the output for update with this table's lock options,
        // the option `auto`.
        let request = LockRequest {
            option: Some("auto".to_string()),
            interval: Some(source.lock.interval()),
            maxwait: Some(source.lock.maxwait()),
        };
        let mut output = State {
            open: Some(Open::attach(out, &request, true)?),
        };
        let wanted = output.open.as_ref().expect("just opened");
        let (start, rows) = gather(&source.table, &wanted.table, start_in, start_out, count)?;
        output.write_with(target, log, put_in_full(rows, start))
    }

    /// `copy(...)`: the tool of the copy.
    pub(super) fn copy(
        &mut self,
        request: &CopyRequest,
        log: &mut Vec<LogLine>,
    ) -> Result<Open, Fail> {
        if self.open.is_none() {
            no_table_specified(log);
            return Err(Fail::Runtime("Unable to create table".to_string()));
        }
        let endian = endian_format(&request.endian)?;
        if request.memory {
            return Err(Fail::Unsupported(
                "table.copy(memorytable=True) is not yet implemented in casa-rs (#1028)"
                    .to_string(),
            ));
        }
        if request.has_dminfo {
            return Err(Fail::Unsupported(
                "table.copy with a dminfo is not yet implemented in casa-rs: a table is copied \
                 with the storage managers it has (#1028)"
                    .to_string(),
            ));
        }
        if request.new_name.is_empty() {
            return Err(Fail::Runtime(
                "BaseTable::makeAbsoluteName - table name '' is invalid".to_string(),
            ));
        }
        self.synchronize()?;
        let open = self.open.as_ref().expect("checked above");
        let target = std::path::absolute(&request.new_name).map_err(io_failure)?;
        let same = same_directory(&target, &open.name);
        // A copy inside the table, or a table inside the copy's name, would
        // copy a directory into itself or remove the table it copies.
        if !same && (target.starts_with(&open.name) || open.name.starts_with(&target)) {
            return Err(Fail::Runtime(format!(
                "Table::copy: {} and {} lie one inside the other",
                target.display(),
                open.name.display()
            )));
        }
        if request.value_copy || request.no_rows {
            if same {
                return Err(Fail::Runtime(format!(
                    "Table::deepCopy: new name equal to old name {}",
                    open.name.display()
                )));
            }
            let options = open.table.single_stman_aipsio_options()?.ok_or_else(|| {
                Fail::Unsupported(
                    "table.copy with values or without rows of a table that is not one \
                     StManAipsIO group (without a manager name of its own) is not yet \
                     implemented in casa-rs: casa-tables cannot write other storage managers \
                     afresh (#1028)"
                        .to_string(),
                )
            })?;
            if has_subtables(&open.table) {
                return Err(Fail::Unsupported(
                    "table.copy with values or without rows of a table with subtables is not \
                     yet implemented in casa-rs (#1028)"
                        .to_string(),
                ));
            }
            clear_target(&target)?;
            let options = options.with_path(&target).with_endian_format(endian);
            if request.no_rows {
                open.table.shallow_copy(options)?;
            } else {
                open.table.deep_copy(options)?;
            }
        } else if !same {
            clear_target(&target)?;
            copy_directory(&open.name, &target).map_err(io_failure)?;
        }
        Open::attach(
            &target.display().to_string(),
            &LockRequest::default(),
            false,
        )
    }
}
