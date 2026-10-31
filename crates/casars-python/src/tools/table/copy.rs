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
//! open read-only on the copy under the default locking; a copy onto the
//! table's own name is that table opened again, with the same lock options
//! and as writable as the first tool (CASA shares one table, locks held
//! included; here each tool has its own lock).
//!
//! `copyrows` writes the rows into the output table as `addrows` does, a
//! table of one `StManAipsIO` group in full; it takes the columns both
//! tables have, by name, and leaves the others as they are.
//!
//! Not implemented, and refusing with `NotImplementedError`: `memorytable`,
//! a `dminfo`, a copy that would have to copy subtables, and a copy that
//! would have to turn a reference table into a table of its own.
//!
//! A copy is made at a name beside the target and put in place of what is
//! there only when it is complete, so a copy that fails leaves the table it
//! was to replace as it was (casacore removes first). A table another tool of
//! this process has open, or another process, is not replaced.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_tables::{
    ColumnType, EndianFormat, LockMode, LockOptions, Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};

use super::cell_convert;
use super::housekeeping::has_subtables;
use super::registry;
use super::state::{Fail, LockRequest, Open, State, no_table_specified};
use super::write::{Persist, Target};
use super::write_structure::{default_row, whole_table};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// The arguments of `copy`.
pub(crate) struct CopyRequest {
    pub(crate) new_name: String,
    pub(crate) deep: bool,
    pub(crate) value_copy: bool,
    pub(crate) has_dminfo: bool,
    pub(crate) endian: String,
    pub(crate) memory: bool,
    pub(crate) no_rows: bool,
}

/// `makeEndianFormat`: the byte order a name asks for.
pub(super) fn endian_format(name: &str) -> Result<EndianFormat, Fail> {
    match name.to_ascii_lowercase().as_str() {
        "big" => Ok(EndianFormat::BigEndian),
        "little" => Ok(EndianFormat::LittleEndian),
        "aipsrc" | "local" => Ok(EndianFormat::LocalEndian),
        _ => Err(Fail::Runtime(format!(
            "endian format '{name}' is incorrect;  must be aipsrc, local, big, or little"
        ))),
    }
}

/// `directory` and everything in it, copied to `to`; the copies of files are
/// writable by their owner whatever the originals were, as casacore's
/// `Directory::copy` makes them (a table in a read-only directory of files
/// is still a table to update when copied).
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
            fs::copy(entry.path(), &target)?;
            make_user_writable(&target)?;
        }
    }
    Ok(())
}

/// Adds the owner's write permission to the regular file at `path`.
fn make_user_writable(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(permissions.mode() | 0o200);
        fs::set_permissions(path, permissions)
    }
    #[cfg(not(unix))]
    {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions)
    }
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

fn io_failure(error: std::io::Error) -> Fail {
    Fail::Runtime(error.to_string())
}

/// `path` with every symbolic link in it resolved. Where `path` does not
/// exist (yet), its deepest existing ancestor is resolved and the rest
/// appended, so that a name to be made is placed by the directory it will be
/// made in.
fn resolved(path: &Path) -> Result<PathBuf, Fail> {
    let mut missing = Vec::new();
    let mut base = path;
    loop {
        match fs::canonicalize(base) {
            Ok(real) => return Ok(missing.iter().rev().fold(real, |at, name| at.join(name))),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let (Some(name), Some(parent)) = (base.file_name(), base.parent()) else {
                    return Err(io_failure(error));
                };
                missing.push(name);
                base = parent;
            }
            Err(error) => return Err(io_failure(error)),
        }
    }
}

/// Whether two paths name the same directory.
pub(super) fn same_directory(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// A copy inside the table, or a table inside the copy's name, would copy a
/// directory into itself or remove the table it copies. The places are
/// compared with their symbolic links resolved, so that a link to the table
/// (or to a directory it lies in) cannot hide the nesting.
pub(super) fn refuse_nested(target: &Path, source: &Path) -> Result<(), Fail> {
    let (real_target, real_source) = (resolved(target)?, resolved(source)?);
    if real_target.starts_with(&real_source) || real_source.starts_with(&real_target) {
        return Err(Fail::Runtime(format!(
            "Table::copy: {} and {} lie one inside the other",
            target.display(),
            source.display()
        )));
    }
    Ok(())
}

/// `prepareCopyRename`: what is at `target` may be replaced when it is a
/// table (a directory with a `table.dat`) or an empty directory. casacore
/// removes any directory there; casa-rs refuses anything else, so that a slip
/// of the name does not delete a directory of other files.
pub(super) fn check_target(target: &Path) -> Result<(), Fail> {
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
                Ok(())
            } else {
                Err(refusal())
            }
        }
        Ok(_) => Err(refusal()),
        Err(_) => Ok(()),
    }
}

/// Whether another process has the table at `target` open. casacore refuses
/// to replace a table its own process has open; a table another process has
/// open would be replaced under its feet, and that process would write its
/// stale rows into the new table.
pub(super) fn used_by_another_process(target: &Path) -> bool {
    target.join("table.dat").is_file()
        && Table::open_with_lock(
            TableOptions::new(target),
            LockOptions::new(LockMode::UserLocking).read_only(true),
        )
        .is_ok_and(|table| table.is_multi_used())
}

/// A name beside `target` for a copy to be made under until it is complete.
pub(super) fn staging_path(target: &Path) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!(
        ".{name}.copying-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Makes the copy with `build` at a name beside `target`, then puts it in
/// place of what is at `target`. A copy that fails part-way is removed and
/// leaves the table at `target` as it was.
pub(super) fn replace_with(
    target: &Path,
    build: impl FnOnce(&Path) -> Result<(), Fail>,
) -> Result<(), Fail> {
    let staging = staging_path(target);
    let placed = build(&staging).and_then(|()| {
        check_target(target)?;
        if fs::symlink_metadata(target).is_ok() {
            fs::remove_dir_all(target).map_err(io_failure)?;
        }
        fs::rename(&staging, target).map_err(io_failure)
    });
    if placed.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    placed
}

/// A column the output shares with the input, and the type its cells are
/// converted to when the two columns differ in type.
struct Shared {
    name: String,
    convert_to: Option<PrimitiveType>,
}

/// The cells of the common columns that are to be copied, row by row; a
/// cell the input leaves undefined is `None`.
struct Rows {
    columns: Vec<String>,
    /// `cells[row][column]`.
    cells: Vec<Vec<Option<Value>>>,
}

/// The columns `source` shares with the output table (`target`), in the
/// output's order. A cell goes to a column of another type as casacore's
/// `TableRow` puts it (`cell_convert`); a shared column of another kind
/// (scalar, array, record), or of a type a cell cannot be converted to, is
/// refused.
fn shared_columns(source: &TableSchema, target: &TableSchema) -> Result<Vec<Shared>, Fail> {
    let mut shared = Vec::new();
    for wanted in target.columns() {
        let Some(column) = source.column(wanted.name()) else {
            continue;
        };
        if std::mem::discriminant(column.column_type())
            != std::mem::discriminant(wanted.column_type())
        {
            return Err(Fail::Runtime(format!(
                "TableCopy::copyRows: column {} has another type in the output table",
                wanted.name()
            )));
        }
        let convert_to = match (column.data_type(), wanted.data_type()) {
            (Some(from), Some(to)) if from != to => {
                if !cell_convert::convertible(wanted.column_type(), from, to) {
                    return Err(cell_convert::refusal(
                        wanted.column_type(),
                        wanted.name(),
                        to,
                    ));
                }
                Some(to)
            }
            _ => None,
        };
        shared.push(Shared {
            name: wanted.name().to_string(),
            convert_to,
        });
    }
    Ok(shared)
}

/// The cells of `rows` in the `shared` columns of `source`, converted to the
/// types of the output's columns.
fn read_rows(source: &Table, shared: &[Shared], rows: &[usize]) -> Result<Rows, Fail> {
    let Some(schema) = source.schema() else {
        return Ok(Rows {
            columns: Vec::new(),
            cells: Vec::new(),
        });
    };
    let mut columns = Vec::new();
    let mut by_column: Vec<std::vec::IntoIter<Option<Value>>> = Vec::new();
    for column in shared {
        let Some(description) = schema.column(&column.name) else {
            continue;
        };
        let accessor = source.column_accessor(&column.name)?;
        let cells: Vec<Option<Value>> = match description.column_type() {
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
        let cells = match column.convert_to {
            Some(to) => cells
                .into_iter()
                .map(|cell| {
                    cell.map(|value| cell_convert::convert(value, to))
                        .transpose()
                })
                .collect::<Result<_, _>>()?,
            None => cells,
        };
        columns.push(column.name.clone());
        by_column.push(cells.into_iter());
    }
    // The cells move from the columns to the rows: the selection is held
    // once, not twice.
    let cells = (0..rows.len())
        .map(|_| {
            by_column
                .iter_mut()
                .map(|cells| cells.next().expect("a cell of every column in every row"))
                .collect()
        })
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

/// What `copyrows` is to copy: the input rows, the output row they start at
/// and the columns the two tables share.
struct Plan {
    start: usize,
    rows: Vec<usize>,
    shared: Vec<Shared>,
}

impl Plan {
    /// Whether the rows written include a row still to be read: a table
    /// copied onto itself with the output starting after the input and
    /// inside it. casacore copies row by row, so such a row is read as the
    /// copy has already changed it.
    fn reads_what_it_wrote(&self) -> bool {
        self.rows
            .first()
            .is_some_and(|&first| self.start > first && self.start - first < self.rows.len())
    }
}

/// The change `copyrows` makes to a table copied onto itself when
/// [`Plan::reads_what_it_wrote`]: each row is read as it stands when its turn
/// comes, then put.
fn put_in_order(plan: Plan) -> impl FnOnce(&mut Table) -> Result<Persist, Fail> {
    move |table| {
        if plan.shared.is_empty() {
            return Ok(Persist::Nothing);
        }
        let options = whole_table(table, "copyrows")?;
        for (offset, &row) in plan.rows.iter().enumerate() {
            let cells = read_rows(table, &plan.shared, &[row])?;
            put_rows(table, plan.start + offset, cells)?;
        }
        table.save(options)?;
        Ok(Persist::Done)
    }
}

/// The input rows `copyrows` copies, and the output row they go to. CASA
/// takes the input rows first (a start beyond them is `no such row`), then
/// checks the output row: a negative number is the end of the table, and a
/// number beyond it is refused.
fn plan(
    source: &Table,
    target: &Table,
    start_in: i64,
    start_out: i64,
    count: i64,
) -> Result<Plan, Fail> {
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
    Ok(Plan {
        start,
        rows,
        shared,
    })
}

/// How `copy` makes its copy.
enum Method {
    /// A copy onto the table's own name: the table as it is.
    Keep,
    /// The table's directory copied as it is.
    Directory,
    /// The table written afresh, with these options (and no rows when the
    /// flag says so).
    Rewrite(TableOptions, bool),
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
        if same_directory(out_path, &source.name) {
            return self.copyrows_onto_itself(start_in, start_out, count, log);
        }
        // CASA opens the output for update with this table's lock options,
        // the option `auto`.
        let request = LockRequest {
            option: Some("auto".to_string()),
            interval: Some(source.lock.interval()),
            maxwait: Some(source.lock.maxwait()),
        };
        let mut output = State::new(Some(Open::attach(out, &request, true)?));
        let wanted = output.open.as_ref().expect("just opened");
        let plan = plan(&source.table, &wanted.table, start_in, start_out, count)?;
        let rows = read_rows(&source.table, &plan.shared, &plan.rows)?;
        let target = Target::Structure("TableCopy::copyRows");
        output.write_with(target, log, put_in_full(rows, plan.start))
    }

    /// `copyrows` of the table onto its own name. CASA opens the output for
    /// update, which is the table itself, read-only or not: a table opened
    /// `nomodify` is opened again for update, and is writable afterwards.
    fn copyrows_onto_itself(
        &mut self,
        start_in: i64,
        start_out: i64,
        count: i64,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.make_writable()?;
        let table = &self.open.as_ref().expect("checked above").table;
        let plan = plan(table, table, start_in, start_out, count)?;
        let target = Target::Structure("TableCopy::copyRows");
        if plan.reads_what_it_wrote() {
            return self.write_with(target, log, put_in_order(plan));
        }
        let rows = read_rows(table, &plan.shared, &plan.rows)?;
        self.write_with(target, log, put_in_full(rows, plan.start))
    }

    /// Opens the table again for update when it was opened `nomodify`, with
    /// the lock options it had. The old handle is released once the new one
    /// is open; a table that cannot be opened for update stays as it was.
    fn make_writable(&mut self) -> Result<(), Fail> {
        let Some(open) = self.open.as_ref() else {
            return Ok(());
        };
        if open.writable {
            return Ok(());
        }
        let name = open.name.display().to_string();
        let mut reopened = Open::attach(&name, &open.lock.request(), true)?;
        reopened.last_modify_counter = open.last_modify_counter;
        self.open = Some(reopened);
        Ok(())
    }

    /// How a copy named `new_name` is made, or why it is refused; nothing is
    /// touched.
    fn copy_method(
        &self,
        request: &CopyRequest,
        endian: EndianFormat,
        same: bool,
    ) -> Result<Method, Fail> {
        let open = self.open.as_ref().expect("a table is open");
        let rewrite = request.value_copy || request.no_rows;
        // CASA's two tools share one table and its write lock; here the
        // second tool would have to take a write lock the first holds.
        if same && open.writable && open.lock.permanent() {
            return Err(Fail::Unsupported(
                "table.copy onto the name of a table open for update under permanent locking \
                 is not yet implemented in casa-rs: the two tools would share one write lock \
                 (#1028)"
                    .to_string(),
            ));
        }
        // casacore's `RefTable::deepCopy` makes a table of its own of a
        // reference table whenever `deep` is given; a plain copy of one
        // stays a reference table, and is the directory copied.
        if open.table.is_reference_table()? && (request.deep || rewrite) {
            return Err(Fail::Unsupported(
                "table.copy of a reference table with deep, valuecopy or norows (which makes a \
                 table of its own of it) is not yet implemented in casa-rs (#1028)"
                    .to_string(),
            ));
        }
        if !rewrite {
            return Ok(if same {
                Method::Keep
            } else {
                Method::Directory
            });
        }
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
        Ok(Method::Rewrite(
            options.with_endian_format(endian),
            request.no_rows,
        ))
    }

    /// Refuses to replace a table another tool of this process has open
    /// (casacore's `prepareCopyRename` looks the target up in its table
    /// cache): that tool would go on writing its rows into the new table. A
    /// tool busy in another thread may be using it, and counts as using it.
    pub(super) fn refuse_open_target(&self, target: &Path) -> Result<(), Fail> {
        let mut in_use = false;
        registry::for_each_other(self.id, |tool| {
            in_use |= tool.is_none_or(|tool| {
                tool.open
                    .as_ref()
                    .is_some_and(|open| same_directory(&open.name, target))
            });
        });
        if in_use {
            return Err(Fail::Runtime(format!(
                "Invalid Table operation: Cannot copy/rename; target table {} is still open \
                 (is in the table cache)",
                target.display()
            )));
        }
        Ok(())
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
        if !same {
            refuse_nested(&target, &open.name)?;
        }
        let method = self.copy_method(request, endian, same)?;
        if !same {
            check_target(&target)?;
            self.refuse_open_target(&target)?;
            if used_by_another_process(&target) {
                return Err(Fail::Runtime(format!(
                    "Table {} is open in another process and was not replaced",
                    target.display()
                )));
            }
        }
        match method {
            Method::Keep => {}
            Method::Directory => replace_with(&target, |staging| {
                copy_directory(&open.name, staging).map_err(io_failure)
            })?,
            Method::Rewrite(options, no_rows) => replace_with(&target, |staging| {
                let options = options.with_path(staging);
                if no_rows {
                    open.table.shallow_copy(options)?;
                } else {
                    open.table.deep_copy(options)?;
                }
                Ok(())
            })?,
        }
        // The table copied onto its own name is that table: the tool of the
        // copy has its lock options and can write if it can. (CASA shares
        // one table between the two tools, locks held included.)
        let (lock, writable) = if same {
            (open.lock.request(), open.writable)
        } else {
            (LockRequest::default(), false)
        };
        Open::attach(&target.display().to_string(), &lock, writable)
    }
}
