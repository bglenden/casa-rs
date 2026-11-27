// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL `query`, `taql`, and `calc` bindings for CASA's `table` tool (#1028).

use std::path::{Path, PathBuf};

use casa_tables::taql::{
    self, CalcOutput, TaqlError, TaqlResult, eval::ExprValue, evaluate_calc, execute_for_tool,
    execute_readonly_for_tool,
};
use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};
use num_complex::Complex64;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use super::copy::{
    check_target, refuse_nested, replace_with, same_directory, used_by_another_process,
};
use super::create::scratch_name;
use super::registry;
use super::state::{Fail, LockRequest, LockSettings, Open, State, no_table, no_table_specified};
use super::write::{Persist, Target, write_root};
use super::{LOGGER, PyTableTool, flag};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

type Reply = Result<CasaValue, Fail>;

fn optional_text(
    value: Option<&Bound<'_, PyAny>>,
    argument: &str,
    default: &str,
) -> PyResult<String> {
    value.map_or_else(
        || Ok(default.to_string()),
        |v| {
            v.extract::<String>()
                .map_err(|_| PyTypeError::new_err(format!("argument _{argument} must be a string")))
        },
    )
}

fn taql_err(cmd: &str, err: TaqlError) -> Fail {
    Fail::Runtime(format!("Error in TaQL command: '{cmd}'\n  {err}"))
}

fn stmt_table_and_giving(
    stmt: &taql::Statement,
) -> Result<(Option<&str>, Option<&taql::ast::GivingClause>, bool), Fail> {
    match stmt {
        taql::Statement::Select(sel) => Ok((
            sel.from.as_ref().map(|t| t.name.as_str()),
            sel.giving.as_ref(),
            false,
        )),
        taql::Statement::Count(cnt) => {
            Ok((cnt.from.as_ref().map(|t| t.name.as_str()), None, false))
        }
        taql::Statement::Update(upd) => {
            Ok((upd.table.as_ref().map(|t| t.name.as_str()), None, true))
        }
        taql::Statement::Insert(ins) => {
            Ok((ins.table.as_ref().map(|t| t.name.as_str()), None, true))
        }
        taql::Statement::Delete(del) => {
            Ok((del.table.as_ref().map(|t| t.name.as_str()), None, true))
        }
        taql::Statement::Calc(_)
        | taql::Statement::CountSelect(_)
        | taql::Statement::AlterTable(_)
        | taql::Statement::CreateTable(_)
        | taql::Statement::DropTable(_) => {
            Err(Fail::Runtime("Failed AlwaysAssert isTable()".to_string()))
        }
    }
}

fn build_query_command(
    source_path: &Path,
    query: &str,
    name: &str,
    sortlist: &str,
    columns: &str,
    style: &str,
) -> String {
    let mut cmd = String::new();
    let style = style.trim();
    if !style.is_empty() {
        cmd.push_str("using style ");
        cmd.push_str(style);
        cmd.push(' ');
    }
    cmd.push_str("select ");
    let columns = columns.trim();
    if !columns.is_empty() {
        cmd.push_str(columns);
        cmd.push(' ');
    }
    cmd.push_str("from \"");
    cmd.push_str(&source_path.display().to_string());
    cmd.push('"');
    let query = query.trim();
    if !query.is_empty() {
        cmd.push_str(" where ");
        cmd.push_str(query);
    }
    let sortlist = sortlist.trim();
    if !sortlist.is_empty() {
        cmd.push_str(" orderby ");
        cmd.push_str(sortlist);
    }
    let name = name.trim();
    if !name.is_empty() {
        cmd.push_str(" giving \"");
        cmd.push_str(name);
        cmd.push('"');
    }
    cmd
}

fn paths_equal(a: &Path, b: &Path) -> bool {
    same_directory(a, b)
        || matches!(
            (std::path::absolute(a), std::path::absolute(b)),
            (Ok(pa), Ok(pb)) if pa == pb
        )
}

fn validate_giving_target(state: Option<&State>, open: &Open, target: &Path) -> Result<(), Fail> {
    for protected in [open.name.as_path(), open.root_name.as_path()]
        .into_iter()
        .chain(open.plain_paths.iter().map(PathBuf::as_path))
    {
        if paths_equal(target, protected) {
            return Err(Fail::Runtime(format!(
                "Target table {} cannot be the same as the open table",
                target.display()
            )));
        }
        refuse_nested(target, protected)?;
    }
    if let Some(state) = state {
        state.refuse_open_target(target)?;
    }
    check_target(target)?;
    if used_by_another_process(target) {
        return Err(Fail::Runtime(format!(
            "Table {} is open in another process and was not replaced",
            target.display()
        )));
    }
    Ok(())
}

fn save_select_result(
    source: &Table,
    row_indices: &[usize],
    columns: &[String],
    target: &Path,
) -> Result<(), Fail> {
    replace_with(target, |staging| {
        if columns.is_empty() {
            let view = source.select_rows(row_indices)?;
            view.save(TableOptions::new(staging))?;
        } else {
            let view = source.select_rows_and_columns(row_indices, columns)?;
            view.save(TableOptions::new(staging))?;
        }
        Ok(())
    })
}

fn persist_mutating_table(
    table: &mut Table,
    stmt: &taql::Statement,
    affected_rows: &[usize],
) -> Result<(), Fail> {
    if table.is_memory() {
        return Ok(());
    }
    if let taql::Statement::Update(upd) = stmt {
        if affected_rows.is_empty() {
            return Ok(());
        }
        let mut columns: Vec<String> = upd.assignments.iter().map(|a| a.column.clone()).collect();
        columns.sort_unstable();
        columns.dedup();
        let mut rows = affected_rows.to_vec();
        rows.sort_unstable();
        rows.dedup();
        let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        match table.prepare_write().save_selected_rows(&col_refs, &rows) {
            Ok(()) => return Ok(()),
            Err(casa_tables::TableError::Storage(msg))
                if msg.starts_with("partial in-place save does not support") => {}
            Err(err) => return Err(Fail::from(err)),
        }
    }
    match table.single_rewrite_options()? {
        Some(options) => table.save(options).map_err(Fail::from),
        None => Err(Fail::Unsupported(
            "table.taql mutating statement on a multi-manager table is not yet implemented in \
             casa-rs (#1037)"
                .to_string(),
        )),
    }
}

fn open_from_select(
    open: &Open,
    source: &Table,
    row_indices: Vec<usize>,
    columns: Vec<String>,
    parent_writable: bool,
    giving: Option<(&Path, bool)>,
) -> Result<Open, Fail> {
    if let Some((target, is_memory)) = giving {
        if is_memory {
            let materialized = materialize_plain_subset(source, &row_indices, &columns)?;
            return Ok(Open::from_detached_table(
                materialized,
                target.to_path_buf(),
                true,
                LockSettings::from_request(&LockRequest::default())?,
            ));
        }
        save_select_result(source, &row_indices, &columns, target)?;
        return Open::attach(
            &target.display().to_string(),
            &LockRequest::default(),
            parent_writable,
        );
    }
    let cols = if columns.is_empty() {
        open.columns.clone()
    } else {
        Some(columns)
    };
    Ok(open.view_columns(scratch_name(), row_indices, cols))
}

fn open_from_taql_result(
    open: &Open,
    root_table: &Table,
    result: TaqlResult,
    writable: bool,
    giving: Option<(&Path, bool)>,
) -> Result<Open, Fail> {
    match result {
        TaqlResult::Select {
            row_indices,
            columns,
        } => {
            let root_rows = open.translate_rows(&row_indices);
            open_from_select(open, root_table, root_rows, columns, writable, giving)
        }
        TaqlResult::Materialized { table } => open_from_materialized(*table, giving),
        _ => Err(Fail::Runtime("Failed AlwaysAssert isTable()".to_string())),
    }
}

fn open_view_table(open: &Open) -> Result<Table, Fail> {
    let view_table = Table::open(TableOptions::new(&open.name))?;
    if let Some(ref cols) = open.columns {
        let rows: Vec<usize> = (0..view_table.row_count()).collect();
        return materialize_plain_subset(&view_table, &rows, cols);
    }
    Ok(view_table)
}

fn execute_readonly_on_open(
    open: &Open,
    stmt: &taql::Statement,
    cmd: &str,
) -> Result<TaqlResult, Fail> {
    if open.is_view() {
        if !open.name.exists() {
            return Err(Fail::Runtime(format!(
                "Error in TaQL command: '{cmd}'\n  Table {} does not exist",
                open.name.display()
            )));
        }
        let view_table = open_view_table(open)?;
        return execute_readonly_for_tool(stmt, &view_table).map_err(|e| taql_err(cmd, e));
    }
    let root = open.root();
    execute_readonly_for_tool(stmt, &root.table).map_err(|e| taql_err(cmd, e))
}

fn materialize_plain_subset(
    source: &Table,
    row_indices: &[usize],
    columns: &[String],
) -> Result<Table, Fail> {
    use casa_core::{RecordField, RecordValue};
    use casa_tables::TableSchema;

    let selected_cols: Vec<casa_tables::ColumnSchema> = if columns.is_empty() {
        source
            .schema()
            .map(|s| s.columns().to_vec())
            .unwrap_or_default()
    } else {
        let Some(schema) = source.schema() else {
            return Err(Fail::Runtime("table has no schema".into()));
        };
        columns
            .iter()
            .filter_map(|c| schema.column(c).cloned())
            .collect()
    };
    let schema =
        TableSchema::new(selected_cols.clone()).map_err(|e| Fail::Runtime(e.to_string()))?;
    let mut table = Table::with_schema_memory(schema);
    let col_names: Vec<&str> = selected_cols.iter().map(|c| c.name()).collect();
    for &row_idx in row_indices {
        let row = source.row_accessor().row(row_idx)?;
        let fields = col_names
            .iter()
            .filter_map(|&name| row.get(name).map(|val| RecordField::new(name, val.clone())))
            .collect();
        table.add_row(RecordValue::new(fields))?;
    }
    Ok(table)
}

fn open_from_materialized(table: Table, giving: Option<(&Path, bool)>) -> Result<Open, Fail> {
    if let Some((target, is_memory)) = giving {
        if is_memory {
            return Ok(Open::from_detached_table(
                table,
                target.to_path_buf(),
                true,
                LockSettings::from_request(&LockRequest::default())?,
            ));
        }
        replace_with(target, |staging| {
            table.save(TableOptions::new(staging))?;
            Ok(())
        })?;
        return Open::attach(&target.display().to_string(), &LockRequest::default(), true);
    }
    Ok(Open::from_detached_table(
        table,
        scratch_name(),
        true,
        LockSettings::from_request(&LockRequest::default())?,
    ))
}

fn execute_mutating_on_disk(
    path: &Path,
    command: &str,
    stmt: &taql::Statement,
) -> Result<(Open, Vec<usize>), Fail> {
    if !path.exists() {
        return Err(Fail::Runtime(format!(
            "Table {} does not exist",
            path.display()
        )));
    }
    let options = LockOptions::new(LockMode::AutoLocking).read_only(false);
    let mut table = Table::open_with_lock(TableOptions::new(path), options)?;
    if !table.lock(LockType::Write, 0)? {
        return Err(Fail::Runtime(format!(
            "Table {} could not be write locked",
            path.display()
        )));
    }
    if table.is_reference_table()? {
        let _ = table.unlock_metadata_only_or_discard();
        return Err(Fail::Unsupported(
            "writing through an opened reference table is not yet implemented in casa-rs (#1028)"
                .to_string(),
        ));
    }
    let outcome = execute_for_tool(stmt, &mut table);
    let persisted = match outcome {
        Ok(ref exec) => persist_mutating_table(&mut table, stmt, &exec.affected_rows),
        Err(_) => Ok(()),
    };
    if outcome.is_err() || persisted.is_err() {
        let _ = table.resync_or_discard();
    }
    let _ = table.unlock_metadata_only_or_discard();
    let exec = outcome.map_err(|e| taql_err(command, e))?;
    persisted?;
    let attached = Open::attach(&path.display().to_string(), &LockRequest::default(), true)?;
    let row_map = match exec.result {
        TaqlResult::Delete { .. } => Vec::new(),
        _ => exec.affected_rows,
    };
    let view = attached.view(scratch_name(), row_map);
    Ok((view, Vec::new()))
}

fn array_expr_to_casavalue(arr: &taql::eval::ArrayValue) -> CasaValue {
    let shape = if arr.shape.is_empty() {
        vec![0]
    } else {
        arr.shape.clone()
    };
    if arr.data.is_empty() {
        return CasaValue::Floats {
            shape,
            values: Vec::new(),
        };
    }
    scalars_to_casavalue(&arr.data, shape)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScalarKind {
    Bool,
    Int,
    Float,
    Complex,
    String,
}

fn promote_kind(a: ScalarKind, b: ScalarKind) -> ScalarKind {
    use ScalarKind::*;
    match (a, b) {
        (String, _) | (_, String) => String,
        (Complex, _) | (_, Complex) => Complex,
        (Float, _) | (_, Float) => Float,
        (Int, _) | (_, Int) => Int,
        (Bool, Bool) => Bool,
    }
}

fn value_scalar_kind(v: &ExprValue) -> ScalarKind {
    match v {
        ExprValue::Bool(_) | ExprValue::Null => ScalarKind::Bool,
        ExprValue::Int(_) => ScalarKind::Int,
        ExprValue::Float(_) | ExprValue::Quantity(_) | ExprValue::DateTime(_) => ScalarKind::Float,
        ExprValue::Complex(_) => ScalarKind::Complex,
        ExprValue::String(_)
        | ExprValue::Regex { .. }
        | ExprValue::Array(_)
        | ExprValue::MaskedArray(..) => ScalarKind::String,
    }
}

fn scalars_to_casavalue(values: &[ExprValue], shape: Vec<usize>) -> CasaValue {
    let kind = values
        .iter()
        .map(value_scalar_kind)
        .reduce(promote_kind)
        .unwrap_or(ScalarKind::Bool);
    match kind {
        ScalarKind::Bool => CasaValue::Bools {
            shape,
            values: values
                .iter()
                .map(|v| matches!(v, ExprValue::Bool(true)))
                .collect(),
        },
        ScalarKind::Int => CasaValue::Ints {
            shape,
            values: values
                .iter()
                .map(|v| match v {
                    ExprValue::Int(i) => *i,
                    ExprValue::Bool(b) => i64::from(*b),
                    _ => 0,
                })
                .collect(),
        },
        ScalarKind::Float => CasaValue::Floats {
            shape,
            values: values
                .iter()
                .map(|v| match v {
                    ExprValue::Float(f) | ExprValue::DateTime(f) => *f,
                    ExprValue::Quantity(q) => q.value(),
                    ExprValue::Int(i) => *i as f64,
                    ExprValue::Bool(b) => f64::from(*b),
                    _ => 0.0,
                })
                .collect(),
        },
        ScalarKind::Complex => CasaValue::Complexes {
            shape,
            values: values
                .iter()
                .map(|v| match v {
                    ExprValue::Complex(c) => *c,
                    ExprValue::Float(f) | ExprValue::DateTime(f) => Complex64::new(*f, 0.0),
                    ExprValue::Quantity(q) => Complex64::new(q.value(), 0.0),
                    ExprValue::Int(i) => Complex64::new(*i as f64, 0.0),
                    _ => Complex64::new(0.0, 0.0),
                })
                .collect(),
        },
        ScalarKind::String => CasaValue::TextArray {
            shape,
            values: values
                .iter()
                .map(|v| match v {
                    ExprValue::String(s) => s.clone(),
                    ExprValue::Regex { pattern, .. } => pattern.clone(),
                    ExprValue::Int(i) => i.to_string(),
                    ExprValue::Float(f) => f.to_string(),
                    ExprValue::Bool(b) => b.to_string(),
                    ExprValue::Null => String::new(),
                    other => format!("{other:?}"),
                })
                .collect(),
        },
    }
}

fn calc_output_to_casavalue(out: CalcOutput) -> CasaValue {
    if out.values.is_empty() {
        return super::values::empty_column(out.empty_scalar_type);
    }
    if out
        .values
        .iter()
        .any(|v| matches!(v, ExprValue::Array(_) | ExprValue::MaskedArray(..)))
    {
        return CasaValue::record(out.values.iter().enumerate().map(|(i, v)| {
            let val = match v {
                ExprValue::Array(arr) => array_expr_to_casavalue(arr),
                ExprValue::MaskedArray(arr, _) => array_expr_to_casavalue(arr),
                scalar => scalars_to_casavalue(std::slice::from_ref(scalar), vec![1]),
            };
            (i.to_string(), val)
        }));
    }
    let shape = match out.single_index_ndim {
        Some(ndim) => {
            let mut s = vec![1usize; ndim];
            s.push(out.values.len());
            s
        }
        None => vec![out.values.len()],
    };
    scalars_to_casavalue(&out.values, shape)
}

impl State {
    /// `query(query='String', name='', sortlist='', columns='', style='')`.
    pub(super) fn query(
        &mut self,
        query: &str,
        name: &str,
        sortlist: &str,
        columns: &str,
        style: &str,
        log: &mut Vec<LogLine>,
    ) -> Result<Open, Fail> {
        let Some(open_check) = self.open.as_ref() else {
            no_table(log);
            no_table_specified(log);
            return Err(Fail::Runtime("Unable to create table".into()));
        };
        let open_name = open_check.name.clone();
        if open_check.is_view() && !open_name.exists() {
            let cmd = build_query_command(&open_name, query, name, sortlist, columns, style);
            return Err(Fail::Runtime(format!(
                "Error in TaQL command: '{cmd}'\n  Table {} does not exist",
                open_name.display()
            )));
        }
        let target_opt = if !name.trim().is_empty() {
            let target =
                std::path::absolute(name.trim()).map_err(|e| Fail::Runtime(e.to_string()))?;
            validate_giving_target(Some(self), open_check, &target)?;
            Some(target)
        } else {
            None
        };

        self.synchronize()?;
        let cmd = build_query_command(&open_name, query, name, sortlist, columns, style);
        let stmt = taql::parse(&cmd).map_err(|e| taql_err(&cmd, e))?;

        let open = self.open.as_ref().expect("checked above");
        let result = execute_readonly_on_open(open, &stmt, &cmd)?;
        let root = open.root();
        open_from_taql_result(
            open,
            &root.table,
            result,
            root.writable,
            target_opt.as_deref().map(|t| (t, false)),
        )
    }

    /// `taql(taqlcommand='TaQL expression')`.
    pub(super) fn taql(&mut self, command: &str, log: &mut Vec<LogLine>) -> Result<Open, Fail> {
        if self.open.is_none() {
            no_table(log);
            no_table_specified(log);
            return Err(Fail::Runtime("Unable to create table".into()));
        }
        let stmt = taql::parse(command).map_err(|e| taql_err(command, e))?;
        let (table_name_opt, giving_opt, mutating) = stmt_table_and_giving(&stmt)?;

        let giving_target = if let Some(giving) = giving_opt {
            if giving.output_options.is_some() || giving.table_name.is_empty() {
                return Err(Fail::Runtime(
                    "This table TaQL output form is not implemented in casa-rs (#1037)".into(),
                ));
            }
            let is_memory = giving
                .output_type
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("MEMORY"));
            let target = if is_memory {
                PathBuf::from(&giving.table_name)
            } else {
                let abs = std::path::absolute(&giving.table_name)
                    .map_err(|e| Fail::Runtime(e.to_string()))?;
                let open = self.open.as_ref().expect("checked above");
                validate_giving_target(Some(self), open, &abs)?;
                abs
            };
            Some((target, is_memory))
        } else {
            None
        };

        let open_name = self.open.as_ref().expect("checked above").name.clone();
        let target_path = table_name_opt.map(PathBuf::from);
        let targets_open = target_path
            .as_ref()
            .is_none_or(|p| paths_equal(p, &open_name));

        if !mutating {
            let giving_ref = giving_target.as_ref().map(|(p, mem)| (p.as_path(), *mem));
            if targets_open {
                self.synchronize()?;
                let open = self.open.as_ref().expect("checked above");
                let res = execute_readonly_on_open(open, &stmt, command)?;
                let root = open.root();
                return open_from_taql_result(open, &root.table, res, root.writable, giving_ref);
            }
            let src_path = target_path.expect("checked");
            if let Some((giving_path, false)) = giving_target.as_ref()
                && (paths_equal(&src_path, giving_path)
                    || refuse_nested(giving_path, &src_path).is_err())
            {
                return Err(Fail::Runtime(format!(
                    "Target table {} cannot be the same as the source table",
                    giving_path.display()
                )));
            }
            if !src_path.exists() {
                return Err(Fail::Runtime(format!(
                    "Error in TaQL command: '{command}'\n  Table {} does not exist",
                    src_path.display()
                )));
            }
            let disk_open = Open::attach(
                &src_path.display().to_string(),
                &LockRequest::default(),
                false,
            )?;
            if let Some((giving_path, false)) = giving_target.as_ref() {
                validate_giving_target(None, &disk_open, giving_path)?;
            }
            let root = disk_open.root();
            let res =
                execute_readonly_for_tool(&stmt, &root.table).map_err(|e| taql_err(command, e))?;
            return open_from_taql_result(&disk_open, &root.table, res, false, giving_ref);
        }

        // Mutating TaQL (UPDATE, INSERT, DELETE).
        if targets_open {
            let open = self.open.as_ref().expect("checked above");
            if open.is_view() {
                if !open.name.exists() {
                    return Err(Fail::Runtime(format!(
                        "Error in TaQL command: '{command}'\n  Table {} does not exist",
                        open.name.display()
                    )));
                }
                return Err(Fail::Unsupported(
                    "writing through an opened reference table is not yet implemented in casa-rs \
                     (#1028)"
                        .to_string(),
                ));
            }
        }
        let abs_target = match target_path {
            Some(ref p) => std::path::absolute(p).map_err(|e| Fail::Runtime(e.to_string()))?,
            None => std::path::absolute(&open_name).map_err(|e| Fail::Runtime(e.to_string()))?,
        };
        let use_open_root = targets_open && {
            let open = self.open.as_ref().expect("checked above");
            let root = open.root();
            root.writable
                && (open.is_memory || root.lock.automatic() || root.table.has_lock(LockType::Write))
        };
        if use_open_root {
            let open = self.open.as_ref().expect("checked above");
            let name = open.name.display().to_string();
            let row_map = open.row_map.clone();
            let mut root = open.root();
            if root.table.is_reference_table()? {
                return Err(Fail::Unsupported(
                    "writing through an opened reference table is not yet implemented in casa-rs \
                     (#1028)"
                        .to_string(),
                ));
            }
            let mut affected_out: Vec<usize> = Vec::new();
            let stmt_clone = stmt.clone();
            let write_target = match &stmt_clone {
                taql::Statement::Update(_) => Target::Cells,
                _ => Target::Structure("Table::taql"),
            };
            let reply = write_root(
                &mut root,
                &name,
                write_target,
                row_map.as_deref(),
                |table, _| {
                    let exec =
                        execute_for_tool(&stmt_clone, table).map_err(|e| taql_err(command, e))?;
                    affected_out = match exec.result {
                        TaqlResult::Delete { .. } => Vec::new(),
                        _ => exec.affected_rows,
                    };
                    if let taql::Statement::Update(ref upd) = stmt_clone {
                        if affected_out.is_empty() {
                            return Ok(Persist::Nothing);
                        }
                        let columns: Vec<String> =
                            upd.assignments.iter().map(|a| a.column.clone()).collect();
                        return Ok(Persist::Rows {
                            columns,
                            rows: affected_out.clone(),
                        });
                    }
                    match table.single_rewrite_options()? {
                        Some(options) => {
                            table.save(options)?;
                            Ok(Persist::Done)
                        }
                        None if table.is_memory() => Ok(Persist::Done),
                        None => Err(Fail::Unsupported(
                            "table.taql mutating statement on a multi-manager table is not yet \
                             implemented in casa-rs (#1037)"
                                .to_string(),
                        )),
                    }
                },
            );
            reply?;
            drop(root);
            return Ok(open.view(scratch_name(), affected_out));
        }

        let (view, _) = execute_mutating_on_disk(&abs_target, command, &stmt)?;
        Ok(view)
    }

    /// `calc(expr, prefix='using style base0, endincl, fortranorder', showtaql=False)`.
    pub(super) fn calc(
        &mut self,
        expr: &str,
        prefix: &str,
        showtaql: bool,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        if self.open.is_none() {
            no_table(log);
        }
        let full_cmd = if prefix.trim().is_empty() {
            format!("calc {expr}")
        } else {
            format!("{} calc {expr}", prefix.trim())
        };
        if showtaql {
            println!("TaQL string: {full_cmd}");
        }
        let stmt = taql::parse(&full_cmd).map_err(|e| taql_err(&full_cmd, e))?;
        let taql::Statement::Calc(calc_stmt) = stmt else {
            return Err(Fail::Runtime(format!(
                "Error in TaQL command: '{full_cmd}'\n  expected CALC statement"
            )));
        };
        if self.open.is_some() {
            self.synchronize()?;
        }
        let output = match self.open.as_ref() {
            Some(open) if !open.is_view() => {
                let root = open.root();
                evaluate_calc(&calc_stmt, Some((open.name.as_path(), &root.table)))
            }
            Some(open) if open.name.exists() => {
                let view_table = open_view_table(open)?;
                evaluate_calc(&calc_stmt, Some((open.name.as_path(), &view_table)))
            }
            _ => evaluate_calc(&calc_stmt, None),
        }
        .map_err(|e| taql_err(&full_cmd, e))?;
        Ok(calc_output_to_casavalue(output))
    }
}

#[pymethods]
impl PyTableTool {
    /// Evaluate a TaQL `CALC` expression.
    #[pyo3(signature = (expr, prefix = None, showtaql = None), text_signature = "($self, expr, prefix='using style base0, endincl, fortranorder', showtaql=False)")]
    fn calc(
        &self,
        py: Python<'_>,
        expr: &Bound<'_, PyAny>,
        prefix: Option<&Bound<'_, PyAny>>,
        showtaql: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let expr = expr
            .extract::<String>()
            .map_err(|_| PyTypeError::new_err("argument _expr must be a string"))?;
        let prefix = optional_text(prefix, "prefix", "using style base0, endincl, fortranorder")?;
        let showtaql = flag(showtaql, false)?;
        self.call(py, move |state, log| {
            state.calc(&expr, &prefix, showtaql, log)
        })
    }

    /// Select rows and columns via a TaQL `SELECT` query, returning a new `table` tool.
    #[pyo3(signature = (query = None, name = None, sortlist = None, columns = None, style = None), text_signature = "($self, query='String', name='', sortlist='', columns='', style='')")]
    fn query(
        &self,
        py: Python<'_>,
        query: Option<&Bound<'_, PyAny>>,
        name: Option<&Bound<'_, PyAny>>,
        sortlist: Option<&Bound<'_, PyAny>>,
        columns: Option<&Bound<'_, PyAny>>,
        style: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let query = optional_text(query, "query", "String")?;
        let name = optional_text(name, "name", "")?;
        let sortlist = optional_text(sortlist, "sortlist", "")?;
        let columns = optional_text(columns, "columns", "")?;
        let style = optional_text(style, "style", "")?;
        let mut lines = Vec::new();
        let next_open = py.detach(|| {
            self.guard()
                .query(&query, &name, &sortlist, &columns, &style, &mut lines)
        });
        emit(py, LOGGER, &lines)?;
        let tool = Self {
            state: registry::track(State::new(Some(next_open?))),
        };
        Ok(Py::new(py, tool)?.into_any())
    }

    /// Execute a full TaQL expression returning a table tool.
    #[pyo3(signature = (taqlcommand = None), text_signature = "($self, taqlcommand='TaQL expression')")]
    fn taql(&self, py: Python<'_>, taqlcommand: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let cmd = optional_text(taqlcommand, "taqlcommand", "TaQL expression")?;
        let mut lines = Vec::new();
        let next_open = py.detach(|| self.guard().taql(&cmd, &mut lines));
        emit(py, LOGGER, &lines)?;
        let tool = Self {
            state: registry::track(State::new(Some(next_open?))),
        };
        Ok(Py::new(py, tool)?.into_any())
    }
}
