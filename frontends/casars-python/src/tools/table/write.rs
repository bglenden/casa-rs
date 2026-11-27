// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's write side for keywords and the table info, as CASA
//! 6.7.6.14's `tb` does it (`tools/casa-compat/table_tool_oracle.py`).
//!
//! Every write is made through to the table on disk and touches only the
//! table's metadata: `Table::save_metadata_only` rewrites `table.dat`'s
//! keywords and `table.info` and leaves the storage managers as they are
//! (`Table::flush` or a plain unlock would rewrite the table as
//! StManAipsIO). A table opened with an automatic lock (`default`, `auto`,
//! `autonoread`) takes the write lock for the write and releases it after;
//! a table opened with `user` locking must hold the write lock, and one
//! opened with `permanent` locking holds it. A table opened read-only
//! refuses. With no table open a write logs and returns `False`.

use casa_core::{RecordField, RecordValue, Value};
use casa_tables::{LockType, Table, TableError, TableOptions};

use super::keyword_value::Incoming;
use super::read::KeywordRef;
use super::state::{Fail, RootOpen, State, no_table};
use crate::convert::value::CasaValue;

type Reply = Result<CasaValue, Fail>;

/// What a write changes; it names the refusal of a table not open for
/// update.
#[derive(Clone, Copy)]
pub(super) enum Target {
    TableKeywords,
    ColumnKeywords,
    Info,
    /// Cells: a table not open for update answers `False`, not an error.
    Cells,
    /// The table's rows or columns, as the casacore operation named here
    /// refuses a table not open for update.
    Structure(&'static str),
    /// A `tablerow` handle writing to a read-only table raises a `RuntimeError`.
    RowProxy,
}

/// What a change leaves to write to the disk.
pub(super) enum Persist {
    /// The table's keywords and info.
    Metadata,
    /// The cells of `rows` in `columns`, in place in each column's storage
    /// manager.
    Rows {
        columns: Vec<String>,
        rows: Vec<usize>,
    },
    /// Nothing: the call selected no cell.
    Nothing,
    /// The change has written itself to the disk (a structural change in
    /// place, or a table written in full).
    Done,
}

/// What `putinfo` changes: the fields its record has.
#[derive(Debug, Default)]
pub(crate) struct InfoUpdate {
    pub(crate) table_type: Option<String>,
    pub(crate) sub_type: Option<String>,
    pub(crate) readme: Option<String>,
}

fn runtime<T>(message: String) -> Result<T, Fail> {
    Err(Fail::Runtime(message))
}

/// The record a dotted `path` names inside `record`, made if `make` is set.
fn descend<'a>(
    record: &'a mut RecordValue,
    path: &str,
    make: bool,
) -> Result<&'a mut RecordValue, Fail> {
    let mut current = record;
    for part in path.split('.') {
        if current.get(part).is_none() {
            if !make {
                return runtime(format!("RecordInterface: field {part} is unknown"));
            }
            current.push(RecordField::new(
                part,
                Value::Record(RecordValue::new(Vec::new())),
            ));
        }
        match current.get_mut(part) {
            Some(Value::Record(inner)) => current = inner,
            _ => return runtime(format!("RecordInterface: field {part} is unknown")),
        }
    }
    Ok(current)
}

/// Defines `name` in `record`: a new field takes the value's natural type,
/// an existing one keeps its own.
fn store(record: &mut RecordValue, name: &str, value: Incoming) -> Result<(), Fail> {
    match record.get_mut(name) {
        Some(old) => {
            let new = value.like(old, name)?;
            *old = new;
        }
        None => record.push(RecordField::new(name, value.natural()?)),
    }
    Ok(())
}

/// `putkeyword`'s rule: by (dotted) name or by 1-based position.
pub(super) fn put(
    record: &mut RecordValue,
    key: &KeywordRef,
    value: Incoming,
    make_subrecord: bool,
) -> Result<(), Fail> {
    match key {
        KeywordRef::Whole => Ok(()),
        KeywordRef::Name(name) => match name.rsplit_once('.') {
            Some((parent, last)) => store(descend(record, parent, make_subrecord)?, last, value),
            None => store(record, name, value),
        },
        KeywordRef::Index(index) => {
            let slot = usize::try_from(*index)
                .ok()
                .filter(|slot| (1..=record.len()).contains(slot));
            let Some(slot) = slot else {
                return runtime("RecordInterface::define - new fieldNumber exceeds #fields".into());
            };
            let field = &mut record.fields_mut()[slot - 1];
            let new = value.like(&field.value, &field.name)?;
            field.value = new;
            Ok(())
        }
    }
}

/// `removekeyword`'s rule; `missing` words the refusal of an absent name.
fn remove(
    record: &mut RecordValue,
    key: &KeywordRef,
    missing: impl Fn(&str) -> String,
) -> Result<(), Fail> {
    match key {
        KeywordRef::Whole => Ok(()),
        KeywordRef::Name(name) => {
            let (parent, last) = name.rsplit_once('.').unwrap_or(("", name));
            let target = if parent.is_empty() {
                record
            } else {
                descend(record, parent, false).map_err(|_| Fail::Runtime(missing(name)))?
            };
            match target.remove(last) {
                Some(_) => Ok(()),
                None => runtime(missing(name)),
            }
        }
        KeywordRef::Index(index) => {
            let name = usize::try_from(*index)
                .ok()
                .and_then(|slot| slot.checked_sub(1))
                .and_then(|slot| record.fields().get(slot))
                .map(|field| field.name.clone());
            match name {
                Some(name) => {
                    record.remove(&name);
                    Ok(())
                }
                None => runtime("RecordRep::deleteDataField".into()),
            }
        }
    }
}

/// The column `column`'s keywords, edited by `edit`.
fn edit_column(
    table: &mut Table,
    column: &str,
    edit: impl FnOnce(&mut RecordValue) -> Result<(), Fail>,
) -> Result<(), Fail> {
    if table
        .schema()
        .and_then(|schema| schema.column(column))
        .is_none()
    {
        return runtime(format!("Table column {column} is unknown"));
    }
    let mut keywords = table
        .column_keywords(column)
        .cloned()
        .unwrap_or_else(|| RecordValue::new(Vec::new()));
    edit(&mut keywords)?;
    table.set_column_keywords(column, keywords);
    Ok(())
}

impl State {
    /// Applies `change` to the open table and saves its metadata, under the
    /// lock the table's lock options call for.
    fn write(
        &mut self,
        target: Target,
        log: &mut Vec<crate::tools::LogLine>,
        change: impl FnOnce(&mut Table) -> Result<(), Fail>,
    ) -> Reply {
        self.write_with(target, log, move |table, _| {
            change(table).map(|()| Persist::Metadata)
        })
    }

    /// As [`write`](Self::write), for a change that says what to persist.
    pub(super) fn write_with(
        &mut self,
        target: Target,
        log: &mut Vec<crate::tools::LogLine>,
        change: impl FnOnce(&mut Table, Option<&[usize]>) -> Result<Persist, Fail>,
    ) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table(log);
            return Ok(CasaValue::Bool(false));
        };
        let name = open.name.display().to_string();
        let row_map = open.row_map.clone();
        let mut root = open.root();
        write_root(&mut root, &name, target, row_map.as_deref(), change)
    }
}

/// Applies `change` directly to `root` and persists what it modified under
/// the table's lock policy. Shared by `State::write_with` and `PyTableRowTool::put`.
pub(super) fn write_root(
    root: &mut RootOpen,
    name: &str,
    target: Target,
    row_map: Option<&[usize]>,
    change: impl FnOnce(&mut Table, Option<&[usize]>) -> Result<Persist, Fail>,
) -> Reply {
    if !root.writable {
        if matches!(target, Target::Cells) {
            return Ok(CasaValue::Bool(false));
        }
        return runtime(match target {
            Target::Cells => String::new(),
            Target::TableKeywords => {
                format!("Table::rwKeywordSet cannot be used: table {name} is not writable")
            }
            Target::ColumnKeywords => {
                format!("TableColumn::rwKeywordSet cannot be used: table {name} is not writable")
            }
            Target::Info => format!("Table {name} is not writable"),
            Target::Structure(operation) => {
                format!("Invalid Table operation: {operation}; table {name} is not writable")
            }
            Target::RowProxy => "TableRowProxy: the given TableRow is not writable".into(),
        });
    }
    super::state::publish_component_keywords(&root.name)?;
    let named_memory = root.table.is_named_memory_manager();
    if root.table.is_memory() && !named_memory {
        change(&mut root.table, row_map)?;
        return Ok(CasaValue::Bool(true));
    }
    let is_reference = root.table.is_reference_table()?;
    if is_reference && matches!(target, Target::Structure(_)) {
        return Err(Fail::Unsupported(
            "structural edits of an opened reference table require a reference descriptor write"
                .to_string(),
        ));
    }
    let automatic = root.lock.automatic();
    let attempts = root.lock.attempts();
    if automatic {
        if !root.table.lock(LockType::Write, attempts)? {
            return runtime(format!("Table {name} could not be write locked"));
        }
    } else if !root.table.has_lock(LockType::Write) {
        return runtime(format!(
            "ColumnSet::doLock: table {name} should be locked when using UserLocking"
        ));
    }
    let saved = change(&mut root.table, row_map).and_then(|persist| {
        if named_memory {
            let metadata = !matches!(target, Target::Cells | Target::RowProxy)
                && !matches!(persist, Persist::Nothing);
            return root
                .table
                .finish_memory_manager_write(metadata)
                .map_err(Fail::from);
        }
        match persist {
            Persist::Nothing | Persist::Done => Ok(()),
            Persist::Metadata => root
                .table
                .save_metadata_only(TableOptions::new(&root.name))
                .map_err(Fail::from),
            Persist::Rows {
                mut columns,
                mut rows,
            } => {
                columns.sort_unstable();
                columns.dedup();
                if is_reference {
                    // Different persisted view rows can refer to the same
                    // root cell. Keep the last occurrence of each view row,
                    // in mutation order, so the last write to the root wins.
                    let mut seen = std::collections::HashSet::new();
                    rows.reverse();
                    rows.retain(|row| seen.insert(*row));
                    rows.reverse();
                } else {
                    rows.sort_unstable();
                    rows.dedup();
                }
                let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                let partial = root
                    .table
                    .prepare_write()
                    .save_selected_rows(&col_refs, &rows);
                match partial {
                    // StManAipsIO keeps its table in one file and writes it
                    // in full, as casacore does. A table that is one such
                    // group is written in full with the group's name and
                    // byte order kept, which is the layout it has. Any
                    // other table must not be rewritten (it would lose its
                    // managers), so the refusal stands.
                    Err(TableError::Storage(message))
                        if message.starts_with("partial in-place save does not support") =>
                    {
                        match root.table.single_rewrite_options()? {
                            Some(options) => root.table.save(options).map_err(Fail::from),
                            None => Err(Fail::from(TableError::Storage(message))),
                        }
                    }
                    other => other.map_err(Fail::from),
                }
            }
        }
    });
    // Successful saves clear only the edits they persisted. After a failure,
    // discard the unpersisted edits while still publishing anything that
    // reached disk; do not let closing the handle retry a failed mutation.
    let reloaded = if named_memory && saved.is_err() {
        root.table
            .finish_memory_manager_write(false)
            .map_err(Fail::from)
    } else if saved.is_err() || is_reference {
        // Repeated reference rows can alias the very same cell.
        root.table.resync_or_discard().map_err(Fail::from)
    } else {
        Ok(())
    };
    let released = if automatic {
        root.table
            .unlock_metadata_only_or_discard()
            .map_err(Fail::from)
    } else {
        Ok(())
    };
    saved?;
    reloaded?;
    released?;
    if matches!(
        target,
        Target::TableKeywords | Target::ColumnKeywords | Target::Info
    ) {
        // casacore shares the keyword object across its in-process handles.
        // Native component caches reload only after a completed metadata write.
        root.seen_keyword_epoch = root
            .keyword_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release)
            .wrapping_add(1);
    }
    Ok(CasaValue::Bool(true))
}

impl State {
    /// `putkeyword(keyword, value, makesubrecord)`.
    pub(super) fn putkeyword(
        &mut self,
        key: KeywordRef,
        value: Incoming,
        make_subrecord: bool,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        if matches!(&key, KeywordRef::Whole) {
            return Ok(CasaValue::Bool(false));
        }
        self.write(Target::TableKeywords, log, move |table| {
            put(table.keywords_mut(), &key, value, make_subrecord)
        })
    }

    /// `putkeywords(value)`: each field is defined as `putkeyword` does.
    pub(super) fn putkeywords(
        &mut self,
        fields: Vec<(String, Incoming)>,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        self.write(Target::TableKeywords, log, move |table| {
            for (name, value) in fields {
                put(table.keywords_mut(), &KeywordRef::Name(name), value, false)?;
            }
            Ok(())
        })
    }

    /// `removekeyword(keyword)`.
    pub(super) fn removekeyword(
        &mut self,
        key: KeywordRef,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        self.write(Target::TableKeywords, log, move |table| {
            remove(table.keywords_mut(), &key, |name| {
                format!("Table keyword {name} does not exist")
            })
        })
    }

    /// `putcolkeyword(columnname, keyword, value)`.
    pub(super) fn putcolkeyword(
        &mut self,
        column: String,
        key: KeywordRef,
        value: Incoming,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        if matches!(&key, KeywordRef::Whole) {
            return Ok(CasaValue::Bool(false));
        }
        if let Some(open) = self.open.as_ref()
            && !open.column_allowed(&column)
        {
            return runtime(format!("Table column {column} is unknown"));
        }
        self.write(Target::ColumnKeywords, log, move |table| {
            edit_column(table, &column, |keywords| put(keywords, &key, value, false))
        })
    }

    /// `putcolkeywords(columnname, value)`.
    pub(super) fn putcolkeywords(
        &mut self,
        column: String,
        fields: Vec<(String, Incoming)>,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        if let Some(open) = self.open.as_ref()
            && !open.column_allowed(&column)
        {
            return runtime(format!("Table column {column} is unknown"));
        }
        self.write(Target::ColumnKeywords, log, move |table| {
            edit_column(table, &column, |keywords| {
                for (name, value) in fields {
                    put(keywords, &KeywordRef::Name(name), value, false)?;
                }
                Ok(())
            })
        })
    }

    /// `removecolkeyword(columnname, keyword)`.
    pub(super) fn removecolkeyword(
        &mut self,
        column: String,
        key: KeywordRef,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        if let Some(open) = self.open.as_ref()
            && !open.column_allowed(&column)
        {
            return runtime(format!("Table column {column} is unknown"));
        }
        self.write(Target::ColumnKeywords, log, move |table| {
            edit_column(table, &column, |keywords| {
                remove(keywords, &key, |name| {
                    format!("Keyword {name} in column {column} does not exist")
                })
            })
        })
    }

    /// `putinfo(value)`: the fields `value` has replace the table's type,
    /// subtype and readme; the readme text is kept as lines.
    pub(super) fn putinfo(
        &mut self,
        update: InfoUpdate,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        self.write(Target::Info, log, move |table| {
            let mut info = table.info().clone();
            if let Some(table_type) = update.table_type {
                info.table_type = table_type;
            }
            if let Some(sub_type) = update.sub_type {
                info.sub_type = sub_type;
            }
            if let Some(readme) = update.readme {
                info.readme = readme.split('\n').map(str::to_string).collect();
            }
            table.set_info(info);
            Ok(())
        })
    }

    /// `addreadmeline(value)`: appends a line to the readme.
    pub(super) fn addreadmeline(
        &mut self,
        line: String,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Reply {
        self.write(Target::Info, log, move |table| {
            let mut info = table.info().clone();
            info.readme.push(line);
            table.set_info(info);
            Ok(())
        })
    }
}
