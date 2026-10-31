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

use casa_tables::{LockType, Table, TableError, TableOptions};
use casa_types::{RecordField, RecordValue, Value};

use super::keyword_value::Incoming;
use super::read::KeywordRef;
use super::state::{Fail, State, no_table};
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
}

/// What a change leaves to write to the disk.
pub(super) enum Persist {
    /// The table's keywords and info.
    Metadata,
    /// The cells of `rows` in `column`, in place in the column's storage
    /// manager.
    Rows { column: String, rows: Vec<usize> },
    /// Nothing: the call selected no cell.
    Nothing,
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
fn put(
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
        self.write_with(target, log, move |table| {
            change(table).map(|()| Persist::Metadata)
        })
    }

    /// As [`write`](Self::write), for a change that says what to persist.
    pub(super) fn write_with(
        &mut self,
        target: Target,
        log: &mut Vec<crate::tools::LogLine>,
        change: impl FnOnce(&mut Table) -> Result<Persist, Fail>,
    ) -> Reply {
        let Some(open) = self.open.as_mut() else {
            no_table(log);
            return Ok(CasaValue::Bool(false));
        };
        let name = open.name.display().to_string();
        if !open.writable {
            if matches!(target, Target::Cells) {
                return Ok(CasaValue::Bool(false));
            }
            return runtime(match target {
                Target::Cells => String::new(),
                Target::TableKeywords => {
                    format!("Table::rwKeywordSet cannot be used: table {name} is not writable")
                }
                Target::ColumnKeywords => {
                    format!(
                        "TableColumn::rwKeywordSet cannot be used: table {name} is not writable"
                    )
                }
                Target::Info => format!("Table {name} is not writable"),
            });
        }
        let automatic = open.lock.automatic();
        if automatic {
            if !open.table.lock(LockType::Write, open.lock.attempts())? {
                return runtime(format!("Table {name} could not be write locked"));
            }
        } else if !open.table.has_lock(LockType::Write) {
            return runtime(format!(
                "ColumnSet::doLock: table {name} should be locked when using UserLocking"
            ));
        }
        let saved = change(&mut open.table).and_then(|persist| match persist {
            Persist::Nothing => Ok(()),
            Persist::Metadata => open
                .table
                .save_metadata_only(TableOptions::new(&open.name))
                .map_err(Fail::from),
            Persist::Rows { column, rows } => {
                let partial = open
                    .table
                    .prepare_write()
                    .save_selected_rows(&[column.as_str()], &rows);
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
                        match open.table.single_stman_aipsio_options()? {
                            Some(options) => open.table.save(options).map_err(Fail::from),
                            None => Err(Fail::from(TableError::Storage(message))),
                        }
                    }
                    other => other.map_err(Fail::from),
                }
            }
        });
        // A table that holds the write lock and has changes it has not
        // flushed is written in full, as StManAipsIO, when it is closed or
        // dropped: a `user` table closed without an unlock, a `permanent`
        // one always (a `casa-tables` defect, #1035; this reload is the
        // workaround and goes with its fix). The change is on disk, or part
        // of it is if the save failed part way; either way the table is
        // reloaded as the disk has it, so that nothing is left to flush, and
        // what reached the disk is still published when the lock is
        // released. A reload that fails marks the table clean all the same
        // (`resync_or_discard`): the full write must never be left to a
        // drop. An automatic table releases through
        // `unlock_metadata_only_or_discard`, which does the same when the
        // release fails. The release comes last whatever else went wrong,
        // so an error never leaves the write lock held with changes.
        let reloaded = if saved.is_err() || !automatic {
            open.table.resync_or_discard().map_err(Fail::from)
        } else {
            Ok(())
        };
        let released = if automatic {
            open.table
                .unlock_metadata_only_or_discard()
                .map_err(Fail::from)
        } else {
            Ok(())
        };
        saved?;
        reloaded?;
        released?;
        Ok(CasaValue::Bool(true))
    }

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
