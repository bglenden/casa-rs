// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's calls that report or move a table's state, as CASA
//! 6.7.6.14's `tb` does them (`tools/casa-compat/table_tool_oracle.py`,
//! scenarios `state` and `state_ms`): `flush`, `resync`, `datachanged`,
//! `ismultiused`, `endianformat`, `setmaxcachesize` and `clearlocks`.
//!
//! `flush` has nothing to write: every change the tool makes is on the disk
//! when its call returns (`write.rs`); a table that keeps its write lock
//! announces them, as casacore's flush does. `setmaxcachesize` checks its
//! arguments as CASA does and sets nothing, since casa-tables sizes the
//! caches of its storage managers itself.

use casa_tables::{EndianFormat, LockType, Table};
use casa_types::{RecordValue, Value};

use super::registry;
use super::state::{Fail, State, no_table_specified};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// Whether `record` holds a reference to another table, at any depth.
fn holds_table_reference(record: &RecordValue) -> bool {
    record.fields().iter().any(|field| match &field.value {
        Value::TableRef(_) => true,
        Value::Record(inner) => holds_table_reference(inner),
        _ => false,
    })
}

/// Whether `table` has subtables: a table-valued keyword of its own or of a
/// column.
pub(super) fn has_subtables(table: &Table) -> bool {
    holds_table_reference(table.keywords())
        || table.schema().is_some_and(|schema| {
            schema.columns().iter().any(|column| {
                table
                    .column_keywords(column.name())
                    .is_some_and(holds_table_reference)
            })
        })
}

impl State {
    /// `endianformat()`: the byte order of the table's files; `little`
    /// with no table open.
    pub(super) fn endianformat(&self, log: &mut Vec<LogLine>) -> Reply {
        let big = match self.open.as_ref() {
            Some(open) => match open.table.stored_endian_format()? {
                Some(format) => format == EndianFormat::BigEndian,
                None => {
                    return Err(Fail::Unsupported(
                        "table.endianformat of a table that is a concatenation of tables is not \
                         yet implemented in casa-rs (#1028)"
                            .to_string(),
                    ));
                }
            },
            None => {
                no_table_specified(log);
                false
            }
        };
        Ok(CasaValue::Text(
            if big { "big" } else { "little" }.to_string(),
        ))
    }

    /// `flush()`. Every change reaches the disk when the call that makes it
    /// returns; what is left is the announcement of them to the table's other
    /// users, which a table holding its write lock (the user and permanent
    /// modes) makes now, as casacore's flush does, and keeps the lock.
    pub(super) fn flush(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_mut() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        open.table.publish_writes()?;
        Ok(CasaValue::Bool(true))
    }

    /// `resync()`: the table takes what another process, or another tool of
    /// this process, has written since it last looked. casacore reads the
    /// lock file without locking; here a table that holds a lock reads it
    /// again and reloads if it has changed, and a table that holds none takes
    /// the read lock, which reloads it if it has changed, and gives it back,
    /// so its lock state is as it was. A table that cannot be locked is not
    /// resynchronized, and the call says so.
    pub(super) fn resync(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_mut() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if open.table.has_lock(LockType::Read) {
            open.table.resync_locked()?;
        } else {
            if !open.table.lock(LockType::Read, open.lock.attempts())? {
                return Err(Fail::Runtime(
                    "Table::resync: the table could not be locked to read what has changed in it"
                        .to_string(),
                ));
            }
            open.table.unlock_metadata_only()?;
        }
        Ok(CasaValue::Bool(true))
    }

    /// `datachanged()`, as casacore's `Table::hasDataChanged`: a table that
    /// holds no lock tries for a read lock without waiting (another process
    /// writing means the data is changing) and gives it back; then the
    /// modify counter the lock file publishes is compared with the one the
    /// last call saw.
    pub(super) fn datachanged(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_mut() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        let took_lock = !open.table.has_lock(LockType::Read);
        if took_lock && !open.table.lock(LockType::Read, 1)? {
            return Ok(CasaValue::Bool(true));
        }
        let counter = open.table.published_modify_counter();
        if took_lock {
            open.table.unlock_metadata_only()?;
        }
        let counter = counter?;
        let changed = counter != open.last_modify_counter;
        open.last_modify_counter = counter;
        Ok(CasaValue::Bool(changed))
    }

    /// `ismultiused(checksubtables)`: whether another process has the table
    /// open. Subtables are not looked at: asking for them of a table that
    /// has any is not implemented.
    pub(super) fn ismultiused(&mut self, check_subtables: bool, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if check_subtables && has_subtables(&open.table) {
            return Err(Fail::Unsupported(
                "table.ismultiused(checksubtables=True) of a table with subtables is not yet \
                 implemented in casa-rs (#1028)"
                    .to_string(),
            ));
        }
        Ok(CasaValue::Bool(open.table.is_multi_used()))
    }

    /// `setmaxcachesize(columnname, nbytes)`, in CASA's order of checks.
    pub(super) fn setmaxcachesize(
        &self,
        column: &str,
        nbytes: i64,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if column.is_empty() {
            log.push(LogLine::warning("Need to specify a column name"));
            return Ok(CasaValue::Bool(false));
        }
        if nbytes <= 0 {
            log.push(LogLine::warning(
                "Need to specify cache size greater than 0",
            ));
            return Ok(CasaValue::Bool(false));
        }
        open.known(column)?;
        Ok(CasaValue::Bool(true))
    }

    /// `clearlocks()`: every table of the process that locks itself
    /// automatically gives up the lock it keeps between calls, whichever tool
    /// has it open, as casacore's `Table::relinquishAutoLocks` does; the
    /// tables locked by the user or permanently keep theirs. Always `True`,
    /// with or without a table. A tool busy in another thread is not asked.
    pub(super) fn clearlocks(&mut self) -> Reply {
        let mut result = self.relinquish_auto_lock();
        registry::for_each_other(self.id, |tool| {
            if let Some(tool) = tool {
                let released = tool.relinquish_auto_lock();
                if result.is_ok() {
                    result = released;
                }
            }
        });
        result?;
        Ok(CasaValue::Bool(true))
    }

    /// Gives up the lock an automatic mode keeps between calls.
    fn relinquish_auto_lock(&mut self) -> Result<(), Fail> {
        if let Some(open) = self.open.as_mut()
            && open.lock.automatic()
            && (open.table.has_lock(LockType::Read) || open.table.has_lock(LockType::Write))
        {
            open.table.unlock_metadata_only()?;
        }
        Ok(())
    }
}
