// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retain the attached keyword table, as CASA's ComponentList retains its Table.
//! Dirty metadata keeps its write lock until release or another in-process
//! tool needs the persisted table. Clean reads synchronize with the published
//! casacore modification counter under a read lock.
use crate::convert::{ConvertError, ConvertResult};
#[cfg(not(unix))]
use casa_tables::TableOptions;
use casa_tables::{Table, TableError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

static TABLES: Mutex<Vec<Weak<Shared>>> = Mutex::new(Vec::new());
static PENDING: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct Shared {
    pub(super) path: PathBuf,
    pub(super) cache: Mutex<Cache>,
}
pub(super) struct Cache {
    pub(super) table: Table,
    pub(super) dirty: bool,
}
fn fail(error: TableError) -> ConvertError {
    ConvertError::runtime(error.to_string())
}

pub(crate) fn attach(path: &Path) -> ConvertResult<Arc<Shared>> {
    let mut table = super::open(path)?;
    #[cfg(unix)]
    table.unlock_metadata_only().map_err(fail)?;
    let shared = Arc::new(Shared {
        path: path.to_path_buf(),
        cache: Mutex::new(Cache {
            table,
            dirty: false,
        }),
    });
    let mut tables = TABLES.lock().unwrap_or_else(PoisonError::into_inner);
    tables.retain(|table| table.strong_count() > 0);
    tables.push(Arc::downgrade(&shared));
    Ok(shared)
}

/// Table tools read from disk. Publish pending component-list keywords before
/// they take their own locks, without holding the registry while doing I/O.
pub(crate) fn flush_pending() -> ConvertResult<()> {
    flush_except(None)
}
pub(super) fn flush_except(own: Option<&Arc<Shared>>) -> ConvertResult<()> {
    if PENDING.load(Ordering::Acquire) == 0 {
        return Ok(());
    }
    let tables = TABLES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    for table in tables {
        if own.is_some_and(|own| Arc::ptr_eq(own, &table)) {
            continue;
        }
        table
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .flush(&table.path)?;
    }
    Ok(())
}
impl Cache {
    pub(super) fn read<T>(
        &mut self,
        _path: &Path,
        work: impl FnOnce(&Table) -> ConvertResult<T>,
    ) -> ConvertResult<T> {
        if self.dirty {
            return work(&self.table);
        }
        #[cfg(unix)]
        {
            // lock() reloads only when casacore's published counter changes.
            // Filesystem timestamps are not a table generation identifier.
            if !self
                .table
                .lock(casa_tables::LockType::Read, 0)
                .map_err(fail)?
            {
                return Err(ConvertError::runtime("Cannot lock table to read keywords"));
            }
            let result = work(&self.table);
            let released = self.table.unlock_metadata_only().map_err(fail);
            result.and_then(|value| released.map(|()| value))
        }
        #[cfg(not(unix))]
        {
            self.table = Table::open(TableOptions::new(_path)).map_err(fail)?;
            work(&self.table)
        }
    }
    pub(super) fn begin_write(&mut self) -> ConvertResult<()> {
        if !self.dirty {
            #[cfg(unix)]
            if !self
                .table
                .lock(casa_tables::LockType::Write, 0)
                .map_err(fail)?
            {
                return Err(ConvertError::runtime("Cannot lock table to write keywords"));
            }
            self.dirty = true;
            PENDING.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }
    pub(super) fn flush(&mut self, path: &Path) -> ConvertResult<()> {
        if !self.dirty {
            return Ok(());
        }
        #[cfg(unix)]
        let result = self.table.unlock_metadata_only_or_discard().map_err(fail);
        #[cfg(not(unix))]
        let result = self
            .table
            .save_metadata_only(TableOptions::new(path))
            .map_err(fail);
        self.dirty = false;
        PENDING.fetch_sub(1, Ordering::Release);
        crate::tools::table::component_keyword_published(path);
        result
    }
}
impl Drop for Shared {
    fn drop(&mut self) {
        let cache = self.cache.get_mut().unwrap_or_else(PoisonError::into_inner);
        // Normal release propagates errors; this protects failed construction
        // and unwinding from a full-row flush by Table's destructor.
        let _ = cache.flush(&self.path);
    }
}
