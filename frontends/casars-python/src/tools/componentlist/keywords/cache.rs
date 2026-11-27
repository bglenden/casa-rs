// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retain the attached keyword table, as CASA's ComponentList retains its Table.
//! Dirty metadata keeps its write lock until release or another in-process
//! tool needs the persisted table. Clean reads keep CASA’s shared read lock;
//! explicit clearlocks relinquishes it, and reacquisition checks the published
//! casacore modification counter. In-process writers notify the shared epoch.
use crate::convert::{ConvertError, ConvertResult};
#[cfg(not(unix))]
use casa_tables::TableOptions;
use casa_tables::{Table, TableError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

static TABLES: Mutex<Vec<Weak<Shared>>> = Mutex::new(Vec::new());
static PENDING: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct Shared {
    pub(super) path: PathBuf,
    pub(super) cache: Mutex<Cache>,
    epoch: Arc<AtomicU64>,
}
pub(super) struct Cache {
    pub(super) table: Table,
    pub(super) dirty: bool,
    epoch: Arc<AtomicU64>,
    seen_epoch: u64,
}
fn fail(error: TableError) -> ConvertError {
    ConvertError::runtime(error.to_string())
}

pub(crate) fn attach(path: &Path) -> ConvertResult<Arc<Shared>> {
    let epoch = crate::tools::table::component_keyword_epoch(path);
    let seen_epoch = epoch.load(Ordering::Acquire);
    let table = super::open(path)?;
    let shared = Arc::new(Shared {
        path: path.to_path_buf(),
        epoch: Arc::clone(&epoch),
        cache: Mutex::new(Cache {
            table,
            dirty: false,
            epoch,
            seen_epoch,
        }),
    });
    let mut tables = TABLES.lock().unwrap_or_else(PoisonError::into_inner);
    tables.retain(|table| table.strong_count() > 0);
    tables.push(Arc::downgrade(&shared));
    Ok(shared)
}

/// Table tools read from disk. Publish pending component-list keywords before
/// they take their own locks, without holding the registry while doing I/O.
pub(crate) fn flush_pending(path: &Path) -> ConvertResult<()> {
    if PENDING.load(Ordering::Acquire) == 0 {
        return Ok(());
    }
    let epoch = crate::tools::table::component_keyword_epoch(path);
    flush_matching(&epoch, None)
}
pub(super) fn flush_except(own: &Arc<Shared>) -> ConvertResult<()> {
    if PENDING.load(Ordering::Acquire) == 0 {
        return Ok(());
    }
    flush_matching(&own.epoch, Some(own))
}
fn flush_matching(epoch: &Arc<AtomicU64>, own: Option<&Arc<Shared>>) -> ConvertResult<()> {
    let tables = TABLES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|table| {
            Arc::ptr_eq(epoch, &table.epoch) && !own.is_some_and(|own| Arc::ptr_eq(own, table))
        })
        .collect::<Vec<_>>();
    for table in tables {
        let mut cache = table.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.dirty {
            cache.flush(&table.path)?;
        }
    }
    Ok(())
}
/// CASA clearlocks gives up automatic component-list locks as well.
pub(crate) fn clear_locks() -> ConvertResult<()> {
    let tables = TABLES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    for table in tables {
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
            // CASA retains this read lock between keyword lookups. After
            // explicit relinquishment, lock() uses the published generation.
            if !self.table.has_lock(casa_tables::LockType::Read)
                && !self
                    .table
                    .lock(casa_tables::LockType::Read, 0)
                    .map_err(fail)?
            {
                return Err(ConvertError::runtime("Cannot lock table to read keywords"));
            }
            let epoch = self.epoch.load(Ordering::Acquire);
            if epoch != self.seen_epoch {
                // A different handle in this process can publish under its
                // write lock while this handle retains its shared read lock.
                self.table.resync().map_err(fail)?;
                self.seen_epoch = epoch;
            }
            work(&self.table)
        }
        #[cfg(not(unix))]
        {
            self.table = Table::open(TableOptions::new(_path)).map_err(fail)?;
            work(&self.table)
        }
    }
    pub(super) fn begin_write(&mut self) -> ConvertResult<()> {
        if !self.dirty {
            // Do not retain a clean read lock while waiting to upgrade: two
            // independent writers would otherwise each wait on the other's
            // read lock. lock(Write) reloads the published generation after
            // acquisition, so the gap cannot overwrite a concurrent update.
            #[cfg(unix)]
            self.table.unlock_metadata_only().map_err(fail)?;
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
            #[cfg(unix)]
            self.table.unlock_metadata_only().map_err(fail)?;
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
        crate::tools::table::notify_keyword_change(path);
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
