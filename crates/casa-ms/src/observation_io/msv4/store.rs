// SPDX-License-Identifier: LGPL-3.0-or-later
//! A Zarr store whose writes replace each object whole: the new bytes go
//! to a temporary file beside the object, are synchronized, and are renamed
//! over it. A reader sees the old object or the new one, never a mixture;
//! a write that fails, or a process killed during it, leaves the old object
//! (or its absence, which reads as the fill value) and at most a stray
//! temporary file, which no Zarr key names.
//!
//! The rename is durable once its directory is synchronized: [`sync`]
//! synchronizes every directory a replacement or erasure changed, and the
//! updater calls it when it finishes. Stray temporary files are not swept.
//!
//! [`sync`]: ReplacingStore::sync
//!
//! zarrs' own `FilesystemStore` writes a key's file in place; this store
//! delegates reads to it and replaces writes. It counts chunk reads and
//! writes, so the updater's whole-unit law can be checked at the store.

use std::collections::HashSet;
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use zarrs::filesystem::FilesystemStore;
use zarrs::storage::byte_range::ByteRangeIterator;
use zarrs::storage::{
    Bytes, MaybeBytesIterator, OffsetBytesIterator, ReadableStorageTraits, StorageError, StoreKey,
    StorePrefix, WritableStorageTraits,
};

/// Where an injected failure strikes a replacement (tests only).
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::observation_io) enum FailurePoint {
    /// Before any byte is written.
    BeforeWrite,
    /// After the temporary file is written and synchronized, before the
    /// rename: as a process killed at that moment leaves it.
    BeforeRename,
}

/// What the store did.
#[derive(Debug, Default)]
pub(super) struct StoreCounters {
    /// Chunk objects read (keys under a `c/` chunk directory).
    pub(super) chunk_reads: AtomicUsize,
    /// Objects replaced.
    pub(super) replaced: AtomicUsize,
    /// Objects erased (a chunk equal to the fill value is not stored).
    pub(super) erased: AtomicUsize,
}

/// See the module documentation.
#[derive(Debug)]
pub(in crate::observation_io) struct ReplacingStore {
    inner: FilesystemStore,
    root: PathBuf,
    /// Names this store's temporary files apart from another process's
    /// with the same id.
    token: u32,
    next: AtomicUsize,
    /// Directories changed since the last [`Self::sync`].
    touched: Mutex<HashSet<PathBuf>>,
    pub(super) counters: Arc<StoreCounters>,
    #[cfg(test)]
    failure: std::sync::Mutex<Option<(usize, FailurePoint)>>,
}

impl ReplacingStore {
    /// A store over the processing set at `root`.
    pub(super) fn new(root: &Path) -> Result<Self, StorageError> {
        Ok(Self {
            inner: FilesystemStore::new(root)
                .map_err(|error| StorageError::Other(error.to_string()))?,
            root: root.to_path_buf(),
            token: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.subsec_nanos()),
            next: AtomicUsize::new(0),
            touched: Mutex::new(HashSet::new()),
            counters: Arc::new(StoreCounters::default()),
            #[cfg(test)]
            failure: std::sync::Mutex::new(None),
        })
    }

    /// Synchronize every directory a replacement or erasure changed since
    /// the last call, so the changes survive a power loss.
    pub(super) fn sync(&self) -> Result<(), StorageError> {
        let touched: Vec<PathBuf> = self
            .touched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .collect();
        for directory in touched {
            File::open(&directory)
                .and_then(|handle| handle.sync_all())
                .map_err(io)?;
        }
        Ok(())
    }

    /// Record that `directory`'s entries changed; `created` adds the
    /// ancestors whose entries a new directory changed.
    fn touch(&self, directory: &Path, created: bool) {
        let mut touched = self.touched.lock().unwrap_or_else(PoisonError::into_inner);
        touched.insert(directory.to_path_buf());
        if created {
            for ancestor in directory.ancestors().skip(1) {
                touched.insert(ancestor.to_path_buf());
                if ancestor == self.root {
                    break;
                }
            }
        }
    }

    /// Fail the replacement that follows `replacements` completed ones, at
    /// `point`.
    #[cfg(test)]
    pub(in crate::observation_io) fn fail_after(&self, replacements: usize, point: FailurePoint) {
        *self.failure.lock().expect("failure hook") = Some((replacements, point));
    }

    /// Whether this replacement is the injected failure at `point`: the
    /// hook's, once its count of replacements has completed.
    #[cfg(test)]
    fn injected(&self, point: FailurePoint) -> bool {
        let mut failure = self.failure.lock().expect("failure hook");
        let due = *failure == Some((self.counters.replaced.load(Ordering::Relaxed), point));
        if due {
            *failure = None;
        }
        due
    }
}

fn io(error: std::io::Error) -> StorageError {
    StorageError::IOError(Arc::new(error))
}

#[cfg(test)]
fn injected_failure() -> StorageError {
    io(std::io::Error::other("injected replacement failure"))
}

impl ReadableStorageTraits for ReplacingStore {
    fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<MaybeBytesIterator<'a>, StorageError> {
        if key.as_str().contains("/c/") {
            self.counters.chunk_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_partial_many(key, byte_ranges)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.inner.size_key(key)
    }

    fn supports_get_partial(&self) -> bool {
        self.inner.supports_get_partial()
    }
}

impl WritableStorageTraits for ReplacingStore {
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        #[cfg(test)]
        if self.injected(FailurePoint::BeforeWrite) {
            return Err(injected_failure());
        }
        let path = self.inner.key_to_fspath(key);
        let directory = path.parent().unwrap_or(&self.root);
        let created = !directory.is_dir();
        std::fs::create_dir_all(directory).map_err(io)?;
        let name = path
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
        let temporary = directory.join(format!(
            ".{name}.replacing-{}-{:08x}-{}",
            std::process::id(),
            self.token,
            self.next.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = File::create_new(&temporary).map_err(io)?;
        file.write_all(&value)
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                let _ = std::fs::remove_file(&temporary);
                io(error)
            })?;
        drop(file);
        #[cfg(test)]
        if self.injected(FailurePoint::BeforeRename) {
            // As a killed process leaves it: the temporary file stays.
            return Err(injected_failure());
        }
        std::fs::rename(&temporary, &path).map_err(|error| {
            let _ = std::fs::remove_file(&temporary);
            io(error)
        })?;
        self.touch(directory, created);
        self.counters.replaced.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn set_partial_many(
        &self,
        _key: &StoreKey,
        _offset_values: OffsetBytesIterator,
    ) -> Result<(), StorageError> {
        Err(StorageError::Unsupported(
            "partial writes: this store replaces objects whole".to_string(),
        ))
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        self.counters.erased.fetch_add(1, Ordering::Relaxed);
        self.inner.erase(key)?;
        let path = self.inner.key_to_fspath(key);
        self.touch(path.parent().unwrap_or(&self.root), false);
        Ok(())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        self.inner.erase_prefix(prefix)
    }

    fn supports_set_partial(&self) -> bool {
        false
    }
}
