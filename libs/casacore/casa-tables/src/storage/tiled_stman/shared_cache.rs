// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct SharedTileKey {
    pub(super) table_id: u64,
    pub(super) dm_seq_nr: u32,
    pub(super) cube_idx: usize,
    pub(super) target_col_idx: usize,
    pub(super) tile_index: usize,
    /// A surviving handle can retain older metadata after a table at the
    /// same path is rewritten. A cached tile is safe to copy into bools
    /// only when it was decoded as a multi-pixel bool tile.
    pub(super) is_bool: bool,
    pub(super) single_pixel: bool,
}

#[derive(Clone)]
pub(super) struct SharedTileEntry {
    pub(super) data: Arc<[u8]>,
    pub(super) bytes: usize,
    pub(super) last_used: u64,
}

pub(super) struct SharedTileCacheState {
    pub(super) budget_bytes: usize,
    pub(super) bytes_used: usize,
    pub(super) clock: u64,
    pub(super) next_table_id: u64,
    pub(super) table_ids: std::collections::HashMap<PathBuf, u64>,
    pub(super) entries: std::collections::HashMap<SharedTileKey, SharedTileEntry>,
}

impl SharedTileCacheState {
    fn new() -> Self {
        Self {
            budget_bytes: DEFAULT_TABLE_CACHE_BYTES,
            bytes_used: 0,
            clock: 0,
            next_table_id: 1,
            table_ids: std::collections::HashMap::new(),
            entries: std::collections::HashMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    pub(super) fn get(&mut self, key: &SharedTileKey) -> Option<Arc<[u8]>> {
        let tick = self.next_tick();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.data.clone())
    }

    fn set_budget(&mut self, budget_bytes: usize) {
        self.budget_bytes = budget_bytes;
        self.evict_to_budget();
    }

    fn reset_for_tests(&mut self) {
        self.entries.clear();
        self.table_ids.clear();
        self.bytes_used = 0;
        self.clock = 0;
        self.next_table_id = 1;
        self.budget_bytes = DEFAULT_TABLE_CACHE_BYTES;
    }

    pub(super) fn table_id(&mut self, table_path: &Path) -> u64 {
        if let Some(id) = self.table_ids.get(table_path) {
            return *id;
        }
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.wrapping_add(1).max(1);
        self.table_ids.insert(table_path.to_path_buf(), id);
        id
    }

    pub(super) fn insert(&mut self, key: SharedTileKey, data: Arc<[u8]>) -> Arc<[u8]> {
        let bytes = data.len();
        if self.budget_bytes == 0 || bytes > self.budget_bytes {
            return data;
        }
        let tick = self.next_tick();
        self.bytes_used += bytes;
        self.entries.insert(
            key,
            SharedTileEntry {
                data: data.clone(),
                bytes,
                last_used: tick,
            },
        );
        self.evict_to_budget();
        data
    }

    fn evict_to_budget(&mut self) {
        while self.bytes_used > self.budget_bytes {
            let Some((evict_key, evict_bytes)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, entry)| (key.clone(), entry.bytes))
            else {
                break;
            };
            self.entries.remove(&evict_key);
            self.bytes_used = self.bytes_used.saturating_sub(evict_bytes);
        }
    }
}

pub(super) static SHARED_TILE_CACHE: LazyLock<Mutex<SharedTileCacheState>> =
    LazyLock::new(|| Mutex::new(SharedTileCacheState::new()));
pub(super) const TILE_READ_SESSION_RECENT_TILE_COUNT: usize = 8;

/// Returns the current process-wide shared table-read cache budget in bytes:
/// 64 MiB until [`set_table_cache_budget_bytes`] changes it.
pub fn table_cache_budget_bytes() -> usize {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .budget_bytes
}

/// Overrides the process-wide shared table-read cache budget in bytes.
///
/// The budget holds for the remainder of the process; excess cached data is
/// evicted immediately.
pub fn set_table_cache_budget_bytes(budget_bytes: usize) {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .set_budget(budget_bytes);
}

#[cfg(test)]
pub(crate) fn reset_table_cache_budget_for_tests() {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .reset_for_tests();
}

#[cfg(test)]
pub(crate) fn shared_tile_cache_entry_count() -> usize {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .entries
        .len()
}

#[cfg(test)]
pub(super) fn shared_tile_cache_entry_count_for_table(table_path: &Path) -> usize {
    let cache = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned");
    let Some(table_id) = cache.table_ids.get(table_path).copied() else {
        return 0;
    };
    cache
        .entries
        .keys()
        .filter(|key| key.table_id == table_id)
        .count()
}

pub(crate) fn invalidate_shared_tile_cache_for_table(table_path: &Path) {
    let mut cache = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned");
    let mut freed_bytes = 0usize;
    let Some(table_id) = cache.table_ids.remove(table_path) else {
        return;
    };
    cache.entries.retain(|key, entry| {
        let keep = key.table_id != table_id;
        if !keep {
            freed_bytes += entry.bytes;
        }
        keep
    });
    cache.bytes_used = cache.bytes_used.saturating_sub(freed_bytes);
}
