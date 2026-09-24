// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plane-sized typed residency for run-owned cube state. Scientific owners choose
//! which planes an operation needs; this layer only moves their numeric storage.

use std::{
    io,
    ops::{Deref, DerefMut, Range},
    path::Path,
    sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use casa_lattices::{LatticeElement, PagedArray, TiledShape};
use casa_tables::{TilePixel, TiledArrayStorageLayout};
use tempfile::TempDir;

type BlockId = usize;
type BackendId = usize;

trait BlockIo: Send + Sync {
    /// Return whether an existing backing was read into the live plane.
    fn prepare(&self, write: bool) -> io::Result<bool>;
    /// Return whether dirty payload was written before release.
    fn evict(&self) -> io::Result<bool>;
    fn resident(&self) -> bool;
    fn cold_bytes(&self) -> usize;
}

trait BackendIo: Send + Sync {
    fn reopen(&self) -> io::Result<()>;
    fn close(&self) -> io::Result<()>;
    fn is_open(&self) -> bool;
}

struct Entry {
    block: Arc<dyn BlockIo>,
    backend: BackendId,
    bytes: usize,
    payload_bytes: usize,
    cold_bytes: usize,
    resident: bool,
    read_pins: usize,
    write_pin: bool,
    next_use: u64,
    last_use: u64,
}

struct BackendEntry {
    backend: Arc<dyn BackendIo>,
    staging_bytes: usize,
    owner_bytes: usize,
    open: bool,
}

struct ResidencyState {
    limit: usize,
    used: usize,
    scratch: usize,
    clock: u64,
    version: u64,
    admitting: bool,
    next_block_id: BlockId,
    next_backend_id: BackendId,
    entries: Registry<Entry>,
    backends: Registry<BackendEntry>,
    metrics: ResidencyMetrics,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResidencyMetrics {
    pub(crate) peak_used_bytes: usize,
    pub(crate) dirty_write_operations: u64,
    pub(crate) dirty_write_bytes: u64,
    pub(crate) reload_read_operations: u64,
    pub(crate) reload_read_bytes: u64,
}

impl ResidencyState {
    fn fixed_bytes(&self) -> usize {
        size_of::<CubeResidency>()
            + 2 * size_of::<usize>()
            + self.entries.heap_bytes()
            + self.backends.heap_bytes()
            + self
                .backends
                .values()
                .map(|entry| entry.owner_bytes)
                .sum::<usize>()
    }
}

// Sorted numeric identities with explicit Vec capacity. Retired slots are
// removed; the run reuses the capacity for subsequent residual generations.
struct Registry<T>(Vec<(usize, T)>);

impl<T> Registry<T> {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn heap_bytes(&self) -> usize {
        self.0.capacity() * size_of::<(usize, T)>()
    }
    fn reserve(&mut self, additional: usize) {
        self.0.reserve_exact(additional);
    }
    fn iter(&self) -> impl Iterator<Item = (&usize, &T)> {
        self.0.iter().map(|(id, entry)| (id, entry))
    }
    fn values(&self) -> impl Iterator<Item = &T> {
        self.0.iter().map(|(_, entry)| entry)
    }
    fn get(&self, id: &usize) -> Option<&T> {
        self.0
            .binary_search_by_key(id, |(key, _)| *key)
            .ok()
            .map(|i| &self.0[i].1)
    }
    fn get_mut(&mut self, id: &usize) -> Option<&mut T> {
        self.0
            .binary_search_by_key(id, |(key, _)| *key)
            .ok()
            .map(|i| &mut self.0[i].1)
    }
    fn insert(&mut self, id: usize, entry: T) {
        assert!(self.0.len() < self.0.capacity());
        self.0.push((id, entry));
    }
    fn remove(&mut self, id: &usize) -> Option<T> {
        self.0
            .binary_search_by_key(id, |(key, _)| *key)
            .ok()
            .map(|i| self.0.remove(i).1)
    }
}

impl<T> std::ops::Index<&usize> for Registry<T> {
    type Output = T;
    fn index(&self, id: &usize) -> &T {
        self.get(id).expect("registered identity")
    }
}

/// The run's physical-owner coordinator. Its limit is the capacity granted by
/// the existing runtime memory authority, not an independently chosen budget.
pub(crate) struct CubeResidency {
    state: Mutex<ResidencyState>,
    wake: Condvar,
    creation: Mutex<()>,
}

impl CubeResidency {
    pub(crate) const fn fixed_owner_bytes() -> usize {
        size_of::<Self>() + 2 * size_of::<usize>()
    }

    pub(crate) fn operation_overhead(requests: usize) -> io::Result<usize> {
        operation_metadata_bytes(requests)
    }

    pub(crate) fn new(admitted_bytes: usize) -> io::Result<Arc<Self>> {
        let owner_bytes = Self::fixed_owner_bytes();
        if admitted_bytes < owner_bytes {
            return Err(invalid("resident capacity cannot hold the coordinator"));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(ResidencyState {
                limit: admitted_bytes,
                used: owner_bytes,
                scratch: 0,
                clock: 0,
                version: 0,
                admitting: false,
                next_block_id: 0,
                next_backend_id: 0,
                entries: Registry::new(),
                backends: Registry::new(),
                metrics: ResidencyMetrics {
                    peak_used_bytes: owner_bytes,
                    ..ResidencyMetrics::default()
                },
            }),
            wake: Condvar::new(),
            creation: Mutex::new(()),
        }))
    }

    /// Wait for an entire operation to fit. No subset is pinned while waiting.
    pub(crate) fn admit(
        self: &Arc<Self>,
        requests: &[BlockRequest],
        scratch_bytes: usize,
    ) -> io::Result<PinnedSet> {
        self.admit_until(requests, scratch_bytes, || false)
    }

    /// A waiting operation observes the owning run's cancellation without
    /// retaining partial pins or relying on another admission to wake it.
    pub(crate) fn admit_until(
        self: &Arc<Self>,
        requests: &[BlockRequest],
        scratch_bytes: usize,
        cancelled: impl Fn() -> bool,
    ) -> io::Result<PinnedSet> {
        loop {
            if cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cube run cancelled",
                ));
            }
            let version = self.state.lock().map_err(poison)?.version;
            match self.try_admit(requests, scratch_bytes) {
                Ok(pins) => return Ok(pins),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let mut state = self.state.lock().map_err(poison)?;
                    while state.version == version {
                        if cancelled() {
                            return Err(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "cube run cancelled",
                            ));
                        }
                        state = self
                            .wake
                            .wait_timeout(state, std::time::Duration::from_millis(50))
                            .map_err(poison)?
                            .0;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn used_bytes(&self) -> usize {
        self.state.lock().expect("residency lock poisoned").used
    }

    /// Proven live plane payload already counted by the process malloc census.
    /// Admission's `used` also includes possible coverage, codec staging and
    /// metadata, none of which is safe to credit against unrelated allocations.
    pub(crate) fn live_payload_bytes(&self) -> usize {
        self.state
            .lock()
            .expect("residency lock poisoned")
            .entries
            .values()
            .filter(|entry| entry.resident && entry.block.resident())
            .map(|entry| entry.payload_bytes)
            .sum()
    }

    pub(crate) fn limit_bytes(&self) -> usize {
        self.state.lock().expect("residency lock poisoned").limit
    }

    pub(crate) fn metrics(&self) -> ResidencyMetrics {
        self.state.lock().expect("residency lock poisoned").metrics
    }

    /// Evict optional planes before returning the corresponding cache capacity
    /// to the owning lease. The old ceiling is restored if live pins prevent
    /// reclamation; any completed evictions remain valid.
    pub(crate) fn shrink_to(self: &Arc<Self>, target: usize) -> io::Result<()> {
        let old = {
            let mut state = self.state.lock().map_err(poison)?;
            if target == 0 || target > state.limit || state.admitting {
                return Err(invalid("invalid managed cache shrink request"));
            }
            let old = state.limit;
            state.limit = target;
            old
        };
        match self.try_admit(&[], 0) {
            Ok(pins) => {
                drop(pins);
                Ok(())
            }
            Err(error) => {
                self.state.lock().map_err(poison)?.limit = old;
                Err(error)
            }
        }
    }

    pub(crate) fn set_next_use(&self, id: BlockId, phase: u64) -> io::Result<()> {
        let mut state = self.state.lock().map_err(poison)?;
        state
            .entries
            .get_mut(&id)
            .ok_or_else(|| invalid("unknown cube block"))?
            .next_use = phase;
        Ok(())
    }

    /// Explicit phase-end spill; write and flush errors remain on this call.
    pub(crate) fn evict_unpinned(&self, id: BlockId) -> io::Result<()> {
        let mut state = self.state.lock().map_err(poison)?;
        if state.admitting {
            return Err(busy("cube admission is in progress"));
        }
        let entry = state
            .entries
            .get(&id)
            .ok_or_else(|| invalid("unknown cube block"))?;
        if entry.read_pins != 0 || entry.write_pin {
            return Err(busy("cannot evict a pinned cube block"));
        }
        if !entry.resident {
            return Ok(());
        }
        let block = entry.block.clone();
        let bytes = entry.bytes;
        state.admitting = true;
        drop(state);
        let result = block.evict();
        let cold_bytes = block.cold_bytes();
        let mut state = self.state.lock().map_err(poison)?;
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        let wrote = result?;
        let entry = state.entries.get_mut(&id).expect("registered block");
        entry.resident = false;
        entry.cold_bytes = cold_bytes;
        let payload_bytes = entry.payload_bytes;
        state.used -= bytes - cold_bytes;
        if wrote {
            state.metrics.dirty_write_operations += 1;
            state.metrics.dirty_write_bytes += payload_bytes as u64;
        }
        Ok(())
    }

    /// Reserve every block and its backend staging before loading any member.
    /// A busy pinned set returns WouldBlock without acquiring a partial set.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        requests: &[BlockRequest],
        scratch_bytes: usize,
    ) -> io::Result<PinnedSet> {
        let scratch_bytes =
            checked_sum(&[scratch_bytes, operation_metadata_bytes(requests.len())?])?;
        let mut ids: Vec<_> = requests.iter().map(|request| request.id).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate block in one operation"));
        }
        let mut state = self.state.lock().map_err(poison)?;
        if state.admitting {
            return Err(busy("another cube admission is in progress"));
        }
        let mut minimum = checked_sum(&[scratch_bytes, state.fixed_bytes()])?;
        let mut requested_backends = Vec::with_capacity(requests.len());
        for request in requests {
            let entry = state
                .entries
                .get(&request.id)
                .ok_or_else(|| invalid("unknown cube block"))?;
            if entry.write_pin || request.write && entry.read_pins != 0 {
                return Err(busy("conflicting cube operation is pinned"));
            }
            minimum = minimum
                .checked_add(entry.bytes)
                .ok_or_else(|| invalid("operation byte count overflow"))?;
            if !requested_backends.contains(&entry.backend) {
                requested_backends.push(entry.backend);
            }
        }
        for backend in &requested_backends {
            minimum = minimum
                .checked_add(state.backends[backend].staging_bytes)
                .ok_or_else(|| invalid("operation byte count overflow"))?;
        }
        if minimum > state.limit {
            return Err(invalid(
                "complete pinned operation exceeds admitted capacity",
            ));
        }
        state.admitting = true;

        loop {
            let missing = ids
                .iter()
                .filter_map(|id| {
                    let entry = &state.entries[id];
                    (!entry.resident).then_some(entry.bytes - entry.cold_bytes)
                })
                .sum::<usize>();
            let staging = requested_backends
                .iter()
                .filter_map(|id| {
                    let backend = &state.backends[id];
                    (!backend.open).then_some(backend.staging_bytes)
                })
                .sum::<usize>();
            // These are subsets of the checked complete-operation minimum.
            let required = missing + staging + scratch_bytes;
            if state
                .used
                .checked_add(required)
                .is_some_and(|total| total <= state.limit)
            {
                break;
            }
            let victim = state
                .entries
                .iter()
                .filter(|(id, entry)| {
                    entry.resident && entry.read_pins == 0 && !entry.write_pin && !ids.contains(id)
                })
                .max_by_key(|(id, entry)| {
                    (
                        entry.next_use,
                        std::cmp::Reverse(entry.last_use),
                        std::cmp::Reverse(**id),
                    )
                })
                .map(|(id, entry)| (*id, entry.block.clone(), entry.bytes));
            if let Some((id, block, bytes)) = victim {
                drop(state);
                let result = block.evict();
                let cold_bytes = block.cold_bytes();
                state = self.state.lock().map_err(poison)?;
                let wrote = match result {
                    Ok(wrote) => wrote,
                    Err(error) => {
                        state.admitting = false;
                        state.version += 1;
                        self.wake.notify_all();
                        return Err(error);
                    }
                };
                let entry = state.entries.get_mut(&id).expect("registered block");
                entry.resident = false;
                entry.cold_bytes = cold_bytes;
                let payload_bytes = entry.payload_bytes;
                state.used -= bytes - cold_bytes;
                if wrote {
                    state.metrics.dirty_write_operations += 1;
                    state.metrics.dirty_write_bytes += payload_bytes as u64;
                }
                continue;
            }
            let idle = state
                .backends
                .iter()
                .find(|(id, backend)| {
                    backend.open
                        && !requested_backends.contains(id)
                        && state
                            .entries
                            .values()
                            .all(|entry| entry.backend != **id || !entry.resident)
                })
                .map(|(id, backend)| (*id, backend.backend.clone(), backend.staging_bytes));
            if let Some((id, backend, bytes)) = idle {
                drop(state);
                let result = backend.close();
                state = self.state.lock().map_err(poison)?;
                if let Err(error) = result {
                    state.admitting = false;
                    state.version += 1;
                    self.wake.notify_all();
                    return Err(error);
                }
                state
                    .backends
                    .get_mut(&id)
                    .expect("registered backend")
                    .open = false;
                state.used -= bytes;
                continue;
            }
            state.admitting = false;
            if state.scratch == 0
                && state
                    .entries
                    .values()
                    .all(|entry| entry.read_pins == 0 && !entry.write_pin)
            {
                return Err(invalid(
                    "partial-plane coverage and live owners leave insufficient capacity",
                ));
            }
            return Err(busy("other pinned operations occupy the required capacity"));
        }

        let mut to_open = Vec::with_capacity(requests.len());
        for id in &requested_backends {
            let backend = &state.backends[id];
            if !backend.open {
                to_open.push((*id, backend.backend.clone()));
            }
        }
        let mut to_load = Vec::with_capacity(requests.len());
        for request in requests {
            let entry = &state.entries[&request.id];
            if !entry.resident {
                to_load.push((request.id, entry.block.clone(), request.write));
            }
        }
        for request in requests {
            let entry = state.entries.get_mut(&request.id).expect("checked block");
            if request.write {
                entry.write_pin = true;
            } else {
                entry.read_pins += 1;
            }
        }
        for (id, _) in &to_open {
            let backend = state.backends.get_mut(id).expect("checked backend");
            backend.open = true;
            state.used += backend.staging_bytes;
        }
        for (id, _, _) in &to_load {
            let entry = state.entries.get_mut(id).expect("checked block");
            entry.resident = true;
            state.used += entry.bytes - entry.cold_bytes;
        }
        state.used += scratch_bytes;
        state.scratch += scratch_bytes;
        state.metrics.peak_used_bytes = state.metrics.peak_used_bytes.max(state.used);
        drop(state);

        let mut reloaded = Vec::new();
        let result = to_open
            .iter()
            .try_for_each(|(_, backend)| backend.reopen())
            .and_then(|_| {
                to_load.iter().try_for_each(|(id, block, write)| {
                    let read = block.prepare(*write)?;
                    if read {
                        reloaded.push(*id);
                    }
                    Ok(())
                })
            });
        let open_results: Vec<_> = to_open
            .iter()
            .map(|(id, backend)| (*id, backend.is_open()))
            .collect();
        let load_results: Vec<_> = to_load
            .iter()
            .map(|(id, block, _)| (*id, block.resident()))
            .collect();
        let mut state = self.state.lock().map_err(poison)?;
        for id in reloaded {
            state.metrics.reload_read_operations += 1;
            state.metrics.reload_read_bytes += state.entries[&id].payload_bytes as u64;
        }
        for (id, open) in open_results {
            if !open {
                let backend = state.backends.get_mut(&id).expect("registered backend");
                backend.open = false;
                state.used -= backend.staging_bytes;
            }
        }
        for (id, resident) in load_results {
            if !resident {
                let entry = state.entries.get_mut(&id).expect("registered block");
                entry.resident = false;
                state.used -= entry.bytes - entry.cold_bytes;
            }
        }
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        if let Err(error) = result {
            for request in requests {
                let entry = state
                    .entries
                    .get_mut(&request.id)
                    .expect("registered block");
                if request.write {
                    entry.write_pin = false;
                } else {
                    entry.read_pins -= 1;
                }
            }
            state.used -= scratch_bytes;
            state.scratch -= scratch_bytes;
            return Err(error);
        }
        Ok(PinnedSet {
            manager: self.clone(),
            requests: requests.to_vec(),
            scratch_bytes,
        })
    }

    /// Close idle tiled handles and release their real codec cache allocation.
    pub(crate) fn reclaim_idle_staging(&self) -> io::Result<()> {
        let mut state = self.state.lock().map_err(poison)?;
        if state.admitting {
            return Err(busy("cube admission is in progress"));
        }
        state.admitting = true;
        loop {
            let idle = state
                .backends
                .iter()
                .find(|(id, backend)| {
                    backend.open
                        && state.entries.values().all(|entry| {
                            entry.backend != **id
                                || !entry.resident && entry.read_pins == 0 && !entry.write_pin
                        })
                })
                .map(|(id, backend)| (*id, backend.backend.clone(), backend.staging_bytes));
            let Some((id, backend, bytes)) = idle else {
                break;
            };
            drop(state);
            let result = backend.close();
            state = self.state.lock().map_err(poison)?;
            if let Err(error) = result {
                state.admitting = false;
                state.version += 1;
                self.wake.notify_all();
                return Err(error);
            }
            state
                .backends
                .get_mut(&id)
                .expect("registered backend")
                .open = false;
            state.used -= bytes;
        }
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct BlockRequest {
    pub(crate) id: BlockId,
    write: bool,
}

pub(crate) struct PinnedSet {
    manager: Arc<CubeResidency>,
    requests: Vec<BlockRequest>,
    scratch_bytes: usize,
}

impl PinnedSet {
    fn permits(&self, id: BlockId, write: bool) -> bool {
        self.requests
            .iter()
            .any(|request| request.id == id && (!write || request.write))
    }
}

impl Drop for PinnedSet {
    fn drop(&mut self) {
        let mut state = self.manager.state.lock().expect("residency lock poisoned");
        state.clock += 1;
        let now = state.clock;
        for request in std::mem::take(&mut self.requests) {
            let entry = state
                .entries
                .get_mut(&request.id)
                .expect("registered block");
            if request.write {
                entry.write_pin = false;
            } else {
                entry.read_pins -= 1;
            }
            entry.last_use = now;
        }
        state.used -= self.scratch_bytes;
        state.scratch -= self.scratch_bytes;
        state.version += 1;
        self.manager.wake.notify_all();
    }
}

struct ArrayBackend<T: LatticeElement + TilePixel> {
    array: Mutex<PagedArray<T>>,
    axes: [usize; 2],
    _directory: TempDir,
}

impl<T: LatticeElement + TilePixel> BackendIo for ArrayBackend<T> {
    fn reopen(&self) -> io::Result<()> {
        self.array.lock().map_err(poison)?.reopen().map_err(other)
    }

    fn close(&self) -> io::Result<()> {
        self.array
            .lock()
            .map_err(poison)?
            .temp_close()
            .map_err(other)
    }

    fn is_open(&self) -> bool {
        !self
            .array
            .lock()
            .expect("cube backend lock poisoned")
            .is_temp_closed()
    }
}

struct PlaneState<T> {
    values: Option<Vec<T>>,
    dirty: bool,
    backed: bool,
    initialized: Coverage,
}

/// Uniform planes need one flag, not a cube-sized validity allocation. Only
/// partial writes materialize a bounded bitmap, charged to the active plane.
struct Coverage {
    words: Option<Vec<u64>>,
    cells: usize,
    valid_cells: usize,
}

impl Coverage {
    fn bytes(cells: usize) -> io::Result<usize> {
        cells
            .checked_add(63)
            .map(|n| n / 64)
            .and_then(|n| n.checked_mul(std::mem::size_of::<u64>()))
            .ok_or_else(|| invalid("plane coverage size overflow"))
    }

    fn new(cells: usize, initialized: bool) -> io::Result<Self> {
        Self::bytes(cells)?;
        Ok(Self {
            words: None,
            cells,
            valid_cells: if initialized { cells } else { 0 },
        })
    }

    fn update(&mut self, range: &Range<usize>, valid: bool) {
        if range.is_empty() {
            return;
        }
        if range.start == 0 && range.end == self.cells {
            self.words = None;
            self.valid_cells = if valid { self.cells } else { 0 };
            return;
        }
        if self.words.is_none() && (self.valid_cells == self.cells) == valid {
            return;
        }
        let words = self.words.get_or_insert_with(|| {
            vec![
                if self.valid_cells == self.cells {
                    u64::MAX
                } else {
                    0
                };
                self.cells.div_ceil(64)
            ]
        });
        let first = range.start / 64;
        let last = (range.end - 1) / 64;
        for (index, word) in words.iter_mut().enumerate().take(last + 1).skip(first) {
            let low = if index == first { range.start % 64 } else { 0 };
            let high = if index == last {
                (range.end - 1) % 64 + 1
            } else {
                64
            };
            let mask = (u64::MAX << low) & (u64::MAX >> (64 - high));
            let before = (*word & mask).count_ones() as usize;
            if valid {
                *word |= mask;
                self.valid_cells += mask.count_ones() as usize - before;
            } else {
                *word &= !mask;
                self.valid_cells -= before;
            }
        }
        if self.valid_cells == 0 || self.valid_cells == self.cells {
            self.words = None;
        }
    }

    fn covers(&self, range: &Range<usize>) -> bool {
        if range.is_empty() {
            return true;
        }
        let Some(words) = &self.words else {
            return self.valid_cells == self.cells;
        };
        let first = range.start / 64;
        let last = (range.end - 1) / 64;
        (first..=last).all(|index| {
            let low = if index == first { range.start % 64 } else { 0 };
            let high = if index == last {
                (range.end - 1) % 64 + 1
            } else {
                64
            };
            let mask = (u64::MAX << low) & (u64::MAX >> (64 - high));
            words[index] & mask == mask
        })
    }

    fn heap_bytes(&self) -> usize {
        self.words
            .as_ref()
            .map_or(0, |words| words.capacity() * size_of::<u64>())
    }
}

struct PlaneBlock<T: LatticeElement + TilePixel> {
    backend: Arc<ArrayBackend<T>>,
    index: usize,
    cells: usize,
    fill: Option<T>,
    state: RwLock<PlaneState<T>>,
}

impl<T: LatticeElement + TilePixel> BlockIo for PlaneBlock<T> {
    fn prepare(&self, write: bool) -> io::Result<bool> {
        let mut state = self.state.write().map_err(poison)?;
        if state.values.is_some() {
            return Ok(false);
        }
        if !write && !state.backed && self.fill.is_none() {
            return Err(invalid("generated plane read before initialization"));
        }
        let mut values = vec![self.fill.unwrap_or_default(); self.cells];
        let reloaded = state.backed;
        if reloaded {
            self.backend
                .array
                .lock()
                .map_err(poison)?
                .read_slice_into(
                    &[0, 0, self.index],
                    &[self.backend.axes[0], self.backend.axes[1], 1],
                    &mut values,
                )
                .map_err(other)?;
        }
        state.values = Some(values);
        Ok(reloaded)
    }

    fn evict(&self) -> io::Result<bool> {
        let mut state = self.state.write().map_err(poison)?;
        let wrote = state.dirty;
        if wrote {
            let values = state
                .values
                .as_ref()
                .ok_or_else(|| invalid("dirty plane has no resident data"))?;
            let mut array = self.backend.array.lock().map_err(poison)?;
            array
                .write_slice_from(
                    &[0, 0, self.index],
                    &[self.backend.axes[0], self.backend.axes[1], 1],
                    values,
                )
                .map_err(other)?;
            array.flush().map_err(other)?;
            state.backed = true;
            state.dirty = false;
        }
        state.values = None;
        Ok(wrote)
    }

    fn resident(&self) -> bool {
        self.state
            .read()
            .expect("cube block lock poisoned")
            .values
            .is_some()
    }

    fn cold_bytes(&self) -> usize {
        self.state
            .read()
            .expect("cube block lock poisoned")
            .initialized
            .heap_bytes()
    }
}

/// One typed array of plane-aligned blocks sharing the run coordinator.
pub(crate) struct ManagedPlaneArray<T: LatticeElement + TilePixel> {
    manager: Arc<CubeResidency>,
    blocks: Vec<(BlockId, Arc<PlaneBlock<T>>)>,
    backend_id: BackendId,
    _backend: Arc<ArrayBackend<T>>,
}

const CUBE_DIRECTORY_PREFIX: &str = ".casa-rs-managed-cube-";
const CUBE_DIRECTORY_RANDOM_BYTES: usize = 12;

pub(crate) struct ManagedArrayFootprint {
    pub(crate) owner_bytes: usize,
    pub(crate) staging_bytes: usize,
    pub(crate) block_bytes: usize,
    pub(crate) registry_bytes: usize,
    pub(crate) creation_bytes: usize,
    pub(crate) storage_bytes: usize,
}

fn planned_array<T: LatticeElement + TilePixel>(
    directory: &Path,
    axis0: usize,
    axis1: usize,
    planes: usize,
) -> io::Result<(TiledArrayStorageLayout, ManagedArrayFootprint)> {
    let cells = axis0
        .checked_mul(axis1)
        .ok_or_else(|| invalid("plane size overflow"))?;
    let bytes = cells
        .checked_mul(size_of::<T>())
        .ok_or_else(|| invalid("plane byte overflow"))?;
    if planes == 0 || bytes == 0 {
        return Err(invalid("cube planes must be positive"));
    }
    let shape = TiledShape::with_tile_shape(vec![axis0, axis1, planes], vec![axis0, axis1, 1])
        .map_err(other)?;
    let layout = PagedArray::<T>::storage_layout(shape, bytes).map_err(other)?;
    let path = directory.join("values");
    let closed_heap = checked_sum(&[6 * size_of::<usize>(), path.as_os_str().len()])?;
    let open_heap =
        PagedArray::<T>::planned_persistent_heap_bytes(&layout, &path).map_err(other)?;
    let staging_bytes = checked_sum(&[
        open_heap - closed_heap,
        layout
            .slice_scratch_bytes()
            .map_err(other)?
            .max(layout.flush_scratch_bytes().map_err(other)?),
    ])?;
    let block_bytes = checked_sum(&[bytes, Coverage::bytes(cells)?])?;
    let owner_bytes = checked_sum(&[
        size_of::<ManagedPlaneArray<T>>(),
        size_of::<ArrayBackend<T>>(),
        2 * size_of::<usize>(),
        closed_heap,
        directory.as_os_str().len(),
        checked_product(
            planes,
            checked_sum(&[
                size_of::<PlaneBlock<T>>(),
                2 * size_of::<usize>(),
                size_of::<(BlockId, Arc<PlaneBlock<T>>)>(),
            ])?,
        )?,
    ])?;
    let registry_bytes = checked_sum(&[
        checked_product(planes, size_of::<(usize, Entry)>())?,
        size_of::<(usize, BackendEntry)>(),
    ])?;
    let creation_bytes = checked_sum(&[
        owner_bytes,
        staging_bytes,
        registry_bytes,
        layout.owned_heap_bytes().map_err(other)?,
    ])?;
    let storage_bytes = layout.storage_bytes().map_err(other)?;
    Ok((
        layout,
        ManagedArrayFootprint {
            owner_bytes,
            staging_bytes,
            block_bytes,
            registry_bytes,
            creation_bytes,
            storage_bytes,
        },
    ))
}

impl<T: LatticeElement + TilePixel> ManagedPlaneArray<T> {
    pub(crate) fn footprint(
        parent: &Path,
        axis0: usize,
        axis1: usize,
        planes: usize,
    ) -> io::Result<ManagedArrayFootprint> {
        let planned_directory = parent.join(format!(
            "{}{}",
            CUBE_DIRECTORY_PREFIX,
            "x".repeat(CUBE_DIRECTORY_RANDOM_BYTES)
        ));
        planned_array::<T>(&planned_directory, axis0, axis1, planes).map(|(_, footprint)| footprint)
    }

    pub(crate) fn create(
        manager: Arc<CubeResidency>,
        parent: &Path,
        axis0: usize,
        axis1: usize,
        planes: usize,
        fill: Option<T>,
    ) -> io::Result<Self> {
        let creation_lock = manager.creation.lock().map_err(poison)?;
        let directory = tempfile::Builder::new()
            .prefix(CUBE_DIRECTORY_PREFIX)
            .rand_bytes(CUBE_DIRECTORY_RANDOM_BYTES)
            .tempdir_in(parent)?;
        let (layout, footprint) = planned_array::<T>(directory.path(), axis0, axis1, planes)?;
        let cells = axis0 * axis1;
        let path = directory.path().join("values");
        // Replacement Vec allocation can overlap its previous registry capacity.
        // Reserve that construction peak, then transfer the retained portion of
        // the same permit into the registered owners, without a release gap.
        let registry_growth = {
            let state = manager.state.lock().map_err(poison)?;
            checked_sum(&[
                checked_product(
                    state
                        .entries
                        .0
                        .len()
                        .checked_add(planes)
                        .ok_or_else(|| invalid("block count overflow"))?,
                    size_of::<(usize, Entry)>(),
                )?,
                checked_product(
                    state
                        .backends
                        .0
                        .len()
                        .checked_add(1)
                        .ok_or_else(|| invalid("backend count overflow"))?,
                    size_of::<(usize, BackendEntry)>(),
                )?,
            ])?
        };
        let creation_bytes = checked_sum(&[
            footprint.owner_bytes,
            footprint.staging_bytes,
            registry_growth,
            layout.owned_heap_bytes().map_err(other)?,
        ])?;
        let mut creation = manager.try_admit(&[], creation_bytes)?;
        let mut array = PagedArray::<T>::create_planned(&layout, &path).map_err(other)?;
        array.temp_close().map_err(other)?;
        let backend = Arc::new(ArrayBackend {
            array: Mutex::new(array),
            axes: [axis0, axis1],
            _directory: directory,
        });
        let mut blocks = Vec::with_capacity(planes);
        for index in 0..planes {
            let block = Arc::new(PlaneBlock {
                backend: backend.clone(),
                index,
                cells,
                fill,
                state: RwLock::new(PlaneState {
                    values: None,
                    dirty: false,
                    backed: false,
                    initialized: Coverage::new(cells, fill.is_some())?,
                }),
            });
            blocks.push((0, block));
        }
        let mut state = manager.state.lock().map_err(poison)?;
        let backend_id = state.next_backend_id;
        let next_backend_id = backend_id
            .checked_add(1)
            .ok_or_else(|| invalid("backend identity overflow"))?;
        let next_block_id = state
            .next_block_id
            .checked_add(planes)
            .ok_or_else(|| invalid("block identity overflow"))?;
        let old_registry = state.entries.heap_bytes() + state.backends.heap_bytes();
        state.entries.reserve(planes);
        state.backends.reserve(1);
        let retained = checked_sum(&[
            footprint.owner_bytes,
            state.entries.heap_bytes() + state.backends.heap_bytes() - old_registry,
        ])?;
        // All fallible preparation finished before registration. On an earlier
        // error, local owners and the creation permit unwind without stale IDs.
        assert!(retained <= creation.scratch_bytes);
        creation.scratch_bytes -= retained;
        state.scratch -= retained;
        state.next_backend_id = next_backend_id;
        for (id, block) in &mut blocks {
            *id = state.next_block_id;
            state.next_block_id += 1;
            state.entries.insert(
                *id,
                Entry {
                    block: block.clone(),
                    backend: backend_id,
                    bytes: footprint.block_bytes,
                    payload_bytes: cells * size_of::<T>(),
                    cold_bytes: 0,
                    resident: false,
                    read_pins: 0,
                    write_pin: false,
                    next_use: u64::MAX,
                    last_use: 0,
                },
            );
        }
        debug_assert_eq!(state.next_block_id, next_block_id);
        state.backends.insert(
            backend_id,
            BackendEntry {
                backend: backend.clone(),
                staging_bytes: footprint.staging_bytes,
                owner_bytes: footprint.owner_bytes,
                open: false,
            },
        );
        drop(state);
        drop(creation);
        drop(creation_lock);
        Ok(Self {
            manager,
            blocks,
            backend_id,
            _backend: backend,
        })
    }

    /// Release a dead scientific owner without spilling its no-longer-needed
    /// resident values. A live pin or backend close failure remains visible.
    pub(crate) fn retire_dead(self) -> io::Result<()> {
        self.retire_dead_after_release(|| {})
    }

    fn retire_dead_after_release(self, after_release: impl FnOnce()) -> io::Result<()> {
        let Self {
            manager,
            blocks,
            backend_id,
            _backend,
        } = self;
        let mut state = manager.state.lock().map_err(poison)?;
        if state.admitting
            || blocks.iter().any(|(id, _)| {
                let entry = &state.entries[id];
                entry.read_pins != 0 || entry.write_pin
            })
        {
            return Err(busy("cannot retire an active cube array"));
        }
        state.admitting = true;
        let backend = state.backends[&backend_id].backend.clone();
        let open = state.backends[&backend_id].open;
        drop(state);
        if open {
            if let Err(error) = backend.close() {
                let mut state = manager.state.lock().map_err(poison)?;
                state.admitting = false;
                state.version += 1;
                manager.wake.notify_all();
                return Err(error);
            }
        }
        let mut state = manager.state.lock().map_err(poison)?;
        let mut released = 0;
        for (id, _) in &blocks {
            let entry = state.entries.remove(id).expect("registered cube block");
            released += if entry.resident {
                entry.bytes
            } else {
                entry.cold_bytes
            };
        }
        let retired = state
            .backends
            .remove(&backend_id)
            .expect("registered cube backend");
        released += retired.owner_bytes
            + if retired.open {
                retired.staging_bytes
            } else {
                0
            };
        drop(state);
        drop(blocks);
        drop(retired);
        drop(backend);
        drop(_backend);
        after_release();
        let mut state = manager.state.lock().map_err(poison)?;
        state.used -= released;
        state.admitting = false;
        state.version += 1;
        manager.wake.notify_all();
        Ok(())
    }

    pub(crate) fn request(&self, index: usize, write: bool) -> io::Result<BlockRequest> {
        Ok(BlockRequest {
            id: self
                .blocks
                .get(index)
                .ok_or_else(|| invalid("plane index outside cube"))?
                .0,
            write,
        })
    }

    pub(crate) fn set_next_use(&self, index: usize, phase: u64) -> io::Result<()> {
        self.manager
            .set_next_use(self.request(index, false)?.id, phase)
    }

    pub(crate) fn read<'a>(
        &'a self,
        pins: &'a PinnedSet,
        index: usize,
        range: Range<usize>,
    ) -> io::Result<PlaneRead<'a, T>> {
        let (id, block) = self
            .blocks
            .get(index)
            .ok_or_else(|| invalid("plane index outside cube"))?;
        if !pins.permits(*id, false)
            || !Arc::ptr_eq(&pins.manager, &self.manager)
            || range.end > block.cells
            || range.start > range.end
        {
            return Err(invalid("read is outside the pinned plane"));
        }
        let state = block.state.read().map_err(poison)?;
        if !state.initialized.covers(&range) {
            return Err(invalid("plane range read before generation"));
        }
        Ok(PlaneRead { state, range })
    }

    pub(crate) fn write<'a>(
        &'a self,
        pins: &'a PinnedSet,
        index: usize,
        range: Range<usize>,
    ) -> io::Result<PlaneWrite<'a, T>> {
        let (id, block) = self
            .blocks
            .get(index)
            .ok_or_else(|| invalid("plane index outside cube"))?;
        if !pins.permits(*id, true)
            || !Arc::ptr_eq(&pins.manager, &self.manager)
            || range.end > block.cells
            || range.start > range.end
        {
            return Err(invalid("write is outside the pinned plane"));
        }
        let mut state = block.state.write().map_err(poison)?;
        state.dirty = true;
        state.initialized.update(&range, false);
        Ok(PlaneWrite { state, range })
    }
}

pub(crate) struct PlaneRead<'a, T> {
    state: RwLockReadGuard<'a, PlaneState<T>>,
    range: Range<usize>,
}

impl<T> Deref for PlaneRead<'_, T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.state.values.as_ref().expect("pinned plane")[self.range.clone()]
    }
}

pub(crate) struct PlaneWrite<'a, T> {
    state: RwLockWriteGuard<'a, PlaneState<T>>,
    range: Range<usize>,
}

impl<T> PlaneWrite<'_, T> {
    /// Make this window readable after the owning kernel completed its write.
    /// Cancellation simply drops the guard, leaving the range uninitialized.
    pub(crate) fn finish(mut self) {
        self.state.initialized.update(&self.range, true);
    }
}

impl<T> Deref for PlaneWrite<'_, T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.state.values.as_ref().expect("pinned plane")[self.range.clone()]
    }
}

impl<T> DerefMut for PlaneWrite<'_, T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.state.values.as_mut().expect("pinned plane")[self.range.clone()]
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn checked_sum(values: &[usize]) -> io::Result<usize> {
    values
        .iter()
        .try_fold(0usize, |sum, value| sum.checked_add(*value))
        .ok_or_else(|| invalid("cube memory size overflow"))
}

fn checked_product(count: usize, bytes: usize) -> io::Result<usize> {
    count
        .checked_mul(bytes)
        .ok_or_else(|| invalid("cube memory size overflow"))
}

fn operation_metadata_bytes(blocks: usize) -> io::Result<usize> {
    checked_product(
        blocks,
        checked_sum(&[
            size_of::<BlockRequest>(),
            2 * size_of::<usize>(),
            size_of::<(BackendId, Arc<dyn BackendIo>)>(),
            size_of::<(BlockId, Arc<dyn BlockIo>, bool)>(),
            2 * size_of::<(usize, bool)>(),
        ])?,
    )
}
fn busy(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}
fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
fn poison<T>(error: std::sync::PoisonError<T>) -> io::Error {
    other(error)
}

/// Mandatory simultaneous owners for a worker wave, in bytes. The separate
/// Cargo build-job limit is not an imaging worker limit.
pub(crate) struct WorkerMemory {
    pub(crate) budget: usize,
    pub(crate) shared: usize,
    pub(crate) backend_staging: usize,
    pub(crate) workspace_per_worker: usize,
    pub(crate) queued_result_per_worker: usize,
}

impl WorkerMemory {
    pub(crate) fn admitted_workers(
        &self,
        requested: usize,
        usable_cpus: usize,
        ready_jobs: usize,
    ) -> io::Result<usize> {
        let fixed = self
            .shared
            .checked_add(self.backend_staging)
            .ok_or_else(|| invalid("shared owner byte count overflow"))?;
        let per_worker = self
            .workspace_per_worker
            .checked_add(self.queued_result_per_worker)
            .ok_or_else(|| invalid("worker owner byte count overflow"))?;
        if requested == 0
            || usable_cpus == 0
            || ready_jobs == 0
            || per_worker == 0
            || fixed > self.budget
        {
            return Err(invalid("no valid worker admission"));
        }
        let workers = requested
            .min(usable_cpus)
            .min(ready_jobs)
            .min((self.budget - fixed) / per_worker);
        if workers == 0 {
            return Err(invalid(
                "one worker and mandatory shared owners exceed budget",
            ));
        }
        Ok(workers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_lattices::TiledFileIoStats;
    use num_complex::Complex32;

    fn fixture<T: LatticeElement + TilePixel>(
        planes: usize,
        slots: usize,
        fill: Option<T>,
    ) -> (TempDir, Arc<CubeResidency>, ManagedPlaneArray<T>) {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 24).unwrap();
        let array =
            ManagedPlaneArray::create(manager.clone(), root.path(), 2, 2, planes, fill).unwrap();
        {
            let mut state = manager.state.lock().unwrap();
            state.limit = state.fixed_bytes()
                + state.backends[&array.backend_id].staging_bytes
                + slots * state.entries[&array.blocks[0].0].bytes
                + operation_metadata_bytes(slots).unwrap();
        }
        (root, manager, array)
    }

    fn write_bytes(stats: TiledFileIoStats) -> usize {
        stats.flat_flush_write_bytes
            + stats.lru_flush_write_bytes
            + stats.lru_batch_flush_bytes
            + stats.direct_tile_write_bytes
    }

    #[test]
    fn real_typed_spill_reload_and_clean_eviction_reconcile_physical_owners() {
        fn exercise<T: LatticeElement + TilePixel + PartialEq + std::fmt::Debug>(values: [T; 4]) {
            let (_root, manager, array) = fixture(3, 1, Some(T::default()));
            let pins = manager
                .admit(&[array.request(0, true).unwrap()], 0)
                .unwrap();
            let mut view = array.write(&pins, 0, 0..4).unwrap();
            view.copy_from_slice(&values);
            view.finish();
            assert_eq!(
                array.blocks[0]
                    .1
                    .state
                    .read()
                    .unwrap()
                    .initialized
                    .heap_bytes(),
                0
            );
            drop(pins);
            drop(
                manager
                    .admit(&[array.request(1, false).unwrap()], 0)
                    .unwrap(),
            );
            assert!(!array.blocks[0].1.resident());
            let written = write_bytes(array._backend.array.lock().unwrap().io_stats());
            assert!(written > 0);
            drop(
                manager
                    .admit(&[array.request(2, false).unwrap()], 0)
                    .unwrap(),
            );
            assert_eq!(
                write_bytes(array._backend.array.lock().unwrap().io_stats()),
                written
            );
            let pins = manager
                .admit(&[array.request(0, false).unwrap()], 0)
                .unwrap();
            assert_eq!(&*array.read(&pins, 0, 0..4).unwrap(), &values);
            {
                let state = manager.state.lock().unwrap();
                let owned = array
                    ._backend
                    .array
                    .lock()
                    .unwrap()
                    .owned_persistent_heap_bytes()
                    .unwrap();
                let backend = &state.backends[&array.backend_id];
                assert!(owned <= backend.owner_bytes + backend.staging_bytes);
                assert!(state.used <= state.limit);
                let block = array.blocks[0].1.state.read().unwrap();
                let actual = block.values.as_ref().unwrap().capacity() * size_of::<T>()
                    + block.initialized.heap_bytes();
                assert!(actual <= state.entries[&array.blocks[0].0].bytes);
            }
            drop(pins);
            manager.evict_unpinned(array.blocks[0].0).unwrap();
            let before = array
                ._backend
                .array
                .lock()
                .unwrap()
                .owned_persistent_heap_bytes()
                .unwrap();
            manager.reclaim_idle_staging().unwrap();
            let after = array
                ._backend
                .array
                .lock()
                .unwrap()
                .owned_persistent_heap_bytes()
                .unwrap();
            assert!(after < before);
            assert!(array._backend.array.lock().unwrap().is_temp_closed());
            assert_eq!(
                manager.used_bytes(),
                manager.state.lock().unwrap().fixed_bytes()
            );
        }
        exercise([1.0_f32, 2.0, 3.0, 4.0]);
        exercise([1.0_f64, 2.0, 3.0, 4.0]);
        exercise([Complex32::new(2.0, -1.0); 4]);
        exercise([false, true, false, true]);
    }

    #[test]
    fn shrinking_cache_spills_before_returning_capacity_and_reloads() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let array =
            ManagedPlaneArray::create(manager.clone(), root.path(), 64, 64, 3, Some(0.0_f32))
                .unwrap();
        for plane in 0..2 {
            let pins = manager
                .admit(&[array.request(plane, true).unwrap()], 0)
                .unwrap();
            let mut values = array.write(&pins, plane, 0..4096).unwrap();
            values.fill((plane + 1) as f32);
            values.finish();
        }
        let one_block = manager.state.lock().unwrap().entries[&array.blocks[0].0].bytes;
        let target = manager.used_bytes() - one_block / 2;
        manager.shrink_to(target).unwrap();
        assert_eq!(manager.limit_bytes(), target);
        assert!(manager.used_bytes() <= manager.limit_bytes());
        assert!(manager.metrics().dirty_write_operations >= 1);
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(&*array.read(&pins, 0, 0..4096).unwrap(), &[1.0; 4096]);
        assert!(manager.metrics().reload_read_operations >= 1);
        assert!(manager.metrics().reload_read_bytes >= 4096 * size_of::<f32>() as u64);
    }

    #[test]
    fn cancelled_ranges_stay_invalid_after_spill_and_reload() {
        for fill in [Some(0.0_f32), None] {
            let (_root, manager, array) = fixture(2, 1, fill);
            // One unfinished plane's bitmap may remain while another is active.
            manager.state.lock().unwrap().limit += Coverage::bytes(4).unwrap();
            let pins = manager
                .admit(&[array.request(0, true).unwrap()], 0)
                .unwrap();
            let mut first = array.write(&pins, 0, 0..2).unwrap();
            first.copy_from_slice(&[1.0, 2.0]);
            first.finish();
            let mut cancelled = array.write(&pins, 0, 2..4).unwrap();
            cancelled[0] = 9.0;
            drop(cancelled);
            assert!(array.read(&pins, 0, 2..4).is_err());
            drop(pins);
            drop(
                manager
                    .admit(&[array.request(1, true).unwrap()], 0)
                    .unwrap(),
            );
            assert_eq!(
                manager.state.lock().unwrap().entries[&array.blocks[0].0].cold_bytes,
                8
            );
            let pins = manager
                .admit(&[array.request(0, false).unwrap()], 0)
                .unwrap();
            assert_eq!(&*array.read(&pins, 0, 0..2).unwrap(), &[1.0, 2.0]);
            assert!(array.read(&pins, 0, 2..4).is_err());
            drop(pins);
            let pins = manager
                .admit(&[array.request(0, true).unwrap()], 0)
                .unwrap();
            drop(array.write(&pins, 0, 0..4).unwrap());
            drop(pins);
            drop(
                manager
                    .admit(&[array.request(1, true).unwrap()], 0)
                    .unwrap(),
            );
            let pins = manager
                .admit(&[array.request(0, false).unwrap()], 0)
                .unwrap();
            assert!(array.read(&pins, 0, 0..4).is_err());
        }
    }

    #[test]
    fn heterogeneous_arrays_share_one_complete_operation_budget() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let real =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 3, Some(0.0))
                .unwrap();
        let complex = ManagedPlaneArray::<Complex32>::create(
            manager.clone(),
            root.path(),
            2,
            2,
            2,
            Some(Complex32::default()),
        )
        .unwrap();
        {
            let mut state = manager.state.lock().unwrap();
            state.limit = state.fixed_bytes()
                + state
                    .backends
                    .values()
                    .map(|backend| backend.staging_bytes)
                    .sum::<usize>()
                + state.entries[&real.blocks[0].0].bytes
                + state.entries[&complex.blocks[0].0].bytes
                + operation_metadata_bytes(2).unwrap();
        }
        let pins = manager
            .admit(
                &[
                    real.request(0, true).unwrap(),
                    complex.request(0, true).unwrap(),
                ],
                0,
            )
            .unwrap();
        assert_eq!(manager.used_bytes(), manager.state.lock().unwrap().limit);
        let mut real_view = real.write(&pins, 0, 0..4).unwrap();
        let mut complex_view = complex.write(&pins, 0, 0..4).unwrap();
        real_view.fill(7.0);
        complex_view.fill(Complex32::new(2.0, -3.0));
        real_view.finish();
        complex_view.finish();
        assert_eq!(
            manager
                .try_admit(&[real.request(1, false).unwrap()], 0)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(pins);
        manager.evict_unpinned(real.blocks[0].0).unwrap();
        manager.evict_unpinned(complex.blocks[0].0).unwrap();
        manager.reclaim_idle_staging().unwrap();
        let pins = manager
            .admit(
                &[
                    real.request(0, false).unwrap(),
                    complex.request(0, false).unwrap(),
                ],
                0,
            )
            .unwrap();
        assert_eq!(real.read(&pins, 0, 0..4).unwrap()[0], 7.0);
        assert_eq!(
            complex.read(&pins, 0, 0..4).unwrap()[0],
            Complex32::new(2.0, -3.0)
        );
    }

    #[test]
    fn complete_access_sets_exclude_reverse_writes_and_allow_shared_reads() {
        let (_root, manager, array) = fixture(2, 2, Some(0.0_f32));
        let first = manager
            .admit(
                &[
                    array.request(0, true).unwrap(),
                    array.request(1, true).unwrap(),
                ],
                0,
            )
            .unwrap();
        assert_eq!(
            manager
                .try_admit(
                    &[
                        array.request(1, true).unwrap(),
                        array.request(0, true).unwrap()
                    ],
                    0
                )
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(
            manager
                .try_admit(&[array.request(0, false).unwrap()], 0)
                .is_err()
        );
        drop(first);
        let second = manager
            .admit(
                &[
                    array.request(1, true).unwrap(),
                    array.request(0, true).unwrap(),
                ],
                0,
            )
            .unwrap();
        drop(second);
        let first_read = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        let second_read = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        let view = array.read(&first_read, 0, 0..4).unwrap();
        assert!(array.blocks[0].1.state.try_write().is_err());
        assert!(array.write(&second_read, 0, 0..4).is_err());
        drop(view);
        drop(first_read);
        drop(second_read);
    }

    #[test]
    fn waiting_admission_observes_cancellation_and_later_progresses() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_root, manager, array) = fixture(2, 1, Some(0.0_f32));
        let first = manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let cancelled = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let (ready, started) = std::sync::mpsc::channel();
            let manager = &manager;
            let array = &array;
            let cancelled = &cancelled;
            let worker = scope.spawn(move || {
                manager
                    .admit_until(&[array.request(1, false).unwrap()], 0, || {
                        let _ = ready.send(());
                        cancelled.load(Ordering::Relaxed)
                    })
                    .err()
                    .unwrap()
                    .kind()
            });
            started
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            cancelled.store(true, Ordering::Relaxed);
            assert_eq!(worker.join().unwrap(), io::ErrorKind::Interrupted);
        });
        drop(first);
        let pins = manager
            .admit(&[array.request(1, false).unwrap()], 0)
            .unwrap();
        drop(pins);
        let pins = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                manager
                    .admit(&[array.request(1, false).unwrap()], 0)
                    .unwrap()
            });
            drop(pins);
            drop(worker.join().unwrap());
        });
    }

    #[test]
    fn retirement_releases_real_owners_before_credit_and_reuses_registry_capacity() {
        let (root, manager, array) = fixture(1, 1, Some(0.0_f32));
        drop(
            manager
                .admit(&[array.request(0, true).unwrap()], 0)
                .unwrap(),
        );
        let before = manager.used_bytes();
        let weak_block = Arc::downgrade(&array.blocks[0].1);
        let weak_backend = Arc::downgrade(&array._backend);
        array
            .retire_dead_after_release(|| {
                assert!(weak_block.upgrade().is_none());
                assert!(weak_backend.upgrade().is_none());
                assert_eq!(manager.used_bytes(), before);
                assert!(manager.state.lock().unwrap().admitting);
            })
            .unwrap();
        let retained_registry = manager.used_bytes();
        assert_eq!(
            retained_registry,
            manager.state.lock().unwrap().fixed_bytes()
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        manager.state.lock().unwrap().limit = 1 << 24;
        let replacement =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 1, None).unwrap();
        replacement.retire_dead().unwrap();
        assert_eq!(manager.used_bytes(), retained_registry);
    }

    #[test]
    fn phase_hint_precedes_lru() {
        let (_root, manager, array) = fixture(3, 2, Some(0.0_f32));
        drop(
            manager
                .admit(
                    &[
                        array.request(0, false).unwrap(),
                        array.request(1, false).unwrap(),
                    ],
                    0,
                )
                .unwrap(),
        );
        array.set_next_use(0, 1).unwrap();
        array.set_next_use(1, 100).unwrap();
        // Keep operation metadata identical to the initial two-plane request.
        let scratch = operation_metadata_bytes(2).unwrap() - operation_metadata_bytes(1).unwrap();
        drop(
            manager
                .admit(&[array.request(2, false).unwrap()], scratch)
                .unwrap(),
        );
        assert!(array.blocks[0].1.resident());
        assert!(!array.blocks[1].1.resident());
        assert!(array.blocks[2].1.resident());
    }

    #[test]
    fn backing_failure_and_impossible_construction_leave_no_stranded_permit() {
        let (_root, manager, array) = fixture(1, 1, None::<f32>);
        assert!(
            manager
                .try_admit(&[array.request(0, false).unwrap()], 0)
                .is_err()
        );
        let pins = manager
            .admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let mut view = array.write(&pins, 0, 0..4).unwrap();
        view.fill(3.0);
        view.finish();
        drop(pins);
        manager.evict_unpinned(array.blocks[0].0).unwrap();
        manager.reclaim_idle_staging().unwrap();
        std::fs::remove_file(
            array
                ._backend
                ._directory
                .path()
                .join("values/table.f0_TSM0"),
        )
        .unwrap();
        assert!(
            manager
                .try_admit(&[array.request(0, false).unwrap()], 0)
                .is_err()
        );
        assert!(!manager.state.lock().unwrap().admitting);
        manager.reclaim_idle_staging().unwrap();
        assert_eq!(
            manager.used_bytes(),
            manager.state.lock().unwrap().fixed_bytes()
        );
        let root = tempfile::tempdir().unwrap();
        let minimum = size_of::<CubeResidency>() + 2 * size_of::<usize>();
        let tiny = CubeResidency::new(minimum).unwrap();
        assert!(
            ManagedPlaneArray::<f32>::create(tiny.clone(), root.path(), 2, 2, 1, None).is_err()
        );
        assert_eq!(tiny.used_bytes(), minimum);
        assert!(tiny.state.lock().unwrap().entries.0.is_empty());
        assert!(tiny.state.lock().unwrap().backends.0.is_empty());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn codec_scratch_and_exact_operation_bound_are_admitted() {
        for planes in [1, 3] {
            let (_root, manager, array) = fixture(planes, 1, Some(false));
            let layout = PagedArray::<bool>::storage_layout(
                TiledShape::with_tile_shape(vec![2, 2, planes], vec![2, 2, 1]).unwrap(),
                4,
            )
            .unwrap();
            let state = manager.state.lock().unwrap();
            let stage = state.backends[&array.backend_id].staging_bytes;
            assert!(stage > layout.cache_payload_bytes().unwrap());
            assert!(
                stage
                    >= layout.cache_payload_bytes().unwrap()
                        + layout.flush_scratch_bytes().unwrap()
            );
            drop(state);
            manager.state.lock().unwrap().limit -= 1;
            assert_eq!(
                manager
                    .try_admit(&[array.request(0, false).unwrap()], 0)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            manager.state.lock().unwrap().limit += 1;
            let pins = manager
                .admit(&[array.request(0, false).unwrap()], 0)
                .unwrap();
            assert_eq!(manager.used_bytes(), manager.state.lock().unwrap().limit);
            drop(pins);
        }
    }

    #[test]
    fn uniform_large_cube_coverage_has_no_per_pixel_resident_bitmap() {
        let maps: Vec<_> = (0..2048)
            .map(|_| Coverage::new(2048 * 2048, true).unwrap())
            .collect();
        assert_eq!(maps.iter().map(Coverage::heap_bytes).sum::<usize>(), 0);
        let mut map = Coverage::new(130, false).unwrap();
        map.update(&(2..129), true);
        assert!(map.covers(&(2..129)));
        assert!(!map.covers(&(1..130)));
        assert_eq!(map.heap_bytes(), Coverage::bytes(130).unwrap());
        map.update(&(0..2), true);
        map.update(&(129..130), true);
        assert!(map.covers(&(0..130)));
        assert_eq!(map.heap_bytes(), 0);
        map.update(&(0..130), false);
        assert_eq!(map.heap_bytes(), 0);
        assert!(!map.covers(&(0..130)));
        assert!(Coverage::bytes(usize::MAX).is_err());
    }

    #[test]
    fn scratch_only_workspaces_wait_without_partial_pins() {
        let manager = CubeResidency::new(4096).unwrap();
        let available = {
            let state = manager.state.lock().unwrap();
            state.limit - state.used
        };
        let first = manager.admit(&[], available).unwrap();
        assert_eq!(
            manager.try_admit(&[], 1).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        let next = manager.admit(&[], available).unwrap();
        drop(next);
        assert_eq!(manager.state.lock().unwrap().scratch, 0);
    }

    #[test]
    fn worker_admission_scales_beyond_four_and_checks_boundaries() {
        let mut memory = WorkerMemory {
            budget: 1000,
            shared: 100,
            backend_staging: 100,
            workspace_per_worker: 40,
            queued_result_per_worker: 10,
        };
        for workers in [1, 2, 4, 8, 16] {
            assert_eq!(memory.admitted_workers(workers, 32, 100).unwrap(), workers);
        }
        assert_eq!(memory.admitted_workers(16, 32, 3).unwrap(), 3);
        memory.budget = 450;
        assert_eq!(memory.admitted_workers(16, 32, 100).unwrap(), 5);
        memory.budget = 249;
        assert!(memory.admitted_workers(1, 1, 1).is_err());
        memory.budget = 250;
        assert_eq!(memory.admitted_workers(1, 1, 1).unwrap(), 1);
        memory.shared = usize::MAX;
        assert!(memory.admitted_workers(16, 16, 16).is_err());
    }
}
