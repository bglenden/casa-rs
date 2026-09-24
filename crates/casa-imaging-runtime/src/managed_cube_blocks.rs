// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plane-sized typed residency for run-owned cube state. Scientific owners choose
//! which planes an operation needs; this layer only moves their numeric storage.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    ops::{Deref, DerefMut, Range},
    path::Path,
    sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use casa_lattices::{LatticeElement, PagedArray, TiledShape};
use casa_tables::TilePixel;
use tempfile::TempDir;

type BlockId = usize;
type BackendId = usize;

trait BlockIo: Send + Sync {
    fn prepare(&self, write: bool) -> io::Result<()>;
    fn evict(&self) -> io::Result<()>;
    fn resident(&self) -> bool;
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
    resident: bool,
    pins: usize,
    next_use: u64,
    last_use: u64,
}

struct BackendEntry {
    backend: Arc<dyn BackendIo>,
    staging_bytes: usize,
    open: bool,
}

struct ResidencyState {
    limit: usize,
    used: usize,
    clock: u64,
    version: u64,
    admitting: bool,
    entries: BTreeMap<BlockId, Entry>,
    backends: BTreeMap<BackendId, BackendEntry>,
}

/// The run's physical-owner coordinator. Its limit is the capacity granted by
/// the existing runtime memory authority, not an independently chosen budget.
pub(crate) struct CubeResidency {
    state: Mutex<ResidencyState>,
    wake: Condvar,
}

impl CubeResidency {
    pub(crate) fn new(admitted_bytes: usize) -> io::Result<Arc<Self>> {
        if admitted_bytes == 0 {
            return Err(invalid("resident capacity must be positive"));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(ResidencyState {
                limit: admitted_bytes,
                used: 0,
                clock: 0,
                version: 0,
                admitting: false,
                entries: BTreeMap::new(),
                backends: BTreeMap::new(),
            }),
            wake: Condvar::new(),
        }))
    }

    /// Wait for an entire operation to fit. No subset is pinned while waiting.
    pub(crate) fn admit(
        self: &Arc<Self>,
        requests: &[BlockRequest],
        scratch_bytes: usize,
    ) -> io::Result<PinnedSet> {
        loop {
            let version = self.state.lock().map_err(poison)?.version;
            match self.try_admit(requests, scratch_bytes) {
                Ok(pins) => return Ok(pins),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let mut state = self.state.lock().map_err(poison)?;
                    while state.version == version {
                        state = self.wake.wait(state).map_err(poison)?;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn register_backend(
        &self,
        backend: Arc<dyn BackendIo>,
        staging_bytes: usize,
    ) -> io::Result<BackendId> {
        let mut state = self.state.lock().map_err(poison)?;
        if staging_bytes > state.limit {
            return Err(invalid("one codec tile exceeds admitted resident capacity"));
        }
        let id = state.backends.len();
        state.backends.insert(
            id,
            BackendEntry {
                backend,
                staging_bytes,
                open: false,
            },
        );
        Ok(id)
    }

    fn register_block(
        &self,
        block: Arc<dyn BlockIo>,
        backend: BackendId,
        bytes: usize,
    ) -> io::Result<BlockId> {
        let mut state = self.state.lock().map_err(poison)?;
        if bytes == 0
            || bytes
                .checked_add(state.backends[&backend].staging_bytes)
                .is_none_or(|n| n > state.limit)
        {
            return Err(invalid(
                "one pinned plane and its codec tile exceed admitted capacity",
            ));
        }
        let id = state.entries.len();
        state.entries.insert(
            id,
            Entry {
                block,
                backend,
                bytes,
                resident: false,
                pins: 0,
                next_use: u64::MAX,
                last_use: 0,
            },
        );
        Ok(id)
    }

    pub(crate) fn used_bytes(&self) -> usize {
        self.state.lock().expect("residency lock poisoned").used
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
        if entry.pins != 0 {
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
        let mut state = self.state.lock().map_err(poison)?;
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        result?;
        state
            .entries
            .get_mut(&id)
            .expect("registered block")
            .resident = false;
        state.used -= bytes;
        Ok(())
    }

    /// Reserve every block and its backend staging before loading any member.
    /// A busy pinned set returns WouldBlock without acquiring a partial set.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        requests: &[BlockRequest],
        scratch_bytes: usize,
    ) -> io::Result<PinnedSet> {
        let ids: BTreeSet<_> = requests.iter().map(|request| request.id).collect();
        if ids.len() != requests.len() {
            return Err(invalid("duplicate block in one operation"));
        }
        let mut state = self.state.lock().map_err(poison)?;
        if state.admitting {
            return Err(busy("another cube admission is in progress"));
        }
        let mut minimum = scratch_bytes;
        let mut requested_backends = BTreeSet::new();
        for request in requests {
            let entry = state
                .entries
                .get(&request.id)
                .ok_or_else(|| invalid("unknown cube block"))?;
            minimum = minimum
                .checked_add(entry.bytes)
                .ok_or_else(|| invalid("operation byte count overflow"))?;
            requested_backends.insert(entry.backend);
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
                    (!entry.resident).then_some(entry.bytes)
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
                .filter(|(id, entry)| entry.resident && entry.pins == 0 && !ids.contains(id))
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
                state = self.state.lock().map_err(poison)?;
                if let Err(error) = result {
                    state.admitting = false;
                    state.version += 1;
                    self.wake.notify_all();
                    return Err(error);
                }
                state
                    .entries
                    .get_mut(&id)
                    .expect("registered block")
                    .resident = false;
                state.used -= bytes;
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
            return Err(busy("other pinned operations occupy the required capacity"));
        }

        let to_open: Vec<_> = requested_backends
            .iter()
            .filter_map(|id| {
                let backend = &state.backends[id];
                (!backend.open).then_some((*id, backend.backend.clone()))
            })
            .collect();
        let to_load: Vec<_> = requests
            .iter()
            .filter_map(|request| {
                let entry = &state.entries[&request.id];
                (!entry.resident).then_some((request.id, entry.block.clone(), request.write))
            })
            .collect();
        for request in requests {
            state
                .entries
                .get_mut(&request.id)
                .expect("checked block")
                .pins += 1;
        }
        for (id, _) in &to_open {
            let backend = state.backends.get_mut(id).expect("checked backend");
            backend.open = true;
            state.used += backend.staging_bytes;
        }
        for (id, _, _) in &to_load {
            let entry = state.entries.get_mut(id).expect("checked block");
            entry.resident = true;
            state.used += entry.bytes;
        }
        state.used += scratch_bytes;
        drop(state);

        let result = to_open
            .iter()
            .try_for_each(|(_, backend)| backend.reopen())
            .and_then(|_| {
                to_load
                    .iter()
                    .try_for_each(|(_, block, write)| block.prepare(*write))
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
                state.used -= entry.bytes;
            }
        }
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        if let Err(error) = result {
            for id in ids {
                state.entries.get_mut(&id).expect("registered block").pins -= 1;
            }
            state.used -= scratch_bytes;
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
        let idle: Vec<_> = state
            .backends
            .iter()
            .filter(|(id, backend)| {
                backend.open
                    && state
                        .entries
                        .values()
                        .all(|entry| entry.backend != **id || !entry.resident && entry.pins == 0)
            })
            .map(|(id, backend)| (*id, backend.backend.clone(), backend.staging_bytes))
            .collect();
        drop(state);
        for (id, backend, bytes) in idle {
            if let Err(error) = backend.close() {
                let mut state = self.state.lock().map_err(poison)?;
                state.admitting = false;
                state.version += 1;
                self.wake.notify_all();
                return Err(error);
            }
            let mut state = self.state.lock().map_err(poison)?;
            state
                .backends
                .get_mut(&id)
                .expect("registered backend")
                .open = false;
            state.used -= bytes;
        }
        let mut state = self.state.lock().map_err(poison)?;
        state.admitting = false;
        state.version += 1;
        self.wake.notify_all();
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct BlockRequest {
    id: BlockId,
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
        state.used -= self.scratch_bytes;
        state.clock += 1;
        let now = state.clock;
        for request in &self.requests {
            let entry = state
                .entries
                .get_mut(&request.id)
                .expect("registered block");
            entry.pins -= 1;
            entry.last_use = now;
        }
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
    initialized: Vec<Range<usize>>,
}

struct PlaneBlock<T: LatticeElement + TilePixel> {
    backend: Arc<ArrayBackend<T>>,
    index: usize,
    cells: usize,
    fill: Option<T>,
    state: RwLock<PlaneState<T>>,
}

impl<T: LatticeElement + TilePixel> BlockIo for PlaneBlock<T> {
    fn prepare(&self, write: bool) -> io::Result<()> {
        let mut state = self.state.write().map_err(poison)?;
        if state.values.is_some() {
            return Ok(());
        }
        if !write && !state.backed && self.fill.is_none() {
            return Err(invalid("generated plane read before initialization"));
        }
        let mut values = vec![self.fill.unwrap_or_default(); self.cells];
        if state.backed {
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
        if self.fill.is_some() && state.initialized.is_empty() {
            state.initialized.push(0..self.cells);
        }
        state.values = Some(values);
        Ok(())
    }

    fn evict(&self) -> io::Result<()> {
        let mut state = self.state.write().map_err(poison)?;
        if state.dirty {
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
        Ok(())
    }

    fn resident(&self) -> bool {
        self.state
            .read()
            .expect("cube block lock poisoned")
            .values
            .is_some()
    }
}

/// One typed array of plane-aligned blocks sharing the run coordinator.
pub(crate) struct ManagedPlaneArray<T: LatticeElement + TilePixel> {
    manager: Arc<CubeResidency>,
    blocks: Vec<(BlockId, Arc<PlaneBlock<T>>)>,
    _backend: Arc<ArrayBackend<T>>,
}

impl<T: LatticeElement + TilePixel> ManagedPlaneArray<T> {
    pub(crate) fn create(
        manager: Arc<CubeResidency>,
        parent: &Path,
        axis0: usize,
        axis1: usize,
        planes: usize,
        fill: Option<T>,
    ) -> io::Result<Self> {
        let cells = axis0
            .checked_mul(axis1)
            .ok_or_else(|| invalid("plane size overflow"))?;
        let bytes = cells
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| invalid("plane byte overflow"))?;
        if planes == 0 || bytes == 0 {
            return Err(invalid("cube planes must be positive"));
        }
        let staging = manager.try_admit(&[], bytes)?;
        let directory = tempfile::Builder::new()
            .prefix(".casa-rs-managed-cube-")
            .tempdir_in(parent)?;
        let shape = TiledShape::with_tile_shape(vec![axis0, axis1, planes], vec![axis0, axis1, 1])
            .map_err(other)?;
        let layout = PagedArray::<T>::storage_layout(shape, bytes).map_err(other)?;
        let mut array = PagedArray::<T>::create_planned(&layout, directory.path().join("values"))
            .map_err(other)?;
        array.temp_close().map_err(other)?;
        drop(staging);
        let backend = Arc::new(ArrayBackend {
            array: Mutex::new(array),
            axes: [axis0, axis1],
            _directory: directory,
        });
        let backend_id = manager.register_backend(backend.clone(), bytes)?;
        let blocks = (0..planes)
            .map(|index| {
                let block = Arc::new(PlaneBlock {
                    backend: backend.clone(),
                    index,
                    cells,
                    fill,
                    state: RwLock::new(PlaneState {
                        values: None,
                        dirty: false,
                        backed: false,
                        initialized: Vec::new(),
                    }),
                });
                let id = manager.register_block(block.clone(), backend_id, bytes)?;
                Ok((id, block))
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            manager,
            blocks,
            _backend: backend,
        })
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
        if !covers(&state.initialized, &range) {
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
        remove_range(&mut state.initialized, &range);
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
        add_range(&mut self.state.initialized, self.range.clone());
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

fn covers(ranges: &[Range<usize>], requested: &Range<usize>) -> bool {
    requested.is_empty()
        || ranges
            .iter()
            .any(|range| range.start <= requested.start && range.end >= requested.end)
}

fn add_range(ranges: &mut Vec<Range<usize>>, mut added: Range<usize>) {
    let mut index = 0;
    while index < ranges.len() {
        if ranges[index].end < added.start || ranges[index].start > added.end {
            index += 1;
        } else {
            added.start = added.start.min(ranges[index].start);
            added.end = added.end.max(ranges[index].end);
            ranges.remove(index);
        }
    }
    ranges.push(added);
    ranges.sort_by_key(|range| range.start);
}

fn remove_range(ranges: &mut Vec<Range<usize>>, removed: &Range<usize>) {
    let mut retained = Vec::with_capacity(ranges.len() + 1);
    for range in ranges.drain(..) {
        if range.end <= removed.start || range.start >= removed.end {
            retained.push(range);
        } else {
            if range.start < removed.start {
                retained.push(range.start..removed.start);
            }
            if range.end > removed.end {
                retained.push(removed.end..range.end);
            }
        }
    }
    *ranges = retained;
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
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

    fn pixel_write_bytes(stats: TiledFileIoStats) -> usize {
        stats.flat_flush_write_bytes
            + stats.lru_flush_write_bytes
            + stats.lru_batch_flush_bytes
            + stats.direct_tile_write_bytes
    }

    #[test]
    fn one_shared_budget_spills_dirty_planes_and_discards_clean_planes_without_writes() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(32).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 3, Some(0.0))
                .unwrap();
        let pinned = manager
            .try_admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let mut window = array.write(&pinned, 0, 0..4).unwrap();
        window.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        window.finish();
        assert_eq!(manager.used_bytes(), 32); // one plane plus one codec tile
        assert_eq!(
            array.blocks[0]
                .1
                .state
                .read()
                .unwrap()
                .values
                .as_ref()
                .unwrap()
                .capacity(),
            4
        );
        drop(pinned);

        let pinned = manager
            .try_admit(&[array.request(1, false).unwrap()], 0)
            .unwrap();
        assert_eq!(&*array.read(&pinned, 1, 0..4).unwrap(), &[0.0; 4]);
        assert_eq!(manager.used_bytes(), 32);
        assert!(array.blocks[0].1.state.read().unwrap().values.is_none());
        drop(pinned);
        let written = array._backend.array.lock().unwrap().io_stats();
        assert!(pixel_write_bytes(written) >= 16);

        let pinned = manager
            .try_admit(&[array.request(2, false).unwrap()], 0)
            .unwrap();
        assert_eq!(&*array.read(&pinned, 2, 0..4).unwrap(), &[0.0; 4]);
        drop(pinned);
        let after_clean_evict = array._backend.array.lock().unwrap().io_stats();
        assert_eq!(
            pixel_write_bytes(after_clean_evict),
            pixel_write_bytes(written)
        );

        let pinned = manager
            .try_admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(
            &*array.read(&pinned, 0, 0..4).unwrap(),
            &[1.0, 2.0, 3.0, 4.0]
        );
        drop(pinned);
        assert_eq!(manager.used_bytes(), 32);
        let other =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 1, Some(0.0))
                .unwrap();
        let before_close = array
            ._backend
            .array
            .lock()
            .unwrap()
            .owned_persistent_heap_bytes()
            .unwrap();
        let pins = manager
            .try_admit(&[other.request(0, false).unwrap()], 0)
            .unwrap();
        drop(pins);
        assert!(array._backend.array.lock().unwrap().is_temp_closed());
        let after_close = array
            ._backend
            .array
            .lock()
            .unwrap()
            .owned_persistent_heap_bytes()
            .unwrap();
        assert!(after_close < before_close);
        assert_eq!(manager.used_bytes(), 32);
    }

    #[test]
    fn operation_is_admitted_atomically_and_pins_block_eviction() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(32).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 4, Some(0.0))
                .unwrap();
        let pins = manager
            .try_admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        assert_eq!(
            manager
                .try_admit(&[array.request(1, false).unwrap()], 0)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(manager.used_bytes(), 32);
        assert_eq!(
            manager
                .try_admit(
                    &[
                        array.request(0, false).unwrap(),
                        array.request(1, false).unwrap()
                    ],
                    0
                )
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        drop(pins);
        let pins = manager
            .try_admit(&[array.request(1, false).unwrap()], 0)
            .unwrap();
        let view = array.read(&pins, 1, 0..4).unwrap();
        assert_eq!(view.len(), 4);
        assert!(array.blocks[1].1.state.try_write().is_err());
        assert!(array.write(&pins, 1, 0..4).is_err());
        drop(view);
        assert!(array.blocks[1].1.state.try_write().is_ok());
        drop(pins);
        assert_eq!(manager.used_bytes(), 32);
    }

    #[test]
    fn waiting_admission_progresses_after_complete_pin_release() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(32).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 2, Some(0.0))
                .unwrap();
        let first = manager
            .admit(&[array.request(0, false).unwrap()], 0)
            .unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let pins = manager
                    .admit(&[array.request(1, false).unwrap()], 0)
                    .unwrap();
                assert_eq!(&*array.read(&pins, 1, 0..4).unwrap(), &[0.0; 4]);
            });
            assert_eq!(
                manager
                    .try_admit(&[array.request(1, false).unwrap()], 0)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            drop(first);
            worker.join().unwrap();
        });
    }

    #[test]
    fn phase_next_use_precedes_lru_when_selecting_a_victim() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(48).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 3, Some(0.0))
                .unwrap();
        let pins = manager
            .try_admit(
                &[
                    array.request(0, false).unwrap(),
                    array.request(1, false).unwrap(),
                ],
                0,
            )
            .unwrap();
        drop(pins);
        array.set_next_use(0, 1).unwrap();
        array.set_next_use(1, 100).unwrap();
        let pins = manager
            .try_admit(&[array.request(2, false).unwrap()], 0)
            .unwrap();
        drop(pins);
        assert!(array.blocks[0].1.resident());
        assert!(!array.blocks[1].1.resident());
        assert!(array.blocks[2].1.resident());
    }

    #[test]
    fn typed_arrays_share_policy_and_failed_generation_releases_pins() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(64).unwrap();
        let real =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 2, None).unwrap();
        let complex = ManagedPlaneArray::<Complex32>::create(
            manager.clone(),
            root.path(),
            2,
            2,
            2,
            Some(Complex32::new(0.0, 0.0)),
        )
        .unwrap();
        assert!(
            manager
                .try_admit(&[real.request(0, false).unwrap()], 0)
                .is_err()
        );
        let pins = manager
            .try_admit(&[real.request(0, true).unwrap()], 0)
            .unwrap();
        let mut cancelled = real.write(&pins, 0, 0..2).unwrap();
        cancelled.copy_from_slice(&[9.0, 9.0]);
        drop(cancelled);
        assert!(real.read(&pins, 0, 0..2).is_err());
        let mut window = real.write(&pins, 0, 0..2).unwrap();
        window.copy_from_slice(&[3.0, 4.0]);
        window.finish();
        assert_eq!(&*real.read(&pins, 0, 0..2).unwrap(), &[3.0, 4.0]);
        assert!(real.read(&pins, 0, 2..4).is_err());
        drop(pins);
        let pins = manager
            .try_admit(&[complex.request(0, true).unwrap()], 0)
            .unwrap();
        let mut window = complex.write(&pins, 0, 0..4).unwrap();
        window[1] = Complex32::new(2.0, -1.0);
        window.finish();
        assert_eq!(
            complex.read(&pins, 0, 1..2).unwrap()[0],
            Complex32::new(2.0, -1.0)
        );
        drop(pins);
        assert!(manager.used_bytes() <= 64);
    }

    #[test]
    fn backing_read_failure_is_returned_and_does_not_strand_admission() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(32).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), root.path(), 2, 2, 1, None).unwrap();
        let pins = manager
            .try_admit(&[array.request(0, true).unwrap()], 0)
            .unwrap();
        let mut window = array.write(&pins, 0, 0..4).unwrap();
        window.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        window.finish();
        drop(pins);
        manager.evict_unpinned(array.blocks[0].0).unwrap();
        assert_eq!(manager.used_bytes(), 16);
        manager.reclaim_idle_staging().unwrap();
        assert_eq!(manager.used_bytes(), 0);
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
        assert_eq!(manager.used_bytes(), 16); // reopened codec tile remains reclaimable
        manager.reclaim_idle_staging().unwrap();
        assert_eq!(manager.used_bytes(), 0);
        assert!(
            manager
                .try_admit(&[array.request(0, false).unwrap()], 0)
                .is_err()
        );
    }

    #[test]
    fn f64_and_packed_support_persist_through_real_backend() {
        let root = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(72).unwrap();
        let values =
            ManagedPlaneArray::<f64>::create(manager.clone(), root.path(), 2, 2, 2, Some(0.0))
                .unwrap();
        let support =
            ManagedPlaneArray::<bool>::create(manager.clone(), root.path(), 2, 2, 2, Some(false))
                .unwrap();
        let pins = manager
            .try_admit(
                &[
                    values.request(0, true).unwrap(),
                    support.request(0, true).unwrap(),
                ],
                0,
            )
            .unwrap();
        let mut value_window = values.write(&pins, 0, 0..4).unwrap();
        value_window.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        value_window.finish();
        let mut support_window = support.write(&pins, 0, 0..4).unwrap();
        support_window.copy_from_slice(&[false, true, false, true]);
        support_window.finish();
        drop(pins);
        // An explicit writeback is required; no destructor performs fallible I/O.
        manager.evict_unpinned(values.blocks[0].0).unwrap();
        manager.evict_unpinned(support.blocks[0].0).unwrap();
        assert_eq!(manager.used_bytes(), 36); // only two codec tiles remain
        manager.reclaim_idle_staging().unwrap();
        assert_eq!(manager.used_bytes(), 0);
        assert!(values._backend.array.lock().unwrap().is_temp_closed());
        assert!(support._backend.array.lock().unwrap().is_temp_closed());
        let pins = manager
            .try_admit(
                &[
                    values.request(0, false).unwrap(),
                    support.request(0, false).unwrap(),
                ],
                0,
            )
            .unwrap();
        assert_eq!(
            &*values.read(&pins, 0, 0..4).unwrap(),
            &[1.0, 2.0, 3.0, 4.0]
        );
        assert_eq!(
            &*support.read(&pins, 0, 0..4).unwrap(),
            &[false, true, false, true]
        );
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
