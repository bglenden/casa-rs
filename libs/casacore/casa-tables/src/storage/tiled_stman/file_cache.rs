// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Flat tile cache: one contiguous byte buffer for all tiles.
/// Allocated lazily on first access, either zeroed (fresh image)
/// or filled from disk (open). Single allocation eliminates
/// per-tile malloc overhead.
pub(super) struct FlatTileCache {
    /// Contiguous byte buffer: `data[i * tile_bytes .. (i+1) * tile_bytes]`.
    pub(super) data: Vec<u8>,
    /// Per-tile dirty flag.
    pub(super) dirty: Vec<bool>,
    pub(super) tile_bytes: usize,
    pub(super) nr_tiles: usize,
    pub(super) allocated: bool,
}

/// LRU tile cache with a bounded number of slots.
///
/// Tiles are loaded on demand and evicted (flushing dirty tiles to disk)
/// when the cache is full. This is used when `max_cache_bytes` is set and
/// smaller than the total tile data, forcing real disk I/O.
pub(super) struct LruTileCache {
    /// Fixed-size byte buffer: `max_slots × tile_bytes`.
    pub(super) data: Vec<u8>,
    /// For each slot: the tile index it holds (`usize::MAX` = empty).
    pub(super) slot_tile: Vec<usize>,
    /// Dirty flag per slot.
    pub(super) slot_dirty: Vec<bool>,
    /// Reverse lookup: tile_index → slot (−1 = not cached).
    /// Sized to `nr_tiles` for O(1) direct-indexed access (like C++ `Block<Int>`).
    pub(super) tile_to_slot: Vec<i32>,
    /// Access counter per slot for LRU eviction.
    pub(super) slot_access: Vec<u64>,
    /// Monotonically increasing access counter.
    pub(super) access_counter: u64,
    /// Maximum number of tile slots.
    pub(super) max_slots: usize,
    /// Number of currently occupied slots.
    pub(super) used_slots: usize,
    /// File handle kept open for reads and writes.
    pub(super) file: std::fs::File,
}

pub(super) enum TileCache {
    Flat(FlatTileCache),
    Lru(LruTileCache),
}

/// Diagnostic counters for tiled file reads and writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TiledFileIoStats {
    /// Slice writes submitted in last-axis-fastest order.
    pub put_slice_c_order_calls: usize,
    /// Slice writes submitted in first-axis-fastest order.
    pub put_slice_fortran_calls: usize,
    /// Tile visits during cached slice writes, including repeat visits.
    pub put_slice_tile_visits: usize,
    /// Element copies into cached tiles during slice writes.
    pub put_slice_copied_elements: usize,
    /// Whole-file cache allocation count.
    pub flat_allocations: usize,
    /// Bytes allocated for the whole-file cache.
    pub flat_allocated_bytes: usize,
    /// Bytes zero-filled during whole-file cache initialization.
    pub flat_zero_fill_bytes: usize,
    /// Tile-file bytes read in bulk into the whole-file cache.
    pub flat_bulk_read_bytes: usize,
    /// Flush requests to the whole-file cache.
    pub flat_flush_calls: usize,
    /// Dirty tiles written by the whole-file cache.
    pub flat_flush_write_tiles: usize,
    /// Bytes written by the whole-file cache during flush.
    pub flat_flush_write_bytes: usize,
    /// Tile cache lookups served by a resident LRU entry.
    pub lru_hits: usize,
    /// Tile cache lookups missing from the LRU.
    pub lru_misses: usize,
    /// New LRU tile slots filled with zeros instead of read from storage.
    pub lru_zero_fill_tiles: usize,
    /// Tiles read from storage into the LRU.
    pub lru_read_tiles: usize,
    /// Bytes read into the LRU from storage.
    pub lru_read_bytes: usize,
    /// Requests that load a group of tiles into the LRU.
    pub lru_batch_loads: usize,
    /// Number of tiles read through grouped LRU loads.
    pub lru_batch_load_tiles: usize,
    /// Bytes read through grouped LRU loads.
    pub lru_batch_load_bytes: usize,
    /// Dirty LRU tiles written on eviction.
    pub lru_dirty_evictions: usize,
    /// Flush requests to the LRU cache.
    pub lru_flush_calls: usize,
    /// Dirty LRU tiles written by a flush.
    pub lru_flush_write_tiles: usize,
    /// Bytes written by an LRU flush.
    pub lru_flush_write_bytes: usize,
    /// Grouped dirty-tile writes issued while flushing the LRU.
    pub lru_batch_flushes: usize,
    /// Tiles written through grouped LRU flushes.
    pub lru_batch_flush_tiles: usize,
    /// Bytes written through grouped LRU flushes.
    pub lru_batch_flush_bytes: usize,
    /// Requests to write aligned tiles directly.
    pub direct_tile_write_calls: usize,
    /// Complete tiles written by the direct-write path.
    pub direct_tile_write_tiles: usize,
    /// Bytes written by the direct-write path.
    pub direct_tile_write_bytes: usize,
    /// Nanoseconds spent packing tiles for direct writes.
    pub direct_tile_pack_ns: usize,
    /// Nanoseconds spent converting byte order on direct writes.
    pub direct_tile_swap_ns: usize,
    /// Nanoseconds spent writing packed tiles to the backing file.
    pub direct_tile_write_ns: usize,
}

impl TiledFileIoStats {
    /// Returns per-counter increments since an earlier sample, saturating at zero after a reset.
    pub fn delta_since(self, before: Self) -> Self {
        Self {
            put_slice_c_order_calls: self
                .put_slice_c_order_calls
                .saturating_sub(before.put_slice_c_order_calls),
            put_slice_fortran_calls: self
                .put_slice_fortran_calls
                .saturating_sub(before.put_slice_fortran_calls),
            put_slice_tile_visits: self
                .put_slice_tile_visits
                .saturating_sub(before.put_slice_tile_visits),
            put_slice_copied_elements: self
                .put_slice_copied_elements
                .saturating_sub(before.put_slice_copied_elements),
            flat_allocations: self
                .flat_allocations
                .saturating_sub(before.flat_allocations),
            flat_allocated_bytes: self
                .flat_allocated_bytes
                .saturating_sub(before.flat_allocated_bytes),
            flat_zero_fill_bytes: self
                .flat_zero_fill_bytes
                .saturating_sub(before.flat_zero_fill_bytes),
            flat_bulk_read_bytes: self
                .flat_bulk_read_bytes
                .saturating_sub(before.flat_bulk_read_bytes),
            flat_flush_calls: self
                .flat_flush_calls
                .saturating_sub(before.flat_flush_calls),
            flat_flush_write_tiles: self
                .flat_flush_write_tiles
                .saturating_sub(before.flat_flush_write_tiles),
            flat_flush_write_bytes: self
                .flat_flush_write_bytes
                .saturating_sub(before.flat_flush_write_bytes),
            lru_hits: self.lru_hits.saturating_sub(before.lru_hits),
            lru_misses: self.lru_misses.saturating_sub(before.lru_misses),
            lru_zero_fill_tiles: self
                .lru_zero_fill_tiles
                .saturating_sub(before.lru_zero_fill_tiles),
            lru_read_tiles: self.lru_read_tiles.saturating_sub(before.lru_read_tiles),
            lru_read_bytes: self.lru_read_bytes.saturating_sub(before.lru_read_bytes),
            lru_batch_loads: self.lru_batch_loads.saturating_sub(before.lru_batch_loads),
            lru_batch_load_tiles: self
                .lru_batch_load_tiles
                .saturating_sub(before.lru_batch_load_tiles),
            lru_batch_load_bytes: self
                .lru_batch_load_bytes
                .saturating_sub(before.lru_batch_load_bytes),
            lru_dirty_evictions: self
                .lru_dirty_evictions
                .saturating_sub(before.lru_dirty_evictions),
            lru_flush_calls: self.lru_flush_calls.saturating_sub(before.lru_flush_calls),
            lru_flush_write_tiles: self
                .lru_flush_write_tiles
                .saturating_sub(before.lru_flush_write_tiles),
            lru_flush_write_bytes: self
                .lru_flush_write_bytes
                .saturating_sub(before.lru_flush_write_bytes),
            lru_batch_flushes: self
                .lru_batch_flushes
                .saturating_sub(before.lru_batch_flushes),
            lru_batch_flush_tiles: self
                .lru_batch_flush_tiles
                .saturating_sub(before.lru_batch_flush_tiles),
            lru_batch_flush_bytes: self
                .lru_batch_flush_bytes
                .saturating_sub(before.lru_batch_flush_bytes),
            direct_tile_write_calls: self
                .direct_tile_write_calls
                .saturating_sub(before.direct_tile_write_calls),
            direct_tile_write_tiles: self
                .direct_tile_write_tiles
                .saturating_sub(before.direct_tile_write_tiles),
            direct_tile_write_bytes: self
                .direct_tile_write_bytes
                .saturating_sub(before.direct_tile_write_bytes),
            direct_tile_pack_ns: self
                .direct_tile_pack_ns
                .saturating_sub(before.direct_tile_pack_ns),
            direct_tile_swap_ns: self
                .direct_tile_swap_ns
                .saturating_sub(before.direct_tile_swap_ns),
            direct_tile_write_ns: self
                .direct_tile_write_ns
                .saturating_sub(before.direct_tile_write_ns),
        }
    }
}

pub(super) fn duration_nanos_usize(duration: Duration) -> usize {
    duration.as_nanos().min(usize::MAX as u128) as usize
}

/// Random-access tile I/O for a `TiledCellStMan` data file.
///
/// Provides direct read/write of individual tiles without loading the entire
/// array into memory. Uses an in-memory tile cache (modeled on C++ casacore's
/// `BucketCache`) to avoid redundant disk I/O. Tiles are byte-swapped on
/// cache load/flush, so the hot path operates on native-endian data.
///
/// # C++ equivalent
///
/// `TSMCube` + `BucketCache` in casacore's tiled storage manager.
pub(crate) struct TiledFileIO {
    pub(super) tsm_path: Box<Path>,
    #[allow(dead_code)]
    pub(super) header_path: Box<Path>,
    pub(super) table_path: Box<Path>,
    pub(super) pixel_type: PrimitiveType,
    pub(super) cube_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) tiles_per_dim: Vec<usize>,
    pub(super) nr_tiles: usize,
    pub(super) elem_size: usize,
    pub(super) tile_bytes: usize,
    pub(super) file_tile_bytes: usize,
    pub(super) tile_nelem: usize,
    pub(super) big_endian: bool,
    pub(super) file_offset: usize,
    pub(super) dm_seq_nr: u32,
    pub(super) storage_data_type: CasacoreDataType,
    /// Precomputed Fortran-order strides for tile indexing.
    pub(super) tile_strides: Vec<usize>,
    /// Precomputed Fortran-order strides for tile-grid indexing.
    pub(super) tiles_per_dim_strides: Vec<usize>,
    /// Tile cache: flat (all-in-memory) or LRU (bounded).
    pub(super) cache: TileCache,
    /// Whether this platform needs byte-swapping for the on-disk format.
    pub(super) needs_swap: bool,
    /// Byte-swap component size (4 for f32/Complex32, 8 for f64/Complex64).
    pub(super) swap_size: usize,
    /// Per-tile flag: `true` when the tile has been written to disk (via
    /// eviction or flush) and must be read back on cache miss. `false` for
    /// tiles that have never been written — these are zero-initialized
    /// without disk I/O.
    pub(super) tile_on_disk: Vec<bool>,
    /// Persistent read file handle (used by flat cache bulk load).
    pub(super) read_file: Option<std::fs::File>,
    pub(super) direct_write_buffer: Vec<u8>,
    pub(super) stats: TiledFileIoStats,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_tiled_file_tile(
    file: &mut (impl std::io::Read + Seek),
    file_pos: u64,
    dst: &mut [u8],
    file_tile_bytes: usize,
    dt: CasacoreDataType,
    tile_nelem: usize,
    needs_swap: bool,
    swap_size: usize,
) -> Result<(), StorageError> {
    if file_tile_bytes == dst.len() {
        file.seek(SeekFrom::Start(file_pos))?;
        file.read_exact(dst)?;
        if needs_swap {
            swap_bytes_inplace(dst, swap_size);
        }
        return Ok(());
    }

    let mut packed = vec![0u8; file_tile_bytes];
    file.seek(SeekFrom::Start(file_pos))?;
    file.read_exact(&mut packed)?;
    let unpacked = read_tile_storage(&packed, dt, tile_nelem);
    debug_assert_eq!(unpacked.len(), dst.len());
    dst.copy_from_slice(&unpacked);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_tiled_file_tile(
    file: &mut std::fs::File,
    file_pos: u64,
    src: &[u8],
    file_tile_bytes: usize,
    dt: CasacoreDataType,
    tile_nelem: usize,
    needs_swap: bool,
    swap_size: usize,
) -> Result<(), StorageError> {
    if file_tile_bytes == src.len() {
        if needs_swap {
            let mut buf = src.to_vec();
            swap_bytes_inplace(&mut buf, swap_size);
            file.seek(SeekFrom::Start(file_pos))?;
            file.write_all(&buf)?;
        } else {
            file.seek(SeekFrom::Start(file_pos))?;
            file.write_all(src)?;
        }
        return Ok(());
    }

    let mut packed = vec![0u8; file_tile_bytes];
    write_tile_storage(&mut packed, dt, src, tile_nelem);
    file.seek(SeekFrom::Start(file_pos))?;
    file.write_all(&packed)?;
    Ok(())
}

impl TiledFileIO {
    /// Ensures the flat cache is allocated (zeroed for fresh, loaded from disk otherwise).
    pub(super) fn ensure_flat_cache_allocated(&mut self) -> Result<(), StorageError> {
        let flat = match &mut self.cache {
            TileCache::Flat(f) => f,
            _ => return Ok(()),
        };
        if flat.allocated {
            return Ok(());
        }
        let total_bytes = flat.nr_tiles * flat.tile_bytes;
        let all_fresh = self.tile_on_disk.iter().all(|&on| !on);
        self.stats.flat_allocations = self.stats.flat_allocations.saturating_add(1);
        self.stats.flat_allocated_bytes =
            self.stats.flat_allocated_bytes.saturating_add(total_bytes);
        if all_fresh {
            flat.data = vec![0u8; total_bytes];
            self.stats.flat_zero_fill_bytes =
                self.stats.flat_zero_fill_bytes.saturating_add(total_bytes);
        } else {
            let file_tile_bytes = self.file_tile_bytes;
            let needs_swap = self.needs_swap;
            let swap_size = self.swap_size;
            let dt = self.storage_data_type;
            let tile_nelem = self.tile_nelem;

            flat.data = vec![0u8; total_bytes];
            {
                let f: &mut std::fs::File;
                let mut temp_file;
                if let Some(ref mut rf) = self.read_file {
                    f = rf;
                } else {
                    temp_file = OpenOptions::new().read(true).open(&self.tsm_path)?;
                    f = &mut temp_file;
                }
                if file_tile_bytes == flat.tile_bytes {
                    f.seek(SeekFrom::Start(self.file_offset as u64))?;
                    f.read_exact(&mut flat.data)?;
                    self.stats.flat_bulk_read_bytes =
                        self.stats.flat_bulk_read_bytes.saturating_add(total_bytes);
                    if needs_swap {
                        swap_bytes_inplace(&mut flat.data, swap_size);
                    }
                } else {
                    for tile_index in 0..flat.nr_tiles {
                        let off = tile_index * flat.tile_bytes;
                        let file_pos = (self.file_offset + tile_index * file_tile_bytes) as u64;
                        read_tiled_file_tile(
                            f,
                            file_pos,
                            &mut flat.data[off..off + flat.tile_bytes],
                            file_tile_bytes,
                            dt,
                            tile_nelem,
                            needs_swap,
                            swap_size,
                        )?;
                        self.stats.flat_bulk_read_bytes = self
                            .stats
                            .flat_bulk_read_bytes
                            .saturating_add(file_tile_bytes);
                    }
                }
            }
        }
        flat.allocated = true;
        Ok(())
    }

    /// Ensures a tile is available in the LRU cache, loading it from disk
    /// and evicting the least-recently-used tile if necessary.
    /// Returns the slot index holding the tile.
    pub(super) fn ensure_lru_tile_loaded(
        &mut self,
        tile_index: usize,
    ) -> Result<usize, StorageError> {
        // Check if already loaded via direct-indexed Vec (O(1)).
        let cached_slot = match &self.cache {
            TileCache::Lru(lru) => lru.tile_to_slot[tile_index],
            _ => return Ok(0), // flat cache — not used
        };
        if cached_slot >= 0 {
            self.stats.lru_hits = self.stats.lru_hits.saturating_add(1);
            return Ok(cached_slot as usize);
        }
        self.stats.lru_misses = self.stats.lru_misses.saturating_add(1);

        let tile_bytes = self.tile_bytes;
        let file_tile_bytes = self.file_tile_bytes;
        let file_offset = self.file_offset;
        let needs_swap = self.needs_swap;
        let swap_size = self.swap_size;
        let dt = self.storage_data_type;
        let tile_nelem = self.tile_nelem;
        let tile_needs_read = self.tile_on_disk[tile_index];

        let lru = match &mut self.cache {
            TileCache::Lru(lru) => lru,
            _ => unreachable!(),
        };

        let slot = if lru.used_slots < lru.max_slots {
            let s = lru.used_slots;
            lru.used_slots += 1;
            s
        } else {
            // Try batch flush (dirty contiguous tiles → single write).
            if Self::try_batch_flush_lru_inner(
                lru,
                &mut self.tile_on_disk,
                tile_bytes,
                file_tile_bytes,
                file_offset,
                needs_swap,
                swap_size,
            )? {
                self.stats.lru_batch_flushes = self.stats.lru_batch_flushes.saturating_add(1);
                self.stats.lru_batch_flush_tiles = self
                    .stats
                    .lru_batch_flush_tiles
                    .saturating_add(lru.max_slots);
                self.stats.lru_batch_flush_bytes = self
                    .stats
                    .lru_batch_flush_bytes
                    .saturating_add(lru.max_slots.saturating_mul(file_tile_bytes));
                let s = lru.used_slots;
                lru.used_slots += 1;
                s
            }
            // Try batch load (clean contiguous tiles → single read prefetch).
            else if let Some(load_count) = Self::try_batch_load_lru_inner(
                lru,
                &self.tile_on_disk,
                tile_index,
                self.nr_tiles,
                tile_bytes,
                file_tile_bytes,
                file_offset,
                needs_swap,
                swap_size,
            )? {
                self.stats.lru_batch_loads = self.stats.lru_batch_loads.saturating_add(1);
                self.stats.lru_batch_load_tiles =
                    self.stats.lru_batch_load_tiles.saturating_add(load_count);
                self.stats.lru_batch_load_bytes = self
                    .stats
                    .lru_batch_load_bytes
                    .saturating_add(load_count.saturating_mul(file_tile_bytes));
                // Tile is already loaded at slot 0 by the batch load.
                return Ok(0);
            } else {
                // Find least-recently-used slot.
                let mut min_slot = 0;
                let mut min_access = u64::MAX;
                for (i, &access) in lru.slot_access[..lru.max_slots].iter().enumerate() {
                    if access < min_access {
                        min_access = access;
                        min_slot = i;
                    }
                }

                // Evict: write dirty tile to disk if needed.
                let old_tile = lru.slot_tile[min_slot];
                if Self::flush_lru_slot_inner(
                    lru,
                    &mut self.tile_on_disk,
                    min_slot,
                    tile_bytes,
                    file_tile_bytes,
                    file_offset,
                    dt,
                    tile_nelem,
                    needs_swap,
                    swap_size,
                )? {
                    self.stats.lru_dirty_evictions =
                        self.stats.lru_dirty_evictions.saturating_add(1);
                    self.stats.lru_flush_write_tiles =
                        self.stats.lru_flush_write_tiles.saturating_add(1);
                    self.stats.lru_flush_write_bytes = self
                        .stats
                        .lru_flush_write_bytes
                        .saturating_add(file_tile_bytes);
                }
                if old_tile != usize::MAX {
                    lru.tile_to_slot[old_tile] = -1;
                }
                min_slot
            }
        };

        // Load tile data into the slot.
        let off = slot * tile_bytes;
        if !tile_needs_read {
            lru.data[off..off + tile_bytes].fill(0);
            self.stats.lru_zero_fill_tiles = self.stats.lru_zero_fill_tiles.saturating_add(1);
        } else {
            let file_pos = (file_offset + tile_index * file_tile_bytes) as u64;
            if file_tile_bytes == tile_bytes {
                #[cfg(unix)]
                lru.file
                    .read_exact_at(&mut lru.data[off..off + tile_bytes], file_pos)?;
                #[cfg(not(unix))]
                {
                    lru.file.seek(SeekFrom::Start(file_pos))?;
                    lru.file.read_exact(&mut lru.data[off..off + tile_bytes])?;
                }
                if needs_swap {
                    swap_bytes_inplace(&mut lru.data[off..off + tile_bytes], swap_size);
                }
            } else {
                read_tiled_file_tile(
                    &mut lru.file,
                    file_pos,
                    &mut lru.data[off..off + tile_bytes],
                    file_tile_bytes,
                    dt,
                    tile_nelem,
                    needs_swap,
                    swap_size,
                )?;
            }
            self.stats.lru_read_tiles = self.stats.lru_read_tiles.saturating_add(1);
            self.stats.lru_read_bytes = self.stats.lru_read_bytes.saturating_add(file_tile_bytes);
        }

        // Update mappings.
        lru.slot_tile[slot] = tile_index;
        lru.tile_to_slot[tile_index] = slot as i32;
        lru.slot_dirty[slot] = false;
        lru.slot_access[slot] = lru.access_counter;
        lru.access_counter += 1;
        Ok(slot)
    }

    /// Attempts to batch-flush all dirty LRU tiles in a single write when
    /// they form a contiguous range of tile indices mapped to sequential slots.
    ///
    /// Returns `true` if the flush was performed (cache is now empty and ready
    /// for reuse), `false` if conditions weren't met (caller should fall back
    /// to per-tile LRU eviction).
    pub(super) fn try_batch_flush_lru_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &mut [bool],
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<bool, StorageError> {
        if file_tile_bytes != tile_bytes {
            return Ok(false);
        }
        // All slots must be used and dirty, and tiles must be in sequential
        // slot order (slot i holds tile base+i) for a single contiguous write.
        let max_slots = lru.max_slots;
        let mut all_dirty = true;
        let base_tile = lru.slot_tile[0];
        let mut contiguous = base_tile != usize::MAX;
        for i in 0..max_slots {
            if !lru.slot_dirty[i] {
                all_dirty = false;
                break;
            }
            if lru.slot_tile[i] != base_tile + i {
                contiguous = false;
                break;
            }
        }
        if !all_dirty || !contiguous {
            return Ok(false);
        }

        // Single write of the entire cache buffer.
        let total_bytes = max_slots * tile_bytes;
        let file_pos = (file_offset + base_tile * tile_bytes) as u64;
        if needs_swap {
            let mut buf = lru.data[..total_bytes].to_vec();
            swap_bytes_inplace(&mut buf, swap_size);
            lru.file.seek(SeekFrom::Start(file_pos))?;
            lru.file.write_all(&buf)?;
        } else {
            lru.file.seek(SeekFrom::Start(file_pos))?;
            lru.file.write_all(&lru.data[..total_bytes])?;
        }

        // Mark all flushed tiles as on-disk and reset cache state.
        for i in 0..max_slots {
            tile_on_disk[base_tile + i] = true;
            lru.tile_to_slot[base_tile + i] = -1;
        }
        lru.slot_tile.fill(usize::MAX);
        lru.slot_dirty.fill(false);
        lru.used_slots = 0;
        lru.access_counter = 0;
        Ok(true)
    }

    /// Flush one dirty LRU slot and mark that tile as persisted on disk.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn flush_lru_slot_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &mut [bool],
        slot: usize,
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        dt: CasacoreDataType,
        tile_nelem: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<bool, StorageError> {
        let tile_idx = lru.slot_tile[slot];
        if tile_idx == usize::MAX || !lru.slot_dirty[slot] {
            return Ok(false);
        }

        let off = slot * tile_bytes;
        let file_pos = (file_offset + tile_idx * file_tile_bytes) as u64;
        write_tiled_file_tile(
            &mut lru.file,
            file_pos,
            &lru.data[off..off + tile_bytes],
            file_tile_bytes,
            dt,
            tile_nelem,
            needs_swap,
            swap_size,
        )?;
        lru.slot_dirty[slot] = false;
        tile_on_disk[tile_idx] = true;
        Ok(true)
    }

    /// Batch-loads a contiguous range of tiles into the LRU cache with a
    /// single read when the cache holds clean, contiguous tiles and the
    /// requested tile continues sequentially.
    ///
    /// Returns the number of loaded tiles if the batch load was performed
    /// (requested tile is now at slot 0), `None` if conditions weren't met.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_batch_load_lru_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &[bool],
        tile_index: usize,
        nr_tiles: usize,
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<Option<usize>, StorageError> {
        if file_tile_bytes != tile_bytes {
            return Ok(None);
        }
        let max_slots = lru.max_slots;

        // Check: current slots hold contiguous clean tiles, and the requested
        // tile continues right after them (sequential scan pattern).
        let base_tile = lru.slot_tile[0];
        if base_tile == usize::MAX || tile_index != base_tile + max_slots {
            return Ok(None);
        }
        for i in 0..max_slots {
            if lru.slot_dirty[i] || lru.slot_tile[i] != base_tile + i {
                return Ok(None);
            }
        }

        // All tiles on disk from tile_index onward? (Required for bulk read.)
        let load_count = max_slots.min(nr_tiles - tile_index);
        let all_on_disk = tile_on_disk[tile_index..tile_index + load_count]
            .iter()
            .all(|&d| d);
        if !all_on_disk {
            return Ok(None);
        }

        // Clear old mappings.
        for i in 0..max_slots {
            lru.tile_to_slot[base_tile + i] = -1;
        }

        // Single read of the contiguous tile range.
        let total_bytes = load_count * tile_bytes;
        let file_pos = (file_offset + tile_index * tile_bytes) as u64;
        lru.file.seek(SeekFrom::Start(file_pos))?;
        lru.file.read_exact(&mut lru.data[..total_bytes])?;
        if needs_swap {
            swap_bytes_inplace(&mut lru.data[..total_bytes], swap_size);
        }

        // Set up new mappings.
        for i in 0..load_count {
            lru.slot_tile[i] = tile_index + i;
            lru.tile_to_slot[tile_index + i] = i as i32;
            lru.slot_dirty[i] = false;
            lru.slot_access[i] = i as u64;
        }
        // Mark any remaining slots as empty (when load_count < max_slots).
        lru.slot_tile[load_count..max_slots].fill(usize::MAX);
        lru.used_slots = load_count;
        lru.access_counter = load_count as u64;
        Ok(Some(load_count))
    }

    /// Gets a tile from the cache (read-only), returned as `&[u8]`.
    #[inline]
    pub(super) fn get_cached_tile(&mut self, tile_index: usize) -> Result<&[u8], StorageError> {
        match &self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    self.ensure_flat_cache_allocated()?;
                }
                let tile_bytes = self.tile_bytes;
                let flat = match &self.cache {
                    TileCache::Flat(f) => f,
                    _ => unreachable!(),
                };
                let off = tile_index * tile_bytes;
                Ok(&flat.data[off..off + tile_bytes])
            }
            TileCache::Lru(_) => {
                let slot = self.ensure_lru_tile_loaded(tile_index)?;
                let tile_bytes = self.tile_bytes;
                let lru = match &mut self.cache {
                    TileCache::Lru(l) => l,
                    _ => unreachable!(),
                };
                lru.slot_access[slot] = lru.access_counter;
                lru.access_counter += 1;
                let off = slot * tile_bytes;
                Ok(&lru.data[off..off + tile_bytes])
            }
        }
    }

    /// Gets a mutable tile from the cache. Marks it dirty.
    #[inline]
    pub(super) fn get_cached_tile_mut(
        &mut self,
        tile_index: usize,
    ) -> Result<&mut [u8], StorageError> {
        match &self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    self.ensure_flat_cache_allocated()?;
                }
                let tile_bytes = self.tile_bytes;
                let flat = match &mut self.cache {
                    TileCache::Flat(f) => f,
                    _ => unreachable!(),
                };
                flat.dirty[tile_index] = true;
                let off = tile_index * tile_bytes;
                Ok(&mut flat.data[off..off + tile_bytes])
            }
            TileCache::Lru(_) => {
                let slot = self.ensure_lru_tile_loaded(tile_index)?;
                let tile_bytes = self.tile_bytes;
                let lru = match &mut self.cache {
                    TileCache::Lru(l) => l,
                    _ => unreachable!(),
                };
                lru.slot_dirty[slot] = true;
                lru.slot_access[slot] = lru.access_counter;
                lru.access_counter += 1;
                let off = slot * tile_bytes;
                Ok(&mut lru.data[off..off + tile_bytes])
            }
        }
    }

    /// Flushes all dirty tiles to disk and clears the cache.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.flush_with_flat_file(|path| OpenOptions::new().write(true).open(path))
    }

    pub(super) fn flush_with_flat_file(
        &mut self,
        open: impl FnOnce(&Path) -> std::io::Result<File>,
    ) -> Result<(), StorageError> {
        match &mut self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    return Ok(());
                }
                self.stats.flat_flush_calls = self.stats.flat_flush_calls.saturating_add(1);
                let has_dirty = flat.dirty.iter().any(|&d| d);
                if has_dirty {
                    let mut f = std::io::BufWriter::new(open(&self.tsm_path)?);

                    if self.file_tile_bytes != self.tile_bytes || self.needs_swap {
                        let tile_bytes = self.tile_bytes;
                        let file_tile_bytes = self.file_tile_bytes;
                        let dt = self.storage_data_type;
                        let tile_nelem = self.tile_nelem;
                        for tile_index in 0..self.nr_tiles {
                            if !flat.dirty[tile_index] {
                                continue;
                            }
                            let off = tile_index * tile_bytes;
                            let offset = self.file_offset + tile_index * file_tile_bytes;
                            write_tiled_file_tile(
                                f.get_mut(),
                                offset as u64,
                                &flat.data[off..off + tile_bytes],
                                file_tile_bytes,
                                dt,
                                tile_nelem,
                                self.needs_swap,
                                self.swap_size,
                            )?;
                            self.stats.flat_flush_write_tiles =
                                self.stats.flat_flush_write_tiles.saturating_add(1);
                            self.stats.flat_flush_write_bytes = self
                                .stats
                                .flat_flush_write_bytes
                                .saturating_add(file_tile_bytes);
                            self.tile_on_disk[tile_index] = true;
                        }
                    } else {
                        let tile_bytes = self.tile_bytes;
                        let mut run_start: Option<usize> = None;
                        for tile_index in 0..=self.nr_tiles {
                            let is_dirty = tile_index < self.nr_tiles && flat.dirty[tile_index];
                            if is_dirty && run_start.is_none() {
                                run_start = Some(tile_index);
                            } else if !is_dirty && let Some(start) = run_start {
                                let byte_start = self.file_offset + start * tile_bytes;
                                let src_start = start * tile_bytes;
                                let src_end = tile_index * tile_bytes;
                                f.seek(SeekFrom::Start(byte_start as u64))?;
                                f.write_all(&flat.data[src_start..src_end])?;
                                self.stats.flat_flush_write_tiles = self
                                    .stats
                                    .flat_flush_write_tiles
                                    .saturating_add(tile_index.saturating_sub(start));
                                self.stats.flat_flush_write_bytes = self
                                    .stats
                                    .flat_flush_write_bytes
                                    .saturating_add(src_end.saturating_sub(src_start));
                                self.tile_on_disk[start..tile_index].fill(true);
                                run_start = None;
                            }
                        }
                    }
                    f.flush()?;
                }
                flat.data.clear();
                flat.data.shrink_to_fit();
                flat.dirty.fill(false);
                flat.allocated = false;
            }
            TileCache::Lru(lru) => {
                self.stats.lru_flush_calls = self.stats.lru_flush_calls.saturating_add(1);
                let tile_bytes = self.tile_bytes;
                let file_tile_bytes = self.file_tile_bytes;
                let file_offset = self.file_offset;
                let needs_swap = self.needs_swap;
                let swap_size = self.swap_size;
                let dt = self.storage_data_type;
                let tile_nelem = self.tile_nelem;
                // Try batch flush first (single write if tiles are contiguous).
                if !Self::try_batch_flush_lru_inner(
                    lru,
                    &mut self.tile_on_disk,
                    tile_bytes,
                    file_tile_bytes,
                    file_offset,
                    needs_swap,
                    swap_size,
                )? {
                    // Fall back to per-tile flush.
                    for slot in 0..lru.used_slots {
                        if Self::flush_lru_slot_inner(
                            lru,
                            &mut self.tile_on_disk,
                            slot,
                            tile_bytes,
                            file_tile_bytes,
                            file_offset,
                            dt,
                            tile_nelem,
                            needs_swap,
                            swap_size,
                        )? {
                            self.stats.lru_flush_write_tiles =
                                self.stats.lru_flush_write_tiles.saturating_add(1);
                            self.stats.lru_flush_write_bytes = self
                                .stats
                                .lru_flush_write_bytes
                                .saturating_add(file_tile_bytes);
                        }
                    }
                    // Reset cache state.
                    for slot in 0..lru.used_slots {
                        let tile_idx = lru.slot_tile[slot];
                        if tile_idx != usize::MAX {
                            lru.tile_to_slot[tile_idx] = -1;
                        }
                    }
                    lru.slot_tile.fill(usize::MAX);
                    lru.slot_dirty.fill(false);
                    lru.used_slots = 0;
                    lru.access_counter = 0;
                } else {
                    self.stats.lru_batch_flushes = self.stats.lru_batch_flushes.saturating_add(1);
                    self.stats.lru_batch_flush_tiles = self
                        .stats
                        .lru_batch_flush_tiles
                        .saturating_add(lru.max_slots);
                    self.stats.lru_batch_flush_bytes = self
                        .stats
                        .lru_batch_flush_bytes
                        .saturating_add(lru.max_slots.saturating_mul(file_tile_bytes));
                }
            }
        }
        Ok(())
    }
}
