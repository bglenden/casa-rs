// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl TiledFileIO {
    /// Creates a new `TiledFileIO`, writing the TSM header and allocating
    /// a zeroed data file on disk.
    pub fn create(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
    ) -> Result<Self, StorageError> {
        Self::create_impl(
            table_path, cube_shape, tile_shape, pixel_type, big_endian, dm_seq_nr, dm_name, 0,
        )
    }

    /// Creates a new `TiledFileIO` with an explicit cache size limit.
    ///
    /// When `max_cache_bytes > 0` and smaller than the total tile data,
    /// an LRU tile cache is used, forcing real disk I/O on eviction/load.
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_cache_limit(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::create_impl(
            table_path,
            cube_shape,
            tile_shape,
            pixel_type,
            big_endian,
            dm_seq_nr,
            dm_name,
            max_cache_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_impl(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        let layout = TiledArrayStorageLayout::new(
            cube_shape,
            tile_shape,
            pixel_type,
            big_endian,
            dm_seq_nr,
            dm_name,
            if max_cache_bytes == 0 {
                usize::MAX
            } else {
                max_cache_bytes
            },
        )?;
        Self::create_planned(table_path, &layout)
    }

    pub(super) fn create_planned(
        table_path: &Path,
        layout: &TiledArrayStorageLayout,
    ) -> Result<Self, StorageError> {
        let tsm_path = tsm_data_path(table_path, layout.dm_seq_nr, 0).into_boxed_path();
        let header_path = table_path
            .join(format!("table.f{}", layout.dm_seq_nr))
            .into_boxed_path();
        layout.write_new_data_file(&tsm_path, 0)?;
        std::fs::write(&header_path, &layout.header)?;
        let cache = if layout.use_lru {
            let file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
            TileCache::Lru(LruTileCache {
                data: vec![0; layout.cache_payload_bytes()?],
                slot_tile: vec![usize::MAX; layout.cache_slots],
                slot_dirty: vec![false; layout.cache_slots],
                tile_to_slot: vec![-1; layout.tile_count],
                slot_access: vec![0; layout.cache_slots],
                access_counter: 0,
                max_slots: layout.cache_slots,
                used_slots: 0,
                file,
            })
        } else {
            TileCache::Flat(FlatTileCache {
                data: Vec::new(),
                dirty: vec![false; layout.tile_count],
                tile_bytes: layout.tile_bytes,
                nr_tiles: layout.tile_count,
                allocated: false,
            })
        };
        let tiles_per_dim = layout
            .cube_shape
            .iter()
            .zip(&layout.tile_shape)
            .map(|(&shape, &tile)| shape.div_ceil(tile))
            .collect::<Vec<_>>();
        let dt = CasacoreDataType::from_primitive_type(layout.pixel_type, false);
        let elem_size = tile_element_size(dt);
        let swap_size = match dt {
            CasacoreDataType::TpComplex => 4,
            CasacoreDataType::TpDComplex => 8,
            _ => elem_size,
        };
        Ok(Self {
            tsm_path,
            header_path,
            table_path: table_path.into(),
            pixel_type: layout.pixel_type,
            cube_shape: layout.cube_shape.clone(),
            tile_shape: layout.tile_shape.clone(),
            tile_strides: fortran_order_strides(&layout.tile_shape),
            tiles_per_dim_strides: fortran_order_strides(&tiles_per_dim),
            tiles_per_dim,
            nr_tiles: layout.tile_count,
            elem_size,
            tile_bytes: layout.tile_bytes,
            file_tile_bytes: layout.file_tile_bytes,
            tile_nelem: layout.tile_elements,
            big_endian: layout.big_endian,
            file_offset: 0,
            dm_seq_nr: layout.dm_seq_nr,
            storage_data_type: dt,
            cache,
            needs_swap: layout.big_endian != cfg!(target_endian = "big"),
            swap_size,
            tile_on_disk: vec![false; layout.tile_count],
            read_file: None,
            direct_write_buffer: Vec::new(),
            stats: TiledFileIoStats::default(),
        })
    }
    /// Opens an existing `TiledFileIO` by reading the TSM header file.
    pub fn open(table_path: &Path, dm_seq_nr: u32) -> Result<Self, StorageError> {
        Self::open_impl(table_path, dm_seq_nr, 0)
    }

    /// Opens an existing `TiledFileIO` with an explicit cache size limit.
    pub fn open_with_cache_limit(
        table_path: &Path,
        dm_seq_nr: u32,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::open_impl(table_path, dm_seq_nr, max_cache_bytes)
    }

    pub(super) fn open_impl(
        table_path: &Path,
        dm_seq_nr: u32,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        let header_path = table_path
            .join(format!("table.f{dm_seq_nr}"))
            .into_boxed_path();
        let (_variant, header) = read_tiled_header(&header_path)?;

        if header.cubes.is_empty() {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: no cubes in header".to_string(),
            ));
        }
        let cube = &header.cubes[0];
        if cube.file_seq_nr < 0 {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: invalid file_seq_nr".to_string(),
            ));
        }
        if header.col_data_types.is_empty() {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: no column data types".to_string(),
            ));
        }

        let dt = header.col_data_types[0];
        let elem_size = tile_element_size(dt);
        crate::storage::TileLayoutPlanner::repository_default()
            .plan_explicit_array(&cube.cube_shape, elem_size, &cube.tile_shape)
            .map_err(|error| StorageError::FormatMismatch(error.to_string()))?;
        let tile_nelem = checked_storage_product(&cube.tile_shape, "tile elements")?;
        let tile_bytes = tile_nelem.checked_mul(elem_size).ok_or_else(|| {
            StorageError::FormatMismatch("tile byte size overflows usize".to_string())
        })?;
        let file_tile_bytes = if dt == CasacoreDataType::TpBool {
            tile_nelem.div_ceil(8)
        } else {
            tile_bytes
        };
        let ndim = cube.cube_shape.len();

        let tiles_per_dim: Vec<usize> = (0..ndim)
            .map(|d| cube.cube_shape[d].div_ceil(cube.tile_shape[d]))
            .collect();
        let nr_tiles = checked_storage_product(&tiles_per_dim, "tile count")?;

        let tsm_path =
            tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32).into_boxed_path();

        let tile_strides = fortran_order_strides(&cube.tile_shape);
        let tiles_per_dim_strides = fortran_order_strides(&tiles_per_dim);
        let needs_swap = header.big_endian != cfg!(target_endian = "big");

        let swap_size = match dt {
            CasacoreDataType::TpComplex => 4,
            CasacoreDataType::TpDComplex => 8,
            _ => elem_size,
        };

        let total_data_bytes = nr_tiles.checked_mul(tile_bytes).ok_or_else(|| {
            StorageError::FormatMismatch("resident tile data size overflows usize".to_string())
        })?;
        let use_lru = max_cache_bytes > 0 && max_cache_bytes < total_data_bytes;
        let (cache, read_file) = if use_lru {
            let max_slots = (max_cache_bytes / tile_bytes).max(1);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tsm_path)
                .or_else(|error| match error.kind() {
                    std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::ReadOnlyFilesystem => {
                        OpenOptions::new().read(true).open(&tsm_path)
                    }
                    _ => Err(error),
                })?;
            (
                TileCache::Lru(LruTileCache {
                    data: vec![0u8; max_slots * tile_bytes],
                    slot_tile: vec![usize::MAX; max_slots],
                    slot_dirty: vec![false; max_slots],
                    tile_to_slot: vec![-1i32; nr_tiles],
                    slot_access: vec![0u64; max_slots],
                    access_counter: 0,
                    max_slots,
                    used_slots: 0,
                    file,
                }),
                None,
            )
        } else {
            let read_file = OpenOptions::new().read(true).open(&tsm_path).ok();
            (
                TileCache::Flat(FlatTileCache {
                    data: Vec::new(),
                    dirty: vec![false; nr_tiles],
                    tile_bytes,
                    nr_tiles,
                    allocated: false,
                }),
                read_file,
            )
        };

        Ok(Self {
            tsm_path,
            header_path,
            table_path: table_path.into(),
            pixel_type: dt.to_primitive_type().ok_or_else(|| {
                StorageError::FormatMismatch(
                    "TiledFileIO: unsupported primitive type in header".to_string(),
                )
            })?,
            cube_shape: cube.cube_shape.clone(),
            tile_shape: cube.tile_shape.clone(),
            tiles_per_dim,
            nr_tiles,
            elem_size,
            tile_bytes,
            file_tile_bytes,
            tile_nelem,
            big_endian: header.big_endian,
            file_offset: cube.file_offset as usize,
            dm_seq_nr,
            storage_data_type: dt,
            tile_strides,
            tiles_per_dim_strides,
            cache,
            needs_swap,
            swap_size,
            tile_on_disk: vec![true; nr_tiles],
            read_file,
            direct_write_buffer: Vec::new(),
            stats: TiledFileIoStats::default(),
        })
    }

    /// Returns the cube shape.
    pub fn cube_shape(&self) -> &[usize] {
        &self.cube_shape
    }

    /// Returns the tile shape.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }

    /// Returns the first cube column's pixel type when it maps to a supported
    /// primitive type.
    pub fn pixel_type(&self) -> Option<PrimitiveType> {
        Some(self.pixel_type)
    }

    /// Whether the on-disk byte order is big-endian.
    pub fn big_endian(&self) -> bool {
        self.big_endian
    }

    /// Returns the DM sequence number.
    pub fn dm_seq_nr(&self) -> u32 {
        self.dm_seq_nr
    }

    /// Returns the path to the table directory.
    pub fn table_path(&self) -> &Path {
        &self.table_path
    }

    /// Returns current diagnostic counters for this tiled file handle.
    pub fn io_stats(&self) -> TiledFileIoStats {
        self.stats
    }

    /// Clears diagnostic counters for this tiled file handle.
    pub fn reset_io_stats(&mut self) {
        self.stats = TiledFileIoStats::default();
    }
}

impl TiledFileIO {
    /// Writes a rectangular slice from an ndarray into the tile file.
    ///
    /// Accepts any ndarray layout (C-order or Fortran-order).
    pub fn put_slice_ndarray<T: TilePixel>(
        &mut self,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), StorageError> {
        let shape: Vec<usize> = data.shape().to_vec();
        let fortran_view = data.t();
        let maybe_fortran_slice = fortran_view.as_slice();
        if let Some(f_data) = maybe_fortran_slice {
            return self.put_slice_fortran::<T>(f_data, start, &shape);
        }
        // Fallback: array is C-order. Use per-element scatter.
        let ndim = self.cube_shape.len();
        let tile_strides = self.tile_strides.clone();
        let tiles_per_dim_strides = self.tiles_per_dim_strides.clone();
        let tile_shape = self.tile_shape.clone();
        let cube_shape = self.cube_shape.clone();

        let mut tile_start = vec![0usize; ndim];
        let mut tile_end = vec![0usize; ndim];
        for d in 0..ndim {
            tile_start[d] = start[d] / tile_shape[d];
            tile_end[d] = (start[d] + shape[d] - 1) / tile_shape[d] + 1;
        }

        let mut tile_pos = tile_start.clone();
        loop {
            let tile_index = dot_product(&tile_pos, &tiles_per_dim_strides);
            let tile_origin: Vec<usize> = (0..ndim).map(|d| tile_pos[d] * tile_shape[d]).collect();

            let mut overlap_lo = vec![0usize; ndim];
            let mut overlap_hi = vec![0usize; ndim];
            let mut valid = true;
            for d in 0..ndim {
                let tile_end_d = std::cmp::min(tile_origin[d] + tile_shape[d], cube_shape[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);
                let inner_count = overlap_hi[0] - overlap_lo[0];

                let outer_dims: Vec<usize> =
                    (1..ndim).map(|d| overlap_hi[d] - overlap_lo[d]).collect();
                let outer_total: usize = outer_dims.iter().product();

                for outer_lin in 0..outer_total {
                    let mut tile_off = (overlap_lo[0] - tile_origin[0]) * tile_strides[0];
                    let mut remaining = outer_lin;
                    let mut nd_idx = vec![0usize; ndim];
                    for d in 1..ndim {
                        let ext = overlap_hi[d] - overlap_lo[d];
                        let coord = remaining % ext;
                        remaining /= ext;
                        tile_off += (overlap_lo[d] - tile_origin[d] + coord) * tile_strides[d];
                        nd_idx[d] = overlap_lo[d] - start[d] + coord;
                    }

                    let x_start = overlap_lo[0] - start[0];
                    for i in 0..inner_count {
                        nd_idx[0] = x_start + i;
                        tile_data[tile_off + i] = data[IxDyn(&nd_idx)];
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }
        Ok(())
    }

    /// Writes a rectangular slice of C-order (row-major) data into tiles.
    ///
    /// Fast path for full, tile-aligned spatial plane groups. The input
    /// remains in C-order, but the output buffer is laid out as contiguous
    /// Fortran-order tiles and written to the tiled data file in one operation.
    pub fn put_aligned_c_order_tiles<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<bool, StorageError> {
        let ndim = self.cube_shape.len();
        if ndim != 4 || start.len() != ndim || shape.len() != ndim {
            return Ok(false);
        }
        if self.file_tile_bytes != self.tile_bytes
            || T::ELEM_SIZE != self.elem_size
            || self.tile_shape[2] != 1
            || self.tile_shape[3] != 1
            || shape[2] != 1
            || shape[3] == 0
            || start[0] != 0
            || start[1] != 0
            || start[2] != 0
            || self.tiles_per_dim[2] != 1
            || shape[0] != self.cube_shape[0]
            || shape[1] != self.cube_shape[1]
            || shape[2] != self.cube_shape[2]
            || !shape[0].is_multiple_of(self.tile_shape[0])
            || !shape[1].is_multiple_of(self.tile_shape[1])
        {
            return Ok(false);
        }
        for d in 0..ndim {
            if start[d].saturating_add(shape[d]) > self.cube_shape[d]
                || !start[d].is_multiple_of(self.tile_shape[d])
            {
                return Ok(false);
            }
        }
        if data.len() != shape.iter().product::<usize>() {
            return Err(StorageError::FormatMismatch(format!(
                "aligned tile write data length {} does not match shape {:?}",
                data.len(),
                shape
            )));
        }
        if !matches!(self.cache, TileCache::Lru(_)) {
            return Ok(false);
        }
        let lru_has_tiles = match &self.cache {
            TileCache::Lru(lru) => lru.used_slots > 0,
            TileCache::Flat(_) => false,
        };
        if lru_has_tiles {
            self.flush()?;
        }

        let tile_x = self.tile_shape[0];
        let tile_y = self.tile_shape[1];
        let tiles_x = shape[0] / tile_x;
        let tiles_y = shape[1] / tile_y;
        let plane_tile_count = tiles_x.saturating_mul(tiles_y);
        let tile_count = plane_tile_count.saturating_mul(shape[3]);
        let base_tile = (start[0] / tile_x) * self.tiles_per_dim_strides[0]
            + (start[1] / tile_y) * self.tiles_per_dim_strides[1]
            + start[2] * self.tiles_per_dim_strides[2]
            + start[3] * self.tiles_per_dim_strides[3];
        if base_tile.saturating_add(tile_count) > self.nr_tiles {
            return Ok(false);
        }

        let total_bytes = tile_count.saturating_mul(self.tile_bytes);
        if self.direct_write_buffer.len() != total_bytes {
            self.direct_write_buffer.resize(total_bytes, 0);
        }
        let pack_started = Instant::now();
        {
            let tile_nelem = self.tile_nelem;
            let typed = tile_as_typed_mut::<T>(&mut self.direct_write_buffer);
            for channel in 0..shape[3] {
                for ty in 0..tiles_y {
                    for tx in 0..tiles_x {
                        let tile_number = channel * plane_tile_count + ty * tiles_x + tx;
                        let tile_base = tile_number * tile_nelem;
                        let src_x0 = tx * tile_x;
                        let src_y0 = ty * tile_y;
                        // Keep both sides of the C-to-Fortran transpose local;
                        // a whole-plane inner loop repeatedly evicts partially
                        // written cache lines. This is traversal only, not a
                        // different persisted tile shape or a scratch buffer.
                        const TRANSPOSE_EDGE: usize = 32;
                        for block_y in (0..tile_y).step_by(TRANSPOSE_EDGE) {
                            for block_x in (0..tile_x).step_by(TRANSPOSE_EDGE) {
                                let end_x = (block_x + TRANSPOSE_EDGE).min(tile_x);
                                let end_y = (block_y + TRANSPOSE_EDGE).min(tile_y);
                                for local_x in block_x..end_x {
                                    let tile_base_x = tile_base + local_x;
                                    let src_base = ((src_x0 + local_x) * shape[1] + src_y0)
                                        * shape[3]
                                        + channel;
                                    for local_y in block_y..end_y {
                                        typed[tile_base_x + local_y * tile_x] =
                                            data[src_base + local_y * shape[3]];
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.stats.direct_tile_pack_ns = self
            .stats
            .direct_tile_pack_ns
            .saturating_add(duration_nanos_usize(pack_started.elapsed()));
        if self.needs_swap {
            let swap_started = Instant::now();
            swap_bytes_inplace(&mut self.direct_write_buffer, self.swap_size);
            self.stats.direct_tile_swap_ns = self
                .stats
                .direct_tile_swap_ns
                .saturating_add(duration_nanos_usize(swap_started.elapsed()));
        }
        let file_pos = (self.file_offset + base_tile * self.file_tile_bytes) as u64;
        let lru = match &mut self.cache {
            TileCache::Lru(lru) => lru,
            TileCache::Flat(_) => return Ok(false),
        };
        let write_started = Instant::now();
        lru.file.seek(SeekFrom::Start(file_pos))?;
        lru.file.write_all(&self.direct_write_buffer)?;
        self.stats.direct_tile_write_ns = self
            .stats
            .direct_tile_write_ns
            .saturating_add(duration_nanos_usize(write_started.elapsed()));
        for tile_index in base_tile..base_tile + tile_count {
            self.tile_on_disk[tile_index] = true;
            lru.tile_to_slot[tile_index] = -1;
        }
        self.stats.direct_tile_write_calls = self.stats.direct_tile_write_calls.saturating_add(1);
        self.stats.direct_tile_write_tiles = self
            .stats
            .direct_tile_write_tiles
            .saturating_add(tile_count);
        self.stats.direct_tile_write_bytes = self
            .stats
            .direct_tile_write_bytes
            .saturating_add(total_bytes);
        Ok(true)
    }

    /// Writes full, tile-aligned spatial plane groups from Fortran-order input.
    pub fn put_aligned_fortran_order_tiles<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<bool, StorageError> {
        let ndim = self.cube_shape.len();
        if ndim != 4 || start.len() != ndim || shape.len() != ndim {
            return Ok(false);
        }
        if self.file_tile_bytes != self.tile_bytes
            || T::ELEM_SIZE != self.elem_size
            || self.tile_shape[2] != 1
            || self.tile_shape[3] != 1
            || shape[2] != 1
            || shape[3] == 0
            || start[0] != 0
            || start[1] != 0
            || start[2] != 0
            || self.tiles_per_dim[2] != 1
            || shape[0] != self.cube_shape[0]
            || shape[1] != self.cube_shape[1]
            || shape[2] != self.cube_shape[2]
            || !shape[0].is_multiple_of(self.tile_shape[0])
            || !shape[1].is_multiple_of(self.tile_shape[1])
        {
            return Ok(false);
        }
        for d in 0..ndim {
            if start[d].saturating_add(shape[d]) > self.cube_shape[d]
                || !start[d].is_multiple_of(self.tile_shape[d])
            {
                return Ok(false);
            }
        }
        if data.len() != shape.iter().product::<usize>() {
            return Err(StorageError::FormatMismatch(format!(
                "aligned tile write data length {} does not match shape {:?}",
                data.len(),
                shape
            )));
        }
        if !matches!(self.cache, TileCache::Lru(_)) {
            return Ok(false);
        }
        let lru_has_tiles = match &self.cache {
            TileCache::Lru(lru) => lru.used_slots > 0,
            TileCache::Flat(_) => false,
        };
        if lru_has_tiles {
            self.flush()?;
        }

        let tile_x = self.tile_shape[0];
        let tile_y = self.tile_shape[1];
        let tiles_x = shape[0] / tile_x;
        let tiles_y = shape[1] / tile_y;
        let plane_tile_count = tiles_x.saturating_mul(tiles_y);
        let tile_count = plane_tile_count.saturating_mul(shape[3]);
        let base_tile = (start[0] / tile_x) * self.tiles_per_dim_strides[0]
            + (start[1] / tile_y) * self.tiles_per_dim_strides[1]
            + start[2] * self.tiles_per_dim_strides[2]
            + start[3] * self.tiles_per_dim_strides[3];
        if base_tile.saturating_add(tile_count) > self.nr_tiles {
            return Ok(false);
        }

        let total_bytes = tile_count.saturating_mul(self.tile_bytes);
        let direct_source_is_file_order = tile_x == shape[0] && tile_y == shape[1];
        if !direct_source_is_file_order || self.needs_swap {
            if self.direct_write_buffer.len() != total_bytes {
                self.direct_write_buffer.resize(total_bytes, 0);
            }
            let pack_started = Instant::now();
            {
                let tile_nelem = self.tile_nelem;
                let plane_nelem = shape[0].saturating_mul(shape[1]).saturating_mul(shape[2]);
                let typed = tile_as_typed_mut::<T>(&mut self.direct_write_buffer);
                if tile_x == shape[0] && tile_y == shape[1] {
                    for channel in 0..shape[3] {
                        let src_base = channel * plane_nelem;
                        let tile_base = channel * tile_nelem;
                        typed[tile_base..tile_base + plane_nelem]
                            .copy_from_slice(&data[src_base..src_base + plane_nelem]);
                    }
                } else {
                    for channel in 0..shape[3] {
                        let channel_data_base = channel * plane_nelem;
                        for ty in 0..tiles_y {
                            for tx in 0..tiles_x {
                                let tile_number = channel * plane_tile_count + ty * tiles_x + tx;
                                let tile_base = tile_number * tile_nelem;
                                let src_x0 = tx * tile_x;
                                let src_y0 = ty * tile_y;
                                for local_y in 0..tile_y {
                                    let src_base =
                                        channel_data_base + src_x0 + (src_y0 + local_y) * shape[0];
                                    let tile_base_y = tile_base + local_y * tile_x;
                                    typed[tile_base_y..tile_base_y + tile_x]
                                        .copy_from_slice(&data[src_base..src_base + tile_x]);
                                }
                            }
                        }
                    }
                }
            }
            self.stats.direct_tile_pack_ns = self
                .stats
                .direct_tile_pack_ns
                .saturating_add(duration_nanos_usize(pack_started.elapsed()));
            if self.needs_swap {
                let swap_started = Instant::now();
                swap_bytes_inplace(&mut self.direct_write_buffer, self.swap_size);
                self.stats.direct_tile_swap_ns = self
                    .stats
                    .direct_tile_swap_ns
                    .saturating_add(duration_nanos_usize(swap_started.elapsed()));
            }
        }
        let file_pos = (self.file_offset + base_tile * self.file_tile_bytes) as u64;
        let lru = match &mut self.cache {
            TileCache::Lru(lru) => lru,
            TileCache::Flat(_) => return Ok(false),
        };
        let write_started = Instant::now();
        lru.file.seek(SeekFrom::Start(file_pos))?;
        if direct_source_is_file_order && !self.needs_swap {
            lru.file.write_all(tile_values_as_bytes(data))?;
        } else {
            lru.file.write_all(&self.direct_write_buffer)?;
        }
        self.stats.direct_tile_write_ns = self
            .stats
            .direct_tile_write_ns
            .saturating_add(duration_nanos_usize(write_started.elapsed()));
        for tile_index in base_tile..base_tile + tile_count {
            self.tile_on_disk[tile_index] = true;
            lru.tile_to_slot[tile_index] = -1;
        }
        self.stats.direct_tile_write_calls = self.stats.direct_tile_write_calls.saturating_add(1);
        self.stats.direct_tile_write_tiles = self
            .stats
            .direct_tile_write_tiles
            .saturating_add(tile_count);
        self.stats.direct_tile_write_bytes = self
            .stats
            .direct_tile_write_bytes
            .saturating_add(total_bytes);
        Ok(true)
    }

    /// Writes a rectangular slice of C-order (row-major) data into tiles.
    pub fn put_slice_c_order<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        self.stats.put_slice_c_order_calls = self.stats.put_slice_c_order_calls.saturating_add(1);
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        let c_strides = c_order_strides(shape);

        // Pick inner axis: axis 0 gives contiguous tile writes (Fortran order),
        // but axis 0's C-stride may be too large at high resolutions.
        // Use axis 0 when the read footprint fits in L1 (~64KB); otherwise
        // fall back to the first non-singleton axis from the end (C-contiguous).
        let l1_threshold = 64 * 1024 / T::ELEM_SIZE; // ~64KB in elements
        let inner_axis = if c_strides[0] * self.tile_shape[0] <= l1_threshold {
            0
        } else {
            // Find the C-contiguous non-singleton axis (walk from end).
            let mut ia = ndim - 1;
            while ia > 0 && shape[ia] <= 1 {
                ia -= 1;
            }
            ia
        };
        let inner_c_stride = c_strides[inner_axis];
        let inner_t_stride = self.tile_strides[inner_axis];

        // Copy self fields into stack arrays before the tile loop to avoid
        // borrow conflicts with get_cached_tile_mut.
        let mut ts = [0usize; MAX_NDIM]; // tile_strides
        let mut tpds = [0usize; MAX_NDIM]; // tiles_per_dim_strides
        let mut tsh = [0usize; MAX_NDIM]; // tile_shape
        let mut csh = [0usize; MAX_NDIM]; // cube_shape
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start_pos = [0usize; MAX_NDIM];
        let mut tile_end_pos = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];
        let mut outer_axes = [0usize; MAX_NDIM];
        let mut outer_dims = [0usize; MAX_NDIM];
        let mut n_outer = 0usize;

        for d in 0..ndim {
            tile_start_pos[d] = start[d] / tsh[d];
            tile_end_pos[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
            if d != inner_axis {
                outer_axes[n_outer] = d;
                n_outer += 1;
            }
        }

        let mut tile_pos = tile_start_pos;
        let mut tile_visits = 0usize;
        let mut copied_elements = 0usize;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                tile_visits = tile_visits.saturating_add(1);
                let inner_count = overlap_hi[inner_axis] - overlap_lo[inner_axis];
                let base_tile_off =
                    (overlap_lo[inner_axis] - tile_origin[inner_axis]) * inner_t_stride;
                let base_c_off = (overlap_lo[inner_axis] - start[inner_axis]) * inner_c_stride;

                let mut outer_total = 1usize;
                for oi in 0..n_outer {
                    outer_dims[oi] = overlap_hi[outer_axes[oi]] - overlap_lo[outer_axes[oi]];
                    outer_total *= outer_dims[oi];
                }
                copied_elements =
                    copied_elements.saturating_add(inner_count.saturating_mul(outer_total));

                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut c_off = base_c_off;
                    for oi in 0..n_outer {
                        let d = outer_axes[oi];
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[oi]) * ts[d];
                        c_off += (overlap_lo[d] - start[d] + outer_coord[oi]) * c_strides[d];
                    }

                    if inner_c_stride == 1 && inner_t_stride == 1 {
                        tile_data[tile_off..tile_off + inner_count]
                            .copy_from_slice(&data[c_off..c_off + inner_count]);
                    } else if inner_t_stride == 1 {
                        unsafe {
                            let tp = tile_data.as_mut_ptr().add(tile_off);
                            let dp = data.as_ptr().add(c_off);
                            for i in 0..inner_count {
                                *tp.add(i) = *dp.add(i * inner_c_stride);
                            }
                        }
                    } else if inner_c_stride == 1 {
                        unsafe {
                            let tp = tile_data.as_mut_ptr().add(tile_off);
                            let dp = data.as_ptr().add(c_off);
                            for i in 0..inner_count {
                                *tp.add(i * inner_t_stride) = *dp.add(i);
                            }
                        }
                    } else {
                        for i in 0..inner_count {
                            tile_data[tile_off + i * inner_t_stride] =
                                data[c_off + i * inner_c_stride];
                        }
                    }

                    let mut carry = true;
                    for oi in 0..n_outer {
                        if carry {
                            outer_coord[oi] += 1;
                            if outer_coord[oi] < outer_dims[oi] {
                                carry = false;
                            } else {
                                outer_coord[oi] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end_pos[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start_pos[d];
                    }
                }
            }
            if carry {
                break;
            }
        }
        self.stats.put_slice_tile_visits =
            self.stats.put_slice_tile_visits.saturating_add(tile_visits);
        self.stats.put_slice_copied_elements = self
            .stats
            .put_slice_copied_elements
            .saturating_add(copied_elements);
        Ok(())
    }

    /// Writes a rectangular slice of Fortran-order data into the tile file.
    pub fn put_slice_fortran<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        self.stats.put_slice_fortran_calls = self.stats.put_slice_fortran_calls.saturating_add(1);
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        let input_strides = fortran_order_strides(shape);

        // Copy self fields to stack arrays to avoid borrow conflicts.
        let mut ts = [0usize; MAX_NDIM];
        let mut tpds = [0usize; MAX_NDIM];
        let mut tsh = [0usize; MAX_NDIM];
        let mut csh = [0usize; MAX_NDIM];
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start = [0usize; MAX_NDIM];
        let mut tile_end = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];

        for d in 0..ndim {
            tile_start[d] = start[d] / tsh[d];
            tile_end[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
        }

        let mut tile_pos = tile_start;
        let mut tile_visits = 0usize;
        let mut copied_elements = 0usize;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                tile_visits = tile_visits.saturating_add(1);
                let inner_count = overlap_hi[0] - overlap_lo[0];
                let base_tile_off = overlap_lo[0] - tile_origin[0];
                let base_input_off = (overlap_lo[0] - start[0]) * input_strides[0];

                let mut outer_dims = [0usize; MAX_NDIM];
                let mut outer_total = 1usize;
                for d in 1..ndim {
                    outer_dims[d - 1] = overlap_hi[d] - overlap_lo[d];
                    outer_total *= outer_dims[d - 1];
                }
                copied_elements =
                    copied_elements.saturating_add(inner_count.saturating_mul(outer_total));

                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut input_off = base_input_off;
                    for d in 1..ndim {
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[d - 1]) * ts[d];
                        input_off +=
                            (overlap_lo[d] - start[d] + outer_coord[d - 1]) * input_strides[d];
                    }

                    tile_data[tile_off..tile_off + inner_count]
                        .copy_from_slice(&data[input_off..input_off + inner_count]);

                    let mut carry = true;
                    for d in 0..ndim.saturating_sub(1) {
                        if carry {
                            outer_coord[d] += 1;
                            if outer_coord[d] < outer_dims[d] {
                                carry = false;
                            } else {
                                outer_coord[d] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }
        self.stats.put_slice_tile_visits =
            self.stats.put_slice_tile_visits.saturating_add(tile_visits);
        self.stats.put_slice_copied_elements = self
            .stats
            .put_slice_copied_elements
            .saturating_add(copied_elements);

        Ok(())
    }

    /// Reads a rectangular slice from the tile file.
    ///
    /// Returns a Fortran-order `ArrayD<T>` of the given `shape`.
    pub fn get_slice<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
    ) -> Result<ArrayD<T>, StorageError> {
        let nelem = shape
            .iter()
            .try_fold(1usize, |n, &dim| n.checked_mul(dim))
            .ok_or_else(|| StorageError::FormatMismatch("slice size overflow".into()))?;
        let mut result = vec![T::default(); nelem];
        self.get_slice_into(start, shape, &mut result)?;
        ArrayD::from_shape_vec(IxDyn(shape).f(), result)
            .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))
    }

    /// Read a rectangular slice into caller-owned Fortran-order storage.
    /// The only tile-sized staging belongs to this storage handle's cache.
    pub fn get_slice_into<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
        result: &mut [T],
    ) -> Result<(), StorageError> {
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        if start.len() != ndim
            || shape.len() != ndim
            || shape.contains(&0)
            || start
                .iter()
                .zip(shape)
                .zip(&self.cube_shape)
                .any(|((&offset, &len), &extent)| {
                    offset.checked_add(len).is_none_or(|end| end > extent)
                })
            || shape.iter().try_fold(1usize, |n, &dim| n.checked_mul(dim)) != Some(result.len())
        {
            return Err(StorageError::FormatMismatch(
                "invalid tiled slice bounds or destination".into(),
            ));
        }
        let result_strides = fortran_order_strides(shape);

        let inner_axis = {
            let (best, _) = shape[..ndim]
                .iter()
                .enumerate()
                .max_by_key(|&(_, &ext)| ext)
                .unwrap();
            if shape[best] > shape[0] { best } else { 0 }
        };
        let inner_t_stride = self.tile_strides[inner_axis];
        let inner_r_stride = result_strides[inner_axis];

        let mut ts = [0usize; MAX_NDIM];
        let mut tpds = [0usize; MAX_NDIM];
        let mut tsh = [0usize; MAX_NDIM];
        let mut csh = [0usize; MAX_NDIM];
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start = [0usize; MAX_NDIM];
        let mut tile_end_pos = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];
        let mut outer_axes = [0usize; MAX_NDIM];
        let mut outer_dims = [0usize; MAX_NDIM];
        let mut n_outer = 0usize;

        for d in 0..ndim {
            tile_start[d] = start[d] / tsh[d];
            tile_end_pos[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
            if d != inner_axis {
                outer_axes[n_outer] = d;
                n_outer += 1;
            }
        }

        let mut tile_pos = tile_start;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let inner_count = overlap_hi[inner_axis] - overlap_lo[inner_axis];
                let base_tile_off =
                    (overlap_lo[inner_axis] - tile_origin[inner_axis]) * inner_t_stride;
                let base_result_off = (overlap_lo[inner_axis] - start[inner_axis]) * inner_r_stride;

                let mut outer_total = 1usize;
                for oi in 0..n_outer {
                    outer_dims[oi] = overlap_hi[outer_axes[oi]] - overlap_lo[outer_axes[oi]];
                    outer_total *= outer_dims[oi];
                }

                let tile_bytes = self.get_cached_tile(tile_index)?;
                let tile_data = tile_as_typed::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut result_off = base_result_off;
                    for oi in 0..n_outer {
                        let d = outer_axes[oi];
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[oi]) * ts[d];
                        result_off +=
                            (overlap_lo[d] - start[d] + outer_coord[oi]) * result_strides[d];
                    }

                    if inner_t_stride == 1 && inner_r_stride == 1 {
                        result[result_off..result_off + inner_count]
                            .copy_from_slice(&tile_data[tile_off..tile_off + inner_count]);
                    } else {
                        unsafe {
                            let rp = result.as_mut_ptr().add(result_off);
                            let tp = tile_data.as_ptr().add(tile_off);
                            for i in 0..inner_count {
                                *rp.add(i * inner_r_stride) = *tp.add(i * inner_t_stride);
                            }
                        }
                    }

                    let mut carry = true;
                    for oi in 0..n_outer {
                        if carry {
                            outer_coord[oi] += 1;
                            if outer_coord[oi] < outer_dims[oi] {
                                carry = false;
                            } else {
                                outer_coord[oi] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end_pos[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }

        Ok(())
    }

    /// Reads the full cube as a Fortran-order `ArrayD<T>`.
    pub fn get_all<T: TilePixel>(&mut self) -> Result<ArrayD<T>, StorageError> {
        let start = vec![0; self.cube_shape.len()];
        let shape = self.cube_shape.clone();
        self.get_slice::<T>(&start, &shape)
    }
}
