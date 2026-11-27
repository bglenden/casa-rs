// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct TileReadSessionFile {
    pub(super) file: std::fs::File,
    pub(super) file_position: u64,
    pub(super) read_ahead_start: u64,
    pub(super) read_ahead: Vec<u8>,
}

#[derive(Default)]
pub(super) struct TileReadSession {
    pub(super) table_id: Option<u64>,
    pub(super) files: std::collections::HashMap<u32, TileReadSessionFile>,
    pub(super) recent_tiles: Vec<(SharedTileKey, Arc<[u8]>)>,
}

impl TileReadSession {
    fn table_id(&mut self, table_path: &Path) -> u64 {
        if let Some(id) = self.table_id {
            return id;
        }
        let mut cache = SHARED_TILE_CACHE
            .lock()
            .expect("shared tile cache lock poisoned");
        let id = cache.table_id(table_path);
        self.table_id = Some(id);
        id
    }

    fn file(
        &mut self,
        table_path: &Path,
        dm_seq_nr: u32,
        file_seq_nr: u32,
    ) -> Result<&mut TileReadSessionFile, StorageError> {
        match self.files.entry(file_seq_nr) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
                let file = OpenOptions::new().read(true).open(&path).map_err(|err| {
                    StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
                })?;
                Ok(entry.insert(TileReadSessionFile {
                    file,
                    file_position: 0,
                    read_ahead_start: 0,
                    read_ahead: Vec::new(),
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn read_tile_streamed(
        &mut self,
        table_path: &Path,
        dm_seq_nr: u32,
        file_seq_nr: u32,
        file_pos: u64,
        dst: &mut [u8],
        file_tile_bytes: usize,
        useful_read_ahead_bytes: usize,
        dt: CasacoreDataType,
        tile_nelem: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<(), StorageError> {
        let session_file = self.file(table_path, dm_seq_nr, file_seq_nr)?;
        let file_end = file_pos
            .checked_add(file_tile_bytes as u64)
            .ok_or_else(|| StorageError::FormatMismatch("tile read offset overflow".to_string()))?;
        let read_ahead_end = session_file
            .read_ahead_start
            .saturating_add(session_file.read_ahead.len() as u64);
        if file_pos < session_file.read_ahead_start || file_end > read_ahead_end {
            if session_file.file_position != file_pos {
                session_file.file.seek(SeekFrom::Start(file_pos))?;
            }
            // Prefetch only the contiguous selected tile run. The next row
            // tile may start at a different channel or may not be selected.
            let read_ahead_bytes = STREAMING_TILED_READ_AHEAD_BYTES
                .min(useful_read_ahead_bytes)
                .max(file_tile_bytes);
            session_file.read_ahead.resize(read_ahead_bytes, 0);
            let mut bytes_read = 0usize;
            while bytes_read < read_ahead_bytes {
                let read = session_file
                    .file
                    .read(&mut session_file.read_ahead[bytes_read..])?;
                if read == 0 {
                    break;
                }
                #[cfg(test)]
                STREAMED_READ_COUNTS.with(|counts| {
                    let (calls, bytes) = counts.get();
                    counts.set((calls + 1, bytes + read));
                });
                bytes_read += read;
            }
            session_file.read_ahead.truncate(bytes_read);
            session_file.read_ahead_start = file_pos;
            session_file.file_position = file_pos + bytes_read as u64;
            if bytes_read < file_tile_bytes {
                return Err(StorageError::FormatMismatch(format!(
                    "short tiled read at byte {file_pos}: expected {file_tile_bytes} bytes, found {bytes_read}"
                )));
            }
        }
        let buffered_start = usize::try_from(file_pos - session_file.read_ahead_start)
            .map_err(|_| StorageError::FormatMismatch("tile buffer offset overflow".to_string()))?;
        let buffered = &session_file.read_ahead[buffered_start..buffered_start + file_tile_bytes];
        if file_tile_bytes == dst.len() {
            dst.copy_from_slice(buffered);
            if needs_swap {
                swap_bytes_inplace(dst, swap_size);
            }
        } else {
            debug_assert_eq!(dt, CasacoreDataType::TpBool);
            debug_assert_eq!(dst.len(), tile_nelem);
            for (pixel, value) in dst.iter_mut().enumerate() {
                *value = u8::from((buffered[pixel / 8] & (1 << (pixel % 8))) != 0);
            }
        }
        Ok(())
    }

    fn recent_tile(&self, key: &SharedTileKey) -> Option<Arc<[u8]>> {
        self.recent_tiles
            .iter()
            .find_map(|(recent_key, data)| (recent_key == key).then(|| Arc::clone(data)))
    }

    fn remember_tile(&mut self, key: SharedTileKey, data: Arc<[u8]>) {
        if let Some(position) = self
            .recent_tiles
            .iter()
            .position(|(recent_key, _)| recent_key == &key)
        {
            let entry = self.recent_tiles.remove(position);
            self.recent_tiles.push(entry);
            return;
        }
        if self.recent_tiles.len() >= TILE_READ_SESSION_RECENT_TILE_COUNT {
            self.recent_tiles.remove(0);
        }
        self.recent_tiles.push((key, data));
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_streamed_column_tile(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube: &TsmCubeInfo,
    bucket_size: usize,
    col_offset_in_tile: usize,
    dt: CasacoreDataType,
    tile_index: usize,
    contiguous_tiles: usize,
    session: &mut TileReadSession,
    tile: &mut Vec<u8>,
) -> Result<(), StorageError> {
    let tile_nelem: usize = cube.tile_shape.iter().product();
    let tile_bytes = tile_nelem * tile_element_size(dt);
    let file_tile_bytes = tile_storage_bytes(dt, tile_nelem);
    let useful_read_ahead_bytes = read_ahead_span(contiguous_tiles, bucket_size, file_tile_bytes)
        .ok_or_else(|| {
        StorageError::FormatMismatch("selected tile run size overflow".into())
    })?;
    let swap_size = match dt {
        CasacoreDataType::TpComplex => 4,
        CasacoreDataType::TpDComplex => 8,
        _ => tile_element_size(dt),
    };
    let needs_swap = header.big_endian != cfg!(target_endian = "big");
    let file_pos =
        (cube.file_offset as usize + tile_index * bucket_size + col_offset_in_tile) as u64;
    tile.resize(tile_bytes, 0);
    session.read_tile_streamed(
        table_path,
        dm_seq_nr,
        cube.file_seq_nr as u32,
        file_pos,
        tile,
        file_tile_bytes,
        useful_read_ahead_bytes,
        dt,
        tile_nelem,
        needs_swap,
        swap_size,
    )
}

pub(super) fn read_ahead_span(
    tiles: usize,
    bucket_bytes: usize,
    column_tile_bytes: usize,
) -> Option<usize> {
    let additional_tiles = tiles.checked_sub(1)?;
    // A bucket can contain large tiles from other columns. Read ahead only
    // when the buffer can reach the end of a second tile of this column.
    if bucket_bytes > STREAMING_TILED_READ_AHEAD_BYTES.saturating_sub(column_tile_bytes) {
        return Some(column_tile_bytes);
    }
    additional_tiles
        .checked_mul(bucket_bytes)?
        .checked_add(column_tile_bytes)
}

/// The byte order of a shared tile's pixels: [`load_shared_column_tile`]
/// and [`load_streamed_column_tile`] swap a tile to the host's order as
/// they read it, so its pixels decode in that order, not the manager's.
pub(super) const SHARED_TILE_BIG_ENDIAN: bool = cfg!(target_endian = "big");

#[allow(clippy::too_many_arguments)]
pub(super) fn load_shared_column_tile(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    bucket_size: usize,
    col_offset_in_tile: usize,
    dt: CasacoreDataType,
    tile_index: usize,
    session: &mut TileReadSession,
) -> Result<Arc<[u8]>, StorageError> {
    let table_id = session.table_id(table_path);
    let tile_nelem: usize = cube.tile_shape.iter().product();
    let key = SharedTileKey {
        table_id,
        dm_seq_nr,
        cube_idx,
        target_col_idx,
        tile_index,
        is_bool: dt == CasacoreDataType::TpBool,
        single_pixel: tile_nelem <= 1,
    };
    if let Some(hit) = session.recent_tile(&key) {
        return Ok(hit);
    }
    if let Some(hit) = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .get(&key)
    {
        session.remember_tile(key, Arc::clone(&hit));
        return Ok(hit);
    }

    let tile_bytes = tile_nelem * tile_element_size(dt);
    let file_tile_bytes = tile_storage_bytes(dt, tile_nelem);
    let swap_size = match dt {
        CasacoreDataType::TpComplex => 4,
        CasacoreDataType::TpDComplex => 8,
        _ => tile_element_size(dt),
    };
    let needs_swap = header.big_endian != cfg!(target_endian = "big");
    let file_pos =
        (cube.file_offset as usize + tile_index * bucket_size + col_offset_in_tile) as u64;
    let session_file = session.file(table_path, dm_seq_nr, cube.file_seq_nr as u32)?;
    let mut tile = vec![0u8; tile_bytes];
    read_tiled_file_tile(
        &mut session_file.file,
        file_pos,
        &mut tile,
        file_tile_bytes,
        dt,
        tile_nelem,
        needs_swap,
        swap_size,
    )?;
    session_file.file_position = file_pos + file_tile_bytes as u64;
    session_file.read_ahead.clear();
    let data: Arc<[u8]> = Arc::from(tile);
    let mut cache = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned");
    if let Some(hit) = cache.get(&key) {
        session.remember_tile(key, Arc::clone(&hit));
        return Ok(hit);
    }
    let cached = cache.insert(key.clone(), data);
    session.remember_tile(key, Arc::clone(&cached));
    Ok(cached)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn reconstruct_cube_column_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    dt: CasacoreDataType,
    elem_size: usize,
    session: &mut TileReadSession,
) -> Result<Vec<u8>, StorageError> {
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);
    let ndim = cube.cube_shape.len();
    let tiles_per_dim: Vec<usize> = (0..ndim)
        .map(|i| cube.cube_shape[i].div_ceil(cube.tile_shape[i]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let cube_nelem: usize = cube.cube_shape.iter().product();
    let mut result = vec![0u8; cube_nelem * elem_size];

    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..ndim)
            .map(|d| tile_pos[d] * cube.tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..ndim)
            .map(|d| std::cmp::min(cube.tile_shape[d], cube.cube_shape[d] - cube_start[d]))
            .collect();
        let tile = load_shared_column_tile(
            table_path,
            dm_seq_nr,
            header,
            cube_idx,
            cube,
            target_col_idx,
            bucket_size,
            col_offsets[target_col_idx],
            dt,
            tile_idx,
            session,
        )?;
        copy_tile_to_cube(
            tile.as_ref(),
            &cube.tile_shape,
            &mut result,
            &cube.cube_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn decode_selected_cube_row_2d_channel_range_typed_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected: SelectedCubeRow,
    channel_start: usize,
    channel_count: usize,
    corr_count: usize,
    tile_corr_count: usize,
    tile_channel_count: usize,
    session: &mut TileReadSession,
) -> Result<Option<ArrayValue>, StorageError> {
    let channel_end = channel_start + channel_count;
    let cell_ndim = cube.cube_shape.len().saturating_sub(1);
    let cell_shape = &cube.cube_shape[..cell_ndim];
    let cell_tile_shape = &cube.tile_shape[..cell_ndim];
    let row_tile = selected.pos_in_cube / cube.tile_shape[cell_ndim];
    let row_in_tile = selected.pos_in_cube % cube.tile_shape[cell_ndim];
    let row_tile_nelem: usize = cell_tile_shape.iter().product();
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&shape, &tile)| shape.div_ceil(tile))
        .collect();
    let tile_grid_strides = fortran_order_strides(&tiles_per_dim);
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);
    let first_channel_tile = channel_start / tile_channel_count;
    let last_channel_tile = (channel_end.saturating_sub(1)) / tile_channel_count;

    match dt {
        CasacoreDataType::TpComplex => {
            let mut values = vec![Complex32::new(0.0, 0.0); corr_count * channel_count];
            for channel_tile in first_channel_tile..=last_channel_tile {
                let tile_channel_start = channel_tile * tile_channel_count;
                let actual_tile_channels =
                    std::cmp::min(tile_channel_count, cell_shape[1] - tile_channel_start);
                let overlap_start = std::cmp::max(channel_start, tile_channel_start);
                let overlap_end =
                    std::cmp::min(channel_end, tile_channel_start + actual_tile_channels);
                if overlap_start >= overlap_end {
                    continue;
                }

                let full_tile_pos = [0usize, channel_tile, row_tile];
                let tile_index: usize = full_tile_pos
                    .iter()
                    .zip(tile_grid_strides.iter())
                    .map(|(pos, stride)| pos * stride)
                    .sum();
                let tile = load_shared_column_tile(
                    table_path,
                    dm_seq_nr,
                    header,
                    cube_idx,
                    cube,
                    target_col_idx,
                    bucket_size,
                    col_offsets[target_col_idx],
                    dt,
                    tile_index,
                    session,
                )?;
                let src_start = row_in_tile * row_tile_nelem * elem_size;
                let src_end = src_start + row_tile_nelem * elem_size;
                let tile_row = &tile[src_start..src_end];

                for channel in overlap_start..overlap_end {
                    let src_channel = channel - tile_channel_start;
                    let dst_channel = channel - channel_start;
                    for corr in 0..corr_count {
                        let src_elem = corr + src_channel * tile_corr_count;
                        let dst_elem = corr + dst_channel * corr_count;
                        let src_byte = src_elem * elem_size;
                        values[dst_elem] = decode_complex32_scalar(
                            &tile_row[src_byte..src_byte + elem_size],
                            header.big_endian,
                        );
                    }
                }
            }
            ArrayD::from_shape_vec(IxDyn(&[corr_count, channel_count]).f(), values)
                .map(ArrayValue::Complex32)
                .map(Some)
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))
        }
        CasacoreDataType::TpBool => {
            let mut values = vec![false; corr_count * channel_count];
            for channel_tile in first_channel_tile..=last_channel_tile {
                let tile_channel_start = channel_tile * tile_channel_count;
                let actual_tile_channels =
                    std::cmp::min(tile_channel_count, cell_shape[1] - tile_channel_start);
                let overlap_start = std::cmp::max(channel_start, tile_channel_start);
                let overlap_end =
                    std::cmp::min(channel_end, tile_channel_start + actual_tile_channels);
                if overlap_start >= overlap_end {
                    continue;
                }

                let full_tile_pos = [0usize, channel_tile, row_tile];
                let tile_index: usize = full_tile_pos
                    .iter()
                    .zip(tile_grid_strides.iter())
                    .map(|(pos, stride)| pos * stride)
                    .sum();
                let tile = load_shared_column_tile(
                    table_path,
                    dm_seq_nr,
                    header,
                    cube_idx,
                    cube,
                    target_col_idx,
                    bucket_size,
                    col_offsets[target_col_idx],
                    dt,
                    tile_index,
                    session,
                )?;
                let src_start = row_in_tile * row_tile_nelem * elem_size;
                let src_end = src_start + row_tile_nelem * elem_size;
                let tile_row = &tile[src_start..src_end];

                for channel in overlap_start..overlap_end {
                    let src_channel = channel - tile_channel_start;
                    let dst_channel = channel - channel_start;
                    for corr in 0..corr_count {
                        let src_elem = corr + src_channel * tile_corr_count;
                        let dst_elem = corr + dst_channel * corr_count;
                        values[dst_elem] = tile_row[src_elem * elem_size] != 0;
                    }
                }
            }
            ArrayD::from_shape_vec(IxDyn(&[corr_count, channel_count]).f(), values)
                .map(ArrayValue::Bool)
                .map(Some)
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "typed 2-D channel-range tiled read does not support {other:?} for column {}",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn decode_tiled_cell_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    session: &mut TileReadSession,
) -> Result<Option<ArrayValue>, StorageError> {
    let raw = reconstruct_cube_column_from_shared_tiles(
        table_path,
        dm_seq_nr,
        header,
        cube_idx,
        cube,
        target_col_idx,
        dt,
        elem_size,
        session,
    )?;
    match decode_array_value(&raw, &cube.cube_shape, dt, SHARED_TILE_BIG_ENDIAN)? {
        Value::Array(array) => Ok(Some(array)),
        other => Err(StorageError::FormatMismatch(format!(
            "tiled cell row '{}' expected array value, found {:?}",
            col_desc.col_name,
            other.kind()
        ))),
    }
}
