// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) type SparseArrayRowValues<'a> = Vec<(usize, Option<&'a ArrayValue>)>;

#[derive(Clone, Copy, Debug)]
pub(super) struct SparseTiledCubeRowPatch {
    pub(super) global_row_idx: usize,
    pub(super) pos_in_cube: usize,
}

pub(crate) fn save_tiled_columns_sparse_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_type_name: &str,
    col_descs: &[ColumnDescContents],
    table_desc: &TableDescContents,
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    if changed_rows.is_empty() {
        return Ok(true);
    }
    if col_descs.len() != sparse_columns.len() {
        return Ok(false);
    }
    match dm_type_name {
        "TiledColumnStMan" => save_tiled_column_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            table_desc,
            sparse_columns,
            changed_rows,
        ),
        "TiledCellStMan" => save_tiled_cell_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            table_desc,
            sparse_columns,
            changed_rows,
        ),
        "TiledDataStMan" => save_tiled_data_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            table_desc,
            sparse_columns,
            changed_rows,
        ),
        "TiledShapeStMan" => save_tiled_shape_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            table_desc,
            sparse_columns,
            changed_rows,
        ),
        _ => Ok(false),
    }
}

pub(super) fn cube_with_single_row_axis(cube: &TsmCubeInfo) -> TsmCubeInfo {
    let mut cube_with_row_axis = cube.clone();
    cube_with_row_axis.cube_shape.push(1);
    cube_with_row_axis.tile_shape.push(1);
    cube_with_row_axis
}

pub(super) fn save_tiled_column_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    table_desc: &TableDescContents,
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Column { .. })
        || header.col_data_types.len() != col_descs.len()
    {
        return Ok(false);
    }
    let Some(cube) = header.cubes.first() else {
        return Ok(false);
    };
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(false);
    }
    let row_axis = cube.cube_shape.len() - 1;
    let row_count = cube.cube_shape[row_axis];
    let row_patches: Vec<_> = changed_rows
        .iter()
        .copied()
        .filter(|&row_idx| row_idx < row_count)
        .map(|row_idx| SparseTiledCubeRowPatch {
            global_row_idx: row_idx,
            pos_in_cube: row_idx,
        })
        .collect();
    if row_patches.is_empty() {
        return Ok(true);
    }
    let bound: Vec<&ColumnDescContents> = col_descs.iter().collect();
    let tiles = tile_columns(
        &variant,
        Cow::Borrowed(&header),
        &bound,
        table_desc,
        rows_reached(changed_rows),
    )?;
    patch_tiled_cube_sparse_rows(
        table_path,
        dm_seq_nr,
        col_descs,
        sparse_columns,
        header.big_endian,
        &tiles.header.col_data_types,
        &tiles.tile_index,
        cube,
        &row_patches,
    )?;
    Ok(true)
}

pub(super) fn save_tiled_cell_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    table_desc: &TableDescContents,
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Cell { .. })
        || header.col_data_types.len() != col_descs.len()
    {
        return Ok(false);
    }
    let bound: Vec<&ColumnDescContents> = col_descs.iter().collect();
    let tiles = tile_columns(
        &variant,
        Cow::Borrowed(&header),
        &bound,
        table_desc,
        rows_reached(changed_rows),
    )?;

    for &row_idx in changed_rows {
        let Some(cube) = header.cubes.get(row_idx) else {
            continue;
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        let cube_with_row_axis = cube_with_single_row_axis(cube);
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &tiles.header.col_data_types,
            &tiles.tile_index,
            &cube_with_row_axis,
            &[SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube: 0,
            }],
        )?;
    }

    Ok(true)
}

pub(super) fn save_tiled_data_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    table_desc: &TableDescContents,
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if header.col_data_types.len() != col_descs.len() {
        return Ok(false);
    }
    let bound: Vec<&ColumnDescContents> = col_descs.iter().collect();
    let tiles = tile_columns(
        &variant,
        Cow::Borrowed(&header),
        &bound,
        table_desc,
        rows_reached(changed_rows),
    )?;
    let TiledVariant::Data {
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    if row_map.is_empty() {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue;
                }
                i - 1
            }
        };
        let cube_idx = cube_map[chunk] as usize;
        if cube_idx >= header.cubes.len() {
            continue;
        }
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &tiles.header.col_data_types,
            &tiles.tile_index,
            cube,
            &row_patches,
        )?;
    }

    Ok(true)
}

pub(super) fn save_tiled_shape_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    table_desc: &TableDescContents,
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if header.col_data_types.len() != col_descs.len() {
        return Ok(false);
    }
    let bound: Vec<&ColumnDescContents> = col_descs.iter().collect();
    let tiles = tile_columns(
        &variant,
        Cow::Borrowed(&header),
        &bound,
        table_desc,
        rows_reached(changed_rows),
    )?;
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    let n_intervals = nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        let interval = row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        let cube_idx = if interval < n_intervals {
            cube_map[interval] as usize
        } else {
            0
        };
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            // A tile patch cannot define a cell without an existing cube.
            // The caller rebuilds the manager and its row map in that case.
            if sparse_columns.iter().flatten().any(|rows| {
                rows.binary_search_by_key(&row_idx, |(row, _)| *row)
                    .is_ok_and(|index| rows[index].1.is_some())
            }) {
                return Ok(false);
            }
            continue;
        }
        let diff = row_map[interval] as usize - row_idx;
        let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &tiles.header.col_data_types,
            &tiles.tile_index,
            cube,
            &row_patches,
        )?;
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SingleColumnCubeRowPatch {
    pub(super) global_row_idx: usize,
    pub(super) pos_in_cube: usize,
}

pub(crate) fn save_tiled_single_column_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    options: SingleColumnTiledSaveOptions<'_>,
) -> Result<bool, StorageError> {
    if changed_rows.is_empty() {
        return Ok(true);
    }
    match options.dm_type_name {
        "TiledColumnStMan" => save_single_column_tiled_column_rows_in_place(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            changed_rows,
            options.big_endian,
        ),
        "TiledShapeStMan" => save_single_column_tiled_shape_rows_in_place(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            changed_rows,
            options.big_endian,
        ),
        _ => Ok(false),
    }
}

pub(super) fn save_single_column_tiled_column_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Column { .. }) || header.col_data_types.len() != 1 {
        return Ok(false);
    }
    let Some(cube) = header.cubes.first() else {
        return Ok(false);
    };
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(false);
    }
    let row_axis = cube.cube_shape.len() - 1;
    let row_count = cube.cube_shape[row_axis];
    let row_patches: Vec<_> = changed_rows
        .iter()
        .copied()
        .filter(|&row_idx| row_idx < values.len() && row_idx < row_count)
        .map(|row_idx| SingleColumnCubeRowPatch {
            global_row_idx: row_idx,
            pos_in_cube: row_idx,
        })
        .collect();
    if row_patches.is_empty() {
        return Ok(true);
    }
    patch_single_column_tiled_cube_rows(
        table_path,
        dm_seq_nr,
        col_desc,
        values,
        big_endian,
        header.col_data_types[0],
        cube,
        &row_patches,
    )?;
    Ok(true)
}

pub(super) fn save_single_column_tiled_shape_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    if header.col_data_types.len() != 1 {
        return Ok(false);
    }
    let n_intervals = nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SingleColumnCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        if row_idx >= values.len() {
            continue;
        }
        let interval = row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        let cube_idx = if interval < n_intervals {
            cube_map[interval] as usize
        } else {
            0
        };
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            if values[row_idx].is_some() {
                return Ok(false);
            }
            continue;
        }
        let diff = row_map[interval] as usize - row_idx;
        let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SingleColumnCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_single_column_tiled_cube_rows(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            big_endian,
            header.col_data_types[0],
            cube,
            &row_patches,
        )?;
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn patch_single_column_tiled_cube_rows(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    col_data_type: CasacoreDataType,
    cube: &TsmCubeInfo,
    row_patches: &[SingleColumnCubeRowPatch],
) -> Result<(), StorageError> {
    let ndim = cube.cube_shape.len();
    if ndim == 0 {
        return Ok(());
    }
    let row_axis = ndim - 1;
    if row_axis == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "single-column tiled sparse save requires array cells for {}",
            col_desc.col_name
        )));
    }

    let cell_shape = &cube.cube_shape[..row_axis];
    let tile_cell_shape = &cube.tile_shape[..row_axis];
    let row_tile_len = cube.tile_shape[row_axis].max(1);
    let row_block_pixels: usize = tile_cell_shape.iter().product();
    let elem_size = tile_element_size(col_data_type);
    let expected_row_bytes = cell_shape.iter().product::<usize>() * elem_size;
    let row_block_bytes = row_block_pixels * elem_size;
    let nrpixels: usize = cube.tile_shape.iter().product();
    let (bucket_size, col_offsets) =
        compute_tile_layout(std::slice::from_ref(&col_data_type), &cube.tile_shape);
    let col_offset = col_offsets[0];
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&cube_len, &tile_len)| cube_len.div_ceil(tile_len))
        .collect();
    let cell_tiles_per_dim = &tiles_per_dim[..row_axis];
    let cell_tile_total = cell_tiles_per_dim.iter().product::<usize>().max(1);

    let mut patches_by_row_tile: std::collections::BTreeMap<usize, Vec<SingleColumnCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &patch in row_patches {
        patches_by_row_tile
            .entry(patch.pos_in_cube / row_tile_len)
            .or_default()
            .push(patch);
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let mut file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
    let tile_storage_len = tile_storage_bytes(col_data_type, nrpixels);
    for (row_tile_idx, row_tile_patches) in patches_by_row_tile {
        if row_tile_idx >= tiles_per_dim[row_axis] {
            continue;
        }
        for cell_tile_linear in 0..cell_tile_total {
            let cell_tile_pos = if cell_tiles_per_dim.is_empty() {
                Vec::new()
            } else {
                linear_to_nd(cell_tile_linear, cell_tiles_per_dim)
            };
            let mut tile_pos = cell_tile_pos.clone();
            tile_pos.push(row_tile_idx);
            let tile_idx = nd_to_linear(&tile_pos, &tiles_per_dim);
            let tile_start = cube.file_offset as usize + tile_idx * bucket_size + col_offset;
            let mut packed_tile = vec![0u8; tile_storage_len];
            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.read_exact(&mut packed_tile)?;
            let mut unpacked_tile = read_tile_storage(&packed_tile, col_data_type, nrpixels);

            let cell_cube_start: Vec<usize> = (0..row_axis)
                .map(|dim| cell_tile_pos.get(dim).copied().unwrap_or(0) * cube.tile_shape[dim])
                .collect();
            let actual_cell_extent: Vec<usize> = (0..row_axis)
                .map(|dim| {
                    std::cmp::min(cube.tile_shape[dim], cell_shape[dim] - cell_cube_start[dim])
                })
                .collect();

            for patch in &row_tile_patches {
                let local_row = patch.pos_in_cube % row_tile_len;
                let row_slice_start = local_row * row_block_bytes;
                let row_slice_end = row_slice_start + row_block_bytes;
                let row_tile = &mut unpacked_tile[row_slice_start..row_slice_end];
                let encoded_row = match values.get(patch.global_row_idx).copied().flatten() {
                    Some(value) => {
                        let (encoded, dt) = encode_array_value(value, big_endian)?;
                        if dt != col_data_type {
                            return Err(StorageError::FormatMismatch(format!(
                                "tiled sparse save column {} expected {:?} but encoded {:?}",
                                col_desc.col_name, col_data_type, dt
                            )));
                        }
                        if encoded.len() != expected_row_bytes {
                            return Err(StorageError::FormatMismatch(format!(
                                "tiled sparse save column {} expected {expected_row_bytes} row bytes but encoded {}",
                                col_desc.col_name,
                                encoded.len()
                            )));
                        }
                        encoded
                    }
                    None => vec![0u8; expected_row_bytes],
                };
                copy_cube_to_tile(
                    &encoded_row,
                    cell_shape,
                    row_tile,
                    tile_cell_shape,
                    &cell_cube_start,
                    &actual_cell_extent,
                    elem_size,
                );
            }

            write_tile_storage(&mut packed_tile, col_data_type, &unpacked_tile, nrpixels);
            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.write_all(&packed_tile)?;
        }
    }

    Ok(())
}

/// Patch the changed rows of one hypercube in place.
///
/// `col_data_types` lists the tile columns in tile order. `tile_index` maps
/// each bound column of `col_descs` and `sparse_columns` to its tile
/// column ([`TileColumns::tile_index`]).
#[allow(clippy::too_many_arguments)]
pub(super) fn patch_tiled_cube_sparse_rows(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues<'_>>],
    big_endian: bool,
    col_data_types: &[CasacoreDataType],
    tile_index: &[Option<usize>],
    cube: &TsmCubeInfo,
    row_patches: &[SparseTiledCubeRowPatch],
) -> Result<(), StorageError> {
    let ndim = cube.cube_shape.len();
    if ndim == 0 {
        return Ok(());
    }
    let row_axis = ndim - 1;
    if row_axis == 0 {
        return Err(StorageError::FormatMismatch(
            "tiled sparse save requires array-valued cells".to_string(),
        ));
    }

    struct SparseColumnPatch<'a> {
        col_data_type: CasacoreDataType,
        elem_size: usize,
        expected_row_bytes: usize,
        row_block_bytes: usize,
        tile_storage_len: usize,
        col_offset: usize,
        encoded_rows: std::collections::HashMap<usize, Cow<'a, [u8]>>,
    }

    let cell_shape = &cube.cube_shape[..row_axis];
    let tile_cell_shape = &cube.tile_shape[..row_axis];
    let row_tile_len = cube.tile_shape[row_axis].max(1);
    let row_block_pixels: usize = tile_cell_shape.iter().product();
    let nrpixels: usize = cube.tile_shape.iter().product();
    let (bucket_size, col_offsets) = compute_tile_layout(col_data_types, &cube.tile_shape);
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&cube_len, &tile_len)| cube_len.div_ceil(tile_len))
        .collect();
    let cell_tiles_per_dim = &tiles_per_dim[..row_axis];
    let cell_tile_total = cell_tiles_per_dim.iter().product::<usize>().max(1);

    // Only this cube's rows: a heterogeneous TiledShapeStMan holds the other
    // rows, with other cell shapes, in other cubes.
    let cube_rows: std::collections::HashSet<usize> = row_patches
        .iter()
        .map(|patch| patch.global_row_idx)
        .collect();
    let mut column_patches = Vec::new();
    for (col_idx, sparse_values) in sparse_columns.iter().enumerate() {
        let Some(sparse_values) = sparse_values else {
            continue;
        };
        let tile = match tile_index.get(col_idx) {
            Some(Some(tile)) => *tile,
            Some(None) => {
                return Err(StorageError::FormatMismatch(format!(
                    "tiled sparse save cannot change id or coordinate column {}",
                    col_descs[col_idx].col_name
                )));
            }
            None => return Ok(()),
        };
        if col_idx >= col_descs.len() || tile >= col_data_types.len() {
            return Ok(());
        }
        let col_data_type = col_data_types[tile];
        let elem_size = tile_element_size(col_data_type);
        if elem_size == 0 {
            return Err(StorageError::FormatMismatch(format!(
                "tiled sparse save does not support non-primitive array column {}",
                col_descs[col_idx].col_name
            )));
        }
        let expected_row_bytes = cell_shape.iter().product::<usize>() * elem_size;
        let mut encoded_rows = std::collections::HashMap::with_capacity(cube_rows.len());
        for (row_idx, value) in sparse_values {
            if !cube_rows.contains(row_idx) {
                continue;
            }
            let encoded = match value {
                Some(value) => {
                    let (encoded, dt) = sparse_write_bytes::encode_row(value, big_endian)?;
                    if dt != col_data_type {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled sparse save column {} expected {:?} but encoded {:?}",
                            col_descs[col_idx].col_name, col_data_type, dt
                        )));
                    }
                    if encoded.len() != expected_row_bytes {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled sparse save column {} expected {expected_row_bytes} row bytes but encoded {}",
                            col_descs[col_idx].col_name,
                            encoded.len()
                        )));
                    }
                    encoded
                }
                None => Cow::Owned(vec![0u8; expected_row_bytes]),
            };
            encoded_rows.insert(*row_idx, encoded);
        }
        if encoded_rows.is_empty() {
            continue;
        }
        column_patches.push(SparseColumnPatch {
            col_data_type,
            elem_size,
            expected_row_bytes,
            row_block_bytes: row_block_pixels * elem_size,
            tile_storage_len: tile_storage_bytes(col_data_type, nrpixels),
            col_offset: col_offsets[tile],
            encoded_rows,
        });
    }
    if column_patches.is_empty() {
        return Ok(());
    }

    let mut patches_by_row_tile: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &patch in row_patches {
        patches_by_row_tile
            .entry(patch.pos_in_cube / row_tile_len)
            .or_default()
            .push(patch);
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let mut file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
    let file_len = file.metadata()?.len();
    let mut packed_bucket = vec![0u8; bucket_size];
    let writes_all_numeric_columns = column_patches.len() == col_data_types.len()
        && column_patches
            .iter()
            .all(|column| column.col_data_type != CasacoreDataType::TpBool);
    for (row_tile_idx, row_tile_patches) in patches_by_row_tile {
        if row_tile_idx >= tiles_per_dim[row_axis] {
            continue;
        }
        let covers_row_tile = writes_all_numeric_columns
            && cube.cube_shape[row_axis] - row_tile_idx * row_tile_len >= row_tile_len
            && row_tile_patches.len() == row_tile_len
            && row_tile_patches.iter().enumerate().all(|(local, patch)| {
                patch.pos_in_cube == row_tile_idx * row_tile_len + local
                    && column_patches
                        .iter()
                        .all(|column| column.encoded_rows.contains_key(&patch.global_row_idx))
            });
        for cell_tile_linear in 0..cell_tile_total {
            let cell_tile_pos = linear_to_nd(cell_tile_linear, cell_tiles_per_dim);
            let mut tile_pos = cell_tile_pos.clone();
            tile_pos.push(row_tile_idx);
            let tile_idx = nd_to_linear(&tile_pos, &tiles_per_dim);
            let tile_start = cube.file_offset as usize + tile_idx * bucket_size;
            let cell_cube_start: Vec<usize> = (0..row_axis)
                .map(|dim| cell_tile_pos.get(dim).copied().unwrap_or(0) * cube.tile_shape[dim])
                .collect();
            let actual_cell_extent: Vec<usize> = (0..row_axis)
                .map(|dim| {
                    std::cmp::min(cube.tile_shape[dim], cell_shape[dim] - cell_cube_start[dim])
                })
                .collect();
            // A complete numeric tile can be replaced from the selected cells.
            // Keep existing padding, packed bits and unassigned columns for
            // partial tiles. A truncated file still takes the checking read.
            if !covers_row_tile
                || actual_cell_extent != tile_cell_shape
                || file_len.saturating_sub(tile_start as u64) < bucket_size as u64
            {
                file.seek(SeekFrom::Start(tile_start as u64))?;
                file.read_exact(&mut packed_bucket)?;
            }

            for column_patch in &column_patches {
                let src_start = column_patch.col_offset;
                let src_end = src_start + column_patch.tile_storage_len;
                let patch_tile = |tile: &mut [u8]| {
                    for patch in &row_tile_patches {
                        let Some(encoded_row) =
                            column_patch.encoded_rows.get(&patch.global_row_idx)
                        else {
                            continue;
                        };
                        debug_assert_eq!(encoded_row.len(), column_patch.expected_row_bytes);
                        let local_row = patch.pos_in_cube % row_tile_len;
                        let row_start = local_row * column_patch.row_block_bytes;
                        let row = &mut tile[row_start..row_start + column_patch.row_block_bytes];
                        copy_cube_to_tile(
                            encoded_row,
                            cell_shape,
                            row,
                            tile_cell_shape,
                            &cell_cube_start,
                            &actual_cell_extent,
                            column_patch.elem_size,
                        );
                    }
                };
                let stored = &mut packed_bucket[src_start..src_end];
                if column_patch.col_data_type == CasacoreDataType::TpBool {
                    let mut unpacked =
                        read_tile_storage(stored, CasacoreDataType::TpBool, nrpixels);
                    patch_tile(&mut unpacked);
                    write_tile_storage(stored, CasacoreDataType::TpBool, &unpacked, nrpixels);
                } else {
                    patch_tile(stored);
                }
            }

            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.write_all(&packed_bucket)?;
        }
    }

    Ok(())
}

pub(super) fn format_shape(shape: &[usize]) -> String {
    let mut out = String::from("[");
    for (idx, value) in shape.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

pub(super) fn encode_column_cube_values(
    values: &[Option<&Value>],
    cube_shape: &[usize],
    cell_nelem: usize,
    elem_size: usize,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let cube_nelem: usize = cube_shape.iter().product();
    let mut cube_bytes = vec![0u8; cube_nelem * elem_size];
    for (row_idx, value) in values.iter().enumerate() {
        if let Some(value) = value {
            let (encoded, _) = encode_array_value(value, big_endian)?;
            let start = row_idx * cell_nelem * elem_size;
            let end = start + cell_nelem * elem_size;
            if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                cube_bytes[start..end].copy_from_slice(&encoded);
            }
        }
    }
    Ok(cube_bytes)
}
