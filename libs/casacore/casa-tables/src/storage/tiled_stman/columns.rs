// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Load columns from a tiled storage manager into row records.
///
/// This is the main entry point called from `CompositeStorage::load_plain_table`.
pub(crate) fn load_tiled_columns(
    table_path: &Path,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let header_path = table_path.join(format!("table.f{}", dm.seq_nr));
    let (variant, header) = read_tiled_header(&header_path)?;

    // Collect column descriptors for the bound columns.
    let col_descs: Vec<&ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &table_desc.columns[*desc_idx])
        .collect();
    let tiles = tile_columns(&variant, Cow::Owned(header), &col_descs, table_desc, nrrow)?;
    // The managers other than TiledDataStMan index the header's types, the
    // tile offsets and the descriptors alike, so they take all three in tile
    // order, and every bound column must be a tile column.
    let header = &*tiles.header;
    let in_tiles = || match tiles.first_untiled(&col_descs) {
        Some(untiled) => Err(untiled_column_error(
            variant.manager_name(),
            &untiled.col_name,
        )),
        None => Ok(in_tile_order(&tiles, &col_descs)),
    };

    match variant {
        TiledVariant::Column { .. } => load_tiled_column_stman(
            table_path,
            dm.seq_nr,
            header,
            &in_tiles()?,
            rows,
            undefined_cells,
            nrrow,
        ),
        TiledVariant::Shape {
            nr_used_row_map,
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_shape_stman(
            table_path,
            dm.seq_nr,
            header,
            &in_tiles()?,
            rows,
            undefined_cells,
            nrrow,
            &ShapeRowMapping {
                nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        TiledVariant::Cell { .. } => load_tiled_cell_stman(
            table_path,
            dm.seq_nr,
            header,
            &in_tiles()?,
            rows,
            undefined_cells,
            nrrow,
        ),
        TiledVariant::Data {
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_data_stman(
            table_path,
            dm.seq_nr,
            &tiles,
            &col_descs,
            rows,
            undefined_cells,
            nrrow,
            row_map,
            cube_map,
            pos_map,
        ),
    }
}

pub(crate) fn collect_tiled_scalar_columns(
    table_path: &Path,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    all_bound_cols: &[(usize, &PlainColumnEntry)],
    requested_columns: Option<&HashSet<&str>>,
    nrrow: usize,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let header_path = table_path.join(format!("table.f{}", dm.seq_nr));
    let (variant, header) = read_tiled_header(&header_path)?;
    let col_descs: Vec<&ColumnDescContents> = all_bound_cols
        .iter()
        .map(|(desc_idx, _)| &table_desc.columns[*desc_idx])
        .collect();
    let tiles = tile_columns(&variant, Cow::Owned(header), &col_descs, table_desc, nrrow)?;
    let header = &*tiles.header;

    match variant {
        TiledVariant::Data {
            ref row_map,
            ref cube_map,
            ..
        } => {
            if row_map.len() != cube_map.len() {
                return Err(StorageError::FormatMismatch(
                    "invalid TiledDataStMan row-map lengths".to_string(),
                ));
            }
            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                if col_desc.is_array
                    || requested_columns
                        .is_some_and(|requested| !requested.contains(col_desc.col_name.as_str()))
                {
                    continue;
                }
                if tiles.tile_index[col_idx].is_some() {
                    return Err(untiled_column_error(
                        variant.manager_name(),
                        &col_desc.col_name,
                    ));
                }
                let mut values = vec![None; nrrow];
                let mut cube_scalars: Vec<Option<ScalarValue>> = vec![None; header.cubes.len()];
                for (row_idx, slot) in values.iter_mut().enumerate() {
                    let chunk = match row_map.binary_search(&(row_idx as u64)) {
                        Ok(i) => i,
                        Err(0) => continue,
                        Err(i) => i - 1,
                    };
                    let Some(&cube_idx) = cube_map.get(chunk) else {
                        continue;
                    };
                    let cube_idx = cube_idx as usize;
                    if cube_idx >= header.cubes.len() {
                        continue;
                    }
                    if cube_scalars[cube_idx].is_none() {
                        let Value::Scalar(scalar) = tile_columns::hypercube_cell(
                            &header.cubes[cube_idx],
                            &col_desc.col_name,
                        )?
                        else {
                            return Err(untiled_column_error(
                                variant.manager_name(),
                                &col_desc.col_name,
                            ));
                        };
                        cube_scalars[cube_idx] = Some(scalar);
                    }
                    *slot = cube_scalars[cube_idx].clone();
                }
                columns.insert(col_desc.col_name.clone(), values);
            }
            Ok(())
        }
        TiledVariant::Column { .. } | TiledVariant::Shape { .. } | TiledVariant::Cell { .. } => {
            if let Some(untiled) = tiles.first_untiled(&col_descs) {
                return Err(untiled_column_error(
                    variant.manager_name(),
                    &untiled.col_name,
                ));
            }
            Ok(())
        }
    }
}

pub(crate) fn load_tiled_column_rows(
    table_path: &Path,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let header_path = table_path.join(format!("table.f{}", dm.seq_nr));
    let (variant, header) = read_tiled_header(&header_path)?;
    let (header, target_col_idx) = tile_target(
        &variant,
        Cow::Owned(header),
        table_desc,
        bound_cols,
        target_desc_idx,
        (dm.seq_nr, selected_rows),
    )?;
    let header = &*header;
    let col_desc = &table_desc.columns[target_desc_idx];
    let dt = header.col_data_types[target_col_idx];
    let elem_size = tile_element_size(dt);
    if elem_size == 0 {
        return Ok(vec![None; selected_rows.len()]);
    }

    match variant {
        TiledVariant::Column { .. } => load_tiled_column_rows_column_variant(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
        ),
        TiledVariant::Shape {
            nr_used_row_map,
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_column_rows_shape_variant(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            &ShapeRowMapping {
                nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        TiledVariant::Cell { .. } => load_tiled_column_rows_cell_variant(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
        ),
        TiledVariant::Data {
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_column_rows_data_variant(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            row_map,
            cube_map,
            pos_map,
        ),
    }
}

/// Load columns from a `TiledColumnStMan` (single hypercube for all rows).
pub(super) fn load_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    if header.cubes.is_empty() {
        return Ok(());
    }
    let cube = &header.cubes[0];
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(());
    }

    // Read the TSM data file.
    let tsm_file_name = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let file_data = std::fs::read(&tsm_file_name).map_err(|e| {
        StorageError::FormatMismatch(format!("cannot read {}: {e}", tsm_file_name.display()))
    })?;

    // Compute tile layout.
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);

    // Cell shape = cube_shape without the last dimension (row dimension).
    let cell_ndim = cube.cube_shape.len() - 1;
    let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
    let cell_nelem: usize = cell_shape.iter().product();

    // For each column, reconstruct the full cube data, then extract per-row slices.
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        if col_idx >= header.col_data_types.len() {
            break;
        }
        let dt = header.col_data_types[col_idx];
        let elem_size = tile_element_size(dt);
        if elem_size == 0 {
            continue;
        }

        let cube_raw = reconstruct_cube_column(
            &file_data,
            cube.file_offset as usize,
            &cube.cube_shape,
            &cube.tile_shape,
            col_offsets[col_idx],
            bucket_size,
            dt,
            elem_size,
        )?;

        // Extract per-row arrays.
        let row_bytes = cell_nelem * elem_size;
        for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
            let start = row_idx * row_bytes;
            let end = start + row_bytes;
            if end > cube_raw.len() {
                break;
            }
            let value =
                decode_array_value(&cube_raw[start..end], &cell_shape, dt, header.big_endian)?;
            row.push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_column_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    if header.cubes.is_empty() {
        return Ok(vec![None; selected_rows.len()]);
    }
    let cube_idx = 0usize;
    let cube = &header.cubes[cube_idx];
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(vec![None; selected_rows.len()]);
    }
    let patches_by_cube = std::iter::once((
        cube_idx,
        selected_rows
            .iter()
            .enumerate()
            .map(|(out_idx, &row_idx)| SelectedCubeRow {
                out_idx,
                pos_in_cube: row_idx,
            })
            .collect(),
    ))
    .collect();
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Row-to-cube mapping tables extracted from `TiledShapeStMan` variant data.
pub(super) struct ShapeRowMapping<'a> {
    pub(super) nr_used_row_map: u32,
    pub(super) row_map: &'a [u32],
    pub(super) cube_map: &'a [u32],
    pub(super) pos_map: &'a [u32],
}

pub(super) fn shape_row_location(
    header: &TiledStManHeader,
    mapping: &ShapeRowMapping<'_>,
    row_index: usize,
) -> Result<Option<(usize, usize)>, StorageError> {
    let interval_count = mapping.nr_used_row_map as usize;
    if interval_count > mapping.row_map.len()
        || interval_count > mapping.cube_map.len()
        || interval_count > mapping.pos_map.len()
    {
        return Err(StorageError::FormatMismatch(
            "invalid TiledShapeStMan row-map lengths".to_string(),
        ));
    }
    let interval = mapping.row_map[..interval_count].partition_point(|&row| row < row_index as u32);
    if interval >= interval_count {
        return Ok(None);
    }
    let cube_index = mapping.cube_map[interval] as usize;
    if cube_index == 0 || cube_index >= header.cubes.len() {
        return Ok(None);
    }
    let row_delta = mapping.row_map[interval] as usize - row_index;
    let Some(position) = (mapping.pos_map[interval] as usize).checked_sub(row_delta) else {
        return Ok(None);
    };
    Ok(Some((cube_index, position)))
}

/// Load columns from a `TiledShapeStMan` (one hypercube per unique shape).
#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    mapping: &ShapeRowMapping<'_>,
) -> Result<(), StorageError> {
    // Pre-read all TSM data files.
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    // Pre-compute per-cube tile layouts and read TSM files.
    struct CubeLayout {
        bucket_size: usize,
        col_offsets: Vec<usize>,
    }
    let mut cube_layouts: Vec<Option<CubeLayout>> = Vec::with_capacity(header.cubes.len());

    for cube in &header.cubes {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            cube_layouts.push(None);
            continue;
        }
        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);
        cube_layouts.push(Some(CubeLayout {
            bucket_size,
            col_offsets,
        }));

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
    }

    // Pre-reconstruct per-cube per-column data.
    struct CubeColumnData {
        raw: Vec<u8>,
        cell_shape: Vec<usize>,
        cell_nelem: usize,
    }
    let mut cube_col_data: Vec<Vec<Option<CubeColumnData>>> =
        Vec::with_capacity(header.cubes.len());

    for (cube_idx, cube) in header.cubes.iter().enumerate() {
        let mut cols = Vec::with_capacity(col_descs.len());
        let layout = &cube_layouts[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() || layout.is_none() {
            for _ in col_descs {
                cols.push(None);
            }
            cube_col_data.push(cols);
            continue;
        }
        let layout = layout.as_ref().unwrap();
        let file_data = &file_cache[&(cube.file_seq_nr as u32)];

        let cell_ndim = cube.cube_shape.len() - 1;
        let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
        let cell_nelem: usize = cell_shape.iter().product();

        for (col_idx, _) in col_descs.iter().enumerate() {
            if col_idx >= header.col_data_types.len() {
                cols.push(None);
                continue;
            }
            let dt = header.col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                cols.push(None);
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                &cube.cube_shape,
                &cube.tile_shape,
                layout.col_offsets[col_idx],
                layout.bucket_size,
                dt,
                elem_size,
            )?;
            cols.push(Some(CubeColumnData {
                raw,
                cell_shape: cell_shape.clone(),
                cell_nelem,
            }));
        }
        cube_col_data.push(cols);
    }

    // Map rows to cubes and extract values.
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals != 0 {
        let row_map = &mapping.row_map[..n_intervals];
        let cube_map = &mapping.cube_map[..n_intervals];
        let pos_map = &mapping.pos_map[..n_intervals];
        for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
            // C++ TiledShapeStMan stores upper bounds for row intervals in rowMap.
            // The containing interval is the first entry whose upper bound is >= row.
            let interval = row_map.partition_point(|&rm| rm < row_idx as u32);
            if interval >= n_intervals {
                continue;
            }

            let cube_idx = cube_map[interval] as usize;
            if cube_idx == 0 || cube_idx >= cube_col_data.len() {
                continue; // dummy hypercube used for undefined rows
            }

            let diff = row_map[interval] as usize - row_idx;
            if diff > pos_map[interval] as usize {
                continue;
            }
            let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
                return Err(StorageError::FormatMismatch(format!(
                    "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                    pos_map[interval]
                )));
            };

            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                if let Some(ref ccd) = cube_col_data[cube_idx][col_idx] {
                    let dt = header.col_data_types[col_idx];
                    let elem_size = tile_element_size(dt);
                    let row_bytes = ccd.cell_nelem * elem_size;
                    let start = pos_in_cube * row_bytes;
                    let end = start + row_bytes;
                    if end <= ccd.raw.len() {
                        let value = decode_array_value(
                            &ccd.raw[start..end],
                            &ccd.cell_shape,
                            dt,
                            header.big_endian,
                        )?;
                        row.upsert(col_desc.col_name.clone(), value);
                    }
                }
            }
        }
    }

    // The cells no hypercube holds are undefined.
    for (row, undefined) in rows.iter_mut().zip(undefined_cells.iter_mut()).take(nrrow) {
        for col_desc in col_descs {
            if row.get(&col_desc.col_name).is_none()
                && let Some(value) = super::super::undefined_indirect_cell(col_desc, undefined)
            {
                row.push(RecordField::new(col_desc.col_name.clone(), value));
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SelectedCubeRow {
    pub(super) out_idx: usize,
    pub(super) pos_in_cube: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_shape_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(vec![None; selected_rows.len()]);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>> =
        std::collections::BTreeMap::new();
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let interval = mapping.row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        if interval >= n_intervals {
            continue;
        }
        let cube_idx = mapping.cube_map[interval] as usize;
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            continue;
        }
        let diff = mapping.row_map[interval] as usize - row_idx;
        if diff > mapping.pos_map[interval] as usize {
            continue;
        }
        let Some(pos_in_cube) = (mapping.pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                mapping.pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
    }
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Load columns from a `TiledDataStMan` (user-controlled hypercube assignment).
///
/// Uses binary search in `row_map` to find the chunk index for each row.
/// `cube_map[chunk]` gives the hypercube and `pos_map[chunk]` gives the
/// starting position in the last dimension. Data columns come from the
/// tiles; an id column's cell is its hypercube's id value.
///
/// # C++ equivalent
///
/// `TiledDataStMan::getArraySection()`.
#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_data_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    tiles: &TileColumns<'_>,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    row_map: &[u64],
    cube_map: &[u32],
    pos_map: &[u32],
) -> Result<(), StorageError> {
    let header = &*tiles.header;
    let tile_types = &header.col_data_types;

    // Pre-read all TSM data files and compute per-cube layouts.
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    struct CubeLayout {
        bucket_size: usize,
        col_offsets: Vec<usize>,
    }
    let mut cube_layouts: Vec<Option<CubeLayout>> = Vec::with_capacity(header.cubes.len());

    for cube in &header.cubes {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            cube_layouts.push(None);
            continue;
        }
        let (bucket_size, col_offsets) = compute_tile_layout(tile_types, &cube.tile_shape);
        cube_layouts.push(Some(CubeLayout {
            bucket_size,
            col_offsets,
        }));

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
    }

    // Pre-reconstruct per-cube per-tile-column data.
    struct CubeColumnData {
        raw: Vec<u8>,
        cell_shape: Vec<usize>,
        cell_nelem: usize,
    }
    let mut cube_col_data: Vec<Vec<Option<CubeColumnData>>> =
        Vec::with_capacity(header.cubes.len());

    for (cube_idx, cube) in header.cubes.iter().enumerate() {
        let mut cols = Vec::with_capacity(tile_types.len());
        let layout = &cube_layouts[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() || layout.is_none() {
            for _ in tile_types {
                cols.push(None);
            }
            cube_col_data.push(cols);
            continue;
        }
        let layout = layout.as_ref().unwrap();
        let file_data = &file_cache[&(cube.file_seq_nr as u32)];

        let cell_ndim = cube.cube_shape.len() - 1;
        let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
        let cell_nelem: usize = cell_shape.iter().product();

        for (col_idx, &dt) in tile_types.iter().enumerate() {
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                cols.push(None);
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                &cube.cube_shape,
                &cube.tile_shape,
                layout.col_offsets[col_idx],
                layout.bucket_size,
                dt,
                elem_size,
            )?;
            cols.push(Some(CubeColumnData {
                raw,
                cell_shape: cell_shape.clone(),
                cell_nelem,
            }));
        }
        cube_col_data.push(cols);
    }

    // Map rows to cubes using binary search in row_map.
    let n_chunks = row_map.len();
    for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
        // Binary search: find the first chunk where row_map[chunk] > row_idx.
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue; // row before first chunk
                }
                i - 1
            }
        };
        if chunk >= n_chunks {
            continue;
        }

        let cube_idx = cube_map[chunk] as usize;
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);

        if cube_idx >= cube_col_data.len() {
            continue;
        }

        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            let Some(tile) = tiles.tile_index[col_idx] else {
                let value =
                    tile_columns::hypercube_cell(&header.cubes[cube_idx], &col_desc.col_name)?;
                row.push(RecordField::new(col_desc.col_name.clone(), value));
                continue;
            };
            if let Some(ref ccd) = cube_col_data[cube_idx][tile] {
                let dt = tile_types[tile];
                let elem_size = tile_element_size(dt);
                let row_bytes = ccd.cell_nelem * elem_size;
                let start = pos_in_cube * row_bytes;
                let end = start + row_bytes;
                if end <= ccd.raw.len() {
                    let value = decode_array_value(
                        &ccd.raw[start..end],
                        &ccd.cell_shape,
                        dt,
                        header.big_endian,
                    )?;
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_data_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    row_map: &[u64],
    cube_map: &[u32],
    pos_map: &[u32],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>> =
        std::collections::BTreeMap::new();
    let n_chunks = row_map.len();
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue;
                }
                i - 1
            }
        };
        if chunk >= n_chunks {
            continue;
        }
        let cube_idx = cube_map[chunk] as usize;
        if cube_idx >= header.cubes.len() {
            continue;
        }
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
    }
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Load columns from a `TiledCellStMan` (one hypercube per row).
pub(super) fn load_tiled_cell_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    for (row_idx, cube) in header.cubes.iter().enumerate().take(nrrow) {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
        let file_data = &file_cache[&fseq];

        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);

        // For TiledCellStMan, the cube IS the cell (no extra row dimension).
        let cell_shape = &cube.cube_shape;

        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            if col_idx >= header.col_data_types.len() {
                break;
            }
            let dt = header.col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                cell_shape,
                &cube.tile_shape,
                col_offsets[col_idx],
                bucket_size,
                dt,
                elem_size,
            )?;
            let value = decode_array_value(&raw, cell_shape, dt, header.big_endian)?;
            rows[row_idx].push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_cell_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let mut outputs = vec![None; selected_rows.len()];
    let mut read_session = TileReadSession::default();

    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let Some(cube) = header.cubes.get(row_idx) else {
            continue;
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        outputs[out_idx] = decode_tiled_cell_from_shared_tiles(
            table_path,
            dm_seq_nr,
            header,
            row_idx,
            cube,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            &mut read_session,
        )?;
    }
    Ok(outputs)
}

pub(super) fn extract_selected_rows_from_cube_raw(
    cube_raw: &[u8],
    cube_shape: &[usize],
    selected_rows: impl IntoIterator<Item = SelectedCubeRow>,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    big_endian: bool,
    output_len: usize,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let cell_ndim = cube_shape.len().saturating_sub(1);
    let cell_shape: Vec<usize> = cube_shape[..cell_ndim].to_vec();
    let cell_nelem: usize = cell_shape.iter().product();
    let elem_size = tile_element_size(dt);
    let row_bytes = cell_nelem * elem_size;
    let mut outputs = vec![None; output_len];

    for selected in selected_rows {
        let start = selected.pos_in_cube * row_bytes;
        let end = start + row_bytes;
        if end > cube_raw.len() {
            continue;
        }
        let value = decode_array_value(&cube_raw[start..end], &cell_shape, dt, big_endian)?;
        match value {
            Value::Array(array) => outputs[selected.out_idx] = Some(array),
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "tiled array column {} decoded as non-array value {:?}",
                    col_desc.col_name, other
                )));
            }
        }
    }

    Ok(outputs)
}
