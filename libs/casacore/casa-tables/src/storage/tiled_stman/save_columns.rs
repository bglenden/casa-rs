// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// The hypercolumn a tiled writer lays out: its name, which the header
/// records, and the table description's private keywords, which hold its
/// definition if the table has one.
#[derive(Clone, Copy)]
pub(super) struct Hypercolumn<'a> {
    pub(super) name: &'a str,
    pub(super) private_keywords: &'a RecordValue,
}

/// Save columns to a tiled storage manager.
///
/// Determines the appropriate variant from the `DataManagerKind` and writes
/// header + tile data files.
///
/// `dm_name` is the hypercolumn / data-manager name written into the
/// TiledStMan header. C++ uses this to look up the DM when opening the
/// table. Pass the column name (or an explicit name) to match C++ behaviour.
/// The tiles are laid out as casacore lays them out: over the hypercolumn's
/// data columns in the order of its definition, if `private_keywords` (the
/// table description's) holds one, else the binding order, sorted by
/// descending pixel size (#755).
#[allow(clippy::too_many_arguments)]
pub(crate) fn save_tiled_columns(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_type_name: &str,
    all_col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    default_tile_shape: Option<&[usize]>,
    dm_name: &str,
    private_keywords: &RecordValue,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::save_tiled_columns dm_seq={} type={} cols={} rows={}",
        dm_seq_nr,
        dm_type_name,
        all_col_descs.len(),
        rows.len()
    ));
    let hypercolumn = Hypercolumn {
        name: dm_name,
        private_keywords,
    };
    let result = match dm_type_name {
        "TiledColumnStMan" => save_tiled_column_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            hypercolumn,
        ),
        "TiledShapeStMan" => save_tiled_shape_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            hypercolumn,
        ),
        "TiledCellStMan" => save_tiled_cell_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            hypercolumn,
        ),
        other => Err(StorageError::FormatMismatch(format!(
            "unknown tiled DM type: {other}"
        ))),
    };
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("dispatch_complete");
    }
    result
}

pub(crate) struct SingleColumnTiledSaveOptions<'a> {
    pub(crate) dm_type_name: &'a str,
    pub(crate) big_endian: bool,
    pub(crate) default_tile_shape: Option<&'a [usize]>,
    pub(crate) dm_name: &'a str,
}

pub(crate) fn save_tiled_single_column_values(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    options: SingleColumnTiledSaveOptions<'_>,
) -> Result<(), StorageError> {
    match options.dm_type_name {
        "TiledColumnStMan" => save_single_column_tiled_column_stman(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            options.big_endian,
            options.default_tile_shape,
            options.dm_name,
        ),
        "TiledShapeStMan" => save_single_column_tiled_shape_stman(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            options.big_endian,
            options.default_tile_shape,
            options.dm_name,
        ),
        _ => {
            let rows: Vec<RecordValue> = values
                .iter()
                .map(|value| match value {
                    Some(value) => RecordValue::new(vec![RecordField::new(
                        col_desc.col_name.clone(),
                        (*value).clone(),
                    )]),
                    None => RecordValue::default(),
                })
                .collect();
            save_tiled_columns(
                table_path,
                dm_seq_nr,
                options.dm_type_name,
                std::slice::from_ref(col_desc),
                &rows,
                options.big_endian,
                options.default_tile_shape,
                options.dm_name,
                &RecordValue::default(),
            )
        }
    }
}

pub(crate) fn save_empty_tiled_shape_column(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    row_count: usize,
    big_endian: bool,
    default_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let col_data_type =
        CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: row_count as u64,
        col_data_types: vec![col_data_type],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim: if col_desc.nrdim > 0 {
            (col_desc.nrdim + 1) as u32
        } else {
            2
        },
        files: Vec::new(),
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: false,
            cube_shape: Vec::new(),
            tile_shape: Vec::new(),
            file_seq_nr: -1,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_tile_shape
            .unwrap_or_default()
            .iter()
            .map(|&value| value as i32)
            .collect(),
        nr_used_row_map: 0,
        row_map: Vec::new(),
        cube_map: Vec::new(),
        pos_map: Vec::new(),
    };
    write_tiled_header(
        &table_path.join(format!("table.f{dm_seq_nr}")),
        &variant,
        &header,
    )
}

pub(super) fn save_single_column_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::column dm_seq={} cols=1 rows={}",
        dm_seq_nr,
        values.len()
    ));
    // With no rows the cube's row axis is empty and it has no tiles, as
    // `TiledColumnStMan::create64` makes it (#764 review).
    let nrrow = values.len();
    let cell_shape: Vec<usize> = col_desc.shape.iter().map(|&s| s as usize).collect();
    let cell_ndim = cell_shape.len();

    let mut cube_shape = cell_shape.clone();
    cube_shape.push(nrrow);

    let col_data_type =
        CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);
    let elem_size = tile_element_size(col_data_type);

    let tile_shape = planned_tile_shape(&cell_shape, Some(nrrow), elem_size, user_tile_shape)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "layout",
            Some(format!(
                "cube_shape={} tile_shape={}",
                format_shape(&cube_shape),
                format_shape(&tile_shape)
            )),
        );
    }

    let cell_nelem: usize = cell_shape.iter().product();

    let (bucket_size, col_offsets) =
        compute_tile_layout(std::slice::from_ref(&col_data_type), &tile_shape);
    let nrdim = (cell_ndim + 1) as u32;
    let tiles_per_dim: Vec<usize> = (0..nrdim as usize)
        .map(|d| cube_shape[d].div_ceil(tile_shape[d]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let nrpixels: usize = tile_shape.iter().product();

    let cube_bytes =
        encode_column_cube_values(values, &cube_shape, cell_nelem, elem_size, big_endian)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "encode_cube_data",
            Some(format!(
                "tiles={} bucket_size={} nrpixels={}",
                nr_tiles, bucket_size, nrpixels
            )),
        );
    }

    let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..nrdim as usize)
            .map(|d| tile_pos[d] * tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..nrdim as usize)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        let mut tile_col = vec![0u8; nrpixels * elem_size];
        copy_cube_to_tile(
            &cube_bytes,
            &cube_shape,
            &mut tile_col,
            &tile_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );

        let dst_start = tile_idx * bucket_size + col_offsets[0];
        write_tile_storage(
            &mut tsm_data[dst_start..dst_start + tile_storage_bytes(col_data_type, nrpixels)],
            col_data_type,
            &tile_col,
            nrpixels,
        );
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("assemble_tiles");
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_tsm_file");
    }

    let default_ts_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types: vec![col_data_type],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: tsm_data.len() as i64,
        })],
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Column {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn save_single_column_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::shape dm_seq={} cols=1 rows={}",
        dm_seq_nr,
        values.len()
    ));
    let nrrow = values.len();
    let col_data_type =
        CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);

    if nrrow == 0 {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: vec![col_data_type],
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: (col_desc.nrdim + 1) as u32,
            files: vec![],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Shape {
            default_tile_shape: vec![],
            nr_used_row_map: 0,
            row_map: vec![],
            cube_map: vec![],
            pos_map: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    let shape_groups = same_shape_group(values).unwrap_or_else(|| {
        let mut shape_groups: Vec<(Vec<usize>, Vec<usize>)> = Vec::new();
        for (row_idx, value) in values.iter().enumerate() {
            let Some(value) = *value else {
                continue;
            };
            let shape = if let Value::Array(av) = value {
                array_shape(av)
            } else {
                vec![]
            };
            if let Some(group) = shape_groups
                .iter_mut()
                .find(|(candidate, _)| *candidate == shape)
            {
                group.1.push(row_idx);
            } else {
                shape_groups.push((shape, vec![row_idx]));
            }
        }
        shape_groups
    });
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "group_shapes",
            Some(format!("shape_groups={}", shape_groups.len())),
        );
    }

    let mut cubes = vec![TsmCubeInfo {
        values: RecordValue::default(),
        extensible: false,
        cube_shape: vec![],
        tile_shape: vec![],
        file_seq_nr: -1,
        file_offset: 0,
    }];
    let mut all_files = Vec::new();
    let mut row_locations = Vec::new();

    let nrdim = if col_desc.nrdim > 0 {
        (col_desc.nrdim + 1) as u32
    } else {
        2
    };

    for (group_idx, (cell_shape, group_rows)) in shape_groups.iter().enumerate() {
        let cube_idx = group_idx + 1;
        let n_in_cube = group_rows.len();

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(n_in_cube);

        let tile_shape = planned_tile_shape(
            cell_shape,
            Some(n_in_cube),
            tile_element_size(col_data_type),
            user_tile_shape,
        )?;

        let file_seq_nr = all_files.len() as u32;
        let (bucket_size, col_offsets) =
            compute_tile_layout(std::slice::from_ref(&col_data_type), &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();
        let cell_nelem: usize = cell_shape.iter().product();
        let elem_size = tile_element_size(col_data_type);

        let group_values: Vec<Option<&Value>> =
            group_rows.iter().map(|&row_idx| values[row_idx]).collect();
        let tsm_path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
        let file_len = if let Some(streamed) = write_single_column_shape_cube_direct_to_file(
            &tsm_path,
            &group_values,
            cell_shape,
            &tile_shape,
            cell_nelem,
            elem_size,
            nr_tiles,
            bucket_size,
            col_offsets[0],
            col_data_type,
            big_endian,
        )? {
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "stream_cube_file",
                    Some(format!(
                        "cube={} shape={} rows={} tile_shape={} bytes={} assemble={:.3}s write={:.3}s",
                        cube_idx,
                        format_shape(&cube_shape),
                        n_in_cube,
                        format_shape(&tile_shape),
                        streamed.bytes,
                        streamed.assemble_seconds,
                        streamed.write_seconds
                    )),
                );
            }
            streamed.bytes
        } else {
            let cube_bytes = encode_column_cube_values(
                &group_values,
                &cube_shape,
                cell_nelem,
                elem_size,
                big_endian,
            )?;

            let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
            for tile_idx in 0..nr_tiles {
                let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
                let cube_start: Vec<usize> = cube_shape
                    .iter()
                    .zip(tile_pos.iter())
                    .zip(tile_shape.iter())
                    .map(|((_, &tp), &ts)| tp * ts)
                    .collect();
                let actual_extent: Vec<usize> = cube_shape
                    .iter()
                    .zip(cube_start.iter())
                    .zip(tile_shape.iter())
                    .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                    .collect();

                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &cube_bytes,
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[0];
                write_tile_storage(
                    &mut tsm_data
                        [dst_start..dst_start + tile_storage_bytes(col_data_type, nrpixels)],
                    col_data_type,
                    &tile_col,
                    nrpixels,
                );
            }
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "assemble_cube",
                    Some(format!(
                        "cube={} shape={} rows={} tile_shape={} bytes={}",
                        cube_idx,
                        format_shape(&cube_shape),
                        n_in_cube,
                        format_shape(&tile_shape),
                        tsm_data.len()
                    )),
                );
            }

            std::fs::write(&tsm_path, &tsm_data)?;
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "write_cube_file",
                    Some(format!(
                        "cube={} shape={} rows={} tile_shape={} bytes={}",
                        cube_idx,
                        format_shape(&cube_shape),
                        n_in_cube,
                        format_shape(&tile_shape),
                        tsm_data.len()
                    )),
                );
            }
            tsm_data.len()
        };

        all_files.push(Some(TsmFileInfo {
            seq_nr: file_seq_nr,
            length: file_len as i64,
        }));

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: file_seq_nr as i32,
            file_offset: 0,
        });

        for (pos_in_cube, &row_idx) in group_rows.iter().enumerate() {
            row_locations.push((row_idx as u32, cube_idx as u32, pos_in_cube as u32));
        }
    }

    let (row_map_vec, cube_map_vec, pos_map_vec) = shape_row_intervals(&mut row_locations);
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail("row_map", Some(format!("intervals={}", row_map_vec.len())));
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else if let Some((first_shape, _)) = shape_groups.first() {
        default_tile_shape_for(first_shape, nrrow, tile_element_size(col_data_type))?
            .iter()
            .map(|&v| v as i32)
            .collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types: vec![col_data_type],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_ts_i32,
        nr_used_row_map: row_map_vec.len() as u32,
        row_map: row_map_vec,
        cube_map: cube_map_vec,
        pos_map: pos_map_vec,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Each entry covers all rows from the previous endpoint through this one.
/// Casacore represents a gap before a defined cell with an interval in cube 0.
/// Rows after the final endpoint are implicitly undefined.
pub(super) fn shape_row_intervals(
    locations: &mut [(u32, u32, u32)],
) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    locations.sort_unstable_by_key(|&(row, _, _)| row);
    let mut rows = Vec::new();
    let mut cubes = Vec::new();
    let mut positions = Vec::<u32>::new();
    let mut next_row = 0u64;
    for &(row, cube, position) in locations.iter() {
        if u64::from(row) > next_row {
            rows.push(row - 1);
            cubes.push(0);
            positions.push(0);
        }
        if u64::from(row) == next_row
            && cubes.last() == Some(&cube)
            && positions
                .last()
                .is_some_and(|&last| last.checked_add(1) == Some(position))
        {
            *rows.last_mut().expect("interval row endpoint") = row;
            *positions.last_mut().expect("interval cube endpoint") = position;
        } else {
            rows.push(row);
            cubes.push(cube);
            positions.push(position);
        }
        next_row = u64::from(row) + 1;
    }
    (rows, cubes, positions)
}

/// Compute the shared byte-aware default tile for a tiled column.
pub(super) fn default_tile_shape_for(
    cell_shape: &[usize],
    nrow: usize,
    bytes_per_cell_element: usize,
) -> Result<Vec<usize>, StorageError> {
    TileLayoutPlanner::repository_default()
        .plan_column(cell_shape, nrow, bytes_per_cell_element)
        .map(|plan| plan.tile_shape().to_vec())
        .map_err(|error| StorageError::FormatMismatch(error.to_string()))
}

pub(super) fn planned_tile_shape(
    cell_shape: &[usize],
    row_count: Option<usize>,
    element_bytes: usize,
    explicit: Option<&[usize]>,
) -> Result<Vec<usize>, StorageError> {
    let axis_order: Vec<usize> = (0..cell_shape.len()).collect();
    TileLayoutPlanner::repository_default()
        .plan(cell_shape, row_count, element_bytes, &axis_order, explicit)
        .map(|plan| plan.tile_shape().to_vec())
        .map_err(|error| StorageError::FormatMismatch(error.to_string()))
}

/// Save with `TiledColumnStMan` (single hypercube, all rows same shape).
pub(super) fn save_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    hypercolumn: Hypercolumn<'_>,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::column dm_seq={} cols={} rows={}",
        dm_seq_nr,
        col_descs.len(),
        rows.len()
    ));
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;
    let tile_order = writer_tile_order(
        hypercolumn.name,
        col_descs,
        &col_data_types,
        hypercolumn.private_keywords,
    )?;
    // With no rows the cube's row axis is empty and it has no tiles, as
    // `TiledColumnStMan::create64` makes it (#764 review).
    if col_descs.is_empty() {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types,
            hypercolumn_name: hypercolumn.name.to_string(),
            max_cache_size: 0,
            nrdim: 0,
            files: vec![],
            cubes: vec![],
        };
        let variant = TiledVariant::Column {
            default_tile_shape: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    // Determine cell shape from the first column's schema.
    let cell_shape: Vec<usize> = col_descs[0].shape.iter().map(|&s| s as usize).collect();
    let cell_ndim = cell_shape.len();

    // Cube shape = cell_shape + [nrow].
    let mut cube_shape = cell_shape.clone();
    cube_shape.push(nrrow);

    let bytes_per_cell_element = col_data_types
        .iter()
        .copied()
        .map(tile_element_size)
        .try_fold(0usize, |sum, bytes| sum.checked_add(bytes))
        .ok_or_else(|| {
            StorageError::FormatMismatch("tiled column element bytes overflow".to_string())
        })?;

    let tile_shape = planned_tile_shape(
        &cell_shape,
        Some(nrrow),
        bytes_per_cell_element,
        user_tile_shape,
    )?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "layout",
            Some(format!(
                "cube_shape={} tile_shape={}",
                format_shape(&cube_shape),
                format_shape(&tile_shape)
            )),
        );
    }

    let (bucket_size, col_offsets) = bound_tile_layout(&col_data_types, &tile_order, &tile_shape);

    // Build per-column cube data (all rows concatenated).
    let nrdim = (cell_ndim + 1) as u32;
    let tiles_per_dim: Vec<usize> = (0..nrdim as usize)
        .map(|d| cube_shape[d].div_ceil(tile_shape[d]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let nrpixels: usize = tile_shape.iter().product();

    // Encode each column's full cube data (column-major, all rows).
    let mut col_cube_data: Vec<Vec<u8>> = Vec::with_capacity(col_descs.len());
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let dt = col_data_types[col_idx];
        let elem_size = tile_element_size(dt);
        let cube_nelem: usize = cube_shape.iter().product();
        let mut cube_bytes = vec![0u8; cube_nelem * elem_size];
        let cell_nelem: usize = cell_shape.iter().product();

        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(&col_desc.col_name) {
                let (encoded, _) = encode_array_value(value, big_endian)?;
                let start = row_idx * cell_nelem * elem_size;
                let end = start + cell_nelem * elem_size;
                if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                    cube_bytes[start..end].copy_from_slice(&encoded);
                }
            }
        }
        col_cube_data.push(cube_bytes);
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "encode_cube_data",
            Some(format!(
                "tiles={} bucket_size={} nrpixels={}",
                nr_tiles, bucket_size, nrpixels
            )),
        );
    }

    // Build tile data for the TSM file.
    let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..nrdim as usize)
            .map(|d| tile_pos[d] * tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..nrdim as usize)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        for (col_idx, _) in col_descs.iter().enumerate() {
            let dt = col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            let col_tile_bytes = tile_storage_bytes(dt, nrpixels);

            // Extract tile data from cube (reverse of copy_tile_to_cube).
            let mut tile_col = vec![0u8; nrpixels * elem_size];
            copy_cube_to_tile(
                &col_cube_data[col_idx],
                &cube_shape,
                &mut tile_col,
                &tile_shape,
                &cube_start,
                &actual_extent,
                elem_size,
            );

            let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
            write_tile_storage(
                &mut tsm_data[dst_start..dst_start + col_tile_bytes],
                dt,
                &tile_col,
                nrpixels,
            );
        }
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("assemble_tiles");
    }

    // Write TSM data file.
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_tsm_file");
    }

    // Build and write header.
    let default_ts_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: hypercolumn.name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: tsm_data.len() as i64,
        })],
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Column {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Save with `TiledShapeStMan` (one hypercube per unique array shape).
#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn save_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    hypercolumn: Hypercolumn<'_>,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::shape dm_seq={} cols={} rows={}",
        dm_seq_nr,
        col_descs.len(),
        rows.len()
    ));
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;
    let bytes_per_cell_element = col_data_types
        .iter()
        .copied()
        .map(tile_element_size)
        .try_fold(0usize, |sum, bytes| sum.checked_add(bytes))
        .ok_or_else(|| {
            StorageError::FormatMismatch("tiled shape element bytes overflow".to_string())
        })?;
    let tile_order = writer_tile_order(
        hypercolumn.name,
        col_descs,
        &col_data_types,
        hypercolumn.private_keywords,
    )?;

    if nrrow == 0 || col_descs.is_empty() {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: col_data_types.clone(),
            hypercolumn_name: hypercolumn.name.to_string(),
            max_cache_size: 0,
            nrdim: if col_descs.is_empty() {
                0
            } else {
                (col_descs[0].nrdim + 1) as u32
            },
            files: vec![],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Shape {
            default_tile_shape: vec![],
            nr_used_row_map: 0,
            row_map: vec![],
            cube_map: vec![],
            pos_map: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    // Group rows by array shape (using first column to determine shape).
    let first_col = &col_descs[0].col_name;
    let mut shape_groups: Vec<(Vec<usize>, Vec<usize>)> = Vec::new(); // (shape, row_indices)
    for (row_idx, row) in rows.iter().enumerate() {
        let shape = if let Some(Value::Array(av)) = row.get(first_col) {
            array_shape(av)
        } else {
            vec![]
        };
        if let Some(group) = shape_groups.iter_mut().find(|(s, _)| *s == shape) {
            group.1.push(row_idx);
        } else {
            shape_groups.push((shape, vec![row_idx]));
        }
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "group_shapes",
            Some(format!("shape_groups={}", shape_groups.len())),
        );
    }

    // Build cubes: cube 0 is dummy, cubes 1..N are real.
    let mut cubes = vec![TsmCubeInfo {
        values: RecordValue::default(),
        extensible: false,
        cube_shape: vec![],
        tile_shape: vec![],
        file_seq_nr: -1,
        file_offset: 0,
    }];
    let mut all_files = Vec::new();
    let mut row_locations = Vec::new();

    let nrdim = if col_descs[0].nrdim > 0 {
        (col_descs[0].nrdim + 1) as u32
    } else {
        2 // at least 2 for shape + row dim
    };

    for (group_idx, (cell_shape, group_rows)) in shape_groups.iter().enumerate() {
        let cube_idx = group_idx + 1; // cube 0 is dummy
        let n_in_cube = group_rows.len();

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(n_in_cube);

        let tile_shape = planned_tile_shape(
            cell_shape,
            Some(n_in_cube),
            bytes_per_cell_element,
            user_tile_shape,
        )?;

        let file_seq_nr = all_files.len() as u32;

        // Build tile data for this cube.
        let (bucket_size, col_offsets) =
            bound_tile_layout(&col_data_types, &tile_order, &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();

        // Encode column data for this cube.
        let cell_nelem: usize = cell_shape.iter().product();
        let mut col_cube_data: Vec<Vec<u8>> = Vec::with_capacity(col_descs.len());
        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            let dt = col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            let cube_nelem: usize = cube_shape.iter().product();
            let mut cube_bytes = vec![0u8; cube_nelem * elem_size];

            for (pos, &row_idx) in group_rows.iter().enumerate() {
                if let Some(value) = rows[row_idx].get(&col_desc.col_name) {
                    let (encoded, _) = encode_array_value(value, big_endian)?;
                    let start = pos * cell_nelem * elem_size;
                    let end = start + cell_nelem * elem_size;
                    if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                        cube_bytes[start..end].copy_from_slice(&encoded);
                    }
                }
            }
            col_cube_data.push(cube_bytes);
        }

        let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
        for tile_idx in 0..nr_tiles {
            let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
            let cube_start: Vec<usize> = cube_shape
                .iter()
                .zip(tile_pos.iter())
                .zip(tile_shape.iter())
                .map(|((_, &tp), &ts)| tp * ts)
                .collect();
            let actual_extent: Vec<usize> = cube_shape
                .iter()
                .zip(cube_start.iter())
                .zip(tile_shape.iter())
                .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                .collect();

            for (col_idx, _) in col_descs.iter().enumerate() {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let col_tile_bytes = tile_storage_bytes(dt, nrpixels);
                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &col_cube_data[col_idx],
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
                write_tile_storage(
                    &mut tsm_data[dst_start..dst_start + col_tile_bytes],
                    dt,
                    &tile_col,
                    nrpixels,
                );
            }
        }

        let tsm_path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
        std::fs::write(&tsm_path, &tsm_data)?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "write_cube",
                Some(format!(
                    "cube={} shape={} rows={} tile_shape={} bytes={}",
                    cube_idx,
                    format_shape(&cube_shape),
                    n_in_cube,
                    format_shape(&tile_shape),
                    tsm_data.len()
                )),
            );
        }

        all_files.push(Some(TsmFileInfo {
            seq_nr: file_seq_nr,
            length: tsm_data.len() as i64,
        }));

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: file_seq_nr as i32,
            file_offset: 0,
        });

        // Build row map entries — one per row for now; merged and sorted below.
        for (pos_in_cube, &row_idx) in group_rows.iter().enumerate() {
            row_locations.push((row_idx as u32, cube_idx as u32, pos_in_cube as u32));
        }
    }

    let (row_map_vec, cube_map_vec, pos_map_vec) = shape_row_intervals(&mut row_locations);
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail("row_map", Some(format!("intervals={}", row_map_vec.len())));
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else if let Some((first_shape, _)) = shape_groups.first() {
        default_tile_shape_for(first_shape, nrrow, bytes_per_cell_element)?
            .iter()
            .map(|&v| v as i32)
            .collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: hypercolumn.name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_ts_i32,
        nr_used_row_map: row_map_vec.len() as u32,
        row_map: row_map_vec,
        cube_map: cube_map_vec,
        pos_map: pos_map_vec,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Save with `TiledCellStMan` (one hypercube per row).
pub(super) fn save_tiled_cell_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    hypercolumn: Hypercolumn<'_>,
) -> Result<(), StorageError> {
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;
    let bytes_per_cell_element = col_data_types
        .iter()
        .copied()
        .map(tile_element_size)
        .try_fold(0usize, |sum, bytes| sum.checked_add(bytes))
        .ok_or_else(|| {
            StorageError::FormatMismatch("tiled cell element bytes overflow".to_string())
        })?;
    let tile_order = writer_tile_order(
        hypercolumn.name,
        col_descs,
        &col_data_types,
        hypercolumn.private_keywords,
    )?;

    let nrdim = if !col_descs.is_empty() && col_descs[0].nrdim > 0 {
        col_descs[0].nrdim as u32
    } else {
        1
    };

    let mut cubes = Vec::with_capacity(nrrow);
    let mut all_files: Vec<Option<TsmFileInfo>> = vec![Some(TsmFileInfo {
        seq_nr: 0,
        length: 0,
    })];
    let mut tsm_data = Vec::new(); // All non-extensible cubes share file 0.

    for row in rows.iter() {
        // Determine cell shape from the first column.
        let first_col = &col_descs[0].col_name;
        let cell_shape = if let Some(Value::Array(av)) = row.get(first_col) {
            array_shape(av)
        } else {
            cubes.push(TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            });
            continue;
        };

        if cell_shape.is_empty() {
            cubes.push(TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            });
            continue;
        }

        let cube_shape = cell_shape.clone();
        let tile_shape =
            planned_tile_shape(&cell_shape, None, bytes_per_cell_element, user_tile_shape)?;

        let (bucket_size, col_offsets) =
            bound_tile_layout(&col_data_types, &tile_order, &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();
        let cell_nelem: usize = cell_shape.iter().product();

        let file_offset = tsm_data.len() as i64;
        let mut tile_data = vec![0u8; nr_tiles * bucket_size];

        // Pre-encode each column's data once (outside the tile loop).
        let encoded_cols: Vec<Vec<u8>> = col_descs
            .iter()
            .enumerate()
            .map(|(col_idx, col_desc)| {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let mut cube_bytes = vec![0u8; cell_nelem * elem_size];
                if let Some(value) = row.get(&col_desc.col_name)
                    && let Ok((encoded, _)) = encode_array_value(value, big_endian)
                    && encoded.len() == cube_bytes.len()
                {
                    cube_bytes = encoded;
                }
                cube_bytes
            })
            .collect();

        for tile_idx in 0..nr_tiles {
            let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
            let cube_start: Vec<usize> = cube_shape
                .iter()
                .zip(tile_pos.iter())
                .zip(tile_shape.iter())
                .map(|((_, &tp), &ts)| tp * ts)
                .collect();
            let actual_extent: Vec<usize> = cube_shape
                .iter()
                .zip(cube_start.iter())
                .zip(tile_shape.iter())
                .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                .collect();

            for (col_idx, _col_desc) in col_descs.iter().enumerate() {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let col_tile_bytes = tile_storage_bytes(dt, nrpixels);

                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &encoded_cols[col_idx],
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
                write_tile_storage(
                    &mut tile_data[dst_start..dst_start + col_tile_bytes],
                    dt,
                    &tile_col,
                    nrpixels,
                );
            }
        }

        tsm_data.extend_from_slice(&tile_data);

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: false,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset,
        });
    }

    // Write TSM data file.
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(ref mut f) = all_files[0] {
        f.length = tsm_data.len() as i64;
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: hypercolumn.name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Cell {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;

    Ok(())
}
