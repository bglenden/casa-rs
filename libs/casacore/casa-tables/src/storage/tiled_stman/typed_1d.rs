// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn load_tiled_column_rows_1d_typed(
    table_path: &Path,
    read_metadata: &TiledReadMetadata,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
) -> Result<SelectedArray1DCells, StorageError> {
    let TiledReadHeader {
        variant, header, ..
    } = read_metadata.header(dm.seq_nr)?;
    let (tile_header, target_col_idx) = tile_target(
        variant,
        Cow::Borrowed(header),
        table_desc,
        bound_cols,
        target_desc_idx,
        (dm.seq_nr, selected_rows),
    )?;
    let header = &*tile_header;
    let col_desc = &table_desc.columns[target_desc_idx];
    let dt = header.col_data_types[target_col_idx];
    let elem_size = tile_element_size(dt);
    if elem_size == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read does not support zero-sized element type for column {}",
            col_desc.col_name
        )));
    }

    match variant {
        TiledVariant::Column { .. } => {
            if header.cubes.is_empty() {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected 1-D read for {} found no TiledColumnStMan cube",
                    col_desc.col_name
                )));
            }
            let patches_by_cube = std::iter::once((
                0usize,
                selected_rows
                    .iter()
                    .enumerate()
                    .map(|(out_idx, &pos_in_cube)| SelectedCubeRow {
                        out_idx,
                        pos_in_cube,
                    })
                    .collect(),
            ))
            .collect();
            load_tiled_column_rows_from_patches_1d_typed(
                table_path,
                dm.seq_nr,
                header,
                target_col_idx,
                col_desc,
                dt,
                elem_size,
                selected_rows.len(),
                patches_by_cube,
            )
        }
        TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } => load_tiled_column_rows_shape_variant_1d_typed(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            &ShapeRowMapping {
                nr_used_row_map: *nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        _ => Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D reads for column {} require TiledColumnStMan or TiledShapeStMan",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fill_tiled_column_rows_1d_typed(
    table_path: &Path,
    read_metadata: &TiledReadMetadata,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
    destination: SelectedArray1DCellsMut<'_>,
) -> Result<SelectedArray1DShape, StorageError> {
    let TiledReadHeader {
        variant, header, ..
    } = read_metadata.header(dm.seq_nr)?;
    let (tile_header, target_col_idx) = tile_target(
        variant,
        Cow::Borrowed(header),
        table_desc,
        bound_cols,
        target_desc_idx,
        (dm.seq_nr, selected_rows),
    )?;
    let header = &*tile_header;
    let col_desc = &table_desc.columns[target_desc_idx];
    let dt = header.col_data_types[target_col_idx];
    let elem_size = tile_element_size(dt);
    if elem_size == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read does not support zero-sized element type for column {}",
            col_desc.col_name
        )));
    }
    match variant {
        TiledVariant::Column { .. } => {
            if header.cubes.is_empty() {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected 1-D read for {} found no TiledColumnStMan cube",
                    col_desc.col_name
                )));
            }
            let patches_by_cube = std::iter::once((
                0usize,
                selected_rows
                    .iter()
                    .enumerate()
                    .map(|(out_idx, &pos_in_cube)| SelectedCubeRow {
                        out_idx,
                        pos_in_cube,
                    })
                    .collect(),
            ))
            .collect();
            fill_tiled_column_rows_from_patches_1d_typed(
                table_path,
                dm.seq_nr,
                header,
                target_col_idx,
                col_desc,
                dt,
                elem_size,
                selected_rows.len(),
                patches_by_cube,
                destination,
            )
        }
        TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } => fill_tiled_column_rows_shape_variant_1d_typed(
            table_path,
            dm.seq_nr,
            header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            &ShapeRowMapping {
                nr_used_row_map: *nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
            destination,
        ),
        _ => Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D reads for column {} require TiledColumnStMan or TiledShapeStMan",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_shape_variant_1d_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
) -> Result<SelectedArray1DCells, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} found an empty row map",
            col_desc.col_name
        )));
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>> =
        std::collections::BTreeMap::new();
    let mut mapped_rows = 0usize;
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let Some((cube_idx, pos_in_cube)) = shape_row_location(header, mapping, row_idx)? else {
            continue;
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
        mapped_rows += 1;
    }
    if mapped_rows != selected_rows.len() {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} mapped {} of {} requested rows",
            col_desc.col_name,
            mapped_rows,
            selected_rows.len()
        )));
    }

    load_tiled_column_rows_from_patches_1d_typed(
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

#[allow(clippy::too_many_arguments)]
pub(super) fn fill_tiled_column_rows_shape_variant_1d_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
    destination: SelectedArray1DCellsMut<'_>,
) -> Result<SelectedArray1DShape, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} found an empty row map",
            col_desc.col_name
        )));
    }
    let mut patches_by_cube = std::collections::BTreeMap::<usize, Vec<SelectedCubeRow>>::new();
    let mut mapped_rows = 0usize;
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let Some((cube_idx, pos_in_cube)) = shape_row_location(header, mapping, row_idx)? else {
            continue;
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
        mapped_rows += 1;
    }
    if mapped_rows != selected_rows.len() {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} mapped {} of {} requested rows",
            col_desc.col_name,
            mapped_rows,
            selected_rows.len()
        )));
    }
    fill_tiled_column_rows_from_patches_1d_typed(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
        destination,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_from_patches_1d_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    row_count: usize,
    patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>>,
) -> Result<SelectedArray1DCells, StorageError> {
    let mut axis0_count = None::<usize>;
    for (&cube_idx, patches) in &patches_by_cube {
        if patches.is_empty() {
            continue;
        }
        let cube = header.cubes.get(cube_idx).ok_or_else(|| {
            StorageError::FormatMismatch(format!("typed selected 1-D read missing cube {cube_idx}"))
        })?;
        let shape = typed_1d_cube_shape(cube)?;
        match axis0_count {
            Some(expected) if expected != shape.axis0_count => {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected 1-D read for {} found mixed axis-0 lengths: {} and {expected}",
                    col_desc.col_name, shape.axis0_count
                )));
            }
            None => axis0_count = Some(shape.axis0_count),
            _ => {}
        }
    }
    let axis0_count = axis0_count.ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} found no selected rows",
            col_desc.col_name
        ))
    })?;
    let sample_count = row_count.saturating_mul(axis0_count);
    let mut session = TileReadSession::default();
    let fill_plan = TypedSelected1DFillPlan {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        dt,
        elem_size,
        patches_by_cube: &patches_by_cube,
        row_count,
        axis0_count,
    };

    match dt {
        CasacoreDataType::TpBool => {
            let mut values = vec![false; sample_count];
            fill_typed_selected_1d_rows(
                fill_plan,
                &mut session,
                &mut values,
                |bytes, _big_endian| bytes[0] != 0,
            )?;
            Ok(SelectedArray1DCells::Bool(SelectedArray1D::new(
                row_count,
                axis0_count,
                values,
            )))
        }
        CasacoreDataType::TpFloat => {
            let mut values = vec![0.0f32; sample_count];
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(SelectedArray1DCells::Float32(SelectedArray1D::new(
                row_count,
                axis0_count,
                values,
            )))
        }
        CasacoreDataType::TpDouble => {
            let mut values = vec![0.0f64; sample_count];
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(SelectedArray1DCells::Float64(SelectedArray1D::new(
                row_count,
                axis0_count,
                values,
            )))
        }
        CasacoreDataType::TpComplex => {
            let mut values = vec![Complex32::new(0.0, 0.0); sample_count];
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(SelectedArray1DCells::Complex32(SelectedArray1D::new(
                row_count,
                axis0_count,
                values,
            )))
        }
        CasacoreDataType::TpDComplex => {
            let mut values = vec![Complex64::new(0.0, 0.0); sample_count];
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(SelectedArray1DCells::Complex64(SelectedArray1D::new(
                row_count,
                axis0_count,
                values,
            )))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read does not support {other:?} for column {}",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fill_tiled_column_rows_from_patches_1d_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    row_count: usize,
    patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>>,
    destination: SelectedArray1DCellsMut<'_>,
) -> Result<SelectedArray1DShape, StorageError> {
    let mut axis0_count = None::<usize>;
    for (&cube_idx, patches) in &patches_by_cube {
        if patches.is_empty() {
            continue;
        }
        let cube = header.cubes.get(cube_idx).ok_or_else(|| {
            StorageError::FormatMismatch(format!("typed selected 1-D read missing cube {cube_idx}"))
        })?;
        let shape = typed_1d_cube_shape(cube)?;
        match axis0_count {
            Some(expected) if expected != shape.axis0_count => {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected 1-D read for {} found mixed axis-0 lengths: {} and {expected}",
                    col_desc.col_name, shape.axis0_count
                )));
            }
            None => axis0_count = Some(shape.axis0_count),
            _ => {}
        }
    }
    let axis0_count = axis0_count.ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "typed selected 1-D read for {} found no selected rows",
            col_desc.col_name
        ))
    })?;
    let sample_count = row_count.saturating_mul(axis0_count);
    let mut session = TileReadSession::default();
    let fill_plan = TypedSelected1DFillPlan {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        dt,
        elem_size,
        patches_by_cube: &patches_by_cube,
        row_count,
        axis0_count,
    };
    match (dt, destination) {
        (CasacoreDataType::TpBool, SelectedArray1DCellsMut::Bool(values)) => {
            values.resize(sample_count, false);
            fill_typed_selected_1d_rows(fill_plan, &mut session, values, |bytes, _| bytes[0] != 0)?;
        }
        (CasacoreDataType::TpFloat, SelectedArray1DCellsMut::Float32(values)) => {
            values.resize(sample_count, 0.0);
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (CasacoreDataType::TpDouble, SelectedArray1DCellsMut::Float64(values)) => {
            values.resize(sample_count, 0.0);
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (CasacoreDataType::TpComplex, SelectedArray1DCellsMut::Complex32(values)) => {
            values.resize(sample_count, Complex32::new(0.0, 0.0));
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (CasacoreDataType::TpDComplex, SelectedArray1DCellsMut::Complex64(values)) => {
            values.resize(sample_count, Complex64::new(0.0, 0.0));
            fill_typed_selected_1d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (actual, destination) => {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 1-D destination {:?} does not match {actual:?} column {}",
                destination.primitive_type(),
                col_desc.col_name
            )));
        }
    }
    Ok(SelectedArray1DShape {
        row_count,
        axis0_count,
    })
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Typed1DCubeShape {
    pub(super) axis0_count: usize,
}

#[derive(Clone, Copy)]
pub(super) struct TypedSelected1DFillPlan<'a> {
    pub(super) table_path: &'a Path,
    pub(super) dm_seq_nr: u32,
    pub(super) header: &'a TiledStManHeader,
    pub(super) target_col_idx: usize,
    pub(super) dt: CasacoreDataType,
    pub(super) elem_size: usize,
    pub(super) patches_by_cube: &'a std::collections::BTreeMap<usize, Vec<SelectedCubeRow>>,
    pub(super) row_count: usize,
    pub(super) axis0_count: usize,
}

impl TypedSelected1DFillPlan<'_> {
    /// The tiles of cube `cube_idx` this read walks, or `None` for a cube
    /// that holds no data.
    fn tiles(&self, cube_idx: usize) -> Result<Option<SelectedTiles<'_>>, StorageError> {
        let Some(cube) = self.header.cubes.get(cube_idx) else {
            return Ok(None);
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            return Ok(None);
        }
        typed_1d_cube_shape(cube)?;
        SelectedTiles::new(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.target_col_idx,
            self.dt,
            cube_idx,
            cube,
        )
        .map(Some)
    }
}

/// Refuse a cube whose tile shape is not of its rank, which the typed
/// shape checks below would otherwise slice past.
pub(super) fn check_tile_rank(cube: &TsmCubeInfo) -> Result<(), StorageError> {
    if cube.tile_shape.len() == cube.cube_shape.len() {
        return Ok(());
    }
    Err(StorageError::FormatMismatch(format!(
        "typed selected read found tile shape {:?} for cube shape {:?}",
        cube.tile_shape, cube.cube_shape
    )))
}

/// The rank-1 cell shape of `cube`. Its axis may lie in several tiles
/// ([`SelectedTiles`]).
pub(super) fn typed_1d_cube_shape(cube: &TsmCubeInfo) -> Result<Typed1DCubeShape, StorageError> {
    check_tile_rank(cube)?;
    let cell_ndim = cube.cube_shape.len().saturating_sub(1);
    let cell_shape = &cube.cube_shape[..cell_ndim];
    let cell_tile_shape = &cube.tile_shape[..cell_ndim];
    if cell_shape.len() != 1 || cell_tile_shape.len() != 1 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 1-D read requires rank-1 cells, found cell_shape={cell_shape:?}"
        )));
    }
    Ok(Typed1DCubeShape {
        axis0_count: cell_shape[0],
    })
}

/// Decode each selected row's cell into `values` (`[row, axis 0]`),
/// gathering it from every tile along axis 0 that holds a part.
pub(super) fn fill_typed_selected_1d_rows<T: Copy>(
    plan: TypedSelected1DFillPlan<'_>,
    session: &mut TileReadSession,
    values: &mut [T],
    decode: fn(&[u8], bool) -> T,
) -> Result<(), StorageError> {
    let elem_size = plan.elem_size;
    for (&cube_idx, patches) in plan.patches_by_cube {
        let Some(tiles) = plan.tiles(cube_idx)? else {
            continue;
        };
        for (row_tile, row_patches) in tiles.rows_by_tile(patches) {
            for span in tiles.axis0_spans() {
                let tile = tiles.shared(tiles.tile_index(&[span.tile, row_tile]), session)?;
                for &selected in &row_patches {
                    let src = tiles.row_offset(selected) * elem_size;
                    let dst = selected.out_idx * plan.axis0_count + span.start;
                    for (axis0, value) in values[dst..dst + span.len].iter_mut().enumerate() {
                        let byte = src + axis0 * elem_size;
                        *value = decode(&tile[byte..byte + elem_size], SHARED_TILE_BIG_ENDIAN);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Copy each selected row's cell into `values` (`[row, axis 0]`), a run of
/// elements from every tile along axis 0 that holds a part.
pub(super) fn fill_typed_selected_1d_rows_by_copy<T: TilePixel>(
    plan: TypedSelected1DFillPlan<'_>,
    session: &mut TileReadSession,
    values: &mut [T],
) -> Result<(), StorageError> {
    debug_assert_eq!(plan.elem_size, T::ELEM_SIZE);
    let elem_size = plan.elem_size;
    let values_bytes = tile_values_as_bytes_mut(values);
    for (&cube_idx, patches) in plan.patches_by_cube {
        let Some(tiles) = plan.tiles(cube_idx)? else {
            continue;
        };
        for (row_tile, row_patches) in tiles.rows_by_tile(patches) {
            for span in tiles.axis0_spans() {
                let tile = tiles.shared(tiles.tile_index(&[span.tile, row_tile]), session)?;
                for &selected in &row_patches {
                    let src_byte = tiles.row_offset(selected) * elem_size;
                    let dst_byte = (selected.out_idx * plan.axis0_count + span.start) * elem_size;
                    copy_pixels::<T>(values_bytes, dst_byte, &tile, src_byte, span.len);
                }
            }
        }
    }
    Ok(())
}
