// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn load_tiled_column_rows_2d_channel_range_typed(
    table_path: &Path,
    read_metadata: &TiledReadMetadata,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
    channel_start: usize,
    channel_count: usize,
) -> Result<Option<SelectedArray2DCells>, StorageError> {
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
            "typed selected 2-D read does not support zero-sized element type for column {}",
            col_desc.col_name
        )));
    }

    match variant {
        TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } => load_tiled_column_rows_shape_variant_2d_channel_range_typed(
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
            channel_start,
            channel_count,
        ),
        _ => Err(StorageError::FormatMismatch(format!(
            "typed selected 2-D channel reads for column {} require TiledShapeStMan Shape variant",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fill_tiled_column_rows_2d_channel_range_typed(
    table_path: &Path,
    read_metadata: &TiledReadMetadata,
    dm: &DataManagerEntry,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
    channel_start: usize,
    channel_count: usize,
    destination: SelectedArray2DCellsMut<'_>,
) -> Result<Option<SelectedArray2DShape>, StorageError> {
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
            "typed selected 2-D read does not support zero-sized element type for column {}",
            col_desc.col_name
        )));
    }
    match variant {
        TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } => fill_tiled_column_rows_shape_variant_2d_channel_range_typed(
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
            channel_start,
            channel_count,
            destination,
            false,
            false,
        ),
        _ => Err(StorageError::FormatMismatch(format!(
            "typed selected 2-D channel reads for column {} require TiledShapeStMan Shape variant",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_tiled_column_rows_shape_variant_2d_channel_range_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
    channel_start: usize,
    channel_count: usize,
) -> Result<Option<SelectedArray2DCells>, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(None);
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
            "typed selected read for {} mapped {} of {} requested rows",
            col_desc.col_name,
            mapped_rows,
            selected_rows.len()
        )));
    }

    let mut corr_count = None::<usize>;
    for (&cube_idx, patches) in &patches_by_cube {
        if patches.is_empty() {
            continue;
        }
        let cube = header.cubes.get(cube_idx).ok_or_else(|| {
            StorageError::FormatMismatch(format!("typed selected read missing cube {cube_idx}"))
        })?;
        let shape = typed_2d_cube_shape(cube, channel_start, channel_count)?;
        match corr_count {
            Some(expected) if expected != shape.corr_count => {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected read for {} found mixed axis-0 lengths: {} and {expected}",
                    col_desc.col_name, shape.corr_count
                )));
            }
            None => corr_count = Some(shape.corr_count),
            _ => {}
        }
    }
    let corr_count = corr_count.ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "typed selected read for {} found no selected rows",
            col_desc.col_name
        ))
    })?;
    let sample_count = channel_count
        .saturating_mul(selected_rows.len())
        .saturating_mul(corr_count);
    let mut session = TileReadSession::default();
    let fill_plan = TypedSelected2DFillPlan {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        column_name: &col_desc.col_name,
        dt,
        elem_size,
        patches_by_cube: &patches_by_cube,
        row_count: selected_rows.len(),
        corr_count,
        channel_start,
        channel_count,
        row_major: false,
        reuse_sparse_tiles: false,
    };

    match dt {
        CasacoreDataType::TpBool => {
            let mut values = vec![false; sample_count];
            fill_typed_selected_2d_rows(fill_plan, &mut session, &mut values, |bytes, _| {
                bytes[0] != 0
            })?;
            Ok(Some(SelectedArray2DCells::Bool(SelectedArray2D::new(
                selected_rows.len(),
                corr_count,
                channel_count,
                values,
            ))))
        }
        CasacoreDataType::TpFloat => {
            let mut values = vec![0.0f32; sample_count];
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(Some(SelectedArray2DCells::Float32(SelectedArray2D::new(
                selected_rows.len(),
                corr_count,
                channel_count,
                values,
            ))))
        }
        CasacoreDataType::TpDouble => {
            let mut values = vec![0.0f64; sample_count];
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(Some(SelectedArray2DCells::Float64(SelectedArray2D::new(
                selected_rows.len(),
                corr_count,
                channel_count,
                values,
            ))))
        }
        CasacoreDataType::TpComplex => {
            let mut values = vec![Complex32::new(0.0, 0.0); sample_count];
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(Some(SelectedArray2DCells::Complex32(SelectedArray2D::new(
                selected_rows.len(),
                corr_count,
                channel_count,
                values,
            ))))
        }
        CasacoreDataType::TpDComplex => {
            let mut values = vec![Complex64::new(0.0, 0.0); sample_count];
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, &mut values)?;
            Ok(Some(SelectedArray2DCells::Complex64(SelectedArray2D::new(
                selected_rows.len(),
                corr_count,
                channel_count,
                values,
            ))))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "typed selected 2-D read does not support {other:?} for column {}",
            col_desc.col_name
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fill_tiled_column_rows_shape_variant_2d_channel_range_typed(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
    channel_start: usize,
    channel_count: usize,
    destination: SelectedArray2DCellsMut<'_>,
    widen_complex: bool,
    reuse_sparse_tiles: bool,
) -> Result<Option<SelectedArray2DShape>, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(None);
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
            "typed selected read for {} mapped {} of {} requested rows",
            col_desc.col_name,
            mapped_rows,
            selected_rows.len()
        )));
    }
    let mut corr_count = None::<usize>;
    for (&cube_idx, patches) in &patches_by_cube {
        if patches.is_empty() {
            continue;
        }
        let cube = header.cubes.get(cube_idx).ok_or_else(|| {
            StorageError::FormatMismatch(format!("typed selected read missing cube {cube_idx}"))
        })?;
        let shape = typed_2d_cube_shape(cube, channel_start, channel_count)?;
        match corr_count {
            Some(expected) if expected != shape.corr_count => {
                return Err(StorageError::FormatMismatch(format!(
                    "typed selected read for {} found mixed axis-0 lengths: {} and {expected}",
                    col_desc.col_name, shape.corr_count
                )));
            }
            None => corr_count = Some(shape.corr_count),
            _ => {}
        }
    }
    let corr_count = corr_count.ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "typed selected read for {} found no selected rows",
            col_desc.col_name
        ))
    })?;
    let sample_count = channel_count
        .saturating_mul(selected_rows.len())
        .saturating_mul(corr_count);
    let mut session = TileReadSession::default();
    let fill_plan = TypedSelected2DFillPlan {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        column_name: &col_desc.col_name,
        dt,
        elem_size,
        patches_by_cube: &patches_by_cube,
        row_count: selected_rows.len(),
        corr_count,
        channel_start,
        channel_count,
        reuse_sparse_tiles,
        row_major: widen_complex
            || matches!(
                &destination,
                SelectedArray2DCellsMut::RowChannelBool(_)
                    | SelectedArray2DCellsMut::RowChannelFloat32(_)
                    | SelectedArray2DCellsMut::RowChannelComplex32(_)
            ),
    };
    match (dt, destination) {
        (
            CasacoreDataType::TpBool,
            SelectedArray2DCellsMut::Bool(values) | SelectedArray2DCellsMut::RowChannelBool(values),
        ) => {
            values.resize(sample_count, false);
            let unpacked = patches_by_cube
                .keys()
                .all(|&cube| header.cubes[cube].tile_shape.iter().product::<usize>() > 1);
            if unpacked {
                // Both tile loaders expand packed booleans into bytes 0 or 1
                // when a tile has more than one pixel. Those bytes are valid
                // bool representations, so the typed copy can place them
                // directly in the destination. A one-pixel tile can instead
                // contain its packed byte and must be decoded below.
                fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, values)?;
            } else {
                fill_typed_selected_2d_rows(fill_plan, &mut session, values, |bytes, _| {
                    bytes[0] != 0
                })?;
            }
        }
        (
            CasacoreDataType::TpFloat,
            SelectedArray2DCellsMut::Float32(values)
            | SelectedArray2DCellsMut::RowChannelFloat32(values),
        ) => {
            values.resize(sample_count, 0.0);
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (CasacoreDataType::TpDouble, SelectedArray2DCellsMut::Float64(values)) => {
            values.resize(sample_count, 0.0);
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (
            CasacoreDataType::TpComplex,
            SelectedArray2DCellsMut::Complex32(values)
            | SelectedArray2DCellsMut::RowChannelComplex32(values),
        ) => {
            values.resize(sample_count, Complex32::new(0.0, 0.0));
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (CasacoreDataType::TpComplex, SelectedArray2DCellsMut::Complex64(values))
            if widen_complex =>
        {
            values.resize(sample_count, Complex64::default());
            fill_typed_selected_2d_rows(fill_plan, &mut session, values, |bytes, big| {
                let value = decode_complex32_scalar(bytes, big);
                Complex64::new(f64::from(value.re), f64::from(value.im))
            })?;
        }
        (CasacoreDataType::TpDComplex, SelectedArray2DCellsMut::Complex64(values)) => {
            values.resize(sample_count, Complex64::new(0.0, 0.0));
            fill_typed_selected_2d_rows_by_copy(fill_plan, &mut session, values)?;
        }
        (actual, destination) => {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 2-D destination {:?} does not match {actual:?} column {}",
                destination.primitive_type(),
                col_desc.col_name
            )));
        }
    }
    Ok(Some(SelectedArray2DShape {
        row_count: selected_rows.len(),
        axis0_count: corr_count,
        channel_count,
    }))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Typed2DCubeShape {
    pub(super) corr_count: usize,
    pub(super) tile_channel_count: usize,
}

#[derive(Clone, Copy)]
pub(super) struct TypedSelected2DFillPlan<'a> {
    pub(super) table_path: &'a Path,
    pub(super) dm_seq_nr: u32,
    pub(super) header: &'a TiledStManHeader,
    pub(super) target_col_idx: usize,
    pub(super) column_name: &'a str,
    pub(super) dt: CasacoreDataType,
    pub(super) elem_size: usize,
    pub(super) patches_by_cube: &'a std::collections::BTreeMap<usize, Vec<SelectedCubeRow>>,
    pub(super) row_count: usize,
    pub(super) corr_count: usize,
    pub(super) channel_start: usize,
    pub(super) channel_count: usize,
    pub(super) row_major: bool,
    pub(super) reuse_sparse_tiles: bool,
}

impl TypedSelected2DFillPlan<'_> {
    /// The tiles of cube `cube_idx` this read walks and the cube's cell
    /// shape, or `None` for a cube that holds no data.
    fn tiles(
        &self,
        cube_idx: usize,
    ) -> Result<Option<(SelectedTiles<'_>, Typed2DCubeShape)>, StorageError> {
        let Some(cube) = self.header.cubes.get(cube_idx) else {
            return Ok(None);
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            return Ok(None);
        }
        let shape = typed_2d_cube_shape(cube, self.channel_start, self.channel_count)?;
        let tiles = SelectedTiles::new(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.target_col_idx,
            self.dt,
            cube_idx,
            cube,
        )?;
        Ok(Some((tiles, shape)))
    }

    /// The selected channel tiles of a cube whose channel tiles are
    /// `tile_channel_count` long: first and last.
    fn channel_tiles(&self, tile_channel_count: usize) -> (usize, usize) {
        let channel_end = self.channel_start + self.channel_count;
        (
            self.channel_start / tile_channel_count,
            channel_end.saturating_sub(1) / tile_channel_count,
        )
    }

    fn stream_tiles(
        &self,
        tiles: &SelectedTiles<'_>,
        row_tiles: usize,
        channel_tiles: usize,
        selected_rows: usize,
    ) -> bool {
        let count = row_tiles
            .saturating_mul(channel_tiles)
            .saturating_mul(tiles.axis0_tiles());
        if count <= TILE_READ_SESSION_RECENT_TILE_COUNT {
            return false;
        }
        // Sparse whole-cell reads can revisit the same physical tiles when a
        // tool reads different products of an open selection. Reuse the
        // existing bounded cache for slices occupying a small share of it.
        !self.reuse_sparse_tiles
            || !tiles.fits_sparse_read_cache(
                count,
                row_tiles,
                selected_rows,
                self.elem_size,
                table_cache_budget_bytes() / 2,
            )
    }

    /// The output sample of `out_idx`'s selected channel `channel`, by the
    /// destination's order (`[channel, row]` or `[row, channel]`).
    fn output_sample(&self, out_idx: usize, channel: usize) -> usize {
        let channel = channel - self.channel_start;
        if self.row_major {
            out_idx * self.channel_count + channel
        } else {
            channel * self.row_count + out_idx
        }
    }
}

/// The rank-2 cell shape of `cube`, whose channel axis holds the selected
/// `channel_start..channel_start + channel_count`. Its axis 0 may lie in
/// several tiles ([`SelectedTiles`]).
pub(super) fn typed_2d_cube_shape(
    cube: &TsmCubeInfo,
    channel_start: usize,
    channel_count: usize,
) -> Result<Typed2DCubeShape, StorageError> {
    check_tile_rank(cube)?;
    let cell_ndim = cube.cube_shape.len().saturating_sub(1);
    let cell_shape = &cube.cube_shape[..cell_ndim];
    let cell_tile_shape = &cube.tile_shape[..cell_ndim];
    if cell_shape.len() != 2 || cell_tile_shape.len() != 2 {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 2-D read requires rank-2 cells, found cell_shape={cell_shape:?}"
        )));
    }
    let channel_end = channel_start.checked_add(channel_count).ok_or_else(|| {
        StorageError::FormatMismatch(
            "typed selected 2-D read overflowed channel bounds".to_string(),
        )
    })?;
    if channel_end > cell_shape[1] {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected 2-D channel range {channel_start}..{channel_end} exceeds cell channel axis with {} channels",
            cell_shape[1]
        )));
    }
    if cell_tile_shape[1] == 0 {
        return Err(StorageError::FormatMismatch(
            "typed selected 2-D read found zero-width channel tile".to_string(),
        ));
    }
    Ok(Typed2DCubeShape {
        corr_count: cell_shape[0],
        tile_channel_count: cell_tile_shape[1],
    })
}

/// Decode each selected row's channel range into `values`, gathering each
/// channel's correlations from every tile along axis 0 that holds a part.
pub(super) fn fill_typed_selected_2d_rows<T: Copy>(
    plan: TypedSelected2DFillPlan<'_>,
    session: &mut TileReadSession,
    values: &mut [T],
    decode: fn(&[u8], bool) -> T,
) -> Result<(), StorageError> {
    let profile = typed_2d_read_profile_enabled();
    let profile_started = profile.then(Instant::now);
    let mut profile_tile_calls = 0usize;
    let mut profile_tile_bytes = 0usize;
    let mut profile_read = Duration::ZERO;
    let mut profile_decode = Duration::ZERO;
    let mut streamed_tile = Vec::new();
    let elem_size = plan.elem_size;
    let channel_end = plan.channel_start + plan.channel_count;
    for (&cube_idx, patches) in plan.patches_by_cube {
        let Some((tiles, shape)) = plan.tiles(cube_idx)? else {
            continue;
        };
        let (first_channel_tile, last_channel_tile) = plan.channel_tiles(shape.tile_channel_count);
        let rows_by_tile = tiles.rows_by_tile(patches);
        // No channel selected leaves the last tile before the first.
        let channel_tile_count = (last_channel_tile + 1).saturating_sub(first_channel_tile);
        let stream_tiles = plan.stream_tiles(
            &tiles,
            rows_by_tile.len(),
            channel_tile_count,
            patches.len(),
        );

        for (row_tile, row_patches) in rows_by_tile {
            for channel_tile in first_channel_tile..=last_channel_tile {
                let tile_channel_start = channel_tile * shape.tile_channel_count;
                let overlap_start = std::cmp::max(plan.channel_start, tile_channel_start);
                let overlap_end =
                    std::cmp::min(channel_end, tile_channel_start + shape.tile_channel_count);
                if overlap_start >= overlap_end {
                    continue;
                }
                for span in tiles.axis0_spans() {
                    let read_started = profile.then(Instant::now);
                    let run = stream_tiles
                        .then(|| tiles.run_from(span, last_channel_tile - channel_tile));
                    let shared_tile = tiles.load(
                        tiles.tile_index(&[span.tile, channel_tile, row_tile]),
                        run,
                        session,
                        &mut streamed_tile,
                    )?;
                    let tile = shared_tile.as_deref().unwrap_or(&streamed_tile);
                    if let Some(started) = read_started {
                        profile_read += started.elapsed();
                        profile_tile_calls += 1;
                        profile_tile_bytes = profile_tile_bytes.saturating_add(tile.len());
                    }

                    let decode_started = profile.then(Instant::now);
                    for &selected in &row_patches {
                        let row = tiles.row_offset(selected);
                        for channel in overlap_start..overlap_end {
                            let src = row + (channel - tile_channel_start) * tiles.tile_axis0_count;
                            let dst = plan.output_sample(selected.out_idx, channel)
                                * plan.corr_count
                                + span.start;
                            for (corr, value) in values[dst..dst + span.len].iter_mut().enumerate()
                            {
                                let byte = (src + corr) * elem_size;
                                *value =
                                    decode(&tile[byte..byte + elem_size], SHARED_TILE_BIG_ENDIAN);
                            }
                        }
                    }
                    if let Some(started) = decode_started {
                        profile_decode += started.elapsed();
                    }
                }
            }
        }
    }
    if let Some(started) = profile_started {
        tracing::debug!(
            target: TYPED_2D_READ_PROFILE,
            "typed_2d_read_profile column={} mode=decode rows={} channels={} corr={} cubes={} tile_calls={} tile_bytes={} value_bytes={} read_ms={:.3} decode_ms={:.3} total_ms={:.3}",
            plan.column_name,
            plan.row_count,
            plan.channel_count,
            plan.corr_count,
            plan.patches_by_cube.len(),
            profile_tile_calls,
            profile_tile_bytes,
            values.len().saturating_mul(std::mem::size_of::<T>()),
            duration_ms_f64(profile_read),
            duration_ms_f64(profile_decode),
            duration_ms_f64(started.elapsed())
        );
    }
    Ok(())
}

/// Copy each selected row's channel range into `values`, each channel's
/// correlations a run from every tile along axis 0 that holds a part.
pub(super) fn fill_typed_selected_2d_rows_by_copy<T: TilePixel>(
    plan: TypedSelected2DFillPlan<'_>,
    session: &mut TileReadSession,
    values: &mut [T],
) -> Result<(), StorageError> {
    debug_assert_eq!(plan.elem_size, T::ELEM_SIZE);
    let profile = typed_2d_read_profile_enabled();
    let profile_started = profile.then(Instant::now);
    let mut profile_tile_calls = 0usize;
    let mut profile_tile_bytes = 0usize;
    let mut profile_read = Duration::ZERO;
    let mut profile_copy = Duration::ZERO;
    let mut streamed_tile = Vec::new();
    let elem_size = plan.elem_size;
    let channel_end = plan.channel_start + plan.channel_count;
    let values_bytes = tile_values_as_bytes_mut(values);
    for (&cube_idx, patches) in plan.patches_by_cube {
        let Some((tiles, shape)) = plan.tiles(cube_idx)? else {
            continue;
        };
        let (first_channel_tile, last_channel_tile) = plan.channel_tiles(shape.tile_channel_count);
        let rows_by_tile = tiles.rows_by_tile(patches);
        // No channel selected leaves the last tile before the first.
        let channel_tile_count = (last_channel_tile + 1).saturating_sub(first_channel_tile);
        let stream_tiles = plan.stream_tiles(
            &tiles,
            rows_by_tile.len(),
            channel_tile_count,
            patches.len(),
        );
        // A tile exactly as long as axis 0 holds a row's channels as one
        // run of whole cells.
        let whole_cells = tiles.tile_axis0_count == plan.corr_count;

        for (row_tile, row_patches) in rows_by_tile {
            for channel_tile in first_channel_tile..=last_channel_tile {
                let tile_channel_start = channel_tile * shape.tile_channel_count;
                let overlap_start = std::cmp::max(plan.channel_start, tile_channel_start);
                let overlap_end =
                    std::cmp::min(channel_end, tile_channel_start + shape.tile_channel_count);
                if overlap_start >= overlap_end {
                    continue;
                }
                for span in tiles.axis0_spans() {
                    let read_started = profile.then(Instant::now);
                    let run = stream_tiles
                        .then(|| tiles.run_from(span, last_channel_tile - channel_tile));
                    let shared_tile = tiles.load(
                        tiles.tile_index(&[span.tile, channel_tile, row_tile]),
                        run,
                        session,
                        &mut streamed_tile,
                    )?;
                    let tile = shared_tile.as_deref().unwrap_or(&streamed_tile);
                    if let Some(started) = read_started {
                        profile_read += started.elapsed();
                        profile_tile_calls += 1;
                        profile_tile_bytes = profile_tile_bytes.saturating_add(tile.len());
                    }

                    let copy_started = profile.then(Instant::now);
                    let channel_bytes = tiles.tile_axis0_count * elem_size;
                    let byte = |sample: usize| (sample * plan.corr_count + span.start) * elem_size;
                    if !plan.row_major {
                        for channel in overlap_start..overlap_end {
                            let source_offset = (channel - tile_channel_start) * channel_bytes;
                            for &selected in &row_patches {
                                let src_byte =
                                    tiles.row_offset(selected) * elem_size + source_offset;
                                let dst_byte = byte(plan.output_sample(selected.out_idx, channel));
                                copy_pixels::<T>(values_bytes, dst_byte, tile, src_byte, span.len);
                            }
                        }
                    } else if whole_cells {
                        let run_bytes = (overlap_end - overlap_start) * channel_bytes;
                        for &selected in &row_patches {
                            let src_byte = tiles.row_offset(selected) * elem_size
                                + (overlap_start - tile_channel_start) * channel_bytes;
                            let dst_byte =
                                byte(plan.output_sample(selected.out_idx, overlap_start));
                            values_bytes[dst_byte..dst_byte + run_bytes]
                                .copy_from_slice(&tile[src_byte..src_byte + run_bytes]);
                        }
                    } else {
                        for &selected in &row_patches {
                            let row_byte = tiles.row_offset(selected) * elem_size;
                            for channel in overlap_start..overlap_end {
                                let src_byte =
                                    row_byte + (channel - tile_channel_start) * channel_bytes;
                                let dst_byte = byte(plan.output_sample(selected.out_idx, channel));
                                copy_pixels::<T>(values_bytes, dst_byte, tile, src_byte, span.len);
                            }
                        }
                    }
                    if let Some(started) = copy_started {
                        profile_copy += started.elapsed();
                    }
                }
            }
        }
    }
    if let Some(started) = profile_started {
        tracing::debug!(
            target: TYPED_2D_READ_PROFILE,
            "typed_2d_read_profile column={} mode=copy rows={} channels={} corr={} cubes={} tile_calls={} tile_bytes={} value_bytes={} read_ms={:.3} copy_ms={:.3} total_ms={:.3}",
            plan.column_name,
            plan.row_count,
            plan.channel_count,
            plan.corr_count,
            plan.patches_by_cube.len(),
            profile_tile_calls,
            profile_tile_bytes,
            values_bytes.len(),
            duration_ms_f64(profile_read),
            duration_ms_f64(profile_copy),
            duration_ms_f64(started.elapsed())
        );
    }
    Ok(())
}
