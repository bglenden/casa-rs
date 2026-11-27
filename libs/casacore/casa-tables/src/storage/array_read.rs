// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl CompositeStorage {
    pub(crate) fn load_array_column_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_array_column(table_path, &table_dat, column, row_hint)
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                array_column_from_snapshot(&snapshot, column)
            }
        }
    }

    pub(crate) fn load_array_column_rows_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_array_column_rows(
                table_path,
                &table_dat,
                column,
                selected_rows,
                row_hint,
            ),
            TableDatResult::Concat(concat) => {
                self.load_concat_rows(table_path, &concat, selected_rows, |path, local| {
                    self.load_array_column_rows_with_row_hint(path, column, local, None)
                })
            }
            TableDatResult::Ref(reference) => {
                if !reference.column_names.iter().any(|name| name == column) {
                    return Ok(vec![None; selected_rows.len()]);
                }
                let parent = add_directory_for_link(&reference.parent_relative_path, table_path);
                self.load_array_column_rows_with_row_hint(
                    &parent,
                    reference.parent_column(column),
                    &reference.parent_rows(selected_rows)?,
                    None,
                )
            }
        }
    }

    pub(crate) fn load_array_column_rows_2d_channel_range_typed_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        channel_start: usize,
        channel_count: usize,
        _row_hint: Option<u64>,
    ) -> Result<Option<SelectedArray2DCells>, StorageError> {
        if selected_rows.is_empty() {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 2-D read for {column} requires at least one selected row"
            )));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                let read_metadata =
                    TiledReadMetadata::open(table_path, &table_dat.column_set.data_managers)?;
                self.load_array_column_rows_2d_channel_range_typed_from_plain(
                    table_path,
                    &table_dat,
                    &read_metadata,
                    column,
                    selected_rows,
                    channel_start,
                    channel_count,
                )
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                Err(StorageError::FormatMismatch(format!(
                    "typed selected 2-D channel reads require a plain table for column '{column}'"
                )))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn load_array_column_rows_2d_channel_range_typed_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        column: &str,
        selected_rows: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, StorageError> {
        let request = SelectedArray2DChannelRead {
            column,
            selected_rows,
            channel_start,
            channel_count,
        };
        self.load_plain_array_column_rows_2d_channel_range_typed(
            table_path,
            table_dat,
            read_metadata,
            request,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn fill_array_column_rows_2d_channel_range_typed_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        column: &str,
        selected_rows: &[usize],
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, StorageError> {
        let request = SelectedArray2DChannelRead {
            column,
            selected_rows,
            channel_start,
            channel_count,
        };
        self.fill_plain_array_column_rows_2d_channel_range_typed(
            table_path,
            table_dat,
            read_metadata,
            request,
            destination,
        )
    }

    pub(crate) fn load_array_column_rows_1d_typed_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        _row_hint: Option<u64>,
    ) -> Result<SelectedArray1DCells, StorageError> {
        if selected_rows.is_empty() {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 1-D read for {column} requires at least one selected row"
            )));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                let read_metadata =
                    TiledReadMetadata::open(table_path, &table_dat.column_set.data_managers)?;
                self.load_array_column_rows_1d_typed_from_plain(
                    table_path,
                    &table_dat,
                    &read_metadata,
                    column,
                    selected_rows,
                )
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                Err(StorageError::FormatMismatch(format!(
                    "typed selected 1-D reads require a plain table for column '{column}'"
                )))
            }
        }
    }

    pub(crate) fn load_array_column_rows_1d_typed_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        column: &str,
        selected_rows: &[usize],
    ) -> Result<SelectedArray1DCells, StorageError> {
        let request = SelectedArray1DRead {
            column,
            selected_rows,
        };
        self.load_plain_array_column_rows_1d_typed(table_path, table_dat, read_metadata, request)
    }

    pub(crate) fn fill_array_column_rows_1d_typed_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        column: &str,
        selected_rows: &[usize],
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, StorageError> {
        let request = SelectedArray1DRead {
            column,
            selected_rows,
        };
        self.fill_plain_array_column_rows_1d_typed(
            table_path,
            table_dat,
            read_metadata,
            request,
            destination,
        )
    }
}

impl CompositeStorage {
    pub(super) fn load_plain_array_column(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if column_is_virtual(table_dat, column) {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            return array_column_from_snapshot(&snapshot, column);
        }

        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;
        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        if dm.type_name == asdm_stman::TYPE {
            return table_dat
                .asdm
                .get(dm.seq_nr)?
                .read(table_dat.asdm_column(col_desc)?, 0..nrrow);
        }
        let mut rows: Vec<RecordValue> = (0..nrrow).map(|_| RecordValue::default()).collect();
        let mut undefined_cells: Vec<HashSet<String>> =
            (0..nrrow).map(|_| HashSet::new()).collect();

        match dm.type_name.as_str() {
            "StManAipsIO" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_stman_aipsio_columns(
                    &data_path,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                    ByteOrder::BigEndian,
                )?;
            }
            "StandardStMan" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_ssm_columns(
                    &data_path,
                    &dm.data,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            "IncrementalStMan" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_ism_columns(
                    &data_path,
                    &dm.data,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::load_tiled_columns(
                    table_path,
                    dm,
                    &table_dat.table_desc,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            other => {
                return Err(StorageError::UnsupportedDataManager(other.to_string()));
            }
        }

        let snapshot = StorageSnapshot {
            row_count: nrrow,
            rows,
            undefined_cells,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
            table_info: TableInfo::default(),
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: Vec::new(),
            retained_read_metadata: None,
        };
        array_column_from_snapshot(&snapshot, column)
    }

    pub(crate) fn load_plain_array_column_rows(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(values) =
            self.load_selected_array_cells(table_path, table_dat, column, selected_rows)?
        {
            return Ok(values);
        }
        // A virtual column, or a layout its data manager cannot read row by
        // row, is read as a whole column.
        let values = self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
        Ok(select_array_rows(&values, selected_rows))
    }

    /// Read the whole stored cells of `selected_rows`, row by row.
    ///
    /// `None` when only a whole-column read can serve them: a virtual column,
    /// or a layout its data manager cannot read row by row (an unparsed
    /// `StManAipsIO` layout, a `StandardStMan` string or record array, or any
    /// other manager).
    pub(super) fn load_selected_array_cells(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
    ) -> Result<Option<Vec<Option<ArrayValue>>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        if matches!(
            dm.type_name.as_str(),
            "CompressComplex" | "CompressComplexSD"
        ) {
            let compression = ComplexCompression::read(&col_desc.keywords, column, &dm.type_name)?;
            let cells = self.load_compressed_complex_cells(
                table_path,
                table_dat,
                &compression,
                col_desc.require_primitive_type()?,
                selected_rows,
            )?;
            return Ok(Some(cells));
        }
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(bound_desc_idx, _)| *bound_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data-manager column binding"
                ))
            })?;

        match dm.type_name.as_str() {
            "StManAipsIO" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| {
                        table_dat.table_desc.columns[*bound_desc_idx].clone()
                    })
                    .collect();
                read_stman_array_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    ByteOrder::BigEndian,
                )
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::load_tiled_column_rows(
                    table_path,
                    dm,
                    &table_dat.table_desc,
                    &bound_cols,
                    desc_idx,
                    selected_rows,
                )
                .map(Some)
            }
            "StandardStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                read_ssm_array_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )
            }
            "IncrementalStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                read_ism_array_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )
            }
            asdm_stman::TYPE => table_dat
                .asdm
                .get(dm.seq_nr)?
                .read(
                    table_dat.asdm_column(col_desc)?,
                    selected_rows.iter().copied(),
                )
                .map(Some),
            _ => Ok(None),
        }
    }

    pub(super) fn load_compressed_complex_cells(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        compression: &ComplexCompression,
        target_type: casa_core::PrimitiveType,
        rows: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if column_is_virtual(table_dat, &compression.stored) {
            return Err(StorageError::FormatMismatch(
                "compressed storage must be a physical column".into(),
            ));
        }
        let cells = self.load_plain_array_column_rows(
            table_path,
            table_dat,
            &compression.stored,
            rows,
            None,
        )?;
        let (scales, offsets) = match &compression.scale {
            ComplexScale::Fixed { scale, offset } => {
                (vec![*scale; rows.len()], vec![*offset; rows.len()])
            }
            ComplexScale::PerRow { scale, offset, .. } => {
                let read = |column: &str| -> Result<Vec<f32>, StorageError> {
                    self.load_plain_scalar_column_rows_in_layout(
                        table_path, table_dat, column, rows, None, true,
                    )?
                    .into_iter()
                    .map(|value| match value {
                        Some(ScalarValue::Float32(value)) => Ok(value),
                        _ => Err(StorageError::FormatMismatch(format!(
                            "compressed scale column {column:?} is not Float32"
                        ))),
                    })
                    .collect()
                };
                (read(scale)?, read(offset)?)
            }
        };
        cells
            .into_iter()
            .zip(scales)
            .zip(offsets)
            .map(|((cell, scale), offset)| {
                cell.map(|value| compression.decode_cell(value, scale, offset, target_type))
                    .transpose()
            })
            .collect()
    }

    pub(crate) fn array_column_tile_bytes(
        &self,
        table_path: &Path,
        column: &str,
    ) -> Result<Option<usize>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }
        let TableDatResult::Plain(table_dat) = read_table_dat_dispatch(&control_path)? else {
            return Ok(None);
        };
        let Some(dm_seq_nr) = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .map(|entry| entry.dm_seq_nr)
        else {
            return Ok(None);
        };
        let Some(dm) = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
        else {
            return Ok(None);
        };
        match dm.type_name.as_str() {
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::tiled_bucket_bytes(table_path, dm.seq_nr).map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn array_cell_read_buffer_bytes(
        &self,
        table_path: &Path,
        column: &str,
    ) -> Result<usize, StorageError> {
        let Some(dm) = self.array_column_tiled_manager(table_path, column)? else {
            return Ok(0);
        };
        let TableDatResult::Plain(table_dat) =
            read_table_dat_dispatch(&table_path.join(TABLE_CONTROL_FILE))?
        else {
            return Ok(0);
        };
        let target = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("missing array column {column}"))
            })?;
        let bound: Vec<_> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.dm_seq_nr == dm)
            .collect();
        tiled_stman::cell_read_buffer_bytes(table_path, dm, &table_dat.table_desc, &bound, target)
    }

    /// The tile shape of array column `column`'s tiled storage manager, in
    /// casacore's axis order (the cell's axes, then rows), or `None` for a
    /// column without a tiled manager.
    pub(crate) fn array_column_tile_shape(
        &self,
        table_path: &Path,
        column: &str,
    ) -> Result<Option<Vec<usize>>, StorageError> {
        let Some(dm) = self.array_column_tiled_manager(table_path, column)? else {
            return Ok(None);
        };
        tiled_stman::tiled_tile_shape(table_path, dm)
    }

    /// The sequence number of `column`'s data manager when it is a tiled
    /// one.
    pub(super) fn array_column_tiled_manager(
        &self,
        table_path: &Path,
        column: &str,
    ) -> Result<Option<u32>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }
        let TableDatResult::Plain(table_dat) = read_table_dat_dispatch(&control_path)? else {
            return Ok(None);
        };
        let Some(entry) = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
        else {
            return Ok(None);
        };
        let tiled = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == entry.dm_seq_nr)
            .is_some_and(|dm| {
                matches!(
                    dm.type_name.as_str(),
                    "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
                )
            });
        Ok(tiled.then_some(entry.dm_seq_nr))
    }

    pub(crate) fn array_column_2d_channel_tile_width(
        &self,
        table_path: &Path,
        column: &str,
    ) -> Result<Option<usize>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        let TableDatResult::Plain(table_dat) = read_table_dat_dispatch(&control_path)? else {
            return Ok(None);
        };
        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        match dm.type_name.as_str() {
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::tiled_2d_channel_tile_width(table_path, dm.seq_nr)
            }
            _ => Ok(None),
        }
    }

    pub(super) fn load_plain_array_column_rows_2d_channel_range_typed(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        request: SelectedArray2DChannelRead<'_>,
    ) -> Result<Option<SelectedArray2DCells>, StorageError> {
        let column = request.column;
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 2-D channel reads do not support virtual columns in table containing '{column}'"
            )));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();

        match selected_cells::channel_read_footprint(&dm.type_name) {
            SelectedReadFootprint::Streamed => {
                tiled_stman::load_tiled_column_rows_2d_channel_range_typed(
                    table_path,
                    read_metadata,
                    dm,
                    &table_dat.table_desc,
                    &bound_cols,
                    desc_idx,
                    request.selected_rows,
                    request.channel_start,
                    request.channel_count,
                )
            }
            SelectedReadFootprint::WholeCells => self
                .whole_cell_read(table_path, table_dat, column, request.selected_rows)
                .load_2d(request.channel_start, request.channel_count),
        }
    }

    pub(super) fn fill_plain_array_column_rows_2d_channel_range_typed(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        request: SelectedArray2DChannelRead<'_>,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, StorageError> {
        let column = request.column;
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }
        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 2-D channel reads do not support virtual columns in table containing '{column}'"
            )));
        }
        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        match selected_cells::channel_read_footprint(&dm.type_name) {
            SelectedReadFootprint::Streamed => {
                tiled_stman::fill_tiled_column_rows_2d_channel_range_typed(
                    table_path,
                    read_metadata,
                    dm,
                    &table_dat.table_desc,
                    &bound_cols,
                    desc_idx,
                    request.selected_rows,
                    request.channel_start,
                    request.channel_count,
                    destination,
                )
            }
            SelectedReadFootprint::WholeCells => self
                .whole_cell_read(table_path, table_dat, column, request.selected_rows)
                .fill_2d(request.channel_start, request.channel_count, destination),
        }
    }

    pub(super) fn load_plain_array_column_rows_1d_typed(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        request: SelectedArray1DRead<'_>,
    ) -> Result<SelectedArray1DCells, StorageError> {
        let column = request.column;
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 1-D reads do not support virtual columns in table containing '{column}'"
            )));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .get(desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();

        match dm.type_name.as_str() {
            "IncrementalStMan" => {
                let mut values = Vec::new();
                let shape = self.fill_plain_array_column_rows_1d_typed(
                    table_path,
                    table_dat,
                    read_metadata,
                    request,
                    SelectedArray1DCellsMut::Float64(&mut values),
                )?;
                Ok(SelectedArray1DCells::Float64(
                    crate::table::SelectedArray1D::new(shape.row_count, shape.axis0_count, values),
                ))
            }
            "TiledColumnStMan" | "TiledShapeStMan" => tiled_stman::load_tiled_column_rows_1d_typed(
                table_path,
                read_metadata,
                dm,
                &table_dat.table_desc,
                &bound_cols,
                desc_idx,
                request.selected_rows,
            ),
            // The managers `selected_cells::cell_read_footprint` reports as
            // `WholeCells`.
            _ => self
                .whole_cell_read(table_path, table_dat, column, request.selected_rows)
                .load_1d(),
        }
    }

    pub(super) fn fill_plain_array_column_rows_1d_typed(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        read_metadata: &TiledReadMetadata,
        request: SelectedArray1DRead<'_>,
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, StorageError> {
        let column = request.column;
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }
        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected 1-D reads do not support virtual columns in table containing '{column}'"
            )));
        }
        let dm_seq_nr = table_dat
            .column_set
            .columns
            .get(desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        match dm.type_name.as_str() {
            "IncrementalStMan" => {
                let SelectedArray1DCellsMut::Float64(values) = destination else {
                    return Err(StorageError::FormatMismatch(format!(
                        "typed selected ISM reads for '{column}' require an f64 destination"
                    )));
                };
                let group_columns: Vec<_> = bound_cols
                    .iter()
                    .map(|(index, _)| &table_dat.table_desc.columns[*index])
                    .collect();
                let target = bound_cols
                    .iter()
                    .position(|(index, _)| *index == desc_idx)
                    .expect("selected ISM column belongs to its data manager");
                let axis0_count = incremental_stman::fill_ism_f64_array_rows(
                    &table_path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", dm.seq_nr)),
                    &dm.data,
                    &group_columns,
                    target,
                    request.selected_rows,
                    values,
                )?;
                Ok(SelectedArray1DShape {
                    row_count: request.selected_rows.len(),
                    axis0_count,
                })
            }
            "TiledColumnStMan" | "TiledShapeStMan" => tiled_stman::fill_tiled_column_rows_1d_typed(
                table_path,
                read_metadata,
                dm,
                &table_dat.table_desc,
                &bound_cols,
                desc_idx,
                request.selected_rows,
                destination,
            ),
            // The managers `selected_cells::cell_read_footprint` reports as
            // `WholeCells`.
            _ => self
                .whole_cell_read(table_path, table_dat, column, request.selected_rows)
                .fill_1d(destination),
        }
    }
}
