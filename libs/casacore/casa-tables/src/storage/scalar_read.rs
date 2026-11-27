// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl CompositeStorage {
    pub(crate) fn load_scalar_columns_with_row_hint(
        &self,
        table_path: &Path,
        row_hint: Option<u64>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_scalar_columns(table_path, &table_dat, row_hint)
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                Ok(scalar_columns_from_snapshot(&snapshot))
            }
        }
    }

    pub(crate) fn load_named_scalar_columns_with_row_hint(
        &self,
        table_path: &Path,
        columns: &HashSet<&str>,
        row_hint: Option<u64>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_scalar_columns_filtered(
                table_path,
                &table_dat,
                row_hint,
                Some(columns),
            ),
            TableDatResult::Concat(concat) => {
                self.load_concat_named_scalars(table_path, &concat, columns)
            }
            TableDatResult::Ref(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                let mut scalar_columns = scalar_columns_from_snapshot(&snapshot);
                scalar_columns
                    .columns
                    .retain(|name, _| columns.contains(name.as_str()));
                Ok(scalar_columns)
            }
        }
    }

    pub(crate) fn load_named_required_scalar_columns_with_row_hint(
        &self,
        table_path: &Path,
        columns: &HashSet<&str>,
        row_hint: Option<u64>,
    ) -> Result<RequiredScalarColumnSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_required_scalar_columns_filtered(
                table_path, &table_dat, row_hint, columns,
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let scalar_columns =
                    self.load_named_scalar_columns_with_row_hint(table_path, columns, row_hint)?;
                required_scalar_columns_from_optional_snapshot(&scalar_columns)
            }
        }
    }

    pub(crate) fn load_scalar_column_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_scalar_column(table_path, &table_dat, column, row_hint)
            }
            TableDatResult::Concat(concat) => {
                self.load_concat_scalar_column(table_path, &concat, column)
            }
            TableDatResult::Ref(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                scalar_column_from_snapshot(&snapshot, column)
            }
        }
    }

    pub(crate) fn load_scalar_column_rows_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        self.load_scalar_column_rows_in_layout(table_path, column, selected_rows, row_hint, false)
    }

    pub(crate) fn load_scalar_column_rows_in_layout(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
        preserve_ism_defaults: bool,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
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
            TableDatResult::Plain(table_dat) => self.load_plain_scalar_column_rows_in_layout(
                table_path,
                &table_dat,
                column,
                selected_rows,
                row_hint,
                preserve_ism_defaults,
            ),
            TableDatResult::Concat(concat) => {
                self.load_concat_rows(table_path, &concat, selected_rows, |path, local| {
                    self.load_scalar_column_rows_in_layout(
                        path,
                        column,
                        local,
                        None,
                        preserve_ism_defaults,
                    )
                })
            }
            TableDatResult::Ref(reference) => {
                if !reference.column_names.iter().any(|name| name == column) {
                    return Ok(vec![None; selected_rows.len()]);
                }
                let parent = add_directory_for_link(&reference.parent_relative_path, table_path);
                let parents = reference.parent_rows(selected_rows)?;
                // A full reference-row read retains the on-disk IncrementalStMan
                // scalar values, including zero when the descriptor default is
                // a different value. Projected reads keep that contract.
                self.load_scalar_column_rows_in_layout(
                    &parent,
                    reference.parent_column(column),
                    &parents,
                    None,
                    true,
                )
            }
        }
    }

    pub(crate) fn load_named_required_scalar_column_rows_with_row_hint(
        &self,
        table_path: &Path,
        columns: &HashSet<&str>,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<RequiredScalarColumnSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }
        let TableDatResult::Plain(table_dat) = read_table_dat_dispatch(&control_path)? else {
            let scalar_columns = columns
                .iter()
                .map(|column| {
                    self.load_scalar_column_rows_with_row_hint(
                        table_path,
                        column,
                        selected_rows,
                        row_hint,
                    )
                    .and_then(|values| {
                        required_scalar_column_from_optional_scalars(&values, column)
                            .map(|values| ((*column).to_string(), values))
                    })
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            return Ok(RequiredScalarColumnSnapshot {
                row_count: selected_rows.len(),
                columns: scalar_columns,
            });
        };

        self.load_named_required_scalar_column_rows_from_plain(
            table_path,
            &table_dat,
            columns,
            selected_rows,
            row_hint,
        )
    }

    pub(crate) fn load_named_required_scalar_column_rows_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        columns: &HashSet<&str>,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<RequiredScalarColumnSnapshot, StorageError> {
        if columns
            .iter()
            .any(|column| column_is_virtual(table_dat, column))
        {
            let scalar_columns = columns
                .iter()
                .map(|column| {
                    self.load_plain_scalar_column_rows(
                        table_path,
                        table_dat,
                        column,
                        selected_rows,
                        row_hint,
                    )
                    .and_then(|values| {
                        required_scalar_column_from_optional_scalars(&values, column)
                            .map(|values| ((*column).to_string(), values))
                    })
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            return Ok(RequiredScalarColumnSnapshot {
                row_count: selected_rows.len(),
                columns: scalar_columns,
            });
        }

        let mut loaded = HashMap::new();
        for dm in &table_dat.column_set.data_managers {
            let bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.dm_seq_nr == dm.seq_nr)
                .collect();
            let requested: Vec<(usize, &_)> = bound_cols
                .iter()
                .enumerate()
                .filter_map(|(dm_col_idx, (desc_idx, entry))| {
                    columns
                        .contains(entry.original_name.as_str())
                        .then_some((dm_col_idx, &table_dat.table_desc.columns[*desc_idx]))
                })
                .collect();
            if requested.is_empty() {
                continue;
            }
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
            match dm.type_name.as_str() {
                "StandardStMan" => loaded.extend(read_ssm_required_scalar_columns_rows(
                    &data_path,
                    &dm.data,
                    &requested,
                    selected_rows,
                )?),
                "IncrementalStMan" => {
                    let all_descs = bound_cols
                        .iter()
                        .map(|(desc_idx, _)| &table_dat.table_desc.columns[*desc_idx])
                        .collect::<Vec<_>>();
                    loaded.extend(read_ism_required_scalar_columns_rows(
                        &data_path,
                        &dm.data,
                        &all_descs,
                        &requested,
                        selected_rows,
                    )?);
                }
                _ => {}
            }
        }

        for &column in columns {
            if loaded.contains_key(column) {
                continue;
            }
            let values = self.load_plain_scalar_column_rows(
                table_path,
                table_dat,
                column,
                selected_rows,
                row_hint,
            )?;
            loaded.insert(
                column.to_string(),
                required_scalar_column_from_optional_scalars(&values, column)?,
            );
        }
        Ok(RequiredScalarColumnSnapshot {
            row_count: selected_rows.len(),
            columns: loaded,
        })
    }

    pub(crate) fn fill_named_required_scalar_column_rows_from_plain(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        selected_rows: &[usize],
        destinations: &mut [RequiredScalarColumnDestination<'_>],
    ) -> Result<(), StorageError> {
        if destinations
            .iter()
            .any(|destination| column_is_virtual(table_dat, destination.column()))
        {
            return Err(StorageError::FormatMismatch(
                "required scalar reusable fills do not support virtual columns".to_string(),
            ));
        }

        let mut outputs = HashMap::with_capacity(destinations.len());
        for destination in destinations.iter_mut() {
            let values = match destination.values_mut() {
                RequiredScalarColumnValuesMut::Bool(values) => {
                    RequiredScalarColumnData::Bool(std::mem::take(*values))
                }
                RequiredScalarColumnValuesMut::Int32(values) => {
                    RequiredScalarColumnData::Int32(std::mem::take(*values))
                }
                RequiredScalarColumnValuesMut::Float32(values) => {
                    RequiredScalarColumnData::Float32(std::mem::take(*values))
                }
                RequiredScalarColumnValuesMut::Float64(values) => {
                    RequiredScalarColumnData::Float64(std::mem::take(*values))
                }
            };
            outputs.insert(destination.column().to_string(), values);
        }

        let fill_result = (|| {
            let mut loaded = HashSet::with_capacity(destinations.len());
            for dm in &table_dat.column_set.data_managers {
                let bound_cols: Vec<(usize, &_)> = table_dat
                    .column_set
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| entry.dm_seq_nr == dm.seq_nr)
                    .collect();
                let requested: Vec<(usize, &_)> = bound_cols
                    .iter()
                    .enumerate()
                    .filter_map(|(dm_col_idx, (desc_idx, entry))| {
                        destinations
                            .iter()
                            .any(|destination| destination.column() == entry.original_name)
                            .then_some((dm_col_idx, &table_dat.table_desc.columns[*desc_idx]))
                    })
                    .collect();
                if requested.is_empty() {
                    continue;
                }
                let data_path = table_path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", dm.seq_nr));
                match dm.type_name.as_str() {
                    "StandardStMan" => read_ssm_required_scalar_columns_rows_reusing(
                        &data_path,
                        &dm.data,
                        &requested,
                        selected_rows,
                        &mut outputs,
                    )?,
                    "IncrementalStMan" => {
                        let all_descs = bound_cols
                            .iter()
                            .map(|(desc_idx, _)| &table_dat.table_desc.columns[*desc_idx])
                            .collect::<Vec<_>>();
                        read_ism_required_scalar_columns_rows_reusing(
                            &data_path,
                            &dm.data,
                            &all_descs,
                            &requested,
                            selected_rows,
                            &mut outputs,
                        )?;
                    }
                    // StManAipsIO (and any other manager) through the
                    // general selected-row reader, as casacore reads it.
                    _ => {
                        for (_, desc) in &requested {
                            self.fill_required_scalar_column_rows_from_plain_reader(
                                table_path,
                                table_dat,
                                &desc.col_name,
                                selected_rows,
                                &mut outputs,
                            )?;
                        }
                    }
                }
                loaded.extend(requested.iter().map(|(_, desc)| desc.col_name.as_str()));
            }
            for destination in destinations.iter() {
                if !loaded.contains(destination.column()) {
                    return Err(StorageError::FormatMismatch(format!(
                        "required scalar reusable fill did not load column '{}'",
                        destination.column()
                    )));
                }
            }
            Ok(())
        })();

        for destination in destinations.iter_mut() {
            let Some(values) = outputs.remove(destination.column()) else {
                continue;
            };
            match (destination.values_mut(), values) {
                (
                    RequiredScalarColumnValuesMut::Bool(destination),
                    RequiredScalarColumnData::Bool(values),
                ) => **destination = values,
                (
                    RequiredScalarColumnValuesMut::Int32(destination),
                    RequiredScalarColumnData::Int32(values),
                ) => **destination = values,
                (
                    RequiredScalarColumnValuesMut::Float32(destination),
                    RequiredScalarColumnData::Float32(values),
                ) => **destination = values,
                (
                    RequiredScalarColumnValuesMut::Float64(destination),
                    RequiredScalarColumnData::Float64(values),
                ) => **destination = values,
                _ => unreachable!("required scalar reusable fill preserves destination types"),
            }
        }
        fill_result
    }

    /// Fill one required scalar column's reusable buffer through the general
    /// selected-row reader. A buffer keeps its type; a column whose stored
    /// type differs is refused.
    ///
    /// The caller prepares a buffer for every requested column, matched by
    /// the name the column was stored under (casacore's `ColumnSet` original
    /// name). A column renamed since it was stored has a different current
    /// name and no buffer under it, and is refused rather than skipped.
    pub(super) fn fill_required_scalar_column_rows_from_plain_reader(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        outputs: &mut HashMap<String, RequiredScalarColumnData>,
    ) -> Result<(), StorageError> {
        let Some(buffer) = outputs.get_mut(column) else {
            return Err(StorageError::FormatMismatch(format!(
                "required selected-row column '{column}' has no destination: it is stored \
                 under another name"
            )));
        };
        let values =
            self.load_plain_scalar_column_rows(table_path, table_dat, column, selected_rows, None)?;
        if values.is_empty() {
            match buffer {
                RequiredScalarColumnData::Bool(values) => values.clear(),
                RequiredScalarColumnData::Int32(values) => values.clear(),
                RequiredScalarColumnData::Float32(values) => values.clear(),
                RequiredScalarColumnData::Float64(values) => values.clear(),
            }
            return Ok(());
        }
        let read = required_scalar_column_from_optional_scalars(&values, column)?;
        if std::mem::discriminant(buffer) != std::mem::discriminant(&read) {
            return Err(StorageError::FormatMismatch(format!(
                "required selected-row column '{column}' has an incompatible stored type"
            )));
        }
        *buffer = read;
        Ok(())
    }
}

impl CompositeStorage {
    pub(super) fn load_plain_scalar_columns(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        self.load_plain_scalar_columns_filtered(table_path, table_dat, row_hint, None)
    }

    pub(crate) fn load_plain_scalar_column(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        if column_is_virtual(table_dat, column) {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            return scalar_column_from_snapshot(&snapshot, column);
        }

        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if col_desc.is_array || col_desc.is_record() {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not a scalar column"
            )));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing ColumnSet binding"
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
                    "scalar column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(candidate_desc_idx, _)| *candidate_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data-manager binding index"
                ))
            })?;

        if dm.type_name == "StManAipsIO" {
            if !data_path.is_file() {
                return Err(StorageError::MissingDataFile(data_path));
            }
            let dm_columns: Vec<_> = bound_cols
                .iter()
                .map(|(candidate_desc_idx, _)| {
                    table_dat.table_desc.columns[*candidate_desc_idx].clone()
                })
                .collect();
            if let Some(values) = read_stman_scalar_column(
                &data_path,
                &dm_columns,
                target_col_idx,
                ByteOrder::BigEndian,
            )? {
                return Ok(values);
            }
        }
        if dm.type_name == "IncrementalStMan" {
            if !data_path.is_file() {
                return Err(StorageError::MissingDataFile(data_path));
            }
            let group_col_descs: Vec<_> = bound_cols
                .iter()
                .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                .collect();
            if let Some(values) = read_ism_scalar_column(
                &data_path,
                &dm.data,
                &group_col_descs,
                target_col_idx,
                nrrow,
            )? {
                return Ok(values);
            }
        }

        let requested = HashSet::from([column]);
        let snapshot = self.load_plain_scalar_columns_filtered(
            table_path,
            table_dat,
            row_hint,
            Some(&requested),
        )?;
        snapshot.columns.get(column).cloned().ok_or_else(|| {
            StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
        })
    }

    pub(super) fn load_plain_scalar_columns_filtered(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
        requested_columns: Option<&HashSet<&str>>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;

        if requested_columns.map_or_else(
            || {
                table_dat
                    .column_set
                    .data_managers
                    .iter()
                    .any(|dm| is_virtual_engine(&dm.type_name))
            },
            |columns| {
                columns
                    .iter()
                    .any(|column| column_is_virtual(table_dat, column))
            },
        ) {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            let mut scalar_columns = scalar_columns_from_snapshot(&snapshot);
            if let Some(requested) = requested_columns {
                scalar_columns
                    .columns
                    .retain(|name, _| requested.contains(name.as_str()));
            }
            return Ok(scalar_columns);
        }

        let mut columns = HashMap::new();

        for dm in &table_dat.column_set.data_managers {
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
            let all_bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();
            if all_bound_cols.is_empty() {
                continue;
            }
            let bound_cols: Vec<(usize, &_)> = all_bound_cols
                .iter()
                .copied()
                .filter(|(_, pc)| {
                    requested_columns
                        .is_none_or(|requested| requested.contains(pc.original_name.as_str()))
                })
                .collect();
            if bound_cols.is_empty() {
                continue;
            }

            match dm.type_name.as_str() {
                "StManAipsIO" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_stman_scalar_columns(
                        &data_path,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        requested_columns,
                        nrrow,
                        ByteOrder::BigEndian,
                        &mut columns,
                    )?;
                }
                "StandardStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_ssm_scalar_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        requested_columns,
                        nrrow,
                        &mut columns,
                    )?;
                }
                "IncrementalStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_ism_scalar_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        requested_columns,
                        nrrow,
                        &mut columns,
                    )?;
                }
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                    if bound_cols
                        .iter()
                        .any(|(desc_idx, _)| !table_dat.table_desc.columns[*desc_idx].is_array)
                    {
                        tiled_stman::collect_tiled_scalar_columns(
                            table_path,
                            dm,
                            &table_dat.table_desc,
                            &all_bound_cols,
                            requested_columns,
                            nrrow,
                            &mut columns,
                        )?;
                    }
                }
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        if let Some(requested) = requested_columns {
            columns.retain(|name, _| requested.contains(name.as_str()));
        }

        Ok(ScalarColumnSnapshot {
            row_count: nrrow,
            columns,
        })
    }

    pub(super) fn load_plain_required_scalar_columns_filtered(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
        requested_columns: &HashSet<&str>,
    ) -> Result<RequiredScalarColumnSnapshot, StorageError> {
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;

        if requested_columns
            .iter()
            .any(|column| column_is_virtual(table_dat, column))
        {
            let scalar_columns = self.load_plain_scalar_columns_filtered(
                table_path,
                table_dat,
                row_hint,
                Some(requested_columns),
            )?;
            return required_scalar_columns_from_optional_snapshot(&scalar_columns);
        }

        for dm in &table_dat.column_set.data_managers {
            let all_bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| dm.seq_nr == pc.dm_seq_nr)
                .collect();
            if all_bound_cols.is_empty() {
                continue;
            }
            let bound_cols: Vec<(usize, &_)> = all_bound_cols
                .iter()
                .copied()
                .filter(|(_, pc)| requested_columns.contains(pc.original_name.as_str()))
                .collect();
            if bound_cols.is_empty() {
                continue;
            }
            if matches!(
                dm.type_name.as_str(),
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
            ) && bound_cols
                .iter()
                .any(|(desc_idx, _)| !table_dat.table_desc.columns[*desc_idx].is_array)
            {
                let scalar_columns = self.load_plain_scalar_columns_filtered(
                    table_path,
                    table_dat,
                    row_hint,
                    Some(requested_columns),
                )?;
                return required_scalar_columns_from_optional_snapshot(&scalar_columns);
            }
        }

        let columns_by_manager = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for dm in &table_dat.column_set.data_managers {
                let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
                let all_bound_cols: Vec<(usize, &_)> = table_dat
                    .column_set
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, pc)| dm.seq_nr == pc.dm_seq_nr)
                    .collect();
                if all_bound_cols.is_empty() {
                    continue;
                }
                let bound_cols: Vec<(usize, &_)> = all_bound_cols
                    .iter()
                    .copied()
                    .filter(|(_, pc)| requested_columns.contains(pc.original_name.as_str()))
                    .collect();
                if bound_cols.is_empty() {
                    continue;
                }

                let dm_type = dm.type_name.clone();
                let dm_data = dm.data.clone();
                let table_columns = &table_dat.table_desc.columns;
                handles.push(scope.spawn(move || {
                    let mut columns = HashMap::new();
                    match dm_type.as_str() {
                        "StManAipsIO" => {
                            if !data_path.is_file() {
                                return Err(StorageError::MissingDataFile(data_path));
                            }
                            collect_stman_required_scalar_columns(
                                &data_path,
                                table_columns,
                                &all_bound_cols,
                                requested_columns,
                                nrrow,
                                ByteOrder::BigEndian,
                                &mut columns,
                            )?;
                        }
                        "StandardStMan" => {
                            if !data_path.is_file() {
                                return Err(StorageError::MissingDataFile(data_path));
                            }
                            collect_ssm_required_scalar_columns(
                                &data_path,
                                &dm_data,
                                table_columns,
                                &all_bound_cols,
                                requested_columns,
                                nrrow,
                                &mut columns,
                            )?;
                        }
                        "IncrementalStMan" => {
                            if !data_path.is_file() {
                                return Err(StorageError::MissingDataFile(data_path));
                            }
                            collect_ism_required_scalar_columns(
                                &data_path,
                                &dm_data,
                                table_columns,
                                &all_bound_cols,
                                requested_columns,
                                all_bound_cols.len(),
                                nrrow,
                                &mut columns,
                            )?;
                        }
                        "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan"
                        | "TiledDataStMan" => {}
                        other => {
                            return Err(StorageError::UnsupportedDataManager(other.to_string()));
                        }
                    }
                    Ok::<_, StorageError>(columns)
                }));
            }

            let mut columns_by_manager = Vec::with_capacity(handles.len());
            for handle in handles {
                columns_by_manager.push(handle.join().map_err(|_| {
                    StorageError::FormatMismatch(
                        "required scalar column loader thread panicked".to_string(),
                    )
                })??);
            }
            Ok::<_, StorageError>(columns_by_manager)
        })?;

        let mut columns = HashMap::new();
        for manager_columns in columns_by_manager {
            for (name, values) in manager_columns {
                if columns.insert(name.clone(), values).is_some() {
                    return Err(StorageError::FormatMismatch(format!(
                        "required scalar column '{name}' loaded more than once"
                    )));
                }
            }
        }

        columns.retain(|name, _| requested_columns.contains(name.as_str()));

        Ok(RequiredScalarColumnSnapshot {
            row_count: nrrow,
            columns,
        })
    }

    pub(crate) fn load_plain_scalar_column_rows(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        self.load_plain_scalar_column_rows_in_layout(
            table_path,
            table_dat,
            column,
            selected_rows,
            row_hint,
            false,
        )
    }

    pub(crate) fn load_plain_scalar_column_rows_in_layout(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
        preserve_ism_defaults: bool,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if col_desc.is_array || col_desc.is_record() {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not a scalar column"
            )));
        }

        if column_is_virtual(table_dat, column) {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            let values = scalar_column_from_snapshot(&snapshot, column)?;
            return Ok(select_scalar_rows(&values, selected_rows));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing ColumnSet binding"
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
                    "scalar column '{column}' missing data manager {dm_seq_nr}"
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
        let target_col_idx = bound_cols
            .iter()
            .position(|(candidate_desc_idx, _)| *candidate_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data-manager binding index"
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
                if let Some(values) = read_stman_scalar_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    ByteOrder::BigEndian,
                )? {
                    return Ok(values);
                }
            }
            "StandardStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ssm_scalar_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )? {
                    return Ok(values);
                }
            }
            "IncrementalStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ism_scalar_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    preserve_ism_defaults,
                )? {
                    return Ok(values);
                }
            }
            _ => {}
        }

        let values = self.load_plain_scalar_column(table_path, table_dat, column, row_hint)?;
        Ok(select_scalar_rows(&values, selected_rows))
    }
}
