// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Read and parse the `table.info` file from a table directory.
///
/// Returns a default `TableInfo` if the file is missing or unreadable.
pub(super) fn load_table_info(table_path: &Path) -> TableInfo {
    let info_path = table_path.join(TABLE_INFO_FILE);
    match fs::read_to_string(&info_path) {
        Ok(contents) => TableInfo::parse(&contents),
        Err(_) => TableInfo::default(),
    }
}

/// Build [`DataManagerInfo`] from the parsed table.dat contents.
pub(super) fn extract_dm_info(
    table_path: &Path,
    table_dat: &table_control::TableDatContents,
) -> Vec<DataManagerInfo> {
    table_dat
        .column_set
        .data_managers
        .iter()
        .map(|dm| {
            let members = || {
                table_dat
                    .column_set
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            };
            let columns: Vec<String> = members().map(|(_, pc)| pc.original_name.clone()).collect();
            // StandardStMan and IncrementalStMan keep their name in their
            // blob, StManAipsIO and the tiled managers in the header of
            // their file (the NAME they were bound with, which a column's
            // declared group need not equal); any other is named by the group
            // its columns declare (the column set lists its columns in the
            // table description's order).
            let stored_name = match dm.type_name.as_str() {
                "StandardStMan" => standard_stman::dm_name_from_blob(&dm.data),
                "IncrementalStMan" => incremental_stman::dm_name_from_blob(&dm.data),
                "StManAipsIO" => stman_aipsio::name_from_file(
                    &table_path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", dm.seq_nr)),
                ),
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                    tiled_stman::name_from_file(
                        &table_path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", dm.seq_nr)),
                    )
                }
                _ => None,
            };
            let name = stored_name.unwrap_or_else(|| {
                members()
                    .find_map(|(index, _)| table_dat.table_desc.columns.get(index))
                    .map(|desc| desc.data_manager_group.clone())
                    .unwrap_or_default()
            });
            DataManagerInfo {
                dm_type: dm.type_name.clone(),
                name,
                seq_nr: dm.seq_nr,
                columns,
            }
        })
        .collect()
}

impl CompositeStorage {
    pub(crate) fn load_with_row_hint(
        &self,
        table_path: &Path,
        row_hint: Option<u64>,
    ) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_table_filtered(table_path, &table_dat, row_hint, None)
            }
            TableDatResult::Ref(ref_dat) => self.load_ref_table(table_path, &ref_dat),
            TableDatResult::Concat(concat_dat) => self.load_concat_table(table_path, &concat_dat),
        }
    }

    pub(crate) fn load_selected_columns_with_row_hint(
        &self,
        table_path: &Path,
        columns: &[&str],
        row_hint: Option<u64>,
    ) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        let requested_columns: HashSet<&str> = columns.iter().copied().collect();
        let mut snapshot = match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_table_filtered(
                table_path,
                &table_dat,
                row_hint,
                Some(&requested_columns),
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let mut snapshot = self.load_with_row_hint(table_path, row_hint)?;
                for row in &mut snapshot.rows {
                    *row = RecordValue::new(
                        row.fields()
                            .iter()
                            .filter(|field| requested_columns.contains(field.name.as_str()))
                            .cloned()
                            .collect(),
                    );
                }
                for undefined in &mut snapshot.undefined_cells {
                    undefined.retain(|column| requested_columns.contains(column.as_str()));
                }
                Ok(snapshot)
            }
        }?;
        for row in &mut snapshot.rows {
            *row = reorder_row_to_requested_columns(row, columns);
        }
        Ok(snapshot)
    }

    pub(crate) fn load_plain_selected_columns(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        columns: &[&str],
        row_hint: Option<u64>,
    ) -> Result<StorageSnapshot, StorageError> {
        let requested = columns.iter().copied().collect();
        let mut snapshot =
            self.load_plain_table_filtered(table_path, table_dat, row_hint, Some(&requested))?;
        for row in &mut snapshot.rows {
            *row = reorder_row_to_requested_columns(row, columns);
        }
        Ok(snapshot)
    }
}

impl CompositeStorage {
    pub(crate) fn load_metadata_only(
        &self,
        table_path: &Path,
    ) -> Result<StorageSnapshot, StorageError> {
        let profile_open = table_open_profile_enabled();
        let load_started_at = std::time::Instant::now();
        if profile_open {
            tracing::debug!(target: TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=storage_load_metadata_only/start",
                table_path.display(),
            );
        }
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }
        if profile_open {
            tracing::debug!(target: TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=storage_load_metadata_only/check_paths elapsed_s={:.3}",
                table_path.display(),
                load_started_at.elapsed().as_secs_f64(),
            );
        }

        let table_dat = read_table_dat_dispatch(&control_path)?;
        if profile_open {
            tracing::debug!(target: TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=storage_load_metadata_only/read_table_dat elapsed_s={:.3}",
                table_path.display(),
                load_started_at.elapsed().as_secs_f64(),
            );
        }
        match table_dat {
            TableDatResult::Plain(table_dat) => {
                let snapshot = self.load_plain_table_metadata(table_path, table_dat)?;
                if profile_open {
                    tracing::debug!(target: TABLE_OPEN_PROFILE,
                        "table_open_profile path={} stage=storage_load_metadata_only/plain_metadata rows={} dm_count={} elapsed_s={:.3}",
                        table_path.display(),
                        snapshot.row_count,
                        snapshot.dm_info.len(),
                        load_started_at.elapsed().as_secs_f64(),
                    );
                }
                Ok(snapshot)
            }
            TableDatResult::Ref(reference) => {
                self.load_ref_table_projected(table_path, &reference, true)
            }
            TableDatResult::Concat(concat) => self.load_concat_metadata(table_path, &concat),
        }
    }
}

impl CompositeStorage {
    /// Load a PlainTable from table.dat contents and data files.
    ///
    /// Uses two-pass loading:
    /// - Pass 1: Load stored columns from storage managers (StManAipsIO, StandardStMan).
    /// - Pass 2: Materialize virtual columns from virtual engines.
    /// - Pass 3: Reject any remaining unknown DM types.
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub(crate) fn load_plain_table_filtered(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
        requested_columns: Option<&HashSet<&str>>,
    ) -> Result<StorageSnapshot, StorageError> {
        if let Some(requested_columns) = requested_columns
            && table_dat
                .column_set
                .data_managers
                .iter()
                .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let mut snapshot =
                self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            for row in &mut snapshot.rows {
                *row = RecordValue::new(
                    row.fields()
                        .iter()
                        .filter(|field| requested_columns.contains(field.name.as_str()))
                        .cloned()
                        .collect(),
                );
            }
            for undefined in &mut snapshot.undefined_cells {
                undefined.retain(|column| requested_columns.contains(column.as_str()));
            }
            return Ok(snapshot);
        }

        let schema = table_dat.to_table_schema()?;
        let keywords = table_dat.table_desc.table_keywords.clone();
        let column_keywords: HashMap<String, RecordValue> = table_dat
            .table_desc
            .columns
            .iter()
            .filter(|c| {
                requested_columns.is_none_or(|requested| requested.contains(c.col_name.as_str()))
            })
            .filter(|c| !c.keywords.fields().is_empty())
            .map(|c| (c.col_name.clone(), c.keywords.clone()))
            .collect();
        // Real-world casacore tables can carry a stale or zero row count in
        // the outer "Table" envelope while the nested ColumnSet/storage-
        // manager state still reflects the actual number of rows. Upstream
        // C++ lets the ColumnSet/data-manager layer override the caller's
        // row count during open, so use the larger of the two counts here.
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;

        let mut rows: Vec<RecordValue> = (0..nrrow).map(|_| RecordValue::default()).collect();
        let mut undefined_cells: Vec<HashSet<String>> =
            (0..nrrow).map(|_| HashSet::new()).collect();
        let mut virtual_columns = HashSet::new();

        // Pass 1: Load stored columns from storage managers.
        for dm in &table_dat.column_set.data_managers {
            if is_virtual_engine(&dm.type_name) {
                continue; // Handled in pass 2.
            }

            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));

            let all_bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();
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
                asdm_stman::TYPE => {
                    asdm_stman::fill_rows(table_dat, dm.seq_nr, &bound_cols, &mut rows)?
                }
                "StManAipsIO" => {
                    if !data_file::DataFile::is_file(&data_path) {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    load_stman_aipsio_columns(
                        &data_path,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        &mut rows,
                        &mut undefined_cells,
                        nrrow,
                        ByteOrder::BigEndian,
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                "StandardStMan" => {
                    if !data_file::DataFile::is_file(&data_path) {
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
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                "IncrementalStMan" => {
                    if !data_file::DataFile::is_file(&data_path) {
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
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
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
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        // Pass 2: Materialize virtual columns.
        // Only clone rows if there are virtual engines that need the stored
        // snapshot — avoids an O(rows × cols) clone for tables with no
        // virtual columns (the common case for MS subtables).
        let has_virtual = table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name));

        if has_virtual {
            let stored_rows = rows.clone();
            for dm in &table_dat.column_set.data_managers {
                if !is_virtual_engine(&dm.type_name) {
                    continue;
                }

                let engine = lookup_engine(&dm.type_name)
                    .ok_or_else(|| StorageError::UnsupportedDataManager(dm.type_name.clone()))?;

                let bound_cols: Vec<(usize, &_)> = table_dat
                    .column_set
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                    .filter(|(_, pc)| {
                        requested_columns
                            .is_none_or(|requested| requested.contains(pc.original_name.as_str()))
                    })
                    .collect();

                if bound_cols.is_empty() {
                    continue;
                }

                for &(desc_idx, _) in &bound_cols {
                    virtual_columns.insert(table_dat.table_desc.columns[desc_idx].col_name.clone());
                }

                let ctx = VirtualContext {
                    col_descs: &table_dat.table_desc.columns,
                    rows: &stored_rows,
                    table_path,
                    nrrow,
                };

                engine.materialize(&ctx, &bound_cols, &mut rows)?;
            }
        }

        let table_info = load_table_info(table_path);

        // Build DM info from table.dat entries.
        let dm_info = extract_dm_info(table_path, table_dat);

        if let Some(requested_columns) = requested_columns {
            for row in &mut rows {
                *row = RecordValue::new(
                    row.fields()
                        .iter()
                        .filter(|field| requested_columns.contains(field.name.as_str()))
                        .cloned()
                        .collect(),
                );
            }
            for undefined in &mut undefined_cells {
                undefined.retain(|column| requested_columns.contains(column.as_str()));
            }
        }

        Ok(StorageSnapshot {
            row_count: rows.len(),
            rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema: Some(schema),
            table_info,
            virtual_columns,
            virtual_bindings: Vec::new(),
            dm_info,
            retained_read_metadata: None,
        })
    }
}

impl CompositeStorage {
    pub(super) fn load_plain_table_metadata(
        &self,
        table_path: &Path,
        table_dat: TableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        let schema = table_dat.to_table_schema()?;
        let keywords = table_dat.table_desc.table_keywords.clone();
        let column_keywords: HashMap<String, RecordValue> = table_dat
            .table_desc
            .columns
            .iter()
            .filter(|c| !c.keywords.fields().is_empty())
            .map(|c| (c.col_name.clone(), c.keywords.clone()))
            .collect();
        let mut virtual_columns = HashSet::new();
        for dm in &table_dat.column_set.data_managers {
            if !is_virtual_engine(&dm.type_name) {
                continue;
            }
            for plain_col in &table_dat.column_set.columns {
                if plain_col.dm_seq_nr != dm.seq_nr {
                    continue;
                }
                virtual_columns.insert(plain_col.original_name.clone());
            }
        }

        let row_count = table_dat.nrrow.max(table_dat.column_set.nrrow) as usize;
        let dm_info = extract_dm_info(table_path, &table_dat);
        let retained_read_metadata = Some(RetainedTableReadMetadata::open(table_path, table_dat)?);

        Ok(StorageSnapshot {
            row_count,
            rows: Vec::new(),
            undefined_cells: Vec::new(),
            keywords,
            column_keywords,
            schema: Some(schema),
            table_info: load_table_info(table_path),
            virtual_columns,
            virtual_bindings: Vec::new(),
            dm_info,
            retained_read_metadata,
        })
    }

    /// Load a RefTable by opening the parent and extracting referenced rows.
    pub(super) fn load_ref_table(
        &self,
        table_path: &Path,
        ref_dat: &RefTableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        self.load_ref_table_projected(table_path, ref_dat, false)
    }

    pub(super) fn load_ref_table_projected(
        &self,
        path: &Path,
        reference: &RefTableDatContents,
        metadata_only: bool,
    ) -> Result<StorageSnapshot, StorageError> {
        let parent_path = add_directory_for_link(&reference.parent_relative_path, path);
        let mut parent = if metadata_only {
            self.load_metadata_only(&parent_path)?
        } else {
            self.load(&parent_path)?
        };
        let names: Vec<_> = reference
            .column_names
            .iter()
            .map(|name| {
                let base = reference
                    .column_name_map
                    .iter()
                    .find(|(view, _)| view == name)
                    .map_or(name, |(_, base)| base);
                (name, base)
            })
            .collect();
        if reference.parent_nrrow > parent.row_count as u64
            && reference
                .row_map
                .iter()
                .any(|&row| row >= parent.row_count as u64)
        {
            return Err(StorageError::FormatMismatch(format!(
                "RefTable references a row beyond its parent ({} rows)",
                parent.row_count
            )));
        }
        let mut rows = Vec::new();
        let mut undefined_cells = Vec::new();
        if !metadata_only {
            rows.reserve(reference.row_map.len());
            undefined_cells.reserve(reference.row_map.len());
            let move_rows = parent.schema.as_ref().is_some_and(|schema| {
                names.len() == schema.columns().len()
                    && names
                        .iter()
                        .zip(schema.columns())
                        .all(|((name, base), column)| {
                            name == base && name.as_str() == column.name()
                        })
            }) && {
                let mut seen = vec![false; parent.rows.len()];
                reference.row_map.iter().all(|&row| {
                    seen.get_mut(row as usize)
                        .is_some_and(|seen| !std::mem::replace(seen, true))
                })
            };
            let move_layout = move_rows
                .then(|| {
                    parent
                        .rows
                        .first()
                        .and_then(|row| full_projection_layout(row.fields(), &names))
                })
                .flatten();
            for &row in &reference.row_map {
                let index = row as usize;
                if let Some(reorder) = &move_layout
                    && parent.rows[index].fields().len() == reorder.names.len()
                    && parent.rows[index]
                        .fields()
                        .iter()
                        .zip(&reorder.names)
                        .all(|(field, name)| &field.name == name)
                {
                    let mut owned = std::mem::take(&mut parent.rows[index]);
                    let fields = owned.fields_mut();
                    for &(a, b) in &reorder.swaps {
                        fields.swap(a, b);
                    }
                    rows.push(owned);
                    undefined_cells.push(std::mem::take(&mut parent.undefined_cells[index]));
                    continue;
                }
                let fields = parent.rows.get(index).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "RefTable references missing parent row {row}"
                    ))
                })?;
                rows.push(RecordValue::new(
                    names
                        .iter()
                        .filter_map(|(name, base)| {
                            fields
                                .get(base.as_str())
                                .map(|value| RecordField::new(name.as_str(), value.clone()))
                        })
                        .collect(),
                ));
                undefined_cells.push(
                    names
                        .iter()
                        .filter(|(_, base)| parent.undefined_cells[index].contains(base.as_str()))
                        .map(|(name, _)| (*name).clone())
                        .collect(),
                );
            }
        }
        let schema = parent
            .schema
            .map(|parent| {
                let columns = names
                    .iter()
                    .map(|(name, base)| {
                        parent
                            .column(base)
                            .cloned()
                            .map(|column| column.renamed((*name).clone()))
                            .ok_or_else(|| {
                                StorageError::Schema(format!(
                                    "RefTable references missing parent column {base}"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                TableSchema::new(columns).map_err(StorageError::from)
            })
            .transpose()?;
        let mut keywords = parent.keywords;
        rebase_table_keywords(&mut keywords, &parent_path, path);
        let column_keywords = names
            .iter()
            .filter_map(|&(name, base)| {
                parent
                    .column_keywords
                    .get(base)
                    .cloned()
                    .map(|mut keywords| {
                        rebase_table_keywords(&mut keywords, &parent_path, path);
                        (name.clone(), keywords)
                    })
            })
            .collect();
        let dm_info = parent
            .dm_info
            .into_iter()
            .filter(|manager| manager.dm_type == asdm_stman::TYPE)
            .filter_map(|mut manager| {
                let columns = names
                    .iter()
                    .filter(|(_, base)| manager.columns.contains(base))
                    .map(|(view, _)| (*view).clone())
                    .collect::<Vec<_>>();
                manager.columns = columns;
                (!manager.columns.is_empty()).then_some(manager)
            })
            .collect();
        Ok(StorageSnapshot {
            row_count: reference.row_map.len(),
            rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema,
            table_info: load_table_info(path),
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info,
            retained_read_metadata: None,
        })
    }

    /// Save a RefTable to disk (table.dat only, no data files).
    pub(crate) fn save_ref_table(
        &self,
        table_path: &Path,
        contents: &RefTableDatContents,
        table_info: &TableInfo,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_ref_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }

    /// Load a ConcatTable by opening all constituent tables and collecting rows.
    ///
    /// Materializes the concatenation: all rows from all constituent tables
    /// are collected into a single `StorageSnapshot`. Keywords and schema
    /// come from the first constituent table.
    pub(super) fn load_concat_table(
        &self,
        table_path: &Path,
        concat_dat: &table_control::ConcatTableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        let mut all_rows = Vec::new();
        let mut schema = None;
        let mut keywords = RecordValue::default();
        let mut column_keywords = HashMap::new();
        let mut undefined_cells = Vec::new();

        for (i, rel_path) in concat_dat.table_paths.iter().enumerate() {
            let abs_path = add_directory_for_link(rel_path, table_path);
            let sub_snapshot = self.load(&abs_path)?;

            if i == 0 {
                schema = sub_snapshot.schema;
                keywords = sub_snapshot.keywords;
                column_keywords = sub_snapshot.column_keywords;
                rebase_table_keywords(&mut keywords, &abs_path, table_path);
                for record in column_keywords.values_mut() {
                    rebase_table_keywords(record, &abs_path, table_path);
                }
            } else {
                concat_projection::merge_member_schema(&mut schema, sub_snapshot.schema)?;
            }
            all_rows.extend(sub_snapshot.rows);
            undefined_cells.extend(sub_snapshot.undefined_cells);
        }

        let table_info = load_table_info(table_path);

        Ok(StorageSnapshot {
            row_count: all_rows.len(),
            rows: all_rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema,
            table_info,
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: vec![],
            retained_read_metadata: None,
        })
    }

    /// Save a ConcatTable to disk (table.dat + table.info only, no data files).
    pub(crate) fn save_concat_table(
        &self,
        table_path: &Path,
        contents: &table_control::ConcatTableDatContents,
        table_info: &TableInfo,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_concat_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }
}
