// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl CompositeStorage {
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub(crate) fn save_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        dm_kind: crate::table::DataManagerKind,
        dm_group_name: Option<&str>,
        casacore_default_ssm: bool,
        preserve_ssm_group_name: bool,
        big_endian: bool,
        tile_shape: Option<&[usize]>,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        if table_path.is_dir()
            && let Ok(entries) = fs::read_dir(table_path)
        {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("table.f") {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }

        fs::create_dir_all(table_path)?;

        let schema = schema.ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;

        let filtered_rows_storage;
        let filtered_rows: &[RecordValue] = if undefined_cells.iter().any(|set| !set.is_empty()) {
            filtered_rows_storage = filter_rows_for_save(rows, undefined_cells);
            &filtered_rows_storage
        } else {
            rows
        };
        let nrrow = filtered_rows.len() as u64;
        let data_path = table_path.join(format!("{}0", TABLE_DATA_FILE_PREFIX));
        let has_virtual = !virtual_bindings.is_empty();

        let stored_rows: Vec<RecordValue>;
        let rows_for_data = if has_virtual {
            stored_rows = rows
                .iter()
                .zip(undefined_cells)
                .map(|(row, undef)| {
                    let stored_fields: Vec<_> = row
                        .fields()
                        .iter()
                        .filter(|f| !virtual_columns.contains(&f.name) && !undef.contains(&f.name))
                        .cloned()
                        .collect();
                    RecordValue::new(stored_fields)
                })
                .collect();
            &stored_rows
        } else {
            filtered_rows
        };

        let dm_type_name;
        let dm_data;
        match dm_kind {
            DataManagerKind::StManAipsIO => {
                dm_type_name = "StManAipsIO".to_string();
                dm_data = Vec::new();
                let mut table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                apply_physical_dm_descriptor_group(
                    &mut table_dat,
                    &dm_type_name,
                    dm_group_name.unwrap_or(&dm_type_name),
                    virtual_columns,
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                let stored_col_descs: Vec<_> = table_dat
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                write_stman_file(
                    &data_path,
                    0,
                    &stored_col_descs,
                    rows_for_data,
                    ByteOrder::BigEndian,
                )?;
            }
            DataManagerKind::StandardStMan => {
                dm_type_name = "StandardStMan".to_string();
                let table_dat_tmp = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                    )
                };
                let stored_col_descs: Vec<_> = table_dat_tmp
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                dm_data = if casacore_default_ssm || preserve_ssm_group_name {
                    write_ssm_file_with_options(
                        &data_path,
                        &stored_col_descs,
                        rows_for_data,
                        big_endian,
                        casacore_default_ssm,
                        if preserve_ssm_group_name {
                            dm_group_name.unwrap_or("StandardStMan")
                        } else {
                            "SSM"
                        },
                    )?
                } else {
                    write_ssm_file(&data_path, &stored_col_descs, rows_for_data, big_endian)?
                };
                let mut table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                apply_physical_dm_descriptor_group(
                    &mut table_dat,
                    &dm_type_name,
                    dm_group_name.unwrap_or(&dm_type_name),
                    virtual_columns,
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::IncrementalStMan => {
                dm_type_name = "IncrementalStMan".to_string();
                let table_dat_tmp = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    "IncrementalStMan",
                    &[],
                    big_endian,
                );
                dm_data = write_ism_file(
                    &data_path,
                    &table_dat_tmp.table_desc.columns,
                    rows,
                    big_endian,
                )?;
                let mut table_dat = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                apply_physical_dm_descriptor_group(
                    &mut table_dat,
                    &dm_type_name,
                    dm_group_name.unwrap_or(&dm_type_name),
                    virtual_columns,
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::TiledColumnStMan
            | DataManagerKind::TiledShapeStMan
            | DataManagerKind::TiledCellStMan
            | DataManagerKind::TiledDataStMan => {
                dm_type_name = match dm_kind {
                    DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
                    DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
                    DataManagerKind::TiledCellStMan => "TiledCellStMan",
                    DataManagerKind::TiledDataStMan => "TiledDataStMan",
                    _ => unreachable!(),
                }
                .to_string();
                dm_data = Vec::new();
                let mut table_dat = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                let default_dm_group_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.clone())
                    .unwrap_or_default();
                let dm_group_name = dm_group_name.unwrap_or(&default_dm_group_name);
                for desc in &mut table_dat.table_desc.columns {
                    desc.data_manager_type = dm_type_name.clone();
                    desc.data_manager_group = dm_group_name.to_string();
                }
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                let first_col_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.as_str())
                    .unwrap_or("");
                tiled_stman::save_tiled_columns(
                    table_path,
                    0,
                    &dm_type_name,
                    &table_dat.table_desc.columns,
                    rows,
                    big_endian,
                    tile_shape,
                    first_col_name,
                    &table_dat.table_desc.private_keywords,
                )?;
            }
        }

        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }
}

impl CompositeStorage {
    /// Rewrite the keywords in `table_path`'s `table.dat` and its
    /// `table.info`, keeping the data-manager layout on disk. The snapshot's
    /// `row_count` is the count the table's sync data publishes, which the
    /// file's row counts are raised to (#880); its rows are not written.
    pub(crate) fn save_metadata_only(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
    ) -> Result<(), StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(mut table_dat) => {
                table_dat.raise_row_count(snapshot.row_count as u64);
                table_dat.table_desc.table_keywords = snapshot.keywords.clone();
                for column in &mut table_dat.table_desc.columns {
                    column.keywords = snapshot
                        .column_keywords
                        .get(&column.col_name)
                        .cloned()
                        .unwrap_or_default();
                }
                write_table_dat(&control_path, &table_dat)?;
                let info_path = table_path.join(TABLE_INFO_FILE);
                fs::write(&info_path, snapshot.table_info.to_string())?;
                Ok(())
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                Err(StorageError::FormatMismatch(
                    "metadata-only save is only supported for PlainTable storage".to_string(),
                ))
            }
        }
    }
}

impl CompositeStorage {
    /// Save with per-column DM bindings.
    ///
    /// Columns listed in `bindings` use their specified DM; all other columns
    /// use the default DM from `dm_kind`. Each DM group writes its own data
    /// file (`table.f0`, `table.f1`, ...) and gets a separate entry in `table.dat`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_with_bindings_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        default_dm: crate::table::DataManagerKind,
        default_dm_group_name: Option<&str>,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
    ) -> Result<(), StorageError> {
        let column_overrides = ColumnOverrides::new();
        self.save_with_bindings_and_column_overrides_borrowed(
            table_path,
            rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema,
            table_info,
            virtual_columns,
            virtual_bindings,
            default_dm,
            default_dm_group_name,
            big_endian,
            default_tile_shape,
            bindings,
            &column_overrides,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub(crate) fn save_with_bindings_and_column_overrides_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        default_dm: crate::table::DataManagerKind,
        default_dm_group_name: Option<&str>,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
        column_overrides: &ColumnOverrides,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        let mut profiler = StorageProfiler::start(format!(
            "CompositeStorage::save_with_bindings path={}",
            table_path.display()
        ));

        fs::create_dir_all(table_path)?;

        let schema = schema.ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;
        // A persisted projection may already omit every undefined cell
        // from its records. Only copy records if a masked value must be removed.
        let rows_need_filter = rows
            .iter()
            .zip(undefined_cells)
            .any(|(row, undefined)| undefined.iter().any(|column| row.get(column).is_some()));
        let filtered_rows_storage =
            rows_need_filter.then(|| filter_rows_for_save(rows, undefined_cells));
        let filtered_rows: &[RecordValue] = filtered_rows_storage.as_deref().unwrap_or(rows);
        // These positions are hints. The indexed writers verify each field
        // name and look it up when later rows have a different layout.
        let row_field_positions = filtered_rows.first().map(|row| {
            row.fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| (field.name.clone(), idx))
                .collect::<HashMap<_, _>>()
        });
        let effective_row_count = column_overrides.row_count().unwrap_or(filtered_rows.len());
        let nrrow = effective_row_count as u64;
        let has_virtual = !virtual_bindings.is_empty();
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "prepare_rows",
                Some(format!(
                    "rows={} columns={} bindings={} virtual={}",
                    nrrow,
                    schema.columns().len(),
                    bindings.len(),
                    has_virtual
                )),
            );
        }

        // Group columns by DM kind. Columns in `bindings` get their own DM;
        // everything else goes into the default DM.
        struct DmGroup {
            dm_kind: DataManagerKind,
            dm_type_name: String,
            dm_group_name: String,
            seq_nr: u32,
            col_names: Vec<String>,
            tile_shape: Option<Vec<usize>>,
        }

        let mut groups: Vec<DmGroup> = Vec::new();

        // Default DM group (seq_nr 0).
        let default_type_name = match default_dm {
            DataManagerKind::StManAipsIO => "StManAipsIO",
            DataManagerKind::StandardStMan => "StandardStMan",
            DataManagerKind::IncrementalStMan => "IncrementalStMan",
            DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
            DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
            DataManagerKind::TiledCellStMan => "TiledCellStMan",
            DataManagerKind::TiledDataStMan => "TiledDataStMan",
        };
        groups.push(DmGroup {
            dm_kind: default_dm,
            dm_type_name: default_type_name.to_string(),
            dm_group_name: default_dm_group_name
                .unwrap_or(default_type_name)
                .to_string(),
            seq_nr: 0,
            col_names: Vec::new(),
            tile_shape: default_tile_shape.map(|s| s.to_vec()),
        });

        // Collect virtual column names for filtering.
        let virtual_col_names: HashSet<&str> = virtual_columns.iter().map(|s| s.as_str()).collect();

        // Build additional DM groups from bindings, then assign columns.
        let mut binding_seq_map: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut next_seq = 1u32;

        for col in schema.columns() {
            let col_name = col.name();
            if virtual_col_names.contains(col_name) {
                continue; // Virtual columns don't get stored DM entries.
            }
            if let Some(binding) = bindings.get(col_name) {
                let group_key = if matches!(
                    binding.data_manager,
                    DataManagerKind::TiledColumnStMan
                        | DataManagerKind::TiledShapeStMan
                        | DataManagerKind::TiledCellStMan
                        | DataManagerKind::TiledDataStMan
                ) {
                    format!("{:?}:{col_name}", binding.data_manager)
                } else {
                    format!("{:?}", binding.data_manager)
                };
                let group_idx = if let Some(&idx) = binding_seq_map.get(&group_key) {
                    idx
                } else {
                    let dm_type = match binding.data_manager {
                        DataManagerKind::StManAipsIO => "StManAipsIO",
                        DataManagerKind::StandardStMan => "StandardStMan",
                        DataManagerKind::IncrementalStMan => "IncrementalStMan",
                        DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
                        DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
                        DataManagerKind::TiledCellStMan => "TiledCellStMan",
                        DataManagerKind::TiledDataStMan => "TiledDataStMan",
                    };
                    let idx = groups.len();
                    groups.push(DmGroup {
                        dm_kind: binding.data_manager,
                        dm_type_name: dm_type.to_string(),
                        dm_group_name: dm_type.to_string(),
                        seq_nr: next_seq,
                        col_names: Vec::new(),
                        tile_shape: binding.tile_shape.clone(),
                    });
                    next_seq += 1;
                    binding_seq_map.insert(group_key, idx);
                    idx
                };
                groups[group_idx].col_names.push(col_name.to_string());
            } else {
                groups[0].col_names.push(col_name.to_string());
            }
        }
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "group_columns",
                Some(format!("groups={} next_seq={next_seq}", groups.len())),
            );
        }

        // Build a combined table.dat with all DM entries.
        // For now, use from_snapshot for the first DM and add extra DM entries manually.
        // This is a simplified approach: we write each group's data separately.

        // First, build a complete table_dat with all DMs.
        let mut dm_entries = Vec::new();
        let mut col_dm_map: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();

        for group in &groups {
            dm_entries.push(table_control::DataManagerEntry {
                type_name: group.dm_type_name.clone(),
                seq_nr: group.seq_nr,
                data: Vec::new(), // Will be filled in for SSM/ISM.
            });
            for col_name in &group.col_names {
                col_dm_map.insert(col_name.clone(), group.seq_nr);
            }
        }

        // Build initial table_dat (using from_snapshot but overriding DM entries).
        let mut table_dat = if has_virtual {
            TableDatContents::from_snapshot_with_virtual(
                schema,
                keywords,
                column_keywords,
                nrrow,
                &groups[0].dm_type_name,
                &[],
                big_endian,
                virtual_bindings,
                table_path,
            )
        } else {
            TableDatContents::from_snapshot(
                schema,
                keywords,
                column_keywords,
                nrrow,
                &groups[0].dm_type_name,
                &[],
                big_endian,
            )
        };
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("build_table_dat");
        }

        // Override DM entries with our multi-DM setup.
        // Preserve any virtual DM entries that from_snapshot_with_virtual added.
        let virtual_dm_entries: Vec<_> = table_dat
            .column_set
            .data_managers
            .iter()
            .filter(|e| e.seq_nr > 0 && is_virtual_engine(&e.type_name))
            .cloned()
            .collect();

        // Update each PlainColumnEntry's dm_seq_nr.
        for pc in &mut table_dat.column_set.columns {
            if let Some(&seq) = col_dm_map.get(&pc.original_name) {
                pc.dm_seq_nr = seq;
            }
        }

        // Override DM entries, filtering out empty groups that have no
        // columns and no data file to avoid "missing table.f<N>" errors.
        let non_empty_groups: Vec<&DmGroup> =
            groups.iter().filter(|g| !g.col_names.is_empty()).collect();
        table_dat.column_set.data_managers = non_empty_groups
            .iter()
            .map(|g| table_control::DataManagerEntry {
                type_name: g.dm_type_name.clone(),
                seq_nr: g.seq_nr,
                data: Vec::new(),
            })
            .collect();
        let group_by_seq: HashMap<u32, &DmGroup> = non_empty_groups
            .iter()
            .map(|group| (group.seq_nr, *group))
            .collect();
        for desc in &mut table_dat.table_desc.columns {
            if let Some(seq) = col_dm_map.get(&desc.col_name)
                && let Some(group) = group_by_seq.get(seq)
            {
                desc.data_manager_type = group.dm_type_name.clone();
                desc.data_manager_group = group.dm_group_name.clone();
            }
        }
        table_dat
            .column_set
            .data_managers
            .extend(virtual_dm_entries);
        table_dat.column_set.seq_count = table_dat
            .column_set
            .data_managers
            .iter()
            .map(|e| e.seq_nr + 1)
            .max()
            .unwrap_or(1);
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "finalize_table_dat",
                Some(format!(
                    "non_empty_groups={} seq_count={}",
                    non_empty_groups.len(),
                    table_dat.column_set.seq_count
                )),
            );
        }

        // Write data files per group.
        for group in &groups {
            if group.col_names.is_empty() {
                continue;
            }
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, group.seq_nr));

            let group_col_set: HashSet<&str> = group.col_names.iter().map(|s| s.as_str()).collect();
            let group_col_descs: Vec<_> = table_dat
                .table_desc
                .columns
                .iter()
                .filter(|c| group_col_set.contains(c.col_name.as_str()))
                .cloned()
                .collect();
            let group_col_indices = row_field_positions.as_ref().and_then(|positions| {
                let indices: Vec<usize> = group_col_descs
                    .iter()
                    .filter_map(|desc| positions.get(&desc.col_name).copied())
                    .collect();
                (indices.len() == group_col_descs.len()).then_some(indices)
            });
            // A group holding a generated array column is written from its
            // sources, or refused before any of its rows are projected.
            let generated_array_sources = generated_array_group_sources(
                group.dm_kind,
                &group_col_descs,
                column_overrides,
                effective_row_count,
            )?;
            let deferred_group = group_col_descs.iter().all(|desc| {
                matches!(
                    column_overrides.get(&desc.col_name),
                    Some(ColumnOverride::Deferred)
                )
            });
            let use_borrowed_tiled_values = matches!(
                group.dm_kind,
                DataManagerKind::TiledColumnStMan
                    | DataManagerKind::TiledShapeStMan
                    | DataManagerKind::TiledCellStMan
                    | DataManagerKind::TiledDataStMan
            ) && group_col_descs.len() == 1;
            let override_columns: Vec<_> = group_col_descs
                .iter()
                .filter_map(|desc| {
                    column_overrides
                        .contains_key(&desc.col_name)
                        .then_some(desc.col_name.as_str())
                })
                .collect();
            let use_direct_indexed_rows = matches!(
                group.dm_kind,
                DataManagerKind::StandardStMan | DataManagerKind::IncrementalStMan
            ) && filtered_rows.len() == effective_row_count
                && group_col_indices.is_some()
                && override_columns.is_empty();
            let group_col_index = group_col_indices
                .as_ref()
                .and_then(|indices| (indices.len() == 1).then_some(indices[0]));
            let group_values = if use_borrowed_tiled_values {
                match column_overrides.get(&group_col_descs[0].col_name) {
                    Some(ColumnOverride::Values(override_values)) => {
                        Some(override_values.iter().map(|value| value.as_ref()).collect())
                    }
                    Some(ColumnOverride::GeneratedScalar(_)) => {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled column override {} cannot be generated scalar",
                            group_col_descs[0].col_name
                        )));
                    }
                    Some(ColumnOverride::GeneratedArray(_)) => {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled column override {} cannot be generated array",
                            group_col_descs[0].col_name
                        )));
                    }
                    Some(ColumnOverride::StreamedScalar(_)) => {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled column override {} cannot be streamed scalar",
                            group_col_descs[0].col_name
                        )));
                    }
                    Some(ColumnOverride::Deferred) => None,
                    None => {
                        if filtered_rows.len() != effective_row_count {
                            return Err(StorageError::FormatMismatch(format!(
                                "column {} has no row values or override for {effective_row_count} generated rows",
                                group_col_descs[0].col_name
                            )));
                        }
                        Some(project_column_values_for_group(
                            filtered_rows,
                            &group_col_descs[0].col_name,
                            group_col_index,
                        ))
                    }
                }
            } else {
                None
            };
            let override_sources = match generated_array_sources {
                Some(sources) => Some(sources),
                None if matches!(
                    group.dm_kind,
                    DataManagerKind::StandardStMan | DataManagerKind::IncrementalStMan
                ) =>
                {
                    override_columns_for_group(
                        group.dm_kind,
                        &group_col_descs,
                        column_overrides,
                        effective_row_count,
                        filtered_rows,
                    )?
                }
                None => None,
            };
            if deferred_group {
                if let Some(profiler) = profiler.as_mut() {
                    profiler.mark_with_detail(
                        "group_projection",
                        Some(format!(
                            "seq={} dm={} cols={} rows={} indexed_projection={} mode=deferred",
                            group.seq_nr,
                            group.dm_type_name,
                            group_col_descs.len(),
                            effective_row_count,
                            group_col_indices.is_some()
                        )),
                    );
                    profiler.mark_with_detail(
                        "group_save",
                        Some(format!(
                            "seq={} dm={} cols={} rows={} deferred=true",
                            group.seq_nr,
                            group.dm_type_name,
                            group_col_descs.len(),
                            effective_row_count
                        )),
                    );
                }
                continue;
            }
            let group_rows =
                if group_values.is_none() && !use_direct_indexed_rows && override_sources.is_none()
                {
                    Some(project_rows_for_group(
                        filtered_rows,
                        effective_row_count,
                        &group_col_descs,
                        group_col_indices.as_deref(),
                        column_overrides,
                    ))
                } else {
                    None
                };
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "group_projection",
                    Some(format!(
                        "seq={} dm={} cols={} rows={} indexed_projection={} mode={}",
                        group.seq_nr,
                        group.dm_type_name,
                        group_col_descs.len(),
                        effective_row_count,
                        group_col_indices.is_some(),
                        if group_values.is_some() {
                            "borrowed_values"
                        } else if use_direct_indexed_rows {
                            "direct_rows"
                        } else if override_sources.is_some() {
                            "override_sources"
                        } else {
                            "projected_rows"
                        }
                    )),
                );
            }

            match group.dm_kind {
                DataManagerKind::StManAipsIO => {
                    write_stman_file(
                        &data_path,
                        group.seq_nr,
                        &group_col_descs,
                        group_rows.as_ref().expect("row projection for StManAipsIO"),
                        ByteOrder::BigEndian,
                    )?;
                }
                DataManagerKind::StandardStMan => {
                    let dm_data = if let Some(override_sources) = override_sources.as_ref() {
                        write_ssm_file_column_sources(
                            &data_path,
                            &group_col_descs,
                            override_sources,
                            big_endian,
                        )?
                    } else if let Some(group_col_indices) = group_col_indices.as_ref() {
                        write_ssm_file_indexed(
                            &data_path,
                            &group_col_descs,
                            filtered_rows,
                            group_col_indices,
                            big_endian,
                        )?
                    } else {
                        write_ssm_file(
                            &data_path,
                            &group_col_descs,
                            group_rows
                                .as_ref()
                                .expect("row projection for StandardStMan"),
                            big_endian,
                        )?
                    };
                    // Update the DM blob in the table_dat.
                    if let Some(entry) = table_dat
                        .column_set
                        .data_managers
                        .iter_mut()
                        .find(|e| e.seq_nr == group.seq_nr)
                    {
                        entry.data = dm_data;
                    }
                }
                DataManagerKind::IncrementalStMan => {
                    let dm_data = if let Some(override_sources) = override_sources.as_ref() {
                        write_ism_file_column_sources(
                            &data_path,
                            &group_col_descs,
                            override_sources,
                            big_endian,
                        )?
                    } else if let Some(group_col_indices) = group_col_indices.as_ref() {
                        write_ism_file_indexed(
                            &data_path,
                            &group_col_descs,
                            filtered_rows,
                            group_col_indices,
                            big_endian,
                        )?
                    } else {
                        write_ism_file(
                            &data_path,
                            &group_col_descs,
                            group_rows
                                .as_ref()
                                .expect("row projection for IncrementalStMan"),
                            big_endian,
                        )?
                    };
                    if let Some(entry) = table_dat
                        .column_set
                        .data_managers
                        .iter_mut()
                        .find(|e| e.seq_nr == group.seq_nr)
                    {
                        entry.data = dm_data;
                    }
                }
                DataManagerKind::TiledColumnStMan
                | DataManagerKind::TiledShapeStMan
                | DataManagerKind::TiledCellStMan
                | DataManagerKind::TiledDataStMan => {
                    // Use the first column name as the hypercolumn/DM name.
                    let first_col = group.col_names.first().map(|s| s.as_str()).unwrap_or("");
                    if let Some(group_values) = group_values.as_ref() {
                        tiled_stman::save_tiled_single_column_values(
                            table_path,
                            group.seq_nr,
                            &group_col_descs[0],
                            group_values,
                            tiled_stman::SingleColumnTiledSaveOptions {
                                dm_type_name: &group.dm_type_name,
                                big_endian,
                                default_tile_shape: group.tile_shape.as_deref(),
                                dm_name: first_col,
                            },
                        )?;
                    } else {
                        tiled_stman::save_tiled_columns(
                            table_path,
                            group.seq_nr,
                            &group.dm_type_name,
                            &group_col_descs,
                            group_rows
                                .as_ref()
                                .expect("row projection for tiled storage"),
                            big_endian,
                            group.tile_shape.as_deref(),
                            first_col,
                            &table_dat.table_desc.private_keywords,
                        )?;
                    }
                }
            }
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "group_save",
                    Some(format!(
                        "seq={} dm={} cols={} rows={}",
                        group.seq_nr,
                        group.dm_type_name,
                        group_col_descs.len(),
                        effective_row_count
                    )),
                );
            }
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_table_dat(&control_path, &table_dat)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("write_control_files");
        }

        Ok(())
    }

    /// Save with per-column DM bindings using an owned snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_with_bindings(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        default_dm: crate::table::DataManagerKind,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
    ) -> Result<(), StorageError> {
        self.save_with_bindings_borrowed(
            table_path,
            &snapshot.rows,
            &snapshot.undefined_cells,
            &snapshot.keywords,
            &snapshot.column_keywords,
            snapshot.schema.as_ref(),
            &snapshot.table_info,
            &snapshot.virtual_columns,
            &snapshot.virtual_bindings,
            default_dm,
            None,
            big_endian,
            default_tile_shape,
            bindings,
        )
    }
}
