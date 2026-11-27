// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl TableWritePlan<'_> {
    /// Validate the complete table and save it to `options.path()`.
    pub fn save(&mut self, options: TableOptions) -> Result<(), TableError> {
        self.table.save(options)
    }

    /// Validate the table and per-column bindings, then save it.
    pub fn save_with_bindings(
        &mut self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        self.validate_bindings(bindings)?;
        self.table.save_with_bindings(options, bindings)
    }

    /// Validate bindings and column overrides, then save without materializing
    /// deferred override columns into row records.
    pub fn save_with_column_overrides(
        &mut self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
        overrides: &ColumnOverrides,
    ) -> Result<(), TableError> {
        self.table.require_full_save_destination(&options.path)?;
        self.validate_bindings(bindings)?;
        let _ = overrides.effective_row_count(self.table.row_count())?;
        if let Some(schema) = self.table.schema() {
            for (column, override_value) in overrides.iter() {
                let schema_column =
                    schema
                        .column(column)
                        .ok_or_else(|| TableError::SchemaColumnUnknown {
                            column: column.clone(),
                        })?;
                match override_value {
                    ColumnOverride::Values(values) => {
                        for (row_index, value) in values.iter().enumerate() {
                            super::super::columns::validate_cell_against_schema_column(
                                row_index,
                                schema_column,
                                value.as_ref(),
                            )?;
                        }
                    }
                    ColumnOverride::GeneratedScalar(column) => {
                        if let Some(runs) = column.scalar_runs() {
                            for run in runs {
                                if let Some(value) = run.value() {
                                    super::super::columns::validate_cell_against_schema_column(
                                        run.start_row(),
                                        schema_column,
                                        Some(&Value::Scalar(value.clone())),
                                    )?;
                                }
                            }
                        }
                    }
                    ColumnOverride::GeneratedArray(column) => {
                        for run in column.array_runs().unwrap_or_default() {
                            if let Some(value) = run.cell() {
                                super::super::columns::validate_cell_against_schema_column(
                                    run.start_row(),
                                    schema_column,
                                    Some(value),
                                )?;
                            }
                        }
                    }
                    ColumnOverride::StreamedScalar(_) | ColumnOverride::Deferred => {}
                }
            }
        }
        self.table
            .persist_with_bindings_and_column_overrides(options, bindings, overrides)
    }

    /// Persist the data-manager groups containing `columns`.
    pub fn save_selected_columns(&mut self, columns: &[&str]) -> Result<(), TableError> {
        self.validate_selection(columns, &[])?;
        let auto_unlock = self.table.begin_write_operation("save selected columns")?;
        let result = self.table.persist_selected_columns_in_place(columns);
        self.table.finish_write_operation(auto_unlock, result)
    }

    /// Persist the selected rows within the data-manager groups containing
    /// `columns`.
    pub fn save_selected_rows(
        &mut self,
        columns: &[&str],
        rows: &[usize],
    ) -> Result<(), TableError> {
        self.validate_selection(columns, rows)?;
        let auto_unlock = self.table.begin_write_operation("save selected rows")?;
        let result = self.table.persist_selected_rows_in_place(columns, rows);
        self.table.finish_write_operation(auto_unlock, result)
    }

    /// Install a newly added tiled column by cloning an existing single-column
    /// tiled manager.
    pub fn add_tiled_column_clone(
        &mut self,
        source_column: &str,
        target_column: &str,
        target_data_manager_name: &str,
    ) -> Result<(), TableError> {
        self.table.persist_added_tiled_column_clone_in_place(
            source_column,
            target_column,
            target_data_manager_name,
        )
    }

    /// Install a newly added variable-shape tiled column.
    pub fn add_tiled_shape_column(
        &mut self,
        column: &str,
        changed_rows: &[usize],
        tile_shape: Option<&[usize]>,
    ) -> Result<(), TableError> {
        self.validate_selection(&[column], changed_rows)?;
        if tile_shape.is_some_and(|shape| shape.is_empty() || shape.contains(&0)) {
            return Err(TableError::Storage(
                "tiled shape must contain non-zero dimensions".to_string(),
            ));
        }
        self.table
            .persist_added_tiled_shape_column_in_place(column, changed_rows, tile_shape, None)
    }

    /// Install a newly added variable-shape column from its completed stream.
    ///
    /// The stream must use the schema's element type and provide one cell for
    /// each table row. The column is published after its payload is installed.
    #[doc(hidden)]
    pub fn add_streamed_tiled_shape_column(
        &mut self,
        column: &str,
        streamed: crate::streamed_tiled::StreamedTiledShapeColumn,
    ) -> Result<(), TableError> {
        self.validate_selection(&[column], &[])?;
        self.table
            .persist_added_tiled_shape_column_in_place(column, &[], None, Some(streamed))
    }

    fn validate_bindings(
        &self,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        for (column, binding) in bindings {
            self.table.require_column(column)?;
            if binding
                .tile_shape
                .as_ref()
                .is_some_and(|shape| shape.is_empty() || shape.contains(&0))
            {
                return Err(TableError::Storage(format!(
                    "binding for column \"{column}\" has an invalid tile shape"
                )));
            }
        }
        Ok(())
    }

    fn validate_selection(&self, columns: &[&str], rows: &[usize]) -> Result<(), TableError> {
        let mut seen_columns = std::collections::HashSet::with_capacity(columns.len());
        for &column in columns {
            self.table.require_column(column)?;
            if !seen_columns.insert(column) {
                return Err(TableError::Storage(format!(
                    "duplicate selected column \"{column}\""
                )));
            }
        }
        let mut seen_rows = std::collections::HashSet::with_capacity(rows.len());
        for &row in rows {
            self.table.require_row_index_without_loading_rows(row)?;
            if !seen_rows.insert(row) {
                return Err(TableError::Storage(format!("duplicate selected row {row}")));
            }
        }
        Ok(())
    }
}

impl Table {
    /// Rewrites only the existing data-manager groups that contain `changed_columns`.
    ///
    /// This is a narrow in-place optimization for schema-stable disk-backed
    /// tables. It preserves untouched `table.f*` payloads and rewrites only
    /// the groups that contain one or more changed columns, updating `table.dat`
    /// when the rewritten storage manager carries a data blob (for example
    /// `StandardStMan` / `IncrementalStMan`).
    ///
    /// Callers must ensure that:
    /// - the table schema and column-keyword layout are unchanged
    /// - all modified values already satisfy the schema
    /// - the table was opened from disk and still points at its source path
    ///
    /// If those conditions do not hold, use the normal save path instead.
    pub(crate) fn persist_selected_columns_in_place(
        &mut self,
        changed_columns: &[&str],
    ) -> Result<(), TableError> {
        self.persist_selected_rows_in_place(changed_columns, &[])
    }

    /// Saves only the affected storage-manager groups for the given columns,
    /// using row hints when available to avoid rewriting untouched tiled rows.
    ///
    /// `changed_rows` may be empty, in which case the save falls back to
    /// column-only invalidation semantics.
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub(crate) fn persist_selected_rows_in_place(
        &mut self,
        changed_columns: &[&str],
        changed_rows: &[usize],
    ) -> Result<(), TableError> {
        if changed_columns.is_empty() {
            return Ok(());
        }
        if self.try_save_reference_cells(changed_columns, changed_rows)? {
            return Ok(());
        }
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(
                "partial in-place save requires a plain disk-backed table".to_string(),
            ));
        }
        if changed_columns
            .iter()
            .any(|name| self.virtual_columns.contains(*name))
            || !self.virtual_bindings.is_empty()
        {
            return Err(TableError::Storage(
                "partial in-place save takes physical column values".to_string(),
            ));
        }

        let source_path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("table has no source path".to_string()))?;
        let changed_rows = if changed_rows.is_empty() {
            None
        } else {
            let mut rows: Vec<usize> = changed_rows
                .iter()
                .copied()
                .filter(|&row_idx| row_idx < self.row_count())
                .collect();
            rows.sort_unstable();
            rows.dedup();
            Some(rows)
        };
        let changed_set: std::collections::HashSet<&str> =
            changed_columns.iter().copied().collect();
        let affected_groups: Vec<_> = self
            .dm_info
            .iter()
            .filter(|dm| {
                dm.columns
                    .iter()
                    .any(|column| changed_set.contains(column.as_str()))
            })
            .cloned()
            .collect();
        if affected_groups
            .iter()
            .any(|group| group.dm_type == crate::storage::asdm_stman::TYPE)
        {
            return Err(TableError::Storage(
                "AsdmStMan columns are read-only".into(),
            ));
        }
        if affected_groups.is_empty() {
            return Ok(());
        }

        let control_path = source_path.join(crate::storage::TABLE_CONTROL_FILE);
        let mut table_dat =
            match crate::storage::table_control::read_table_dat_dispatch(&control_path)? {
                crate::storage::table_control::TableDatResult::Plain(table_dat) => table_dat,
                _ => {
                    return Err(TableError::Storage(
                        "partial in-place save only supports plain tables".to_string(),
                    ));
                }
            };
        let persisted_rows = self.require_persisted_rows(source_path, &table_dat)?;
        table_dat.raise_row_count(persisted_rows);

        let mut profiler = StorageProfiler::start(format!(
            "Table::save_selected_columns_in_place path={}",
            source_path.display()
        ));
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "start",
                Some(format!(
                    "rows={} changed_columns={} affected_groups={} changed_rows={}",
                    self.row_count(),
                    changed_columns.len(),
                    affected_groups.len(),
                    changed_rows.as_ref().map_or(0, |rows| rows.len())
                )),
            );
        }

        self.note_persisted_write(source_path);
        for group in affected_groups {
            let data_path = source_path.join(format!(
                "{}{}",
                crate::storage::TABLE_DATA_FILE_PREFIX,
                group.seq_nr
            ));
            let group_col_set: std::collections::HashSet<&str> =
                group.columns.iter().map(|column| column.as_str()).collect();
            let group_col_descs: Vec<_> = table_dat
                .table_desc
                .columns
                .iter()
                .filter(|desc| group_col_set.contains(desc.col_name.as_str()))
                .cloned()
                .collect();
            if group_col_descs.is_empty() {
                return Err(TableError::Storage(format!(
                    "data manager group {} has no columns in current table.dat",
                    group.seq_nr
                )));
            }

            match group.dm_type.as_str() {
                "StManAipsIO" => {
                    let group_changed_columns: Vec<&str> = group_col_descs
                        .iter()
                        .filter_map(|desc| {
                            changed_set
                                .contains(desc.col_name.as_str())
                                .then_some(desc.col_name.as_str())
                        })
                        .collect();
                    let sparse_saved = match changed_rows.as_ref() {
                        Some(rows) => {
                            let sparse_values = collect_sparse_column_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                            )?;
                            crate::storage::stman_aipsio::save_stman_file_rows_in_place(
                                &data_path,
                                &group_col_descs,
                                &sparse_values,
                                casa_aipsio::ByteOrder::BigEndian,
                            )?
                        }
                        None => false,
                    };
                    if !sparse_saved {
                        let rows = build_group_rows_from_current_cells(
                            self,
                            self.row_count(),
                            &group_col_descs,
                        )?;
                        crate::storage::stman_aipsio::write_stman_file(
                            &data_path,
                            group.seq_nr,
                            &group_col_descs,
                            &rows,
                            casa_aipsio::ByteOrder::BigEndian,
                        )?;
                    }
                }
                "StandardStMan" => {
                    let group_changed_columns: std::collections::HashSet<&str> = group_col_descs
                        .iter()
                        .filter_map(|desc| {
                            changed_set
                                .contains(desc.col_name.as_str())
                                .then_some(desc.col_name.as_str())
                        })
                        .collect();
                    let sparse_saved = if let Some(rows) = changed_rows.as_ref() {
                        if let Some(sparse_group_columns) =
                            collect_sparse_scalar_group_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                            )?
                        {
                            let dm_data = table_dat
                                .column_set
                                .data_managers
                                .iter()
                                .find(|entry| entry.seq_nr == group.seq_nr)
                                .map(|entry| entry.data.clone())
                                .ok_or_else(|| {
                                    TableError::Storage(format!(
                                        "StandardStMan group {} is missing data-manager metadata",
                                        group.seq_nr
                                    ))
                                })?;
                            crate::storage::standard_stman::save_ssm_file_scalar_columns_sparse_rows_in_place(
                                &data_path,
                                &dm_data,
                                &group_col_descs,
                                &sparse_group_columns,
                                rows,
                                table_dat.big_endian,
                            )?
                        } else if let Some(columns) =
                            collect_sparse_array_group_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                                true,
                                ArrayValue::clone,
                            )?
                        {
                            let dm_data = table_dat
                                .column_set
                                .data_managers
                                .iter()
                                .find(|entry| entry.seq_nr == group.seq_nr)
                                .ok_or_else(|| {
                                    TableError::Storage("SSM group metadata missing".into())
                                })?;
                            crate::storage::standard_stman::save_ssm_array_rows_in_place(
                                &data_path,
                                &dm_data.data,
                                &group_col_descs,
                                columns,
                                table_dat.big_endian,
                            )?
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !sparse_saved {
                        let rows = build_group_rows_from_current_cells(
                            self,
                            self.row_count(),
                            &group_col_descs,
                        )?;
                        let stored_name = table_dat
                            .column_set
                            .data_managers
                            .iter()
                            .find(|entry| entry.seq_nr == group.seq_nr)
                            .and_then(|entry| {
                                crate::storage::standard_stman::dm_name_from_blob(&entry.data)
                            });
                        let dm_name = stored_name
                            .as_deref()
                            .filter(|name| !name.is_empty())
                            .unwrap_or(if group.name.is_empty() {
                                "SSM"
                            } else {
                                group.name.as_str()
                            });
                        let dm_data = crate::storage::standard_stman::rewrite_ssm_rows(
                            &data_path,
                            &data_path,
                            &group_col_descs,
                            &rows,
                            dm_name,
                        )?;
                        if let Some(entry) = table_dat
                            .column_set
                            .data_managers
                            .iter_mut()
                            .find(|entry| entry.seq_nr == group.seq_nr)
                        {
                            entry.data = dm_data;
                        }
                    }
                }
                "IncrementalStMan" => {
                    let group_changed_columns: std::collections::HashSet<&str> = group_col_descs
                        .iter()
                        .filter_map(|desc| {
                            changed_set
                                .contains(desc.col_name.as_str())
                                .then_some(desc.col_name.as_str())
                        })
                        .collect();
                    let sparse_saved = if let Some(rows) = changed_rows.as_ref() {
                        if let Some(sparse_group_columns) =
                            collect_sparse_scalar_group_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                            )?
                        {
                            crate::storage::incremental_stman::save_ism_file_scalar_columns_sparse_rows_in_place(
                                &data_path,
                                &group_col_descs,
                                &sparse_group_columns,
                                rows,
                                table_dat.big_endian,
                            )?
                        } else if let Some(scalar_group_columns) =
                            borrow_scalar_group_columns_from_current_cells(self, &group_col_descs)?
                        {
                            crate::storage::incremental_stman::save_ism_file_scalar_columns_rows_in_place(
                                &data_path,
                                &group_col_descs,
                                &scalar_group_columns,
                                rows,
                                table_dat.big_endian,
                            )?
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    let dm_data = if sparse_saved {
                        None
                    } else if let Some(scalar_group_columns) =
                        borrow_scalar_group_columns_from_current_cells(self, &group_col_descs)?
                    {
                        Some(
                            crate::storage::incremental_stman::write_ism_file_scalar_columns(
                                &data_path,
                                &group_col_descs,
                                &scalar_group_columns,
                                table_dat.big_endian,
                            )?,
                        )
                    } else {
                        let rows = build_group_rows_from_current_cells(
                            self,
                            self.row_count(),
                            &group_col_descs,
                        )?;
                        Some(crate::storage::incremental_stman::write_ism_file(
                            &data_path,
                            &group_col_descs,
                            &rows,
                            table_dat.big_endian,
                        )?)
                    };
                    if let Some(dm_data) = dm_data
                        && let Some(entry) = table_dat
                            .column_set
                            .data_managers
                            .iter_mut()
                            .find(|entry| entry.seq_nr == group.seq_nr)
                    {
                        entry.data = dm_data;
                    }
                }
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                    let group_changed_columns: std::collections::HashSet<&str> = group_col_descs
                        .iter()
                        .filter_map(|desc| {
                            changed_set
                                .contains(desc.col_name.as_str())
                                .then_some(desc.col_name.as_str())
                        })
                        .collect();
                    let dm_name = if group_col_descs[0].data_manager_group.is_empty() {
                        group_col_descs[0].col_name.as_str()
                    } else {
                        group_col_descs[0].data_manager_group.as_str()
                    };
                    let sparse_saved = if let Some(rows) = changed_rows.as_ref() {
                        // A TiledDataStMan's scalar columns are its id columns,
                        // held in the hypercube values records, not in its tiles.
                        if let Some(sparse_group_columns) =
                            collect_sparse_array_group_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                                group.dm_type == "TiledDataStMan",
                                std::convert::identity,
                            )?
                        {
                            crate::storage::tiled_stman::save_tiled_columns_sparse_rows_in_place(
                                source_path,
                                group.seq_nr,
                                &group.dm_type,
                                &group_col_descs,
                                &table_dat.table_desc,
                                &sparse_group_columns,
                                rows,
                            )?
                        } else if group_col_descs.len() == 1 {
                            let values = collect_column_values_from_current_cells(
                                self,
                                self.row_count(),
                                &group_col_descs[0],
                            )?;
                            let value_refs: Vec<_> =
                                values.iter().map(|value| value.as_ref()).collect();
                            crate::storage::tiled_stman::save_tiled_single_column_rows_in_place(
                                source_path,
                                group.seq_nr,
                                &group_col_descs[0],
                                &value_refs,
                                rows,
                                crate::storage::tiled_stman::SingleColumnTiledSaveOptions {
                                    dm_type_name: &group.dm_type,
                                    big_endian: table_dat.big_endian,
                                    default_tile_shape: None,
                                    dm_name,
                                },
                            )?
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !sparse_saved {
                        if group_col_descs.len() == 1 {
                            let values = collect_column_values_from_current_cells(
                                self,
                                self.row_count(),
                                &group_col_descs[0],
                            )?;
                            let value_refs: Vec<_> =
                                values.iter().map(|value| value.as_ref()).collect();
                            crate::storage::tiled_stman::save_tiled_single_column_values(
                                source_path,
                                group.seq_nr,
                                &group_col_descs[0],
                                &value_refs,
                                crate::storage::tiled_stman::SingleColumnTiledSaveOptions {
                                    dm_type_name: &group.dm_type,
                                    big_endian: table_dat.big_endian,
                                    default_tile_shape: None,
                                    dm_name,
                                },
                            )?;
                        } else {
                            let rows = build_group_rows_from_current_cells(
                                self,
                                self.row_count(),
                                &group_col_descs,
                            )?;
                            crate::storage::tiled_stman::save_tiled_columns(
                                source_path,
                                group.seq_nr,
                                &group.dm_type,
                                &group_col_descs,
                                &rows,
                                table_dat.big_endian,
                                None,
                                dm_name,
                                &table_dat.table_desc.private_keywords,
                            )?;
                        }
                    }
                }
                other => {
                    return Err(TableError::Storage(format!(
                        "partial in-place save does not support data manager type {other}"
                    )));
                }
            }

            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "group_save",
                    Some(format!(
                        "seq={} dm={} cols={}",
                        group.seq_nr,
                        group.dm_type,
                        group.columns.len()
                    )),
                );
            }
        }

        crate::storage::table_control::write_table_dat(&control_path, &table_dat)?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("write_control_file");
        }
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(source_path);
        self.inner.refresh_read_metadata(table_dat)?;
        self.inner
            .note_selected_cells_saved(changed_columns, changed_rows.as_deref().unwrap_or(&[]));
        Ok(())
    }

    /// Drop successfully persisted lazy cell updates for a bounded write batch.
    ///
    /// This construction API does not alter on-disk data. It releases the
    /// pending values that were just saved by
    /// [`Self::persist_selected_rows_in_place`] so a long mutation
    /// does not retain every completed batch.
    #[doc(hidden)]
    pub fn discard_persisted_cell_updates(&mut self, columns: &[&str], rows: &[usize]) {
        self.inner.discard_pending_cell_updates(columns, rows);
    }

    /// Persists a newly-added array column by cloning an existing single-column
    /// tiled data manager and binding the clone to `target_column`.
    ///
    /// This is the storage-layout clone used by casacore
    /// `TableCopy::cloneColumnTyped` + `TableCopy::copyColumnData` for
    /// MeasurementSet scratch columns: the target starts as a byte-for-byte copy
    /// of the source tiled payload, then callers can sparsely patch rows.
    ///
    /// Callers must ensure that `target_column` already exists in the in-memory
    /// schema, is not yet present on disk, and has the same primitive array
    /// type as `source_column`.
    pub(crate) fn persist_added_tiled_column_clone_in_place(
        &mut self,
        source_column: &str,
        target_column: &str,
        target_data_manager_name: &str,
    ) -> Result<(), TableError> {
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(
                "cloning a tiled column in place requires a plain disk-backed table".to_string(),
            ));
        }
        if self.virtual_columns.contains(source_column)
            || self.virtual_columns.contains(target_column)
            || !self.virtual_bindings.is_empty()
        {
            return Err(TableError::Storage(
                "cloning a tiled column in place does not support virtual columns".to_string(),
            ));
        }

        let source_path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("table has no source path".to_string()))?
            .clone();
        let target_schema_column = self
            .inner
            .schema()
            .and_then(|schema| schema.column(target_column))
            .cloned()
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: target_column.to_string(),
            })?;

        let auto_unlock = self.begin_write_operation("save_added_tiled_column_clone")?;
        let result = (|| {
            let control_path = source_path.join(crate::storage::TABLE_CONTROL_FILE);
            let mut table_dat =
                match crate::storage::table_control::read_table_dat_dispatch(&control_path)? {
                    crate::storage::table_control::TableDatResult::Plain(table_dat) => table_dat,
                    _ => {
                        return Err(TableError::Storage(
                            "cloning a tiled column in place only supports plain tables"
                                .to_string(),
                        ));
                    }
                };
            let persisted_rows = self.require_persisted_rows(&source_path, &table_dat)?;
            table_dat.raise_row_count(persisted_rows);

            if table_dat
                .table_desc
                .columns
                .iter()
                .any(|desc| desc.col_name == target_column)
            {
                return Err(TableError::Storage(format!(
                    "column \"{target_column}\" already exists on disk"
                )));
            }

            let source_desc = table_dat
                .table_desc
                .columns
                .iter()
                .find(|desc| desc.col_name == source_column)
                .cloned()
                .ok_or_else(|| TableError::SchemaColumnUnknown {
                    column: source_column.to_string(),
                })?;
            let source_plain_column = table_dat
                .column_set
                .columns
                .iter()
                .find(|entry| entry.original_name == source_column)
                .cloned()
                .ok_or_else(|| {
                    TableError::Storage(format!(
                        "column \"{source_column}\" is not bound to an on-disk data manager"
                    ))
                })?;
            let source_dm = table_dat
                .column_set
                .data_managers
                .iter()
                .find(|entry| entry.seq_nr == source_plain_column.dm_seq_nr)
                .cloned()
                .ok_or_else(|| {
                    TableError::Storage(format!(
                        "data manager {} for column \"{source_column}\" is missing",
                        source_plain_column.dm_seq_nr
                    ))
                })?;
            if !matches!(
                source_dm.type_name.as_str(),
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
            ) {
                return Err(TableError::Storage(format!(
                    "column \"{source_column}\" is stored in {}, not a tiled data manager",
                    source_dm.type_name
                )));
            }
            let source_group_columns = table_dat
                .column_set
                .columns
                .iter()
                .filter(|entry| entry.dm_seq_nr == source_plain_column.dm_seq_nr)
                .count();
            if source_group_columns != 1 {
                return Err(TableError::Storage(format!(
                    "column \"{source_column}\" shares data manager {} with {source_group_columns} columns; clone currently supports single-column tiled managers",
                    source_plain_column.dm_seq_nr
                )));
            }

            let mut target_desc =
                crate::storage::table_control::ColumnDescContents::from_column_schema(
                    &target_schema_column,
                );
            if source_desc.require_primitive_type()? != target_desc.require_primitive_type()? {
                return Err(TableError::Storage(format!(
                    "cannot clone \"{source_column}\" into \"{target_column}\" with a different primitive type"
                )));
            }
            target_desc.data_manager_type = source_dm.type_name.clone();
            target_desc.data_manager_group = target_data_manager_name.to_string();
            if let Some(keywords) = self.inner.column_keywords(target_column) {
                target_desc.keywords = keywords.clone();
            } else {
                target_desc.keywords = source_desc.keywords.clone();
            }

            let target_seq_nr = table_dat
                .column_set
                .data_managers
                .iter()
                .map(|dm| dm.seq_nr)
                .max()
                .map_or(0, |seq| seq + 1);
            self.note_persisted_write(&source_path);
            crate::storage::tiled_stman::clone_tiled_manager_files(
                &source_path,
                source_plain_column.dm_seq_nr,
                target_seq_nr,
                target_data_manager_name,
            )?;

            table_dat.table_desc.columns.push(target_desc);
            table_dat
                .column_set
                .columns
                .push(crate::storage::table_control::PlainColumnEntry {
                    original_name: target_column.to_string(),
                    dm_seq_nr: target_seq_nr,
                    is_array: true,
                });
            table_dat.column_set.data_managers.push(
                crate::storage::table_control::DataManagerEntry {
                    type_name: source_dm.type_name.clone(),
                    seq_nr: target_seq_nr,
                    data: source_dm.data.clone(),
                },
            );
            table_dat.column_set.seq_count = table_dat
                .column_set
                .data_managers
                .iter()
                .map(|dm| dm.seq_nr + 1)
                .max()
                .unwrap_or(1);
            crate::storage::table_control::write_table_dat(&control_path, &table_dat)?;
            crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&source_path);
            self.dm_info.push(crate::storage::DataManagerInfo {
                dm_type: source_dm.type_name,
                name: target_data_manager_name.to_string(),
                seq_nr: target_seq_nr,
                columns: vec![target_column.to_string()],
            });
            self.inner.note_added_column_saved(target_column);
            self.inner.refresh_read_metadata(table_dat)
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Persists a newly-added variable-shape array column as a standalone
    /// `TiledShapeStMan` data manager without rewriting existing managers.
    ///
    /// The column must already exist in the in-memory schema and must not
    /// exist in the on-disk `table.dat` descriptor. Defined cells are written
    /// to the tiled row map; undefined or absent cells are left undefined.
    ///
    /// C++ equivalent: `Table::addColumn(desc, TiledShapeStMan(...))` followed
    /// by cell writes.
    pub(crate) fn persist_added_tiled_shape_column_in_place(
        &mut self,
        column: &str,
        changed_rows: &[usize],
        tile_shape: Option<&[usize]>,
        streamed: Option<crate::streamed_tiled::StreamedTiledShapeColumn>,
    ) -> Result<(), TableError> {
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(
                "adding a tiled column in place requires a plain disk-backed table".to_string(),
            ));
        }
        if self.virtual_columns.contains(column) || !self.virtual_bindings.is_empty() {
            return Err(TableError::Storage(
                "adding a tiled column in place does not support virtual columns".to_string(),
            ));
        }

        let source_path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("table has no source path".to_string()))?
            .clone();
        let schema_column = self
            .inner
            .schema()
            .and_then(|schema| schema.column(column))
            .cloned()
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: column.to_string(),
            })?;

        let auto_unlock = self.begin_write_operation("save_added_tiled_shape_column")?;
        let result = (|| {
            let control_path = source_path.join(crate::storage::TABLE_CONTROL_FILE);
            let mut table_dat =
                match crate::storage::table_control::read_table_dat_dispatch(&control_path)? {
                    crate::storage::table_control::TableDatResult::Plain(table_dat) => table_dat,
                    _ => {
                        return Err(TableError::Storage(
                            "adding a tiled column in place only supports plain tables".to_string(),
                        ));
                    }
                };
            let persisted_rows = self.require_persisted_rows(&source_path, &table_dat)?;
            table_dat.raise_row_count(persisted_rows);

            if table_dat
                .table_desc
                .columns
                .iter()
                .any(|desc| desc.col_name == column)
            {
                return Err(TableError::Storage(format!(
                    "column \"{column}\" already exists on disk"
                )));
            }

            let mut desc = crate::storage::table_control::ColumnDescContents::from_column_schema(
                &schema_column,
            );
            desc.data_manager_type = "TiledShapeStMan".to_string();
            desc.data_manager_group = column.to_string();
            if let Some(keywords) = self.inner.column_keywords(column) {
                desc.keywords = keywords.clone();
            }

            if streamed
                .as_ref()
                .is_some_and(|stream| !stream.matches_descriptor(&desc))
            {
                return Err(TableError::Storage(format!(
                    "streamed column {column:?} does not match the declared array type or rank"
                )));
            }
            if let Some(stream) = &streamed
                && stream.row_count() != self.row_count()
            {
                return Err(TableError::Storage(format!(
                    "streamed column {column:?} has {} rows but the table has {}",
                    stream.row_count(),
                    self.row_count(),
                )));
            }

            let seq_nr = table_dat
                .column_set
                .data_managers
                .iter()
                .map(|dm| dm.seq_nr)
                .max()
                .map_or(0, |seq| seq + 1);
            self.note_persisted_write(&source_path);
            let pending_values = self.inner.pending_array_cells(column);
            if let Some(streamed) = streamed {
                crate::streamed_tiled::install_streamed_tiled_shape_column(
                    &source_path,
                    seq_nr,
                    column,
                    streamed,
                )?;
            } else if changed_rows.is_empty()
                && pending_values.is_none_or(|values| values.is_empty())
            {
                crate::storage::tiled_stman::save_empty_tiled_shape_column(
                    &source_path,
                    seq_nr,
                    &desc,
                    self.row_count(),
                    table_dat.big_endian,
                    tile_shape,
                    column,
                )?;
            } else {
                let mut value_storage = vec![None; self.row_count()];
                if let Some(pending_values) = pending_values {
                    for (row_idx, value) in pending_values {
                        if *row_idx < value_storage.len() {
                            value_storage[*row_idx] = Some(Value::Array(value.clone()));
                        }
                    }
                }
                for &row_idx in changed_rows {
                    if row_idx >= value_storage.len() || value_storage[row_idx].is_some() {
                        continue;
                    }
                    value_storage[row_idx] = current_value_for_column(self, row_idx, &desc)?;
                }
                let values: Vec<Option<&Value>> =
                    value_storage.iter().map(Option::as_ref).collect();

                crate::storage::tiled_stman::save_tiled_single_column_values(
                    &source_path,
                    seq_nr,
                    &desc,
                    &values,
                    crate::storage::tiled_stman::SingleColumnTiledSaveOptions {
                        dm_type_name: "TiledShapeStMan",
                        big_endian: table_dat.big_endian,
                        default_tile_shape: tile_shape,
                        dm_name: column,
                    },
                )?;
            }

            table_dat.table_desc.columns.push(desc);
            table_dat
                .column_set
                .columns
                .push(crate::storage::table_control::PlainColumnEntry {
                    original_name: column.to_string(),
                    dm_seq_nr: seq_nr,
                    is_array: true,
                });
            table_dat.column_set.data_managers.push(
                crate::storage::table_control::DataManagerEntry {
                    type_name: "TiledShapeStMan".to_string(),
                    seq_nr,
                    data: Vec::new(),
                },
            );
            table_dat.column_set.seq_count = table_dat
                .column_set
                .data_managers
                .iter()
                .map(|dm| dm.seq_nr + 1)
                .max()
                .unwrap_or(1);
            crate::storage::table_control::write_table_dat(&control_path, &table_dat)?;
            crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&source_path);
            self.dm_info.push(crate::storage::DataManagerInfo {
                dm_type: "TiledShapeStMan".to_string(),
                name: column.to_string(),
                seq_nr,
                columns: vec![column.to_string()],
            });
            self.inner.note_added_column_saved(column);
            self.inner.refresh_read_metadata(table_dat)
        })();
        self.finish_write_operation(auto_unlock, result)
    }
}
