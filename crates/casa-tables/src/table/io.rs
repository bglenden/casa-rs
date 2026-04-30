// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
#[cfg(unix)]
use crate::lock::read_sync_data_from_table_dir;
use crate::storage::StorageProfiler;
use casa_types::{ArrayValue, ScalarValue};

type SparseArrayRowValues = Vec<(usize, Option<ArrayValue>)>;
type SparseArrayColumns = Vec<Option<SparseArrayRowValues>>;
type SparseScalarRowValues = Vec<(usize, Option<ScalarValue>)>;
type SparseScalarColumns = Vec<Option<SparseScalarRowValues>>;

impl Table {
    /// Opens an existing table from disk.
    ///
    /// Reads schema, keywords, column keywords, and table metadata eagerly from
    /// the directory identified by `options.path()`. Row payloads are
    /// materialized lazily on first row access.
    ///
    /// This keeps `open()` much closer to C++ casacore's lazy table-open
    /// behavior for large persistent tables such as MeasurementSets.
    ///
    /// If the on-disk table is a reference table (`RefTable` type marker), the
    /// parent table is opened automatically and the referenced rows are
    /// materialized into this table.
    pub fn open(options: TableOptions) -> Result<Self, TableError> {
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        let storage = CompositeStorage;
        let snapshot = storage.load_metadata_only(&options.path)?;
        #[cfg(unix)]
        let row_hint = read_sync_data_from_table_dir(&options.path)
            .ok()
            .flatten()
            .map(|sync| sync.nrrow as usize)
            .unwrap_or(0);
        #[cfg(not(unix))]
        let row_hint = 0;
        let virtual_cols = snapshot.virtual_columns;
        let info = snapshot.table_info;
        Ok(Self {
            inner: TableImpl::with_lazy_rows_keywords_and_schema(
                snapshot.row_count.max(row_hint),
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
                options.path.clone(),
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: virtual_cols,
            virtual_bindings: Vec::new(),
            table_info: info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        })
    }

    /// Opens only table metadata from disk, without materializing rows.
    ///
    /// This loads schema, keywords, column keywords, table info, and data
    /// manager metadata, but intentionally leaves the row store empty.
    ///
    /// It is primarily intended for layered APIs such as paged images that
    /// need cheap access to table metadata while reading pixel data through a
    /// separate tiled I/O path. Row- and cell-level accessors on the returned
    /// table therefore behave as if the table has zero rows.
    ///
    /// This is a Rust-specific optimization; casacore C++ does not expose a
    /// direct equivalent on `Table`.
    pub fn open_metadata_only(options: TableOptions) -> Result<Self, TableError> {
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        let storage = CompositeStorage;
        let snapshot = storage.load_metadata_only(&options.path)?;
        Ok(Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.undefined_cells,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: snapshot.virtual_columns,
            virtual_bindings: Vec::new(),
            table_info: snapshot.table_info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        })
    }

    /// Saves the table to disk.
    ///
    /// Validates the table against its schema (if any), then writes all rows,
    /// keywords, column keywords, and schema to the directory specified by
    /// `options.path()`. The data manager format is determined by
    /// `options.data_manager()`. The directory need not exist beforehand;
    /// the storage layer creates it.
    ///
    /// Returns [`TableError::Storage`] on I/O failure.
    pub fn save(&self, options: TableOptions) -> Result<(), TableError> {
        self.validate()?;
        self.save_assuming_valid(options)
    }

    /// Saves the table to disk without re-validating every row against the schema.
    ///
    /// This is intended for advanced callers that already know the in-memory
    /// table is schema-valid because all mutations went through validating APIs
    /// such as [`add_row`](crate::Table::add_row), [`add_column`](crate::Table::add_column),
    /// [`cell_accessor_mut`](crate::Table::cell_accessor_mut), and
    /// [`column_accessor_mut`](crate::Table::column_accessor_mut). It preserves the exact same
    /// on-disk format as [`save`](Table::save), but skips the extra full-table
    /// validation pass before serialization.
    ///
    /// Callers that are unsure whether the table state is valid should keep
    /// using [`save`](Table::save).
    pub fn save_assuming_valid(&self, options: TableOptions) -> Result<(), TableError> {
        let storage = CompositeStorage;
        storage.save_borrowed(
            &options.path,
            self.inner.rows()?,
            self.inner.undefined_cells()?,
            self.inner.keywords(),
            self.inner.all_column_keywords(),
            self.inner.schema(),
            &self.table_info,
            &self.virtual_columns,
            &self.virtual_bindings,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
        )?;
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        Ok(())
    }

    /// Saves only table metadata back to disk without rewriting row storage.
    ///
    /// This updates `table.dat` keyword records, column keyword records, and
    /// `table.info`, while preserving the existing on-disk data-manager layout.
    /// It is intended for layered APIs such as tiled images that mutate table
    /// metadata but keep their payload in a separate storage manager.
    ///
    /// This is a Rust-specific optimization and currently supports only plain
    /// tables on disk.
    pub fn save_metadata_only(&self, options: TableOptions) -> Result<(), TableError> {
        let snapshot = StorageSnapshot {
            row_count: 0,
            rows: Vec::new(),
            undefined_cells: Vec::new(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: self.dm_info.clone(),
        };
        let storage = CompositeStorage;
        storage.save_metadata_only(&options.path, &snapshot)?;
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        Ok(())
    }

    /// Save the table with per-column data manager bindings.
    ///
    /// Columns listed in `bindings` are stored using their specified DM;
    /// all other stored columns use the default DM from `options`.
    ///
    /// This allows mixing storage managers within one table, for example
    /// scalars in StandardStMan and arrays in TiledColumnStMan.
    ///
    /// For large heterogeneous tables, this is usually the preferred public
    /// save entrypoint because it gives the storage layer enough information to
    /// take specialized per-column write paths instead of pushing every column
    /// through one generic layout. In practice that matters most for workloads
    /// such as MeasurementSet writes, where slowly changing scalars, ordinary
    /// scalars, and large array columns benefit from different storage
    /// managers.
    ///
    /// If the table state is already known to be schema-valid, prefer
    /// [`save_with_bindings_assuming_valid`](Self::save_with_bindings_assuming_valid)
    /// to avoid the extra full-table validation pass before serialization.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use casa_tables::{Table, TableOptions, DataManagerKind, ColumnBinding};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let table = Table::default();
    /// let mut bindings = HashMap::new();
    /// bindings.insert("DATA".to_string(), ColumnBinding {
    ///     data_manager: DataManagerKind::TiledColumnStMan,
    ///     tile_shape: Some(vec![4, 32]),
    /// });
    /// table.save_with_bindings(
    ///     TableOptions::new("/tmp/my_table"),
    ///     &bindings,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn save_with_bindings(
        &self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        self.validate()?;
        self.save_with_bindings_assuming_valid(options, bindings)
    }

    /// Save the table with per-column data manager bindings without re-validating rows.
    ///
    /// This is the bindings-aware counterpart to [`save_assuming_valid`](Self::save_assuming_valid).
    /// It preserves the same on-disk layout as [`save_with_bindings`](Self::save_with_bindings),
    /// but skips the extra full-table validation pass and avoids cloning the entire row set
    /// before serialization.
    ///
    /// This entrypoint is intended for high-volume writers that already know
    /// their table contents satisfy the schema through validated construction.
    /// On large tables it avoids a measurable amount of redundant work while
    /// still using the same bindings-driven storage-manager layout as
    /// [`save_with_bindings`](Self::save_with_bindings).
    pub fn save_with_bindings_assuming_valid(
        &self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "Table::save_with_bindings path={}",
            options.path.display()
        ));
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "validate",
                Some(format!(
                    "rows={} bindings={} skipped=true",
                    self.inner.row_count(),
                    bindings.len()
                )),
            );
        }
        let storage = CompositeStorage;
        storage.save_with_bindings_borrowed(
            &options.path,
            self.inner.rows()?,
            self.inner.undefined_cells()?,
            self.inner.keywords(),
            self.inner.all_column_keywords(),
            self.inner.schema(),
            &self.table_info,
            &self.virtual_columns,
            &self.virtual_bindings,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
            bindings,
        )?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("storage_save");
        }
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        Ok(())
    }

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
    pub fn save_selected_columns_in_place_assuming_valid(
        &self,
        changed_columns: &[&str],
    ) -> Result<(), TableError> {
        self.save_selected_rows_in_place_assuming_valid(changed_columns, &[])
    }

    /// Saves only the affected storage-manager groups for the given columns,
    /// using row hints when available to avoid rewriting untouched tiled rows.
    ///
    /// `changed_rows` may be empty, in which case the save falls back to
    /// column-only invalidation semantics.
    pub fn save_selected_rows_in_place_assuming_valid(
        &self,
        changed_columns: &[&str],
        changed_rows: &[usize],
    ) -> Result<(), TableError> {
        if changed_columns.is_empty() {
            return Ok(());
        }
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(
                "partial in-place save requires a plain disk-backed table".to_string(),
            ));
        }
        if !self.virtual_columns.is_empty() || !self.virtual_bindings.is_empty() {
            return Err(TableError::Storage(
                "partial in-place save does not support virtual columns".to_string(),
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
                            &group_col_descs,
                            &rows,
                            casa_aipsio::ByteOrder::BigEndian,
                        )?;
                    }
                }
                "StandardStMan" => {
                    let rows = build_group_rows_from_current_cells(
                        self,
                        self.row_count(),
                        &group_col_descs,
                    )?;
                    let dm_data = crate::storage::standard_stman::write_ssm_file(
                        &data_path,
                        &group_col_descs,
                        &rows,
                        table_dat.big_endian,
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
                    if let Some(dm_data) = dm_data {
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
                        if let Some(sparse_group_columns) =
                            collect_sparse_array_group_values_from_current_cells(
                                self,
                                rows,
                                &group_col_descs,
                                &group_changed_columns,
                            )?
                        {
                            crate::storage::tiled_stman::save_tiled_columns_sparse_rows_in_place(
                                source_path,
                                group.seq_nr,
                                &group.dm_type,
                                &group_col_descs,
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
        Ok(())
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
    pub fn save_added_tiled_shape_column_in_place_assuming_valid(
        &mut self,
        column: &str,
        changed_rows: &[usize],
        tile_shape: Option<&[usize]>,
    ) -> Result<(), TableError> {
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(
                "adding a tiled column in place requires a plain disk-backed table".to_string(),
            ));
        }
        if !self.virtual_columns.is_empty() || !self.virtual_bindings.is_empty() {
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

            let seq_nr = table_dat
                .column_set
                .data_managers
                .iter()
                .map(|dm| dm.seq_nr)
                .max()
                .map_or(0, |seq| seq + 1);
            let mut value_storage = vec![None; self.row_count()];
            if let Some(pending_values) = self.inner.pending_array_cells(column) {
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
            let values: Vec<Option<&Value>> = value_storage.iter().map(Option::as_ref).collect();

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
                seq_nr,
                columns: vec![column.to_string()],
            });
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Returns the filesystem path this table was opened from or saved to,
    /// if any. In-memory tables that have never been persisted return `None`.
    pub fn path(&self) -> Option<&Path> {
        self.source_path.as_deref()
    }

    /// Sets the source path for this table.
    ///
    /// Normally set automatically by [`open`](Table::open) and
    /// [`save`](Table::save). You can call this explicitly before creating
    /// a [`RefTable`](crate::RefTable) that saves to disk, if the table
    /// was constructed in-memory but you want to establish a parent path.
    pub fn set_path(&mut self, path: impl AsRef<Path>) {
        self.source_path = Some(path.as_ref().to_path_buf());
    }

    /// Returns the table metadata (type and subtype) from `table.info`.
    ///
    /// Tables loaded from disk carry the persisted values; newly created
    /// tables return the default (empty strings).
    ///
    /// # C++ equivalent
    ///
    /// `Table::tableInfo()`.
    pub fn info(&self) -> &TableInfo {
        &self.table_info
    }

    /// Replaces the table metadata (type and subtype).
    ///
    /// The new values are persisted on the next [`save`](Table::save).
    ///
    /// # C++ equivalent
    ///
    /// `Table::tableInfo()` (mutable overload) followed by `Table::flushTableInfo()`.
    pub fn set_info(&mut self, info: TableInfo) {
        self.table_info = info;
    }

    /// Returns data manager information for this table.
    ///
    /// Each [`crate::storage::DataManagerInfo`] describes one storage manager
    /// instance and
    /// the columns it manages. The list is populated when a table is loaded
    /// from disk; for memory-only tables the list is empty.
    ///
    /// # C++ equivalent
    ///
    /// `Table::dataManagerInfo()`.
    pub fn data_manager_info(&self) -> &[crate::storage::DataManagerInfo] {
        &self.dm_info
    }

    /// Returns a human-readable summary of the table's structure.
    ///
    /// Includes row count, column names and types, and (for disk-loaded
    /// tables) data manager assignments.
    ///
    /// # C++ equivalent
    ///
    /// `Table::showStructure(ostream)`, `showtableinfo` utility.
    pub fn show_structure(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let _ = writeln!(out, "Table: {} rows", self.row_count());

        if !self.table_info.table_type.is_empty() || !self.table_info.sub_type.is_empty() {
            let _ = writeln!(
                out,
                "  Type = {}  SubType = {}",
                self.table_info.table_type, self.table_info.sub_type
            );
        }

        if let Some(schema) = self.schema() {
            let _ = writeln!(out, "Columns ({}):", schema.columns().len());
            for col in schema.columns() {
                let type_str = match col.column_type() {
                    crate::schema::ColumnType::Scalar => {
                        format!(
                            "Scalar {}",
                            col.data_type()
                                .map_or("Record".into(), |dt| format!("{dt:?}"))
                        )
                    }
                    crate::schema::ColumnType::Array(contract) => {
                        let dt = col.data_type().map_or("?".into(), |dt| format!("{dt:?}"));
                        format!("Array<{dt}> {contract:?}")
                    }
                    crate::schema::ColumnType::Record => "Record".to_string(),
                };
                let _ = writeln!(out, "  {} : {}", col.name(), type_str);
            }
        }

        if !self.dm_info.is_empty() {
            let _ = writeln!(out, "Data managers ({}):", self.dm_info.len());
            for dm in &self.dm_info {
                let _ = writeln!(
                    out,
                    "  [{}] {} -> [{}]",
                    dm.seq_nr,
                    dm.dm_type,
                    dm.columns.join(", ")
                );
            }
        }

        out
    }

    /// Returns a formatted tree of the table's keyword sets.
    ///
    /// Includes both table-level keywords and per-column keywords.
    ///
    /// # C++ equivalent
    ///
    /// `TableRecord::print(ostream)`.
    pub fn show_keywords(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let kw = self.keywords();
        if !kw.fields().is_empty() {
            let _ = writeln!(out, "Table keywords:");
            for field in kw.fields() {
                let _ = writeln!(out, "  {} = {:?}", field.name, field.value);
            }
        }

        let col_kw = self.inner.all_column_keywords();
        for (col_name, rec) in col_kw {
            if !rec.fields().is_empty() {
                let _ = writeln!(out, "Column \"{}\" keywords:", col_name);
                for field in rec.fields() {
                    let _ = writeln!(out, "  {} = {:?}", field.name, field.value);
                }
            }
        }

        out
    }

    // -------------------------------------------------------------------
    // Lifecycle operations
    // -------------------------------------------------------------------

    /// Writes the current in-memory state back to the table's source path.
    ///
    /// The table must have been loaded with [`open`](Table::open) or
    /// previously saved with [`save`](Table::save) so that
    /// [`path`](Table::path) is `Some`. Returns an error if no source path
    /// is set.
    ///
    /// This is the Rust equivalent of the C++ `Table::flush()` call.
    pub fn flush(&self) -> Result<(), TableError> {
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("cannot flush: table has no source path".into()))?;
        let opts = TableOptions::new(path);
        self.save(opts)
    }

    /// Discards all in-memory changes and reloads the table from disk.
    ///
    /// The table must have a source path (set by [`open`](Table::open) or
    /// [`save`](Table::save)). After resync the in-memory state matches the
    /// on-disk state exactly.
    ///
    /// # C++ equivalent
    ///
    /// `Table::resync()`.
    pub fn resync(&mut self) -> Result<(), TableError> {
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("cannot resync: table has no source path".into()))?
            .clone();
        let opts = TableOptions::new(&path);
        let mut reloaded = Table::open(opts)?;
        self.inner = std::mem::take(&mut reloaded.inner);
        self.virtual_columns = std::mem::take(&mut reloaded.virtual_columns);
        self.virtual_bindings = std::mem::take(&mut reloaded.virtual_bindings);
        self.table_info = std::mem::take(&mut reloaded.table_info);
        // Preserve source_path, kind, marked_for_delete, and lock_state.
        Ok(())
    }

    /// Marks this table for deletion when it is dropped.
    ///
    /// If the table has a [`source_path`](Table::path), the table directory
    /// is recursively removed when the `Table` value is dropped.
    ///
    /// # C++ equivalent
    ///
    /// `Table::markForDelete()`.
    pub fn mark_for_delete(&mut self) {
        self.marked_for_delete = true;
    }

    /// Installs an external lock synchronization hook.
    ///
    /// The hook's methods are called around every file-level lock
    /// acquire/release pair so that an external lock manager can stay in
    /// sync. Pass `None` to remove a previously installed hook.
    ///
    /// # C++ equivalent
    ///
    /// `TableLockData::setExternalLockSync()`.
    pub fn set_external_sync(&mut self, sync: Option<Box<dyn crate::lock::ExternalLockSync>>) {
        self.external_sync = sync;
    }

    /// Clears the mark-for-delete flag.
    ///
    /// # C++ equivalent
    ///
    /// `Table::unmarkForDelete()`.
    pub fn unmark_for_delete(&mut self) {
        self.marked_for_delete = false;
    }

    /// Returns `true` if this table is marked for deletion on drop.
    ///
    /// # C++ equivalent
    ///
    /// `Table::isMarkedForDelete()`.
    pub fn is_marked_for_delete(&self) -> bool {
        self.marked_for_delete
    }

    /// Creates a deep copy of this table at the given path.
    ///
    /// All rows, keywords, column keywords, and schema are written to a new
    /// table directory. The storage manager can differ from the source table,
    /// enabling format migration (e.g. `StManAipsIO` to `StandardStMan`).
    ///
    /// # C++ equivalent
    ///
    /// `Table::deepCopy` via `TableCopy::makeEmptyTable` +
    /// `TableCopy::copyRows`.
    pub fn deep_copy(&self, opts: TableOptions) -> Result<(), TableError> {
        self.save(opts)
    }

    /// Creates a shallow copy of this table at the given path.
    ///
    /// Copies schema, table keywords, and column keywords but **no row data**.
    /// The resulting table has the same structure but zero rows.
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::makeEmptyTable(name, ..., noRows=True)`.
    pub fn shallow_copy(&self, opts: TableOptions) -> Result<(), TableError> {
        self.validate()?;
        let snapshot = StorageSnapshot {
            row_count: 0,
            rows: Vec::new(),
            undefined_cells: Vec::new(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: std::collections::HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save(
            &opts.path,
            &snapshot,
            opts.data_manager,
            opts.endian_format.is_big_endian(),
            opts.tile_shape.as_deref(),
        )?;
        Ok(())
    }
}

fn build_group_rows_from_current_cells(
    table: &Table,
    row_count: usize,
    col_descs: &[crate::storage::table_control::ColumnDescContents],
) -> Result<Vec<RecordValue>, TableError> {
    (0..row_count)
        .map(|row_index| {
            let mut fields = Vec::with_capacity(col_descs.len());
            for col_desc in col_descs {
                if let Some(value) = current_value_for_column(table, row_index, col_desc)? {
                    fields.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
            Ok(RecordValue::new(fields))
        })
        .collect()
}

fn collect_column_values_from_current_cells(
    table: &Table,
    row_count: usize,
    col_desc: &crate::storage::table_control::ColumnDescContents,
) -> Result<Vec<Option<Value>>, TableError> {
    (0..row_count)
        .map(|row_index| current_value_for_column(table, row_index, col_desc))
        .collect()
}

type SparseCurrentColumnValues<'a> =
    std::collections::HashMap<&'a str, Vec<(usize, Option<Value>)>>;

fn collect_sparse_column_values_from_current_cells<'a>(
    table: &Table,
    row_indices: &[usize],
    col_descs: &[crate::storage::table_control::ColumnDescContents],
    changed_columns: &[&'a str],
) -> Result<SparseCurrentColumnValues<'a>, TableError> {
    let mut patches = std::collections::HashMap::with_capacity(changed_columns.len());
    for &column in changed_columns {
        let Some(col_desc) = col_descs.iter().find(|desc| desc.col_name == column) else {
            continue;
        };
        let mut values = Vec::with_capacity(row_indices.len());
        for &row_index in row_indices {
            values.push((
                row_index,
                current_value_for_column(table, row_index, col_desc)?,
            ));
        }
        patches.insert(column, values);
    }
    Ok(patches)
}

fn borrow_scalar_group_columns_from_current_cells<'a>(
    table: &'a Table,
    col_descs: &[crate::storage::table_control::ColumnDescContents],
) -> Result<Option<Vec<&'a [Option<ScalarValue>]>>, TableError> {
    let mut values = Vec::with_capacity(col_descs.len());
    for col_desc in col_descs {
        if col_desc.is_array || col_desc.is_record() {
            return Ok(None);
        }
        if table.inner.has_pending_scalar_cells(&col_desc.col_name) {
            return Ok(None);
        }
        let Some(column_values) = table.inner.scalar_column_values(&col_desc.col_name)? else {
            return Ok(None);
        };
        values.push(column_values);
    }
    Ok(Some(values))
}

fn collect_sparse_scalar_group_values_from_current_cells(
    table: &Table,
    row_indices: &[usize],
    col_descs: &[crate::storage::table_control::ColumnDescContents],
    changed_columns: &std::collections::HashSet<&str>,
) -> Result<Option<SparseScalarColumns>, TableError> {
    let mut columns = Vec::with_capacity(col_descs.len());
    for col_desc in col_descs {
        if col_desc.is_array || col_desc.is_record() {
            return Ok(None);
        }
        if !changed_columns.contains(col_desc.col_name.as_str()) {
            columns.push(None);
            continue;
        }
        let Some(pending_values) = table.inner.pending_scalar_cells(&col_desc.col_name) else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(row_indices.len());
        for &row_index in row_indices {
            if let Some(value) = pending_values.get(&row_index) {
                values.push((row_index, Some(value.clone())));
            }
        }
        columns.push(Some(values));
    }
    Ok(Some(columns))
}

fn collect_sparse_array_group_values_from_current_cells(
    table: &Table,
    row_indices: &[usize],
    col_descs: &[crate::storage::table_control::ColumnDescContents],
    changed_columns: &std::collections::HashSet<&str>,
) -> Result<Option<SparseArrayColumns>, TableError> {
    let mut columns = Vec::with_capacity(col_descs.len());
    for col_desc in col_descs {
        if !col_desc.is_array || col_desc.is_record() {
            return Ok(None);
        }
        if !changed_columns.contains(col_desc.col_name.as_str()) {
            columns.push(None);
            continue;
        }
        let Some(pending_values) = table.inner.pending_array_cells(&col_desc.col_name) else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(row_indices.len());
        for &row_index in row_indices {
            if let Some((_, value)) = pending_values
                .iter()
                .find(|(pending_row, _)| *pending_row == row_index)
            {
                values.push((row_index, Some(value.clone())));
            }
        }
        columns.push(Some(values));
    }
    Ok(Some(columns))
}

fn current_value_for_column(
    table: &Table,
    row_index: usize,
    col_desc: &crate::storage::table_control::ColumnDescContents,
) -> Result<Option<Value>, TableError> {
    if col_desc.is_record() {
        return Err(TableError::Storage(format!(
            "partial in-place save does not support record column {}",
            col_desc.col_name
        )));
    }
    if col_desc.is_array {
        match table
            .column_accessor(&col_desc.col_name)
            .and_then(|column| column.array_cell(row_index))
        {
            Ok(value) => Ok(Some(Value::Array(value.clone()))),
            Err(TableError::ColumnNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    } else {
        match table
            .column_accessor(&col_desc.col_name)
            .and_then(|column| column.scalar_cell(row_index))
        {
            Ok(value) => Ok(Some(Value::Scalar(value.clone()))),
            Err(TableError::ColumnNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
