// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
#[cfg(unix)]
use crate::lock::read_sync_data_from_table_dir;
use crate::storage::StorageProfiler;
use casa_core::{ArrayValue, ScalarValue};

mod persist_rows;

type SparseArrayColumns<T> = Vec<Option<Vec<(usize, Option<T>)>>>;
type SparseScalarRowValues = Vec<(usize, Option<ScalarValue>)>;
type SparseScalarColumns = Vec<Option<SparseScalarRowValues>>;

impl Table {
    /// Prepare a validated persistence scope.
    ///
    /// Individual plan operations validate their complete knowable scope
    /// before starting I/O. The mutable borrow prevents schema or binding
    /// changes while the plan is active.
    ///
    /// ```compile_fail
    /// use casa_tables::{ColumnSchema, Table, TableSchema};
    /// use casa_core::PrimitiveType;
    ///
    /// let schema = TableSchema::new(vec![
    ///     ColumnSchema::scalar("id", PrimitiveType::Int32),
    /// ]).unwrap();
    /// let mut table = Table::with_schema(schema.clone());
    /// let mut plan = table.prepare_write();
    /// table.set_schema(schema).unwrap(); // cannot mutate schema while borrowed by the plan
    /// let _ = &mut plan;
    /// ```
    pub fn prepare_write(&mut self) -> TableWritePlan<'_> {
        TableWritePlan { table: self }
    }

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
        let profile_open = crate::storage::table_open_profile_enabled();
        let open_started_at = std::time::Instant::now();
        tracing::info!(
            path = %options.path.display(),
            "opening CASA table"
        );
        if profile_open {
            tracing::debug!(target: crate::storage::TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=start",
                options.path.display()
            );
        }
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        if profile_open {
            tracing::debug!(target: crate::storage::TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=invalidate_shared_tile_cache elapsed_s={:.3}",
                options.path.display(),
                open_started_at.elapsed().as_secs_f64(),
            );
        }
        let storage = CompositeStorage;
        let snapshot = storage.load_metadata_only(&options.path).map_err(|error| {
            tracing::error!(
                casa.priority = "SEVERE",
                path = %options.path.display(),
                error = %error,
                "failed to open CASA table"
            );
            error
        })?;
        if profile_open {
            tracing::debug!(target: crate::storage::TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=load_metadata_only rows={} dm_count={} elapsed_s={:.3}",
                options.path.display(),
                snapshot.row_count,
                snapshot.dm_info.len(),
                open_started_at.elapsed().as_secs_f64(),
            );
        }
        #[cfg(unix)]
        let row_hint = read_sync_data_from_table_dir(&options.path)
            .ok()
            .flatten()
            .map(|sync| sync.nrrow as usize)
            .unwrap_or(0);
        #[cfg(not(unix))]
        let row_hint = 0;
        if Self::memory_manager_snapshot(&snapshot) {
            let rows = snapshot.row_count.max(row_hint);
            return Ok(Self::from_memory_manager_snapshot(
                options.path.clone(),
                snapshot,
                rows,
            ));
        }
        let virtual_cols = snapshot.virtual_columns;
        let info = snapshot.table_info;
        let table = Self {
            inner: TableImpl::with_lazy_rows_keywords_and_schema(
                snapshot.row_count.max(row_hint),
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
                options.path.clone(),
                snapshot.retained_read_metadata,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: virtual_cols,
            virtual_bindings: Vec::new(),
            table_info: info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            measures: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        };
        if profile_open {
            tracing::debug!(target: crate::storage::TABLE_OPEN_PROFILE,
                "table_open_profile path={} stage=done row_hint={} rows={} elapsed_s={:.3}",
                options.path.display(),
                row_hint,
                table.row_count(),
                open_started_at.elapsed().as_secs_f64(),
            );
        }
        tracing::info!(
            path = %options.path.display(),
            rows = table.row_count(),
            elapsed_ms = open_started_at.elapsed().as_millis() as u64,
            "opened CASA table"
        );
        Ok(table)
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
            measures: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        })
    }

    /// Clone a persisted table descriptor for bounded construction of a new
    /// table with an explicit row count and externally supplied columns.
    ///
    /// No source rows or empty per-row records are retained. Callers must
    /// provide an override for every output column before saving.
    #[doc(hidden)]
    pub fn open_metadata_for_construction(
        options: TableOptions,
        row_count: usize,
    ) -> Result<Self, TableError> {
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        let storage = CompositeStorage;
        let snapshot = storage.load_metadata_only(&options.path)?;
        Ok(Self {
            inner: TableImpl::with_unloaded_rows_keywords_and_schema(
                row_count,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: None,
            kind: TableKind::Plain,
            virtual_columns: snapshot.virtual_columns,
            virtual_bindings: Vec::new(),
            table_info: snapshot.table_info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            measures: None,
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
        self.require_full_save_destination(&options.path)?;
        self.validate()?;
        self.persist_all(options)
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
    pub(crate) fn persist_all(&self, options: TableOptions) -> Result<(), TableError> {
        self.require_full_save_destination(&options.path)?;
        tracing::info!(
            path = %options.path.display(),
            rows = self.inner.row_count(),
            "saving CASA table"
        );
        self.note_persisted_write(&options.path);
        let storage = CompositeStorage;
        storage
            .save_borrowed(
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
                options.data_manager_group.as_deref(),
                options.casacore_default_ssm,
                options.preserve_ssm_group_name,
                options.endian_format.is_big_endian(),
                options.tile_shape.as_deref(),
            )
            .map_err(|error| {
                tracing::error!(
                    casa.priority = "SEVERE",
                    path = %options.path.display(),
                    error = %error,
                    "failed to save CASA table"
                );
                error
            })?;
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        self.note_full_save(&options.path)?;
        tracing::info!(
            path = %options.path.display(),
            rows = self.inner.row_count(),
            "saved CASA table"
        );
        Ok(())
    }

    fn require_full_save_destination(&self, target: &Path) -> Result<(), TableError> {
        if !self
            .source_path
            .as_deref()
            .is_some_and(|path| super::locking::same_directory(path, target))
        {
            return Ok(());
        }
        let control = target.join(crate::storage::TABLE_CONTROL_FILE);
        let persisted_asdm = control.is_file()
            && matches!(
                crate::storage::read_table_dat_dispatch(&control)?,
                crate::storage::TableDatResult::Plain(table)
                    if table.column_set.data_managers.iter().any(|manager| {
                        manager.type_name == crate::storage::asdm_stman::TYPE
                    })
            );
        if persisted_asdm || self.has_asdm_manager() {
            return Err(TableError::Storage(
                "AsdmStMan is read-only. Materialize the table at a new path.".into(),
            ));
        }
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
        if self.try_save_reference_metadata(&options.path)? {
            return Ok(());
        }
        let snapshot = StorageSnapshot {
            // The rows the sync data at the target publishes, which its
            // `table.dat` can trail (#880). This save writes no rows, so this
            // handle's own count, which includes unsaved ones, is not used.
            row_count: persisted_rows::published_row_count(&options.path) as usize,
            rows: Vec::new(),
            undefined_cells: Vec::new(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: self.dm_info.clone(),
            retained_read_metadata: None,
        };
        self.note_persisted_write(&options.path);
        let storage = CompositeStorage;
        storage.save_metadata_only(&options.path, &snapshot)?;
        if self
            .source_path
            .as_deref()
            .is_some_and(|path| super::locking::same_directory(path, &options.path))
        {
            self.inner.note_metadata_saved();
        }
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
    /// High-throughput mutation code should prepare its rows and then use
    /// [`prepare_write`](Self::prepare_write) so the persistence scope is
    /// validated and represented by one exclusive plan.
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
        self.require_full_save_destination(&options.path)?;
        self.validate()?;
        self.persist_with_bindings(options, bindings)
    }

    /// Save the table with per-column data manager bindings without re-validating rows.
    ///
    /// This is the bindings-aware counterpart to [`persist_all`](Self::persist_all).
    /// It preserves the same on-disk layout as [`save_with_bindings`](Self::save_with_bindings),
    /// but skips the extra full-table validation pass and avoids cloning the entire row set
    /// before serialization.
    ///
    /// This entrypoint is intended for high-volume writers that already know
    /// their table contents satisfy the schema through validated construction.
    /// On large tables it avoids a measurable amount of redundant work while
    /// still using the same bindings-driven storage-manager layout as
    /// [`save_with_bindings`](Self::save_with_bindings).
    pub(crate) fn persist_with_bindings(
        &self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        self.require_full_save_destination(&options.path)?;
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
        self.note_persisted_write(&options.path);
        let storage = CompositeStorage;
        storage
            .save_with_bindings_borrowed(
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
                options.data_manager_group.as_deref(),
                options.endian_format.is_big_endian(),
                options.tile_shape.as_deref(),
                bindings,
            )
            .map_err(|error| {
                tracing::error!(
                    casa.priority = "SEVERE",
                    path = %options.path.display(),
                    bindings = bindings.len(),
                    error = %error,
                    "failed to save CASA table with bindings"
                );
                error
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("storage_save");
        }
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        self.note_full_save(&options.path)?;
        tracing::info!(
            path = %options.path.display(),
            rows = self.inner.row_count(),
            bindings = bindings.len(),
            "saved CASA table with bindings"
        );
        Ok(())
    }

    /// Save the table with per-column bindings and column-value overrides.
    ///
    /// This advanced writer is for high-volume materialization paths that
    /// build some columns column-wise while the rest of the table is still
    /// represented as rows. Overrides can be materialized values, generated
    /// scalar cells, or deferred single-column tiled data-manager groups
    /// installed by a separate streaming path.
    ///
    /// The owning [`TableWritePlan`] validates overrides and bindings before
    /// entering this persistence backend.
    pub(crate) fn persist_with_bindings_and_column_overrides(
        &self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
        column_overrides: &ColumnOverrides,
    ) -> Result<(), TableError> {
        let row_count = column_overrides.effective_row_count(self.inner.row_count())?;
        if let Some(schema) = self.inner.schema() {
            for (column, _) in column_overrides.iter() {
                if !schema.contains_column(column) {
                    return Err(TableError::SchemaColumnUnknown {
                        column: column.clone(),
                    });
                }
            }
        }

        let mut profiler = StorageProfiler::start(format!(
            "Table::save_with_bindings_and_column_overrides path={}",
            options.path.display()
        ));
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "validate",
                Some(format!(
                    "rows={} bindings={} overrides={} skipped=true",
                    row_count,
                    bindings.len(),
                    column_overrides.len()
                )),
            );
        }
        self.note_persisted_write(&options.path);
        let storage = CompositeStorage;
        storage
            .save_with_bindings_and_column_overrides_borrowed(
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
                options.data_manager_group.as_deref(),
                options.endian_format.is_big_endian(),
                options.tile_shape.as_deref(),
                bindings,
                column_overrides,
            )
            .map_err(|error| {
                tracing::error!(
                    casa.priority = "SEVERE",
                    path = %options.path.display(),
                    bindings = bindings.len(),
                    overrides = column_overrides.len(),
                    error = %error,
                    "failed to save CASA table with column overrides"
                );
                error
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("storage_save");
        }
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&options.path);
        self.note_full_save(&options.path)?;
        tracing::info!(
            path = %options.path.display(),
            rows = row_count,
            bindings = bindings.len(),
            overrides = column_overrides.len(),
            "saved CASA table with column overrides"
        );
        Ok(())
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
        self.inner.note_metadata_change();
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

    /// What the typed selected 2-D channel-range readers hold for `column`
    /// besides their packed output; see [`SelectedReadFootprint`].
    ///
    /// Only `TiledShapeStMan` streams the selected channels. A column with no
    /// stored data manager (a table held in memory) counts as streamed.
    #[must_use]
    pub fn selected_channel_read_footprint(&self, column: &str) -> SelectedReadFootprint {
        self.column_data_manager(column)
            .map_or(SelectedReadFootprint::Streamed, |manager| {
                crate::storage::channel_read_footprint(manager)
            })
    }

    /// What the typed selected 1-D readers hold for `column` besides their
    /// packed output; see [`SelectedReadFootprint`].
    ///
    /// `IncrementalStMan`, `TiledColumnStMan` and `TiledShapeStMan` stream the
    /// selected rows. A column with no stored data manager (a table held in
    /// memory) counts as streamed.
    #[must_use]
    pub fn selected_cell_read_footprint(&self, column: &str) -> SelectedReadFootprint {
        self.column_data_manager(column)
            .map_or(SelectedReadFootprint::Streamed, |manager| {
                crate::storage::cell_read_footprint(manager)
            })
    }

    fn column_data_manager(&self, column: &str) -> Option<&str> {
        self.dm_info
            .iter()
            .find(|manager| manager.columns.iter().any(|name| name == column))
            .map(|manager| manager.dm_type.as_str())
    }

    /// Bytes of the largest tile of `column`'s tiled storage manager, which
    /// reading or writing one of its cells holds whole.
    ///
    /// Columns without a tiled manager (or with none on disk yet) and
    /// in-memory tables return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// When the table's description or the manager's header cannot be read.
    pub fn array_column_tile_bytes(&self, column: &str) -> Result<Option<usize>, TableError> {
        let Some(path) = self.path() else {
            return Ok(None);
        };
        crate::storage::CompositeStorage
            .array_column_tile_bytes(path, column)
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to inspect the tile size of column '{column}' in table {}: {err}",
                    path.display()
                ))
            })
    }

    /// The tile shape of a tiled array column, in casacore's axis order
    /// (the cell's axes, then rows): the store's natural read unit.
    ///
    /// Non-tiled columns and in-memory tables return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// When the table's description or the manager's header cannot be read.
    pub fn array_column_tile_shape(&self, column: &str) -> Result<Option<Vec<usize>>, TableError> {
        let Some(path) = self.path() else {
            return Ok(None);
        };
        crate::storage::CompositeStorage
            .array_column_tile_shape(path, column)
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to inspect the tile shape of column '{column}' in table {}: {err}",
                    path.display()
                ))
            })
    }

    /// Returns the channel-axis tile width for a tiled rank-2 array column.
    ///
    /// Non-tiled columns, in-memory tables, or columns whose tiled shape is not
    /// a 2-D cell plus row axis return `Ok(None)`.
    pub fn array_column_2d_channel_tile_width(
        &self,
        column: &str,
    ) -> Result<Option<usize>, TableError> {
        if let Some(schema) = self.schema()
            && !schema
                .columns()
                .iter()
                .any(|candidate| candidate.name() == column)
        {
            return Err(TableError::ColumnNotFound {
                row_index: 0,
                column: column.to_string(),
            });
        }
        let Some(path) = self.path() else {
            return Ok(None);
        };
        crate::storage::CompositeStorage
            .array_column_2d_channel_tile_width(path, column)
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to inspect tiled channel tile width for column '{column}' from table {}: {err}",
                    path.display()
                ))
            })
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
    /// On a table opened with locking that holds the write lock, the write
    /// is published in the lock file's sync data when the lock is released
    /// (see [`unlock`](Table::unlock)).
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
    /// A table opened with locking keeps any write it made to the disk under
    /// the write lock it holds: releasing the lock still publishes it, so
    /// other processes holding the table open re-read it. Only changes that
    /// never reached the disk are discarded without a trace.
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
        self.reload_disk_state_at(&path)
    }

    pub(super) fn reload_disk_state_at(&mut self, path: &Path) -> Result<(), TableError> {
        let mut reloaded = Table::open(TableOptions::new(path))?;
        self.inner = std::mem::take(&mut reloaded.inner);
        #[cfg(unix)]
        if self.lock_state.is_some() {
            self.inner.track_dirty_changes();
        }
        self.virtual_columns = std::mem::take(&mut reloaded.virtual_columns);
        self.virtual_bindings = std::mem::take(&mut reloaded.virtual_bindings);
        self.table_info = std::mem::take(&mut reloaded.table_info);
        self.dm_info = std::mem::take(&mut reloaded.dm_info);
        // Keep this handle's locks and unpublished writes. The reloaded
        // state is already backed by disk.
        self.mark_flushed();
        Ok(())
    }

    /// The byte order of the files of this table as they are on disk, from
    /// the marker in `table.dat`; for a reference table, the byte order of the
    /// table it refers to, as casacore's `RefTable` reports it. `None` for a
    /// table that has no directory, a concatenation of tables, or a reference
    /// chain that is too deep or cannot be read.
    ///
    /// # C++ equivalent
    ///
    /// `Table::endianFormat()`.
    pub fn stored_endian_format(&self) -> Result<Option<EndianFormat>, TableError> {
        let Some(path) = self.source_path.as_ref() else {
            return Ok(None);
        };
        Ok(stored_endian_format_of(path, 0))
    }

    /// Whether the directory of this table holds a reference table (a view
    /// of rows and columns of another table, as a selection makes), rather
    /// than a table of its own.
    pub fn is_reference_table(&self) -> Result<bool, TableError> {
        let Some(path) = self.source_path.as_ref() else {
            return Ok(false);
        };
        let control_path = path.join(crate::storage::TABLE_CONTROL_FILE);
        Ok(crate::storage::table_control::is_reference_descriptor(
            &control_path,
        )?)
    }

    /// The options that write this table in full as it is laid out on disk,
    /// when that layout is a single `StManAipsIO` group holding every column:
    /// the group's name and the byte order of its files are kept. `None` for
    /// any other layout (several groups, another manager, a table that is
    /// not on disk, a manager that has a name of its own in the header of its
    /// file, which the writer would drop), which a full
    /// [`save`](Table::save) would change.
    ///
    /// `StManAipsIO` keeps its columns in one file written in full, as
    /// casacore writes it, so such a table can be saved in full where a
    /// partial in-place save is not supported (for example with a record
    /// column) without changing its layout.
    pub fn single_stman_aipsio_options(&self) -> Result<Option<TableOptions>, TableError> {
        let Some(path) = self.source_path.as_ref() else {
            return Ok(None);
        };
        let control_path = path.join(crate::storage::TABLE_CONTROL_FILE);
        let crate::storage::table_control::TableDatResult::Plain(table_dat) =
            crate::storage::table_control::read_table_dat_dispatch(&control_path)?
        else {
            return Ok(None);
        };
        let managers = &table_dat.column_set.data_managers;
        let groups: std::collections::HashSet<&str> = table_dat
            .table_desc
            .columns
            .iter()
            .map(|column| column.data_manager_group.as_str())
            .collect();
        let [manager] = managers.as_slice() else {
            return Ok(None);
        };
        let (Some(group), 1) = (groups.iter().next(), groups.len()) else {
            return Ok(None);
        };
        if manager.type_name != "StManAipsIO"
            || manager.seq_nr != 0
            || table_dat
                .table_desc
                .columns
                .iter()
                .any(|column| column.data_manager_type != "StManAipsIO")
        {
            return Ok(None);
        }
        // A full save writes the empty manager name casa-rs always writes in
        // the file's header, where casacore keeps the name the manager was
        // bound with; a manager with a name of its own would lose it, so such
        // a table is not one to rewrite.
        let stored_name = crate::storage::stman_aipsio::name_from_file(&path.join(format!(
            "{}{}",
            crate::storage::TABLE_DATA_FILE_PREFIX,
            manager.seq_nr
        )));
        if stored_name.is_some_and(|name| !name.is_empty()) {
            return Ok(None);
        }
        let format = if table_dat.big_endian {
            EndianFormat::BigEndian
        } else {
            EndianFormat::LittleEndian
        };
        Ok(Some(
            TableOptions::new(path)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_data_manager_group(*group)
                .with_endian_format(format),
        ))
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
        self.require_full_save_destination(&opts.path)?;
        self.validate()?;
        self.note_persisted_write(&opts.path);
        let storage = CompositeStorage;
        storage.save_borrowed(
            &opts.path,
            &[],
            &[],
            self.inner.keywords(),
            self.inner.all_column_keywords(),
            self.inner.schema(),
            &self.table_info,
            &std::collections::HashSet::new(),
            &[],
            opts.data_manager,
            opts.data_manager_group.as_deref(),
            opts.casacore_default_ssm,
            opts.preserve_ssm_group_name,
            opts.endian_format.is_big_endian(),
            opts.tile_shape.as_deref(),
        )?;
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&opts.path);
        Ok(())
    }
}

fn build_group_rows_from_current_cells(
    table: &Table,
    row_count: usize,
    col_descs: &[crate::storage::table_control::ColumnDescContents],
) -> Result<Vec<RecordValue>, TableError> {
    let mut rows: Vec<_> = (0..row_count)
        .map(|_| RecordValue::new(Vec::with_capacity(col_descs.len())))
        .collect();
    let indices = (0..row_count).collect::<Vec<_>>();
    for col_desc in col_descs {
        let name = &col_desc.col_name;
        if col_desc.is_array && !col_desc.is_record() {
            // Managers such as IncrementalStMan scan array storage even for
            // a single row. Read the column once while rebuilding the group.
            let values = table.get_array_cells_owned_uncached(name, &indices)?;
            for (row, value) in rows.iter_mut().zip(values) {
                if let Some(value) = value {
                    row.push(RecordField::new(name.clone(), Value::Array(value)));
                }
            }
        } else {
            for (index, row) in rows.iter_mut().enumerate() {
                if let Some(value) = current_value_for_column(table, index, col_desc)? {
                    row.push(RecordField::new(name.clone(), value));
                }
            }
        }
    }
    Ok(rows)
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
        if !changed_columns.contains(col_desc.col_name.as_str()) {
            columns.push(None);
            continue;
        }
        if col_desc.is_array || col_desc.is_record() {
            return Ok(None);
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

fn collect_sparse_array_group_values_from_current_cells<'a, T>(
    table: &'a Table,
    row_indices: &[usize],
    col_descs: &[crate::storage::table_control::ColumnDescContents],
    changed_columns: &std::collections::HashSet<&str>,
    unchanged_scalars_allowed: bool,
    payload: impl Fn(&'a ArrayValue) -> T,
) -> Result<Option<SparseArrayColumns<T>>, TableError> {
    let mut columns = Vec::with_capacity(col_descs.len());
    for col_desc in col_descs {
        let unchanged = !changed_columns.contains(col_desc.col_name.as_str());
        if !col_desc.is_array || col_desc.is_record() {
            if unchanged_scalars_allowed && unchanged && !col_desc.is_record() {
                columns.push(None);
                continue;
            }
            return Ok(None);
        }
        if unchanged {
            columns.push(None);
            continue;
        }
        let Some(pending_values) = table.inner.pending_array_cells(&col_desc.col_name) else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(row_indices.len());
        if row_indices.windows(2).all(|pair| pair[0] < pair[1])
            && pending_values.windows(2).all(|pair| pair[0].0 < pair[1].0)
        {
            let mut slot = 0;
            for &row_index in row_indices {
                while slot < pending_values.len() && pending_values[slot].0 < row_index {
                    slot += 1;
                }
                if slot < pending_values.len() && pending_values[slot].0 == row_index {
                    values.push((row_index, Some(payload(&pending_values[slot].1))));
                }
            }
        } else {
            // Unsorted pending updates can repeat a row: the last value wins.
            let pending_by_row: std::collections::HashMap<usize, &ArrayValue> = pending_values
                .iter()
                .map(|(row_index, value)| (*row_index, value))
                .collect();
            for &row_index in row_indices {
                if let Some(value) = pending_by_row.get(&row_index) {
                    values.push((row_index, Some(payload(value))));
                }
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

/// The byte order of the table at `path`, following reference tables to the
/// table they refer to (`depth` bounds a chain that loops).
fn stored_endian_format_of(path: &Path, depth: usize) -> Option<EndianFormat> {
    use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
    let control_path = path.join(crate::storage::TABLE_CONTROL_FILE);
    match read_table_dat_dispatch(&control_path).ok()? {
        TableDatResult::Plain(table_dat) => Some(if table_dat.big_endian {
            EndianFormat::BigEndian
        } else {
            EndianFormat::LittleEndian
        }),
        TableDatResult::Ref(reference) if depth < 32 => stored_endian_format_of(
            &crate::table_path::add_directory_for_link(&reference.parent_relative_path, path),
            depth + 1,
        ),
        _ => None,
    }
}
