// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
#[cfg(unix)]
use crate::lock::read_sync_data_from_table_dir;

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
        let snapshot = StorageSnapshot {
            row_count: self.inner.row_count(),
            rows: self.inner.rows()?.to_vec(),
            undefined_cells: self.inner.undefined_cells()?.to_vec(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save(
            &options.path,
            &snapshot,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
        )?;
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
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use casacore_tables::{Table, TableOptions, DataManagerKind, ColumnBinding};
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
        let snapshot = StorageSnapshot {
            row_count: self.inner.row_count(),
            rows: self.inner.rows()?.to_vec(),
            undefined_cells: self.inner.undefined_cells()?.to_vec(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save_with_bindings(
            &options.path,
            &snapshot,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
            bindings,
        )?;
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
