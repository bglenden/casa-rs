// SPDX-License-Identifier: LGPL-3.0-or-later
//! A disk table whose schema persists and whose cells live in MemoryStMan.

use std::fs;

use crate::storage::table_control::{TableDatContents, write_table_dat};
use crate::storage::{DataManagerInfo, StorageError, TABLE_CONTROL_FILE, TABLE_INFO_FILE};

use super::*;

const MANAGER: &str = "MemoryStMan";

impl Table {
    /// Bind this table's cells to a single named MemoryStMan, and save its
    /// descriptor to disk. This table keeps the current cell values in
    /// memory; reopening the path starts with default values, as in
    /// casacore's `MSMBase::open64`.
    ///
    /// The output path must not already exist. This operation is intended
    /// for a newly constructed table (e.g. a FITS import with memory storage).
    #[cfg(unix)]
    pub fn into_named_memory_manager(
        mut self,
        path: &Path,
        manager_name: &str,
        lock: LockOptions,
    ) -> Result<Self, TableError> {
        let io = |error| StorageError::Io(error);
        fs::create_dir(path).map_err(io)?;
        if let Err(error) = self.write_memory_layout(path, manager_name) {
            let _ = fs::remove_dir_all(path);
            return Err(error);
        }
        self.kind = TableKind::Memory;
        self.source_path = Some(path.to_path_buf());
        self.dm_info = vec![DataManagerInfo {
            dm_type: MANAGER.to_string(),
            name: manager_name.into(),
            seq_nr: 0,
            columns: self
                .schema()
                .expect("checked when writing")
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect(),
        }];
        Self::open_with_lock_using(TableOptions::new(path), lock, |_| Ok(self))
    }

    fn write_memory_layout(&self, path: &Path, name: &str) -> Result<(), TableError> {
        let schema = self.schema().ok_or_else(|| {
            StorageError::FormatMismatch("a MemoryStMan descriptor requires a schema".into())
        })?;
        let mut contents = TableDatContents::from_snapshot(
            schema,
            self.keywords(),
            self.inner.all_column_keywords(),
            self.row_count() as u64,
            MANAGER,
            &[],
            EndianFormat::default().is_big_endian(),
        );
        if schema.columns().is_empty() {
            contents.column_set.data_managers.clear();
        }
        for column in &mut contents.table_desc.columns {
            column.data_manager_type = MANAGER.into();
            column.data_manager_group = name.into();
        }
        write_table_dat(&path.join(TABLE_CONTROL_FILE), &contents)?;
        fs::write(path.join(TABLE_INFO_FILE), self.table_info.to_string())
            .map_err(StorageError::Io)?;
        Ok(())
    }

    /// Whether this table stores its cells in a memory manager while keeping
    /// a descriptor and a lock file at a persistent path.
    pub fn is_named_memory_manager(&self) -> bool {
        self.kind == TableKind::Memory && self.source_path.is_some()
    }

    /// Finish a write to a named MemoryStMan table. Metadata changes write
    /// the descriptor; cell changes stay on the current handle.
    pub fn finish_memory_manager_write(&self, metadata: bool) -> Result<(), TableError> {
        if self.is_named_memory_manager()
            && let Some(path) = &self.source_path
        {
            if metadata {
                let group = self
                    .dm_info
                    .first()
                    .map_or(MANAGER, |manager| manager.name.as_str());
                self.note_persisted_write(path);
                self.write_memory_layout(path, group)?;
            }
            self.mark_flushed();
        }
        Ok(())
    }

    pub(super) fn memory_manager_snapshot(snapshot: &StorageSnapshot) -> bool {
        !snapshot.dm_info.is_empty()
            && snapshot
                .dm_info
                .iter()
                .all(|manager| manager.dm_type == MANAGER)
    }

    pub(super) fn from_memory_manager_snapshot(
        path: PathBuf,
        mut snapshot: StorageSnapshot,
        rows: usize,
    ) -> Self {
        // MSMBase::flush writes no manager name. A reopened manager reports
        // its type name as its group (even if a named handle created it).
        for manager in &mut snapshot.dm_info {
            manager.name = MANAGER.into();
        }
        let default = snapshot
            .schema
            .as_ref()
            .expect("memory table has a schema")
            .default_row();
        let inner = TableImpl::with_rows_keywords_and_schema(
            vec![default; rows],
            Vec::new(),
            snapshot.keywords,
            snapshot.column_keywords,
            snapshot.schema,
        );
        Self {
            inner,
            source_path: Some(path),
            kind: TableKind::Memory,
            virtual_columns: snapshot.virtual_columns,
            virtual_bindings: Vec::new(),
            table_info: snapshot.table_info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            measures: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        }
    }
}
