// SPDX-License-Identifier: LGPL-3.0-or-later
//! A disk table whose schema persists and whose cells live in MemoryStMan.

use std::fs;

use crate::storage::data_type::CasacoreDataType;
use crate::storage::stman_array_file::make_empty_array;
use crate::storage::table_control::{TableDatContents, write_table_dat};
use crate::storage::{DataManagerInfo, StorageError, TABLE_CONTROL_FILE, TABLE_INFO_FILE};
use casa_types::ScalarValue;

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
    pub fn save_with_memory_manager(
        &mut self,
        path: &Path,
        manager_name: &str,
    ) -> Result<(), TableError> {
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
        Ok(())
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
        for column in &mut contents.table_desc.columns {
            column.data_manager_type = MANAGER.into();
            column.data_manager_group = name.into();
        }
        write_table_dat(&path.join(TABLE_CONTROL_FILE), &contents)?;
        fs::write(path.join(TABLE_INFO_FILE), self.table_info.to_string())
            .map_err(StorageError::Io)?;
        Ok(())
    }

    /// Save schema, keywords and row count for a named table created with
    /// `save_with_memory_manager`. Cells are never written by this method.
    /// For an anonymous in-memory table this is a no-op.
    pub fn flush_memory_manager_layout(&self) -> Result<(), TableError> {
        if let (TableKind::Memory, Some(path), [manager]) =
            (self.kind, &self.source_path, self.dm_info.as_slice())
            && manager.dm_type == MANAGER
        {
            self.write_memory_layout(path, &manager.name)?;
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
        let default = default_row(snapshot.schema.as_ref().expect("memory table has a schema"));
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

fn default_row(schema: &TableSchema) -> RecordValue {
    let value = |column: &ColumnSchema| match (column.column_type(), column.data_type()) {
        (ColumnType::Scalar, Some(ty)) => Some(Value::Scalar(zero(ty))),
        (ColumnType::Array(ArrayShapeContract::Fixed { shape }), Some(ty)) => Some(
            make_empty_array(CasacoreDataType::from_primitive_type(ty, false), shape),
        ),
        _ => None,
    };
    RecordValue::new(
        schema
            .columns()
            .iter()
            .filter_map(|col| value(col).map(|value| RecordField::new(col.name(), value)))
            .collect(),
    )
}

fn zero(ty: PrimitiveType) -> ScalarValue {
    use PrimitiveType::*;
    match ty {
        Bool => ScalarValue::Bool(false),
        UInt8 => ScalarValue::UInt8(0),
        UInt16 => ScalarValue::UInt16(0),
        UInt32 => ScalarValue::UInt32(0),
        Int16 => ScalarValue::Int16(0),
        Int32 => ScalarValue::Int32(0),
        Int64 => ScalarValue::Int64(0),
        Float32 => ScalarValue::Float32(0.0),
        Float64 => ScalarValue::Float64(0.0),
        Complex32 => ScalarValue::Complex32(Default::default()),
        Complex64 => ScalarValue::Complex64(Default::default()),
        String => ScalarValue::String(Default::default()),
    }
}
