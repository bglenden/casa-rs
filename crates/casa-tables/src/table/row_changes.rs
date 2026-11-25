// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
use crate::storage::column_changes::ColumnGroupPayload;
use crate::storage::row_changes::{Rows, StagedEdit};

/// One column to install in an existing disk table.
pub struct ColumnAddition {
    /// The column's schema.
    pub schema: ColumnSchema,
    /// The default cell for each existing row, or `None` if undefined.
    pub default: Option<Value>,
    /// Keyword values stored with the new descriptor.
    pub keywords: RecordValue,
    /// Preferred manager class when a new binding is needed.
    pub manager_type: Option<String>,
    /// Preferred group name when a new binding is needed.
    pub manager_group: Option<String>,
}

/// Install the new columns together in their own storage manager.
pub struct ManagerAddition {
    /// Casacore manager class (for example, `StandardStMan`).
    pub kind: String,
    /// Name of this manager in the table.
    pub name: String,
    /// Manager configuration (such as tiled default shape).
    pub spec: RecordValue,
}

/// A new CASA CompressComplex column and its completed backing data.
pub struct CompressedComplexArray<'a> {
    /// Descriptor for the logical Complex32 array.
    pub schema: ColumnSchema,
    /// Name of the StandardStMan scale and offset manager.
    pub scalar_manager: &'a str,
    /// Name of the TiledShapeStMan packed-cell manager.
    pub tiled_manager: &'a str,
    /// Packed Int32 cells in table row order.
    pub packed: crate::StreamedTiledShapeColumn,
    /// Per-row floating-point scales.
    pub scales: Vec<Option<ScalarValue>>,
    /// Per-row floating-point offsets.
    pub offsets: Vec<Option<ScalarValue>>,
}

impl CompressedComplexArray<'_> {
    fn groups(
        &self,
    ) -> [(
        Vec<ColumnAddition>,
        Option<ManagerAddition>,
        ColumnGroupPayload<'_>,
    ); 3] {
        let logical = self.schema.name();
        let stored = format!("{logical}_COMPRESSED");
        let scale = format!("{logical}_SCALE");
        let offset = format!("{logical}_OFFSET");
        let add = |schema, keywords| ColumnAddition {
            schema,
            default: None,
            keywords,
            manager_type: None,
            manager_group: None,
        };
        let group = |kind: &str, name: &str| {
            Some(ManagerAddition {
                kind: kind.to_owned(),
                name: name.to_owned(),
                spec: RecordValue::default(),
            })
        };
        let scalars = [&offset, &scale].map(|column| {
            add(
                ColumnSchema::scalar(column, PrimitiveType::Float32),
                RecordValue::default(),
            )
        });
        let packed = ColumnSchema::array_variable(&stored, PrimitiveType::Int32, Some(2));
        let keywords =
            crate::storage::virtual_compress::automatic_complex_keywords(&stored, &scale, &offset);
        [
            (
                scalars.into(),
                group("StandardStMan", self.scalar_manager),
                ColumnGroupPayload::Scalars([&self.offsets, &self.scales]),
            ),
            (
                vec![add(packed, RecordValue::default())],
                group("TiledShapeStMan", self.tiled_manager),
                ColumnGroupPayload::Tiled(&self.packed),
            ),
            (
                vec![add(self.schema.clone(), keywords)],
                group("CompressComplex", logical),
                ColumnGroupPayload::CompressedEngine,
            ),
        ]
    }
}

impl Table {
    /// Append rows to a plain disk table without changing its storage
    /// manager bindings. Standard and AipsIO managers use `defaults`;
    /// incremental managers inherit their preceding values and variable
    /// tiled arrays remain undefined.
    ///
    /// # Errors
    ///
    /// The table has no durable plain layout, has unsaved edits, contains
    /// a storage manager that cannot append, or cannot be locked or written.
    pub fn append_rows_in_place(
        &mut self,
        count: usize,
        defaults: &RecordValue,
    ) -> Result<(), TableError> {
        if count == 0 {
            return Ok(());
        }
        self.edit_rows_in_place(Rows::Append { count, defaults }, true)
    }

    /// Append distinct records in a single edit, preserving the table's
    /// existing manager bindings. Standard, AipsIO and incremental managers
    /// accept explicit record values. Omitted cells keep the manager's
    /// normal append defaults, including inherited incremental values.
    ///
    /// # Errors
    /// The table cannot be locked or written, a row disagrees with its
    /// schema, or a manager cannot append explicit records.
    pub fn append_records_in_place(&mut self, records: &[RecordValue]) -> Result<(), TableError> {
        self.append_records(records, true)
    }

    /// Append records using [`Self::append_records_in_place`]'s persistence
    /// rules and consume the handle. The written layout is available to a
    /// fresh reader without reloading this handle first.
    ///
    /// # Errors
    /// As in [`Self::append_records_in_place`].
    pub fn append_records_and_close(mut self, records: &[RecordValue]) -> Result<(), TableError> {
        self.append_records(records, false)
    }

    fn append_records(&mut self, records: &[RecordValue], reload: bool) -> Result<(), TableError> {
        if records.is_empty() {
            return Ok(());
        }
        let schema = self.schema().ok_or(TableError::PreparedRowRequiresSchema)?;
        for (offset, row) in records.iter().enumerate() {
            let row_index = self.row_count() + offset;
            for field in row.fields() {
                let column = schema.column(&field.name).ok_or_else(|| {
                    TableError::RowContainsUnknownColumn {
                        row_index,
                        column: field.name.clone(),
                    }
                })?;
                super::columns::validate_cell_against_schema_column(
                    row_index,
                    column,
                    Some(&field.value),
                )?;
            }
        }
        let defaults = schema.default_row();
        self.edit_rows_in_place(
            Rows::AppendRecords {
                records,
                defaults: &defaults,
            },
            reload,
        )
    }

    /// Keep the given rows in an existing plain table, in ascending order,
    /// and delete all others. This operation preserves manager bindings.
    ///
    /// # Errors
    ///
    /// The row sequence is unsorted or contains repetitions or invalid row
    /// numbers; the table cannot be written or has a manager that refuses
    /// to remove rows (for example, IncrementalStMan).
    pub fn retain_rows_in_place(&mut self, kept: &[usize]) -> Result<(), TableError> {
        if kept.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(TableError::Storage(
                "retained rows must strictly increase".into(),
            ));
        }
        self.edit_rows_in_place(Rows::Keep(kept), true)
    }

    /// Add columns without changing the files for managers that do not
    /// receive a column. When supplied, `manager` binds all the new
    /// columns to one new manager. Otherwise the first manager that
    /// accepts addition supplies the storage, as in casacore.
    ///
    /// # Errors
    ///
    /// The table is not a clean plain table, a column or manager name
    /// already exists, the selected binding is unsupported, or a storage
    /// file cannot be read or written.
    pub fn add_columns_in_place(
        &mut self,
        columns: Vec<ColumnAddition>,
        manager: Option<ManagerAddition>,
    ) -> Result<(), TableError> {
        if columns.is_empty() {
            return Ok(());
        }
        self.edit_clean_layout("add columns in place", move |this, path, mut dat, old| {
            dat.raise_row_count(old);
            let plan =
                StagedEdit::stage_columns(path, dat, columns, manager, this.data_manager_info())?;
            this.note_persisted_write(path);
            plan.publish()?;
            this.reload_disk_state_at(path)
        })
    }

    /// Install complete CASA CompressComplex arrays with their packed cells
    /// and automatic per-row scale and offset. Stage all requested groups
    /// before publishing a single descriptor update.
    ///
    /// # Errors
    /// A binding conflicts with the current table, the data do not match
    /// the requested columns, or a storage file cannot be published.
    pub fn add_compressed_complex_arrays_in_place(
        &mut self,
        arrays: &[CompressedComplexArray<'_>],
    ) -> Result<(), TableError> {
        if arrays.iter().any(|array| {
            array.schema.data_type() != Some(PrimitiveType::Complex32)
                || [
                    array.packed.row_count(),
                    array.scales.len(),
                    array.offsets.len(),
                ]
                .into_iter()
                .any(|count| count != self.row_count())
        }) {
            return Err(TableError::Storage(
                "compressed complex layout requires one packed cell, scale and offset per row"
                    .into(),
            ));
        }
        self.edit_clean_layout(
            "add compressed arrays in place",
            move |this, path, mut dat, old| {
                dat.raise_row_count(old);
                let groups = arrays.iter().flat_map(CompressedComplexArray::groups);
                let plan =
                    StagedEdit::stage_column_groups(path, dat, groups, this.data_manager_info())?;
                this.note_persisted_write(path);
                plan.publish()?;
                this.reload_disk_state_at(path)
            },
        )
    }

    fn edit_rows_in_place(&mut self, rows: Rows<'_>, reload: bool) -> Result<(), TableError> {
        self.edit_clean_layout("edit rows in place", |this, path, dat, old| {
            if let Rows::Keep(kept) = rows {
                if kept.last().is_some_and(|&row| row as u64 >= old) {
                    return Err(TableError::Storage("retained row is out of range".into()));
                }
                if kept.len() as u64 == old {
                    return Ok(());
                }
            }
            let plan = StagedEdit::stage(path, dat, old, rows)?;
            this.note_persisted_write(path);
            plan.publish()?;
            if reload {
                // Publish the count before reopening; the old lock sync
                // count otherwise hides removals.
                this.note_full_save(path)?;
                this.reload_disk_state_at(path)
            } else {
                // The handle is consumed. Its lock still announces the
                // persisted row count when released.
                this.mark_flushed();
                Ok(())
            }
        })
    }

    fn edit_clean_layout(
        &mut self,
        operation: &str,
        edit: impl FnOnce(
            &mut Self,
            &Path,
            crate::storage::TableDatContents,
            u64,
        ) -> Result<(), TableError>,
    ) -> Result<(), TableError> {
        let path = self.source_path.clone().ok_or_else(|| {
            TableError::Storage("in-place row edits require an existing table".into())
        })?;
        if self.kind != TableKind::Plain || self.holds_unflushed_changes() {
            return Err(TableError::Storage(
                "in-place row edits require a clean plain table".into(),
            ));
        }
        let auto_unlock = self.begin_write_operation(operation)?;
        let result = (|| {
            #[cfg(unix)]
            if auto_unlock {
                self.reload_if_published()?;
            }
            let dat = crate::storage::table_control::read_table_dat(
                &path.join(crate::storage::TABLE_CONTROL_FILE),
            )?;
            let old = self.require_persisted_rows(&path, &dat)?;
            edit(self, &path, dat, old)
        })();
        self.finish_write_operation(auto_unlock, result)
    }
}
