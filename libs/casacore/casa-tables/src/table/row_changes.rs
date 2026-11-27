// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
use crate::TableWriteLock;
use crate::lock::WriteSyncCheckpoint;
use crate::storage::column_changes::ColumnGroupPayload;
use crate::storage::row_changes::{ReversibleEdit, Rows, StagedEdit};
use crate::storage::tiled_stman::shape_append::ShapeAppend;

/// Appended array cells that can be copied directly from the donor's
/// encoded storage. Other cells still need to be supplied as records.
pub struct EncodedArrayAppend(ShapeAppend);

/// Optional changes applied in the same staged edit as appended records.
#[derive(Default)]
pub struct RecordAppendOptions<'a> {
    /// Whole-donor array cells supplied directly from encoded storage.
    pub encoded_arrays: Option<&'a EncodedArrayAppend>,
    /// Set an existing scalar column before appending the new rows.
    pub existing_scalar: Option<(&'a str, &'a ScalarValue)>,
    /// Set a column keyword in the new table descriptor.
    pub column_keyword: Option<(&'a str, &'a str, &'a Value)>,
}

impl EncodedArrayAppend {
    /// Columns for which every appended cell is provided by the encoded plan.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.columns()
    }
}

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
    pub packed: crate::streamed_tiled::StreamedTiledShapeColumn,
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

/// A staged edit to one physical table, holding its write lock.
pub struct PreparedTableEdit<'lock> {
    edit: ReversibleEdit,
    lock: &'lock mut TableWriteLock,
    original_sync: WriteSyncCheckpoint,
    attempted_sync: bool,
}

impl PreparedTableEdit<'_> {
    /// Write the staged changes, retaining the original files for rollback.
    ///
    /// # Errors
    /// The table identity or one of the table writes failed. Call
    /// [`Self::restore`] to undo any partial updates before releasing the lock.
    pub fn publish(&mut self) -> Result<(), TableError> {
        if !self.lock.matches_table_path(self.lock.path()) {
            return Err(TableError::Storage(format!(
                "{} was replaced before publication",
                self.lock.path().display()
            )));
        }
        self.lock.record_write();
        self.edit.apply().map_err(Into::into)
    }

    /// Roll back this edit and discard its staging files on success.
    ///
    /// # Errors
    /// On recovery failure, returns the path to the retained backups.
    pub fn restore(mut self) -> Result<(), TableError> {
        if !self.lock.matches_table_path(self.lock.path()) {
            return Err(TableError::Storage(format!(
                "{} was replaced before rollback. Original files staged at {}",
                self.lock.path().display(),
                self.edit.preserve().display(),
            )));
        }
        if let Err(error) = self.edit.restore() {
            return Err(TableError::Storage(format!(
                "{error}; original table files retained at {}",
                self.edit.preserve().display()
            )));
        }
        self.lock
            .restore_sync(self.original_sync, self.attempted_sync)
    }

    /// Announce the new shape to casacore while keeping the lock and backups.
    ///
    /// Retain this append until every edit in the batch has been announced.
    /// Dropping the append then discards its backups.
    ///
    /// # Errors
    /// Returns an error if the write could not be announced to other readers.
    pub fn synchronize(&mut self) -> Result<(), TableError> {
        self.synchronize_with(TableWriteLock::sync_recorded_write)
    }

    fn synchronize_with(
        &mut self,
        sync: impl FnOnce(&mut TableWriteLock) -> Result<(), TableError>,
    ) -> Result<(), TableError> {
        self.attempted_sync = true;
        sync(self.lock)
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
        let defaults = self.record_append_defaults(records)?;
        self.edit_rows_in_place(
            Rows::AppendRecords {
                records,
                defaults: &defaults,
                encoded: None,
                existing_scalar: None,
            },
            reload,
        )
    }

    /// Stage distinct appended records without changing existing table files.
    ///
    /// The caller must open this table after acquiring its write lock. Use
    /// [`PreparedTableEdit::publish`] to apply the changes and keep the
    /// prior files while other operations in the batch are published.
    ///
    /// # Errors
    /// The table or lock is invalid, the schema cannot admit the rows,
    /// or a manager cannot prepare append changes.
    pub fn prepare_record_append<'lock>(
        &self,
        records: &[RecordValue],
        lock: &'lock mut TableWriteLock,
    ) -> Result<PreparedTableEdit<'lock>, TableError> {
        self.prepare_record_append_with_options(records, RecordAppendOptions::default(), lock)
    }

    /// Identify compatible tiled array columns for a whole-donor append.
    ///
    /// Open the donor and target under their read and write locks and retain
    /// both locks through staging. Only supply names whose cells can be
    /// copied without transformation. Read the remaining cells normally.
    ///
    /// # Errors
    /// The donor or target is unpersisted, or its headers cannot be read.
    pub fn plan_encoded_array_append(
        &self,
        donor: &Self,
        columns: &[&str],
    ) -> Result<EncodedArrayAppend, TableError> {
        let target = self.path().ok_or(TableError::PreparedRowRequiresSchema)?;
        let incoming = donor.path().ok_or(TableError::PreparedRowRequiresSchema)?;
        if columns.is_empty() || self.kind != TableKind::Plain || donor.kind != TableKind::Plain {
            return Ok(EncodedArrayAppend(ShapeAppend::empty(
                target,
                self.row_count(),
                donor.row_count(),
            )));
        }
        Ok(EncodedArrayAppend(ShapeAppend::plan(
            target, incoming, columns,
        )?))
    }

    /// Stage distinct appended records with additional atomic edits. Records
    /// omit columns already supplied by the encoded donor plan.
    ///
    /// # Errors
    /// As in [`Self::prepare_record_append`], or the options conflict with
    /// the target schema, current storage, or supplied records.
    pub fn prepare_record_append_with_options<'lock>(
        &self,
        records: &[RecordValue],
        options: RecordAppendOptions<'_>,
        lock: &'lock mut TableWriteLock,
    ) -> Result<PreparedTableEdit<'lock>, TableError> {
        if records.is_empty() {
            return Err(TableError::Storage("no records to append".into()));
        }
        let (path, mut dat, old) = self.staged_edit_snapshot(lock)?;
        let encoded = options.encoded_arrays.map(|encoded| &encoded.0);
        if let Some(encoded) = encoded {
            encoded.validate(path, old, records.len())?;
        }
        if let Some((name, value)) = options.existing_scalar {
            let column = self
                .schema()
                .and_then(|schema| schema.column(name))
                .ok_or_else(|| TableError::SchemaColumnUnknown {
                    column: name.into(),
                })?;
            super::columns::validate_cell_against_schema_column(
                0,
                column,
                Some(&Value::Scalar(value.clone())),
            )?;
        }
        if let Some((name, key, value)) = options.column_keyword {
            let column = dat
                .table_desc
                .columns
                .iter_mut()
                .find(|column| column.col_name == name)
                .ok_or_else(|| TableError::SchemaColumnUnknown {
                    column: name.into(),
                })?;
            column.keywords.upsert(key, value.clone());
        }
        let defaults = self.record_append_defaults(records)?;
        let rows = Rows::AppendRecords {
            records,
            defaults: &defaults,
            encoded,
            existing_scalar: options.existing_scalar,
        };
        let edit = StagedEdit::stage(path, dat, old, rows)?.prepare_publication(true)?;
        let original_sync = lock.checkpoint_sync()?;
        Ok(PreparedTableEdit {
            edit,
            lock,
            original_sync,
            attempted_sync: false,
        })
    }

    /// Stage replacements for existing physical columns without changing
    /// the row count or unrelated storage managers. The input vectors are
    /// in table row order. Column keywords can be included in the same edit.
    ///
    /// # Errors
    /// The table cannot be locked, a column or keyword target is unknown, a
    /// cell disagrees with the schema, or its storage cannot be rewritten.
    pub fn prepare_column_replacement<'lock>(
        &self,
        columns: &std::collections::BTreeMap<String, Vec<Option<Value>>>,
        column_keyword: Option<(&str, &str, &Value)>,
        lock: &'lock mut TableWriteLock,
    ) -> Result<PreparedTableEdit<'lock>, TableError> {
        let (path, mut dat, old) = self.staged_edit_snapshot(lock)?;
        let schema = self.schema().ok_or(TableError::PreparedRowRequiresSchema)?;
        for (name, values) in columns {
            let column = schema
                .column(name)
                .ok_or_else(|| TableError::SchemaColumnUnknown {
                    column: name.clone(),
                })?;
            if values.len() as u64 != old {
                return Err(TableError::Storage(format!(
                    "column {name} has {} replacement cells for {old} rows",
                    values.len()
                )));
            }
            for (row, value) in values.iter().enumerate() {
                super::columns::validate_cell_against_schema_column(row, column, value.as_ref())?;
            }
        }
        if let Some((name, key, value)) = column_keyword {
            let column = dat
                .table_desc
                .columns
                .iter_mut()
                .find(|column| column.col_name == name)
                .ok_or_else(|| TableError::SchemaColumnUnknown {
                    column: name.into(),
                })?;
            column.keywords.upsert(key, value.clone());
        }
        let rows = Rows::ReplaceColumns(columns);
        let edit = StagedEdit::stage(path, dat, old, rows)?.prepare_publication(true)?;
        let original_sync = lock.checkpoint_sync()?;
        Ok(PreparedTableEdit {
            edit,
            lock,
            original_sync,
            attempted_sync: false,
        })
    }

    fn staged_edit_snapshot(
        &self,
        lock: &TableWriteLock,
    ) -> Result<(&Path, crate::storage::TableDatContents, u64), TableError> {
        let path = self.path().ok_or(TableError::PreparedRowRequiresSchema)?;
        if self.kind != TableKind::Plain
            || self.holds_unflushed_changes()
            || !lock.matches_table_path(path)
        {
            return Err(TableError::Storage(
                "preparing an edit requires a clean, locked plain table".into(),
            ));
        }
        let dat = crate::storage::table_control::read_table_dat(
            &path.join(crate::storage::TABLE_CONTROL_FILE),
        )?;
        dat.require_separate_storage_for_write()?;
        let old = self.require_persisted_rows(path, &dat)?;
        Ok((path, dat, old))
    }

    fn record_append_defaults(&self, records: &[RecordValue]) -> Result<RecordValue, TableError> {
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
        Ok(schema.default_row())
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
            dat.require_separate_storage_for_write()?;
            let old = self.require_persisted_rows(&path, &dat)?;
            edit(self, &path, dat, old)
        })();
        self.finish_write_operation(auto_unlock, result)
    }
}

#[cfg(all(test, unix))]
mod prepared_append_tests {
    use super::*;

    fn record(value: i32) -> RecordValue {
        RecordValue::new(vec![casa_core::RecordField::new(
            "id",
            Value::Scalar(casa_core::ScalarValue::Int32(value)),
        )])
    }

    fn make_table(path: &Path) -> (Table, TableWriteLock) {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table.add_row(record(4)).unwrap();
        table
            .save(TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan))
            .unwrap();
        let mut lock = TableWriteLock::acquire_to_reread(path, 1).unwrap();
        lock.record_write();
        lock.sync_recorded_write().unwrap();
        (Table::open(TableOptions::new(path)).unwrap(), lock)
    }

    #[test]
    fn prepared_scalar_replacement_preserves_its_neighbors_and_can_roll_back() {
        use casa_core::RecordField;
        use std::collections::BTreeMap;

        let root = tempfile::tempdir().unwrap();
        for (manager, endian) in [
            (
                DataManagerKind::StandardStMan,
                crate::EndianFormat::BigEndian,
            ),
            (
                DataManagerKind::StandardStMan,
                crate::EndianFormat::LittleEndian,
            ),
            (
                DataManagerKind::IncrementalStMan,
                crate::EndianFormat::BigEndian,
            ),
        ] {
            let path = root.path().join(format!("{manager:?}-{endian:?}"));
            let schema = TableSchema::new(vec![
                ColumnSchema::scalar("id", PrimitiveType::Int32),
                ColumnSchema::scalar("keep", PrimitiveType::Float64),
            ])
            .unwrap();
            let mut table = Table::with_schema(schema);
            let rows = if manager == DataManagerKind::StandardStMan {
                3000
            } else {
                69
            };
            for id in 0..rows {
                table
                    .add_row(RecordValue::new(vec![
                        RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                        RecordField::new(
                            "keep",
                            Value::Scalar(ScalarValue::Float64(f64::from(id) / 2.0)),
                        ),
                    ]))
                    .unwrap();
            }
            table
                .save(
                    TableOptions::new(&path)
                        .with_data_manager(manager)
                        .with_endian_format(endian),
                )
                .unwrap();
            let mut guard = TableWriteLock::acquire_to_reread(&path, 1).unwrap();
            let table = Table::open(TableOptions::new(&path)).unwrap();
            let before = table.scalar_columns_owned(&["id", "keep"]).unwrap();
            let manager_file = path.join("table.f0");
            let original = std::fs::read(&manager_file).unwrap();
            let replacement = BTreeMap::from([(
                "id".to_owned(),
                (0..rows)
                    .map(|id| Some(Value::Scalar(ScalarValue::Int32(id ^ 1))))
                    .collect(),
            )]);
            let mut staged = table
                .prepare_column_replacement(&replacement, None, &mut guard)
                .unwrap();
            staged.publish().unwrap();
            staged.synchronize().unwrap();
            let reopen = || {
                Table::open(TableOptions::new(&path))
                    .unwrap()
                    .scalar_columns_owned(&["id", "keep"])
                    .unwrap()
            };
            let after = reopen();
            assert_eq!(
                after["id"],
                (0..rows)
                    .map(|id| Some(ScalarValue::Int32(id ^ 1)))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(after["keep"], before["keep"]);
            if manager == DataManagerKind::StandardStMan {
                let edited = std::fs::read(&manager_file).unwrap();
                assert_eq!(edited.len(), original.len());
                assert_eq!(
                    edited.iter().zip(&original).filter(|(a, b)| a != b).count(),
                    rows as usize
                );
            }
            staged.restore().unwrap();
            assert_eq!(reopen(), before);
            assert_eq!(std::fs::read(&manager_file).unwrap(), original);
        }
    }

    #[test]
    fn failed_sync_of_one_append_restores_both_tables_and_their_published_counts() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        let (a, mut a_lock) = make_table(&first);
        let (b, mut b_lock) = make_table(&second);
        let counters = [
            a_lock.modify_counter().unwrap(),
            b_lock.modify_counter().unwrap(),
        ];
        let mut edited_a = a.prepare_record_append(&[record(8)], &mut a_lock).unwrap();
        let mut edited_b = b.prepare_record_append(&[record(9)], &mut b_lock).unwrap();
        edited_a.publish().unwrap();
        edited_b.publish().unwrap();
        edited_a.synchronize().unwrap();
        assert!(
            edited_b
                .synchronize_with(|lock| {
                    lock.sync_recorded_write()?;
                    Err(TableError::Storage("injected sync failure".into()))
                })
                .is_err()
        );
        edited_b.restore().unwrap();
        edited_a.restore().unwrap();

        for (path, lock, old_counter) in [
            (&first, a_lock, counters[0]),
            (&second, b_lock, counters[1]),
        ] {
            let recorded = crate::lock::read_sync_data_from_table_dir(path)
                .unwrap()
                .unwrap();
            assert_eq!(recorded.nrrow, 1);
            assert_eq!(Some(recorded.modify_counter), old_counter);
            lock.release().unwrap();
            let reopened = Table::open(TableOptions::new(path)).unwrap();
            assert_eq!(reopened.rows().unwrap(), &[record(4)]);
        }
    }
}
