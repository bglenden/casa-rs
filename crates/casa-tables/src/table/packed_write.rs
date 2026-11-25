// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::Range;

use super::*;
use crate::TableWriteLock;
use crate::storage::TABLE_CONTROL_FILE;
use crate::storage::data_type::CasacoreDataType;
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::storage::tiled_stman::packed_update::PackedArrayUpdate;

impl TableWritePlan<'_> {
    /// Replace consecutive rank-2 Complex32 cells using native-width values
    /// in Fortran order, with cells back to back. The caller holds the
    /// table's write lock.
    ///
    /// Returns `false` without writing if the cells do not cover complete
    /// tile bands in a single-column TiledShapeStMan cube. The final band
    /// may include padding past the end of the cube. The caller can then
    /// use the ordinary cell write path. An error can follow a partial write.
    pub fn try_save_packed_complex_rows(
        &mut self,
        column: &str,
        rows: Range<usize>,
        shape: [usize; 2],
        values: &[Complex32],
    ) -> Result<bool, TableError> {
        let table = &mut *self.table;
        if table.kind != TableKind::Plain
            || !table.virtual_columns.is_empty()
            || !table.virtual_bindings.is_empty()
            || rows.is_empty()
            || rows.end > table.row_count()
            || shape.contains(&0)
            || shape[0]
                .checked_mul(shape[1])
                .and_then(|size| size.checked_mul(rows.len()))
                != Some(values.len())
        {
            return Ok(false);
        }
        let Some(schema) = table.schema().and_then(|schema| schema.column(column)) else {
            return Ok(false);
        };
        let shape_matches = match schema.column_type() {
            ColumnType::Array(ArrayShapeContract::Fixed { shape: fixed }) => fixed == &shape,
            ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
                ndim.is_none_or(|dim| dim == 2)
            }
            _ => false,
        };
        if schema.data_type() != Some(PrimitiveType::Complex32) || !shape_matches {
            return Ok(false);
        }
        let Some(path) = table.source_path.as_deref() else {
            return Ok(false);
        };
        if !holds_write_lock_for(table, path)
            || !table.inner.can_save_packed_array_rows(path, column)
        {
            return Ok(false);
        }
        // User and no-read lock modes can open without a read lock. Use the
        // descriptor and tile header read under the write lock for all offsets.
        let TableDatResult::Plain(mut table_dat) =
            read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))?
        else {
            return Ok(false);
        };
        let persisted_rows = table.require_persisted_rows(path, &table_dat)?;
        table_dat.raise_row_count(persisted_rows);
        let Some(update) = CompositeStorage.prepare_packed_array_update(
            path,
            &table_dat,
            column,
            rows,
            shape,
            CasacoreDataType::TpComplex,
        )?
        else {
            return Ok(false);
        };
        table.note_persisted_write(path);
        let result = update.write(values);
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(path);
        table.inner.discard_pending_cell_updates(&[column], &[]);
        result?;
        table.inner.refresh_read_metadata(table_dat)?;
        Ok(true)
    }

    /// Replace consecutive rank-2 cells from Fortran-order arrays, with one
    /// cell per element of the last axis. Supports native-width Bool, Float32
    /// and Complex32 cells in single-column TiledShapeStMan storage.
    ///
    /// Each column must cover complete tile bands, optionally including the
    /// padded last band of its cube. Returns `false` without writing for any
    /// unsupported layout. The caller must hold the physical table write lock.
    /// An I/O error can follow a partial write, as for other in-place saves.
    pub fn try_save_packed_array_rows_holding(
        &mut self,
        rows: Range<usize>,
        columns: &[(&str, &ArrayValue)],
        lock: &mut TableWriteLock,
    ) -> Result<bool, TableError> {
        let table = &mut *self.table;
        if table.kind != TableKind::Plain
            || !table.virtual_columns.is_empty()
            || !table.virtual_bindings.is_empty()
            || rows.is_empty()
            || rows.end > table.row_count()
            || columns.is_empty()
        {
            return Ok(false);
        }
        let Some(path) = table.source_path.as_deref() else {
            return Ok(false);
        };
        if !lock.matches_table_path(path) {
            return Err(TableError::NotLocked {
                operation: "save packed table rows".into(),
            });
        }
        let TableDatResult::Plain(mut table_dat) =
            read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))?
        else {
            return Ok(false);
        };
        let persisted_rows = table.require_persisted_rows(path, &table_dat)?;
        table_dat.raise_row_count(persisted_rows);
        let mut updates = Vec::with_capacity(columns.len());
        let mut names = HashSet::with_capacity(columns.len());
        for &(name, array) in columns {
            let &[axis0, axis1, count] = array.shape() else {
                return Ok(false);
            };
            let shape = [axis0, axis1];
            let Some(values) = PackedValues::new(array) else {
                return Ok(false);
            };
            let Some(schema) = table.schema().and_then(|schema| schema.column(name)) else {
                return Ok(false);
            };
            let shape_matches = match schema.column_type() {
                ColumnType::Array(ArrayShapeContract::Fixed { shape: fixed }) => fixed == &shape,
                ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
                    ndim.is_none_or(|dim| dim == 2)
                }
                _ => false,
            };
            let (element, storage) = values.data_type();
            if count != rows.len()
                || shape.contains(&0)
                || !shape_matches
                || schema.data_type() != Some(element)
                || !names.insert(name)
                || !table.inner.can_save_packed_array_rows(path, name)
            {
                return Ok(false);
            }
            let Some(update) = CompositeStorage.prepare_packed_array_update(
                path,
                &table_dat,
                name,
                rows.clone(),
                shape,
                storage,
            )?
            else {
                return Ok(false);
            };
            updates.push((update, values));
        }
        lock.record_write();
        let result = updates
            .into_iter()
            .try_for_each(|(update, values)| values.write(update));
        crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(path);
        result?;
        Ok(true)
    }
}

enum PackedValues<'a> {
    Bool(&'a [bool]),
    Float32(&'a [f32]),
    Complex32(&'a [Complex32]),
}

impl<'a> PackedValues<'a> {
    fn new(array: &'a ArrayValue) -> Option<Self> {
        match array {
            ArrayValue::Bool(values) if values.view().reversed_axes().is_standard_layout() => {
                values.as_slice_memory_order().map(Self::Bool)
            }
            ArrayValue::Float32(values) if values.view().reversed_axes().is_standard_layout() => {
                values.as_slice_memory_order().map(Self::Float32)
            }
            ArrayValue::Complex32(values) if values.view().reversed_axes().is_standard_layout() => {
                values.as_slice_memory_order().map(Self::Complex32)
            }
            _ => None,
        }
    }

    fn data_type(&self) -> (PrimitiveType, CasacoreDataType) {
        match self {
            Self::Bool(_) => (PrimitiveType::Bool, CasacoreDataType::TpBool),
            Self::Float32(_) => (PrimitiveType::Float32, CasacoreDataType::TpFloat),
            Self::Complex32(_) => (PrimitiveType::Complex32, CasacoreDataType::TpComplex),
        }
    }

    fn write(self, update: PackedArrayUpdate) -> Result<(), crate::storage::StorageError> {
        match self {
            Self::Bool(values) => update.write(values),
            Self::Float32(values) => update.write(values),
            Self::Complex32(values) => update.write(values),
        }
    }
}

#[cfg(unix)]
fn holds_write_lock_for(table: &Table, path: &Path) -> bool {
    table.lock_state.as_ref().is_some_and(|lock| {
        lock.lock_file.has_lock(LockType::Write) && super::locking::same_directory(&lock.path, path)
    })
}

#[cfg(not(unix))]
fn holds_write_lock_for(_table: &Table, _path: &Path) -> bool {
    false
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    fn zero_table() -> Table {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            PrimitiveType::Complex32,
            vec![4, 6],
        )])
        .unwrap();
        let mut original = Table::with_schema(schema);
        for _ in 0..6 {
            let data = ArrayValue::Complex32(ArrayD::zeros(IxDyn(&[4, 6])));
            original
                .add_row(RecordValue::new(vec![RecordField::new(
                    "DATA",
                    Value::Array(data),
                )]))
                .unwrap();
        }
        original
    }

    fn tiled_options(path: &Path, endian: EndianFormat) -> TableOptions {
        TableOptions::new(path)
            .with_data_manager(DataManagerKind::TiledShapeStMan)
            .with_tile_shape(vec![2, 3, 2])
            .with_endian_format(endian)
    }

    #[test]
    fn packed_write_uses_the_selected_tiles_in_both_byte_orders() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("packed_write.table");
        let original = zero_table();
        let values: Vec<_> = (0..48)
            .map(|index| Complex32::new(index as f32, -0.5 * index as f32))
            .collect();
        for endian in [EndianFormat::LittleEndian, EndianFormat::BigEndian] {
            original.save(tiled_options(&path, endian)).unwrap();
            let mut table = Table::open_with_lock(
                TableOptions::new(&path),
                LockOptions::new(crate::lock::LockMode::AutoLocking),
            )
            .unwrap();
            assert!(table.lock(LockType::Write, 1).unwrap());
            let mut write = table.prepare_write();
            assert!(
                !write
                    .try_save_packed_complex_rows("DATA", 1..3, [4, 6], &values)
                    .unwrap()
            );
            assert!(
                write
                    .try_save_packed_complex_rows("DATA", 2..4, [4, 6], &values)
                    .unwrap()
            );
            table.unlock().unwrap();

            let reopened = Table::open(TableOptions::new(&path)).unwrap();
            let column = reopened.column_accessor("DATA").unwrap();
            let Some(ArrayValue::Complex32(output)) = column
                .packed_2d_cells_uncached(&[0, 1, 2, 3, 4, 5])
                .unwrap()
            else {
                panic!("expected packed tiled read");
            };
            for ([corr, chan, row], cell) in output
                .indexed_iter()
                .map(|(index, cell)| ([index[0], index[1], index[2]], cell))
            {
                let expected = if (2..4).contains(&row) {
                    values[(row - 2) * 24 + chan * 4 + corr]
                } else {
                    Complex32::default()
                };
                assert_eq!(*cell, expected);
            }
        }
    }

    #[test]
    fn packed_write_uses_geometry_read_under_the_write_lock() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("retiled.table");
        let before = tiled_options(&path, EndianFormat::LittleEndian);
        let after = tiled_options(&path, EndianFormat::BigEndian).with_tile_shape(vec![4, 1, 2]);
        zero_table().save(before.clone()).unwrap();

        let user = LockOptions::new(crate::lock::LockMode::UserLocking);
        let mut table = Table::open_with_lock_using(before.clone(), user.clone(), |options| {
            let old = Table::open(options.clone())?;
            let mut other = Table::open_with_lock(options, user)?;
            assert!(other.lock(LockType::Write, 1)?);
            other.save(after)?;
            other.unlock()?;
            Ok(old)
        })
        .unwrap();
        assert!(table.lock(LockType::Write, 1).unwrap());
        let values: Vec<_> = (0..48)
            .map(|index| Complex32::new(index as f32, 7.0))
            .collect();
        assert!(
            table
                .prepare_write()
                .try_save_packed_complex_rows("DATA", 2..4, [4, 6], &values)
                .unwrap()
        );
        for handle in [&table, &Table::open(before).unwrap()] {
            let Some(ArrayValue::Complex32(array)) = handle
                .column_accessor("DATA")
                .unwrap()
                .packed_2d_cells_uncached(&[2, 3])
                .unwrap()
            else {
                panic!("expected tiled data");
            };
            assert_eq!(array.as_slice_memory_order().unwrap(), values);
        }
        table.unlock().unwrap();
    }

    #[test]
    fn packed_bool_tail_under_external_lock_preserves_padding() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("flags.table");
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "FLAG",
            PrimitiveType::Bool,
            vec![1, 3],
        )])
        .unwrap();
        let mut original = Table::with_schema(schema);
        for _ in 0..5 {
            original
                .add_row(RecordValue::new(vec![RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[1, 3]), false))),
                )]))
                .unwrap();
        }
        let options =
            tiled_options(&path, EndianFormat::LittleEndian).with_tile_shape(vec![1, 1, 3]);
        original.save(options.clone()).unwrap();
        let payload = std::fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|entry| entry.file_name().to_string_lossy().contains("_TSM"))
            .unwrap()
            .path();
        let before = [0xf8, 0xf8, 0xf8, 0xfc, 0xfc, 0xfc];
        assert_eq!(
            std::fs::metadata(&payload).unwrap().len(),
            before.len() as u64
        );
        std::fs::write(&payload, before).unwrap();
        let mut lock = TableWriteLock::acquire_to_reread(&path, 0).unwrap();
        let mut table = Table::open(options).unwrap();
        let values = ArrayValue::Bool(
            ArrayD::from_shape_vec(
                IxDyn(&[1, 3, 2]).f(),
                vec![true, false, true, false, true, true],
            )
            .unwrap(),
        );
        assert!(
            table
                .prepare_write()
                .try_save_packed_array_rows_holding(3..5, &[("FLAG", &values)], &mut lock)
                .unwrap()
        );
        lock.release().unwrap();
        assert_eq!(
            std::fs::read(payload).unwrap(),
            [0xf8, 0xf8, 0xf8, 0xfd, 0xfe, 0xff],
        );
        let actual = table
            .column_accessor("FLAG")
            .unwrap()
            .packed_2d_cells_uncached(&[3, 4])
            .unwrap();
        assert_eq!(actual, Some(values));
    }

    #[test]
    fn packed_write_refuses_a_table_with_another_tables_lock() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("a.table");
        let other = scratch.path().join("b.table");
        let options = tiled_options(&path, EndianFormat::LocalEndian);
        let other_options = tiled_options(&other, EndianFormat::LocalEndian);
        let original = zero_table();
        original.save(options.clone()).unwrap();
        original.save(other_options).unwrap();
        let data = |path: &Path| {
            let payload = std::fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap())
                .find(|entry| entry.file_name().to_string_lossy().contains("_TSM"))
                .unwrap();
            std::fs::read(payload.path()).unwrap()
        };
        let before = [data(&path), data(&other)];
        let mut table = Table::open_with_lock(
            options,
            LockOptions::new(crate::lock::LockMode::UserLocking),
        )
        .unwrap();
        assert!(table.lock(LockType::Write, 1).unwrap());
        table.set_path(&other);
        table.resync().unwrap();
        assert!(
            !table
                .prepare_write()
                .try_save_packed_complex_rows("DATA", 2..4, [4, 6], &[Complex32::new(3.0, 4.0); 48])
                .unwrap()
        );
        assert_eq!([data(&path), data(&other)], before);
        table.set_path(&path);
        table.unlock().unwrap();
    }
}
