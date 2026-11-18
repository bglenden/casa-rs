// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::Range;

use super::*;
use crate::storage::TABLE_CONTROL_FILE;
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};

impl TableWritePlan<'_> {
    /// Replace consecutive rank-2 Complex32 cells using native-width values
    /// in Fortran order, with cells back to back. The caller holds the
    /// table's write lock.
    ///
    /// Returns `false` without writing if this is not a complete tile
    /// replacement in a single-column TiledShapeStMan cube. The caller can
    /// then use the ordinary cell write path. An error can follow a partial
    /// write, as with other in-place saves.
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
            || !table.inner.can_save_packed_complex_rows(path, column)
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
        let Some(update) = CompositeStorage
            .prepare_packed_complex_update(path, &table_dat, column, rows, shape)?
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
    use ndarray::{ArrayD, IxDyn};

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
