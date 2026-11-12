// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded header-only array inspection through a column capability.
use super::*;

impl TableColumn<'_> {
    /// Returns whether an array cell is defined without reading its payload or
    /// populating the table-level row cache.
    ///
    /// This metadata-only probe requires a lazy, disk-backed
    /// `TiledShapeStMan` column.
    pub fn array_cell_is_defined_uncached(&self, row_index: usize) -> Result<bool, TableError> {
        if row_index >= self.table.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.table.row_count(),
            });
        }
        self.table
            .inner
            .array_cell_is_defined_uncached(row_index, &self.column)
    }

    /// Returns the selected cells' shapes without reading array payloads.
    ///
    /// The result preserves row order and duplicates, and has `None` for
    /// undefined cells. This metadata-only operation requires an unmodified,
    /// lazy disk-backed column in a supported storage manager. It never
    /// falls back to materializing numerical cells or the whole table.
    ///
    /// # Errors
    /// A row is out of bounds, the column is not an array, its manager
    /// lacks metadata-only shapes, or the table has pending cell/row edits.
    ///
    /// # C++ equivalent
    /// `TableColumn::shape(row)` / `ArrayColumn::shape(row)`, using each
    /// manager's shape headers rather than its array-data getter.
    pub fn array_shapes_uncached(
        &self,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, TableError> {
        for &row in rows {
            self.table.require_row(row)?;
        }
        self.table
            .inner
            .array_cell_shapes_uncached(&self.column, rows)
    }
}
