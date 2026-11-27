// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded header-only array inspection through a column capability.
use std::borrow::Cow;

use super::*;

impl TableColumn<'_> {
    /// Upper bound on tiled I/O scratch for one uncached array-cell read,
    /// apart from the cell and its decoded output. Inspects storage headers
    /// only, including the physical column of a compressed complex array.
    /// Returns zero for in-memory columns and non-tiled managers.
    ///
    /// The bound includes decoded tiles, transient shared-tile copies and
    /// file read-ahead; it does not include table metadata or process caches.
    pub fn array_cell_read_buffer_bytes(&self) -> Result<usize, TableError> {
        let Some(path) = self.table.path() else {
            return Ok(0);
        };
        crate::storage::CompositeStorage
            .array_cell_read_buffer_bytes(path, &self.shape_column()?)
            .map_err(TableError::from)
    }

    /// Returns whether an array cell is defined without reading its payload or
    /// populating the table-level row cache.
    ///
    /// The metadata-only probe uses the backing column for compressed
    /// arrays and requires a supported lazy, disk-backed manager.
    pub fn array_cell_is_defined_uncached(&self, row_index: usize) -> Result<bool, TableError> {
        if row_index >= self.table.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.table.row_count(),
            });
        }
        self.table
            .inner
            .array_cell_is_defined_uncached(row_index, &self.shape_column()?)
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
            .array_cell_shapes_uncached(&self.shape_column()?, rows)
    }

    fn shape_column(&self) -> Result<Cow<'_, str>, TableError> {
        Ok(match self.table.complex_compression(&self.column)? {
            Some(compression) => Cow::Owned(compression.stored),
            None => Cow::Borrowed(&self.column),
        })
    }
}
