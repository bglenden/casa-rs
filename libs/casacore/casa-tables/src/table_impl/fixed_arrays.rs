// SPDX-License-Identifier: LGPL-3.0-or-later
//! Packed whole-cell access without retaining per-cell ndarray values.

use std::path::Path;

use super::*;

impl TableImpl {
    pub(crate) fn packed_2d_cells_uncached(
        &self,
        column: &str,
        rows: &[usize],
        widen_complex: bool,
    ) -> Result<Option<ArrayValue>, TableError> {
        if self.loaded_rows.get().is_some()
            || self.rows_reshaped()
            || self.pending_array_cells.by_column.contains_key(column)
        {
            return Ok(None);
        }
        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };
        let Some(metadata) = source.read_metadata.get() else {
            return Ok(None);
        };
        CompositeStorage
            .load_uniform_2d_from_plain(&source.path, metadata, column, rows, widen_complex)
            .map_err(TableError::from)
    }

    pub(crate) fn can_save_packed_array_rows(&self, path: &Path, column: &str) -> bool {
        !self.rows_are_loaded()
            && !self.rows_reshaped()
            && !self.pending_array_cells.by_column.contains_key(column)
            // A loaded array column can contain edits outside the selection.
            && !self.loaded_array_columns.get(column).is_some_and(|cache| cache.get().is_some())
            && self.lazy_rows.as_ref().is_some_and(|source| source.path == path)
    }

    pub(crate) fn fixed_2d_cells_uncached(
        &self,
        column: &str,
        rows: &[usize],
        shape: [usize; 2],
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        if self.loaded_rows.get().is_some()
            || self.pending_array_cells.by_column.contains_key(column)
        {
            return Ok(None);
        }
        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };
        let metadata = source.read_metadata()?;
        CompositeStorage
            .load_fixed_2d_from_plain(&source.path, &metadata.table_dat, column, rows, shape)
            .map_err(|error| {
                TableError::Storage(format!(
                    "failed to read fixed-shape column {column}: {error}"
                ))
            })
    }
}
