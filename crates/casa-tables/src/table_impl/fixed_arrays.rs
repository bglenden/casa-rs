// SPDX-License-Identifier: LGPL-3.0-or-later
//! Packed whole-cell access without retaining per-cell ndarray values.

use super::*;

impl TableImpl {
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
