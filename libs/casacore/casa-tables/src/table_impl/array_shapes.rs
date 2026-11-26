// SPDX-License-Identifier: LGPL-3.0-or-later
//! Metadata-only array inspections do not populate numerical row caches.
use super::*;

impl TableImpl {
    /// Use the retained shape map when validating writes to an existing
    /// stored array cell. A pending override or row edit owns the current shape.
    pub(crate) fn try_retained_array_shape(
        &self,
        row: usize,
        column: &str,
    ) -> Result<Option<Option<Vec<usize>>>, TableError> {
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
        let shapes = CompositeStorage.plain_array_cell_shapes_uncached(
            &source.path,
            metadata,
            column,
            &[row],
        )?;
        Ok(shapes.into_iter().next())
    }

    fn metadata_source(&self, column: &str) -> Result<&LazyRowsSource, TableError> {
        if self.loaded_rows.get().is_some() || self.lazy_rows.is_none() {
            return Err(TableError::Storage(format!(
                "{column} metadata-only inspection requires a lazy disk-backed table"
            )));
        }
        if self.pending_array_cells.by_column.contains_key(column) {
            return Err(TableError::Storage(format!(
                "{column} metadata-only inspection does not support pending array-cell overrides"
            )));
        }
        Ok(self
            .lazy_rows
            .as_ref()
            .expect("lazy source checked before metadata inspection"))
    }

    pub(crate) fn array_cell_is_defined_uncached(
        &self,
        row: usize,
        column: &str,
    ) -> Result<bool, TableError> {
        let source = self.metadata_source(column)?;
        let metadata = source.read_metadata()?;
        CompositeStorage
            .plain_array_cell_is_defined_uncached(&metadata.table_dat, &metadata.tiled, column, row)
            .map_err(|error| {
                TableError::Storage(format!(
                    "failed to inspect {column} cell {row} in table {}: {error}",
                    source.path.display()
                ))
            })
    }

    pub(crate) fn array_cell_shapes_uncached(
        &self,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, TableError> {
        let source = self.metadata_source(column)?;
        if self.rows_reshaped.load(Ordering::Relaxed) {
            return Err(TableError::Storage(
                "metadata-only shapes do not support changed row registration".to_string(),
            ));
        }
        if source.read_metadata.get().is_none()
            && let Some(shapes) =
                CompositeStorage.load_array_shapes_for_projection(&source.path, column, rows)?
        {
            return Ok(shapes);
        }
        let metadata = source.read_metadata()?;
        CompositeStorage
            .plain_array_cell_shapes_uncached(&source.path, metadata, column, rows)
            .map_err(|error| {
                TableError::Storage(format!(
                    "failed to inspect {column} shapes in table {}: {error}",
                    source.path.display()
                ))
            })
    }
}
