// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl TableImpl {
    pub(super) fn load_rows_now(source: &LazyRowsSource) -> Result<LoadedRows, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_rows_now path={}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let row_hint = Some(source.row_count_hint as u64);
        let snapshot = match source.retained_asdm_dat() {
            Some(table_dat) => {
                storage.load_plain_table_filtered(&source.path, table_dat, row_hint, None)
            }
            None => storage.load_with_row_hint(&source.path, row_hint),
        }
        .map_err(|err| {
            TableError::Storage(format!(
                "failed to materialize rows for table {}: {err}",
                source.path.display()
            ))
        })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} rows={}",
                    source.row_count_hint, snapshot.row_count
                )),
            );
        }
        let loaded = eager_loaded_rows(snapshot.rows, snapshot.undefined_cells);
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "eager_loaded_rows",
                Some(format!(
                    "rows={} undefined_rows={}",
                    loaded.rows.len(),
                    loaded.undefined_cells.len()
                )),
            );
        }
        Ok(loaded)
    }

    pub(super) fn load_scalar_column_now(
        source: &LazyRowsSource,
        column: &str,
    ) -> Result<Vec<Option<ScalarValue>>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_scalar_column_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let row_hint = Some(source.row_count_hint as u64);
        let values = match source.retained_asdm_dat() {
            Some(table_dat) => {
                storage.load_plain_scalar_column(&source.path, table_dat, column, row_hint)
            }
            None => storage.load_scalar_column_with_row_hint(&source.path, column, row_hint),
        }
        .map_err(|err| {
            TableError::Storage(format!(
                "failed to load scalar column '{column}' for table {}: {err}",
                source.path.display()
            ))
        })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} values={}",
                    source.row_count_hint,
                    values.len()
                )),
            );
        }
        Ok(values)
    }

    pub(super) fn load_array_column_rows_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_array_column_rows_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let values = match source.read_metadata.get() {
            Some(metadata) => storage.load_plain_array_column_rows(
                &source.path,
                &metadata.table_dat,
                column,
                row_indices,
                Some(source.row_count_hint as u64),
            ),
            None => storage.load_array_column_rows_with_row_hint(
                &source.path,
                column,
                row_indices,
                Some(source.row_count_hint as u64),
            ),
        }
        .map_err(|err| {
            TableError::Storage(format!(
                "failed to load selected rows for array column '{column}' from table {}: {err}",
                source.path.display()
            ))
        })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_rows={} values={}",
                    source.row_count_hint,
                    row_indices.len(),
                    values.len()
                )),
            );
        }
        Ok(values)
    }

    pub(super) fn load_array_column_rows_2d_channel_range_typed_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_array_column_rows_2d_channel_range_typed_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let read_metadata = source.read_metadata()?;
        let values = storage
            .load_array_column_rows_2d_channel_range_typed_from_plain(
                &source.path,
                &read_metadata.table_dat,
                &read_metadata.tiled,
                column,
                row_indices,
                channel_start,
                channel_count,
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load typed selected channel range for array column '{column}' from table {}: {err}",
                    source.path.display()
                ))
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_rows={} channel_start={} channel_count={} values={}",
                    source.row_count_hint,
                    row_indices.len(),
                    channel_start,
                    channel_count,
                    values.as_ref().map_or(0, SelectedArray2DCells::row_count)
                )),
            );
        }
        Ok(values)
    }

    pub(super) fn fill_array_column_rows_2d_channel_range_typed_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, TableError> {
        let storage = CompositeStorage;
        let read_metadata = source.read_metadata()?;
        storage
            .fill_array_column_rows_2d_channel_range_typed_from_plain(
                &source.path,
                &read_metadata.table_dat,
                &read_metadata.tiled,
                column,
                row_indices,
                channel_start,
                channel_count,
                destination,
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to fill typed selected channel range for array column '{column}' from table {}: {err}",
                    source.path.display()
                ))
            })
    }

    pub(super) fn load_array_column_rows_1d_typed_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
    ) -> Result<SelectedArray1DCells, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_array_column_rows_1d_typed_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let read_metadata = source.read_metadata()?;
        let values = storage
            .load_array_column_rows_1d_typed_from_plain(
                &source.path,
                &read_metadata.table_dat,
                &read_metadata.tiled,
                column,
                row_indices,
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load typed selected 1-D rows for array column '{column}' from table {}: {err}",
                    source.path.display()
                ))
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_rows={} values={}",
                    source.row_count_hint,
                    row_indices.len(),
                    values.row_count()
                )),
            );
        }
        Ok(values)
    }

    pub(super) fn fill_array_column_rows_1d_typed_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, TableError> {
        let storage = CompositeStorage;
        let read_metadata = source.read_metadata()?;
        storage
            .fill_array_column_rows_1d_typed_from_plain(
                &source.path,
                &read_metadata.table_dat,
                &read_metadata.tiled,
                column,
                row_indices,
                destination,
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to fill typed selected 1-D rows for array column '{column}' from table {}: {err}",
                    source.path.display()
                ))
            })
    }

    pub(super) fn load_scalar_column_rows_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
        preserve_ism_defaults: bool,
    ) -> Result<Vec<Option<ScalarValue>>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_scalar_column_rows_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let values = match source.read_metadata.get() {
            Some(metadata) => storage.load_plain_scalar_column_rows_in_layout(
                &source.path,
                &metadata.table_dat,
                column,
                row_indices,
                Some(source.row_count_hint as u64),
                preserve_ism_defaults,
            ),
            None => storage.load_scalar_column_rows_in_layout(
                &source.path,
                column,
                row_indices,
                Some(source.row_count_hint as u64),
                preserve_ism_defaults,
            ),
        }
        .map_err(|err| {
            TableError::Storage(format!(
                "failed to load selected rows for scalar column '{column}' from table {}: {err}",
                source.path.display()
            ))
        })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_rows={} values={}",
                    source.row_count_hint,
                    row_indices.len(),
                    values.len()
                )),
            );
        }
        Ok(values)
    }

    /// Lays over freshly loaded rows the cells written while the table was
    /// read lazily: those of the scalar and array columns read so far (a
    /// write updates a column read in place) and the sparse ones written to
    /// the rest. The disk has not these writes, so the rows loaded from it
    /// would lose them; and a cell written is defined, whatever the disk had.
    pub(super) fn overlay_lazy_writes(&self, loaded: &mut LoadedRows) {
        let mut defined = |column: &str, row_index: usize| {
            if let Some(undefined) = loaded.undefined_cells.get_mut(row_index) {
                undefined.remove(column);
            }
        };
        for (column, cached) in &self.loaded_scalar_columns {
            if let Some(values) = cached.get() {
                apply_prepared_scalar_column(&mut loaded.rows, column, values);
                for (row_index, value) in values.iter().enumerate() {
                    if value.is_some() {
                        defined(column, row_index);
                    }
                }
            }
        }
        for (column, cached) in &self.loaded_array_columns {
            if let Some(values) = cached.get() {
                apply_prepared_array_column(&mut loaded.rows, column, values);
                for (row_index, value) in values.iter().enumerate() {
                    if value.is_some() {
                        defined(column, row_index);
                    }
                }
            }
        }
        for (column, overrides) in &self.pending_scalar_cells.by_column {
            apply_prepared_scalar_overrides(&mut loaded.rows, column, overrides);
            for row_index in overrides.keys() {
                defined(column, *row_index);
            }
        }
        for (column, overrides) in &self.pending_array_cells.by_column {
            apply_prepared_array_overrides(&mut loaded.rows, column, overrides);
            for (row_index, _) in &overrides.rows {
                defined(column, *row_index);
            }
        }
    }

    pub(super) fn ensure_loaded(&self) -> Result<&LoadedRows, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            return Ok(loaded);
        }
        let mut loaded = match &self.lazy_rows {
            Some(source) => Self::load_rows_now(source)?,
            None => LoadedRows {
                rows: Vec::new(),
                undefined_cells: Vec::new(),
            },
        };
        self.overlay_lazy_writes(&mut loaded);
        self.loaded_rows
            .set(loaded)
            .expect("initialize immutable row store");
        Ok(self
            .loaded_rows
            .get()
            .expect("row store initialized before shared access"))
    }

    pub(super) fn ensure_loaded_mut(&mut self) -> Result<&mut LoadedRows, TableError> {
        if self.loaded_rows.get().is_none() {
            let mut loaded = match &self.lazy_rows {
                Some(source) => Self::load_rows_now(source)?,
                None => LoadedRows {
                    rows: Vec::new(),
                    undefined_cells: Vec::new(),
                },
            };
            self.overlay_lazy_writes(&mut loaded);
            self.loaded_rows
                .set(loaded)
                .expect("initialize mutable row store");
        }
        if !self.pending_array_cells.by_column.is_empty() {
            let pending = std::mem::take(&mut self.pending_array_cells.by_column);
            if let Some(loaded) = self.loaded_rows.get_mut() {
                for (column, rows) in pending {
                    for (row_index, value) in rows.rows {
                        if let Some(row) = loaded.rows.get_mut(row_index) {
                            row.upsert(column.clone(), Value::Array(value));
                        }
                    }
                }
            }
        }
        if !self.pending_scalar_cells.by_column.is_empty() {
            let pending = std::mem::take(&mut self.pending_scalar_cells.by_column);
            if let Some(loaded) = self.loaded_rows.get_mut() {
                for (column, rows) in pending {
                    for (row_index, value) in rows {
                        if let Some(row) = loaded.rows.get_mut(row_index) {
                            row.upsert(column.clone(), Value::Scalar(value));
                        }
                    }
                }
            }
        }
        Ok(self
            .loaded_rows
            .get_mut()
            .expect("row store initialized before mutable access"))
    }
}

impl TableImpl {
    pub(crate) fn scalar_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<LazyScalarLookup<'_>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let Some(row) = loaded.rows.get(row_index) else {
                return Ok(LazyScalarLookup::Missing);
            };
            return Ok(match row.get(column) {
                Some(Value::Scalar(scalar)) => LazyScalarLookup::Hit(scalar),
                Some(_) => LazyScalarLookup::Unknown,
                None => LazyScalarLookup::Missing,
            });
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(LazyScalarLookup::Unknown);
        };

        if let Some(overrides) = self.pending_scalar_cells.by_column.get(column)
            && let Some(value) = overrides.get(&row_index)
        {
            return Ok(LazyScalarLookup::Hit(value));
        }

        let Some(cached_column) = self.loaded_scalar_columns.get(column) else {
            return Ok(LazyScalarLookup::Unknown);
        };
        if let Some(values) = cached_column.get() {
            return match values.get(row_index) {
                Some(Some(value)) => Ok(LazyScalarLookup::Hit(value)),
                Some(None) | None => Ok(LazyScalarLookup::Missing),
            };
        }
        let loaded = Self::load_scalar_column_now(source, column)?;
        let _ = cached_column.set(loaded);
        match cached_column
            .get()
            .expect("scalar column initialized before shared access")
            .get(row_index)
        {
            Some(Some(value)) => Ok(LazyScalarLookup::Hit(value)),
            Some(None) | None => Ok(LazyScalarLookup::Missing),
        }
    }

    pub(crate) fn scalar_column_values(
        &self,
        column: &str,
    ) -> Result<Option<&[Option<ScalarValue>]>, TableError> {
        if self.loaded_rows.get().is_some() {
            return Ok(None);
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let Some(cached_column) = self.loaded_scalar_columns.get(column) else {
            return Ok(None);
        };
        if cached_column.get().is_none() {
            let loaded = Self::load_scalar_column_now(source, column)?;
            let _ = cached_column.set(loaded);
        }

        Ok(cached_column.get().map(|values| values.as_slice()))
    }

    pub(crate) fn scalar_cells_owned(
        &self,
        column: &str,
    ) -> Result<Option<Vec<Option<ScalarValue>>>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let values = loaded
                .rows
                .iter()
                .map(|row| match row.get(column) {
                    Some(Value::Scalar(scalar)) => Some(scalar.clone()),
                    _ => None,
                })
                .collect();
            return Ok(Some(values));
        }

        if let Some(cached_column) = self.loaded_scalar_columns.get(column)
            && let Some(values) = cached_column.get()
        {
            let mut values = values.clone();
            if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
                for (&row_index, value) in overrides {
                    if let Some(cell) = values.get_mut(row_index) {
                        *cell = Some(value.clone());
                    }
                }
            }
            return Ok(Some(values));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let mut values = Self::load_scalar_column_now(source, column)?;
        if let Some(cached_column) = self.loaded_scalar_columns.get(column)
            && cached_column.get().is_none()
        {
            let _ = cached_column.set(values.clone());
        }
        if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
            for (&row_index, value) in overrides {
                if let Some(cell) = values.get_mut(row_index) {
                    *cell = Some(value.clone());
                }
            }
        }
        Ok(Some(values))
    }
}
