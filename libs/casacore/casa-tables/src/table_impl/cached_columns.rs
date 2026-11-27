// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl TableImpl {
    pub(crate) fn scalar_columns_owned(
        &self,
        columns: &[&str],
    ) -> Result<Option<ScalarColumnValueMap>, TableError> {
        if columns.is_empty() {
            return Ok(Some(HashMap::new()));
        }

        if let Some(loaded) = self.loaded_rows.get() {
            let mut values_by_column = std::collections::HashMap::with_capacity(columns.len());
            for &column in columns {
                let values = loaded
                    .rows
                    .iter()
                    .map(|row| match row.get(column) {
                        Some(Value::Scalar(scalar)) => Some(scalar.clone()),
                        _ => None,
                    })
                    .collect();
                values_by_column.insert(column.to_string(), values);
            }
            return Ok(Some(values_by_column));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_scalar_columns_now path={} columns={columns:?}",
            source.path.display()
        ));
        let requested = columns
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let snapshot = CompositeStorage
            .load_named_scalar_columns_with_row_hint(
                &source.path,
                &requested,
                Some(source.row_count_hint as u64),
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load scalar columns {:?} for table {}: {err}",
                    columns,
                    source.path.display()
                ))
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_columns={} loaded_columns={}",
                    source.row_count_hint,
                    columns.len(),
                    snapshot.columns.len(),
                )),
            );
        }
        let mut values_by_column = snapshot.columns;
        for &column in columns {
            let values = values_by_column
                .entry(column.to_string())
                .or_insert_with(|| vec![None; source.row_count_hint]);
            if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
                for (&row_index, value) in overrides {
                    if let Some(cell) = values.get_mut(row_index) {
                        *cell = Some(value.clone());
                    }
                }
            }
            if let Some(cached_column) = self.loaded_scalar_columns.get(column)
                && cached_column.get().is_none()
            {
                let _ = cached_column.set(values.clone());
            }
        }
        Ok(Some(values_by_column))
    }

    pub(crate) fn required_scalar_columns_owned(
        &self,
        columns: &[&str],
    ) -> Result<Option<RequiredScalarColumnValueMap>, TableError> {
        if columns.is_empty() {
            return Ok(Some(HashMap::new()));
        }

        if let Some(loaded) = self.loaded_rows.get() {
            let mut values_by_column = HashMap::with_capacity(columns.len());
            for &column in columns {
                let values = loaded
                    .rows
                    .iter()
                    .enumerate()
                    .map(|(row, row_value)| match row_value.get(column) {
                        Some(Value::Scalar(scalar)) => Ok(Some(scalar.clone())),
                        _ => Err(TableError::Storage(format!(
                            "required scalar column {column} row {row} is missing"
                        ))),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                values_by_column.insert(
                    column.to_string(),
                    required_scalar_column_data_from_optional_scalars(&values, column)?,
                );
            }
            return Ok(Some(values_by_column));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_required_scalar_columns_now path={} columns={columns:?}",
            source.path.display()
        ));
        let requested = columns
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let snapshot = CompositeStorage
            .load_named_required_scalar_columns_with_row_hint(
                &source.path,
                &requested,
                Some(source.row_count_hint as u64),
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load required scalar columns {:?} for table {}: {err}",
                    columns,
                    source.path.display()
                ))
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} requested_columns={} loaded_columns={}",
                    source.row_count_hint,
                    columns.len(),
                    snapshot.columns.len(),
                )),
            );
        }
        let mut values_by_column = snapshot.columns;
        for &column in columns {
            let Some(values) = values_by_column.get_mut(column) else {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} was not loaded"
                )));
            };
            if required_scalar_column_data_len(values) != source.row_count_hint {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} length {} does not match row count {}",
                    required_scalar_column_data_len(values),
                    source.row_count_hint
                )));
            }
            if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
                for (&row_index, value) in overrides {
                    set_required_scalar_column_value(values, row_index, value, column)?;
                }
            }
        }
        Ok(Some(values_by_column))
    }

    pub(crate) fn scalar_cells_owned_for_rows(
        &self,
        row_indices: &[usize],
        column: &str,
    ) -> Result<Option<Vec<Option<ScalarValue>>>, TableError> {
        self.selected_scalar_cells(row_indices, column, false)
    }

    pub(crate) fn retained_copy_descriptor(&self) -> Option<&TableDatContents> {
        Some(&self.lazy_rows.as_ref()?.read_metadata.get()?.table_dat)
    }

    pub(crate) fn selected_scalar_cells(
        &self,
        row_indices: &[usize],
        column: &str,
        preserve_ism_defaults: bool,
    ) -> Result<Option<Vec<Option<ScalarValue>>>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let values = row_indices
                .iter()
                .map(|&row_index| {
                    loaded
                        .rows
                        .get(row_index)
                        .and_then(|row| match row.get(column) {
                            Some(Value::Scalar(scalar)) => Some(scalar.clone()),
                            _ => None,
                        })
                })
                .collect();
            return Ok(Some(values));
        }

        if !preserve_ism_defaults
            && let Some(cached_column) = self.loaded_scalar_columns.get(column)
            && let Some(values) = cached_column.get()
        {
            let selected = row_indices
                .iter()
                .map(|&row_index| values.get(row_index).cloned().unwrap_or(None))
                .collect();
            return Ok(Some(selected));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let mut values =
            Self::load_scalar_column_rows_now(source, column, row_indices, preserve_ism_defaults)?;
        if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
            for (out_idx, &row_index) in row_indices.iter().enumerate() {
                if let Some(value) = overrides.get(&row_index)
                    && let Some(cell) = values.get_mut(out_idx)
                {
                    *cell = Some(value.clone());
                }
            }
        }
        Ok(Some(values))
    }

    pub(crate) fn required_scalar_columns_owned_for_rows(
        &self,
        row_indices: &[usize],
        columns: &[&str],
    ) -> Result<Option<RequiredScalarColumnValueMap>, TableError> {
        if self.loaded_rows.get().is_some() || self.lazy_rows.is_none() {
            let mut values_by_column = HashMap::with_capacity(columns.len());
            for &column in columns {
                let Some(values) = self.scalar_cells_owned_for_rows(row_indices, column)? else {
                    return Ok(None);
                };
                values_by_column.insert(
                    column.to_string(),
                    required_scalar_column_data_from_optional_scalars(&values, column)?,
                );
            }
            return Ok(Some(values_by_column));
        }

        let source = self
            .lazy_rows
            .as_ref()
            .expect("lazy source checked before selected scalar load");
        let Some(metadata) = source.try_read_plain_metadata()? else {
            // Let the logical column reader resolve view and concatenation
            // rows. The manager fast path below uses plain-table indices.
            return Ok(None);
        };
        let requested = columns.iter().copied().collect::<HashSet<_>>();
        let mut values_by_column = CompositeStorage
            .load_named_required_scalar_column_rows_from_plain(
                &source.path,
                &metadata.table_dat,
                &requested,
                row_indices,
                Some(source.row_count_hint as u64),
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load required selected scalar columns {columns:?} from table {}: {err}",
                    source.path.display()
                ))
            })?
            .columns;
        for &column in columns {
            let values = values_by_column.get_mut(column).ok_or_else(|| {
                TableError::Storage(format!(
                    "required selected scalar column {column} was not loaded"
                ))
            })?;
            if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
                for (out_idx, &row_index) in row_indices.iter().enumerate() {
                    if let Some(value) = overrides.get(&row_index) {
                        set_required_scalar_column_value(values, out_idx, value, column)?;
                    }
                }
            }
        }
        Ok(Some(values_by_column))
    }

    pub(crate) fn required_scalar_columns_for_rows_into(
        &self,
        row_indices: &[usize],
        destinations: &mut [RequiredScalarColumnDestination<'_>],
    ) -> Result<(), TableError> {
        if self.loaded_rows.get().is_some() || self.lazy_rows.is_none() {
            return Err(TableError::Storage(
                "required scalar reusable fills require a lazy disk-backed table".to_string(),
            ));
        }
        if destinations.iter().any(|destination| {
            self.pending_scalar_cells
                .by_column
                .contains_key(destination.column())
        }) {
            return Err(TableError::Storage(
                "required scalar reusable fills do not support pending scalar-cell overrides"
                    .to_string(),
            ));
        }
        // A prepared write can update an initialized scalar cache in place.
        // For edited handles let the caller use the cache-aware cell reader.
        if self.generation() != 0
            && destinations
                .iter()
                .any(|destination| self.has_loaded_scalar_column(destination.column()))
        {
            return Err(TableError::Storage(
                "required scalar reusable fills do not support cached columns on edited tables"
                    .to_string(),
            ));
        }
        let source = self
            .lazy_rows
            .as_ref()
            .expect("lazy source checked before reusable selected scalar load");
        CompositeStorage
            .fill_named_required_scalar_column_rows_from_plain(
                &source.path,
                source.plain_table_dat()?,
                row_indices,
                destinations,
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to fill required selected scalar columns from table {}: {err}",
                    source.path.display()
                ))
            })
    }

    pub(crate) fn array_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<LazyArrayLookup<'_>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let Some(row) = loaded.rows.get(row_index) else {
                return Ok(LazyArrayLookup::Missing);
            };
            return Ok(match row.get(column) {
                Some(Value::Array(array)) => LazyArrayLookup::Hit(array),
                Some(_) => LazyArrayLookup::Unknown,
                None => LazyArrayLookup::Missing,
            });
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(LazyArrayLookup::Unknown);
        };
        if let Some(overrides) = self.pending_array_cells.by_column.get(column)
            && let Some(value) = overrides.get(row_index)
        {
            return Ok(LazyArrayLookup::Hit(value));
        }
        let Some(cached_column) = self.loaded_array_columns.get(column) else {
            return Ok(LazyArrayLookup::Unknown);
        };
        if let Some(values) = cached_column.get() {
            return match values.get(row_index) {
                Some(Some(value)) => Ok(LazyArrayLookup::Hit(value)),
                Some(None) | None => Ok(LazyArrayLookup::Missing),
            };
        }

        let Some(buffered_column) = self.buffered_array_cells.get(column) else {
            return Ok(LazyArrayLookup::Unknown);
        };
        if buffered_column.get().is_none() {
            let _ = buffered_column.set(new_buffered_array_column(self.row_count()));
        }
        let buffered = buffered_column
            .get()
            .expect("buffered array column initialized before access");
        let Some(buffered_cell) = buffered.get(row_index) else {
            return Ok(LazyArrayLookup::Missing);
        };
        if buffered_cell.get().is_none() {
            let loaded = Self::load_array_column_rows_now(source, column, &[row_index])?;
            let value = loaded.into_iter().next().unwrap_or(None);
            let _ = buffered_cell.set(value);
        }
        match buffered_cell
            .get()
            .expect("buffered array cell initialized before access")
        {
            Some(value) => Ok(LazyArrayLookup::Hit(value)),
            None => Ok(LazyArrayLookup::Missing),
        }
    }

    pub(crate) fn array_cells_owned(
        &self,
        row_indices: &[usize],
        column: &str,
    ) -> Result<Option<Vec<Option<ArrayValue>>>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let values = row_indices
                .iter()
                .map(|&row_index| {
                    loaded
                        .rows
                        .get(row_index)
                        .and_then(|row| match row.get(column) {
                            Some(Value::Array(array)) => Some(array.clone()),
                            _ => None,
                        })
                })
                .collect();
            return Ok(Some(values));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };
        let mut values = if let Some(cached_column) = self.loaded_array_columns.get(column)
            && let Some(cached_values) = cached_column.get()
        {
            row_indices
                .iter()
                .map(|&row_index| cached_values.get(row_index).cloned().unwrap_or(None))
                .collect()
        } else if let Some(buffered_column) = self.buffered_array_cells.get(column) {
            if buffered_column.get().is_none() {
                let _ = buffered_column.set(new_buffered_array_column(self.row_count()));
            }
            let buffered = buffered_column
                .get()
                .expect("buffered array column initialized before selected-row access");
            let mut missing_rows = Vec::new();
            for &row_index in row_indices {
                let Some(buffered_cell) = buffered.get(row_index) else {
                    continue;
                };
                if buffered_cell.get().is_none() {
                    missing_rows.push(row_index);
                }
            }
            if !missing_rows.is_empty() {
                let loaded = Self::load_array_column_rows_now(source, column, &missing_rows)?;
                for (row_index, value) in missing_rows.into_iter().zip(loaded) {
                    if let Some(buffered_cell) = buffered.get(row_index) {
                        let _ = buffered_cell.set(value);
                    }
                }
            }
            row_indices
                .iter()
                .map(|&row_index| {
                    buffered
                        .get(row_index)
                        .and_then(|cell| cell.get())
                        .cloned()
                        .unwrap_or(None)
                })
                .collect()
        } else {
            Self::load_array_column_rows_now(source, column, row_indices)?
        };
        if let Some(overrides) = self.pending_array_cells.by_column.get(column) {
            for (out_idx, &row_index) in row_indices.iter().enumerate() {
                if let Some(value) = overrides.get(row_index) {
                    values[out_idx] = Some(value.clone());
                }
            }
        }

        Ok(Some(values))
    }

    pub(crate) fn array_cells_owned_uncached(
        &self,
        row_indices: &[usize],
        column: &str,
    ) -> Result<Option<Vec<Option<ArrayValue>>>, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            let values = row_indices
                .iter()
                .map(|&row_index| {
                    loaded
                        .rows
                        .get(row_index)
                        .and_then(|row| match row.get(column) {
                            Some(Value::Array(array)) => Some(array.clone()),
                            _ => None,
                        })
                })
                .collect();
            return Ok(Some(values));
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let mut values = Self::load_array_column_rows_now(source, column, row_indices)?;
        if let Some(overrides) = self.pending_array_cells.by_column.get(column) {
            for (out_idx, &row_index) in row_indices.iter().enumerate() {
                if let Some(value) = overrides.get(row_index) {
                    values[out_idx] = Some(value.clone());
                }
            }
        }

        Ok(Some(values))
    }

    pub(crate) fn array_cells_2d_channel_range_typed_uncached(
        &self,
        row_indices: &[usize],
        column: &str,
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        if self.loaded_rows.get().is_some() {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads require a lazy disk-backed table"
            )));
        }
        if self.pending_array_cells.by_column.contains_key(column) {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads do not support pending array-cell overrides"
            )));
        }

        let Some(source) = &self.lazy_rows else {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads require a lazy disk-backed table"
            )));
        };
        Self::load_array_column_rows_2d_channel_range_typed_now(
            source,
            column,
            row_indices,
            channel_start,
            channel_count,
        )
    }

    pub(crate) fn array_cells_2d_channel_range_typed_uncached_into(
        &self,
        row_indices: &[usize],
        column: &str,
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, TableError> {
        if self.loaded_rows.get().is_some() {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads require a lazy disk-backed table"
            )));
        }
        if self.pending_array_cells.by_column.contains_key(column) {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads do not support pending array-cell overrides"
            )));
        }
        let Some(source) = &self.lazy_rows else {
            return Err(TableError::Storage(format!(
                "{column} typed selected channel reads require a lazy disk-backed table"
            )));
        };
        Self::fill_array_column_rows_2d_channel_range_typed_now(
            source,
            column,
            row_indices,
            channel_start,
            channel_count,
            destination,
        )
    }

    pub(crate) fn array_cells_1d_typed_uncached(
        &self,
        row_indices: &[usize],
        column: &str,
    ) -> Result<SelectedArray1DCells, TableError> {
        if self.loaded_rows.get().is_some() {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads require a lazy disk-backed table"
            )));
        }
        if self.pending_array_cells.by_column.contains_key(column) {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads do not support pending array-cell overrides"
            )));
        }

        let Some(source) = &self.lazy_rows else {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads require a lazy disk-backed table"
            )));
        };
        Self::load_array_column_rows_1d_typed_now(source, column, row_indices)
    }

    pub(crate) fn array_cells_1d_typed_uncached_into(
        &self,
        row_indices: &[usize],
        column: &str,
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, TableError> {
        if self.loaded_rows.get().is_some() {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads require a lazy disk-backed table"
            )));
        }
        if self.pending_array_cells.by_column.contains_key(column) {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads do not support pending array-cell overrides"
            )));
        }
        let Some(source) = &self.lazy_rows else {
            return Err(TableError::Storage(format!(
                "{column} typed selected 1-D reads require a lazy disk-backed table"
            )));
        };
        Self::fill_array_column_rows_1d_typed_now(source, column, row_indices, destination)
    }
}
