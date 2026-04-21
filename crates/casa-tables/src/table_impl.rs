// SPDX-License-Identifier: LGPL-3.0-or-later
use std::cell::OnceCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::schema::{ColumnType, TableSchema};
use crate::storage::{CompositeStorage, StorageProfiler};
use crate::table::TableError;

#[derive(Debug, Clone)]
struct LazyRowsSource {
    path: PathBuf,
    row_count_hint: usize,
}

#[derive(Debug)]
struct LoadedRows {
    rows: Vec<RecordValue>,
    undefined_cells: Vec<HashSet<String>>,
}

#[derive(Debug)]
struct LoadedScalarColumns {
    columns: HashMap<String, Vec<Option<ScalarValue>>>,
}

#[derive(Debug, Default)]
struct PendingScalarCells {
    by_column: HashMap<String, HashMap<usize, ScalarValue>>,
}

#[derive(Debug)]
struct PendingArrayColumn {
    rows: Vec<(usize, ArrayValue)>,
    sorted: bool,
}

impl Default for PendingArrayColumn {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            sorted: true,
        }
    }
}

impl PendingArrayColumn {
    fn insert(&mut self, row_index: usize, value: ArrayValue) {
        if let Some((last_row_index, last_value)) = self.rows.last_mut()
            && *last_row_index == row_index
        {
            *last_value = value;
            return;
        }

        let keeps_sorted = self
            .rows
            .last()
            .is_none_or(|(last_row_index, _)| *last_row_index <= row_index);
        self.sorted &= keeps_sorted;
        self.rows.push((row_index, value));
    }

    fn get(&self, row_index: usize) -> Option<&ArrayValue> {
        if self.sorted {
            let found = self
                .rows
                .partition_point(|(candidate, _)| *candidate <= row_index);
            if found == 0 {
                return None;
            }
            let (candidate, value) = &self.rows[found - 1];
            return (*candidate == row_index).then_some(value);
        }

        self.rows
            .iter()
            .rev()
            .find(|(candidate, _)| *candidate == row_index)
            .map(|(_, value)| value)
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }
}

#[derive(Debug, Default)]
struct PendingArrayCells {
    by_column: HashMap<String, PendingArrayColumn>,
}

pub(crate) enum LazyScalarLookup<'a> {
    Hit(&'a ScalarValue),
    Missing,
    Unknown,
}

pub(crate) enum LazyArrayLookup<'a> {
    Hit(&'a ArrayValue),
    Missing,
    Unknown,
}

fn lazy_array_column_store(
    schema: Option<&TableSchema>,
) -> HashMap<String, OnceCell<Vec<Option<ArrayValue>>>> {
    schema
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| matches!(column.column_type(), ColumnType::Array(_)))
        .map(|column| (column.name().to_string(), OnceCell::new()))
        .collect()
}

fn eager_loaded_rows(
    rows: Vec<RecordValue>,
    mut undefined_cells: Vec<HashSet<String>>,
) -> LoadedRows {
    if undefined_cells.len() != rows.len() {
        undefined_cells = (0..rows.len()).map(|_| HashSet::new()).collect();
    }
    LoadedRows {
        rows,
        undefined_cells,
    }
}

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    loaded_rows: OnceCell<LoadedRows>,
    loaded_scalar_columns: OnceCell<LoadedScalarColumns>,
    pending_scalar_cells: PendingScalarCells,
    loaded_array_columns: HashMap<String, OnceCell<Vec<Option<ArrayValue>>>>,
    pending_array_cells: PendingArrayCells,
    lazy_rows: Option<LazyRowsSource>,
    persisted_row_count: usize,
    keywords: RecordValue,
    column_keywords: HashMap<String, RecordValue>,
    schema: Option<TableSchema>,
}

impl TableImpl {
    pub(crate) fn new() -> Self {
        let loaded_rows = OnceCell::new();
        loaded_rows
            .set(LoadedRows {
                rows: Vec::new(),
                undefined_cells: Vec::new(),
            })
            .expect("initialize empty row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: OnceCell::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: HashMap::new(),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count: 0,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
        }
    }

    pub(crate) fn from_rows(rows: Vec<RecordValue>) -> Self {
        let persisted_row_count = rows.len();
        let loaded_rows = OnceCell::new();
        loaded_rows
            .set(eager_loaded_rows(rows, Vec::new()))
            .expect("initialize eager row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: OnceCell::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: HashMap::new(),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
        }
    }

    pub(crate) fn with_rows_keywords_and_schema(
        rows: Vec<RecordValue>,
        undefined_cells: Vec<HashSet<String>>,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
    ) -> Self {
        let persisted_row_count = rows.len();
        let loaded_rows = OnceCell::new();
        loaded_rows
            .set(eager_loaded_rows(rows, undefined_cells))
            .expect("initialize eager row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: OnceCell::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: lazy_array_column_store(schema.as_ref()),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count,
            keywords,
            column_keywords,
            schema,
        }
    }

    pub(crate) fn with_lazy_rows_keywords_and_schema(
        row_count: usize,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
        path: PathBuf,
    ) -> Self {
        Self {
            loaded_rows: OnceCell::new(),
            loaded_scalar_columns: OnceCell::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: lazy_array_column_store(schema.as_ref()),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: Some(LazyRowsSource {
                path,
                row_count_hint: row_count,
            }),
            persisted_row_count: row_count,
            keywords,
            column_keywords,
            schema,
        }
    }

    fn load_rows_now(source: &LazyRowsSource) -> Result<LoadedRows, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_rows_now path={}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let snapshot = storage
            .load_with_row_hint(&source.path, Some(source.row_count_hint as u64))
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

    fn load_scalar_columns_now(source: &LazyRowsSource) -> Result<LoadedScalarColumns, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_scalar_columns_now path={}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let snapshot = storage
            .load_scalar_columns_with_row_hint(&source.path, Some(source.row_count_hint as u64))
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load scalar columns for table {}: {err}",
                    source.path.display()
                ))
            })?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "storage_load_complete",
                Some(format!(
                    "row_count_hint={} columns={}",
                    source.row_count_hint,
                    snapshot.columns.len()
                )),
            );
        }
        Ok(LoadedScalarColumns {
            columns: snapshot.columns,
        })
    }

    fn load_array_column_now(
        source: &LazyRowsSource,
        column: &str,
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_array_column_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let values = storage
            .load_array_column_with_row_hint(
                &source.path,
                column,
                Some(source.row_count_hint as u64),
            )
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to load array column '{column}' for table {}: {err}",
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

    fn load_array_column_rows_now(
        source: &LazyRowsSource,
        column: &str,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        let mut profiler = StorageProfiler::start(format!(
            "table_impl::load_array_column_rows_now path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let values = storage
            .load_array_column_rows_with_row_hint(
                &source.path,
                column,
                row_indices,
                Some(source.row_count_hint as u64),
            )
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

    fn ensure_loaded(&self) -> Result<&LoadedRows, TableError> {
        if let Some(loaded) = self.loaded_rows.get() {
            return Ok(loaded);
        }
        let loaded = match &self.lazy_rows {
            Some(source) => Self::load_rows_now(source)?,
            None => LoadedRows {
                rows: Vec::new(),
                undefined_cells: Vec::new(),
            },
        };
        self.loaded_rows
            .set(loaded)
            .expect("initialize immutable row store");
        Ok(self
            .loaded_rows
            .get()
            .expect("row store initialized before shared access"))
    }

    fn ensure_loaded_mut(&mut self) -> Result<&mut LoadedRows, TableError> {
        if self.loaded_rows.get().is_none() {
            let loaded = match &self.lazy_rows {
                Some(source) => Self::load_rows_now(source)?,
                None => LoadedRows {
                    rows: Vec::new(),
                    undefined_cells: Vec::new(),
                },
            };
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

    pub(crate) fn add_row(&mut self, row: RecordValue) -> Result<(), TableError> {
        let loaded = self.ensure_loaded_mut()?;
        loaded.rows.push(row);
        loaded.undefined_cells.push(HashSet::new());
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        Ok(())
    }

    pub(crate) fn rows(&self) -> Result<&[RecordValue], TableError> {
        Ok(self.ensure_loaded()?.rows.as_slice())
    }

    pub(crate) fn undefined_cells(&self) -> Result<&[HashSet<String>], TableError> {
        Ok(self.ensure_loaded()?.undefined_cells.as_slice())
    }

    pub(crate) fn undefined_cells_mut(&mut self) -> Result<&mut [HashSet<String>], TableError> {
        Ok(self.ensure_loaded_mut()?.undefined_cells.as_mut_slice())
    }

    pub(crate) fn row_count(&self) -> usize {
        self.loaded_rows
            .get()
            .map_or(self.persisted_row_count, |loaded| loaded.rows.len())
    }

    pub(crate) fn row(&self, row_index: usize) -> Result<Option<&RecordValue>, TableError> {
        Ok(self.ensure_loaded()?.rows.get(row_index))
    }

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

        if self.loaded_scalar_columns.get().is_none() {
            let loaded = Self::load_scalar_columns_now(source)?;
            let _ = self.loaded_scalar_columns.set(loaded);
        }
        let columns = self
            .loaded_scalar_columns
            .get()
            .expect("scalar columns initialized before access");
        let Some(values) = columns.columns.get(column) else {
            return Ok(LazyScalarLookup::Unknown);
        };
        match values.get(row_index) {
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

        if self.loaded_scalar_columns.get().is_none() {
            let loaded = Self::load_scalar_columns_now(source)?;
            let _ = self.loaded_scalar_columns.set(loaded);
        }

        Ok(self
            .loaded_scalar_columns
            .get()
            .and_then(|columns| columns.columns.get(column).map(|values| values.as_slice())))
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

        if let Some(columns) = self.loaded_scalar_columns.get()
            && let Some(values) = columns.columns.get(column)
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

        let mut profiler = StorageProfiler::start(format!(
            "table_impl::scalar_cells_owned path={} column={column}",
            source.path.display()
        ));
        let storage = CompositeStorage;
        let mut values = storage
            .load_scalar_column_with_row_hint(
                &source.path,
                column,
                Some(source.row_count_hint as u64),
            )
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
        if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
            for (&row_index, value) in overrides {
                if let Some(cell) = values.get_mut(row_index) {
                    *cell = Some(value.clone());
                }
            }
        }
        Ok(Some(values))
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
        if cached_column.get().is_none() {
            let loaded = Self::load_array_column_now(source, column)?;
            let _ = cached_column.set(loaded);
        }
        let values = cached_column
            .get()
            .expect("array column initialized before access");
        match values.get(row_index) {
            Some(Some(value)) => Ok(LazyArrayLookup::Hit(value)),
            Some(None) | None => Ok(LazyArrayLookup::Missing),
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

    pub(crate) fn row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<Option<&mut RecordValue>, TableError> {
        Ok(self.ensure_loaded_mut()?.rows.get_mut(row_index))
    }

    pub(crate) fn rows_mut(&mut self) -> Result<&mut [RecordValue], TableError> {
        Ok(self.ensure_loaded_mut()?.rows.as_mut_slice())
    }

    pub(crate) fn set_cached_scalar_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: ScalarValue,
    ) -> Result<Option<ScalarValue>, TableError> {
        if let Some(loaded_rows) = self.loaded_rows.get_mut() {
            let Some(row) = loaded_rows.rows.get_mut(row_index) else {
                return Ok(Some(value));
            };
            row.upsert(column.to_string(), Value::Scalar(value));
            return Ok(None);
        }

        if row_index >= self.row_count() {
            return Ok(Some(value));
        }

        if let Some(loaded) = self.loaded_scalar_columns.get_mut()
            && let Some(column_values) = loaded.columns.get_mut(column)
        {
            if let Some(cell) = column_values.get_mut(row_index) {
                *cell = Some(value);
                return Ok(None);
            }
            return Ok(Some(value));
        }

        self.pending_scalar_cells
            .by_column
            .entry(column.to_string())
            .or_default()
            .insert(row_index, value);
        Ok(None)
    }

    pub(crate) fn set_cached_array_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: ArrayValue,
    ) -> Result<Option<ArrayValue>, TableError> {
        if let Some(loaded_rows) = self.loaded_rows.get_mut() {
            let Some(row) = loaded_rows.rows.get_mut(row_index) else {
                return Ok(Some(value));
            };
            row.upsert(column.to_string(), Value::Array(value));
            return Ok(None);
        }

        if row_index >= self.row_count() {
            return Ok(Some(value));
        }

        if let Some(cached_column) = self.loaded_array_columns.get_mut(column) {
            if cached_column.get().is_some() {
                let column_values = cached_column
                    .get_mut()
                    .expect("array column initialized before mutable access");
                if let Some(cell) = column_values.get_mut(row_index) {
                    *cell = Some(value);
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }
        self.pending_array_cells
            .by_column
            .entry(column.to_string())
            .or_default()
            .insert(row_index, value);
        Ok(None)
    }

    pub(crate) fn reserve_pending_array_cells(&mut self, column: &str, additional: usize) {
        self.pending_array_cells
            .by_column
            .entry(column.to_string())
            .or_default()
            .reserve(additional);
    }

    pub(crate) fn pending_scalar_cell_values(
        &self,
        column: &str,
    ) -> Option<Vec<(usize, ScalarValue)>> {
        self.pending_scalar_cells.by_column.get(column).map(|rows| {
            rows.iter()
                .map(|(&row_index, value)| (row_index, value.clone()))
                .collect()
        })
    }

    pub(crate) fn undefined_for_row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<Option<&mut HashSet<String>>, TableError> {
        Ok(self.ensure_loaded_mut()?.undefined_cells.get_mut(row_index))
    }

    pub(crate) fn remove_row(&mut self, index: usize) -> Result<RecordValue, TableError> {
        let loaded = self.ensure_loaded_mut()?;
        loaded.undefined_cells.remove(index);
        let removed = loaded.rows.remove(index);
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        Ok(removed)
    }

    pub(crate) fn insert_row(&mut self, index: usize, row: RecordValue) -> Result<(), TableError> {
        let loaded = self.ensure_loaded_mut()?;
        loaded.rows.insert(index, row);
        loaded.undefined_cells.insert(index, HashSet::new());
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        Ok(())
    }

    pub(crate) fn keywords(&self) -> &RecordValue {
        &self.keywords
    }

    pub(crate) fn keywords_mut(&mut self) -> &mut RecordValue {
        &mut self.keywords
    }

    pub(crate) fn column_keywords(&self, column: &str) -> Option<&RecordValue> {
        self.column_keywords.get(column)
    }

    pub(crate) fn set_column_keywords(&mut self, column: String, keywords: RecordValue) {
        self.column_keywords.insert(column, keywords);
    }

    pub(crate) fn all_column_keywords(&self) -> &HashMap<String, RecordValue> {
        &self.column_keywords
    }

    pub(crate) fn remove_column_keywords(&mut self, column: &str) -> Option<RecordValue> {
        self.column_keywords.remove(column)
    }

    pub(crate) fn rename_column_keywords(&mut self, old: &str, new: String) {
        if let Some(kw) = self.column_keywords.remove(old) {
            self.column_keywords.insert(new, kw);
        }
    }

    pub(crate) fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    pub(crate) fn set_schema(&mut self, schema: Option<TableSchema>) {
        self.schema = schema;
    }

    /// Replace all inner state from a storage snapshot.
    ///
    /// Used by `Table::lock()` when sync data indicates another process
    /// modified the table and a full reload is needed.
    pub(crate) fn replace_from_snapshot(
        &mut self,
        rows: Vec<RecordValue>,
        undefined_cells: Vec<HashSet<String>>,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
    ) {
        self.loaded_rows = {
            let loaded_rows = OnceCell::new();
            loaded_rows
                .set(eager_loaded_rows(rows, undefined_cells))
                .expect("replace eager row store");
            loaded_rows
        };
        self.loaded_scalar_columns = OnceCell::new();
        self.pending_scalar_cells = PendingScalarCells::default();
        self.loaded_array_columns = lazy_array_column_store(schema.as_ref());
        self.pending_array_cells = PendingArrayCells::default();
        self.persisted_row_count = self.loaded_rows.get().map_or(0, |loaded| loaded.rows.len());
        self.lazy_rows = None;
        self.keywords = keywords;
        self.column_keywords = column_keywords;
        self.schema = schema;
    }

    #[cfg(test)]
    pub(crate) fn has_loaded_rows(&self) -> bool {
        self.loaded_rows.get().is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_loaded_array_column(&self, column: &str) -> bool {
        self.loaded_array_columns
            .get(column)
            .is_some_and(|cached| cached.get().is_some())
    }

    #[cfg(test)]
    pub(crate) fn has_pending_array_cells(&self, column: &str) -> bool {
        self.pending_array_cells
            .by_column
            .get(column)
            .is_some_and(|cells| !cells.rows.is_empty())
    }

    pub(crate) fn has_pending_scalar_cells(&self, column: &str) -> bool {
        self.pending_scalar_cells
            .by_column
            .get(column)
            .is_some_and(|cells| !cells.is_empty())
    }
}
