// SPDX-License-Identifier: LGPL-3.0-or-later
mod array_shapes;
mod dirty;
pub(crate) use dirty::DirtyChanges;
mod fixed_arrays;
mod read_cache;

use std::collections::HashMap;
use std::collections::HashSet;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::schema::{ColumnType, TableSchema};
use crate::storage::{
    CompositeStorage, RequiredScalarColumnData, RetainedTableReadMetadata, StorageProfiler,
    TABLE_CONTROL_FILE, TableDatContents, TableDatResult, read_table_dat_dispatch,
};
use crate::table::{
    RequiredScalarColumnDestination, SelectedArray1DCells, SelectedArray1DCellsMut,
    SelectedArray1DShape, SelectedArray2DCells, SelectedArray2DCellsMut, SelectedArray2DShape,
    TableError,
};

mod cached_columns;
mod lazy_reads;

type ScalarColumnValueMap = HashMap<String, Vec<Option<ScalarValue>>>;
type RequiredScalarColumnValueMap = HashMap<String, RequiredScalarColumnData>;

#[derive(Debug)]
struct LazyRowsSource {
    path: PathBuf,
    row_count_hint: usize,
    read_metadata: OnceLock<RetainedTableReadMetadata>,
}

impl LazyRowsSource {
    fn read_metadata(&self) -> Result<&RetainedTableReadMetadata, TableError> {
        self.try_read_plain_metadata()?.ok_or_else(|| {
            TableError::Storage(format!(
                "typed selected reads require a plain table at {}",
                self.path.display()
            ))
        })
    }

    fn try_read_plain_metadata(&self) -> Result<Option<&RetainedTableReadMetadata>, TableError> {
        if self.read_metadata.get().is_none() {
            let control_path = self.path.join(TABLE_CONTROL_FILE);
            let parsed = read_table_dat_dispatch(&control_path).map_err(|error| {
                TableError::Storage(format!(
                    "failed to read control metadata for table {}: {error}",
                    self.path.display()
                ))
            })?;
            let table_dat = match parsed {
                TableDatResult::Plain(table_dat) => table_dat,
                TableDatResult::Ref(_) | TableDatResult::Concat(_) => return Ok(None),
            };
            let read_metadata =
                RetainedTableReadMetadata::open(&self.path, table_dat).map_err(|error| {
                    TableError::Storage(format!(
                        "failed to initialize retained read metadata for table {}: {error}",
                        self.path.display()
                    ))
                })?;
            let _ = self.read_metadata.set(read_metadata);
        }
        Ok(self.read_metadata.get())
    }

    fn plain_table_dat(&self) -> Result<&TableDatContents, TableError> {
        Ok(&self.read_metadata()?.table_dat)
    }

    fn retained_asdm_dat(&self) -> Option<&TableDatContents> {
        let table_dat = &self.read_metadata.get()?.table_dat;
        table_dat
            .column_set
            .data_managers
            .iter()
            .any(|manager| manager.type_name == crate::storage::asdm_stman::TYPE)
            .then_some(table_dat)
    }
}

#[derive(Debug)]
struct LoadedRows {
    rows: Vec<RecordValue>,
    undefined_cells: Vec<HashSet<String>>,
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

type BufferedArrayColumn = Vec<OnceLock<Option<ArrayValue>>>;

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
) -> HashMap<String, OnceLock<Vec<Option<ArrayValue>>>> {
    schema
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| matches!(column.column_type(), ColumnType::Array(_)))
        .map(|column| (column.name().to_string(), OnceLock::new()))
        .collect()
}

fn lazy_scalar_column_store(
    schema: Option<&TableSchema>,
) -> HashMap<String, OnceLock<Vec<Option<ScalarValue>>>> {
    schema
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| matches!(column.column_type(), ColumnType::Scalar))
        .map(|column| (column.name().to_string(), OnceLock::new()))
        .collect()
}

fn lazy_buffered_array_cell_store(
    schema: Option<&TableSchema>,
) -> HashMap<String, OnceLock<BufferedArrayColumn>> {
    schema
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| matches!(column.column_type(), ColumnType::Array(_)))
        .map(|column| (column.name().to_string(), OnceLock::new()))
        .collect()
}

fn new_buffered_array_column(row_count: usize) -> BufferedArrayColumn {
    (0..row_count).map(|_| OnceLock::new()).collect()
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

fn apply_prepared_scalar_column(
    rows: &mut [RecordValue],
    column: &str,
    values: &[Option<ScalarValue>],
) {
    for (row_index, value) in values.iter().enumerate() {
        if let Some(value) = value
            && let Some(row) = rows.get_mut(row_index)
        {
            row.upsert(column.to_string(), Value::Scalar(value.clone()));
        }
    }
}

fn apply_prepared_scalar_overrides(
    rows: &mut [RecordValue],
    column: &str,
    values: &HashMap<usize, ScalarValue>,
) {
    for (&row_index, value) in values {
        if let Some(row) = rows.get_mut(row_index) {
            row.upsert(column.to_string(), Value::Scalar(value.clone()));
        }
    }
}

fn apply_prepared_array_column(
    rows: &mut [RecordValue],
    column: &str,
    values: &[Option<ArrayValue>],
) {
    for (row_index, value) in values.iter().enumerate() {
        if let Some(value) = value
            && let Some(row) = rows.get_mut(row_index)
        {
            row.upsert(column.to_string(), Value::Array(value.clone()));
        }
    }
}

fn apply_prepared_array_overrides(
    rows: &mut [RecordValue],
    column: &str,
    values: &PendingArrayColumn,
) {
    for (row_index, value) in &values.rows {
        if let Some(row) = rows.get_mut(*row_index) {
            row.upsert(column.to_string(), Value::Array(value.clone()));
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    loaded_rows: OnceLock<LoadedRows>,
    loaded_scalar_columns: HashMap<String, OnceLock<Vec<Option<ScalarValue>>>>,
    pending_scalar_cells: PendingScalarCells,
    loaded_array_columns: HashMap<String, OnceLock<Vec<Option<ArrayValue>>>>,
    buffered_array_cells: HashMap<String, OnceLock<BufferedArrayColumn>>,
    pending_array_cells: PendingArrayCells,
    lazy_rows: Option<LazyRowsSource>,
    persisted_row_count: usize,
    keywords: RecordValue,
    column_keywords: HashMap<String, RecordValue>,
    schema: Option<TableSchema>,
    /// Counts the changes made to rows, cells, keywords and the schema
    /// through this value; every mutator advances it. A lock compares it
    /// with the value at the last flush to tell whether there is anything
    /// to write (casacore's `tableChanged`/`dataManChanged` flags).
    generation: u64,
    dirty: Option<std::sync::Mutex<dirty::DirtyChanges>>,
    /// Whether rows were added, inserted or removed since the rows were
    /// loaded or last saved in full, so that they no longer line up with
    /// the rows on disk. A partial in-place save refuses the table then
    /// (#880). It is atomic because a full save, which takes a shared
    /// borrow, clears it.
    rows_reshaped: AtomicBool,
}

impl TableImpl {
    /// The change count; see the `generation` field.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the rows changed shape since they matched the disk; see the
    /// `rows_reshaped` field.
    pub(crate) fn rows_reshaped(&self) -> bool {
        self.rows_reshaped.load(Ordering::Relaxed)
    }

    /// Record that the rows on disk now match these rows, after a full save.
    pub(crate) fn note_rows_persisted(&self) {
        self.rows_reshaped.store(false, Ordering::Relaxed);
    }

    pub(crate) fn retained_lazy_metadata_heap_bytes(&self) -> Option<usize> {
        if self.loaded_rows.get().is_some() || self.lazy_rows.is_none() {
            return None;
        }
        self.lazy_rows.as_ref()?.read_metadata.get()?;
        self.retained_owned_metadata_heap_bytes()
    }

    pub(crate) fn retained_cell_cache_bytes(&self) -> Option<usize> {
        // Metadata readers have no need to retain a fully materialized row store.
        if self.loaded_rows.get().is_some() {
            return None;
        }
        let mut bytes = 0usize;
        for column in self
            .loaded_scalar_columns
            .values()
            .filter_map(OnceLock::get)
        {
            bytes = bytes.checked_add(
                column
                    .capacity()
                    .checked_mul(size_of::<Option<ScalarValue>>())?,
            )?;
            for value in column.iter().flatten() {
                if let ScalarValue::String(text) = value {
                    bytes = bytes.checked_add(text.capacity())?;
                }
            }
        }
        for column in self.loaded_array_columns.values().filter_map(OnceLock::get) {
            bytes = bytes.checked_add(
                column
                    .capacity()
                    .checked_mul(size_of::<Option<ArrayValue>>())?,
            )?;
            for array in column.iter().flatten() {
                bytes = bytes.checked_add(array.retained_heap_bytes()?)?;
            }
        }
        for column in self.buffered_array_cells.values().filter_map(OnceLock::get) {
            bytes = bytes.checked_add(
                column
                    .capacity()
                    .checked_mul(size_of::<OnceLock<Option<ArrayValue>>>())?,
            )?;
            for array in column.iter().filter_map(OnceLock::get).flatten() {
                bytes = bytes.checked_add(array.retained_heap_bytes()?)?;
            }
        }
        Some(bytes)
    }

    pub(crate) fn retained_owned_metadata_heap_bytes(&self) -> Option<usize> {
        if self
            .loaded_scalar_columns
            .values()
            .any(|values| values.get().is_some())
            || self
                .loaded_array_columns
                .values()
                .any(|values| values.get().is_some())
            || self
                .buffered_array_cells
                .values()
                .any(|values| values.get().is_some())
            || !self.pending_scalar_cells.by_column.is_empty()
            || !self.pending_array_cells.by_column.is_empty()
        {
            return None;
        }

        let mut bytes = 0usize;
        if let Some(lazy_rows) = self.lazy_rows.as_ref() {
            bytes = bytes.checked_add(path_heap_bytes(&lazy_rows.path))?;
            if let Some(metadata) = lazy_rows.read_metadata.get() {
                bytes = bytes.checked_add(metadata.retained_heap_bytes()?)?;
            }
        }
        if let Some(rows) = self.loaded_rows.get() {
            bytes = bytes
                .checked_add(rows.rows.capacity().checked_mul(size_of::<RecordValue>())?)?
                .checked_add(
                    rows.undefined_cells
                        .capacity()
                        .checked_mul(size_of::<HashSet<String>>())?,
                )?;
            for row in &rows.rows {
                bytes = bytes.checked_add(row.retained_heap_bytes()?)?;
            }
            for undefined in &rows.undefined_cells {
                bytes =
                    bytes.checked_add(undefined.capacity().checked_mul(size_of::<String>())?)?;
                for name in undefined {
                    bytes = bytes.checked_add(name.capacity())?;
                }
            }
        }
        bytes = bytes
            .checked_add(string_keyed_map_heap_bytes(&self.loaded_scalar_columns)?)?
            .checked_add(string_keyed_map_heap_bytes(&self.loaded_array_columns)?)?
            .checked_add(string_keyed_map_heap_bytes(&self.buffered_array_cells)?)?
            .checked_add(string_keyed_map_heap_bytes(
                &self.pending_scalar_cells.by_column,
            )?)?
            .checked_add(string_keyed_map_heap_bytes(
                &self.pending_array_cells.by_column,
            )?)?
            .checked_add(self.keywords.retained_heap_bytes()?)?
            .checked_add(
                self.column_keywords
                    .capacity()
                    .checked_mul(size_of::<(String, RecordValue)>())?,
            )?;
        for (column, keywords) in &self.column_keywords {
            bytes = bytes
                .checked_add(column.capacity())?
                .checked_add(keywords.retained_heap_bytes()?)?;
        }
        if let Some(schema) = &self.schema {
            bytes = bytes.checked_add(schema.retained_heap_bytes()?)?;
        }
        Some(bytes)
    }

    pub(crate) fn new() -> Self {
        let loaded_rows = OnceLock::new();
        loaded_rows
            .set(LoadedRows {
                rows: Vec::new(),
                undefined_cells: Vec::new(),
            })
            .expect("initialize empty row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: HashMap::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: HashMap::new(),
            buffered_array_cells: HashMap::new(),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count: 0,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
            generation: 0,
            dirty: None,
            rows_reshaped: AtomicBool::new(false),
        }
    }

    pub(crate) fn from_rows(rows: Vec<RecordValue>) -> Self {
        let persisted_row_count = rows.len();
        let loaded_rows = OnceLock::new();
        loaded_rows
            .set(eager_loaded_rows(rows, Vec::new()))
            .expect("initialize eager row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: HashMap::new(),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: HashMap::new(),
            buffered_array_cells: HashMap::new(),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
            generation: 0,
            dirty: None,
            rows_reshaped: AtomicBool::new(false),
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
        let loaded_rows = OnceLock::new();
        loaded_rows
            .set(eager_loaded_rows(rows, undefined_cells))
            .expect("initialize eager row store");
        Self {
            loaded_rows,
            loaded_scalar_columns: lazy_scalar_column_store(schema.as_ref()),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: lazy_array_column_store(schema.as_ref()),
            buffered_array_cells: lazy_buffered_array_cell_store(schema.as_ref()),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count,
            keywords,
            column_keywords,
            schema,
            generation: 0,
            dirty: None,
            rows_reshaped: AtomicBool::new(false),
        }
    }

    pub(crate) fn with_lazy_rows_keywords_and_schema(
        row_count: usize,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
        path: PathBuf,
        retained_read_metadata: Option<RetainedTableReadMetadata>,
    ) -> Self {
        let read_metadata = OnceLock::new();
        if let Some(retained_read_metadata) = retained_read_metadata {
            read_metadata
                .set(retained_read_metadata)
                .expect("initialize retained table read metadata");
        }
        Self {
            loaded_rows: OnceLock::new(),
            loaded_scalar_columns: lazy_scalar_column_store(schema.as_ref()),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: lazy_array_column_store(schema.as_ref()),
            buffered_array_cells: lazy_buffered_array_cell_store(schema.as_ref()),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: Some(LazyRowsSource {
                path,
                row_count_hint: row_count,
                read_metadata,
            }),
            persisted_row_count: row_count,
            keywords,
            column_keywords,
            schema,
            generation: 0,
            dirty: None,
            rows_reshaped: AtomicBool::new(false),
        }
    }

    pub(crate) fn with_unloaded_rows_keywords_and_schema(
        row_count: usize,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
    ) -> Self {
        Self {
            loaded_rows: OnceLock::new(),
            loaded_scalar_columns: lazy_scalar_column_store(schema.as_ref()),
            pending_scalar_cells: PendingScalarCells::default(),
            loaded_array_columns: lazy_array_column_store(schema.as_ref()),
            buffered_array_cells: lazy_buffered_array_cell_store(schema.as_ref()),
            pending_array_cells: PendingArrayCells::default(),
            lazy_rows: None,
            persisted_row_count: row_count,
            keywords,
            column_keywords,
            schema,
            generation: 0,
            dirty: None,
            rows_reshaped: AtomicBool::new(false),
        }
    }

    pub(crate) fn add_row(&mut self, row: RecordValue) -> Result<(), TableError> {
        let first = self.row_count();
        let loaded = self.ensure_loaded_mut()?;
        loaded.rows.push(row);
        loaded.undefined_cells.push(HashSet::new());
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        *self.rows_reshaped.get_mut() = true;
        self.note_row_append(first);
        Ok(())
    }

    pub(crate) fn rows(&self) -> Result<&[RecordValue], TableError> {
        Ok(self.ensure_loaded()?.rows.as_slice())
    }

    pub(crate) fn prepared_rows(
        &self,
        columns: &[&str],
    ) -> Result<Option<Vec<RecordValue>>, TableError> {
        if self.loaded_rows.get().is_some() {
            return Ok(None);
        }

        let Some(source) = &self.lazy_rows else {
            return Ok(None);
        };

        let storage = CompositeStorage;
        let row_hint = Some(source.row_count_hint as u64);
        let mut rows = match source.retained_asdm_dat() {
            Some(table_dat) => {
                storage.load_plain_selected_columns(&source.path, table_dat, columns, row_hint)
            }
            None => storage.load_selected_columns_with_row_hint(&source.path, columns, row_hint),
        }
        .map_err(|err| {
            TableError::Storage(format!(
                "failed to load prepared rows for table {}: {err}",
                source.path.display()
            ))
        })?
        .rows;

        for &column in columns {
            if let Some(cached_column) = self.loaded_scalar_columns.get(column)
                && let Some(values) = cached_column.get()
            {
                apply_prepared_scalar_column(&mut rows, column, values);
            }
            if let Some(overrides) = self.pending_scalar_cells.by_column.get(column) {
                apply_prepared_scalar_overrides(&mut rows, column, overrides);
            }
            if let Some(cached_column) = self.loaded_array_columns.get(column)
                && let Some(values) = cached_column.get()
            {
                apply_prepared_array_column(&mut rows, column, values);
            }
            if let Some(overrides) = self.pending_array_cells.by_column.get(column) {
                apply_prepared_array_overrides(&mut rows, column, overrides);
            }
        }

        Ok(Some(rows))
    }

    pub(crate) fn undefined_cells(&self) -> Result<&[HashSet<String>], TableError> {
        Ok(self.ensure_loaded()?.undefined_cells.as_slice())
    }

    pub(crate) fn undefined_cells_mut(&mut self) -> Result<&mut [HashSet<String>], TableError> {
        self.note_change();
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

    pub(crate) fn row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<Option<&mut RecordValue>, TableError> {
        self.note_row_change(row_index);
        Ok(self.ensure_loaded_mut()?.rows.get_mut(row_index))
    }

    pub(crate) fn rows_mut(&mut self) -> Result<&mut [RecordValue], TableError> {
        self.note_change();
        Ok(self.ensure_loaded_mut()?.rows.as_mut_slice())
    }

    pub(crate) fn set_cached_scalar_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: ScalarValue,
    ) -> Result<Option<ScalarValue>, TableError> {
        self.note_cell_change(row_index, column);
        if let Some(loaded_rows) = self.loaded_rows.get_mut() {
            // Cells written before the rows were loaded are in them now; an
            // older value must not be saved over this one.
            self.pending_scalar_cells.by_column.clear();
            self.pending_array_cells.by_column.clear();
            let Some(row) = loaded_rows.rows.get_mut(row_index) else {
                return Ok(Some(value));
            };
            row.upsert(column.to_string(), Value::Scalar(value));
            return Ok(None);
        }

        if row_index >= self.row_count() {
            return Ok(Some(value));
        }

        if let Some(cached_column) = self.loaded_scalar_columns.get_mut(column)
            && cached_column.get().is_some()
        {
            let column_values = cached_column
                .get_mut()
                .expect("scalar column initialized before mutable access");
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
        self.note_cell_change(row_index, column);
        if let Some(loaded_rows) = self.loaded_rows.get_mut() {
            // As in `set_cached_scalar_cell`.
            self.pending_scalar_cells.by_column.clear();
            self.pending_array_cells.by_column.clear();
            let Some(row) = loaded_rows.rows.get_mut(row_index) else {
                return Ok(Some(value));
            };
            row.upsert(column.to_string(), Value::Array(value));
            return Ok(None);
        }

        if row_index >= self.row_count() {
            return Ok(Some(value));
        }

        if let Some(cached_column) = self.loaded_array_columns.get_mut(column)
            && cached_column.get().is_some()
        {
            let column_values = cached_column
                .get_mut()
                .expect("array column initialized before mutable access");
            if let Some(cell) = column_values.get_mut(row_index) {
                *cell = Some(value);
                return Ok(None);
            }
            return Ok(Some(value));
        }
        if let Some(pending) = self.pending_array_cells.by_column.get_mut(column) {
            pending.insert(row_index, value);
        } else {
            self.pending_array_cells
                .by_column
                .entry(column.to_string())
                .or_default()
                .insert(row_index, value);
        }
        Ok(None)
    }

    pub(crate) fn reserve_pending_array_cells(&mut self, column: &str, additional: usize) {
        self.pending_array_cells
            .by_column
            .entry(column.to_string())
            .or_default()
            .reserve(additional);
    }

    pub(crate) fn pending_scalar_cells(
        &self,
        column: &str,
    ) -> Option<&HashMap<usize, ScalarValue>> {
        self.pending_scalar_cells.by_column.get(column)
    }

    pub(crate) fn pending_array_cells(&self, column: &str) -> Option<&[(usize, ArrayValue)]> {
        self.pending_array_cells
            .by_column
            .get(column)
            .map(|cells| cells.rows.as_slice())
    }

    pub(crate) fn discard_pending_cell_updates(&mut self, columns: &[&str], rows: &[usize]) {
        let rows = rows.iter().copied().collect::<HashSet<_>>();
        for column in columns {
            if let Some(values) = self.pending_scalar_cells.by_column.get_mut(*column) {
                values.retain(|row, _| !rows.contains(row));
                if values.is_empty() {
                    self.pending_scalar_cells.by_column.remove(*column);
                }
            }
            if let Some(values) = self.pending_array_cells.by_column.get_mut(*column) {
                values.rows.retain(|(row, _)| !rows.contains(row));
                if values.rows.is_empty() {
                    self.pending_array_cells.by_column.remove(*column);
                }
            }
            // Persisted values are now authoritative. Drop any lazy read cache
            // populated before the mutation so subsequent access observes the
            // just-written cells and long sessions release completed payloads.
            if let Some(values) = self.loaded_scalar_columns.get_mut(*column) {
                *values = OnceLock::new();
            }
            if let Some(values) = self.loaded_array_columns.get_mut(*column) {
                *values = OnceLock::new();
            }
            if let Some(values) = self.buffered_array_cells.get_mut(*column) {
                *values = OnceLock::new();
            }
        }
    }

    pub(crate) fn undefined_for_row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<Option<&mut HashSet<String>>, TableError> {
        self.note_row_change(row_index);
        Ok(self.ensure_loaded_mut()?.undefined_cells.get_mut(row_index))
    }

    pub(crate) fn remove_row(&mut self, index: usize) -> Result<RecordValue, TableError> {
        self.note_change();
        let loaded = self.ensure_loaded_mut()?;
        loaded.undefined_cells.remove(index);
        let removed = loaded.rows.remove(index);
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        *self.rows_reshaped.get_mut() = true;
        Ok(removed)
    }

    pub(crate) fn insert_row(&mut self, index: usize, row: RecordValue) -> Result<(), TableError> {
        self.note_change();
        let loaded = self.ensure_loaded_mut()?;
        loaded.rows.insert(index, row);
        loaded.undefined_cells.insert(index, HashSet::new());
        self.persisted_row_count = loaded.rows.len();
        self.lazy_rows = None;
        *self.rows_reshaped.get_mut() = true;
        Ok(())
    }

    pub(crate) fn keywords(&self) -> &RecordValue {
        &self.keywords
    }

    pub(crate) fn keywords_mut(&mut self) -> &mut RecordValue {
        self.note_metadata_change();
        &mut self.keywords
    }

    pub(crate) fn replace_keyword_records(
        &mut self,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
    ) {
        self.keywords = keywords;
        self.column_keywords = column_keywords;
    }

    pub(crate) fn column_keywords(&self, column: &str) -> Option<&RecordValue> {
        self.column_keywords.get(column)
    }

    pub(crate) fn set_column_keywords(&mut self, column: String, keywords: RecordValue) {
        self.note_metadata_change();
        self.column_keywords.insert(column, keywords);
    }

    pub(crate) fn all_column_keywords(&self) -> &HashMap<String, RecordValue> {
        &self.column_keywords
    }

    pub(crate) fn remove_column_keywords(&mut self, column: &str) -> Option<RecordValue> {
        self.note_metadata_change();
        self.column_keywords.remove(column)
    }

    pub(crate) fn rename_column_keywords(&mut self, old: &str, new: String) {
        self.note_metadata_change();
        if let Some(kw) = self.column_keywords.remove(old) {
            self.column_keywords.insert(new, kw);
        }
    }

    pub(crate) fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    pub(crate) fn set_schema(&mut self, schema: Option<TableSchema>) {
        self.note_change();
        self.install_schema(schema);
    }

    pub(crate) fn install_added_column(
        &mut self,
        schema: TableSchema,
        column: &str,
        default: Option<Value>,
    ) -> Result<(), TableError> {
        self.note_column_added(column);
        self.install_schema(Some(schema));
        if let Some(value) = default {
            for row in &mut self.ensure_loaded_mut()?.rows {
                row.push(casa_core::RecordField::new(column, value.clone()));
            }
        }
        Ok(())
    }

    fn install_schema(&mut self, schema: Option<TableSchema>) {
        self.loaded_scalar_columns = lazy_scalar_column_store(schema.as_ref());
        self.loaded_array_columns = lazy_array_column_store(schema.as_ref());
        self.buffered_array_cells = lazy_buffered_array_cell_store(schema.as_ref());
        self.schema = schema;
    }

    pub(crate) fn refresh_read_metadata(
        &mut self,
        table_dat: TableDatContents,
    ) -> Result<(), TableError> {
        if let Some(source) = self.lazy_rows.as_mut() {
            source.read_metadata.take();
            let metadata =
                RetainedTableReadMetadata::open(&source.path, table_dat).map_err(|error| {
                    TableError::Storage(format!(
                        "failed to refresh read metadata for table {}: {error}",
                        source.path.display()
                    ))
                })?;
            source
                .read_metadata
                .set(metadata)
                .expect("read metadata cleared before refreshing");
        }
        Ok(())
    }

    pub(crate) fn rows_are_loaded(&self) -> bool {
        self.loaded_rows.get().is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_loaded_rows(&self) -> bool {
        self.rows_are_loaded()
    }

    #[cfg(test)]
    pub(crate) fn has_loaded_array_column(&self, column: &str) -> bool {
        self.loaded_array_columns
            .get(column)
            .is_some_and(|cached| cached.get().is_some())
    }

    #[cfg(test)]
    pub(crate) fn has_buffered_array_column(&self, column: &str) -> bool {
        self.buffered_array_cells
            .get(column)
            .is_some_and(|cached| cached.get().is_some())
    }

    #[cfg(test)]
    pub(crate) fn has_cached_control_metadata(&self) -> bool {
        self.lazy_rows
            .as_ref()
            .is_some_and(|source| source.read_metadata.get().is_some())
    }

    #[cfg(test)]
    pub(crate) fn cached_tiled_header_count(&self) -> usize {
        self.lazy_rows
            .as_ref()
            .and_then(|source| source.read_metadata.get())
            .map_or(0, |metadata| metadata.tiled.cached_header_count())
    }

    #[cfg(test)]
    pub(crate) fn cached_control_metadata_heap_bytes(&self) -> usize {
        self.lazy_rows
            .as_ref()
            .and_then(|source| source.read_metadata.get())
            .and_then(|metadata| metadata.table_dat.retained_heap_bytes())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn cached_tiled_header_heap_bytes(&self) -> usize {
        self.lazy_rows
            .as_ref()
            .and_then(|source| source.read_metadata.get())
            .and_then(|metadata| metadata.tiled.retained_heap_bytes())
            .unwrap_or(0)
    }

    pub(crate) fn has_loaded_scalar_column(&self, column: &str) -> bool {
        self.loaded_scalar_columns
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

pub(crate) fn path_heap_bytes(path: &std::path::Path) -> usize {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().len()
    }
    #[cfg(not(unix))]
    {
        path.as_os_str().to_string_lossy().len()
    }
}

fn string_keyed_map_heap_bytes<V>(map: &HashMap<String, V>) -> Option<usize> {
    map.capacity()
        .checked_mul(size_of::<(String, V)>())?
        .checked_add(
            map.keys()
                .try_fold(0_usize, |bytes, key| bytes.checked_add(key.capacity()))?,
        )
}

fn required_scalar_column_data_len(values: &RequiredScalarColumnData) -> usize {
    match values {
        RequiredScalarColumnData::Bool(values) => values.len(),
        RequiredScalarColumnData::Int32(values) => values.len(),
        RequiredScalarColumnData::Float32(values) => values.len(),
        RequiredScalarColumnData::Float64(values) => values.len(),
    }
}

fn set_required_scalar_column_value(
    values: &mut RequiredScalarColumnData,
    row_index: usize,
    value: &ScalarValue,
    column: &str,
) -> Result<(), TableError> {
    match (values, value) {
        (RequiredScalarColumnData::Bool(values), ScalarValue::Bool(value)) => {
            let Some(slot) = values.get_mut(row_index) else {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} override row {row_index} is out of bounds"
                )));
            };
            *slot = *value;
        }
        (RequiredScalarColumnData::Int32(values), ScalarValue::Int32(value)) => {
            let Some(slot) = values.get_mut(row_index) else {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} override row {row_index} is out of bounds"
                )));
            };
            *slot = *value;
        }
        (RequiredScalarColumnData::Float32(values), ScalarValue::Float32(value)) => {
            let Some(slot) = values.get_mut(row_index) else {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} override row {row_index} is out of bounds"
                )));
            };
            *slot = *value;
        }
        (RequiredScalarColumnData::Float64(values), ScalarValue::Float64(value)) => {
            let Some(slot) = values.get_mut(row_index) else {
                return Err(TableError::Storage(format!(
                    "required scalar column {column} override row {row_index} is out of bounds"
                )));
            };
            *slot = *value;
        }
        (_, other) => {
            return Err(TableError::Storage(format!(
                "required scalar column {column} override has incompatible type {:?}",
                other.primitive_type()
            )));
        }
    }
    Ok(())
}

fn required_scalar_column_data_from_optional_scalars(
    values: &[Option<ScalarValue>],
    column: &str,
) -> Result<RequiredScalarColumnData, TableError> {
    let Some(first) = values.iter().find_map(|value| value.as_ref()) else {
        return Err(TableError::Storage(format!(
            "required scalar column {column} has no values"
        )));
    };
    match first {
        ScalarValue::Bool(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Bool(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Bool"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Bool),
        ScalarValue::Int32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Int32(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Int32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Int32),
        ScalarValue::Float32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float32(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Float32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Float32),
        ScalarValue::Float64(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float64(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Float64"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Float64),
        other => Err(TableError::Storage(format!(
            "required scalar column {column} has unsupported type {:?}",
            other.primitive_type()
        ))),
    }
}
