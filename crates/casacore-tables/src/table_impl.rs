// SPDX-License-Identifier: LGPL-3.0-or-later
use std::cell::OnceCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use casacore_types::RecordValue;

use crate::schema::TableSchema;
use crate::storage::CompositeStorage;
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
        let storage = CompositeStorage;
        let snapshot = storage
            .load_with_row_hint(&source.path, Some(source.row_count_hint as u64))
            .map_err(|err| {
                TableError::Storage(format!(
                    "failed to materialize rows for table {}: {err}",
                    source.path.display()
                ))
            })?;
        Ok(eager_loaded_rows(snapshot.rows, snapshot.undefined_cells))
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

    pub(crate) fn row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<Option<&mut RecordValue>, TableError> {
        Ok(self.ensure_loaded_mut()?.rows.get_mut(row_index))
    }

    pub(crate) fn rows_mut(&mut self) -> Result<&mut [RecordValue], TableError> {
        Ok(self.ensure_loaded_mut()?.rows.as_mut_slice())
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
        self.persisted_row_count = self.loaded_rows.get().map_or(0, |loaded| loaded.rows.len());
        self.lazy_rows = None;
        self.keywords = keywords;
        self.column_keywords = column_keywords;
        self.schema = schema;
    }
}
