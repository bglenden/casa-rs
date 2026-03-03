// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::HashMap;

use casacore_types::RecordValue;

use crate::schema::TableSchema;

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    rows: Vec<RecordValue>,
    keywords: RecordValue,
    column_keywords: HashMap<String, RecordValue>,
    schema: Option<TableSchema>,
}

impl TableImpl {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_rows(rows: Vec<RecordValue>) -> Self {
        Self {
            rows,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
        }
    }

    pub(crate) fn with_rows_keywords_and_schema(
        rows: Vec<RecordValue>,
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
    ) -> Self {
        Self {
            rows,
            keywords,
            column_keywords,
            schema,
        }
    }

    pub(crate) fn add_row(&mut self, row: RecordValue) {
        self.rows.push(row);
    }

    pub(crate) fn rows(&self) -> &[RecordValue] {
        &self.rows
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn row(&self, row_index: usize) -> Option<&RecordValue> {
        self.rows.get(row_index)
    }

    pub(crate) fn row_mut(&mut self, row_index: usize) -> Option<&mut RecordValue> {
        self.rows.get_mut(row_index)
    }

    pub(crate) fn rows_mut(&mut self) -> &mut [RecordValue] {
        &mut self.rows
    }

    pub(crate) fn remove_row(&mut self, index: usize) -> RecordValue {
        self.rows.remove(index)
    }

    pub(crate) fn insert_row(&mut self, index: usize, row: RecordValue) {
        self.rows.insert(index, row);
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
        keywords: RecordValue,
        column_keywords: HashMap<String, RecordValue>,
        schema: Option<TableSchema>,
    ) {
        self.rows = rows;
        self.keywords = keywords;
        self.column_keywords = column_keywords;
        self.schema = schema;
    }
}
