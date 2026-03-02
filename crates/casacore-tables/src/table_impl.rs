use casacore_types::RecordValue;

use crate::schema::TableSchema;

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    rows: Vec<RecordValue>,
    keywords: RecordValue,
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
            schema: None,
        }
    }

    pub(crate) fn with_rows_keywords_and_schema(
        rows: Vec<RecordValue>,
        keywords: RecordValue,
        schema: Option<TableSchema>,
    ) -> Self {
        Self {
            rows,
            keywords,
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

    pub(crate) fn keywords(&self) -> &RecordValue {
        &self.keywords
    }

    pub(crate) fn keywords_mut(&mut self) -> &mut RecordValue {
        &mut self.keywords
    }

    pub(crate) fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    pub(crate) fn set_schema(&mut self, schema: Option<TableSchema>) {
        self.schema = schema;
    }
}
