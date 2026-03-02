use casacore_types::RecordValue;

use crate::schema::TableSchema;

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    records: Vec<RecordValue>,
    keywords: RecordValue,
    schema: Option<TableSchema>,
}

impl TableImpl {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_records(records: Vec<RecordValue>) -> Self {
        Self {
            records,
            keywords: RecordValue::default(),
            schema: None,
        }
    }

    pub(crate) fn with_records_keywords_and_schema(
        records: Vec<RecordValue>,
        keywords: RecordValue,
        schema: Option<TableSchema>,
    ) -> Self {
        Self {
            records,
            keywords,
            schema,
        }
    }

    pub(crate) fn push_record(&mut self, record: RecordValue) {
        self.records.push(record);
    }

    pub(crate) fn records(&self) -> &[RecordValue] {
        &self.records
    }

    pub(crate) fn row_count(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn row(&self, row_index: usize) -> Option<&RecordValue> {
        self.records.get(row_index)
    }

    pub(crate) fn row_mut(&mut self, row_index: usize) -> Option<&mut RecordValue> {
        self.records.get_mut(row_index)
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
