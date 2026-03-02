use casacore_types::RecordValue;

#[derive(Debug, Default)]
pub(crate) struct TableImpl {
    records: Vec<RecordValue>,
}

impl TableImpl {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_records(records: Vec<RecordValue>) -> Self {
        Self { records }
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
}
