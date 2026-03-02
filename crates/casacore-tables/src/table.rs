use std::path::{Path, PathBuf};

use casacore_types::RecordValue;
use thiserror::Error;

use crate::table_impl::TableImpl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    pub path: PathBuf,
}

impl TableOptions {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug, Error)]
pub enum TableError {
    #[error("table operation is not implemented yet: {0}")]
    NotYetImplemented(&'static str),
    #[error("aipsio internal error: {0}")]
    AipsIo(String),
}

impl From<crate::aipsio::AipsIoInternalError> for TableError {
    fn from(value: crate::aipsio::AipsIoInternalError) -> Self {
        Self::AipsIo(value.to_string())
    }
}

#[derive(Debug, Default)]
pub struct Table {
    inner: TableImpl,
}

impl Table {
    pub fn new() -> Self {
        Self {
            inner: TableImpl::new(),
        }
    }

    pub fn from_records(records: Vec<RecordValue>) -> Self {
        Self {
            inner: TableImpl::from_records(records),
        }
    }

    pub fn open(_options: TableOptions) -> Result<Self, TableError> {
        Err(TableError::NotYetImplemented(
            "disk-backed table loading is pending",
        ))
    }

    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    pub fn records(&self) -> &[RecordValue] {
        self.inner.records()
    }

    pub fn push_record(&mut self, record: RecordValue) {
        self.inner.push_record(record);
    }
}

#[cfg(test)]
mod tests {
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    use super::Table;

    #[test]
    fn table_keeps_records_in_order() {
        let first = RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]);
        let second = RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(2)),
        )]);

        let table = Table::from_records(vec![first.clone(), second.clone()]);
        assert_eq!(table.row_count(), 2);
        assert_eq!(table.records(), &[first, second]);
    }
}
