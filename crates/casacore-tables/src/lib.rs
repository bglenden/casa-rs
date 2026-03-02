pub mod schema;
pub mod table;
mod table_impl;

pub(crate) mod aipsio;
pub(crate) mod storage;

pub use schema::{
    ArrayShapeContract, ColumnOptions, ColumnSchema, ColumnType, SchemaError, TableSchema,
};
pub use table::{
    ColumnCellIter, ColumnCellRef, RecordColumnCell, RecordColumnIter, RowRange, Table, TableError,
    TableOptions,
};
