//! Read and write casacore-compatible tables.
//!
//! This crate provides a Rust API for creating, persisting, and reopening
//! tables in the casacore on-disk format. Tables written by this crate can
//! be read by C++ casacore and vice versa.
//!
//! # Key types
//!
//! - [`Table`] — the main entry point for creating and querying tables.
//! - [`TableSchema`] / [`ColumnSchema`] — define column names, types, and shapes.
//! - [`TableOptions`] — configure the path and storage manager for persistence.
//! - [`DataManagerKind`] — choose between `StManAipsIO` and `StandardStMan`.
//!
//! # Demo program
//!
//! A runnable demo (Rust equivalent of the C++ `tTable` test program) is
//! included. Each crate in the workspace follows the same convention: demo
//! logic lives in a [`demo`] module, and a thin example binary drives it.
//!
//! ```bash
//! cargo run -p casacore-tables --example t_table
//! ```

mod schema;
mod table;
mod table_impl;

pub(crate) mod aipsio;
pub(crate) mod storage;

pub mod demo;

pub use schema::{
    ArrayShapeContract, ColumnOptions, ColumnSchema, ColumnType, SchemaError, TableSchema,
};
pub use table::{
    ColumnCellIter, ColumnCellRef, ColumnChunkIter, DataManagerKind, RecordColumnCell,
    RecordColumnIter, RowRange, Table, TableError, TableOptions,
};
