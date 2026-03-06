// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and write casacore-compatible tables.
//!
//! This crate is the Rust implementation of the **Casacore Table Data System
//! (CTDS)**. It provides a native Rust API for creating, reading, writing, and
//! persisting tables in the casacore on-disk format. Tables written by this
//! crate can be read by C++ casacore and vice versa; the binary layout of both
//! supported storage managers is preserved exactly.
//!
//! # What is a casacore table?
//!
//! A casacore table is a rectangular data structure: a sequence of rows, each
//! of which is a record (a map from column name to typed value). In addition,
//! a table carries table-level keywords and per-column keywords — both stored
//! as [`casacore_types::RecordValue`] maps. From C++ casacore's `Tables.h`:
//!
//! > "Tables are the fundamental storage mechanism. A table consists of rows
//! > and columns with keyword/value pairs. Cells can contain scalars, direct
//! > arrays, or indirect arrays."
//!
//! Supported cell types mirror the C++ standard set: `Bool`, `Int32`,
//! `Int64`, `Float32`, `Float64`, `Complex`, `DComplex`, and `String`, plus
//! n-dimensional arrays of each scalar type.
//!
//! # Key types
//!
//! | Type | Role |
//! |------|------|
//! | [`Table`] | Create, query, and persist a table |
//! | [`ConcatTable`] | A read-only virtual concatenation of tables |
//! | [`RefTable`] | A view over a parent table's rows and/or columns |
//! | [`SortOrder`] | Ascending or descending sort direction |
//! | [`TableGroup`] | A group of rows with equal key values |
//! | [`TableIterator`] | Iterate groups of rows by key columns |
//! | [`TableSchema`] | Declare column names, types, and array shapes |
//! | [`ColumnSchema`] | Schema for a single column |
//! | [`TableKind`] | Distinguish plain (disk-backed) from memory (transient) tables |
//! | [`TableOptions`] | Bundle a filesystem path with a storage-manager choice |
//! | [`DataManagerKind`] | Choose between `StManAipsIO` and `StandardStMan` |
//! | [`EndianFormat`] | Choose big-endian, little-endian, or host byte order |
//! | [`RowRange`] | Select a contiguous or strided subset of rows |
//! | [`LockMode`] | Choose a locking strategy (permanent, user, auto, none) |
//! | [`LockOptions`] | Bundle lock mode with inspection interval |
//! | [`LockType`] | Distinguish read vs. write locks |
//! | [`TableError`] | All errors from table operations |
//!
//! # Storage managers
//!
//! This crate supports five on-disk formats, all interoperable with C++
//! casacore:
//!
//! - [`DataManagerKind::StManAipsIO`] — each column is written as a single
//!   flat AipsIO stream. This is the simplest layout and the crate default.
//!   It is compatible with older versions of casacore.
//!
//! - [`DataManagerKind::StandardStMan`] — data is partitioned into
//!   fixed-size buckets. This is the default storage manager in C++ casacore
//!   and provides more efficient random access for large tables.
//!
//! - [`DataManagerKind::TiledColumnStMan`] — stores multi-dimensional
//!   array data in rectangular tiles within a single hypercube. All rows
//!   must have the same array shape. Best for large fixed-shape columns.
//!
//! - [`DataManagerKind::TiledShapeStMan`] — one hypercube per unique
//!   array shape. Rows with different shapes are automatically grouped.
//!   Standard format for variable-shape visibility columns.
//!
//! - [`DataManagerKind::TiledCellStMan`] — one hypercube per row,
//!   allowing fully variable shapes. Most memory-intensive variant.
//!
//! # Reference tables (views)
//!
//! A [`RefTable`] is a lightweight view over a parent [`Table`], selecting
//! specific rows and/or columns without copying data. All cell reads and
//! writes pass through to the parent.
//!
//! ```rust
//! # use casacore_tables::{Table, TableSchema, ColumnSchema};
//! # use casacore_types::*;
//! # let schema = TableSchema::new(vec![
//! #     ColumnSchema::scalar("id", PrimitiveType::Int32),
//! # ]).unwrap();
//! # let mut table = Table::with_schema(schema);
//! # table.add_row(RecordValue::new(vec![RecordField::new("id", Value::Scalar(ScalarValue::Int32(0)))])).unwrap();
//! # table.add_row(RecordValue::new(vec![RecordField::new("id", Value::Scalar(ScalarValue::Int32(1)))])).unwrap();
//! # table.add_row(RecordValue::new(vec![RecordField::new("id", Value::Scalar(ScalarValue::Int32(2)))])).unwrap();
//! // Select rows by index, by column name, or by predicate:
//! let view = table.select_rows(&[0, 2]).unwrap();
//! assert_eq!(view.row_count(), 2);
//! ```
//!
//! A `RefTable` can be [saved](RefTable::save) to disk in C++-compatible
//! format. When reopened via [`Table::open`], the view is materialized
//! (the parent is loaded and referenced rows extracted).
//!
//! # Sorting and grouped iteration
//!
//! [`Table::sort`] returns a [`RefTable`] with rows reordered by one or
//! more key columns. [`Table::iter_groups`] groups rows by equal key
//! values, yielding [`TableGroup`] values (C++ equivalent:
//! `TableIterator`).
//!
//! ```rust
//! # use casacore_tables::{Table, TableSchema, ColumnSchema, SortOrder};
//! # use casacore_types::*;
//! # let schema = TableSchema::new(vec![
//! #     ColumnSchema::scalar("id", PrimitiveType::Int32),
//! # ]).unwrap();
//! # let mut table = Table::with_schema(schema);
//! # for i in [3, 1, 2] {
//! #     table.add_row(RecordValue::new(vec![
//! #         RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
//! #     ])).unwrap();
//! # }
//! // Sort ascending by id:
//! let sorted = table.sort(&[("id", SortOrder::Ascending)]).unwrap();
//! assert_eq!(sorted.row_count(), 3);
//! ```
//!
//! # Table concatenation and copy
//!
//! [`Table::concat`] virtually combines multiple tables with the same
//! schema into a single [`ConcatTable`] view without copying data.
//! [`Table::deep_copy`] saves all rows with a (possibly different)
//! storage manager, and [`Table::shallow_copy`] saves schema and
//! keywords with zero rows.
//!
//! ```rust,no_run
//! # use casacore_tables::{Table, TableOptions, DataManagerKind};
//! // Concatenate two tables:
//! let t1 = Table::open(TableOptions::new("/tmp/part1")).unwrap();
//! let t2 = Table::open(TableOptions::new("/tmp/part2")).unwrap();
//! let concat = Table::concat(vec![t1, t2]).unwrap();
//! assert_eq!(concat.row_count(), 10); // combined rows
//!
//! // Deep copy with storage manager conversion:
//! let table = Table::open(TableOptions::new("/tmp/original")).unwrap();
//! table.deep_copy(
//!     TableOptions::new("/tmp/copy")
//!         .with_data_manager(DataManagerKind::StandardStMan),
//! ).unwrap();
//! ```
//!
//! # Table locking (Unix)
//!
//! When multiple processes share a table on disk, file-based locking
//! prevents data corruption. Open a table with [`Table::open_with_lock`]
//! and a [`LockOptions`] to enable locking. The lock file (`table.lock`)
//! uses the same binary format as C++ casacore, so Rust and C++ processes
//! can safely share tables.
//!
//! Available lock modes (see [`LockMode`]):
//!
//! - **`PermanentLocking`** — acquire on open, hold until close.
//! - **`UserLocking`** — explicit [`Table::lock`] / [`Table::unlock`] calls.
//! - **`AutoLocking`** — open with a read lock; mutating operations acquire
//!   temporary write locks automatically.
//! - **`NoLocking`** — no lock file (the default, for single-process use;
//!   unsafe for concurrent writers).
//!
//! # Memory tables
//!
//! A memory table ([`TableKind::Memory`]) holds all data in process memory
//! and is deleted when dropped. Locking is a no-op (always writable).
//! Memory tables support all the same operations as plain tables: row
//! and column CRUD, keywords, sorting, selection, and grouped iteration.
//!
//! Use [`Table::new_memory`] or [`Table::with_schema_memory`] to create
//! one. Call [`Table::save`] to materialize a memory table to disk
//! (producing a regular plain table). Use [`Table::to_memory`] to create
//! a transient copy of any existing table.
//!
//! ```rust
//! use casacore_tables::{Table, TableSchema, ColumnSchema, TableKind};
//! use casacore_types::*;
//!
//! let schema = TableSchema::new(vec![
//!     ColumnSchema::scalar("id", PrimitiveType::Int32),
//! ]).unwrap();
//! let mut table = Table::with_schema_memory(schema);
//! table.add_row(RecordValue::new(vec![
//!     RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
//! ])).unwrap();
//! assert!(table.is_memory());
//! assert_eq!(table.row_count(), 1);
//! ```
//!
//! C++ equivalent: `MemoryTable`, `MemoryStMan`.
//!
//! # Typical workflow
//!
//! ```rust,no_run
//! use casacore_tables::{
//!     Table, TableOptions, TableSchema, ColumnSchema, DataManagerKind,
//! };
//! use casacore_types::{PrimitiveType, RecordValue, RecordField, Value, ScalarValue};
//!
//! // 1. Define the schema.
//! let schema = TableSchema::new(vec![
//!     ColumnSchema::scalar("id",   PrimitiveType::Int32),
//!     ColumnSchema::scalar("flux", PrimitiveType::Float64),
//! ]).expect("valid schema");
//!
//! // 2. Build a table and add rows.
//! let mut table = Table::with_schema(schema);
//! table.add_row(RecordValue::new(vec![
//!     RecordField::new("id",   Value::Scalar(ScalarValue::Int32(1))),
//!     RecordField::new("flux", Value::Scalar(ScalarValue::Float64(3.14))),
//! ])).expect("schema-compliant row");
//!
//! // 3. Persist to disk.
//! table.save(TableOptions::new("/tmp/my_table")).expect("save");
//!
//! // 4. Reopen (e.g. in a different process or after C++ casacore wrote it).
//! let reopened = Table::open(TableOptions::new("/tmp/my_table")).expect("open");
//! assert_eq!(reopened.row_count(), 1);
//! ```
//!
//! # Relationship to C++ casacore
//!
//! In C++ casacore the same functionality is split across `Table`,
//! `ScalarColumn<T>`, `ArrayColumn<T>`, and `TableRecord`. The Rust [`Table`]
//! type unifies all of these into a single, dynamically typed interface.
//! Column type safety is enforced at runtime by the accessor methods rather
//! than through compile-time generics.
//!
//! # Demo program
//!
//! A runnable demo (Rust equivalent of the C++ `tTable` test program) is
//! included. Demo logic lives in the [`demo`] module; a thin example binary
//! drives it:
//!
//! ```bash
//! cargo run -p casacore-tables --example t_table
//! ```

mod concat_table;
mod indexing;
mod ref_table;
mod schema;
mod sorting;
mod table;
mod table_impl;

pub(crate) mod aipsio;
pub(crate) mod lock;
pub(crate) mod storage;

pub mod demo;
pub mod table_measures;
pub mod table_quantum;
pub mod taql;

pub use concat_table::ConcatTable;
pub use indexing::ColumnsIndex;
pub use lock::{ExternalLockSync, LockMode, LockOptions, LockType};
pub use ref_table::RefTable;
pub use schema::{
    ArrayShapeContract, ColumnOptions, ColumnSchema, ColumnType, SchemaError, TableSchema,
};
pub use sorting::{TableGroup, TableIterator};
pub use storage::{DataManagerInfo, TableInfo};
pub use table::{
    ColumnBinding, ColumnCellIter, ColumnCellRef, ColumnChunkIter, DataManagerKind, EndianFormat,
    QueryResult, RecordColumnCell, RecordColumnIter, RowRange, Slicer, SortOrder, Table,
    TableError, TableKind, TableOptions,
};
