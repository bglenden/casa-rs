# Tables Closeout Inventory

Complete catalog of every gap between casa-rs and the C++ casacore Tables
module. Each item has consistent detail suitable for later grouping into
wave implementation documents.

---

## Status Legend

| Tag | Meaning |
|-----|---------|
| **IMPLEMENT** | Will be built |
| **DEFER** | Out of scope (reason given) |

---

## 1. Interop & Storage Gaps

Items from the completion plan's fixture families that remain incomplete.

### 1.2 Scalar Record Columns On-Disk

**Status:** IMPLEMENT

**C++ behaviour:** `ScalarRecordColumnData` serialises each `TableRecord`
cell into a `Vector<uChar>` via AipsIO framing, then stores it as an
indirect array in any storage manager. Reading deserialises the byte
vector back to a `TableRecord`. The column's `DataType` is `TpRecord`.

**Rust current state:** The schema type `ColumnType::Record` exists.
In-memory `record_cell()`/`set_record_cell()` work. But the storage
layer has no `TpRecord` code path — loading a C++-written table with
a record column would fail or produce wrong data.

**Work required:**
- Implement record-to-bytes serialisation using AipsIO framing (the
  `casacore-aipsio` crate already has the primitives).
- Store as indirect `Vector<uChar>` in `StManAipsIO`, `StandardStMan`,
  and `IncrementalStMan`.
- Read path: detect `TpRecord` type code, read byte vector, deserialise.

**Files to modify:**
- `crates/casacore-tables/src/storage/stman_aipsio.rs`
- `crates/casacore-tables/src/storage/standard_stman.rs`
- `crates/casacore-tables/src/storage/incremental_stman.rs`
- Possibly `crates/casacore-aipsio/src/aipsio.rs` (if new framing needed)

**Tests:**
- New `CppTableFixture::ScalarRecord` for StManAipsIO and SSM.
- C++ shim: write table with non-empty record cells, verify round-trip.
- Full 2x2 cross-matrix.
- Edge cases: empty record cell, nested sub-record, record with array field.

---

### 1.3 Undefined Scalar Cell Interop

**Status:** IMPLEMENT

**C++ behaviour:** A `ColumnDesc` with `option = ColumnDesc::Undefined`
allows scalar cells to be absent. `isDefined(rownr)` returns `False`
for unwritten cells. The storage manager tracks definedness per cell.

**Rust current state:** `ColumnOptions { undefined: true }` exists in
the schema. `is_cell_defined()` works in-memory. No interop fixture
verifies that C++-written undefined cells are correctly decoded, or
that Rust-written undefined cells round-trip through C++.

**Work required:**
- Verify that the storage read path correctly handles undefined cells
  for each manager (StManAipsIO, SSM, ISM). May already work if the
  on-disk flag byte is read correctly — needs investigation.
- Add interop fixtures to prove it.

**Files to modify:**
- Possibly `crates/casacore-tables/src/storage/stman_aipsio.rs` (if fix needed)
- `crates/casacore-test-support/src/cpp/` (new C++ shim functions)
- `crates/casacore-test-support/tests/` (new fixture tests)

**Tests:**
- New `CppTableFixture::UndefinedCell` for StManAipsIO and SSM.
- C++ shim: write table where some scalar cells are deliberately unwritten.
- 2x2 cross-matrix: verify `is_cell_defined()` returns correct results.

---

### 1.4 Column Keyword Interop (C++ Verify)

**Status:** IMPLEMENT

**C++ behaviour:** Column keywords are persisted as part of the column
descriptor in `table.dat`. Both table-level and column-level keywords
round-trip through C++.

**Rust current state:** Column keywords exist and round-trip RR (Rust
write → Rust read). But the `CppTableFixture` enum has no variant for
column-keyword verification, so RC/CR/CC cells are never tested.

**Work required:**
- Add `CppTableFixture::ColumnKeywords` (AipsIO) and
  `CppTableFixture::SsmColumnKeywords` (SSM).
- C++ shim: write and verify column keywords.

**Files to modify:**
- `crates/casacore-test-support/src/cpp/` (shim extensions)
- `crates/casacore-test-support/tests/tables_cross_matrix_stman_aipsio.rs`
- `crates/casacore-test-support/tests/tables_cross_matrix_standard_stman.rs`

**Tests:**
- Full 2x2 cross-matrix for column keywords under both managers.

---

### 1.5 Mixed Schema Fixture

**Status:** IMPLEMENT

**C++ behaviour:** A single table can contain scalar, fixed-array,
variable-array, record, and keyword columns simultaneously.

**Rust current state:** No fixture exercises all column types together
in a single table.

**Work required:**
- Define a fixture combining: Bool scalar, Int32 scalar, Float64 scalar,
  String scalar, Float32[2,3] fixed array, variable Float64 array,
  record column, table keywords, and column keywords.
- Full 2x2 for at least one storage manager.

**Files to modify:**
- `crates/casacore-test-support/src/cpp/` (new shim)
- `crates/casacore-test-support/tests/` (new test file or section)
- `crates/casacore-test-support/src/table_interop.rs` (fixture definition)

**Tests:**
- Full 2x2 cross-matrix for SSM (most general manager).

**Note:** Depends on items 1.1 and 1.2 being completed first.

---

### 1.6 Row Range + Stride Interop Fixture

**Status:** IMPLEMENT

**C++ behaviour:** `getColumnRange(Slicer(start, length, stride))`
returns only the specified rows.

**Rust current state:** `get_column_range(RowRange::with_stride(...))`
works and has unit tests, but no interop fixture writes a table to disk
and verifies strided reads against C++ expectations.

**Work required:**
- Write a table with known values, read back with various strides,
  verify correct rows returned.

**Files to modify:**
- `crates/casacore-test-support/tests/` (new test or section)

**Tests:**
- RR fixture: 20-row table, read with stride 3, verify indices.
- RC fixture if feasible (C++ shim reads with stride and verifies).

---

## 9. Table Metadata & Keywords

### 9.1 `TableInfo` — Type, SubType, Readme

**Status:** IMPLEMENT

**C++ behaviour:** `TableInfo` stores `type` (string), `subType`
(string), and `readme` (multi-line string) in `table.info` as plain
text. Predefined types include `MEASUREMENTSET`, `PAGEDIMAGE`, etc.
Both plain tables and ref tables have their own TableInfo.

**Rust current state:** `table.info` is not read or written. No
`Table::info()` or `Table::set_info()` methods.

**Work required:**
- Define `TableInfo` struct with `type_name`, `sub_type`, `readme` fields.
- Read from `table.info` during `Table::open()`.
- Write to `table.info` during `Table::save()`.
- Public API: `Table::info() -> &TableInfo`,
  `Table::set_info(info: TableInfo)`.
- File format: `Type = X\nSubType = Y\n\nreadme lines...`

**Files to modify:**
- `crates/casacore-tables/src/table.rs` (API surface)
- `crates/casacore-tables/src/table_impl.rs` (implementation)
- `crates/casacore-tables/src/storage/table_control.rs` (persistence)

**Tests:**
- Set info, save, reopen → info preserved.
- C++ interop: Rust-written table.info readable by C++.
- Predefined type enum for common types (MeasurementSet, etc.).

---

### 9.2 Subtable References in Keywords

**Status:** IMPLEMENT

**C++ behaviour:** A `TableRecord` field can hold a `Table` reference
via `TableKeyword`. The referenced table is lazily opened on access.
The keyword value is stored as a relative path string. This is how
MeasurementSets link to their subtables (e.g. `ANTENNA`, `FIELD`).

**Rust current state:** `RecordValue` can contain scalars, arrays, and
nested records. It cannot contain table references. Keywords that are
subtable pointers in C++ are either skipped or read as strings.

**Work required:**
- Add `Value::Table(PathBuf)` variant or equivalent to represent a
  table reference in the keyword set.
- On read: detect the table-reference type code in the keyword record,
  store as a path.
- On write: emit the correct type code and relative path.
- Lazy-open accessor: `Table::keyword_as_table(name) -> Result<Table>`.

**Files to modify:**
- `crates/casacore-types/src/lib.rs` (Value variant)
- `crates/casacore-tables/src/table.rs` (accessor)
- `crates/casacore-tables/src/storage/table_control.rs` (read/write)

**Tests:**
- Create table with subtable keyword, save, reopen → path preserved.
- C++ interop: MeasurementSet subtable references readable.

---

## 10. Table Operations

### 10.1 `Table::flush()`

**Status:** IMPLEMENT

**C++ behaviour:** `flush(fsync, recursive)` writes pending buffered
data to disk. `fsync=true` calls `fsync(2)`. `recursive=true` also
flushes subtables.

**Rust current state:** No explicit `flush()`. Data is written during
`save()` or implicitly when unlocking.

**Work required:**
- Add `Table::flush(fsync: bool)`.
- For disk-backed tables, write any dirty in-memory state to storage
  files without requiring a full `save()`.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/table_impl.rs`

**Tests:**
- Modify cell, flush, open in another handle → change visible.

---

### 10.2 `Table::resync()`

**Status:** IMPLEMENT

**C++ behaviour:** Re-reads the on-disk table state into the in-memory
representation. Used with `NoReadLocking` modes where changes from
other processes aren't detected automatically.

**Rust current state:** Not implemented.

**Work required:**
- Re-read `table.dat` and storage manager metadata.
- Invalidate any cached column data.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/table_impl.rs`

**Tests:**
- Process A writes rows; process B calls `resync()` and sees new rows.

---

### 10.3 `Table::reopen_rw()`

**Status:** IMPLEMENT

**C++ behaviour:** Upgrades a read-only table handle to read-write.
Throws if the table files are not writable.

**Rust current state:** Not implemented. Tables are opened either
read-only or read-write at `open()` time.

**Work required:**
- Check file permissions, re-acquire lock as write lock if needed.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Open read-only, `reopen_rw()`, modify cell, save → works.
- `reopen_rw()` on a read-only filesystem → error.

---

### 10.4 `markForDelete()` / `unmarkForDelete()`

**Status:** IMPLEMENT

**C++ behaviour:** Schedules a table for deletion when the last
reference is dropped. Used by `Table::Scratch` tables.

**Rust current state:** Not implemented.

**Work required:**
- Add `Table::mark_for_delete()` / `Table::unmark_for_delete()`.
- On `Drop`, if marked, delete the table directory.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Create, mark_for_delete, drop → directory gone.
- Create, mark, unmark, drop → directory remains.

---

### 10.5 Table Set Algebra

**Status:** IMPLEMENT

**C++ behaviour:** `table1 & table2` (intersection), `table1 | table2`
(union), `table1 - table2` (difference), `table1 ^ table2`
(symmetric difference), `!table1` (complement relative to root).
Operands must share the same root table.

**Rust current state:** Not implemented. `Table` has no set-algebra
operators.

**Work required:**
- Implement as methods on `Table` or `RefTable` returning a new `RefTable`.
- Operate on row-index sets.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/ref_table.rs`

**Tests:**
- Intersection of two overlapping selections → common rows only.
- Union → all unique rows from both.
- Complement of a selection → all other rows.

---

### 10.6 `Table::dataManagerInfo()`

**Status:** IMPLEMENT

**C++ behaviour:** Returns a `Record` describing each data manager
instance: `TYPE`, `NAME`, `COLUMNS`, plus manager-specific fields
like `BUCKETSIZE`.

**Rust current state:** Not exposed as public API. Storage manager
metadata is crate-internal.

**Work required:**
- Add `Table::data_manager_info() -> RecordValue`.
- Populate from the internal `CompositeStorage` registry.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/storage/mod.rs`

**Tests:**
- Open a table, inspect data_manager_info → correct types and column lists.

---

## 11. Locking Extensions

### 11.1 Additional Lock Modes

**Status:** IMPLEMENT

**C++ behaviour:** `TableLock` has 8 modes: `PermanentLocking`,
`PermanentLockingWait`, `AutoLocking`, `AutoNoReadLocking`,
`UserLocking`, `UserNoReadLocking`, `NoLocking`, `DefaultLocking`.

**Rust current state:** `LockMode` has 5: `PermanentLocking`,
`PermanentLockingWait`, `AutoLocking`, `UserLocking`, `NoLocking`.

**Work required:**
- Add `AutoNoReadLocking` — like `AutoLocking` but no read lock needed.
- Add `UserNoReadLocking` — like `UserLocking` but no read lock needed.
- Add `DefaultLocking` — use table's existing mode or `AutoLocking`.

**Files to modify:**
- `crates/casacore-tables/src/lock/mod.rs`
- `crates/casacore-tables/src/lock/lock_file.rs`
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Open with `AutoNoReadLocking`, read without acquiring read lock.
- `DefaultLocking` inherits from existing table.

---

### 11.2 `ExternalLockSync`

**Status:** IMPLEMENT

**C++ behaviour:** A callback interface for external tools that need to
coordinate locking with the table system. The table acquires the
external lock before the internal file lock.

**Rust current state:** Not implemented.

**Work required:**
- Define trait: `ExternalLockSync { fn lock(&self); fn unlock(&self); fn has_lock(&self) -> bool; }`
- `Table::set_external_lock_sync(Box<dyn ExternalLockSync>)`.
- Hook into lock acquire/release path.

**Files to modify:**
- `crates/casacore-tables/src/lock/mod.rs`
- `crates/casacore-tables/src/lock/lock_file.rs`
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Set mock ExternalLockSync, verify lock/unlock called in correct order.
- No external sync set → zero overhead (opt-in).

---

## 12. Iteration & Indexing

### 12.1 `TableIterator` Descending Sort

**Status:** IMPLEMENT

**C++ behaviour:** `TableIterator` accepts per-column `Order` (Ascending
or Descending). Also supports `NoSort` option (assumes data is
pre-sorted) and custom `BaseCompare` objects for non-standard grouping.

**Rust current state:** `Table::iter_groups()` sorts ascending only.
No per-column order. No `NoSort` option.

**Work required:**
- Accept per-column sort orders in `iter_groups()`.
- Add `NoSort` option for pre-sorted data.

**Files to modify:**
- `crates/casacore-tables/src/sorting.rs`
- `crates/casacore-tables/src/table.rs`

**Tests:**
- `iter_groups` with descending order → groups in descending key order.
- `NoSort` on already-sorted data → same result, faster.

---

### 12.2 `ColumnsIndexArray`

**Status:** IMPLEMENT

**C++ behaviour:** Like `ColumnsIndex` but for a single **array**
column. Each cell can contain multiple key values, so one row can
match multiple keys. Supported types: uChar, Short, Int, uInt,
Int64, String.

**Rust current state:** Not implemented. `ColumnsIndex` only handles
scalar columns.

**Work required:**
- New struct `ColumnsIndexArray` that builds a sorted index from
  all elements of an array column.
- Same lookup API as `ColumnsIndex` but with `unique` flag to
  deduplicate results.

**Files to modify:**
- `crates/casacore-tables/src/indexing.rs`

**Tests:**
- Array column with values [1,2,3] in row 0, [2,3,4] in row 1.
  Lookup key=2 → rows [0, 1]. With unique=true → [0, 1] deduplicated.
- Range lookup on array index.

---

### 12.3 `TableIterator` Custom Compare Objects

**Status:** DEFER

**Reason:** Custom compare objects (e.g. `CompareIntervalReal` for time
binning) require runtime polymorphism. The standard ascending/descending
grouping covers the vast majority of use cases. Can be added later if
needed by a specific downstream application.

---

## 13. Row-Level Access

### 13.1 `TableRow` / `ROTableRow`

**Status:** IMPLEMENT

**C++ behaviour:** `ROTableRow` reads an entire row into a `TableRecord`
by row number. `TableRow` adds write access. Can select/exclude specific
columns. Caches last row. `getDefined()` tracks per-column definedness.

**Rust current state:** `Table::row(index) -> Option<&RecordValue>` and
`row_mut()` exist but they return the in-memory row directly. For
disk-backed tables there is no `TableRow`-like interface that reads a
specific row from storage into a record.

**Work required:**
- For in-memory tables, the existing API is sufficient.
- For disk-backed tables opened without full materialisation (future),
  a `TableRow` adapter would be needed.
- Lower priority since current Rust tables materialise all rows on open.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Read row 5 of a 50-row disk table → correct record.

---

### 13.2 `RowCopier`

**Status:** IMPLEMENT

**C++ behaviour:** Copies selected columns from a row in one table to a
row in another. Supports column name remapping. Types must match.

**Rust current state:** Not implemented. Users must manually copy cell
by cell.

**Work required:**
- `RowCopier::new(out: &mut Table, input: &Table)` — all matching columns.
- `RowCopier::new_mapped(out, input, out_names, in_names)` — explicit mapping.
- `copy(to_row, from_row)` method.

**Files to modify:**
- `crates/casacore-tables/src/table.rs` (or new file `row_copier.rs`)

**Tests:**
- Copy row 3 from table A to row 0 of table B → values match.
- Column name remapping works.
- Type mismatch → error.

---

## 14. Copy Utilities

### 14.1 `TableCopy` Static Methods

**Status:** IMPLEMENT (subset)

**C++ behaviour:** `TableCopy` provides: `makeEmptyTable()`,
`makeEmptyMemoryTable()`, `copyRows()`, `copyInfo()`, `copySubTables()`,
`cloneColumn()`, `copyColumnData()`, `fillArrayColumn()`,
`fillColumnData()`.

**Rust current state:** `Table::deep_copy()` and `shallow_copy()` exist.
No fine-grained copy utilities.

**Work required (implement):**
- `Table::copy_rows(out, input, start_out, start_in, nrow)`.
- `Table::copy_info(out, input)` — copies TableInfo (after item 10.1).
- `Table::fill_column(column, value)` — fill scalar or array column
  with constant.

**Deferred:**
- `cloneColumn`, `cloneColumnTyped` — specialised; low priority.
- `copySubTables` — depends on subtable keyword support (item 10.2).

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- `copy_rows`: copy 5 rows from table A to table B at offset 10.
- `fill_column`: fill Float64 column with 0.0 → all cells are 0.0.

---

## 15. Cell & Column Slicing

### 15.1 `Slicer` Type and Cell-Level Array Slicing

**Status:** IMPLEMENT

**C++ behaviour:** `Slicer(start, end, stride)` specifies a rectangular
sub-region of an N-dimensional array. `ArrayColumn::getSlice(row, slicer)`
reads only the specified sub-array. `putSlice()` writes a sub-region.
Negative indices are supported (Python-style: -1 = last element).

**Rust current state:** No `Slicer` type. No cell-level slice API.
Users must read the entire array and slice with `ndarray`.

**Work required:**
- Define `Slicer` struct: `start: Vec<isize>`, `end: Vec<isize>`,
  `stride: Vec<usize>`.
- `Table::get_cell_slice(col, row, slicer) -> Result<ArrayValue>`.
- `Table::put_cell_slice(col, row, slicer, data) -> Result<()>`.
- For `TiledStMan`, slicing can be pushed down to tile-level I/O.
- For other managers, read full array then slice in memory.

**Files to modify:**
- `crates/casacore-tables/src/table.rs` (API)
- `crates/casacore-tables/src/table_impl.rs` (implementation)
- `crates/casacore-tables/src/storage/mod.rs` (Slicer type)
- `crates/casacore-tables/src/storage/tiled_stman.rs` (optimised path)
- `crates/casacore-tables/src/storage/standard_stman.rs` (full-read path)

**Tests:**
- Read slice [1:3, 0:2] of a [4,4] array cell → [2,2] result.
- Write slice, read back → only slice modified.
- Negative indices: [-1] → last element.
- Stride: [0:4:2] → every other element.
- C++ interop: sliced read matches C++ `getSlice()`.

---

### 15.2 `getColumnRange` with Array Slicing

**Status:** IMPLEMENT

**C++ behaviour:** `getColumnRange(rowSlicer, arraySlicer)` combines
row-range selection with per-cell array slicing in a single operation.

**Rust current state:** `get_column_range()` selects rows but reads
full cells. No combined row+array slicing.

**Work required:**
- Overload or parameter extension for `get_column_range` that accepts
  both a `RowRange` and a `Slicer`.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Read rows 0-5 with array slice [0:2] → 6 cells, each sliced.

---

## 16. Virtual Column Engines

### 16.2 `MappedArrayEngine`

**Status:** DEFER

**Reason:** Pure type-cast between numeric array types. Relatively niche
use case (e.g. exposing double as float). Can be added if a downstream
user needs it. The `ScaledArrayEngine` with scale=1, offset=0 can
approximate this for float types.

---

### 16.3 `RetypedArrayEngine`

**Status:** DEFER

**Reason:** Requires the virtual type to implement a complex static
interface (`shape()`, `set()`, `get()`, `newCopyInfo()`) that doesn't
map cleanly to Rust traits. Used primarily for specialised types like
`StokesVector`. Very niche; defer until an actual downstream need arises.

---

## 17. Storage Options

### 17.1 `StorageOption` (MultiFile, MultiHDF5)

**Status:** DEFER

**Reason:** `MultiFile` packs all storage manager files into a single OS
file (useful for Lustre). `MultiHDF5` does the same via HDF5. Both are
performance/deployment optimisations, not correctness requirements.
The traditional `SepFile` mode (each manager has its own files) is
the default and what casa-rs currently uses. Can be added later for
HPC deployments.

---

### 17.2 `TSMOption` (MMap, Cache, Buffer)

**Status:** DEFER

**Reason:** Controls how TiledStMan accesses data (memory-mapped vs.
buffered vs. cached). Currently casa-rs uses simple buffered I/O.
Performance optimisation; not needed for correctness or API parity.

---

## 19. Record Expression Filtering

### 19.1 `RecordGram` / `RecordExpr`

**Status:** DEFER

**Reason:** Allows evaluating TaQL expressions over standalone `Record`
objects (not tables). Used internally by `LogFilterTaql` and some
specialised tools. Low priority for table parity; can be added if
LogTables support (item 24) is implemented.

---

## 20. Table Construction

### 20.1 `SetupNewTable` Builder Pattern

**Status:** DEFER

**Reason:** In C++, `SetupNewTable` is a builder that binds columns to
data managers before passing to the `Table` constructor. In Rust,
`TableOptions` + `save_with_bindings()` + `ColumnBinding` already
provide equivalent functionality with a more Rust-idiomatic API.
No additional work needed unless exact C++ API mirroring is desired.

---

## 21. Diagnostic & Introspection

### 21.1 `Table::show_structure()`

**Status:** IMPLEMENT

**C++ behaviour:** Writes a human-readable dump of the table's structure
to a stream: column names, types, which data manager serves each
column, optionally subtable names.

**Rust current state:** Not implemented. Users must inspect the schema
and data_manager_info manually.

**Work required:**
- Add `Table::show_structure(writer: &mut impl Write)`.
- Format column info, data manager bindings, shape info.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Output contains column names and types.
- Output matches expected format for a known table.

---

### 21.2 `Table::show_keyword_sets()`

**Status:** IMPLEMENT

**C++ behaviour:** Prints table-level and column-level keyword sets.

**Rust current state:** Not implemented.

**Work required:**
- Add `Table::show_keywords(writer: &mut impl Write)`.
- Recursively print keyword records.

**Files to modify:**
- `crates/casacore-tables/src/table.rs`

**Tests:**
- Output includes table keyword names and values.

---

## 22. Command-Line Tools

### 22.2 `showtableinfo` — Table Structure Viewer

**Status:** IMPLEMENT

**C++ equivalent:** `tables/apps/showtableinfo.cc`.

**Work required:**
- New binary: open table, call `show_structure()` and `show_keywords()`.
- Display row count, column list, data manager info.

**Tests:**
- Run against a test table, verify correct output.

---

### 22.3 `showtablelock` — Lock Status Viewer

**Status:** DEFER

**Reason:** Niche diagnostic tool. Can be added if multi-process locking
becomes a common deployment scenario.

---

### 22.4 `tablefromascii` — ASCII Import

**Status:** DEFER — depends on item 26 (ReadAsciiTable).

---

### 22.5 `lsmf` / `tomf` — MultiFile Tools

**Status:** DEFER — depends on MultiFile support (item 18.1).

---

## 23. Logging via Tables

### 23.1 `TableLogSink` / `LogFilterTaql`

**Status:** DEFER

**Reason:** The LogTables subsystem persists log messages as table rows
and filters them with TaQL expressions. This is primarily used by CASA
applications (imaging, calibration) rather than the table system itself.
Belongs in a higher-level crate (e.g. `casacore-logging`) if needed.

---

## 24. Column-as-Vector Arithmetic

### 24.1 `TableVector<T>`

**Status:** DEFER

**Reason:** `TableVector` treats a scalar column as a math vector,
supporting element-wise arithmetic and logical operations. In Rust,
users can achieve the same result by reading a column into an `ndarray`
and using its arithmetic operators. A dedicated wrapper adds minimal
value over `ndarray` integration.

---

## 25. ASCII Table Import

### 25.1 `ReadAsciiTable`

**Status:** DEFER

**Reason:** Creates a casacore table from whitespace/CSV files. Useful
but not part of core table semantics. Can be implemented as a utility
function in a later convenience crate. The C++ implementation supports
auto-header inference, DMS/HMS angle parsing, and keyword blocks —
significant scope for a feature with niche usage.

---

## 26. Observatory-Specific & Parallel Storage Managers

### 26.1 `Adios2StMan`

**Status:** DEFER

**Reason:** MPI-parallel I/O storage manager. Requires ADIOS2 library
dependency and MPI runtime. Not needed for single-process table access.

---

### 26.2 `AlternateMans` (AntennaPairStMan, SiscoStMan, etc.)

**Status:** DEFER

**Reason:** Observatory-specific storage managers for specialised data
patterns. Should be contributed by downstream users if needed.

---

### 26.3 `Dysco` Lossy Compression

**Status:** DEFER

**Reason:** Lossy compression for radio astronomy visibility data.
Significant standalone scope. Should be a separate crate if needed.

---

