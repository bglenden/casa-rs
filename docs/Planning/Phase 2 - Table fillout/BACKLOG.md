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

## 12. Iteration & Indexing


### 12.3 `TableIterator` Custom Compare Objects

**Status:** DEFER

**Reason:** Custom compare objects (e.g. `CompareIntervalReal` for time
binning) require runtime polymorphism. The standard ascending/descending
grouping covers the vast majority of use cases. Can be added later if
needed by a specific downstream application.

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

## 22. Command-Line Tools


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

