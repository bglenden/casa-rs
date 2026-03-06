# Wave 2 - TableQuantum keyword semantics and APIs

## Origin

- Backlog items: 2.1, 2.2, 2.3, 2.4.

## Goal

- Implement casacore-compatible `QuantumUnits`/`VariableUnits` behavior for
  scalar and array table columns, with typed column accessors.

## Non-goals

- `MEASINFO`/TableMeasures descriptor handling (Wave 6).
- Measure frame conversions (Waves 3-4).
- Complex-valued quantum columns (rare; can be added later).

## Scope

### Read path

- Detect quantum columns by inspecting column keywords for `QuantumUnits` or
  `VariableUnits`.
- Reconstruct `TableQuantumDesc` from persisted column keywords.
- Read fixed-unit columns: value + stored unit → `Quantity`.
- Read variable-unit columns: value from data column + unit from units column → `Quantity`.
- Support per-element variable units (array String units column) and per-row
  variable units (scalar String units column).
- On-read unit conversion: read with an explicit target unit.

### Write path

- Write `QuantumUnits` keyword (Vector<String>) for fixed-unit columns.
- Write `VariableUnits` keyword (String) for variable-unit columns.
- Validate that a variable-units column exists, is String-typed, and has
  compatible shape (scalar for per-row, array for per-element).
- Auto-create the units column if it doesn't exist yet (matching C++ behavior).

### API/docs/demo

- `TableQuantumDesc` — descriptor type with `has_quanta()`, `is_unit_variable()`,
  `column_name()`, `unit_column_name()`, `units()`, `write()`, `reconstruct()`.
- `ScalarQuantColumn<T>` — read/write `Quantity` values from scalar columns.
- `ArrayQuantColumn<T>` — read/write arrays of `Quantity` from array columns.
- Convenience methods on `Table`: `has_quantum_column()`, `quantum_desc()`.
- Demo program equivalent to key parts of C++ `tTableQuantum`.
- Rustdoc at C++-doxygen-comparable detail for all public types/methods.

## Dependencies

- Wave 1 completed (quanta module with Unit, Quantity, parsing).

## Ordering constraints

- Must run after Wave 1 ✓
- Must run before Wave 6 (TableMeasures descriptors).

## C++ Reference Points

| C++ Class | C++ File | Rust Equivalent |
|-----------|----------|-----------------|
| `TableQuantumDesc` | `measures/TableMeasures/TableQuantumDesc.{h,cc}` | `table_quantum::TableQuantumDesc` |
| `ScalarQuantColumn<T>` | `measures/TableMeasures/ScalarQuantColumn.{h,tcc}` | `table_quantum::ScalarQuantColumn` |
| `ArrayQuantColumn<T>` | `measures/TableMeasures/ArrayQuantColumn.{h,tcc}` | `table_quantum::ArrayQuantColumn` |
| `tTableQuantum` | `measures/TableMeasures/test/tTableQuantum.cc` | `tests/table_quantum.rs` |

## On-disk Keyword Format

Keywords are stored at column level (in column keyword records, not table keywords).

### Fixed units

Column keyword record contains:
```
QuantumUnits: Vector<String>   // e.g. ["deg"] or ["MHz", "GHz"]
```

### Variable units

Column keyword record contains:
```
VariableUnits: String          // name of the String column holding units
```

The two keywords are mutually exclusive.

## Files to Modify

| File | Change |
|------|--------|
| `crates/casacore-tables/src/table_quantum.rs` | **New**: `TableQuantumDesc`, `ScalarQuantColumn`, `ArrayQuantColumn` |
| `crates/casacore-tables/src/lib.rs` | Add `pub mod table_quantum` |
| `crates/casacore-tables/src/table.rs` | Add `has_quantum_column()`, `quantum_desc()` convenience methods |
| `crates/casacore-test-support/src/cpp/casacore_cpp_table_quantum.cpp` | **New**: C++ shim for interop |
| `crates/casacore-test-support/src/table_interop.rs` | Add quantum column helpers |
| `crates/casacore-test-support/build.rs` | Register new C++ shim file |

## Files to Create

| File | Purpose |
|------|---------|
| `crates/casacore-tables/src/table_quantum.rs` | Core implementation |
| `crates/casacore-tables/tests/table_quantum.rs` | Rust-only unit tests |
| `crates/casacore-test-support/tests/table_quantum_interop.rs` | 2×2 interop tests |
| `crates/casacore-test-support/src/cpp/casacore_cpp_table_quantum.cpp` | C++ shim |

## Implementation Details

### 1. `TableQuantumDesc`

```rust
pub struct TableQuantumDesc {
    column_name: String,
    units: Vec<String>,           // empty if variable
    units_column_name: Option<String>,  // Some if variable
}

impl TableQuantumDesc {
    /// Create descriptor with fixed unit(s).
    pub fn with_unit(column: &str, unit: &str) -> Self;
    pub fn with_units(column: &str, units: &[&str]) -> Self;

    /// Create descriptor with variable units stored in another column.
    pub fn with_variable_units(column: &str, units_column: &str) -> Self;

    /// Check if a column has quantum keywords.
    pub fn has_quanta(table: &Table, column: &str) -> bool;

    /// Reconstruct descriptor from column keywords.
    pub fn reconstruct(table: &Table, column: &str) -> Option<Self>;

    /// Write keywords to a table's column.
    pub fn write(&self, table: &mut Table) -> Result<(), TableError>;

    pub fn column_name(&self) -> &str;
    pub fn is_unit_variable(&self) -> bool;
    pub fn unit_column_name(&self) -> Option<&str>;
    pub fn units(&self) -> &[String];
}
```

### 2. `ScalarQuantColumn`

```rust
pub struct ScalarQuantColumn<'a> {
    table: &'a Table,
    column_name: String,
    desc: TableQuantumDesc,
    convert_unit: Option<Unit>,  // optional on-read conversion
}

impl<'a> ScalarQuantColumn<'a> {
    pub fn new(table: &'a Table, column: &str) -> Result<Self, TableError>;
    pub fn with_unit(table: &'a Table, column: &str, unit: &str) -> Result<Self, TableError>;
    pub fn get(&self, row: usize) -> Result<Quantity, TableError>;
    pub fn get_in(&self, row: usize, unit: &str) -> Result<Quantity, TableError>;
    pub fn is_unit_variable(&self) -> bool;
    pub fn units(&self) -> &[String];
}

pub struct ScalarQuantColumnMut<'a> {
    table: &'a mut Table,
    column_name: String,
    desc: TableQuantumDesc,
}

impl<'a> ScalarQuantColumnMut<'a> {
    pub fn new(table: &'a mut Table, column: &str) -> Result<Self, TableError>;
    pub fn put(&mut self, row: usize, value: &Quantity) -> Result<(), TableError>;
}
```

### 3. `ArrayQuantColumn` (analogous structure)

Similar to `ScalarQuantColumn` but works with arrays of `Quantity`.
Three modes for variable units:
- Per-element: units column is an Array<String> column with same shape
- Per-row: units column is a Scalar<String> column (one unit per row)
- Fixed: units stored in keyword

### 4. C++ Interop Shim

```cpp
// Create a table with quantum columns (fixed + variable), write data
int table_quantum_create_cpp(const char* path);

// Read quantum values back and return them for Rust comparison
int table_quantum_read_cpp(const char* path, double* values_out,
    char* units_out, int buf_len);

// Verify a Rust-written quantum table from C++
int table_quantum_verify_cpp(const char* path, int* ok_out);
```

### 5. Keyword I/O

`QuantumUnits` is written as `Value::Array(ArrayValue::String(...))` in the
column's keyword record. `VariableUnits` is written as
`Value::Scalar(ScalarValue::String(...))`.

Reading checks `column_keywords("col").get("QuantumUnits")` or
`.get("VariableUnits")` — the existing `RecordValue` API handles this directly
with no new serialization code needed.

## Implementation Order

1. `TableQuantumDesc` — keyword read/write, `has_quanta`, `reconstruct`
2. Rust-only tests for `TableQuantumDesc` (round-trip: write → read back)
3. `ScalarQuantColumn` / `ScalarQuantColumnMut` — read/write with fixed units
4. Variable-unit support in `ScalarQuantColumn`
5. `ArrayQuantColumn` / `ArrayQuantColumnMut` — fixed + variable units
6. C++ shim for interop (create/read/verify)
7. 2×2 interop tests (RR, RC, CR, CC)
8. Performance comparison: Rust vs C++ for quantum column read/write throughput
9. Demo program
10. Rustdoc pass

## Test Plan

### Rust-only tests (`tests/table_quantum.rs`)

- Round-trip `TableQuantumDesc` with fixed unit(s)
- Round-trip `TableQuantumDesc` with variable units
- `has_quanta` returns true/false correctly
- `ScalarQuantColumn` read/write with fixed "deg" unit
- `ScalarQuantColumn` read with on-read conversion ("deg" → "rad")
- `ScalarQuantColumn` with variable units (different unit per row)
- `ArrayQuantColumn` with fixed single unit
- `ArrayQuantColumn` with fixed multiple units (one per axis)
- `ArrayQuantColumn` with per-element variable units
- `ArrayQuantColumn` with per-row variable units (scalar String column)
- Error: variable units column doesn't exist
- Error: variable units column isn't String type
- Edge: empty array put/get
- Edge: column with no quantum keywords

### 2×2 Interop tests (`tests/table_quantum_interop.rs`)

All guarded with `#![cfg(has_casacore_cpp)]`.

**RR** (Rust write → Rust read):
- Fixed-unit scalar, variable-unit scalar
- Fixed-unit array, variable-unit array (per-element and per-row)

**RC** (Rust write → C++ read):
- C++ shim opens Rust-written table, verifies quantum values and units

**CR** (C++ write → Rust read):
- C++ shim creates table with quantum columns, Rust reads and validates

**CC** (C++ write → C++ read):
- Sanity baseline: C++ shim round-trips internally

### Performance test (`tests/table_quantum_perf_vs_cpp.rs`)

**Lessons from Wave 1**: Measure what you intend. Conversion benchmarks must
pre-parse units outside the timing loop to avoid measuring parse overhead.
Compare equivalent operations — if C++ constructs objects once, Rust must too.

- Workload: 10,000 rows × scalar quantum column, read all values
- Workload: 10,000 rows × array quantum column (shape [4]), read all values
- Both sides pre-construct column accessors before timing loop
- Report ratio, warn if Rust > 2× slower
- Use `--release` for meaningful comparison

## Lessons Learned from Wave 1

Applied to this wave's plan:

1. **Benchmark fairness**: C++ and Rust must measure equivalent operations.
   The wave 1 conversion benchmark initially re-parsed unit strings every
   iteration (Rust) while C++ constructed objects once — a 12× apparent gap
   that was actually 0.16× (Rust faster) when measured correctly. All wave 2
   benchmarks will pre-construct column accessors outside the timing loop.

2. **Parser edge cases surface during interop**: Wave 1 discovered two parser
   bugs (leading `/`, digit-suffixed unit names like `S0`) only when
   cross-validating all 21 constants against C++. Wave 2 should test with
   real-world unit strings from astronomical tables (e.g. `Jy/beam`,
   `km/s`, `deg`, `Hz`) and stress unusual cases.

3. **Lock-free hot paths**: The wave 1 registry lookup was slow because it
   acquired `RwLock` reads on every call. Wave 2 column accessors should
   cache the resolved `Unit` and `UnitVal` at construction time, not on
   every `get()`/`put()` call.

4. **Tolerance for derived constants**: Some C++ constants are computed
   differently (e.g. k = R/NA) vs Rust (direct CODATA value). Interop
   tests should use appropriate tolerances and document why.

5. **Immutable-first lookup order**: Check immutable/cached data before
   acquiring locks. The wave 2 `ScalarQuantColumn` should resolve its
   unit metadata once at construction, then `get()` is a simple
   multiply+divide with no locking.

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed: keywords use standard
      AipsIO Record serialization, already handled by existing table I/O.
- [x] Data-table dependency reviewed: N/A (no measures data needed).
- [x] Performance workload defined (10k row scalar + array quantum read).
- [x] Non-goals documented.

## Implementation checklist

- [x] `TableQuantumDesc` with `has_quanta`, `reconstruct`, `write`.
- [x] `ScalarQuantColumn` / `ScalarQuantColumnMut` for fixed + variable units.
- [x] `ArrayQuantColumn` / `ArrayQuantColumnMut` for fixed + variable units.
- [x] Rust-only round-trip tests.
- [x] C++ shim for create/read/verify.
- [x] 2×2 interop tests (RR/RC/CR/CC).
- [x] Performance comparison vs C++.
- [x] Demo program.
- [x] Rustdoc pass.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.
- [x] Run `casa-review` skill and resolve any issues it flags.

## Results

- Date: 2026-03-05
- Commit: 43a2dd8
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (32 new tests, 1 pre-existing flaky UDF test)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: PASS (scalar fixed, scalar variable, array fixed, array variable per-row, array variable per-element)
  - RC: PASS (scalar values + units, quantum keyword verification)
  - CR: PASS (scalar fixed, scalar variable, array fixed, array variable per-row)
  - CC: PASS (scalar fixed + variable roundtrip)
- Performance (release, 10k rows × 10 iterations):
  - Scalar read: Rust 16.9 ns/op, C++ 14.1 ns/op, ratio 1.19x
  - Array read (4-element): Rust 103.8 ns/op, C++ 446.0 ns/op, ratio 0.23x (Rust 4.3x faster)
- Skips/blockers/follow-ups:
  - tQuantumHolder (Quantity serialization for transport) not implemented — out of scope for this wave
  - No standalone tUnit/tQuantum example in casacore-types — low priority, tests cover the functionality

## Lessons learned

- C++ `ScalarQuantColumn`/`ArrayQuantColumn` constructors get the variable units column name
  from the persisted quantum keywords automatically. Do NOT pass the units column name as a
  third constructor argument — it will be interpreted as a `Unit` string for on-read conversion.
- C++ `TableQuantumDesc` constructor for variable units (`const String& unitCol`) is ambiguous
  with the `Unit` constructor when passing a string literal. Use `const Char*` variable or
  explicit `String()` cast.
- C++ default storage manager is `StandardStMan`; Rust `Table::open` handles both AipsIO and
  StandardStMan, but interop tests should use `StManAipsIO` for the C++ shim to ensure
  consistent behavior with Rust's default.
- Rust's array quantum read is ~4x faster than C++ because the `ArrayQuantColumn` avoids
  allocating individual `Quantum<Double>` objects per element — it reads the raw array once
  and pairs values with a single cached `Unit`.
