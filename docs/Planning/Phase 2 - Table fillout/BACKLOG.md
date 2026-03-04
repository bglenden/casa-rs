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

## 2. TaQL Expression Syntax

Foundational syntax extensions that many later items depend on.

### 2.1 Regex Literal Syntax

**Status:** IMPLEMENT

**C++ behaviour:** TaQL supports `p/pattern/flags` for POSIX-style regex
and `m/pattern/flags` for Perl-style. The `~=` operator matches a value
against a regex. The `regex()` function also creates regex objects.

**Rust current state:** The `regex()` function exists and works. The
literal `p/…/` syntax is not lexed.

**Work required:**
- Lexer: new `RegexLiteral` token recognising `p/…/` and `m/…/`.
- Parser: parse into `Expr::Regex { pattern, flags }`.
- Eval: compile with `regex` crate, use in `~=` (already partially
  supported via the `regex()` function path).

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs`
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

**Tests:**
- `WHERE name ~= p/SRC_0[0-3]/` matches rows 0–3.
- `WHERE name ~= p/src/i` case-insensitive match.
- Error on malformed regex.

---

### 2.2 Array Indexing & Slicing

**Status:** IMPLEMENT

**C++ behaviour:** `col[i]` extracts the i-th element. `col[i,j]`
indexes a 2-D array. `col[start:end]` and `col[start:end:step]` extract
sub-arrays. TaQL uses 1-based Glish-style indexing by default.

**Rust current state:** Not implemented. Array values can only be
accessed as whole cells.

**Work required:**
- Parser: parse postfix `[…]` after an expression as
  `Expr::ArrayIndex { expr, indices }` or
  `Expr::ArraySlice { expr, slices }`.
- Eval: extract element (returns scalar) or sub-array (returns array)
  from `TableValue::Array` using `ndarray` slicing.
- Handle 1-based default indexing (see item 19.1 for TaQL styles).

**Files to modify:**
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

**Tests:**
- `SELECT arr_col[1]` extracts first element.
- `SELECT arr_col[1:3]` extracts sub-array.
- `SELECT arr_col[1:5:2]` extracts with stride.
- `SELECT arr_col[1,2]` indexes 2-D.
- Out-of-bounds returns error.

---

### 2.3 Bitwise Operators

**Status:** IMPLEMENT

**C++ behaviour:** `&` (AND), `|` (OR), `^` (XOR), `~` (NOT) operate on
integer values. Distinct from logical `&&`/`||`/`!`.

**Rust current state:** `~` (BitNot) is lexed as a unary operator.
`&` and `|` are lexed only as `&&`/`||` logical operators. No `^`.

**Work required:**
- Lexer: distinguish single `&`/`|` (bitwise) from `&&`/`||` (logical).
  Add `^` token.
- Parser: new precedence levels between logical and comparison.
- Eval: integer-only; type-check error on float/string/complex.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs`
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

**Tests:**
- `WHERE (flags & 0x04) != 0` — bitwise AND mask.
- `CALC 0xFF ^ 0x0F` — XOR.
- `CALC ~0` — bitwise NOT.
- Type error on `1.5 & 2`.

---

### 2.4 `IN` with Set/Range Syntax

**Status:** IMPLEMENT

**C++ behaviour:** `col IN [1,2,3]` for discrete sets.
`col IN [1:10]` for inclusive range. `col IN [1:10:2]` for strided
range. Negation via `NOT IN`.

**Rust current state:** `IN (1,2,3)` with parenthesised comma list
works. Square-bracket set and range syntax not supported.

**Work required:**
- Parser: parse `IN [...]` with range `:` syntax inside brackets.
- Eval: expand range to membership test; for discrete set use hash
  lookup.

**Files to modify:**
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

**Tests:**
- `WHERE id IN [1:5]` — range [1,5] inclusive.
- `WHERE id IN [1,3,5]` — discrete set with brackets.
- `WHERE id NOT IN [1:3]` — negated range.

---

### 2.5 `CASE WHEN … THEN … ELSE … END`

**Status:** IMPLEMENT

**C++ behaviour:** Standard SQL conditional expression. Multi-branch.
```
CASE WHEN flux > 10 THEN 'bright'
     WHEN flux > 1  THEN 'medium'
     ELSE 'faint' END
```

**Rust current state:** Not implemented.

**Work required:**
- AST: `Expr::Case { branches: Vec<(Expr, Expr)>, else_expr: Option<Box<Expr>> }`.
- Parser: parse `CASE` keyword, collect `WHEN`/`THEN` pairs, optional
  `ELSE`, expect `END`.
- Eval: evaluate branches in order, return first match.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs` (CASE, WHEN, THEN, ELSE, END tokens)
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

**Tests:**
- Multi-branch CASE in SELECT column list.
- CASE in WHERE clause.
- CASE with no ELSE (returns NULL).
- Nested CASE expressions.

---

## 3. TaQL Functions — Running & Boxed Windows

### 3.1 Running Window Functions

**Status:** IMPLEMENT

**C++ behaviour:** Running aggregates operate over all rows from the
first row up to the current row, in the current sort order.
16 functions: `RUNNINGMIN`, `RUNNINGMAX`, `RUNNINGMEAN`, `RUNNINGSUM`,
`RUNNINGVARIANCE`, `RUNNINGSTDDEV`, `RUNNINGRMS`, `RUNNINGMEDIAN`,
`RUNNINGANY`, `RUNNINGALL`, `RUNNINGNTRUE`, `RUNNINGNFALSE`,
`RUNNINGCOUNT`, `RUNNINGSUMSQR`, `RUNNINGPRODUCT`, `RUNNINGFRACTILE`.

Also sample-variance variants: `RUNNINGSAMPLEVARIANCE`,
`RUNNINGSAMPLESTDDEV`. And `RUNNINGAVDEV`.

**Rust current state:** Not implemented. No window function infrastructure.

**Work required:**
- Add `WindowKind` enum (`Running`, `Boxed { half_width }`) to
  function dispatch.
- Implement a two-pass execution model: first materialise column
  values in sort order, then compute running aggregates.
- Use incremental accumulators where possible (sum, mean, variance
  via Welford's). Median and fractile require buffering.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/exec.rs` (window execution pass)
- `crates/casacore-tables/src/taql/ast.rs` (if new node type needed)

**Tests:**
- `RUNNINGSUM(flux)` over 10 rows — verify cumulative sums.
- `RUNNINGMEAN(flux)` — verify running average.
- `RUNNINGMEDIAN(flux)` — verify running median.
- Edge: single row, NULL values.

---

### 3.2 Boxed (Sliding Window) Functions

**Status:** IMPLEMENT

**C++ behaviour:** Boxed aggregates operate over a symmetric window of
`2*halfwidth+1` rows centred on the current row. Same 16+3 function
names as running but prefixed `BOXED`. Takes a half-width parameter:
`BOXEDMEAN(col, 3)` means window of 7 rows.

**Rust current state:** Not implemented.

**Work required:**
- Same infrastructure as running windows (item 3.1).
- Sliding-window accumulators for sum/mean/count (add front, remove
  back). Median/fractile require sorted window buffer.
- Handle boundary: window truncated at table edges (partial window).

**Files to modify:**
- Same as item 3.1.

**Tests:**
- `BOXEDMEAN(flux, 2)` over 10 rows — verify windowed means.
- `BOXEDMIN(flux, 1)` — 3-element window min.
- Edge: half_width larger than table, half_width = 0.

---

## 4. TaQL Functions — Partial-Axis Reductions

### 4.1 Partial-Axis Reduction Functions

**Status:** IMPLEMENT

**C++ behaviour:** Functions like `SUMS(array, axes)` reduce an array
along specified axes, returning a lower-dimensional array. The `axes`
argument is an array of axis indices. 14 functions:
`SUMS`, `SUMSQRS`, `PRODUCTS`, `MINS`, `MAXS`, `MEANS`, `VARIANCES`,
`STDDEVS`, `RMSS`, `MEDIANS`, `ANYS`, `ALLS`, `NTRUES`, `NFALSES`.

Also sample-variance variants: `SAMPLEVARIANCES`, `SAMPLESTDDEVS`.
And `AVDEVS`.

**Rust current state:** Full-array reductions exist (e.g. `sum(array)`
returns a scalar). Partial-axis reductions (`sums(array, [0])`) do not.

**Work required:**
- Register `sums`, `products`, etc. as multi-arg variants.
- Use `ndarray` lane iteration (`.lanes(axis)`) to reduce along
  specified axes.
- Return array with reduced shape.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`

**Tests:**
- `SUMS(arr, [1])` on a [3,4] array → [3] result.
- `MEANS(arr, [0])` on a [3,4] array → [4] result.
- Multi-axis: `SUMS(arr, [0,1])` → scalar.
- Edge: reduce along only axis of 1-D array → scalar.

---

## 5. TaQL Functions — Miscellaneous Missing

### 6.1 `pattern()` and `sqlpattern()` Functions

**Status:** IMPLEMENT

**C++ behaviour:** `pattern(glob_string)` converts a shell-style glob
pattern to a `TaqlRegex`. `sqlpattern(sql_string)` converts a SQL
`LIKE`-style pattern (`%`/`_`) to a `TaqlRegex`. Both are used with
the `~=` match operator.

**Rust current state:** `regex()` function exists. `pattern()` and
`sqlpattern()` do not.

**Work required:**
- `pattern()`: convert `*` → `.*`, `?` → `.`, escape other regex chars.
- `sqlpattern()`: convert `%` → `.*`, `_` → `.`, escape other regex chars.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`

**Tests:**
- `WHERE name ~= pattern('SRC_*')` matches all SRC_ prefixed.
- `WHERE name ~= sqlpattern('SRC_%')` matches same.

---

### 6.2 `string()` Formatting Function (Extended Forms)

**Status:** IMPLEMENT

**C++ behaviour:** `string(value)` converts to string. Extended forms:
`string(value, format)` with printf-style format,
`string(value, width, precision)`.

**Rust current state:** `string(value)` exists (basic conversion).
Format and width/precision variants do not.

**Work required:**
- Detect 2-arg and 3-arg calls.
- Implement printf-style format or width+precision formatting.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`

**Tests:**
- `string(3.14159, '%8.3f')` → `"   3.142"`.
- `string(42, 6)` → `"    42"`.

---

### 6.3 `iskeyword()` Function — Proper Implementation

**Status:** IMPLEMENT

**C++ behaviour:** `iskeyword(name)` returns `True` if the named
keyword exists in the table's keyword set.

**Rust current state:** Stub that always returns `false`.

**Work required:**
- Pass the table's keyword `RecordValue` into the evaluation context.
- Check if the named field exists.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/eval.rs` (context needs keyword access)

**Tests:**
- `iskeyword('telescope')` → `True` when keyword exists.
- `iskeyword('nonexistent')` → `False`.

---

### 6.4 `rand()` — Proper RNG

**Status:** IMPLEMENT

**C++ behaviour:** `rand()` returns a uniform random double in [0, 1).
Different value per row per evaluation.

**Rust current state:** Uses a deterministic hash of the row index —
produces the same value every time for the same row. Not truly random.

**Work required:**
- Replace hash-based implementation with a proper PRNG (e.g.
  `rand::thread_rng()` or a seeded `SmallRng`).
- Ensure different values per row within one query.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs`

**Tests:**
- `CALC rand()` returns value in [0, 1).
- Two calls in the same SELECT produce different columns.

---

## 6. TaQL Group Aggregates

### 6.1 Array Group Aggregates (`GMINS`, `GMAXS`, etc.)

**Status:** IMPLEMENT

**C++ behaviour:** Array variants collect per-element statistics across
all rows in a group, returning an array result. 16 functions:
`GMINS`, `GMAXS`, `GSUMS`, `GPRODUCTS`, `GSUMSQRS`, `GMEANS`,
`GVARIANCES`, `GSAMPLEVARIANCES`, `GSTDDEVS`, `GSAMPLESTDDEVS`,
`GRMSS`, `GANYS`, `GALLS`, `GNTRUES`, `GNFALSES`, `GHIST`.

**Rust current state:** Not implemented. The `Accumulator` type handles
scalar aggregation only.

**Work required:**
- Add `ArrayAccumulator` that maintains per-element running statistics.
- All input arrays in a group must have the same shape (error otherwise).
- `GHIST(col, nbins, min, max)` returns an integer array of bin counts.

**Files to modify:**
- `crates/casacore-tables/src/taql/aggregate.rs`
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/functions.rs`

**Tests:**
- `SELECT GSUMS(arr_col) GROUP BY category` — element-wise sum.
- `GHIST(flux, 5, 0.0, 50.0) GROUP BY category` — histogram per group.
- Shape mismatch across rows in a group → error.

---

### 6.2 `GAGGR` / `GROWID` Lazy Aggregates

**Status:** IMPLEMENT

**C++ behaviour:** `GAGGR(col)` (alias `GSTACK`) collects all values in
a group into a single array. `GROWID()` collects all row IDs in a
group into an array. Both are "lazy" — they buffer all values and
produce results after grouping completes.

**Rust current state:** Not implemented.

**Work required:**
- Add lazy accumulator variants that buffer values during grouping.
- `GAGGR` returns an array column whose elements are the collected values.
- `GROWID` returns an integer array of row indices.

**Files to modify:**
- `crates/casacore-tables/src/taql/aggregate.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `SELECT GAGGR(id) GROUP BY category` — each group gets array of IDs.
- `SELECT GROWID() GROUP BY category` — each group gets array of row indices.

---

## 7. TaQL Statements & Clauses

### 7.1 Column Aliases (`AS`)

**Status:** IMPLEMENT

**C++ behaviour:** `SELECT col AS alias_name` renames the result column.
`SELECT flux*2 AS double_flux` gives computed columns a name.

**Rust current state:** The parser has `SelectColumn { expr, alias:
Option<String> }` in the AST. The `AS` keyword is lexed. Parsing may
partially work but execution does not rename output columns.

**Work required:**
- Verify parser handles `AS` correctly.
- Exec: apply alias as the column name in the result `RefTable`.

**Files to modify:**
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `SELECT flux AS f` — result table has column named "f".
- `SELECT flux*2 AS double_flux` — computed column named "double_flux".

---

### 7.2 `COUNT` Statement

**Status:** IMPLEMENT

**C++ behaviour:** `COUNT SELECT ... FROM ... WHERE ...` returns a
single-row, single-column result with the matching row count.

**Rust current state:** Not implemented as a statement. `SELECT COUNT(*)`
works as an aggregate.

**Work required:**
- Token: `COUNT` already exists (used for aggregate).
- Parser: detect `COUNT` at statement level before `SELECT`.
- Exec: run the inner SELECT, return single-row table with count.

**Files to modify:**
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `COUNT SELECT * FROM t WHERE id > 40` → 9.
- `COUNT SELECT * FROM t` → 50.

---

### 7.3 `GIVING` Clause

**Status:** IMPLEMENT

**C++ behaviour:** `SELECT ... FROM ... GIVING tablename [AS PLAIN|MEMORY]`
writes the query result into a new table. Without `AS`, defaults to a
RefTable on disk. `AS PLAIN` materialises a full PlainTable. `AS MEMORY`
creates an in-memory table.

**Rust current state:** Not implemented.

**Work required:**
- Parser: parse `GIVING` + table name + optional `AS` type after
  WHERE/ORDER/LIMIT.
- Exec: after computing the result, save it via `Table::save()` or
  `RefTable::save()` to the given path.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs` (`GIVING` keyword)
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `SELECT * FROM t WHERE flux > 10 GIVING '/tmp/bright.tbl'` — new table exists.
- `GIVING ... AS PLAIN` — result is a PlainTable, not RefTable.
- `GIVING ... AS MEMORY` — result is a memory table.
- Result table openable via `Table::open()` and contains correct rows.

---

### 7.4 Set Operations (`UNION`, `INTERSECT`, `EXCEPT`)

**Status:** IMPLEMENT

**C++ behaviour:** `(SELECT ...) UNION (SELECT ...)` merges row sets.
`INTERSECT` keeps rows in both. `EXCEPT` keeps rows in left but not
right. Operates on row numbers when both queries are from the same
root table.

**Rust current state:** Not implemented.

**Work required:**
- Parser: parse binary set operators between parenthesised queries.
- Exec: compute each query's row set, apply set operation, return
  result as a RefTable.
- Validate column compatibility between operands.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs` (`UNION`, `INTERSECT`, `EXCEPT`)
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `(SELECT * WHERE id < 10) UNION (SELECT * WHERE id > 45)` — combined rows.
- `INTERSECT` on overlapping ranges → only common rows.
- `EXCEPT` removes second set from first.
- Incompatible column schemas → error.

---

### 7.5 Subqueries

**Status:** IMPLEMENT

**C++ behaviour:** `WHERE col IN [SELECT subcol FROM ...]` evaluates the
inner query first, uses its result as the set for the outer `IN`.
Scalar subqueries can appear in column expressions.

**Rust current state:** Not implemented.

**Work required:**
- Parser: allow `SELECT` inside `IN [...]` brackets.
- Parser: allow `(SELECT ...)` as a scalar expression.
- Exec: evaluate inner query, extract column values, use as set/scalar.

**Files to modify:**
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `WHERE id IN [SELECT id FROM t WHERE category = 'star']`.
- Scalar subquery: `SELECT (SELECT MAX(flux) FROM t) - flux AS diff`.

---

### 7.6 `CREATE TABLE` Statement

**Status:** IMPLEMENT

**C++ behaviour:** `CREATE TABLE name (col1 TYPE1, col2 TYPE2, ...)
[LIMIT n] [DMINFO ...]` creates a new table with the given schema.

**Rust current state:** Not implemented.

**Work required:**
- Parser: `CREATE TABLE` + name + parenthesised column definitions.
- Exec: build `TableSchema`, create `Table`, optionally add rows, save.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs` (`CREATE`)
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `CREATE TABLE '/tmp/new.tbl' (id INT, name STRING)` — table exists with 0 rows.
- Created table openable via `Table::open()`.

---

### 7.7 `DROP TABLE` Statement

**Status:** IMPLEMENT

**C++ behaviour:** `DROP TABLE name` deletes a table from disk.

**Rust current state:** Not implemented.

**Work required:**
- Parser: `DROP TABLE` + table name.
- Exec: remove the table directory.

**Files to modify:**
- `crates/casacore-tables/src/taql/token.rs` (`DROP` — may already exist for `ALTER TABLE DROP COLUMN`)
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- Create a table, DROP it, verify directory removed.
- DROP non-existent table → error.

---

## 8. TaQL JOIN, HAVING & UDF

### 8.1 JOIN Execution

**Status:** IMPLEMENT

**C++ behaviour:** `SELECT ... FROM t1 JOIN t2 ON t1.key = t2.key`
supports `INNER JOIN`, `LEFT JOIN`, `CROSS JOIN`. The `ON` condition
is split at `AND` boundaries; each part must contain `=` or `IN`.
Nested joins supported.

**Rust current state:** The AST has `JoinClause` with `JoinType` enum
(Inner, Left, Right, Cross). The parser recognises JOIN syntax.
Execution is not implemented — the exec engine ignores joins.

**Work required:**
- Exec: implement nested-loop join as the baseline algorithm.
- Optimise with `ColumnsIndex` when available on the join key.
- Build a combined row context for evaluating expressions that
  reference columns from both tables.

**Files to modify:**
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/eval.rs` (multi-table context)

**Tests:**
- `INNER JOIN` on matching key → correct combined rows.
- `LEFT JOIN` → all left rows, NULLs for unmatched right.
- `CROSS JOIN` → cartesian product.
- JOIN with index-accelerated key lookup.

---

### 8.2 `HAVING` Clause

**Status:** IMPLEMENT

**C++ behaviour:** `HAVING expr` filters groups after `GROUP BY`
aggregation. The expression can reference aggregate results.

**Rust current state:** The AST has `having: Option<Expr>` in
`SelectStatement`. The parser recognises HAVING. Execution does not
apply the filter.

**Work required:**
- Exec: after computing GROUP BY aggregates, evaluate HAVING predicate
  on each aggregated row. Remove rows that don't satisfy it.

**Files to modify:**
- `crates/casacore-tables/src/taql/exec.rs`

**Tests:**
- `GROUP BY category HAVING COUNT(*) > 5` — only large groups.
- `GROUP BY category HAVING AVG(flux) > 10.0`.

---

### 8.3 User-Defined Function (UDF) Framework

**Status:** IMPLEMENT

**C++ behaviour:** `UDFBase` is an abstract base class. Users derive
from it, implement `setup()` and `get*()` methods, provide a
`makeObject()` factory, and register it. TaQL calls UDFs via
`libname.funcname` namespace syntax. Libraries are loaded dynamically.

**Rust current state:** Not implemented.

**Work required:**
- Define public trait `TaqlFunction`:
  ```rust
  pub trait TaqlFunction: Send + Sync {
      fn name(&self) -> &str;
      fn evaluate(&self, args: &[ExprValue]) -> Result<ExprValue, TaqlError>;
  }
  ```
- Registration: `TaqlEngine::register_function(Box<dyn TaqlFunction>)`.
- Function lookup: check registry before built-in dispatch.
- No dynamic library loading needed initially — static registration
  is sufficient for Rust.

**Files to modify:**
- `crates/casacore-tables/src/taql/functions.rs` (trait + registry)
- `crates/casacore-tables/src/taql/mod.rs` (re-export)

**Tests:**
- Register a custom `double_it` function, call in TaQL.
- UDF name collision with built-in → UDF wins.
- Unregistered function name → error.

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

### 16.1 `VirtualTaQLColumn`

**Status:** IMPLEMENT

**C++ behaviour:** A virtual column engine that computes cell values
by evaluating a TaQL expression. The expression string is stored
persistently in the engine's spec record. Read-only. Supports scalar
and array output. Constant expressions are cached. Registered as
`"VirtualTaQLColumn"`.

**Rust current state:** Not implemented. The virtual engine registry
does not include `VirtualTaQLColumn`.

**Work required:**
- New file: `storage/virtual_taql_column.rs`.
- Implement `VirtualColumnEngine` trait.
- On `prepare()`: parse and compile the TaQL expression.
- On `get()`: evaluate the expression for the given row.
- Cache result for constant expressions.
- Register in engine registry as `"VirtualTaQLColumn"`.
- Persist expression string in engine spec record.

**Files to modify:**
- `crates/casacore-tables/src/storage/virtual_taql_column.rs` (new)
- `crates/casacore-tables/src/storage/virtual_engine.rs` (registration)
- `crates/casacore-tables/src/storage/mod.rs` (re-export)
- `crates/casacore-tables/src/table.rs` (bind method)

**Tests:**
- `bind_virtual_taql_column("virt", "flux * 2.0")` — read → double flux.
- Constant expression: `bind_virtual_taql_column("ones", "1.0")` → cached.
- Save + reopen → expression persisted and re-evaluated.
- C++ interop: C++ reads Rust-written VirtualTaQLColumn.

---

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

## 18. TaQL Styles & Configuration

### 18.1 TaQL Indexing Styles (Glish vs Python)

**Status:** IMPLEMENT

**C++ behaviour:** `TaQLStyle` allows selecting between:
- **Glish** (default): 1-based indices, Fortran axis order, inclusive end.
- **Python**: 0-based indices, C axis order, exclusive end.
Set via `USING STYLE PYTHON` in the query or via `TaQLStyle::set()`.

**Rust current state:** Hardcoded to 1-based Glish-style indexing
(matching C++ default). No way to switch to Python style.

**Work required:**
- Add `TaqlStyle` configuration to the execution context.
- Parser: recognise `USING STYLE PYTHON/GLISH` clause.
- Eval: apply style-dependent index adjustment in array indexing.

**Files to modify:**
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/eval.rs`
- `crates/casacore-tables/src/taql/parser.rs`

**Tests:**
- `USING STYLE PYTHON SELECT arr[0]` → first element (0-based).
- Default style: `SELECT arr[1]` → first element (1-based).

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

### 22.1 `taql` — Interactive TaQL Executor

**Status:** IMPLEMENT

**C++ equivalent:** `tables/apps/taql.cc` — reads TaQL commands from
stdin or arguments, executes against tables, prints results.

**Work required:**
- New binary: `crates/casacore-tables/examples/taql.rs` or standalone
  `src/bin/taql.rs`.
- Read TaQL from stdin or `--command` argument.
- Execute via `Table::execute_taql()`.
- Pretty-print result table.

**Tests:**
- Run against a test table, verify output matches expected.

---

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

