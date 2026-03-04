# Wave 13 — Remaining C++ Tables Parity Gaps

This document covers **all remaining parity gaps** between casa-rs and
the C++ casacore Tables module. Items are grouped into sub-waves (13a–13h)
with explicit closeout criteria and parallelization notes.

---

## Status Legend

| Tag | Meaning |
|-----|---------|
| **IMPLEMENT** | Will be built in this wave |
| **DEFER** | Out of scope for now (documented why) |

---

## High-Level Gap Inventory

### Implement

| # | Gap | Sub-wave |
|---|-----|----------|
| 1 | TaQL regex literal syntax (`p/…/`) | 13a |
| 2 | TaQL array indexing & slicing (`col[i,j]`, `col[start:end:step]`) | 13a |
| 3 | TaQL bitwise operators (`&`, `\|`, `^`, `~`) | 13a |
| 4 | TaQL `IN` with set/range syntax | 13a |
| 5 | TaQL `BETWEEN … AND` | 13a |
| 6 | TaQL `LIKE` / `NOT LIKE` pattern matching | 13a |
| 7 | TaQL `CASE WHEN … THEN … ELSE … END` | 13a |
| 8 | TaQL `IIF(cond, true_val, false_val)` | 13a |
| 9 | Running window functions (RUNNINGMIN/MAX/MEAN/etc.) | 13b |
| 10 | Boxed window functions (BOXEDMIN/MAX/MEAN/etc.) | 13b |
| 11 | Partial-axis array reductions (SUMS, MEANS, etc. with axes arg) | 13c |
| 12 | Array manipulation (TRANSPOSE, RESIZE, DIAGONAL, ARRAY, etc.) | 13c |
| 13 | Group aggregate functions (GMIN, GMAX, GMEAN, etc.) | 13d |
| 14 | Special aggregates (GMEDIAN, GVARIANCE, GSTDDEV, GHIST) | 13d |
| 15 | COUNT statement | 13e |
| 16 | GIVING clause (write query results to a new table) | 13e |
| 17 | TaQL set operations (UNION, INTERSECT, EXCEPT) | 13e |
| 18 | Column aliases (`AS`) in SELECT | 13e |
| 19 | Subqueries in WHERE / column expressions | 13e |
| 20 | CREATE TABLE statement | 13e |
| 21 | DROP TABLE statement | 13e |
| 22 | Table operations: `addRow()`, `removeRow()` via TaQL (INSERT/DELETE already work) | 13f |
| 23 | `VirtualTaQLColumn` engine (column whose value is a TaQL expression) | 13f |
| 24 | `TableInfo` metadata (type, subtype, readme string) | 13f |
| 25 | `TableKeyword` / keyword get/set on table and columns | 13f |
| 26 | JOIN execution (INNER, LEFT, CROSS) | 13g |
| 27 | HAVING clause (post-aggregation filter) | 13g |
| 28 | User-Defined Function (UDF) registration framework | 13g |
| 29 | `ExternalLockSync` (external-tool lock handshake) | 13h |
| 30 | `TableIterator` descending sort order | 13h |
| 31 | Cell-level array slicing (read/write a slice of an array cell) | 13h |

### Defer

| # | Gap | Reason |
|---|-----|--------|
| D1 | Parallel storage manager (multi-threaded column I/O) | Performance optimisation; not required for correctness or API parity |
| D2 | MeasurementSet convenience layer | Large domain-specific layer; belongs in a separate `casacore-ms` crate |
| D3 | Legacy `TableGram` C-grammar parser | We use a hand-written Pratt parser; no need for yacc/bison compat |
| D4 | `LofarStMan` and other observatory-specific storage managers | Niche; can be contributed by downstream users |
| D5 | Python bindings (`python-casacore` parity) | Separate project (`pyo3` wrapper crate) |

---

## Sub-Wave Details

### Wave 13a — TaQL Expression Syntax (foundational)

**Summary:** Extend the lexer, parser, and evaluator to support all
remaining expression-level syntax that downstream waves depend on.

**Parallel:** No — this is foundational. Waves 13b–13e and 13g depend on it.

#### Items

1. **Regex literal syntax** — `p/pattern/flags`
   - Lexer: new `RegexLiteral` token variant
   - Parser: parse into `Expr::Regex { pattern, flags }`
   - Eval: compile with `regex` crate, apply in `WHERE` / `HAVING`

2. **Array indexing & slicing** — `col[i]`, `col[i,j]`, `col[start:end]`, `col[start:end:step]`
   - Parser: parse postfix `[…]` as `Expr::ArrayIndex` / `Expr::ArraySlice`
   - Eval: extract element or sub-array from `TableValue::Array`

3. **Bitwise operators** — `&`, `|`, `^`, `~`
   - Lexer: distinguish bitwise `&`/`|` from logical `&&`/`||`
   - Parser: new precedence levels for bitwise ops
   - Eval: integer-only, type-check and error on float/string

4. **`IN` with set/range** — `col IN [1,2,3]`, `col IN [1:10]`
   - Parser: parse `IN` + bracketed list or range
   - Eval: membership test against value set or range bounds

5. **`BETWEEN … AND`** — `col BETWEEN a AND b`
   - Parser: ternary expression
   - Eval: equivalent to `col >= a AND col <= b`

6. **`LIKE` / `NOT LIKE`** — SQL-style `%` and `_` wildcards
   - Parser: binary operator
   - Eval: convert pattern to regex, match

7. **`CASE WHEN … THEN … ELSE … END`**
   - Parser: multi-branch conditional expression
   - AST: `Expr::Case { branches: Vec<(Expr, Expr)>, else_expr: Option<Expr> }`

8. **`IIF(cond, true_val, false_val)`**
   - Functions: register as built-in, evaluate lazily

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/token.rs` | New token variants |
| `crates/casacore-tables/src/taql/lexer.rs` | Lex regex literals, bitwise ops |
| `crates/casacore-tables/src/taql/ast.rs` | New AST node variants |
| `crates/casacore-tables/src/taql/parser.rs` | Parse all new syntax |
| `crates/casacore-tables/src/taql/eval.rs` | Evaluate new expressions |
| `crates/casacore-tables/src/taql/functions.rs` | `IIF` built-in |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] `cargo test --workspace` passes
- [ ] At least **20 new tests** covering each syntax form
- [ ] `cargo clippy` clean
- [ ] Doc comments on all new AST variants

---

### Wave 13b — Running & Boxed Window Functions

**Summary:** Implement the 32 running/boxed window aggregate functions
from C++ `TableExprGroupKey`.

**Parallel:** Yes — can run after 13a completes.

#### Items

**Running functions** (operate over rows in current order):
- `RUNNIN`, `RUNNINGMIN`, `RUNNINGMAX`, `RUNNINGMEAN`
- `RUNNINGVARIANCE`, `RUNNINGSTDDEV`, `RUNNINGRMS`
- `RUNNINGMEDIAN`, `RUNNINGANY`, `RUNNINGALL`
- `RUNNINGNTRUE`, `RUNNINGNFALSE`, `RUNNINGCOUNT`
- `RUNNINGSUM`, `RUNNINGSUMSQR`, `RUNNINGPRODUCT`

**Boxed (sliding window) functions** (operate over a fixed window):
- `BOXEDMIN`, `BOXEDMAX`, `BOXEDMEAN`
- `BOXEDVARIANCE`, `BOXEDSTDDEV`, `BOXEDRMS`
- `BOXEDMEDIAN`, `BOXEDANY`, `BOXEDALL`
- `BOXEDNTRUE`, `BOXEDNFALSE`, `BOXEDCOUNT`
- `BOXEDSUM`, `BOXEDSUMSQR`, `BOXEDPRODUCT`

#### Implementation Approach

- Add `WindowKind` enum (`Running`, `Boxed { half_width }`) to `functions.rs`
- Implement incremental accumulators for efficiency (avoid O(n²))
- Boxed functions take a half-width parameter: `BOXEDMEAN(col, 3)`

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/functions.rs` | Window function implementations |
| `crates/casacore-tables/src/taql/exec.rs` | Window function execution pass |
| `crates/casacore-tables/src/taql/ast.rs` | Window function AST nodes (if needed) |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **16 new tests** (cover each function family + edge cases)
- [ ] Incremental accumulators used (no O(n²) full-scan per row)
- [ ] `cargo clippy` clean
- [ ] Doc comments on public function signatures

---

### Wave 13c — Partial-Axis Reductions & Array Manipulation

**Summary:** Implement array reduction functions that operate along
specified axes, plus array reshaping/manipulation utilities.

**Parallel:** Yes — can run after 13a completes.

#### Items

**Partial-axis reductions** (operate along specified axes of an N-D array):
- `SUMS(array, axes)`, `SUMSQRS(array, axes)`
- `PRODUCTS(array, axes)`
- `MINS(array, axes)`, `MAXS(array, axes)`
- `MEANS(array, axes)`, `VARIANCES(array, axes)`, `STDDEVS(array, axes)`
- `RMSS(array, axes)`
- `MEDIANS(array, axes)`
- `ANYS(array, axes)`, `ALLS(array, axes)`
- `NTRUES(array, axes)`, `NFALSES(array, axes)`

**Array manipulation:**
- `TRANSPOSE(array)` / `TRANSPOSE(array, axes)` — axis permutation
- `RESIZE(array, shape)` — reshape with zero-fill or truncation
- `DIAGONAL(array)` — extract diagonal of 2-D array
- `ARRAY(scalar, shape)` — create array filled with scalar
- `MARRAY(data_array, mask_array)` — create masked array
- `FLATTEN(array)` — collapse to 1-D
- `NEGATEMASK(masked_array)` — invert mask
- `REPLACEMASKED(masked_array, value)` — fill masked values
- `REPLACEUNMASKED(masked_array, value)` — fill unmasked values

#### Implementation Approach

- Use `ndarray` axis iteration for partial reductions
- Masked arrays represented as `(Array, BoolArray)` tuple internally

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/functions.rs` | All new functions |
| `crates/casacore-tables/src/taql/eval.rs` | Masked array value support (if needed) |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **12 new tests** covering axis reductions + array manipulation
- [ ] Partial-axis reductions verified against `ndarray` reference outputs
- [ ] `cargo clippy` clean
- [ ] Doc comments on all public functions

---

### Wave 13d — Group Aggregates & Special Aggregates

**Summary:** Extend the GROUP BY aggregation engine with the full set of
C++ grouped aggregate functions.

**Parallel:** Yes — can run after 13a completes.

#### Items

**Standard group aggregates** (these supplement existing SUM/AVG/MIN/MAX/COUNT):
- `GMIN`, `GMAX`, `GMEAN`, `GSUM`
- `GVARIANCE`, `GSTDDEV`, `GRMS`
- `GCOUNT`, `GNTRUE`, `GNFALSE`
- `GANY`, `GALL`
- `GSUMSQR`, `GPRODUCT`

**Special aggregates:**
- `GMEDIAN` — median within each group (requires sorting)
- `GVARIANCE` / `GSTDDEV` — Welford's online algorithm
- `GHIST(col, nbins, min, max)` — histogram per group
- `GFRACTILE(col, fraction)` — quantile within group
- `GAGGR(col)` — collect all values in group into an array

#### Implementation Approach

- Extend `aggregate.rs` with new accumulator types
- `GMEDIAN` and `GFRACTILE` require buffering all group values
- `GHIST` returns an array column (bin counts)

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/aggregate.rs` | New accumulator types |
| `crates/casacore-tables/src/taql/functions.rs` | Register group functions |
| `crates/casacore-tables/src/taql/exec.rs` | Wire up new aggregates in GROUP BY path |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **10 new tests** (each aggregate family + GHIST + GFRACTILE)
- [ ] Welford's algorithm used for variance/stddev (numerically stable)
- [ ] `cargo clippy` clean
- [ ] Doc comments on all aggregate functions

---

### Wave 13e — TaQL Statements, Aliases, GIVING & Set Operations

**Summary:** Complete the TaQL statement-level features: new statement
types, column aliases, the GIVING clause, and set operations.

**Parallel:** Yes — can run after 13a completes.

#### Items

1. **Column aliases** — `SELECT col AS alias_name`
   - Parser: parse `AS` after column expression
   - Exec: rename result columns

2. **COUNT statement** — `COUNT SELECT … FROM …`
   - Parser: new statement variant
   - Exec: return single-row table with row count

3. **GIVING clause** — `SELECT … FROM … GIVING tablename [AS PLAIN|MEMORY]`
   - Parser: parse `GIVING` after WHERE/ORDER/LIMIT
   - Exec: write result set to a new on-disk or memory table

4. **Set operations** — `(SELECT …) UNION (SELECT …)`, `INTERSECT`, `EXCEPT`
   - Parser: parse binary set operators between parenthesised queries
   - Exec: merge/intersect/subtract row sets
   - Handle column compatibility checks

5. **Subqueries** — `WHERE col IN [SELECT subcol FROM …]`
   - Parser: allow `SELECT` inside `IN` brackets and as scalar expression
   - Exec: evaluate inner query first, use result in outer predicate

6. **CREATE TABLE** — `CREATE TABLE name (col1 type1, col2 type2, …)`
   - Parser: new statement
   - Exec: create table with specified schema

7. **DROP TABLE** — `DROP TABLE name`
   - Parser: new statement
   - Exec: remove table from disk

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/ast.rs` | New statement/expression AST variants |
| `crates/casacore-tables/src/taql/parser.rs` | Parse all new syntax |
| `crates/casacore-tables/src/taql/exec.rs` | Execute new statements + GIVING + set ops |
| `crates/casacore-tables/src/taql/token.rs` | New keywords (GIVING, UNION, INTERSECT, EXCEPT, CREATE, DROP, COUNT) |
| `crates/casacore-tables/src/taql/lexer.rs` | Lex new keywords |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **15 new tests** covering each statement type + set operation
- [ ] GIVING writes a valid table readable by `Table::open()`
- [ ] Set operations validate column compatibility
- [ ] `cargo clippy` clean
- [ ] Doc comments on all new AST/exec paths

---

### Wave 13f — Table Operations, VirtualTaQLColumn & TableInfo

**Summary:** Non-TaQL table infrastructure: the `VirtualTaQLColumn`
engine, `TableInfo` metadata, and keyword access.

**Parallel:** Yes — fully independent of TaQL waves.

#### Items

1. **`VirtualTaQLColumn`** — a virtual column engine whose cell values
   are computed by evaluating a TaQL expression per row.
   - New file: `crates/casacore-tables/src/storage/virtual_taql_column.rs`
   - Register in virtual engine registry
   - Expression is stored as a string in the column keyword set

2. **`TableInfo`** — metadata block stored in `table.info`
   - Fields: `type` (string), `subType` (string), `readme` (string)
   - Read/write via `Table::info()` / `Table::set_info()`
   - Persisted in the table directory as `table.info`

3. **Keyword access** — get/set keywords on table and columns
   - `Table::keyword(name) -> Option<TableValue>`
   - `Table::set_keyword(name, value)`
   - `Table::column_keyword(col, name) -> Option<TableValue>`
   - `Table::set_column_keyword(col, name, value)`
   - Keywords are stored in `TableRecord` structures

4. **`Table::rename_column()`** — rename a column (schema-level operation)

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/storage/virtual_taql_column.rs` | **New file** |
| `crates/casacore-tables/src/storage/virtual_engine.rs` | Register VirtualTaQLColumn |
| `crates/casacore-tables/src/storage/mod.rs` | Re-export |
| `crates/casacore-tables/src/table.rs` | `info()`, `set_info()`, keyword API, `rename_column()` |
| `crates/casacore-tables/src/table_impl.rs` | Implementation details |
| `crates/casacore-tables/src/storage/table_control.rs` | TableInfo persistence |
| `crates/casacore-tables/tests/taql.rs` | VirtualTaQLColumn tests |

#### Closeout Criteria

- [ ] At least **15 new tests** (VirtualTaQLColumn, TableInfo round-trip, keyword get/set)
- [ ] VirtualTaQLColumn values match manual TaQL evaluation
- [ ] TableInfo survives close/reopen cycle
- [ ] C++ interop test: table with TableInfo readable by C++
- [ ] `cargo clippy` clean
- [ ] Doc comments on all public API additions

---

### Wave 13g — JOIN Execution, HAVING & UDF Framework

**Summary:** The most complex TaQL extension: multi-table JOIN, the
HAVING clause, and a user-defined function registration system.

**Parallel:** Partially — depends on 13a for expression syntax, but
otherwise independent.

#### Items

1. **JOIN execution**
   - Support `INNER JOIN … ON`, `LEFT JOIN … ON`, `CROSS JOIN`
   - Parser: extend `FROM` clause to accept join syntax
   - Exec: nested-loop join (sufficient for typical table sizes)
   - Index-accelerated join when `ColumnsIndex` is available on join key

2. **HAVING clause** — filter after GROUP BY aggregation
   - Parser: parse `HAVING` after `GROUP BY`
   - Exec: evaluate predicate on aggregated result rows

3. **UDF framework** — allow users to register custom functions
   - Public trait: `TaqlFunction` with `name()`, `return_type()`, `evaluate()`
   - Registration: `TaqlEngine::register_function(Box<dyn TaqlFunction>)`
   - Lookup: function registry checked before built-in fallback
   - Thread-safety: `Send + Sync` bound on trait

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/taql/ast.rs` | JOIN AST nodes, HAVING node |
| `crates/casacore-tables/src/taql/parser.rs` | Parse JOIN, HAVING |
| `crates/casacore-tables/src/taql/exec.rs` | JOIN execution engine, HAVING filter |
| `crates/casacore-tables/src/taql/functions.rs` | UDF trait + registry |
| `crates/casacore-tables/src/taql/mod.rs` | Re-export UDF types |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **10 new tests** (INNER/LEFT/CROSS JOIN, HAVING, UDF registration + invocation)
- [ ] JOIN on indexed column uses `ColumnsIndex` for O(1) lookup
- [ ] UDF trait is `Send + Sync` and documented
- [ ] `cargo clippy` clean
- [ ] Doc comments on public JOIN/HAVING/UDF API

---

### Wave 13h — ExternalLockSync, TableIterator Desc & Cell Slicing

**Summary:** Remaining non-TaQL infrastructure items.

**Parallel:** Yes — fully independent of all other waves.

#### Items

1. **`ExternalLockSync`** — handshake protocol for external tools
   - Trait: `ExternalLockSync` with `lock()`, `unlock()`, `has_lock()` methods
   - `Table::set_external_lock_sync(Box<dyn ExternalLockSync>)`
   - When set, table acquires external lock before internal file lock
   - Default: no external sync (current behavior)

2. **`TableIterator` descending sort** — iterate groups in descending order
   - `TableIterator::new_descending(table, sort_columns)` or add
     `SortOrder::Descending` support to existing iterator
   - Currently only ascending iteration is supported

3. **Cell-level array slicing** — read/write a sub-section of an array cell
   - `Table::get_cell_slice(col, row, slicer) -> Array`
   - `Table::put_cell_slice(col, row, slicer, data)`
   - `Slicer` type specifying start, end, stride per axis
   - Avoids reading the entire array when only a sub-region is needed

#### Files to Modify

| File | Changes |
|------|---------|
| `crates/casacore-tables/src/table.rs` | `set_external_lock_sync()`, cell slice API |
| `crates/casacore-tables/src/table_impl.rs` | Cell slice implementation |
| `crates/casacore-tables/src/lock/mod.rs` | ExternalLockSync trait + integration |
| `crates/casacore-tables/src/lock/lock_file.rs` | Hook external sync into lock acquire/release |
| `crates/casacore-tables/src/sorting.rs` | Descending TableIterator |
| `crates/casacore-tables/src/storage/mod.rs` | Slicer type, column slice read/write |
| `crates/casacore-tables/src/storage/standard_stman.rs` | Slice-level I/O |
| `crates/casacore-tables/src/storage/tiled_stman.rs` | Slice-level I/O (natural fit for tiled) |
| `crates/casacore-tables/tests/taql.rs` | Integration tests |

#### Closeout Criteria

- [ ] At least **10 new tests** (external lock lifecycle, descending iteration, cell slice round-trip)
- [ ] Cell slicing works with both `StandardStMan` and `TiledStMan`
- [ ] ExternalLockSync is opt-in and zero-cost when unused
- [ ] C++ interop test: sliced cell read matches C++ `getSlice()`
- [ ] `cargo clippy` clean
- [ ] Doc comments on all public types

---

## Parallelization Summary

```
                    ┌─────────────────────────────────────────────┐
                    │              Wave 13a (foundational)        │
                    │     TaQL expression syntax extensions       │
                    └──────┬──────┬──────┬──────┬────────────────┘
                           │      │      │      │
                    ┌──────▼──┐ ┌─▼────┐ ┌▼─────┐ ┌▼─────────┐
                    │ 13b     │ │ 13c  │ │ 13d  │ │ 13e       │
                    │ Window  │ │ Array│ │Group │ │ Statements│
                    │ funcs   │ │ ops  │ │aggs  │ │ & set ops │
                    └─────────┘ └──────┘ └──────┘ └───────────┘

  ┌──────────────────────┐                    ┌──────────────┐
  │ 13f (independent)    │                    │ 13g          │
  │ VirtualTaQLColumn    │                    │ JOIN + HAVING│
  │ TableInfo, Keywords  │                    │ UDF framework│
  └──────────────────────┘                    └──────────────┘

  ┌──────────────────────┐
  │ 13h (independent)    │
  │ ExternalLockSync     │
  │ Iterator desc, slice │
  └──────────────────────┘
```

**Maximum parallelism:** After 13a completes, up to **6 agents** can work
simultaneously (13b, 13c, 13d, 13e, 13g, plus either 13f or 13h which
can start immediately).

**Critical path:** 13a → 13g (JOIN is the most complex single item).

---

## Global Closeout Gate

After all sub-waves complete:

- [ ] `cargo fmt --all -- --check` — zero violations
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75` — coverage ≥ 75%
- [ ] `cargo doc --workspace --no-deps` — zero warnings
- [ ] All new public API items have `///` doc comments
- [ ] C++ interop tests pass (where applicable)
- [ ] `examples/t_table.rs` updated if new public API is added
- [ ] SPDX headers on all new files
