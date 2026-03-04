# Wave 9: TaQL (Table Query Language)

**Why:** TaQL is the standard way to query casacore tables in astronomy
pipelines. Without it, users must write manual loops for filtering, joining,
and aggregating.

**C++ reference:** 55 header files in `tables/TaQL/`. Core: `TableParse.h`,
`TaQLNode.h`, `ExprNode.h`.

## Scope (phased internally)

**9a. Lexer & parser** — tokenize and parse TaQL strings into an AST.
Support SELECT, WHERE, ORDER BY, LIMIT.

**9b. Expression evaluator** — evaluate scalar expressions (arithmetic,
comparison, boolean logic, string operations) against table rows.

**9c. SELECT execution** — column projection + row filtering → RefTable.

**9d. UPDATE / INSERT / DELETE** — mutation operations.

**9e. Aggregation** — GROUP BY with SUM, AVG, MIN, MAX, COUNT.

**9f. JOIN** — cross-table joins.

**9g. Built-in functions** — math (sin, cos, sqrt, ...), string (regex,
upper, lower, ...), array (shape, nelements, ...), datetime.

## Tests

- Parse round-trip for each statement type.
- SELECT with WHERE on various types.
- ORDER BY single and multiple columns.
- GROUP BY with aggregates.
- UPDATE modifying cells.
- 2×2: Run same TaQL on Rust and C++, compare results.
