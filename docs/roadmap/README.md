# casa-rs Implementation Roadmap

This roadmap organizes remaining casacore Tables module functionality into
waves of related concepts. Each wave is self-contained: it adds a coherent
set of features, tests them (including 2×2 C++ interop), documents them at
C++ doxygen level, and closes with a quality gate before commit/push.

Each wave's detailed specification lives in its own file (linked below) so
that only the relevant wave needs to be in context during implementation.

## Wave status

| Wave | Name | Status | Summary |
|------|------|--------|---------|
| 0 | Foundation | **DONE** | Types, table CRUD, two storage managers, AipsIO, demos, docs |
| 1 | [Endian support](wave-01-endian.md) | Not started | Full big/little/local endian read/write for all storage managers |
| 2 | [Schema mutation & row ops](wave-02-schema-mutation.md) | Not started | Add/remove/rename columns, delete rows |
| 3 | [Table locking](wave-03-locking.md) | Not started | File-based multi-process locking (`TableLock`) |
| 4 | [Reference tables & selections](wave-04-ref-tables.md) | Not started | Row/column views without copying (`RefTable`) |
| 5 | [Sorting & table iteration](wave-05-sorting.md) | Not started | Sort by key columns, grouped sub-table iteration |
| 6 | [Table concatenation & copy](wave-06-concat-copy.md) | Not started | Virtual concatenation, deep copy with DM conversion |
| 7 | [Column indexing](wave-07-indexing.md) | Not started | In-memory index for fast scalar column lookups |
| 8 | [Memory tables](wave-08-memory-tables.md) | Not started | Non-persistent in-memory tables (`MemoryStMan`) |
| 9 | [TaQL](wave-09-taql.md) | Not started | Table Query Language: SELECT, WHERE, JOIN, GROUP BY, etc. |
| 10 | [Tiled storage managers](wave-10-tiled-stman.md) | Not started | Hypercube tiling for efficient N-D array access |
| 11 | [Incremental storage manager](wave-11-incremental-stman.md) | Not started | Delta-compression for slowly-changing columns |
| 12 | [Virtual column engines](wave-12-virtual-columns.md) | Not started | Computed columns, scaling/compression engines |

## Deferred (add when real usage requires)

| Area | Rationale |
|------|-----------|
| ADIOS2 storage | HPC-specific; add if/when HPC users need it |
| Table proxy / Python bindings | Use PyO3 binding to existing Rust types; separate crate |
| Log tables | Integrate into Rust logging ecosystem (tracing/log); separate crate |
| Sub-table `::` path notation | Convenience; can use plain path manipulation meanwhile |

---

## Wave closeout gate

Every wave must pass ALL of the following before commit/push:

### Code quality
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace` — all tests pass, zero regressions
- `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` — zero doc warnings

### Test coverage
- Unit tests for all new public methods and error paths
- **2×2 interop matrix**: Rust-writes → C++-reads AND C++-writes → Rust-reads
  (via `casacore-test-support` shims, skipped when `pkg-config casacore` missing)
- Regression: all pre-existing tests still pass

### Documentation
- `///` doc comments on every new public type, method, enum variant
- Level of detail comparable to corresponding C++ doxygen `.h` files
- C++ class/function names referenced for cross-referencing
- Crate-level `//!` docs updated if the wave adds a new major concept

### Housekeeping
- SPDX header (`// SPDX-License-Identifier: LGPL-3.0-or-later`) on all new `.rs` files
- Demo program updated or added if the wave adds user-visible workflow changes
- Wave status updated in this roadmap file
- Single focused commit with descriptive message, then push
