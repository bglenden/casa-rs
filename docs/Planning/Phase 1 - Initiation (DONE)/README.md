# Phase 1 Summary

Phase 1 is complete. The detailed per-wave planning files were removed after
their outcomes were consolidated; use git history if an old wave spec needs to
be reconstructed.

## Delivered Scope

| Wave | Name | Status | Summary |
|------|------|--------|---------|
| 0 | Foundation | **DONE** | Types, table CRUD, two storage managers, AipsIO, demos, docs |
| 1 | Endian support | **DONE** | Full big/little/local endian read/write for all storage managers |
| 2 | Schema mutation & row ops | **DONE** | Add/remove/rename columns, delete rows |
| 3 | Table locking | **DONE** | File-based multi-process locking (`TableLock`) |
| 4 | Reference tables & selections | **DONE** | Row/column views without copying (`RefTable`) |
| 5 | Sorting & table iteration | **DONE** | Sort by key columns, grouped sub-table iteration |
| 6 | Table concatenation & copy | **DONE** | Virtual concatenation, deep copy with DM conversion |
| 7 | Column indexing | Deferred | In-memory index for fast scalar column lookups |
| 8 | Memory tables | **DONE** | Non-persistent in-memory tables (`MemoryStMan`) |
| 9 | TaQL | Deferred | Table Query Language: `SELECT`, `WHERE`, `JOIN`, `GROUP BY`, etc. |
| 10 | Tiled storage managers | Deferred | Hypercube tiling for efficient N-D array access |
| 11 | Incremental storage manager | Deferred | Delta-compression for slowly-changing columns |
| 12 | Virtual column engines | Deferred | Computed columns, scaling/compression engines |

## Deferred Areas

| Area | Rationale |
|------|-----------|
| ADIOS2 storage | HPC-specific; add if/when HPC users need it |
| Table proxy / Python bindings | Use PyO3 binding to existing Rust types; separate crate |
| Log tables | Integrate into Rust logging ecosystem (`tracing`/`log`); separate crate |
| Sub-table `::` path notation | Convenience only; plain path manipulation already works |
