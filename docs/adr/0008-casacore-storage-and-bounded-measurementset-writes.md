# ADR-0008: Casacore storage and bounded MeasurementSet writes

Status: accepted

Date: 2026-07-18

Reaffirmed: 2026-08-26

Amended: 2026-10-09, on owner direction: casa-rs writes nothing into a
MeasurementSet that CASA does not write.

Amended: 2026-10-10, on owner direction: in-place mutation waits for a table
lock another process holds, as casacore does, instead of being refused at
once.

## Context

CASA interoperability depends on the casacore table data model and persisted
data-manager metadata, not on a casa-rs-specific row layout. MeasurementSets
may be created by CASA with different valid storage-manager bindings, tile
shapes, hypercube layouts, and variable array shapes. At the same time,
production writers must not materialize a large MAIN table or retain payloads
proportional to its row count.

Earlier design discussion considered rollback or snapshot machinery for large
writes. No product requirement currently calls for transactional rollback,
historical generations, or user-visible snapshots, and those mechanisms would
add persistent state and recovery complexity to the interoperability boundary.

## Decision

`Table` row, column, and cell accessors remain the primary public table-data
interface. A column's storage manager is a strategic creation-time choice.
Opening an existing table reads its persisted data-manager sequence, type,
columns, properties, and tile or hypercube metadata; mutation preserves those
bindings rather than replacing them with casa-rs conventions.

`casa-tables` natively reads the supported standard casacore managers and
writes their canonical formats. A manager implemented only by an external
casacore plugin may be reported as unsupported. `TiledShapeStMan` uses one
hypercube for each distinct cell shape, and rows with the same shape reuse that
hypercube. Unused planned shapes do not create payload files or row maps.

MAIN-table producers use one `MeasurementSetWritePlan` and one
`MeasurementSetWriteSession`. The immutable plan names every owned scalar and
array column, derives tile geometry from the existing storage planner, fixes
batch and queue sizes, reserves every scalar and array writer buffer, and
reports the maximum modeled writer-owned resident bytes. The session streams
typed cells, installs the planned columns, and reports rows, bytes, producer
time, bounded-queue wait, assembly, physical-write, and finalization time.

Creation uses a sibling staging directory and publishes it only after a
complete interoperable table has been written. In-place mutation holds
casacore's table write lock from its first change until it completes or is
abandoned, as CASA does. When another process holds a lock on the table,
in-place mutation waits for it, as casacore does by default: the waiter adds
its process id to the request list in `table.lock`, so that a holder using
casacore's `AutoLocking` releases its lock at its next inspection, and the
wait is logged when it starts, periodically while it lasts, and when it ends.
A writer that finds another process wrote the table while it waited is
refused, because what it read beforehand is stale. A save that locks several
tables of a MeasurementSet never waits while it holds another table's lock.
A conflicting handle in the same process is refused at once, because it may
belong to the waiting thread. casa-rs does not yet release a lock it holds
when another process requests it
([#694](https://github.com/bglenden/casa-rs/issues/694)); a waiter, CASA's or
casa-rs's, waits until a casa-rs holder, such as an imaging run with its
retained read locks, finishes, and the imaging writer of
`MODEL_DATA` and `CORRECTED_DATA`, which upgrades a read lock it holds, still
makes one attempt for the write lock. On a file system
without lock support (`fcntl` refused with `ENOLCK`, or `ENOTSUP` as on macOS
SMB mounts) the table is used unlocked, with a warning, as casacore does for
`ENOLCK`; there is then no cross-process exclusion. casa-rs adds nothing of its
own to a MeasurementSet: no table keywords, marker files, generations or
identities.
An interrupted in-place write may leave cells partly written, as an
interrupted CASA write does; rerunning the producing task recomputes them.

The persistence layer does not provide rollback, snapshot generations,
journaling, or copy-on-write recovery. Such a feature requires a new concrete
product requirement and a separate architecture decision.

This applies explicitly to imaging `MODEL_DATA` and `CORRECTED_DATA`.
Prediction writes selected cells in place under the exact source-scoped table
lock. The writer retains at most one array cell, persists that cell through
the selected-row/selected-column table seam, and discards its cache entry before
accepting another row; it does not materialize MAIN rows or the full column.
Unrelated MeasurementSets may therefore progress concurrently while two live
writers for the same source remain mutually exclusive. Successful completion
flushes the column and releases the lock. An interrupted write may leave
partial derived values, which the next run recomputes. Full-column staging copies, backup columns, content
digests, and rollback are prohibited unless a later concrete requirement
demonstrates that CASA-compatible in-place behavior is inadequate and
separately accounts for the I/O and storage cost.

## Consequences

- CASA-generated MeasurementSets need not follow casa-rs creation conventions
  to be readable or safely mutated.
- Memory use is planned from the real column shapes and writer buffers rather
  than a fixed row-count heuristic.
- New-output failure is isolated before publication. In-place failure may have
  written some cells, as in CASA; rerunning the task recomputes them.
- A MeasurementSet written by casa-rs carries only what CASA would write, so
  CASA and casa-rs can each open the other's output without preparation.
- Flag-version tables remain an explicit domain feature of `flagmanager`; they
  are not a transaction or rollback mechanism for general table writes.
- Storage changes require Rust-read/Rust-write and C++-read/C++-write
  interoperability evidence, including heterogeneous `TiledShapeStMan` data
  when that path changes.
