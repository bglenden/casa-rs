# ADR-0008: Casacore storage and bounded MeasurementSet writes

Status: Accepted

Date: 2026-07-18

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
complete interoperable table has been written. In-place mutation creates a
small incomplete-write marker before the first physical change and removes it
after successful finalization. An interrupted mutation is detectable and is
not presented as complete.

The persistence layer does not provide rollback, snapshot generations,
journaling, or copy-on-write recovery. Such a feature requires a new concrete
product requirement and a separate architecture decision.

## Consequences

- CASA-generated MeasurementSets need not follow casa-rs creation conventions
  to be readable or safely mutated.
- Memory use is planned from the real column shapes and writer buffers rather
  than a fixed row-count heuristic.
- New-output failure is isolated before publication. In-place failure may have
  written some cells, but the marker prevents silent acceptance as complete.
- Flag-version tables remain an explicit domain feature of `flagmanager`; they
  are not a transaction or rollback mechanism for general table writes.
- Storage changes require Rust-read/Rust-write and C++-read/C++-write
  interoperability evidence, including heterogeneous `TiledShapeStMan` data
  when that path changes.
