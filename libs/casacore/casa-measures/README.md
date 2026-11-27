# casa-measures

Load CASA-table runtime data for astronomical measure conversions.

## Main types

- `MeasuresRuntime` — A validated table tree with explicit discovery and ownership.
- `RuntimePolicy` — Freshness and provenance requirements for opening a tree.
- `SnapshotProvenance` — The CASA and IERS releases used to build the snapshot.
- `EopTable` — Earth orientation samples used by epoch and direction conversions.

Inspect the release snapshot provenance before obtaining its archive.

```rust
use casa_measures::packaged_snapshot_provenance;

let provenance = packaged_snapshot_provenance().unwrap();
assert!(!provenance.measures_version.is_empty());
```
