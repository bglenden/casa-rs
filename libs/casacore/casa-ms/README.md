# casa-ms

Read, write, and select visibility data in casacore MeasurementSets.

## Main types

- `MeasurementSet` — Typed MeasurementSet storage, subtables, and visibility access.
- `MeasurementSetBuilder` — Construction of MeasurementSet schemas and tables.
- `MsSelection` — A reusable selection over MAIN rows, channels, and products.

Select two field IDs for a later visibility operation.

```rust
use casa_ms::MsSelection;

let _selection = MsSelection::new().field(&[0, 2]);
```
