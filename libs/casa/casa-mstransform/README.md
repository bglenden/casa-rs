# casa-mstransform

Select, average, regrid, and materialize CASA MeasurementSets.

## Main types

- `MsTransformRequest` — Input, output, selection, and transformation options.
- `MsTransformReport` — Counts and metadata from a completed transform.
- `TransformDataColumn` — The requested mapping of input visibility columns.
- `TimeAverageOptions` — A time window in seconds and permitted metadata spans.

Choose raw visibility samples explicitly when preparing a transform.

```rust
use casa_mstransform::TransformDataColumn;

let column = TransformDataColumn::Data;
assert_ne!(column, TransformDataColumn::default());
```
