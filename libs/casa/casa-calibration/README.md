# casa-calibration

Solve and apply CASA-compatible calibration for radio interferometers.

## Main types

- `table::CalibrationTableError` — Errors reading or interpreting a calibration table.
- `apply::ApplyCalibrationTableSpec` — A table selection, interpolation, and application specification.
- `solve::GainType` — Supported classes of gain solution.

Parse a calibration table specification without opening a MeasurementSet.

```rust
use casa_calibration::callib::parse_apply_specs;

let entries = parse_apply_specs("example", "caltable='gain.cal'").unwrap();
assert_eq!(entries.len(), 1);
```
