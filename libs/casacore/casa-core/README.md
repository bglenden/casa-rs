# casa-core

Define CASA values, astronomical measures, units, and reference-frame conversions.

## Main types

- `Value` — A typed casacore scalar, array, or record.
- `measures::MDirection` — An astronomical direction in an explicit reference frame.
- `Correlation` — A Stokes or receptor-correlation product.

Construct a floating-point value without inferring its wire type.

```rust
use casa_core::{ScalarValue, Value};

let value = Value::Scalar(ScalarValue::Float64(1.42e9));
assert!(matches!(value, Value::Scalar(ScalarValue::Float64(_))));
```
