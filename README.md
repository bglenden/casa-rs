# casa-rs

Rust foundations for casacore-compatible persistent data structures.

This README is for API users. Contributor/developer policy is in `AGENTS.md`.

## Public API Crates

- `casacore-types`: scalar/array/record value model.
- `casacore-tables`: table-facing API crate (current facade is intentionally small).

`casacore-aipsio` is primarily an internal implementation crate used by table internals.

## Current User-Facing Capabilities

- Scalars including complex and string values.
- N-dimensional arrays via `ndarray`-backed `ArrayValue`.
- Records (`name -> value` fields) with recursive value support.

## Quick Start

From this repository workspace:

```bash
cargo test --workspace
```

Build API docs:

```bash
cargo doc --workspace --no-deps
```

Then open:

- `target/doc/casacore_types/index.html`
- `target/doc/casacore_tables/index.html`

## Minimal Example (`casacore-types`)

```rust
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

let temperature = Value::Scalar(ScalarValue::Float64(273.15));

let spectrum = Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0, 3.0]));

let metadata = Value::Record(RecordValue::new(vec![
    RecordField::new("name", Value::Scalar(ScalarValue::String("demo".into()))),
    RecordField::new("temperature", temperature.clone()),
    RecordField::new("spectrum", spectrum.clone()),
]));
```

## Demo Program

Rust equivalent of casacore C++ `tAipsIO`:

```bash
cargo run -p casacore-aipsio --example t_aipsio
```
