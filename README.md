# casa-rs

Rust foundations for casacore-compatible persistent data structures.

This README is for API users. Contributor/developer policy is in `AGENTS.md`.

## Public API Crates

- [`casacore-types`](target/doc/casacore_types/index.html): scalar/array/record value model.
- [`casacore-tables`](target/doc/casacore_tables/index.html): table-facing API crate (current facade is intentionally small).

`casacore-aipsio` is primarily an internal implementation crate used by table internals.

Build and browse the full API docs with:

```bash
cargo doc --workspace --no-deps --open
```

## Current User-Facing Capabilities

- Scalars including complex and string values.
- N-dimensional arrays via `ndarray`-backed `ArrayValue`.
- Records (`name -> value` fields) with recursive value support.

## Quick Start

From this repository workspace:

```bash
cargo test --workspace
```

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

## Demo Programs

Each crate that wraps a C++ casacore module includes a Rust demo program
equivalent to the corresponding C++ test/demo. These demos:

- Show idiomatic Rust usage of the crate's public API.
- Include the essential C++ source as comments for comparison.
- Are runnable via `cargo run -p <crate> --example <name>`.

| Crate | Demo | C++ original | Run |
|---|---|---|---|
| `casacore-aipsio` | `t_aipsio` | `tAipsIO.cc` | `cargo run -p casacore-aipsio --example t_aipsio` |
| `casacore-tables` | `t_table` | `tTable.cc` | `cargo run -p casacore-tables --example t_table` |

Demo source lives in each crate's `examples/` directory. The demo logic
is in a `demo` module within the crate, so `cargo doc` renders it alongside
the API docs.

## License

Licensed under the [GNU Lesser General Public License v3.0 or later](COPYING.LESSER)
(SPDX: `LGPL-3.0-or-later`).
