# casa-rs

Rust foundations for casacore-compatible persistent data structures.

This README is for API users. Contributor/developer policy is in `AGENTS.md`.

## API Documentation

[**Browse the API docs**](https://bglenden.github.io/casa-rs/)

Public API crates:

- `casacore-types`: scalar/array/record value model.
- `casacore-tables`: table-facing API crate (current facade is intentionally small).

`casacore-aipsio` is primarily an internal implementation crate used by table internals.

## Current User-Facing Capabilities

- Scalars including complex and string values.
- N-dimensional arrays via `ndarray`-backed `ArrayValue`.
- Records (`name -> value` fields) with recursive value support.

## Casacore C++ Module Coverage

Status legend:
- `Available now`: implemented and usable in this repo today.
- `Planned`: explicitly scoped in an existing phase plan.
- `Deferred/Not planned`: intentionally out of current scope.

| casacore-c++ module | casa-rs status | Notes |
|---|---|---|
| `casa` | Partial / Available now | Core value model (`casacore-types`) exists; broader `casa` utility surface is not a parity target. |
| `tables` | Available now + Planned | Core table persistence APIs exist; closeout parity tracked in [Phase 2](docs/Planning/Phase%202%20-%20Table%20fillout/README.md). |
| `measures` | Planned | Scoped in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md). |
| `meas` (TaQL UDF) | Planned (core subset) | Core subset in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md); full catalog deferred. |
| `ms` | Planned | Typed MeasurementSet workflows in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md). |
| `derivedmscal` | Planned (core subset) | Core derived quantities in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md); full parity deferred. |
| `coordinates` | Planned | Coordinate core in Phase 3; broader parity in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `lattices` | Planned | Scoped in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `images` | Planned | Scoped in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `fits` | Deferred/Not planned (full parity) | No full casacore `fits` parity phase; targeted FITS/WCS interop only. |
| `msfits` | Deferred | Deferred in planning; depends on broader FITS and MS parity. |
| `scimath`, `scimath_f` | Not planned | Prefer Rust community math/fitting/statistics crates when needed. |
| `python`, `python3` | Deferred until needed | No current parity target for casacore Python converters/bindings. |
| `mirlib` | Not planned | Out of scope for this Rust implementation. |

Phase progress snapshot by wave file status (2026-03-04): Phase 2 `1/24`,
Phase 3 `0/8`, Phase 4 `0/8`, Phase 5 `0/10` finished.

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
