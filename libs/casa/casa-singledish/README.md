# casa-singledish

Reduce and import single-dish spectra in CASA MeasurementSets.

## Main types

- `SingleDishSession` — Single-dish data selection and operation state.
- `SingleDishSmoothing` — Channel-domain smoothing kernel and output configuration.
- `SingleDishAtmosphere` — Atmospheric correction settings for selected spectra.
- `SingleDishLineFit` — Line-fit configuration for a selected spectrum.

Set the input path on a new session before starting an operation.

```rust
use casa_singledish::SingleDishSession;

let mut session = SingleDishSession::default();
assert!(session.open("input.ms"));
assert!(session.close());
```
